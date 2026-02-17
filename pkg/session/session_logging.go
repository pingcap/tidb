// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"context"
	"fmt"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	session_metrics "github.com/pingcap/tidb/pkg/session/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// GetStartTSFromSession returns the startTS in the session `se`
func GetStartTSFromSession(se any) (startTS, processInfoID uint64) {
	tmp, ok := se.(*session)
	if !ok {
		logutil.BgLogger().Error("GetStartTSFromSession failed, can't transform to session struct")
		return 0, 0
	}
	txnInfo := tmp.TxnInfo()
	if txnInfo != nil {
		startTS = txnInfo.StartTS
		if txnInfo.ProcessInfo != nil {
			processInfoID = txnInfo.ProcessInfo.ConnectionID
		}
	}
	logutil.BgLogger().Debug(
		"GetStartTSFromSession getting startTS of internal session",
		zap.Uint64("startTS", startTS), zap.Time("start time", oracle.GetTimeFromTS(startTS)))

	return startTS, processInfoID
}

// logStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc and normal SQL
// if variable.ProcessGeneralLog is set.
func logStmt(execStmt *executor.ExecStmt, s *session) {
	vars := s.GetSessionVars()
	isCrucial := false
	switch stmt := execStmt.StmtNode.(type) {
	case *ast.DropIndexStmt:
		isCrucial = true
		if stmt.IsHypo {
			isCrucial = false
		}
	case *ast.CreateIndexStmt:
		isCrucial = true
		if stmt.IndexOption != nil && stmt.IndexOption.Tp == ast.IndexTypeHypo {
			isCrucial = false
		}
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt,
		*ast.RenameUserStmt, *ast.CreateBindingStmt, *ast.DropBindingStmt, *ast.SetBindingStmt, *ast.BRIEStmt:
		isCrucial = true
	}

	if isCrucial {
		user := vars.User
		schemaVersion := s.GetInfoSchema().SchemaMetaVersion()
		if ss, ok := execStmt.StmtNode.(ast.SensitiveStmtNode); ok {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("secure text", ss.SecureText()),
				zap.Stringer("user", user))
		} else {
			logutil.BgLogger().Info("CRUCIAL OPERATION",
				zap.Uint64("conn", vars.ConnectionID),
				zap.Int64("schemaVersion", schemaVersion),
				zap.String("cur_db", vars.CurrentDB),
				zap.String("sql", execStmt.StmtNode.Text()),
				zap.Stringer("user", user))
		}
	} else {
		logGeneralQuery(execStmt, s, false)
	}
}

func logGeneralQuery(execStmt *executor.ExecStmt, s *session, isPrepared bool) {
	vars := s.GetSessionVars()
	if vardef.ProcessGeneralLog.Load() && !vars.InRestrictedSQL {
		var query string
		if isPrepared {
			query = execStmt.OriginText()
		} else {
			query = execStmt.GetTextToLog(false)
		}

		query = executor.QueryReplacer.Replace(query)
		if vars.EnableRedactLog != errors.RedactLogEnable {
			query += redact.String(vars.EnableRedactLog, vars.PlanCacheParams.String())
		}

		fields := []zapcore.Field{
			zap.Uint64("conn", vars.ConnectionID),
			zap.String("session_alias", vars.SessionAlias),
			zap.String("user", vars.User.LoginString()),
			zap.Int64("schemaVersion", s.GetInfoSchema().SchemaMetaVersion()),
			zap.Uint64("txnStartTS", vars.TxnCtx.StartTS),
			zap.Uint64("forUpdateTS", vars.TxnCtx.GetForUpdateTS()),
			zap.Bool("isReadConsistency", vars.IsIsolation(ast.ReadCommitted)),
			zap.String("currentDB", vars.CurrentDB),
			zap.Bool("isPessimistic", vars.TxnCtx.IsPessimistic),
			zap.String("sessionTxnMode", vars.GetReadableTxnMode()),
			zap.String("sql", query),
		}
		if ot := execStmt.OriginText(); ot != execStmt.Text() {
			fields = append(fields, zap.String("originText", strconv.Quote(ot)))
		}
		logutil.GeneralLogger.Info("GENERAL_LOG", fields...)
	}
}

func (s *session) recordOnTransactionExecution(err error, counter int, duration float64, isInternal bool) {
	if s.sessionVars.TxnCtx.IsPessimistic {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationPessimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationPessimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationPessimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionPessimisticOKGeneral.Observe(float64(counter))
			}
		}
	} else {
		if err != nil {
			if isInternal {
				session_metrics.TransactionDurationOptimisticAbortInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticAbortGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticErrorGeneral.Observe(float64(counter))
			}
		} else {
			if isInternal {
				session_metrics.TransactionDurationOptimisticCommitInternal.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKInternal.Observe(float64(counter))
			} else {
				session_metrics.TransactionDurationOptimisticCommitGeneral.Observe(duration)
				session_metrics.StatementPerTransactionOptimisticOKGeneral.Observe(float64(counter))
			}
		}
	}
}

func (s *session) checkPlacementPolicyBeforeCommit(ctx context.Context) error {
	var err error
	// Get the txnScope of the transaction we're going to commit.
	txnScope := s.GetSessionVars().TxnCtx.TxnScope
	if txnScope == "" {
		txnScope = kv.GlobalTxnScope
	}
	if txnScope != kv.GlobalTxnScope {
		is := s.GetInfoSchema().(infoschema.InfoSchema)
		deltaMap := s.GetSessionVars().TxnCtx.TableDeltaMap
		for physicalTableID := range deltaMap {
			var tableName string
			var partitionName string
			tblInfo, _, partInfo := is.FindTableByPartitionID(physicalTableID)
			if tblInfo != nil && partInfo != nil {
				tableName = tblInfo.Meta().Name.String()
				partitionName = partInfo.Name.String()
			} else {
				tblInfo, _ := is.TableByID(ctx, physicalTableID)
				tableName = tblInfo.Meta().Name.String()
			}
			bundle, ok := is.PlacementBundleByPhysicalTableID(physicalTableID)
			if !ok {
				errMsg := fmt.Sprintf("table %v doesn't have placement policies with txn_scope %v",
					tableName, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v doesn't have placement policies with txn_scope %v",
						tableName, partitionName, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			dcLocation, ok := bundle.GetLeaderDC(placement.DCLabelKey)
			if !ok {
				errMsg := fmt.Sprintf("table %v's leader placement policy is not defined", tableName)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader placement policy is not defined", tableName, partitionName)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			if dcLocation != txnScope {
				errMsg := fmt.Sprintf("table %v's leader location %v is out of txn_scope %v", tableName, dcLocation, txnScope)
				if len(partitionName) > 0 {
					errMsg = fmt.Sprintf("table %v's partition %v's leader location %v is out of txn_scope %v",
						tableName, partitionName, dcLocation, txnScope)
				}
				err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(errMsg)
				break
			}
			// FIXME: currently we assume the physicalTableID is the partition ID. In future, we should consider the situation
			// if the physicalTableID belongs to a Table.
			partitionID := physicalTableID
			tbl, _, partitionDefInfo := is.FindTableByPartitionID(partitionID)
			if tbl != nil {
				tblInfo := tbl.Meta()
				state := tblInfo.Partition.GetStateByID(partitionID)
				if state == model.StateGlobalTxnOnly {
					err = dbterror.ErrInvalidPlacementPolicyCheck.GenWithStackByArgs(
						fmt.Sprintf("partition %s of table %s can not be written by local transactions when its placement policy is being altered",
							tblInfo.Name, partitionDefInfo.Name))
					break
				}
			}
		}
	}
	return err
}
