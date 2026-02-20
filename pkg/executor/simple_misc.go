// Copyright 2016 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"syscall"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tls"
	"go.uber.org/zap"
)

func (e *SimpleExec) executeAlterInstance(s *ast.AlterInstanceStmt) error {
	if s.ReloadTLS {
		logutil.BgLogger().Info("execute reload tls", zap.Bool("NoRollbackOnError", s.NoRollbackOnError))
		sm := e.Ctx().GetSessionManager()
		tlsCfg, _, err := util.LoadTLSCertificates(
			variable.GetSysVar("ssl_ca").Value,
			variable.GetSysVar("ssl_key").Value,
			variable.GetSysVar("ssl_cert").Value,
			config.GetGlobalConfig().Security.AutoTLS,
			config.GetGlobalConfig().Security.RSAKeySize,
		)
		if err != nil {
			if !s.NoRollbackOnError || tls.RequireSecureTransport.Load() {
				return err
			}
			logutil.BgLogger().Warn("reload TLS fail but keep working without TLS due to 'no rollback on error'")
		}
		sm.UpdateTLSConfig(tlsCfg)
	}
	return nil
}

func (e *SimpleExec) executeDropStats(ctx context.Context, s *ast.DropStatsStmt) (err error) {
	h := domain.GetDomain(e.Ctx()).StatsHandle()
	var statsIDs []int64
	// TODO: GLOBAL option will be deprecated. Also remove this condition when the syntax is removed
	if s.IsGlobalStats {
		tnW := e.ResolveCtx.GetTableName(s.Tables[0])
		statsIDs = []int64{tnW.TableInfo.ID}
	} else {
		if len(s.PartitionNames) == 0 {
			for _, table := range s.Tables {
				tnW := e.ResolveCtx.GetTableName(table)
				partitionStatIDs, _, err := core.GetPhysicalIDsAndPartitionNames(tnW.TableInfo, nil)
				if err != nil {
					return err
				}
				statsIDs = append(statsIDs, partitionStatIDs...)
				statsIDs = append(statsIDs, tnW.TableInfo.ID)
			}
		} else {
			// TODO: drop stats for specific partition is deprecated. Also remove this condition when the syntax is removed
			tnW := e.ResolveCtx.GetTableName(s.Tables[0])
			if statsIDs, _, err = core.GetPhysicalIDsAndPartitionNames(tnW.TableInfo, s.PartitionNames); err != nil {
				return err
			}
		}
	}
	if err := h.DeleteTableStatsFromKV(statsIDs, true); err != nil {
		return err
	}
	return h.Update(ctx, e.Ctx().GetInfoSchema().(infoschema.InfoSchema))
}

func (e *SimpleExec) autoNewTxn() bool {
	// Some statements cause an implicit commit
	// See https://dev.mysql.com/doc/refman/5.7/en/implicit-commit.html
	switch e.Statement.(type) {
	// Data definition language (DDL) statements that define or modify database objects.
	// (handled in DDL package)
	// Statements that implicitly use or modify tables in the mysql database.
	case *ast.CreateUserStmt, *ast.AlterUserStmt, *ast.DropUserStmt, *ast.RenameUserStmt, *ast.RevokeRoleStmt, *ast.GrantRoleStmt:
		return true
	// Transaction-control and locking statements.  BEGIN, LOCK TABLES, SET autocommit = 1 (if the value is not already 1), START TRANSACTION, UNLOCK TABLES.
	// (handled in other place)
	// Data loading statements. LOAD DATA
	// (handled in other place)
	// Administrative statements. TODO: ANALYZE TABLE, CACHE INDEX, CHECK TABLE, FLUSH, LOAD INDEX INTO CACHE, OPTIMIZE TABLE, REPAIR TABLE, RESET (but not RESET PERSIST).
	case *ast.FlushStmt:
		return true
	}
	return false
}

func (e *SimpleExec) executeShutdown() error {
	sessVars := e.Ctx().GetSessionVars()
	logutil.BgLogger().Info("execute shutdown statement", zap.Uint64("conn", sessVars.ConnectionID))
	p, err := os.FindProcess(os.Getpid())
	if err != nil {
		return err
	}

	// Call with async
	go asyncDelayShutdown(p, time.Second)

	return nil
}

// #14239 - https://github.com/pingcap/tidb/issues/14239
// Need repair 'shutdown' command behavior.
// Response of TiDB is different to MySQL.
// This function need to run with async model, otherwise it will block main coroutine
func asyncDelayShutdown(p *os.Process, delay time.Duration) {
	time.Sleep(delay)
	// Send SIGTERM instead of SIGKILL to allow graceful shutdown and cleanups to work properly.
	err := p.Signal(syscall.SIGTERM)
	if err != nil {
		panic(err)
	}

	// Sending SIGKILL should not be needed as SIGTERM should cause a graceful shutdown after
	// n seconds as configured by the GracefulWaitBeforeShutdown. This is here in case that doesn't
	// work for some reason.
	graceTime := config.GetGlobalConfig().GracefulWaitBeforeShutdown

	// The shutdown is supposed to start at graceTime and is allowed to take up to 10s.
	time.Sleep(time.Second * time.Duration(graceTime+10))
	logutil.BgLogger().Info("Killing process as grace period is over", zap.Int("pid", p.Pid), zap.Int("graceTime", graceTime))
	err = p.Kill()
	if err != nil {
		panic(err)
	}
}

func (e *SimpleExec) executeSetSessionStates(ctx context.Context, s *ast.SetSessionStatesStmt) error {
	var sessionStates sessionstates.SessionStates
	decoder := json.NewDecoder(bytes.NewReader([]byte(s.SessionStates)))
	decoder.UseNumber()
	if err := decoder.Decode(&sessionStates); err != nil {
		return errors.Trace(err)
	}
	return e.Ctx().DecodeStates(ctx, &sessionStates)
}

func (e *SimpleExec) executeAdmin(s *ast.AdminStmt) error {
	switch s.Tp {
	case ast.AdminReloadStatistics:
		return e.executeAdminReloadStatistics(s)
	case ast.AdminFlushPlanCache:
		return e.executeAdminFlushPlanCache(s)
	case ast.AdminSetBDRRole:
		return e.executeAdminSetBDRRole(s)
	case ast.AdminUnsetBDRRole:
		return e.executeAdminUnsetBDRRole()
	}
	return nil
}

func (e *SimpleExec) executeAdminReloadStatistics(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminReloadStatistics {
		return errors.New("This AdminStmt is not ADMIN RELOAD STATS_EXTENDED")
	}
	if !e.Ctx().GetSessionVars().EnableExtendedStats {
		return errors.New("Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	}
	return domain.GetDomain(e.Ctx()).StatsHandle().ReloadExtendedStatistics()
}

func (e *SimpleExec) executeAdminFlushPlanCache(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminFlushPlanCache {
		return errors.New("This AdminStmt is not ADMIN FLUSH PLAN_CACHE")
	}
	if s.StatementScope == ast.StatementScopeGlobal {
		return errors.New("Do not support the 'admin flush global scope.'")
	}
	if !e.Ctx().GetSessionVars().EnablePreparedPlanCache {
		e.Ctx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("The plan cache is disable. So there no need to flush the plan cache"))
		return nil
	}
	now := types.NewTime(types.FromGoTime(time.Now().In(e.Ctx().GetSessionVars().StmtCtx.TimeZone())), mysql.TypeTimestamp, 3)
	e.Ctx().GetSessionVars().LastUpdateTime4PC = now
	e.Ctx().GetSessionPlanCache().DeleteAll()
	if s.StatementScope == ast.StatementScopeInstance {
		// Record the timestamp. When other sessions want to use the plan cache,
		// it will check the timestamp first to decide whether the plan cache should be flushed.
		domain.GetDomain(e.Ctx()).SetExpiredTimeStamp4PC(now)
	}
	return nil
}

func (e *SimpleExec) executeAdminSetBDRRole(s *ast.AdminStmt) error {
	if s.Tp != ast.AdminSetBDRRole {
		return errors.New("This AdminStmt is not ADMIN SET BDR_ROLE")
	}

	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(meta.NewMutator(txn).SetBDRRole(string(s.BDRRole)))
}

func (e *SimpleExec) executeAdminUnsetBDRRole() error {
	txn, err := e.Ctx().Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(meta.NewMutator(txn).ClearBDRRole())
}

func (e *SimpleExec) executeSetResourceGroupName(s *ast.SetResourceGroupStmt) error {
	var name string
	if s.Name.L != "" {
		if _, ok := e.is.ResourceGroupByName(s.Name); !ok {
			return infoschema.ErrResourceGroupNotExists.GenWithStackByArgs(s.Name.O)
		}
		name = s.Name.L
	} else {
		name = resourcegroup.DefaultResourceGroupName
	}
	e.Ctx().GetSessionVars().SetResourceGroupName(name)
	return nil
}

// executeAlterRange is used to alter range configuration. currently, only config placement policy.
func (e *SimpleExec) executeAlterRange(s *ast.AlterRangeStmt) error {
	if s.RangeName.L != placement.KeyRangeGlobal && s.RangeName.L != placement.KeyRangeMeta {
		return errors.New("range name is not supported")
	}
	if s.PlacementOption.Tp != ast.PlacementOptionPolicy {
		return errors.New("only support alter range policy")
	}
	bundle := &placement.Bundle{}
	policyName := ast.NewCIStr(s.PlacementOption.StrValue)
	if policyName.L != placement.DefaultKwd {
		policy, ok := e.is.PolicyByName(policyName)
		if !ok {
			return infoschema.ErrPlacementPolicyNotExists.GenWithStackByArgs(policyName.O)
		}
		tmpBundle, err := placement.NewBundleFromOptions(policy.PlacementSettings)
		if err != nil {
			return err
		}
		// reset according range
		bundle = tmpBundle.RebuildForRange(s.RangeName.L, policyName.L)
	} else {
		// delete all rules
		bundle = bundle.RebuildForRange(s.RangeName.L, policyName.L)
		bundle = &placement.Bundle{ID: bundle.ID}
	}

	return infosync.PutRuleBundlesWithDefaultRetry(context.Background(), []*placement.Bundle{bundle})
}
