// Copyright 2024 PingCAP, Inc.
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
//
// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package ddl

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"go.uber.org/zap"
)

var (
	// TestBarrierCheckColumnarReplicaAvailability is only used in tests for notifying
	// that a waiting is triggered.
	TestBarrierCheckColumnarReplicaAvailability chan int = nil
)

func (e *executor) waitColumnarReplicaAvailable(sctx sessionctx.Context, schemaID int64, physicalTableID int64, tableIdent ast.Ident) error {
	// Note: tableIdent and indexName is only used for printing messages or logs.
	// They are not used for checking table existence because table or schema may be renamed
	// during the waiting period.

	// In `checkColumnarReplicaAvailability`, we will execute an internal SQL to check the availability
	// of tiflash Replica. When the transaction is enabled, the execution of the internal SQL may
	// change the transaction information of sctx, so we do not use sctx to maintain the transaction information.
	tempCtx, err := e.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		e.sessPool.Put(tempCtx)
	}()
	tempCtx.GetSessionVars().InRestrictedSQL = true
	session := sess.NewSession(tempCtx)

	waitTimeout := 1 * time.Hour

	maxFailures := 10
	failures := 0

	dedupLogCounter := 0

	startWaitingTime := time.Now()
	ticker := time.Tick(waitTimeout)
	for {
		select {
		case <-ticker:
			return dbterror.ErrCancelledDDLJob.GenWithStack("Index is created but failed to wait for columnar replica available. This means you cannot be utilized now, until replication is finished. This issue is usually caused by too many data to replicate, or replication is stuck due to some problems.")
		default:
		}

		signal := sctx.GetSessionVars().SQLKiller.GetKillSignal()
		killed := signal == sqlkiller.QueryInterrupted
		if killed {
			return dbterror.ErrCancelledDDLJob.GenWithStack("DDL job is cancelled")
		}

		is := e.infoCache.GetLatest()
		t, ok := is.TableByID(e.ctx, physicalTableID) // TODO: Support partition table
		if !ok {
			// Cannot use infoschema.ErrTableNotExists, because DDL framework will turn it into schema outdate error.
			return dbterror.ErrCancelledDDLJob.GenWithStack("Table '%s' not found", tableIdent)
		}

		if t.Meta().TiFlashReplica == nil || t.Meta().TiFlashReplica.Count == 0 {
			// SET TIFLASH REPLICA 0 is called during waiting period.
			// No need to wait any more.
			return dbterror.ErrCancelledDDLJob.GenWithStack("Columnar replica is not configured for table '%s'", tableIdent)
		}

		// TODO: By using e.ctx, this may run long time and cannot be killed?
		isAvailable, err := e.checkColumnarReplicaAvailability(e.ctx, session, physicalTableID)
		if err != nil {
			failures++
			logutil.BgLogger().Error("[ddl] check columnar replica availability failed",
				zap.Stringer("table", tableIdent),
				zap.Error(err))
			if failures >= maxFailures {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Index is created but failed to wait for columnar replica available after some retries: %s", err)
			}
		}

		if isAvailable {
			break
		}

		// Print log every 30 seconds
		if dedupLogCounter%30 == 0 {
			logutil.BgLogger().Info("[ddl] waiting columnar replica available",
				zap.Stringer("table", tableIdent),
				zap.Int64("schemaID", schemaID),
				zap.Int64("physicalTableID", physicalTableID),
				zap.Duration("elapsed", time.Since(startWaitingTime)),
				zap.Duration("maxWaitTimeout", waitTimeout))
			dedupLogCounter++
		}

		time.Sleep(1000 * time.Millisecond)
	}
	return nil
}

func (e *executor) checkColumnarReplicaAvailability(ctx context.Context, session *sess.Session, physicalTableID int64) (bool, error) {
	// Note: this function assumes table exist and replicaCount is configured.
	// Remember to check them before calling this function.

	// This mock has higher priority, so that we can override behavior.
	failpoint.Inject("MockCheckColumnarReplicaAvailability", func(val failpoint.Value) {
		if valInt, ok := val.(int); ok {
			// Move forward the barrier if exists.
			if TestBarrierCheckColumnarReplicaAvailability != nil {
				select {
				case TestBarrierCheckColumnarReplicaAvailability <- 1:
				default:
				}
			}
			if valInt < 0 {
				failpoint.Return(false, dbterror.ErrTiFlashBackfillIndex.FastGenByArgs("mock a check error"))
			} else if valInt == 0 {
				failpoint.Return(false, nil)
			} else {
				failpoint.Return(true, nil)
			}
		}
	})

	// This mock exists so that we can simply use one failpoint to continue the whole index adding process.
	failpoint.Inject("MockCheckColumnarIndexProcess", func(val failpoint.Value) {
		if valInt, ok := val.(int); ok {
			if valInt < 0 {
				failpoint.Return(false, dbterror.ErrTiFlashBackfillIndex.FastGenByArgs("mock a check error"))
			} else if valInt == 0 {
				failpoint.Return(false, nil)
			} else {
				failpoint.Return(true, nil)
			}
		}
	})

	// TODO: Support partition table
	sql := fmt.Sprintf("select available from information_schema.tiflash_replica where table_id = %d", physicalTableID)
	rows, err := session.Execute(ctx, sql, "add_columnar_index_check_result")
	if err != nil || len(rows) == 0 {
		return false, errors.Trace(err)
	}
	notAvailableCnt := 0
	for _, row := range rows {
		if row.GetInt64(0) == 0 {
			notAvailableCnt++
		}
	}
	if notAvailableCnt > 0 {
		return false, nil
	}
	return true, nil
}
