// Copyright 2026 PingCAP, Inc.
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
)

func (e *executor) waitColumnarReplicaAvailable(sctx sessionctx.Context, tableID int64, tableIdent ast.Ident) error {
	// Use a separate session: the caller may have an active transaction.
	tempCtx, err := e.sessPool.Get()
	if err != nil {
		return errors.Trace(err)
	}
	defer e.sessPool.Put(tempCtx)
	tempCtx.GetSessionVars().InRestrictedSQL = true
	session := sess.NewSession(tempCtx)
	waitCtx, cancel := context.WithTimeout(e.ctx, time.Hour)
	defer cancel()
	killer := &sctx.GetSessionVars().SQLKiller
	killCh := killer.GetKillEventChan()
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-killCh:
			cancel()
		case <-waitCtx.Done():
		}
	}()
	defer func() { cancel(); <-watchDone }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	failures := 0
	for {
		if err := killer.HandleSignal(); err != nil {
			return err
		}
		if err := waitCtx.Err(); err != nil {
			return dbterror.ErrCancelledDDLJob.GenWithStack("Index is created but waiting for columnar replica availability stopped: %s", err)
		}
		tbl, ok := e.infoCache.GetLatest().TableByID(waitCtx, tableID)
		if !ok {
			return dbterror.ErrCancelledDDLJob.GenWithStack("Table '%s' not found", tableIdent)
		}
		if tbl.Meta().TiFlashReplica == nil || tbl.Meta().TiFlashReplica.Count == 0 {
			return dbterror.ErrCancelledDDLJob.GenWithStack("Columnar replica is not configured for table '%s'", tableIdent)
		}
		available, err := e.checkColumnarReplicaAvailability(waitCtx, session, tableID)
		if err != nil {
			failures++
			if failures >= 10 {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Index is created but columnar replica availability could not be checked: %s", err)
			}
		} else {
			failures = 0
			if available {
				return nil
			}
		}
		failpoint.InjectCall("waitColumnarReplicaAvailable")
		select {
		case <-waitCtx.Done():
		case <-ticker.C:
		}
	}
}

func (e *executor) checkColumnarReplicaAvailability(ctx context.Context, session *sess.Session, physicalTableID int64) (bool, error) {
	// Note: this function assumes table exist and replicaCount is configured.
	// Remember to check them before calling this function.

	// This mock has higher priority, so that we can override behavior.
	failpoint.Inject("MockCheckColumnarReplicaAvailability", func(val failpoint.Value) {
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
