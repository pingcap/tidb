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

package ddl

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"go.uber.org/zap"
)
func (d *ddl) startCleanDeadTableLock() {
	defer func() {
		d.wg.Done()
	}()

	defer tidbutil.Recover(metrics.LabelDDL, "startCleanDeadTableLock", nil, false)

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !d.ownerManager.IsOwner() {
				continue
			}
			deadLockTables, err := d.tableLockCkr.GetDeadLockedTables(d.ctx, d.infoCache.GetLatest())
			if err != nil {
				logutil.DDLLogger().Info("get dead table lock failed.", zap.Error(err))
				continue
			}
			for se, tables := range deadLockTables {
				err := d.cleanDeadTableLock(tables, se)
				if err != nil {
					logutil.DDLLogger().Info("clean dead table lock failed.", zap.Error(err))
				}
			}
		case <-d.ctx.Done():
			return
		}
	}
}

// cleanDeadTableLock uses to clean dead table locks.
func (d *ddl) cleanDeadTableLock(unlockTables []model.TableLockTpInfo, se model.SessionInfo) error {
	if len(unlockTables) == 0 {
		return nil
	}
	args := &model.LockTablesArgs{
		UnlockTables: unlockTables,
		SessionInfo:  se,
	}
	job := &model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   unlockTables[0].SchemaID,
		TableID:    unlockTables[0].TableID,
		Type:       model.ActionUnlockTable,
		BinlogInfo: &model.HistoryInfo{},
	}

	ctx, err := d.sessPool.Get()
	if err != nil {
		return err
	}
	defer d.sessPool.Put(ctx)
	err = d.executor.doDDLJob2(ctx, job, args)
	return errors.Trace(err)
}

// SwitchMDL enables MDL or disable MDL.
func (d *ddl) SwitchMDL(enable bool) error {
	isEnableBefore := vardef.IsMDLEnabled()
	if isEnableBefore == enable {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	// Check if there is any DDL running.
	// This check can not cover every corner cases, so users need to guarantee that there is no DDL running by themselves.
	sessCtx, err := d.sessPool.Get()
	if err != nil {
		return err
	}
	defer d.sessPool.Put(sessCtx)
	se := sess.NewSession(sessCtx)
	rows, err := se.Execute(ctx, "select 1 from mysql.tidb_ddl_job", "check job")
	if err != nil {
		return err
	}
	if len(rows) != 0 {
		return errors.New("please wait for all jobs done")
	}

	vardef.SetEnableMDL(enable)
	err = kv.RunInNewTxn(kv.WithInternalSourceType(context.Background(), kv.InternalTxnDDL), d.store, true, func(_ context.Context, txn kv.Transaction) error {
		m := meta.NewMutator(txn)
		oldEnable, _, err := m.GetMetadataLock()
		if err != nil {
			return err
		}
		if oldEnable != enable {
			err = m.SetMetadataLock(enable)
		}
		return err
	})
	if err != nil {
		logutil.DDLLogger().Warn("switch metadata lock feature", zap.Bool("enable", enable), zap.Error(err))
		return err
	}
	logutil.DDLLogger().Info("switch metadata lock feature", zap.Bool("enable", enable))
	return nil
}

// delayForAsyncCommit sleeps `SafeWindow + AllowedClockDrift` before a DDL job finishes.
// It should be called before any DDL that could break data consistency.
// This provides a safe window for async commit and 1PC to commit with an old schema.
func delayForAsyncCommit() {
	if vardef.IsMDLEnabled() {
		// If metadata lock is enabled. The transaction of DDL must begin after
		// pre-write of the async commit transaction, then the commit ts of DDL
		// must be greater than the async commit transaction. In this case, the
		// corresponding schema of the async commit transaction is correct.
		// suppose we're adding index:
		// - schema state -> StateWriteOnly with version V
		// - some txn T started using async commit and version V,
		//   and T do pre-write before or after V+1
		// - schema state -> StateWriteReorganization with version V+1
		// - T commit finish, with TS
		// - 'wait schema synced' finish
		// - schema state -> Done with version V+2, commit-ts of this
		//   transaction must > TS, so it's safe for T to commit.
		return
	}
	cfg := config.GetGlobalConfig().TiKVClient.AsyncCommit
	duration := cfg.SafeWindow + cfg.AllowedClockDrift
	logutil.DDLLogger().Info("sleep before DDL finishes to make async commit and 1PC safe",
		zap.Duration("duration", duration))
	time.Sleep(duration)
}

var (
	// RunInGoTest is used to identify whether ddl in running in the test.
	RunInGoTest bool
)
