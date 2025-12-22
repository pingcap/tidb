// Copyright 2025 PingCAP, Inc.
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

package bindinfo

import (
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/stringutil"
)

// BindingOperator is used to operate (create/drop/update/GC) bindings.
type BindingOperator interface {
	// CreateBinding creates a Bindings to the storage and the cache.
	// It replaces all the exists bindings for the same normalized SQL.
	CreateBinding(sctx sessionctx.Context, bindings []*Binding) (err error)

	// DropBinding drop Bindings to the storage and Bindings int the cache.
	DropBinding(sqlDigests []string) (deletedRows uint64, err error)

	// SetBindingStatus set a Bindings's status to the storage and bind cache.
	SetBindingStatus(newStatus, sqlDigest string) (ok bool, err error)

	// GCBinding physically removes the deleted bind records in mysql.bind_info.
	GCBinding() (err error)
}

type bindingOperator struct {
	cache BindingCacheUpdater
	sPool util.DestroyableSessionPool
}

func newBindingOperator(sPool util.DestroyableSessionPool, cache BindingCacheUpdater) BindingOperator {
	return &bindingOperator{
		sPool: sPool,
		cache: cache,
	}
}

// TestTimeLagInLoadingBinding is used for test only.
var TestTimeLagInLoadingBinding stringutil.StringerStr = "TestTimeLagInLoadingBinding"

// CreateBinding creates a Bindings to the storage and the cache.
// It replaces all the exists bindings for the same normalized SQL.
func (op *bindingOperator) CreateBinding(sctx sessionctx.Context, bindings []*Binding) (err error) {
	for _, binding := range bindings {
		if err := prepareHints(sctx, binding); err != nil {
			return err
		}
	}
	defer func() {
		if err == nil {
			err = op.cache.LoadFromStorageToCache(false)
		}
	}()

	var mockTimeLag time.Duration // mock time lag between different TiDB instances for test, see #64250.
	if intest.InTest && sctx.Value(TestTimeLagInLoadingBinding) != nil {
		mockTimeLag = sctx.Value(TestTimeLagInLoadingBinding).(time.Duration)
	}

	return callWithSCtx(op.sPool, true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		for i, binding := range bindings {
			now := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 6)
			if intest.InTest && mockTimeLag != 0 {
				now = types.NewTime(types.FromGoTime(time.Now().Add(-mockTimeLag)), mysql.TypeTimestamp, 6)
			}

			updateTs := now.String()
			_, err = exec(
				sctx,
				`UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %?`,
				StatusDeleted,
				updateTs,
				binding.OriginalSQL,
				updateTs,
			)
			if err != nil {
				return err
			}

			binding.CreateTime = now
			binding.UpdateTime = now

			// TODO: update the sql_mode or sctx.types.Flag to let execution engine returns errors like dataTooLong,
			// overflow directly.

			// Insert the Bindings to the storage.
			var sqlDigest, planDigest any // null by default
			if binding.SQLDigest != "" {
				sqlDigest = binding.SQLDigest
			}
			if binding.PlanDigest != "" {
				planDigest = binding.PlanDigest
			}
			_, err = exec(
				sctx,
				`INSERT INTO mysql.bind_info(
 original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source, sql_digest, plan_digest
) VALUES (%?,%?, %?, %?, %?, %?, %?, %?, %?, %?, %?)`,
				binding.OriginalSQL,
				binding.BindSQL,
				strings.ToLower(binding.Db),
				binding.Status,
				binding.CreateTime.String(),
				binding.UpdateTime.String(),
				binding.Charset,
				binding.Collation,
				binding.Source,
				sqlDigest,
				planDigest,
			)
			failpoint.Inject("CreateGlobalBindingNthFail", func(val failpoint.Value) {
				n := val.(int)
				if n == i {
					err = errors.NewNoStackError("An injected error")
				}
			})
			if err != nil {
				return err
			}

			warnings, _, err := execRows(sctx, "show warnings")
			if err != nil {
				return err
			}
			if len(warnings) != 0 {
				return errors.New(warnings[0].GetString(2))
			}
		}
		return nil
	})
}

// DropBinding drop Bindings to the storage and Bindings int the cache.
func (op *bindingOperator) DropBinding(sqlDigests []string) (deletedRows uint64, err error) {
	if len(sqlDigests) == 0 {
		return 0, errors.New("sql digest is empty")
	}
	defer func() {
		if err == nil {
			err = op.cache.LoadFromStorageToCache(false)
		}
	}()

	err = callWithSCtx(op.sPool, true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		for _, sqlDigest := range sqlDigests {
			updateTs := types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 6).String()
			_, err = exec(
				sctx,
				`UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status != %?`,
				StatusDeleted,
				updateTs,
				sqlDigest,
				updateTs,
				StatusDeleted,
			)
			if err != nil {
				return err
			}
			deletedRows += sctx.GetSessionVars().StmtCtx.AffectedRows()
		}
		return nil
	})
	if err != nil {
		deletedRows = 0
	}
	return deletedRows, err
}

// SetBindingStatus set a Bindings's status to the storage and bind cache.
func (op *bindingOperator) SetBindingStatus(newStatus, sqlDigest string) (ok bool, err error) {
	var (
		updateTs               types.Time
		oldStatus0, oldStatus1 string
	)
	if newStatus == StatusDisabled {
		// For compatibility reasons, when we need to 'set binding disabled for <stmt>',
		// we need to consider both the 'enabled' and 'using' status.
		oldStatus0 = StatusUsing
		oldStatus1 = StatusEnabled
	} else if newStatus == StatusEnabled {
		// In order to unify the code, two identical old statuses are set.
		oldStatus0 = StatusDisabled
		oldStatus1 = StatusDisabled
	}

	defer func() {
		if err == nil {
			err = op.cache.LoadFromStorageToCache(false)
		}
	}()

	err = callWithSCtx(op.sPool, true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with SetBindingStatus on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		updateTs = types.NewTime(types.FromGoTime(time.Now()), mysql.TypeTimestamp, 6)
		updateTsStr := updateTs.String()

		_, err = exec(sctx, `UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status IN (%?, %?)`,
			newStatus, updateTsStr, sqlDigest, updateTsStr, oldStatus0, oldStatus1)
		ok = sctx.GetSessionVars().StmtCtx.AffectedRows() > 0
		return err
	})
	return
}

// GCBinding physically removes the deleted bind records in mysql.bind_info.
func (op *bindingOperator) GCBinding() (err error) {
	return callWithSCtx(op.sPool, true, func(sctx sessionctx.Context) error {
		// Lock mysql.bind_info to synchronize with CreateBinding / AddBinding / DropBinding on other tidb instances.
		if err = lockBindInfoTable(sctx); err != nil {
			return err
		}

		// To make sure that all the deleted bind records have been acknowledged to all tidb,
		// we only garbage collect those records with update_time before 10 leases.
		updateTime := time.Now().Add(-(10 * Lease))
		updateTimeStr := types.NewTime(types.FromGoTime(updateTime), mysql.TypeTimestamp, 6).String()
		_, err = exec(sctx, `DELETE FROM mysql.bind_info WHERE status = 'deleted' and update_time < %?`, updateTimeStr)
		return err
	})
}

// lockBindInfoTable simulates `LOCK TABLE mysql.bind_info WRITE` by acquiring a pessimistic lock on a
// special builtin row of mysql.bind_info. Note that this function must be called with h.sctx.Lock() held.
// We can replace this implementation to normal `LOCK TABLE mysql.bind_info WRITE` if that feature is
// generally available later.
// This lock would enforce the CREATE / DROP GLOBAL BINDING statements to be executed sequentially,
// even if they come from different tidb instances.
func lockBindInfoTable(sctx sessionctx.Context) error {
	// h.sctx already locked.
	_, err := exec(sctx, LockBindInfoSQL)
	return err
}
