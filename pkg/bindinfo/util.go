// Copyright 2023 PingCAP, Inc.
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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/bindinfo/internal/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// exec is a helper function to execute sql and return RecordSet.
func exec(sctx sessionctx.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	sqlExec := sctx.GetSQLExecutor()
	return sqlExec.ExecuteInternal(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo), sql, args...)
}

// execRows is a helper function to execute sql and return rows and fields.
func execRows(sctx sessionctx.Context, sql string, args ...any) (rows []chunk.Row, fields []*resolve.ResultField, err error) {
	sqlExec := sctx.GetRestrictedSQLExecutor()
	return sqlExec.ExecRestrictedSQL(kv.WithInternalSourceType(context.Background(), kv.InternalTxnBindInfo),
		[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
}

var (
	// UpdateBindingUsageInfoBatchSize indicates the batch size when updating binding usage info to storage.
	UpdateBindingUsageInfoBatchSize = 100
	// MaxWriteInterval indicates the interval at which a write operation needs to be performed after a binding has not been read.
	MaxWriteInterval = 6 * time.Hour
)

func (u *globalBindingHandle) updateBindingUsageInfoToStorage(bindings []Binding) error {
	toWrite := make([]Binding, 0, UpdateBindingUsageInfoBatchSize)
	now := time.Now()
	cnt := 0
	defer func() {
		if cnt > 0 {
			logutil.BindLogger().Info("update binding usage info to storage", zap.Int("count", cnt), zap.Duration("duration", time.Since(now)))
		}
	}()
	for _, binding := range bindings {
		lastUsed := binding.LastUsedAt
		if lastUsed == nil {
			continue
		}
		lastSaved := binding.LastSavedAt
		if shouldUpdateBinding(lastSaved, lastUsed) {
			toWrite = append(toWrite, binding)
			cnt++
		}
		if len(toWrite) == UpdateBindingUsageInfoBatchSize {
			err := u.updateBindingUsageInfoToStorageInternal(toWrite)
			if err != nil {
				return err
			}
			toWrite = toWrite[:0]
		}
	}
	if len(toWrite) > 0 {
		err := u.updateBindingUsageInfoToStorageInternal(toWrite)
		if err != nil {
			return err
		}
	}
	return nil
}

func (u *globalBindingHandle) updateBindingUsageInfoToStorageInternal(bindings []Binding) error {
	err := u.callWithSCtx(true, func(sctx sessionctx.Context) (err error) {
		if err = lockBindInfoTable(sctx); err != nil {
			return errors.Trace(err)
		}
		// lockBindInfoTable is to prefetch the rows and lock them, it is good for performance when
		// there are many bindings to update with multi tidb nodes.
		// in the performance test, it takes 26.24s to update 44679 bindings.
		if err = addLockForBinds(sctx, bindings); err != nil {
			return errors.Trace(err)
		}
		for _, binding := range bindings {
			lastUsed := binding.LastUsedAt
			intest.Assert(lastUsed != nil)
			err = saveBindingUsage(sctx, binding.SQLDigest, binding.PlanDigest, *lastUsed)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err == nil {
		ts := time.Now()
		for _, binding := range bindings {
			binding.UpdateLastSavedAt(&ts)
			err := u.getCache().SetBinding(binding.SQLDigest, []Binding{binding})
			if err != nil {
				logutil.BindLogger().Warn("update binding cache error", zap.Error(err))
			}
		}
	}
	return err
}

func shouldUpdateBinding(lastSaved, lastUsed *time.Time) bool {
	if lastSaved == nil {
		// If it has never been written before, it will be written.
		return true
	}
	// If a certain amount of time specified by MaxWriteInterval has passed since the last record was written,
	// and it has been used in between, it will be written.
	return time.Since(*lastSaved) >= MaxWriteInterval && lastUsed.After(*lastSaved)
}

func addLockForBinds(sctx sessionctx.Context, bindings []Binding) error {
	condition := make([]string, 0, len(bindings))
	for _, binding := range bindings {
		sqlDigest := binding.SQLDigest
		planDigest := binding.PlanDigest
		sql := fmt.Sprintf("('%s'", sqlDigest)
		if planDigest == "" {
			sql += ",NULL)"
		} else {
			sql += fmt.Sprintf(",'%s')", planDigest)
		}
		condition = append(condition, sql)
	}
	locksql := "select 1 from mysql.bind_info use index(digest_index) where (plan_digest, sql_digest) in (" +
		strings.Join(condition, " , ") + ") for update"
	_, err := exec(sctx, locksql)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func saveBindingUsage(sctx sessionctx.Context, sqldigest, planDigest string, ts time.Time) error {
	lastUsedTime := ts.UTC().Format(types.TimeFormat)
	var sql = "UPDATE mysql.bind_info USE INDEX(digest_index) SET last_used_date = CONVERT_TZ(%?, '+00:00', @@TIME_ZONE) WHERE sql_digest = %?"
	if planDigest == "" {
		sql += " AND plan_digest = ''"
	} else {
		sql += fmt.Sprintf(" AND plan_digest = '%s'", planDigest)
	}
	_, err := exec(
		sctx,
		sql,
		lastUsedTime, sqldigest,
	)
	return err
}
