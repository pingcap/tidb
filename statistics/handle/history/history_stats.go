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

package history

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics/handle/cache"
	"github.com/pingcap/tidb/util/sqlexec"
)

// RecordHistoricalStatsMeta records the historical stats meta.
func RecordHistoricalStatsMeta(sctx sessionctx.Context, tableID int64, version uint64, source string) error {
	if tableID == 0 || version == 0 {
		return errors.Errorf("tableID %d, version %d are invalid", tableID, version)
	}
	if !sctx.GetSessionVars().EnableHistoricalStats {
		return nil
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	exec := sctx.(sqlexec.SQLExecutor)
	rexec := sctx.(sqlexec.RestrictedSQLExecutor)
	rows, _, err := rexec.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, "select modify_count, count from mysql.stats_meta where table_id = %? and version = %?", tableID, version)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return errors.New("no historical meta stats can be recorded")
	}
	modifyCount, count := rows[0].GetInt64(0), rows[0].GetInt64(1)

	_, err = exec.ExecuteInternal(ctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()

	const sql = "REPLACE INTO mysql.stats_meta_history(table_id, modify_count, count, version, source, create_time) VALUES (%?, %?, %?, %?, %?, NOW())"
	if _, err := exec.ExecuteInternal(ctx, sql, tableID, modifyCount, count, version, source); err != nil {
		return errors.Trace(err)
	}
	cache.TableRowStatsCache.Invalidate(tableID)
	return nil
}

// finishTransaction will execute `commit` when error is nil, otherwise `rollback`.
func finishTransaction(ctx context.Context, exec sqlexec.SQLExecutor, err error) error {
	if err == nil {
		_, err = exec.ExecuteInternal(ctx, "commit")
	} else {
		_, err1 := exec.ExecuteInternal(ctx, "rollback")
		terror.Log(errors.Trace(err1))
	}
	return errors.Trace(err)
}
