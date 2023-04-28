// Copyright 2022 PingCAP, Inc.
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

package handle

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	// StatsMetaHistorySourceAnalyze indicates stats history meta source from analyze
	StatsMetaHistorySourceAnalyze = "analyze"
	// StatsMetaHistorySourceLoadStats indicates stats history meta source from load stats
	StatsMetaHistorySourceLoadStats = "load stats"
	// StatsMetaHistorySourceFlushStats indicates stats history meta source from flush stats
	StatsMetaHistorySourceFlushStats = "flush stats"
	// StatsMetaHistorySourceExtendedStats indicates stats history meta source from extended stats
	StatsMetaHistorySourceExtendedStats = "extended stats"
	// StatsMetaHistorySourceSchemaChange indicates stats history meta source from schema change
	StatsMetaHistorySourceSchemaChange = "schema change"
	// StatsMetaHistorySourceFeedBack indicates stats history meta source from feedback
	StatsMetaHistorySourceFeedBack = "feedback"
)

func recordHistoricalStatsMeta(sctx sessionctx.Context, tableID int64, version uint64, source string) error {
	if tableID == 0 || version == 0 {
		return errors.Errorf("tableID %d, version %d are invalid", tableID, version)
	}
	historicalStatsEnabled, err := checkHistoricalStatsEnable(sctx)
	if err != nil {
		return errors.Errorf("check tidb_enable_historical_stats failed: %v", err)
	}
	if !historicalStatsEnabled {
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
	return nil
}

func (h *Handle) recordHistoricalStatsMeta(tableID int64, version uint64, source string) {
	v := h.statsCache.Load()
	if v == nil {
		return
	}
	sc := v.(statsCache)
	tbl, ok := sc.Get(tableID)
	if !ok {
		return
	}
	if !tbl.IsInitialized() {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	err := recordHistoricalStatsMeta(h.mu.ctx, tableID, version, source)
	if err != nil {
		logutil.BgLogger().Error("record historical stats meta failed",
			zap.Int64("table-id", tableID),
			zap.Uint64("version", version),
			zap.String("source", source),
			zap.Error(err))
	}
}
