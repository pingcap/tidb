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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/types"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	statsutil "github.com/pingcap/tidb/pkg/statistics/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
)

// statsHistoryImpl implements util.StatsHistory.
type statsHistoryImpl struct {
	statsHandle types.StatsHandle
}

// NewStatsHistory creates a new StatsHistory.
func NewStatsHistory(statsHandle types.StatsHandle,
) types.StatsHistory {
	return &statsHistoryImpl{
		statsHandle: statsHandle,
	}
}

// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
func (sh *statsHistoryImpl) RecordHistoricalStatsToStorage(dbName string, tableInfo *model.TableInfo, physicalID int64, isPartition bool) (uint64, error) {
	var js *statsutil.JSONTable
	var err error
	if isPartition {
		js, err = sh.statsHandle.TableStatsToJSON(dbName, tableInfo, physicalID, 0)
	} else {
		js, err = sh.statsHandle.DumpStatsToJSON(dbName, tableInfo, nil, true)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	if js == nil {
		statslogutil.StatsLogger().Warn("no stats data to record", zap.String("dbName", dbName), zap.String("tableName", tableInfo.Name.O))
		return 0, nil
	}
	var version uint64
	err = handleutil.CallWithSCtx(sh.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		version, err = RecordHistoricalStatsToStorage(sctx, physicalID, js)
		return err
	}, handleutil.FlagWrapTxn)
	return version, err
}

// RecordHistoricalStatsMeta records the historical stats meta in mysql.stats_meta_history with a single transaction.
func (sh *statsHistoryImpl) RecordHistoricalStatsMeta(version uint64, source string, enforce bool, tableIDs ...int64) {
	if version == 0 {
		return
	}

	var filteredTableIDs []int64
	if enforce {
		filteredTableIDs = tableIDs
	} else {
		filteredTableIDs = make([]int64, 0, len(tableIDs))
		for _, tableID := range tableIDs {
			tbl, ok := sh.statsHandle.Get(tableID)
			if tableID == 0 || !ok || !tbl.IsInitialized() {
				continue
			}
			filteredTableIDs = append(filteredTableIDs, tableID)
		}
	}

	err := handleutil.CallWithSCtx(sh.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		if !sctx.GetSessionVars().EnableHistoricalStats {
			return nil
		}
		return RecordHistoricalStatsMeta(handleutil.StatsCtx, sctx, version, source, filteredTableIDs...)
	}, handleutil.FlagWrapTxn)

	if err != nil {
		statslogutil.StatsLogger().Error("record historical stats meta failed",
			zap.Uint64("version", version),
			zap.String("source", source),
			zap.Int64s("tableIDs", tableIDs),
			zap.Int64s("filteredTableIDs", filteredTableIDs),
			zap.Error(err))
	}
}

// CheckHistoricalStatsEnable checks whether historical stats is enabled.
func (sh *statsHistoryImpl) CheckHistoricalStatsEnable() (enable bool, err error) {
	err = handleutil.CallWithSCtx(sh.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		enable = sctx.GetSessionVars().EnableHistoricalStats
		return nil
	})
	return
}

// RecordHistoricalStatsMeta records the historical stats meta for multiple tables.
func RecordHistoricalStatsMeta(
	ctx context.Context,
	sctx sessionctx.Context,
	version uint64,
	source string,
	tableIDs ...int64,
) error {
	intest.Assert(version != 0, "version should not be zero")
	intest.AssertFunc(func() bool {
		for _, id := range tableIDs {
			if id == 0 {
				return false
			}
		}
		return true
	}, "tableIDs should not contain 0")
	intest.Assert(len(tableIDs) > 0, "tableIDs should not be empty")
	if len(tableIDs) == 0 {
		return nil
	}

	// Enable prepared statement cache to avoid repeated compilation of the same statement.
	if _, err := handleutil.ExecWithCtx(ctx, sctx, "SET tidb_enable_prepared_plan_cache = ON"); err != nil {
		return errors.Trace(err)
	}
	defer func() {
		_, err := handleutil.ExecWithCtx(ctx, sctx, "SET tidb_enable_prepared_plan_cache = OFF")
		if err != nil {
			statslogutil.StatsLogger().Error("failed to disable prepared statement cache", zap.Error(errors.Trace(err)))
		}
	}()
	prepareSelectForUpdate := `
		PREPARE select_stmt FROM 'SELECT * FROM mysql.stats_meta WHERE table_id = ? AND version = ? FOR UPDATE'
	`
	if _, err := handleutil.ExecWithCtx(ctx, sctx, prepareSelectForUpdate); err != nil {
		return errors.Trace(err)
	}
	defer func() {
		_, err := handleutil.ExecWithCtx(ctx, sctx, "DEALLOCATE PREPARE select_stmt")
		if err != nil {
			statslogutil.StatsLogger().Error("failed to deallocate prepared statement", zap.Error(errors.Trace(err)))
		}
	}()

	for _, tableID := range tableIDs {
		_, err := handleutil.ExecWithCtx(ctx, sctx, fmt.Sprintf("SET @table_id = %d", tableID))
		if err != nil {
			return errors.Trace(err)
		}
		_, err = handleutil.ExecWithCtx(ctx, sctx, fmt.Sprintf("SET @version = %d", version))
		if err != nil {
			return errors.Trace(err)
		}
		_, err = handleutil.ExecWithCtx(ctx, sctx, "EXECUTE select_stmt USING @table_id, @version")
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Convert tableIDs to string for SQL IN clause
	tableIDStrs := make([]string, 0, len(tableIDs))
	for _, id := range tableIDs {
		tableIDStrs = append(tableIDStrs, strconv.FormatInt(id, 10))
	}
	tableIDsStr := strings.Join(tableIDStrs, ",")

	// Single query that combines SELECT and INSERT
	sql := fmt.Sprintf(`REPLACE INTO mysql.stats_meta_history(table_id, modify_count, count, version, source, create_time)
		SELECT table_id, modify_count, count, %d, '%s', NOW()
		FROM mysql.stats_meta
		WHERE table_id IN (%s) AND version = %d`,
		version, source, tableIDsStr, version)

	_, err := handleutil.ExecWithCtx(ctx, sctx, sql)
	if err != nil {
		return errors.Trace(err)
	}

	// Invalidate cache for all tables
	for _, tableID := range tableIDs {
		cache.TableRowStatsCache.Invalidate(tableID)
	}

	return nil
}

// Max column size is 6MB. Refer https://docs.pingcap.com/tidb/dev/tidb-limitations/#limitation-on-a-single-column
// but here is less than 6MB, because stats_history has other info except stats_data.
const maxColumnSize = 5 << 20

// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
func RecordHistoricalStatsToStorage(sctx sessionctx.Context, physicalID int64, js *statsutil.JSONTable) (uint64, error) {
	version := uint64(0)
	if len(js.Partitions) == 0 {
		version = js.Version
	} else {
		for _, p := range js.Partitions {
			version = max(version, p.Version)
		}
	}
	blocks, err := storage.JSONTableToBlocks(js, maxColumnSize)
	if err != nil {
		return version, errors.Trace(err)
	}

	ts := time.Now().Format("2006-01-02 15:04:05.999999")
	const sql = "INSERT INTO mysql.stats_history(table_id, stats_data, seq_no, version, create_time) VALUES (%?, %?, %?, %?, %?)" +
		"ON DUPLICATE KEY UPDATE stats_data=%?, create_time=%?"
	for i := 0; i < len(blocks); i++ {
		if _, err = handleutil.Exec(sctx, sql, physicalID, blocks[i], i, version, ts, blocks[i], ts); err != nil {
			return 0, errors.Trace(err)
		}
	}
	return version, err
}
