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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
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

// RecordHistoricalStatsMeta records the historical stats meta in mysql.stats_meta_history one by one.
func (sh *statsHistoryImpl) RecordHistoricalStatsMeta(version uint64, source string, enforce bool, tableIDs ...int64) {
	if version == 0 {
		return
	}

	var targetedTableIDs []int64
	if enforce {
		targetedTableIDs = tableIDs
	} else {
		targetedTableIDs = make([]int64, 0, len(tableIDs))
		for _, tableID := range tableIDs {
			tbl, ok := sh.statsHandle.Get(tableID)
			if tableID == 0 || !ok || !tbl.IsInitialized() {
				continue
			}
			targetedTableIDs = append(targetedTableIDs, tableID)
		}
	}
	shouldSkipHistoricalStats := false
	err := handleutil.CallWithSCtx(sh.statsHandle.SPool(), func(sctx sessionctx.Context) error {
		if !sctx.GetSessionVars().EnableHistoricalStats {
			shouldSkipHistoricalStats = true
			return nil
		}
		return nil
	}, handleutil.FlagWrapTxn)
	if err != nil {
		statslogutil.StatsLogger().Error("failed to check historical stats enable status",
			zap.Uint64("version", version),
			zap.String("source", source),
			zap.Int64s("tableIDs", tableIDs),
			zap.Int64s("targetedTableIDs", targetedTableIDs),
			zap.Error(err))
		return
	}
	if !shouldSkipHistoricalStats {
		for _, tableID := range targetedTableIDs {
			err := handleutil.CallWithSCtx(sh.statsHandle.SPool(), func(sctx sessionctx.Context) error {
				return RecordHistoricalStatsMeta(handleutil.StatsCtx, sctx, version, source, tableID)
			}, handleutil.FlagWrapTxn)
			if err != nil {
				statslogutil.StatsLogger().Error("record historical stats meta failed",
					zap.Uint64("version", version),
					zap.String("source", source),
					zap.Int64("tableID", tableID),
					zap.Error(err))
			}
		}
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

// RecordHistoricalStatsMeta records the historical stats meta in mysql.stats_meta_history with the given version and source.
func RecordHistoricalStatsMeta(
	ctx context.Context,
	sctx sessionctx.Context,
	version uint64,
	source string,
	tableID int64,
) error {
	intest.Assert(version != 0, "version should not be zero")
	intest.Assert(tableID != 0, "tableID should not be zero")
	if tableID == 0 || version == 0 {
		return errors.Errorf("tableID %d, version %d are invalid", tableID, version)
	}

	rows, _, err := handleutil.ExecRowsWithCtx(
		ctx,
		sctx,
		"SELECT modify_count, count FROM mysql.stats_meta WHERE table_id = %? and version = %? FOR UPDATE",
		tableID,
		version,
	)
	if err != nil {
		return errors.Trace(err)
	}
	intest.Assert(len(rows) != 0, "no historical meta stats can be recorded")
	if len(rows) == 0 {
		return errors.New("no historical meta stats can be recorded")
	}

	modifyCount, count := rows[0].GetInt64(0), rows[0].GetInt64(1)
	const sql = "REPLACE INTO mysql.stats_meta_history(table_id, modify_count, count, version, source, create_time) VALUES (%?, %?, %?, %?, %?, NOW())"
	if _, err := handleutil.ExecWithCtx(
		ctx,
		sctx,
		sql,
		tableID,
		modifyCount,
		count,
		version,
		source,
	); err != nil {
		return errors.Trace(err)
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
	for i := range blocks {
		if _, err = handleutil.Exec(sctx, sql, physicalID, blocks[i], i, version, ts, blocks[i], ts); err != nil {
			return 0, errors.Trace(err)
		}
	}
	return version, err
}
