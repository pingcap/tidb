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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/handle/cache"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// statsHistoryImpl implements util.StatsHistory.
type statsHistoryImpl struct {
	pool       util.SessionPool
	statsCache util.StatsCache

	// TODO: use interfaces instead of raw function pointers
	tableStatsToJSON func(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*storage.JSONTable, error)
	dumpStatsToJSON  func(dbName string, tableInfo *model.TableInfo, historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*storage.JSONTable, error)
}

// NewStatsHistory creates a new StatsHistory.
func NewStatsHistory(pool util.SessionPool, statsCache util.StatsCache,
	tableStatsToJSON func(dbName string, tableInfo *model.TableInfo, physicalID int64, snapshot uint64) (*storage.JSONTable, error),
	dumpStatsToJSON func(dbName string, tableInfo *model.TableInfo, historyStatsExec sqlexec.RestrictedSQLExecutor, dumpPartitionStats bool) (*storage.JSONTable, error),
) util.StatsHistory {
	return &statsHistoryImpl{
		pool:             pool,
		statsCache:       statsCache,
		tableStatsToJSON: tableStatsToJSON,
		dumpStatsToJSON:  dumpStatsToJSON,
	}
}

// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
func (sh *statsHistoryImpl) RecordHistoricalStatsToStorage(dbName string, tableInfo *model.TableInfo, physicalID int64, isPartition bool) (uint64, error) {
	var js *storage.JSONTable
	var err error
	if isPartition {
		js, err = sh.tableStatsToJSON(dbName, tableInfo, physicalID, 0)
	} else {
		js, err = sh.dumpStatsToJSON(dbName, tableInfo, nil, true)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}

	var version uint64
	err = util.CallWithSCtx(sh.pool, func(sctx sessionctx.Context) error {
		version, err = RecordHistoricalStatsToStorage(sctx, physicalID, js)
		return err
	}, util.FlagWrapTxn)
	return version, err
}

// RecordHistoricalStatsMeta records stats meta of the specified version to stats_meta_history table.
func (sh *statsHistoryImpl) RecordHistoricalStatsMeta(tableID int64, version uint64, source string) {
	if version == 0 {
		return
	}
	tbl, ok := sh.statsCache.Get(tableID)
	if !ok {
		return
	}
	if !tbl.IsInitialized() {
		return
	}
	err := util.CallWithSCtx(sh.pool, func(sctx sessionctx.Context) error {
		return RecordHistoricalStatsMeta(sctx, tableID, version, source)
	})
	if err != nil { // just log the error, hide the error from the outside caller.
		logutil.BgLogger().Error("record historical stats meta failed",
			zap.Int64("table-id", tableID),
			zap.Uint64("version", version),
			zap.String("source", source),
			zap.Error(err))
	}
}

// CheckHistoricalStatsEnable checks whether historical stats is enabled.
func (sh *statsHistoryImpl) CheckHistoricalStatsEnable() (enable bool, err error) {
	err = util.CallWithSCtx(sh.pool, func(sctx sessionctx.Context) error {
		enable = sctx.GetSessionVars().EnableHistoricalStats
		return nil
	})
	return
}

// RecordHistoricalStatsMeta records the historical stats meta.
func RecordHistoricalStatsMeta(sctx sessionctx.Context, tableID int64, version uint64, source string) error {
	if tableID == 0 || version == 0 {
		return errors.Errorf("tableID %d, version %d are invalid", tableID, version)
	}
	if !sctx.GetSessionVars().EnableHistoricalStats {
		return nil
	}
	rows, _, err := util.ExecRows(sctx, "select modify_count, count from mysql.stats_meta where table_id = %? and version = %?", tableID, version)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		return errors.New("no historical meta stats can be recorded")
	}
	modifyCount, count := rows[0].GetInt64(0), rows[0].GetInt64(1)

	_, err = util.Exec(sctx, "begin pessimistic")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = util.FinishTransaction(sctx, err)
	}()

	const sql = "REPLACE INTO mysql.stats_meta_history(table_id, modify_count, count, version, source, create_time) VALUES (%?, %?, %?, %?, %?, NOW())"
	if _, err := util.Exec(sctx, sql, tableID, modifyCount, count, version, source); err != nil {
		return errors.Trace(err)
	}
	cache.TableRowStatsCache.Invalidate(tableID)
	return nil
}

// Max column size is 6MB. Refer https://docs.pingcap.com/tidb/dev/tidb-limitations/#limitation-on-a-single-column
const maxColumnSize = 6 << 20

// RecordHistoricalStatsToStorage records the given table's stats data to mysql.stats_history
func RecordHistoricalStatsToStorage(sctx sessionctx.Context, physicalID int64, js *storage.JSONTable) (uint64, error) {
	version := uint64(0)
	if len(js.Partitions) == 0 {
		version = js.Version
	} else {
		for _, p := range js.Partitions {
			version = p.Version
			if version != 0 {
				break
			}
		}
	}
	blocks, err := storage.JSONTableToBlocks(js, maxColumnSize)
	if err != nil {
		return version, errors.Trace(err)
	}

	ts := time.Now().Format("2006-01-02 15:04:05.999999")
	const sql = "INSERT INTO mysql.stats_history(table_id, stats_data, seq_no, version, create_time) VALUES (%?, %?, %?, %?, %?)"
	for i := 0; i < len(blocks); i++ {
		if _, err := util.Exec(sctx, sql, physicalID, blocks[i], i, version, ts); err != nil {
			return 0, errors.Trace(err)
		}
	}
	return version, err
}
