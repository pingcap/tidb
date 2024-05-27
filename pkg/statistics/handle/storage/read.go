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

package storage

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func StatsMetaCountAndModifyCount(sctx sessionctx.Context, tableID int64) (count, modifyCount int64, isNull bool, err error) {
	rows, _, err := util.ExecRows(sctx, "select count, modify_count from mysql.stats_meta where table_id = %?", tableID)
	if err != nil {
		return 0, 0, false, err
	}
	if len(rows) == 0 {
		return 0, 0, true, nil
	}
	count = int64(rows[0].GetUint64(0))
	modifyCount = rows[0].GetInt64(1)
	return count, modifyCount, false, nil
}

// HistMetaFromStorage reads the meta info of the histogram from the storage.
func HistMetaFromStorage(sctx sessionctx.Context, item *model.TableItemID, possibleColInfo *model.ColumnInfo) (*statistics.Histogram, *types.Datum, int64, int64, error) {
	isIndex := 0
	var tp *types.FieldType
	if item.IsIndex {
		isIndex = 1
		tp = types.NewFieldType(mysql.TypeBlob)
	} else {
		tp = &possibleColInfo.FieldType
	}
	rows, _, err := util.ExecRows(sctx,
		"select distinct_count, version, null_count, tot_col_size, stats_ver, correlation, flag, last_analyze_pos from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?",
		item.TableID,
		item.ID,
		isIndex,
	)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if len(rows) == 0 {
		return nil, nil, 0, 0, nil
	}
	hist := statistics.NewHistogram(item.ID, rows[0].GetInt64(0), rows[0].GetInt64(2), rows[0].GetUint64(1), tp, chunk.InitialCapacity, rows[0].GetInt64(3))
	hist.Correlation = rows[0].GetFloat64(5)
	lastPos := rows[0].GetDatum(7, types.NewFieldType(mysql.TypeBlob))
	return hist, &lastPos, rows[0].GetInt64(4), rows[0].GetInt64(6), nil
}

// HistogramFromStorage reads histogram from storage.
func HistogramFromStorage(sctx sessionctx.Context, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64, totColSize int64, corr float64) (_ *statistics.Histogram, err error) {
	rows, fields, err := util.ExecRows(sctx, "select count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %? order by bucket_id", tableID, isIndex, colID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := statistics.NewHistogram(colID, distinct, nullCount, ver, tp, bucketSize, totColSize)
	hg.Correlation = corr
	totalCount := int64(0)
	for i := 0; i < bucketSize; i++ {
		count := rows[i].GetInt64(0)
		repeats := rows[i].GetInt64(1)
		var upperBound, lowerBound types.Datum
		if isIndex == 1 {
			lowerBound = rows[i].GetDatum(2, &fields[2].Column.FieldType)
			upperBound = rows[i].GetDatum(3, &fields[3].Column.FieldType)
		} else {
			d := rows[i].GetDatum(2, &fields[2].Column.FieldType)
			// For new collation data, when storing the bounds of the histogram, we store the collate key instead of the
			// original value.
			// But there's additional conversion logic for new collation data, and the collate key might be longer than
			// the FieldType.flen.
			// If we use the original FieldType here, there might be errors like "Invalid utf8mb4 character string"
			// or "Data too long".
			// So we change it to TypeBlob to bypass those logics here.
			if tp.EvalType() == types.ETString && tp.GetType() != mysql.TypeEnum && tp.GetType() != mysql.TypeSet {
				tp = types.NewFieldType(mysql.TypeBlob)
			}
			lowerBound, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(3, &fields[3].Column.FieldType)
			upperBound, err = d.ConvertTo(statistics.UTCWithAllowInvalidDateCtx, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		totalCount += count
		hg.AppendBucketWithNDV(&lowerBound, &upperBound, totalCount, repeats, rows[i].GetInt64(4))
	}
	hg.PreCalculateScalar()
	return hg, nil
}

// CMSketchAndTopNFromStorage reads CMSketch and TopN from storage.
func CMSketchAndTopNFromStorage(sctx sessionctx.Context, tblID int64, isIndex, histID int64) (_ *statistics.CMSketch, _ *statistics.TopN, err error) {
	topNRows, _, err := util.ExecRows(sctx, "select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, nil, err
	}
	rows, _, err := util.ExecRows(sctx, "select cm_sketch from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return statistics.DecodeCMSketchAndTopN(nil, topNRows)
	}
	return statistics.DecodeCMSketchAndTopN(rows[0].GetBytes(0), topNRows)
}

// CMSketchFromStorage reads CMSketch from storage
func CMSketchFromStorage(sctx sessionctx.Context, tblID int64, isIndex int, histID int64) (_ *statistics.CMSketch, err error) {
	rows, _, err := util.ExecRows(sctx, "select cm_sketch from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return statistics.DecodeCMSketch(rows[0].GetBytes(0))
}

// TopNFromStorage reads TopN from storage
func TopNFromStorage(sctx sessionctx.Context, tblID int64, isIndex int, histID int64) (_ *statistics.TopN, err error) {
	rows, _, err := util.ExecRows(sctx, "select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return statistics.DecodeTopN(rows), nil
}

// FMSketchFromStorage reads FMSketch from storage
func FMSketchFromStorage(sctx sessionctx.Context, tblID int64, isIndex, histID int64) (_ *statistics.FMSketch, err error) {
	rows, _, err := util.ExecRows(sctx, "select value from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return statistics.DecodeFMSketch(rows[0].GetBytes(0))
}

// CheckSkipPartition checks if we can skip loading the partition.
func CheckSkipPartition(sctx sessionctx.Context, tblID int64, isIndex int) error {
	rows, _, err := util.ExecRows(sctx, "select distinct_count from mysql.stats_histograms where table_id =%? and is_index = %?", tblID, isIndex)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return types.ErrPartitionStatsMissing
	}
	return nil
}

// CheckSkipColumnPartiion checks if we can skip loading the partition.
func CheckSkipColumnPartiion(sctx sessionctx.Context, tblID int64, isIndex int, histsID int64) error {
	rows, _, err := util.ExecRows(sctx, "select distinct_count from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histsID)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return types.ErrPartitionColumnStatsMissing
	}
	return nil
}

// ExtendedStatsFromStorage reads extended stats from storage.
func ExtendedStatsFromStorage(sctx sessionctx.Context, table *statistics.Table, tableID int64, loadAll bool) (*statistics.Table, error) {
	failpoint.Inject("injectExtStatsLoadErr", func() {
		failpoint.Return(nil, errors.New("gofail extendedStatsFromStorage error"))
	})
	lastVersion := uint64(0)
	if table.ExtendedStats != nil && !loadAll {
		lastVersion = table.ExtendedStats.LastUpdateVersion
	} else {
		table.ExtendedStats = statistics.NewExtendedStatsColl()
	}
	rows, _, err := util.ExecRows(sctx, "select name, status, type, column_ids, stats, version from mysql.stats_extended where table_id = %? and status in (%?, %?, %?) and version > %?",
		tableID, statistics.ExtendedStatsInited, statistics.ExtendedStatsAnalyzed, statistics.ExtendedStatsDeleted, lastVersion)
	if err != nil || len(rows) == 0 {
		return table, nil
	}
	for _, row := range rows {
		lastVersion = max(lastVersion, row.GetUint64(5))
		name := row.GetString(0)
		status := uint8(row.GetInt64(1))
		if status == statistics.ExtendedStatsDeleted || status == statistics.ExtendedStatsInited {
			delete(table.ExtendedStats.Stats, name)
		} else {
			item := &statistics.ExtendedStatsItem{
				Tp: uint8(row.GetInt64(2)),
			}
			colIDs := row.GetString(3)
			err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
			if err != nil {
				statslogutil.StatsLogger().Error("decode column IDs failed", zap.String("column_ids", colIDs), zap.Error(err))
				return nil, err
			}
			statsStr := row.GetString(4)
			if item.Tp == ast.StatsTypeCardinality || item.Tp == ast.StatsTypeCorrelation {
				if statsStr != "" {
					item.ScalarVals, err = strconv.ParseFloat(statsStr, 64)
					if err != nil {
						statslogutil.StatsLogger().Error("parse scalar stats failed", zap.String("stats", statsStr), zap.Error(err))
						return nil, err
					}
				}
			} else {
				item.StringVals = statsStr
			}
			table.ExtendedStats.Stats[name] = item
		}
	}
	table.ExtendedStats.LastUpdateVersion = lastVersion
	return table, nil
}

func indexStatsFromStorage(sctx sessionctx.Context, row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, loadAll bool, lease time.Duration, tracker *memory.Tracker) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	statsVer := row.GetInt64(7)
	idx := table.Indices[histID]
	flag := row.GetInt64(8)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))

	for _, idxInfo := range tableInfo.Indices {
		if histID != idxInfo.ID {
			continue
		}
		table.ColAndIdxExistenceMap.InsertIndex(idxInfo.ID, idxInfo, statsVer != statistics.Version0)
		// All the objects in the table shares the same stats version.
		// Update here.
		if statsVer != statistics.Version0 {
			table.StatsVer = int(statsVer)
			table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, histVer)
		}
		// We will not load buckets, topn and cmsketch if:
		// 1. lease > 0, and:
		// 2. the index doesn't have any of buckets, topn, cmsketch in memory before, and:
		// 3. loadAll is false.
		// 4. lite-init-stats is true(remove the condition when lite init stats is GA).
		notNeedLoad := lease > 0 &&
			(idx == nil || ((!idx.IsStatsInitialized() || idx.IsAllEvicted()) && idx.LastUpdateVersion < histVer)) &&
			!loadAll &&
			config.GetGlobalConfig().Performance.LiteInitStats
		if notNeedLoad {
			// If we don't have this index in memory, skip it.
			if idx == nil {
				return nil
			}
			idx = &statistics.Index{
				Histogram:  *statistics.NewHistogram(histID, distinct, nullCount, histVer, types.NewFieldType(mysql.TypeBlob), 0, 0),
				StatsVer:   statsVer,
				Info:       idxInfo,
				Flag:       flag,
				PhysicalID: table.PhysicalID,
			}
			if idx.IsAnalyzed() {
				idx.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
			lastAnalyzePos.Copy(&idx.LastAnalyzePos)
			break
		}
		if idx == nil || idx.LastUpdateVersion < histVer || loadAll {
			hg, err := HistogramFromStorage(sctx, table.PhysicalID, histID, types.NewFieldType(mysql.TypeBlob), distinct, 1, histVer, nullCount, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}
			cms, topN, err := CMSketchAndTopNFromStorage(sctx, table.PhysicalID, 1, idxInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			var fmSketch *statistics.FMSketch
			if loadAll {
				// FMSketch is only used when merging partition stats into global stats. When merging partition stats into global stats,
				// we load all the statistics, i.e., loadAll is true.
				fmSketch, err = FMSketchFromStorage(sctx, table.PhysicalID, 1, histID)
				if err != nil {
					return errors.Trace(err)
				}
			}
			idx = &statistics.Index{
				Histogram:  *hg,
				CMSketch:   cms,
				TopN:       topN,
				FMSketch:   fmSketch,
				Info:       idxInfo,
				StatsVer:   statsVer,
				Flag:       flag,
				PhysicalID: table.PhysicalID,
			}
			if statsVer != statistics.Version0 {
				idx.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			}
			lastAnalyzePos.Copy(&idx.LastAnalyzePos)
		}
		break
	}
	if idx != nil {
		if tracker != nil {
			tracker.Consume(idx.MemoryUsage().TotalMemoryUsage())
		}
		table.Indices[histID] = idx
	} else {
		logutil.BgLogger().Debug("we cannot find index id in table info. It may be deleted.", zap.Int64("indexID", histID), zap.String("table", tableInfo.Name.O))
	}
	return nil
}

func columnStatsFromStorage(sctx sessionctx.Context, row chunk.Row, table *statistics.Table, tableInfo *model.TableInfo, loadAll bool, lease time.Duration, tracker *memory.Tracker) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	totColSize := row.GetInt64(6)
	statsVer := row.GetInt64(7)
	correlation := row.GetFloat64(9)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	col := table.Columns[histID]
	flag := row.GetInt64(8)

	for _, colInfo := range tableInfo.Columns {
		if histID != colInfo.ID {
			continue
		}
		table.ColAndIdxExistenceMap.InsertCol(histID, colInfo, statsVer != statistics.Version0 || distinct > 0 || nullCount > 0)
		// All the objects in the table shares the same stats version.
		// Update here.
		if statsVer != statistics.Version0 {
			table.StatsVer = int(statsVer)
			table.LastAnalyzeVersion = max(table.LastAnalyzeVersion, histVer)
		}
		isHandle := tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag())
		// We will not load buckets, topn and cmsketch if:
		// 1. lease > 0, and:
		// 2. this column is not handle or lite-init-stats is true(remove the condition when lite init stats is GA), and:
		// 3. the column doesn't have any of buckets, topn, cmsketch in memory before, and:
		// 4. loadAll is false.
		//
		// Here is the explanation of the condition `!col.IsStatsInitialized() || col.IsAllEvicted()`.
		// For one column:
		// 1. If there is no stats for it in the storage(i.e., analyze has never been executed before), then its stats status
		//    would be `!col.IsStatsInitialized()`. In this case we should go the `notNeedLoad` path.
		// 2. If there exists stats for it in the storage but its stats status is `col.IsAllEvicted()`, there are two
		//    sub cases for this case. One is that the column stats have never been used/needed by the optimizer so they have
		//    never been loaded. The other is that the column stats were loaded and then evicted. For the both sub cases,
		//    we should go the `notNeedLoad` path.
		// 3. If some parts(Histogram/TopN/CMSketch) of stats for it exist in TiDB memory currently, we choose to load all of
		//    its new stats once we find stats version is updated.
		notNeedLoad := lease > 0 &&
			(!isHandle || config.GetGlobalConfig().Performance.LiteInitStats) &&
			(col == nil || ((!col.IsStatsInitialized() || col.IsAllEvicted()) && col.LastUpdateVersion < histVer)) &&
			!loadAll
		if notNeedLoad {
			// If we don't have the column in memory currently, just skip it.
			if col == nil {
				return nil
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *statistics.NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
				Info:       colInfo,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				Flag:       flag,
				StatsVer:   statsVer,
			}
			if col.StatsAvailable() {
				col.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			col.Histogram.Correlation = correlation
			break
		}
		if col == nil || col.LastUpdateVersion < histVer || loadAll {
			hg, err := HistogramFromStorage(sctx, table.PhysicalID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totColSize, correlation)
			if err != nil {
				return errors.Trace(err)
			}
			cms, topN, err := CMSketchAndTopNFromStorage(sctx, table.PhysicalID, 0, colInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			var fmSketch *statistics.FMSketch
			if loadAll {
				// FMSketch is only used when merging partition stats into global stats. When merging partition stats into global stats,
				// we load all the statistics, i.e., loadAll is true.
				fmSketch, err = FMSketchFromStorage(sctx, table.PhysicalID, 0, histID)
				if err != nil {
					return errors.Trace(err)
				}
			}
			col = &statistics.Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *hg,
				Info:       colInfo,
				CMSketch:   cms,
				TopN:       topN,
				FMSketch:   fmSketch,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				Flag:       flag,
				StatsVer:   statsVer,
			}
			if col.StatsAvailable() {
				col.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			break
		}
		if col.TotColSize != totColSize {
			newCol := *col
			newCol.TotColSize = totColSize
			col = &newCol
		}
		break
	}
	if col != nil {
		if tracker != nil {
			tracker.Consume(col.MemoryUsage().TotalMemoryUsage())
		}
		table.Columns[col.ID] = col
	} else {
		// If we didn't find a Column or Index in tableInfo, we won't load the histogram for it.
		// But don't worry, next lease the ddl will be updated, and we will load a same table for two times to
		// avoid error.
		logutil.BgLogger().Debug("we cannot find column in table info now. It may be deleted", zap.Int64("colID", histID), zap.String("table", tableInfo.Name.O))
	}
	return nil
}

// TableStatsFromStorage loads table stats info from storage.
func TableStatsFromStorage(sctx sessionctx.Context, snapshot uint64, tableInfo *model.TableInfo, tableID int64, loadAll bool, lease time.Duration, table *statistics.Table) (_ *statistics.Table, err error) {
	tracker := memory.NewTracker(memory.LabelForAnalyzeMemory, -1)
	tracker.AttachTo(sctx.GetSessionVars().MemTracker)
	defer tracker.Detach()
	// If table stats is pseudo, we also need to copy it, since we will use the column stats when
	// the average error rate of it is small.
	if table == nil || snapshot > 0 {
		histColl := statistics.HistColl{
			PhysicalID:     tableID,
			HavePhysicalID: true,
			Columns:        make(map[int64]*statistics.Column, 4),
			Indices:        make(map[int64]*statistics.Index, 4),
		}
		table = &statistics.Table{
			HistColl:              histColl,
			ColAndIdxExistenceMap: statistics.NewColAndIndexExistenceMap(len(tableInfo.Columns), len(tableInfo.Indices)),
		}
	} else {
		// We copy it before writing to avoid race.
		table = table.Copy()
	}
	table.Pseudo = false

	realtimeCount, modidyCount, isNull, err := StatsMetaCountAndModifyCount(sctx, tableID)
	if err != nil || isNull {
		return nil, err
	}
	table.ModifyCount = modidyCount
	table.RealtimeCount = realtimeCount

	rows, _, err := util.ExecRows(sctx, "select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from mysql.stats_histograms where table_id = %?", tableID)
	// Check deleted table.
	if err != nil || len(rows) == 0 {
		return nil, nil
	}
	for _, row := range rows {
		if err := sctx.GetSessionVars().SQLKiller.HandleSignal(); err != nil {
			return nil, err
		}
		if row.GetInt64(1) > 0 {
			err = indexStatsFromStorage(sctx, row, table, tableInfo, loadAll, lease, tracker)
		} else {
			err = columnStatsFromStorage(sctx, row, table, tableInfo, loadAll, lease, tracker)
		}
		if err != nil {
			return nil, err
		}
	}
	return ExtendedStatsFromStorage(sctx, table, tableID, loadAll)
}

// LoadHistogram will load histogram from storage.
func LoadHistogram(sctx sessionctx.Context, tableID int64, isIndex int, histID int64, tableInfo *model.TableInfo) (*statistics.Histogram, error) {
	row, _, err := util.ExecRows(sctx, "select distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, histID)
	if err != nil || len(row) == 0 {
		return nil, err
	}
	distinct := row[0].GetInt64(0)
	histVer := row[0].GetUint64(1)
	nullCount := row[0].GetInt64(2)
	var totColSize int64
	var corr float64
	var tp types.FieldType
	if isIndex == 0 {
		totColSize = row[0].GetInt64(3)
		corr = row[0].GetFloat64(6)
		for _, colInfo := range tableInfo.Columns {
			if histID != colInfo.ID {
				continue
			}
			tp = colInfo.FieldType
			break
		}
		return HistogramFromStorage(sctx, tableID, histID, &tp, distinct, isIndex, histVer, nullCount, totColSize, corr)
	}
	return HistogramFromStorage(sctx, tableID, histID, types.NewFieldType(mysql.TypeBlob), distinct, isIndex, histVer, nullCount, 0, 0)
}

// LoadNeededHistograms will load histograms for those needed columns/indices.
func LoadNeededHistograms(sctx sessionctx.Context, statsCache statstypes.StatsCache, loadFMSketch bool) (err error) {
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		if !item.IsIndex {
			err = loadNeededColumnHistograms(sctx, statsCache, item.TableItemID, loadFMSketch, item.FullLoad)
		} else {
			// Index is always full load.
			err = loadNeededIndexHistograms(sctx, statsCache, item.TableItemID, loadFMSketch)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// CleanFakeItemsForShowHistInFlights cleans the invalid inserted items.
func CleanFakeItemsForShowHistInFlights(statsCache statstypes.StatsCache) int {
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	reallyNeeded := 0
	for _, item := range items {
		tbl, ok := statsCache.Get(item.TableID)
		if !ok {
			asyncload.AsyncLoadHistogramNeededItems.Delete(item.TableItemID)
			continue
		}
		loadNeeded := false
		if item.IsIndex {
			_, loadNeeded = tbl.IndexIsLoadNeeded(item.ID)
		} else {
			var analyzed bool
			_, loadNeeded, analyzed = tbl.ColumnIsLoadNeeded(item.ID, item.FullLoad)
			loadNeeded = loadNeeded && analyzed
		}
		if !loadNeeded {
			asyncload.AsyncLoadHistogramNeededItems.Delete(item.TableItemID)
			continue
		}
		reallyNeeded++
	}
	return reallyNeeded
}

func loadNeededColumnHistograms(sctx sessionctx.Context, statsCache statstypes.StatsCache, col model.TableItemID, loadFMSketch bool, fullLoad bool) (err error) {
	tbl, ok := statsCache.Get(col.TableID)
	if !ok {
		return nil
	}
	var colInfo *model.ColumnInfo
	_, loadNeeded, analyzed := tbl.ColumnIsLoadNeeded(col.ID, true)
	if !loadNeeded || !analyzed {
		asyncload.AsyncLoadHistogramNeededItems.Delete(col)
		return nil
	}
	colInfo = tbl.ColAndIdxExistenceMap.GetCol(col.ID)
	hg, _, statsVer, _, err := HistMetaFromStorage(sctx, &col, colInfo)
	if hg == nil || err != nil {
		asyncload.AsyncLoadHistogramNeededItems.Delete(col)
		return err
	}
	var (
		cms  *statistics.CMSketch
		topN *statistics.TopN
		fms  *statistics.FMSketch
	)
	if fullLoad {
		hg, err = HistogramFromStorage(sctx, col.TableID, col.ID, &colInfo.FieldType, hg.NDV, 0, hg.LastUpdateVersion, hg.NullCount, hg.TotColSize, hg.Correlation)
		if err != nil {
			return errors.Trace(err)
		}
		cms, topN, err = CMSketchAndTopNFromStorage(sctx, col.TableID, 0, col.ID)
		if err != nil {
			return errors.Trace(err)
		}
		if loadFMSketch {
			fms, err = FMSketchFromStorage(sctx, col.TableID, 0, col.ID)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	colHist := &statistics.Column{
		PhysicalID: col.TableID,
		Histogram:  *hg,
		Info:       colInfo,
		CMSketch:   cms,
		TopN:       topN,
		FMSketch:   fms,
		IsHandle:   tbl.IsPkIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
		StatsVer:   statsVer,
	}
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	tbl, ok = statsCache.Get(col.TableID)
	if !ok {
		return nil
	}
	tbl = tbl.Copy()
	if colHist.StatsAvailable() {
		if fullLoad {
			colHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
		} else {
			colHist.StatsLoadedStatus = statistics.NewStatsAllEvictedStatus()
		}
		tbl.LastAnalyzeVersion = max(tbl.LastAnalyzeVersion, colHist.LastUpdateVersion)
		if statsVer != statistics.Version0 {
			tbl.StatsVer = int(statsVer)
		}
	}
	tbl.Columns[col.ID] = colHist
	statsCache.UpdateStatsCache([]*statistics.Table{tbl}, nil)
	asyncload.AsyncLoadHistogramNeededItems.Delete(col)
	if col.IsSyncLoadFailed {
		logutil.BgLogger().Warn("Hist for column should already be loaded as sync but not found.",
			zap.Int64("table_id", colHist.PhysicalID),
			zap.Int64("column_id", colHist.Info.ID),
			zap.String("column_name", colHist.Info.Name.O))
	}
	return nil
}

func loadNeededIndexHistograms(sctx sessionctx.Context, statsCache statstypes.StatsCache, idx model.TableItemID, loadFMSketch bool) (err error) {
	tbl, ok := statsCache.Get(idx.TableID)
	if !ok {
		return nil
	}
	_, loadNeeded := tbl.IndexIsLoadNeeded(idx.ID)
	if !loadNeeded {
		asyncload.AsyncLoadHistogramNeededItems.Delete(idx)
		return nil
	}
	hgMeta, lastAnalyzePos, statsVer, flag, err := HistMetaFromStorage(sctx, &idx, nil)
	if hgMeta == nil || err != nil {
		asyncload.AsyncLoadHistogramNeededItems.Delete(idx)
		return err
	}
	idxInfo := tbl.ColAndIdxExistenceMap.GetIndex(idx.ID)
	hg, err := HistogramFromStorage(sctx, idx.TableID, idx.ID, types.NewFieldType(mysql.TypeBlob), hgMeta.NDV, 1, hgMeta.LastUpdateVersion, hgMeta.NullCount, hgMeta.TotColSize, hgMeta.Correlation)
	if err != nil {
		return errors.Trace(err)
	}
	cms, topN, err := CMSketchAndTopNFromStorage(sctx, idx.TableID, 1, idx.ID)
	if err != nil {
		return errors.Trace(err)
	}
	var fms *statistics.FMSketch
	if loadFMSketch {
		fms, err = FMSketchFromStorage(sctx, idx.TableID, 1, idx.ID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	idxHist := &statistics.Index{Histogram: *hg, CMSketch: cms, TopN: topN, FMSketch: fms,
		Info: idxInfo, StatsVer: statsVer,
		Flag: flag, PhysicalID: idx.TableID,
		StatsLoadedStatus: statistics.NewStatsFullLoadStatus()}
	lastAnalyzePos.Copy(&idxHist.LastAnalyzePos)

	tbl, ok = statsCache.Get(idx.TableID)
	if !ok {
		return nil
	}
	tbl = tbl.Copy()
	if idxHist.StatsVer != statistics.Version0 {
		tbl.StatsVer = int(idxHist.StatsVer)
	}
	tbl.Indices[idx.ID] = idxHist
	tbl.LastAnalyzeVersion = max(tbl.LastAnalyzeVersion, idxHist.LastUpdateVersion)
	statsCache.UpdateStatsCache([]*statistics.Table{tbl}, nil)
	if idx.IsSyncLoadFailed {
		logutil.BgLogger().Warn("Hist for index should already be loaded as sync but not found.",
			zap.Int64("table_id", idx.TableID),
			zap.Int64("index_id", idxHist.Info.ID),
			zap.String("index_name", idxHist.Info.Name.O))
	}
	asyncload.AsyncLoadHistogramNeededItems.Delete(idx)
	return nil
}

// StatsMetaByTableIDFromStorage gets the stats meta of a table from storage.
func StatsMetaByTableIDFromStorage(sctx sessionctx.Context, tableID int64, snapshot uint64) (version uint64, modifyCount, count int64, err error) {
	var rows []chunk.Row
	if snapshot == 0 {
		rows, _, err = util.ExecRows(sctx,
			"SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", tableID)
	} else {
		rows, _, err = util.ExecWithOpts(sctx,
			[]sqlexec.OptionFuncAlias{sqlexec.ExecOptionWithSnapshot(snapshot), sqlexec.ExecOptionUseCurSession},
			"SELECT version, modify_count, count from mysql.stats_meta where table_id = %? order by version", tableID)
	}
	if err != nil || len(rows) == 0 {
		return
	}
	version = rows[0].GetUint64(0)
	modifyCount = rows[0].GetInt64(1)
	count = rows[0].GetInt64(2)
	return
}
