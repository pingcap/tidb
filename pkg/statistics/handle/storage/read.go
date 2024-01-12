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
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
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
			// Invalid date values may be inserted into table under some relaxed sql mode. Those values may exist in statistics.
			// Hence, when reading statistics, we should skip invalid date check. See #39336.
			sc := stmtctx.NewStmtCtxWithTimeZone(time.UTC)
			sc.SetTypeFlags(sc.TypeFlags().WithIgnoreInvalidDateErr(true).WithIgnoreZeroInDate(true))
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
			lowerBound, err = d.ConvertTo(sc.TypeCtx(), tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(3, &fields[3].Column.FieldType)
			upperBound, err = d.ConvertTo(sc.TypeCtx(), tp)
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
			Columns:        make(map[int64]*statistics.Column, len(tableInfo.Columns)),
			Indices:        make(map[int64]*statistics.Index, len(tableInfo.Indices)),
		}
		table = &statistics.Table{
			HistColl: histColl,
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
	items := statistics.HistogramNeededItems.AllItems()
	for _, item := range items {
		if !item.IsIndex {
			err = loadNeededColumnHistograms(sctx, statsCache, item, loadFMSketch)
		} else {
			err = loadNeededIndexHistograms(sctx, statsCache, item, loadFMSketch)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func loadNeededColumnHistograms(sctx sessionctx.Context, statsCache statstypes.StatsCache, col model.TableItemID, loadFMSketch bool) (err error) {
	tbl, ok := statsCache.Get(col.TableID)
	if !ok {
		return nil
	}
	c, ok := tbl.Columns[col.ID]
	if !ok || !c.IsLoadNeeded() {
		statistics.HistogramNeededItems.Delete(col)
		return nil
	}
	hg, err := HistogramFromStorage(sctx, col.TableID, c.ID, &c.Info.FieldType, c.Histogram.NDV, 0, c.LastUpdateVersion, c.NullCount, c.TotColSize, c.Correlation)
	if err != nil {
		return errors.Trace(err)
	}
	cms, topN, err := CMSketchAndTopNFromStorage(sctx, col.TableID, 0, col.ID)
	if err != nil {
		return errors.Trace(err)
	}
	var fms *statistics.FMSketch
	if loadFMSketch {
		fms, err = FMSketchFromStorage(sctx, col.TableID, 0, col.ID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	rows, _, err := util.ExecRows(sctx, "select stats_ver from mysql.stats_histograms where is_index = 0 and table_id = %? and hist_id = %?", col.TableID, col.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", col.TableID), zap.Int64("hist_id", col.ID))
		return errors.Trace(fmt.Errorf("fail to get stats version for this histogram, table_id:%v, hist_id:%v", col.TableID, col.ID))
	}
	statsVer := rows[0].GetInt64(0)
	colHist := &statistics.Column{
		PhysicalID: col.TableID,
		Histogram:  *hg,
		Info:       c.Info,
		CMSketch:   cms,
		TopN:       topN,
		FMSketch:   fms,
		IsHandle:   c.IsHandle,
		StatsVer:   statsVer,
	}
	if colHist.StatsAvailable() {
		colHist.StatsLoadedStatus = statistics.NewStatsFullLoadStatus()
	}
	// Reload the latest stats cache, otherwise the `updateStatsCache` may fail with high probability, because functions
	// like `GetPartitionStats` called in `fmSketchFromStorage` would have modified the stats cache already.
	tbl, ok = statsCache.Get(col.TableID)
	if !ok {
		return nil
	}
	tbl = tbl.Copy()
	tbl.Columns[c.ID] = colHist
	statsCache.UpdateStatsCache([]*statistics.Table{tbl}, nil)
	statistics.HistogramNeededItems.Delete(col)
	return nil
}

func loadNeededIndexHistograms(sctx sessionctx.Context, statsCache statstypes.StatsCache, idx model.TableItemID, loadFMSketch bool) (err error) {
	tbl, ok := statsCache.Get(idx.TableID)
	if !ok {
		return nil
	}
	index, ok := tbl.Indices[idx.ID]
	if !ok {
		statistics.HistogramNeededItems.Delete(idx)
		return nil
	}
	hg, err := HistogramFromStorage(sctx, idx.TableID, index.ID, types.NewFieldType(mysql.TypeBlob), index.Histogram.NDV, 1, index.LastUpdateVersion, index.NullCount, index.TotColSize, index.Correlation)
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
	rows, _, err := util.ExecRows(sctx, "select stats_ver from mysql.stats_histograms where is_index = 1 and table_id = %? and hist_id = %?", idx.TableID, idx.ID)
	if err != nil {
		return errors.Trace(err)
	}
	if len(rows) == 0 {
		logutil.BgLogger().Error("fail to get stats version for this histogram", zap.Int64("table_id", idx.TableID), zap.Int64("hist_id", idx.ID))
		return errors.Trace(fmt.Errorf("fail to get stats version for this histogram, table_id:%v, hist_id:%v", idx.TableID, idx.ID))
	}
	idxHist := &statistics.Index{Histogram: *hg, CMSketch: cms, TopN: topN, FMSketch: fms,
		Info: index.Info, StatsVer: rows[0].GetInt64(0),
		Flag: index.Flag, PhysicalID: idx.TableID,
		StatsLoadedStatus: statistics.NewStatsFullLoadStatus()}
	index.LastAnalyzePos.Copy(&idxHist.LastAnalyzePos)

	tbl, ok = statsCache.Get(idx.TableID)
	if !ok {
		return nil
	}
	tbl = tbl.Copy()
	tbl.Indices[idx.ID] = idxHist
	statsCache.UpdateStatsCache([]*statistics.Table{tbl}, nil)
	statistics.HistogramNeededItems.Delete(idx)
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
