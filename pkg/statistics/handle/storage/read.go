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
	"context"
	"encoding/json"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"go.uber.org/zap"
)

// StatsMetaCountAndModifyCount reads count and modify_count for the given table from mysql.stats_meta.
func StatsMetaCountAndModifyCount(
	ctx context.Context,
	sctx sessionctx.Context,
	tableID int64,
) (count, modifyCount int64, isNull bool, err error) {
	return statsMetaCountAndModifyCount(ctx, sctx, tableID, false)
}

// StatsMetaCountAndModifyCountForUpdate reads count and modify_count for the given table from mysql.stats_meta with lock.
func StatsMetaCountAndModifyCountForUpdate(
	ctx context.Context,
	sctx sessionctx.Context,
	tableID int64,
) (count, modifyCount int64, isNull bool, err error) {
	return statsMetaCountAndModifyCount(ctx, sctx, tableID, true)
}

func statsMetaCountAndModifyCount(
	ctx context.Context,
	sctx sessionctx.Context,
	tableID int64,
	forUpdate bool,
) (count, modifyCount int64, isNull bool, err error) {
	sql := "select count, modify_count from mysql.stats_meta where table_id = %?"
	if forUpdate {
		sql += " for update"
	}
	rows, _, err := util.ExecRowsWithCtx(ctx, sctx, sql, tableID)
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

// HistMetaFromStorageWithHighPriority reads the meta info of the histogram from the storage.
func HistMetaFromStorageWithHighPriority(sctx sessionctx.Context, item *model.TableItemID, possibleColInfo *model.ColumnInfo) (*statistics.Histogram, int64, error) {
	isIndex := 0
	var tp *types.FieldType
	if item.IsIndex {
		isIndex = 1
		tp = types.NewFieldType(mysql.TypeBlob)
	} else {
		tp = &possibleColInfo.FieldType
	}
	rows, _, err := util.ExecRows(sctx,
		"select high_priority distinct_count, version, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms where table_id = %? and hist_id = %? and is_index = %?",
		item.TableID,
		item.ID,
		isIndex,
	)
	if err != nil {
		return nil, 0, err
	}
	if len(rows) == 0 {
		return nil, 0, nil
	}
	hist := statistics.NewHistogram(item.ID, rows[0].GetInt64(0), rows[0].GetInt64(2), rows[0].GetUint64(1), tp, chunk.InitialCapacity, rows[0].GetInt64(3))
	hist.Correlation = rows[0].GetFloat64(5)
	return hist, rows[0].GetInt64(4), nil
}

// HistogramFromStorageWithPriority wraps the HistogramFromStorage with the given kv.Priority.
// Sync load and async load will use high priority to get data.
func HistogramFromStorageWithPriority(
	sctx sessionctx.Context,
	tableID int64,
	colID int64,
	tp *types.FieldType,
	distinct int64,
	isIndex int,
	ver uint64,
	nullCount int64,
	totColSize int64,
	corr float64,
	priority int,
) (*statistics.Histogram, error) {
	selectPrefix := "select "
	switch priority {
	case kv.PriorityHigh:
		selectPrefix += "high_priority "
	case kv.PriorityLow:
		selectPrefix += "low_priority "
	}
	rows, fields, err := util.ExecRows(sctx, selectPrefix+"count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %? order by bucket_id", tableID, isIndex, colID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := statistics.NewHistogram(colID, distinct, nullCount, ver, tp, bucketSize, totColSize)
	hg.Correlation = corr
	totalCount := int64(0)
	for i := range bucketSize {
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
			lowerBound, err = convertBoundFromBlob(statistics.UTCWithAllowInvalidDateCtx, d, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(3, &fields[3].Column.FieldType)
			upperBound, err = convertBoundFromBlob(statistics.UTCWithAllowInvalidDateCtx, d, tp)
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

// CMSketchAndTopNFromStorageWithHighPriority reads CMSketch and TopN from storage.
func CMSketchAndTopNFromStorageWithHighPriority(sctx sessionctx.Context, tblID int64, isIndex, histID, statsVer int64) (_ *statistics.CMSketch, _ *statistics.TopN, err error) {
	topNRows, _, err := util.ExecRows(sctx, "select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, nil, err
	}
	// If we are on version higher than 1. Don't read Count-Min Sketch.
	if statsVer > statistics.Version1 {
		return statistics.DecodeCMSketchAndTopN(nil, topNRows)
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

