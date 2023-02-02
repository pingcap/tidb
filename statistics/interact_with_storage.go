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

package statistics

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

// StatsReader is used for simplifying code that needs to read statistics from system tables(mysql.stats_xxx) in different sqls
// but requires the same transactions.
//
// Note that:
// 1. Remember to call (*StatsReader).Close after reading all statistics.
// 2. StatsReader is not thread-safe. Different goroutines cannot call (*StatsReader).Read concurrently.
type StatsReader struct {
	ctx      sqlexec.RestrictedSQLExecutor
	snapshot uint64
}

// GetStatsReader returns a StatsReader.
func GetStatsReader(snapshot uint64, exec sqlexec.RestrictedSQLExecutor) (reader *StatsReader, err error) {
	failpoint.Inject("mockGetStatsReaderFail", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("gofail genStatsReader error"))
		}
	})
	if snapshot > 0 {
		return &StatsReader{ctx: exec, snapshot: snapshot}, nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("getStatsReader panic %v", r)
		}
	}()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	failpoint.Inject("mockGetStatsReaderPanic", nil)
	_, err = exec.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "begin")
	if err != nil {
		return nil, err
	}
	return &StatsReader{ctx: exec}, nil
}

// Read is a thin wrapper reading statistics from storage by sql command.
func (sr *StatsReader) Read(sql string, args ...interface{}) (rows []chunk.Row, fields []*ast.ResultField, err error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	if sr.snapshot > 0 {
		return sr.ctx.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseSessionPool, sqlexec.ExecOptionWithSnapshot(sr.snapshot)}, sql, args...)
	}
	return sr.ctx.ExecRestrictedSQL(ctx, []sqlexec.OptionFuncAlias{sqlexec.ExecOptionUseCurSession}, sql, args...)
}

// IsHistory indicates whether to read history statistics.
func (sr *StatsReader) IsHistory() bool {
	return sr.snapshot > 0
}

// Close closes the StatsReader.
func (sr *StatsReader) Close() error {
	if sr.IsHistory() || sr.ctx == nil {
		return nil
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	_, err := sr.ctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, "commit")
	return err
}

// HistogramFromStorage reads histogram from storage.
func HistogramFromStorage(reader *StatsReader, tableID int64, colID int64, tp *types.FieldType, distinct int64, isIndex int, ver uint64, nullCount int64, totColSize int64, corr float64) (_ *Histogram, err error) {
	rows, fields, err := reader.Read("select count, repeats, lower_bound, upper_bound, ndv from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %? order by bucket_id", tableID, isIndex, colID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	bucketSize := len(rows)
	hg := NewHistogram(colID, distinct, nullCount, ver, tp, bucketSize, totColSize)
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
			sc := &stmtctx.StatementContext{TimeZone: time.UTC, AllowInvalidDate: true, IgnoreZeroInDate: true}
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
			lowerBound, err = d.ConvertTo(sc, tp)
			if err != nil {
				return nil, errors.Trace(err)
			}
			d = rows[i].GetDatum(3, &fields[3].Column.FieldType)
			upperBound, err = d.ConvertTo(sc, tp)
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
func CMSketchAndTopNFromStorage(reader *StatsReader, tblID int64, isIndex, histID int64) (_ *CMSketch, _ *TopN, err error) {
	topNRows, _, err := reader.Read("select HIGH_PRIORITY value, count from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, nil, err
	}
	rows, _, err := reader.Read("select cm_sketch from mysql.stats_histograms where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return DecodeCMSketchAndTopN(nil, topNRows)
	}
	return DecodeCMSketchAndTopN(rows[0].GetBytes(0), topNRows)
}

// FMSketchFromStorage reads FMSketch from storage
func FMSketchFromStorage(reader *StatsReader, tblID int64, isIndex, histID int64) (_ *FMSketch, err error) {
	rows, _, err := reader.Read("select value from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tblID, isIndex, histID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return DecodeFMSketch(rows[0].GetBytes(0))
}

// ColumnCountFromStorage reads column count from storage
func ColumnCountFromStorage(reader *StatsReader, tableID, colID, statsVer int64) (int64, error) {
	count := int64(0)
	rows, _, err := reader.Read("select sum(count) from mysql.stats_buckets where table_id = %? and is_index = 0 and hist_id = %?", tableID, colID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// If there doesn't exist any buckets, the SQL will return NULL. So we only use the result if it's not NULL.
	if !rows[0].IsNull(0) {
		count, err = rows[0].GetMyDecimal(0).ToInt()
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if statsVer >= Version2 {
		// Before stats ver 2, histogram represents all data in this column.
		// In stats ver 2, histogram + TopN represent all data in this column.
		// So we need to add TopN total count here.
		rows, _, err = reader.Read("select sum(count) from mysql.stats_top_n where table_id = %? and is_index = 0 and hist_id = %?", tableID, colID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if !rows[0].IsNull(0) {
			topNCount, err := rows[0].GetMyDecimal(0).ToInt()
			if err != nil {
				return 0, errors.Trace(err)
			}
			count += topNCount
		}
	}
	return count, err
}

// ExtendedStatsFromStorage reads extended stats from storage.
func ExtendedStatsFromStorage(reader *StatsReader, table *Table, physicalID int64, loadAll bool) (*Table, error) {
	failpoint.Inject("injectExtStatsLoadErr", func() {
		failpoint.Return(nil, errors.New("gofail extendedStatsFromStorage error"))
	})
	lastVersion := uint64(0)
	if table.ExtendedStats != nil && !loadAll {
		lastVersion = table.ExtendedStats.LastUpdateVersion
	} else {
		table.ExtendedStats = NewExtendedStatsColl()
	}
	rows, _, err := reader.Read("select name, status, type, column_ids, stats, version from mysql.stats_extended where table_id = %? and status in (%?, %?, %?) and version > %?", physicalID, ExtendedStatsInited, ExtendedStatsAnalyzed, ExtendedStatsDeleted, lastVersion)
	if err != nil || len(rows) == 0 {
		return table, nil
	}
	for _, row := range rows {
		lastVersion = mathutil.Max(lastVersion, row.GetUint64(5))
		name := row.GetString(0)
		status := uint8(row.GetInt64(1))
		if status == ExtendedStatsDeleted || status == ExtendedStatsInited {
			delete(table.ExtendedStats.Stats, name)
		} else {
			item := &ExtendedStatsItem{
				Tp: uint8(row.GetInt64(2)),
			}
			colIDs := row.GetString(3)
			err := json.Unmarshal([]byte(colIDs), &item.ColIDs)
			if err != nil {
				logutil.BgLogger().Error("[stats] decode column IDs failed", zap.String("column_ids", colIDs), zap.Error(err))
				return nil, err
			}
			statsStr := row.GetString(4)
			if item.Tp == ast.StatsTypeCardinality || item.Tp == ast.StatsTypeCorrelation {
				if statsStr != "" {
					item.ScalarVals, err = strconv.ParseFloat(statsStr, 64)
					if err != nil {
						logutil.BgLogger().Error("[stats] parse scalar stats failed", zap.String("stats", statsStr), zap.Error(err))
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

func indexStatsFromStorage(reader *StatsReader, row chunk.Row, table *Table, tableInfo *model.TableInfo) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	statsVer := row.GetInt64(7)
	idx := table.Indices[histID]
	errorRate := ErrorRate{}
	flag := row.GetInt64(8)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	if (!IsAnalyzed(flag) || reader.IsHistory()) && idx != nil {
		errorRate = idx.ErrorRate
	}
	for _, idxInfo := range tableInfo.Indices {
		if histID != idxInfo.ID {
			continue
		}
		if idx == nil || idx.LastUpdateVersion < histVer {
			hg, err := HistogramFromStorage(reader, table.PhysicalID, histID, types.NewFieldType(mysql.TypeBlob), distinct, 1, histVer, nullCount, 0, 0)
			if err != nil {
				return errors.Trace(err)
			}
			cms, topN, err := CMSketchAndTopNFromStorage(reader, table.PhysicalID, 1, idxInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			fmSketch, err := FMSketchFromStorage(reader, table.PhysicalID, 1, histID)
			if err != nil {
				return errors.Trace(err)
			}
			idx = &Index{
				Histogram:  *hg,
				CMSketch:   cms,
				TopN:       topN,
				FMSketch:   fmSketch,
				Info:       idxInfo,
				ErrorRate:  errorRate,
				StatsVer:   statsVer,
				Flag:       flag,
				PhysicalID: table.PhysicalID,
			}
			if statsVer != Version0 {
				idx.StatsLoadedStatus = NewStatsFullLoadStatus()
			}
			lastAnalyzePos.Copy(&idx.LastAnalyzePos)
		}
		break
	}
	if idx != nil {
		table.Indices[histID] = idx
	} else {
		logutil.BgLogger().Debug("we cannot find index id in table info. It may be deleted.", zap.Int64("indexID", histID), zap.String("table", tableInfo.Name.O))
	}
	return nil
}

func columnStatsFromStorage(reader *StatsReader, row chunk.Row, table *Table, tableInfo *model.TableInfo, loadAll bool, lease time.Duration) error {
	histID := row.GetInt64(2)
	distinct := row.GetInt64(3)
	histVer := row.GetUint64(4)
	nullCount := row.GetInt64(5)
	totColSize := row.GetInt64(6)
	statsVer := row.GetInt64(7)
	correlation := row.GetFloat64(9)
	lastAnalyzePos := row.GetDatum(10, types.NewFieldType(mysql.TypeBlob))
	col := table.Columns[histID]
	errorRate := ErrorRate{}
	flag := row.GetInt64(8)
	if (!IsAnalyzed(flag) || reader.IsHistory()) && col != nil {
		errorRate = col.ErrorRate
	}
	for _, colInfo := range tableInfo.Columns {
		if histID != colInfo.ID {
			continue
		}
		isHandle := tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag())
		// We will not load buckets if:
		// 1. lease > 0, and:
		// 2. this column is not handle, and:
		// 3. the column doesn't has any statistics before, and:
		// 4. loadAll is false.
		notNeedLoad := lease > 0 &&
			!isHandle &&
			(col == nil || !col.IsStatsInitialized() && col.LastUpdateVersion < histVer) &&
			!loadAll
		if notNeedLoad {
			count, err := ColumnCountFromStorage(reader, table.PhysicalID, histID, statsVer)
			if err != nil {
				return errors.Trace(err)
			}
			col = &Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *NewHistogram(histID, distinct, nullCount, histVer, &colInfo.FieldType, 0, totColSize),
				Info:       colInfo,
				Count:      count + nullCount,
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				Flag:       flag,
				StatsVer:   statsVer,
			}
			// When adding/modifying a column, we create its stats(all values are default values) without setting stats_ver.
			// So we need add col.Count > 0 here.
			if statsVer != Version0 || col.Count > 0 {
				col.StatsLoadedStatus = NewStatsAllEvictedStatus()
			}
			lastAnalyzePos.Copy(&col.LastAnalyzePos)
			col.Histogram.Correlation = correlation
			break
		}
		if col == nil || col.LastUpdateVersion < histVer || loadAll {
			hg, err := HistogramFromStorage(reader, table.PhysicalID, histID, &colInfo.FieldType, distinct, 0, histVer, nullCount, totColSize, correlation)
			if err != nil {
				return errors.Trace(err)
			}
			cms, topN, err := CMSketchAndTopNFromStorage(reader, table.PhysicalID, 0, colInfo.ID)
			if err != nil {
				return errors.Trace(err)
			}
			var fmSketch *FMSketch
			if loadAll {
				// FMSketch is only used when merging partition stats into global stats. When merging partition stats into global stats,
				// we load all the statistics, i.e., loadAll is true.
				fmSketch, err = FMSketchFromStorage(reader, table.PhysicalID, 0, histID)
				if err != nil {
					return errors.Trace(err)
				}
			}
			col = &Column{
				PhysicalID: table.PhysicalID,
				Histogram:  *hg,
				Info:       colInfo,
				CMSketch:   cms,
				TopN:       topN,
				FMSketch:   fmSketch,
				ErrorRate:  errorRate,
				IsHandle:   tableInfo.PKIsHandle && mysql.HasPriKeyFlag(colInfo.GetFlag()),
				Flag:       flag,
				StatsVer:   statsVer,
			}
			// Column.Count is calculated by Column.TotalRowCount(). Hence we don't set Column.Count when initializing col.
			col.Count = int64(col.TotalRowCount())
			// When adding/modifying a column, we create its stats(all values are default values) without setting stats_ver.
			// So we need add colHist.Count > 0 here.
			if statsVer != Version0 || col.Count > 0 {
				col.StatsLoadedStatus = NewStatsFullLoadStatus()
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
func TableStatsFromStorage(reader *StatsReader, tableInfo *model.TableInfo, physicalID int64, loadAll bool, lease time.Duration, table *Table) (_ *Table, err error) {
	// If table stats is pseudo, we also need to copy it, since we will use the column stats when
	// the average error rate of it is small.
	if table == nil || reader.IsHistory() {
		histColl := HistColl{
			PhysicalID:     physicalID,
			HavePhysicalID: true,
			Columns:        make(map[int64]*Column, len(tableInfo.Columns)),
			Indices:        make(map[int64]*Index, len(tableInfo.Indices)),
		}
		table = &Table{
			HistColl: histColl,
		}
	} else {
		// We copy it before writing to avoid race.
		table = table.Copy()
	}
	table.Pseudo = false

	rows, _, err := reader.Read("select modify_count, count from mysql.stats_meta where table_id = %?", physicalID)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	table.ModifyCount = rows[0].GetInt64(0)
	table.Count = rows[0].GetInt64(1)

	rows, _, err = reader.Read("select table_id, is_index, hist_id, distinct_count, version, null_count, tot_col_size, stats_ver, flag, correlation, last_analyze_pos from mysql.stats_histograms where table_id = %?", physicalID)
	// Check deleted table.
	if err != nil || len(rows) == 0 {
		return nil, nil
	}
	for _, row := range rows {
		if row.GetInt64(1) > 0 {
			err = indexStatsFromStorage(reader, row, table, tableInfo)
		} else {
			err = columnStatsFromStorage(reader, row, table, tableInfo, loadAll, lease)
		}
		if err != nil {
			return nil, err
		}
	}
	return ExtendedStatsFromStorage(reader, table, physicalID, loadAll)
}
