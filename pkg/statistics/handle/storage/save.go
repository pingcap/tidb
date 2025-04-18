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
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// batchInsertSize is the batch size used by internal SQL to insert values to some system table.
const batchInsertSize = 10

// maxInsertLength is the length limit for internal insert SQL.
const maxInsertLength = 1024 * 1024

func saveTopNToStorage(sctx sessionctx.Context, tableID int64, isIndex int, histID int64, topN *statistics.TopN) error {
	if topN == nil {
		return nil
	}
	for i := 0; i < len(topN.TopN); {
		end := i + batchInsertSize
		if end > len(topN.TopN) {
			end = len(topN.TopN)
		}
		sql := new(strings.Builder)
		sql.WriteString("insert into mysql.stats_top_n (table_id, is_index, hist_id, value, count) values ")
		for j := i; j < end; j++ {
			topn := topN.TopN[j]
			val := sqlescape.MustEscapeSQL("(%?, %?, %?, %?, %?)", tableID, isIndex, histID, topn.Encoded, topn.Count)
			if j > i {
				val = "," + val
			}
			if j > i && sql.Len()+len(val) > maxInsertLength {
				end = j
				break
			}
			sql.WriteString(val)
		}
		i = end
		if _, err := util.Exec(sctx, sql.String()); err != nil {
			return err
		}
	}
	return nil
}

func saveBucketsToStorage(sctx sessionctx.Context, tableID int64, isIndex int, hg *statistics.Histogram) (err error) {
	if hg == nil {
		return
	}
	sc := sctx.GetSessionVars().StmtCtx
	for i := 0; i < len(hg.Buckets); {
		end := i + batchInsertSize
		if end > len(hg.Buckets) {
			end = len(hg.Buckets)
		}
		sql := new(strings.Builder)
		sql.WriteString("insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, count, repeats, lower_bound, upper_bound, ndv) values ")
		for j := i; j < end; j++ {
			bucket := hg.Buckets[j]
			count := bucket.Count
			if j > 0 {
				count -= hg.Buckets[j-1].Count
			}
			var upperBound types.Datum
			upperBound, err = convertBoundToBlob(sc.TypeCtx(), *hg.GetUpper(j))
			if err != nil {
				return
			}
			var lowerBound types.Datum
			lowerBound, err = convertBoundToBlob(sc.TypeCtx(), *hg.GetLower(j))
			if err != nil {
				return
			}
			val := sqlescape.MustEscapeSQL("(%?, %?, %?, %?, %?, %?, %?, %?, %?)", tableID, isIndex, hg.ID, j, count, bucket.Repeat, lowerBound.GetBytes(), upperBound.GetBytes(), bucket.NDV)
			if j > i {
				val = "," + val
			}
			if j > i && sql.Len()+len(val) > maxInsertLength {
				end = j
				break
			}
			sql.WriteString(val)
		}
		i = end
		if _, err = util.Exec(sctx, sql.String()); err != nil {
			return
		}
	}
	return
}

// SaveAnalyzeResultToStorage saves the analyze result to the storage.
func SaveAnalyzeResultToStorage(sctx sessionctx.Context,
	results *statistics.AnalyzeResults, analyzeSnapshot bool) (statsVer uint64, err error) {
	needDumpFMS := results.TableID.IsPartitionTable()
	tableID := results.TableID.GetStatisticsID()
	ctx := util.StatsCtx
	txn, err := sctx.Txn(true)
	if err != nil {
		return 0, err
	}
	version := txn.StartTS()
	// 1. Save mysql.stats_meta.
	var rs sqlexec.RecordSet
	// Lock this row to prevent writing of concurrent analyze.
	rs, err = util.Exec(sctx, "select snapshot, count, modify_count from mysql.stats_meta where table_id = %? for update", tableID)
	if err != nil {
		return 0, err
	}
	var rows []chunk.Row
	rows, err = sqlexec.DrainRecordSet(ctx, rs, sctx.GetSessionVars().MaxChunkSize)
	if err != nil {
		return 0, err
	}
	err = rs.Close()
	if err != nil {
		return 0, err
	}
	var curCnt, curModifyCnt int64
	if len(rows) > 0 {
		snapshot := rows[0].GetUint64(0)
		// A newer version analyze result has been written, so skip this writing.
		// For multi-valued index or global index analyze, this check is not needed because we expect there's another normal v2 analyze
		// table task that may update the snapshot in stats_meta table (that task may finish before or after this task).
		if snapshot >= results.Snapshot && results.StatsVer == statistics.Version2 && !results.ForMVIndexOrGlobalIndex {
			return
		}
		curCnt = int64(rows[0].GetUint64(1))
		curModifyCnt = rows[0].GetInt64(2)
	}

	if len(rows) == 0 || results.StatsVer != statistics.Version2 {
		// 1-1.
		// a. There's no existing records we can update, we must insert a new row. Or
		// b. it's stats v1.
		// In these cases, we use REPLACE INTO to directly insert/update the version, count and snapshot.
		snapShot := results.Snapshot
		count := results.Count
		if results.ForMVIndexOrGlobalIndex {
			snapShot = 0
			count = 0
		}
		if _, err = util.Exec(sctx,
			"replace into mysql.stats_meta (version, table_id, count, snapshot, last_stats_histograms_version) values (%?, %?, %?, %?, %?)",
			version,
			tableID,
			count,
			snapShot,
			version,
		); err != nil {
			return 0, err
		}
		statsVer = version
	} else if results.ForMVIndexOrGlobalIndex {
		// 1-2. There's already an existing record for this table, and we are handling stats for mv index now.
		// In this case, we only update the version. See comments for AnalyzeResults.ForMVIndex for more details.
		if _, err = util.Exec(sctx,
			"update mysql.stats_meta set version=%?, last_stats_histograms_version=%? where table_id=%?",
			version,
			version,
			tableID,
		); err != nil {
			return 0, err
		}
	} else {
		// 1-3. There's already an existing records for this table, and we are handling a normal v2 analyze.
		modifyCnt := curModifyCnt - results.BaseModifyCnt
		if modifyCnt < 0 {
			modifyCnt = 0
		}
		statslogutil.StatsLogger().Info("incrementally update modifyCount",
			zap.Int64("tableID", tableID),
			zap.Int64("curModifyCnt", curModifyCnt),
			zap.Int64("results.BaseModifyCnt", results.BaseModifyCnt),
			zap.Int64("modifyCount", modifyCnt))
		var cnt int64
		if analyzeSnapshot {
			cnt = curCnt + results.Count - results.BaseCount
			if cnt < 0 {
				cnt = 0
			}
			statslogutil.StatsLogger().Info("incrementally update count",
				zap.Int64("tableID", tableID),
				zap.Int64("curCnt", curCnt),
				zap.Int64("results.Count", results.Count),
				zap.Int64("results.BaseCount", results.BaseCount),
				zap.Int64("count", cnt))
		} else {
			cnt = results.Count
			if cnt < 0 {
				cnt = 0
			}
			statslogutil.StatsLogger().Info("directly update count",
				zap.Int64("tableID", tableID),
				zap.Int64("results.Count", results.Count),
				zap.Int64("count", cnt))
		}
		if _, err = util.Exec(sctx,
			"update mysql.stats_meta set version=%?, modify_count=%?, count=%?, snapshot=%?, last_stats_histograms_version=%? where table_id=%?",
			version,
			modifyCnt,
			cnt,
			results.Snapshot,
			version,
			tableID,
		); err != nil {
			return 0, err
		}
		statsVer = version
	}
	// 2. Save histograms.
	for _, result := range results.Ars {
		for i, hg := range result.Hist {
			// It's normal virtual column, skip it.
			if hg == nil {
				continue
			}
			var cms *statistics.CMSketch
			if results.StatsVer != statistics.Version2 {
				cms = result.Cms[i]
			}
			cmSketch, err := statistics.EncodeCMSketchWithoutTopN(cms)
			if err != nil {
				return 0, err
			}
			fmSketch, err := statistics.EncodeFMSketch(result.Fms[i])
			if err != nil {
				return 0, err
			}
			// Delete outdated data
			if _, err = util.Exec(sctx, "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tableID, result.IsIndex, hg.ID); err != nil {
				return 0, err
			}
			if err = saveTopNToStorage(sctx, tableID, result.IsIndex, hg.ID, result.TopNs[i]); err != nil {
				return 0, err
			}
			if _, err := util.Exec(sctx, "delete from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tableID, result.IsIndex, hg.ID); err != nil {
				return 0, err
			}
			if fmSketch != nil && needDumpFMS {
				if _, err = util.Exec(sctx, "insert into mysql.stats_fm_sketch (table_id, is_index, hist_id, value) values (%?, %?, %?, %?)", tableID, result.IsIndex, hg.ID, fmSketch); err != nil {
					return 0, err
				}
			}
			if _, err = util.Exec(sctx, "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation) values (%?, %?, %?, %?, %?, %?, %?, GREATEST(%?, 0), %?, %?)",
				tableID, result.IsIndex, hg.ID, hg.NDV, version, hg.NullCount, cmSketch, hg.TotColSize, results.StatsVer, hg.Correlation); err != nil {
				return 0, err
			}
			if _, err = util.Exec(sctx, "delete from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?", tableID, result.IsIndex, hg.ID); err != nil {
				return 0, err
			}
			err = saveBucketsToStorage(sctx, tableID, result.IsIndex, hg)
			if err != nil {
				return 0, err
			}
			if result.IsIndex == 0 {
				if _, err = util.Exec(sctx, "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = values(last_analyzed_at)", tableID, hg.ID); err != nil {
					return 0, err
				}
			}
		}
	}
	// 3. Save extended statistics.
	extStats := results.ExtStats
	if extStats == nil || len(extStats.Stats) == 0 {
		return
	}
	var bytes []byte
	var statsStr string
	for name, item := range extStats.Stats {
		bytes, err = json.Marshal(item.ColIDs)
		if err != nil {
			return 0, err
		}
		strColIDs := string(bytes)
		switch item.Tp {
		case ast.StatsTypeCardinality, ast.StatsTypeCorrelation:
			statsStr = fmt.Sprintf("%f", item.ScalarVals)
		case ast.StatsTypeDependency:
			statsStr = item.StringVals
		}
		if _, err = util.Exec(sctx, "replace into mysql.stats_extended values (%?, %?, %?, %?, %?, %?, %?)", name, item.Tp, tableID, strColIDs, statsStr, version, statistics.ExtendedStatsAnalyzed); err != nil {
			return 0, err
		}
	}
	return
}

// SaveColOrIdxStatsToStorage saves the column or index statistics to the storage.
// If count is negative, both count and modify count would not be used and not be written to the table. Unless, corresponding
// fields in the stats_meta table will be updated.
// TODO: refactor to reduce the number of parameters
func SaveColOrIdxStatsToStorage(
	sctx sessionctx.Context,
	tableID int64,
	count, modifyCount int64,
	isIndex int,
	hg *statistics.Histogram,
	cms *statistics.CMSketch,
	topN *statistics.TopN,
	statsVersion int,
	updateAnalyzeTime bool,
) (statsVer uint64, err error) {
	version, err := util.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	// If the count is less than 0, then we do not want to update the modify count and count.
	if count >= 0 {
		_, err = util.Exec(sctx, "replace into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values (%?, %?, %?, %?, %?)", version, tableID, count, modifyCount, version)
	} else {
		_, err = util.Exec(sctx, "update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %?", version, version, tableID)
	}
	if err != nil {
		return 0, err
	}
	statsVer = version
	cmSketch, err := statistics.EncodeCMSketchWithoutTopN(cms)
	if err != nil {
		return 0, err
	}
	// Delete outdated data
	if _, err = util.Exec(sctx, "delete from mysql.stats_top_n where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return 0, err
	}
	if err = saveTopNToStorage(sctx, tableID, isIndex, hg.ID, topN); err != nil {
		return 0, err
	}
	if _, err := util.Exec(sctx, "delete from mysql.stats_fm_sketch where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return 0, err
	}
	if _, err = util.Exec(sctx, "replace into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation) values (%?, %?, %?, %?, %?, %?, %?, GREATEST(%?, 0), %?, %?)",
		tableID, isIndex, hg.ID, hg.NDV, version, hg.NullCount, cmSketch, hg.TotColSize, statsVersion, hg.Correlation); err != nil {
		return 0, err
	}
	if _, err = util.Exec(sctx, "delete from mysql.stats_buckets where table_id = %? and is_index = %? and hist_id = %?", tableID, isIndex, hg.ID); err != nil {
		return 0, err
	}
	err = saveBucketsToStorage(sctx, tableID, isIndex, hg)
	if err != nil {
		return 0, err
	}
	if updateAnalyzeTime && isIndex == 0 {
		if _, err = util.Exec(sctx, "insert into mysql.column_stats_usage (table_id, column_id, last_analyzed_at) values(%?, %?, current_timestamp()) on duplicate key update last_analyzed_at = current_timestamp()", tableID, hg.ID); err != nil {
			return 0, err
		}
	}
	return
}

// SaveMetaToStorage will batch save stats_meta to storage.
func SaveMetaToStorage(
	sctx sessionctx.Context,
	refreshLastHistVer bool,
	metaUpdates []statstypes.MetaUpdate,
) (uint64, error) {
	version, err := util.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	var sql string
	values := make([]string, 0, len(metaUpdates))
	if refreshLastHistVer {
		for _, metaUpdate := range metaUpdates {
			values = append(values, fmt.Sprintf("(%d, %d, %d, %d, %d)", version, metaUpdate.PhysicalID, metaUpdate.Count, metaUpdate.ModifyCount, version))
		}
		sql = fmt.Sprintf("insert into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values %s "+
			"on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count), last_stats_histograms_version = values(last_stats_histograms_version)", strings.Join(values, ","))
	} else {
		for _, metaUpdate := range metaUpdates {
			values = append(values, fmt.Sprintf("(%d, %d, %d, %d)", version, metaUpdate.PhysicalID, metaUpdate.Count, metaUpdate.ModifyCount))
		}
		sql = fmt.Sprintf("insert into mysql.stats_meta (version, table_id, count, modify_count) values %s "+
			"on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count)", strings.Join(values, ","))
	}
	_, err = util.Exec(sctx, sql)
	return version, errors.Trace(err)
}

// InsertColStats2KV insert a record to stats_histograms with distinct_count 1
// and insert a bucket to stats_buckets with default value. This operation also
// updates version.
func InsertColStats2KV(
	ctx context.Context,
	sctx sessionctx.Context,
	physicalID int64,
	colInfos []*model.ColumnInfo,
) (uint64, error) {
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	// First of all, we update the version.
	_, err = util.ExecWithCtx(
		ctx, sctx,
		"update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %?",
		startTS, startTS, physicalID,
	)
	if err != nil {
		return 0, errors.Trace(err)
	}
	// If we didn't update anything by last SQL, it means the stats of this table does not exist.
	if sctx.GetSessionVars().StmtCtx.AffectedRows() == 0 {
		return startTS, nil
	}

	// By this step we can get the count of this table, then we can sure the count and repeats of bucket.
	var rs sqlexec.RecordSet
	rs, err = util.ExecWithCtx(
		ctx, sctx,
		"select count from mysql.stats_meta where table_id = %?",
		physicalID,
	)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer terror.Call(rs.Close)
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	if err != nil {
		return 0, errors.Trace(err)
	}
	count := req.GetRow(0).GetInt64(0)
	for _, colInfo := range colInfos {
		value, err := table.GetColOriginDefaultValue(sctx.GetExprCtx(), colInfo)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if value.IsNull() {
			// If the adding column has default value null, all the existing rows have null value on the newly added column.
			if _, err = util.ExecWithCtx(
				ctx, sctx,
				`insert into mysql.stats_histograms
					(version, table_id, is_index, hist_id, distinct_count, null_count)
				values (%?, %?, 0, %?, 0, %?)`,
				startTS, physicalID, colInfo.ID, count,
			); err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}

		// If this stats doest not exist, we insert histogram meta first, the distinct_count will always be one.
		if _, err = util.ExecWithCtx(
			ctx, sctx,
			`insert into mysql.stats_histograms
				(version, table_id, is_index, hist_id, distinct_count, tot_col_size)
			values (%?, %?, 0, %?, 1, GREATEST(%?, 0))`,
			startTS, physicalID, colInfo.ID, int64(len(value.GetBytes()))*count,
		); err != nil {
			return 0, errors.Trace(err)
		}
		value, err = value.ConvertTo(sctx.GetSessionVars().StmtCtx.TypeCtx(), types.NewFieldType(mysql.TypeBlob))
		if err != nil {
			return 0, errors.Trace(err)
		}
		// There must be only one bucket for this new column and the value is the default value.
		if _, err = util.ExecWithCtx(
			ctx, sctx,
			`insert into mysql.stats_buckets
				(table_id, is_index, hist_id, bucket_id, repeats, count, lower_bound, upper_bound)
			values (%?, 0, %?, 0, %?, %?, %?, %?)`,
			physicalID, colInfo.ID, count, count, value.GetBytes(), value.GetBytes(),
		); err != nil {
			return 0, errors.Trace(err)
		}
	}
	return startTS, nil
}

// InsertTableStats2KV inserts a record standing for a new table to stats_meta
// and inserts some records standing for the new columns and indices which belong
// to this table.
func InsertTableStats2KV(
	ctx context.Context,
	sctx sessionctx.Context,
	info *model.TableInfo,
	physicalID int64,
) (uint64, error) {
	startTS, err := util.GetStartTS(sctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if _, err = util.ExecWithCtx(
		ctx, sctx,
		"insert ignore into mysql.stats_meta (version, table_id, last_stats_histograms_version) values(%?, %?, %?)",
		startTS, physicalID, startTS,
	); err != nil {
		return 0, errors.Trace(err)
	}
	for _, col := range info.Columns {
		if _, err = util.ExecWithCtx(
			ctx, sctx,
			`insert ignore into mysql.stats_histograms
				(table_id, is_index, hist_id, distinct_count, version)
			values (%?, 0, %?, 0, %?)`,
			physicalID, col.ID, startTS,
		); err != nil {
			return 0, errors.Trace(err)
		}
	}
	for _, idx := range info.Indices {
		if _, err = util.ExecWithCtx(
			ctx, sctx,
			`insert ignore into mysql.stats_histograms
				(table_id, is_index, hist_id, distinct_count, version)
			values(%?, 1, %?, 0, %?)`,
			physicalID, idx.ID, startTS,
		); err != nil {
			return 0, errors.Trace(err)
		}
	}
	return startTS, nil
}

// convertBoundToBlob converts the bound to blob. The `blob` will be used to store in the `mysql.stats_buckets` table.
// The `convertBoundFromBlob(convertBoundToBlob(a))` should be equal to `a`.
// TODO: add a test to make sure that this assumption is correct.
func convertBoundToBlob(ctx types.Context, d types.Datum) (types.Datum, error) {
	return d.ConvertTo(ctx, types.NewFieldType(mysql.TypeBlob))
}
