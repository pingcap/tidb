// Copyright 2017 PingCAP, Inc.
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
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/sqlexec"
)

// HandleDDLEvent begins to process a ddl task.
func (h *Handle) HandleDDLEvent(t *util.Event) error {
	switch t.Tp {
	case model.ActionCreateTable, model.ActionTruncateTable:
		ids := h.getInitStateTableIDs(t.TableInfo)
		for _, id := range ids {
			if err := h.insertTableStats2KV(t.TableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionAddColumn, model.ActionAddColumns, model.ActionModifyColumn:
		ids := h.getInitStateTableIDs(t.TableInfo)
		for _, id := range ids {
			if err := h.insertColStats2KV(id, t.ColumnInfos); err != nil {
				return err
			}
		}
	case model.ActionAddTablePartition, model.ActionTruncateTablePartition:
		for _, def := range t.PartInfo.Definitions {
			if err := h.insertTableStats2KV(t.TableInfo, def.ID); err != nil {
				return err
			}
		}
	case model.ActionDropTablePartition:
		pruneMode := h.CurrentPruneMode()
		if pruneMode == variable.Dynamic && t.PartInfo != nil {
			if err := h.updateGlobalStats(t.TableInfo); err != nil {
				return err
			}
		}
	}
	return nil
}

// analyzeOptionDefault saves the default values of NumBuckets and NumTopN.
// These values will be used in dynamic mode when we drop table partition and then need to merge global-stats.
// These values originally came from the analyzeOptionDefault structure in the planner/core/planbuilder.go file.
var analyzeOptionDefault = map[ast.AnalyzeOptionType]uint64{
	ast.AnalyzeOptNumBuckets: 256,
	ast.AnalyzeOptNumTopN:    20,
}

// updateGlobalStats will trigger the merge of global-stats when we drop table partition
func (h *Handle) updateGlobalStats(tblInfo *model.TableInfo) error {
	// We need to merge the partition-level stats to global-stats when we drop table partition in dynamic mode.
	tableID := tblInfo.ID
	se, err := h.pool.Get()
	if err != nil {
		return err
	}
	sctx := se.(sessionctx.Context)
	is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
	h.pool.Put(se)
	globalStats, err := h.TableStatsFromStorage(tblInfo, tableID, true, 0)
	if err != nil {
		return err
	}
	// If we do not currently have global-stats, no new global-stats will be generated.
	if globalStats == nil {
		return nil
	}
	opts := make(map[ast.AnalyzeOptionType]uint64, len(analyzeOptionDefault))
	for key, val := range analyzeOptionDefault {
		opts[key] = val
	}
	// Use current global-stats related information to construct the opts for `MergePartitionStats2GlobalStats` function.
	globalColStatsTopNNum, globalColStatsBucketNum := 0, 0
	for colID := range globalStats.Columns {
		globalColStatsTopN := globalStats.Columns[colID].TopN
		if globalColStatsTopN != nil && len(globalColStatsTopN.TopN) > globalColStatsTopNNum {
			globalColStatsTopNNum = len(globalColStatsTopN.TopN)
		}
		globalColStats := globalStats.Columns[colID]
		if globalColStats != nil && len(globalColStats.Buckets) > globalColStatsBucketNum {
			globalColStatsBucketNum = len(globalColStats.Buckets)
		}
	}
	if globalColStatsTopNNum != 0 {
		opts[ast.AnalyzeOptNumTopN] = uint64(globalColStatsTopNNum)
	}
	if globalColStatsBucketNum != 0 {
		opts[ast.AnalyzeOptNumBuckets] = uint64(globalColStatsBucketNum)
	}
	// Generate the new column global-stats
	newColGlobalStats, err := h.mergePartitionStats2GlobalStats(h.mu.ctx, opts, is, tblInfo, 0, nil)
	if err != nil {
		return err
	}
	for i := 0; i < newColGlobalStats.Num; i++ {
		hg, cms, topN, fms := newColGlobalStats.Hg[i], newColGlobalStats.Cms[i], newColGlobalStats.TopN[i], newColGlobalStats.Fms[i]
		// fms for global stats doesn't need to dump to kv.
		err = h.SaveStatsToStorage(tableID, newColGlobalStats.Count, 0, hg, cms, topN, fms, 2, 1, false, false)
		if err != nil {
			return err
		}
	}

	// Generate the new index global-stats
	globalIdxStatsTopNNum, globalIdxStatsBucketNum := 0, 0
	for idx := range tblInfo.Indices {
		globalIdxStatsTopN := globalStats.Indices[int64(idx)].TopN
		if globalIdxStatsTopN != nil && len(globalIdxStatsTopN.TopN) > globalIdxStatsTopNNum {
			globalIdxStatsTopNNum = len(globalIdxStatsTopN.TopN)
		}
		globalIdxStats := globalStats.Indices[int64(idx)]
		if globalIdxStats != nil && len(globalIdxStats.Buckets) > globalIdxStatsBucketNum {
			globalIdxStatsBucketNum = len(globalIdxStats.Buckets)
		}
		if globalIdxStatsTopNNum != 0 {
			opts[ast.AnalyzeOptNumTopN] = uint64(globalIdxStatsTopNNum)
		}
		if globalIdxStatsBucketNum != 0 {
			opts[ast.AnalyzeOptNumBuckets] = uint64(globalIdxStatsBucketNum)
		}
		newIndexGlobalStats, err := h.mergePartitionStats2GlobalStats(h.mu.ctx, opts, is, tblInfo, 1, []int64{int64(idx)})
		if err != nil {
			return err
		}
		for i := 0; i < newIndexGlobalStats.Num; i++ {
			hg, cms, topN, fms := newIndexGlobalStats.Hg[i], newIndexGlobalStats.Cms[i], newIndexGlobalStats.TopN[i], newIndexGlobalStats.Fms[i]
			// fms for global stats doesn't need to dump to kv.
			err = h.SaveStatsToStorage(tableID, newIndexGlobalStats.Count, 1, hg, cms, topN, fms, 2, 1, false, false)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *Handle) getInitStateTableIDs(tblInfo *model.TableInfo) (ids []int64) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}
	}
	ids = make([]int64, 0, len(pi.Definitions)+1)
	for _, def := range pi.Definitions {
		ids = append(ids, def.ID)
	}
	if h.CurrentPruneMode() == variable.Dynamic {
		ids = append(ids, tblInfo.ID)
	}
	return ids
}

// DDLEventCh returns ddl events channel in handle.
func (h *Handle) DDLEventCh() chan *util.Event {
	return h.ddlEventCh
}

// insertTableStats2KV inserts a record standing for a new table to stats_meta and inserts some records standing for the
// new columns and indices which belong to this table.
func (h *Handle) insertTableStats2KV(info *model.TableInfo, physicalID int64) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			err = h.recordHistoricalStatsMeta(physicalID, statsVer)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()
	ctx := context.Background()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	if _, err := exec.ExecuteInternal(ctx, "insert into mysql.stats_meta (version, table_id) values(%?, %?)", startTS, physicalID); err != nil {
		return err
	}
	statsVer = startTS
	for _, col := range info.Columns {
		if _, err := exec.ExecuteInternal(ctx, "insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%?, 0, %?, 0, %?)", physicalID, col.ID, startTS); err != nil {
			return err
		}
	}
	for _, idx := range info.Indices {
		if _, err := exec.ExecuteInternal(ctx, "insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%?, 1, %?, 0, %?)", physicalID, idx.ID, startTS); err != nil {
			return err
		}
	}
	return nil
}

// insertColStats2KV insert a record to stats_histograms with distinct_count 1 and insert a bucket to stats_buckets with default value.
// This operation also updates version.
func (h *Handle) insertColStats2KV(physicalID int64, colInfos []*model.ColumnInfo) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			err = h.recordHistoricalStatsMeta(physicalID, statsVer)
		}
	}()
	h.mu.Lock()
	defer h.mu.Unlock()

	ctx := context.TODO()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.ExecuteInternal(ctx, "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(ctx, exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	// First of all, we update the version.
	_, err = exec.ExecuteInternal(ctx, "update mysql.stats_meta set version = %? where table_id = %?", startTS, physicalID)
	if err != nil {
		return
	}
	statsVer = startTS
	// If we didn't update anything by last SQL, it means the stats of this table does not exist.
	if h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0 {
		// By this step we can get the count of this table, then we can sure the count and repeats of bucket.
		var rs sqlexec.RecordSet
		rs, err = exec.ExecuteInternal(ctx, "select count from mysql.stats_meta where table_id = %?", physicalID)
		if err != nil {
			return
		}
		defer terror.Call(rs.Close)
		req := rs.NewChunk(nil)
		err = rs.Next(ctx, req)
		if err != nil {
			return
		}
		count := req.GetRow(0).GetInt64(0)
		for _, colInfo := range colInfos {
			value := types.NewDatum(colInfo.GetOriginDefaultValue())
			value, err = value.ConvertTo(h.mu.ctx.GetSessionVars().StmtCtx, &colInfo.FieldType)
			if err != nil {
				return
			}
			if value.IsNull() {
				// If the adding column has default value null, all the existing rows have null value on the newly added column.
				if _, err := exec.ExecuteInternal(ctx, "insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, null_count) values (%?, %?, 0, %?, 0, %?)", startTS, physicalID, colInfo.ID, count); err != nil {
					return err
				}
			} else {
				// If this stats exists, we insert histogram meta first, the distinct_count will always be one.
				if _, err := exec.ExecuteInternal(ctx, "insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, tot_col_size) values (%?, %?, 0, %?, 1, %?)", startTS, physicalID, colInfo.ID, int64(len(value.GetBytes()))*count); err != nil {
					return err
				}
				value, err = value.ConvertTo(h.mu.ctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
				if err != nil {
					return
				}
				// There must be only one bucket for this new column and the value is the default value.
				if _, err := exec.ExecuteInternal(ctx, "insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, repeats, count, lower_bound, upper_bound) values (%?, 0, %?, 0, %?, %?, %?, %?)", physicalID, colInfo.ID, count, count, value.GetBytes(), value.GetBytes()); err != nil {
					return err
				}
			}
		}
	}
	return
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
