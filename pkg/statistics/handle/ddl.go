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
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	statsutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// HandleDDLEvent begins to process a ddl task.
func (h *Handle) HandleDDLEvent(t *util.Event) error {
	switch t.Tp {
	case model.ActionCreateTable, model.ActionTruncateTable:
		ids, err := h.getInitStateTableIDs(t.TableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.insertTableStats2KV(t.TableInfo, id); err != nil {
				return err
			}
		}
	case model.ActionDropTable:
		ids, err := h.getInitStateTableIDs(t.TableInfo)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := h.resetTableStats2KVForDrop(id); err != nil {
				return err
			}
		}
	case model.ActionAddColumn, model.ActionModifyColumn:
		ids, err := h.getInitStateTableIDs(t.TableInfo)
		if err != nil {
			return err
		}
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
		pruneMode, err := h.GetCurrentPruneMode()
		if err != nil {
			return err
		}
		if variable.PartitionPruneMode(pruneMode) == variable.Dynamic && t.PartInfo != nil {
			if err := h.updateGlobalStats(t.TableInfo); err != nil {
				return err
			}
		}
		for _, def := range t.PartInfo.Definitions {
			if err := h.resetTableStats2KVForDrop(def.ID); err != nil {
				return err
			}
		}
	case model.ActionReorganizePartition:
		for _, def := range t.PartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.insertTableStats2KV(t.TableInfo, def.ID); err != nil {
				return err
			}
			// Do not update global stats, since the data have not changed!
		}
	case model.ActionAlterTablePartitioning:
		// Add partitioning
		for _, def := range t.PartInfo.Definitions {
			// TODO: Should we trigger analyze instead of adding 0s?
			if err := h.insertTableStats2KV(t.TableInfo, def.ID); err != nil {
				return err
			}
		}
		fallthrough
	case model.ActionRemovePartitioning:
		// Change id for global stats, since the data has not changed!
		// Note that t.TableInfo is the current (new) table info
		// and t.PartInfo.NewTableID is actually the old table ID!
		// (see onReorganizePartition)
		return h.changeGlobalStatsID(t.PartInfo.NewTableID, t.TableInfo.ID)
	case model.ActionFlashbackCluster:
		return h.updateStatsVersion()
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

// updateStatsVersion will set statistics version to the newest TS,
// then tidb-server will reload automatic.
func (h *Handle) updateStatsVersion() error {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		return storage.UpdateStatsVersion(sctx)
	}, statsutil.FlagWrapTxn)
}

// updateGlobalStats will trigger the merge of global-stats when we drop table partition
func (h *Handle) updateGlobalStats(tblInfo *model.TableInfo) error {
	// We need to merge the partition-level stats to global-stats when we drop table partition in dynamic mode.
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		tableID := tblInfo.ID
		is := sessiontxn.GetTxnManager(sctx).GetTxnInfoSchema()
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
		newColGlobalStats, err := h.mergePartitionStats2GlobalStats(opts, is, tblInfo, false, nil, nil)
		if err != nil {
			return err
		}
		if len(newColGlobalStats.MissingPartitionStats) > 0 {
			logutil.BgLogger().Warn("missing partition stats when merging global stats", zap.String("table", tblInfo.Name.L),
				zap.String("item", "columns"), zap.Strings("missing", newColGlobalStats.MissingPartitionStats))
		}
		for i := 0; i < newColGlobalStats.Num; i++ {
			hg, cms, topN := newColGlobalStats.Hg[i], newColGlobalStats.Cms[i], newColGlobalStats.TopN[i]
			if hg == nil {
				// All partitions have no stats so global stats are not created.
				continue
			}
			// fms for global stats doesn't need to dump to kv.
			err = h.SaveStatsToStorage(tableID, newColGlobalStats.Count, newColGlobalStats.ModifyCount,
				0, hg, cms, topN, 2, 1, false, StatsMetaHistorySourceSchemaChange)
			if err != nil {
				return err
			}
		}

		// Generate the new index global-stats
		globalIdxStatsTopNNum, globalIdxStatsBucketNum := 0, 0
		for _, idx := range tblInfo.Indices {
			globalIdxStatsTopN := globalStats.Indices[idx.ID].TopN
			if globalIdxStatsTopN != nil && len(globalIdxStatsTopN.TopN) > globalIdxStatsTopNNum {
				globalIdxStatsTopNNum = len(globalIdxStatsTopN.TopN)
			}
			globalIdxStats := globalStats.Indices[idx.ID]
			if globalIdxStats != nil && len(globalIdxStats.Buckets) > globalIdxStatsBucketNum {
				globalIdxStatsBucketNum = len(globalIdxStats.Buckets)
			}
			if globalIdxStatsTopNNum != 0 {
				opts[ast.AnalyzeOptNumTopN] = uint64(globalIdxStatsTopNNum)
			}
			if globalIdxStatsBucketNum != 0 {
				opts[ast.AnalyzeOptNumBuckets] = uint64(globalIdxStatsBucketNum)
			}
			newIndexGlobalStats, err := h.mergePartitionStats2GlobalStats(opts, is, tblInfo, true, []int64{idx.ID}, nil)
			if err != nil {
				return err
			}
			if len(newIndexGlobalStats.MissingPartitionStats) > 0 {
				logutil.BgLogger().Warn("missing partition stats when merging global stats", zap.String("table", tblInfo.Name.L),
					zap.String("item", "index "+idx.Name.L), zap.Strings("missing", newIndexGlobalStats.MissingPartitionStats))
			}
			for i := 0; i < newIndexGlobalStats.Num; i++ {
				hg, cms, topN := newIndexGlobalStats.Hg[i], newIndexGlobalStats.Cms[i], newIndexGlobalStats.TopN[i]
				if hg == nil {
					// All partitions have no stats so global stats are not created.
					continue
				}
				// fms for global stats doesn't need to dump to kv.
				err = h.SaveStatsToStorage(tableID, newIndexGlobalStats.Count, newIndexGlobalStats.ModifyCount, 1, hg, cms, topN, 2, 1, false, StatsMetaHistorySourceSchemaChange)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (h *Handle) changeGlobalStatsID(from, to int64) (err error) {
	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		for _, table := range []string{"stats_meta", "stats_top_n", "stats_fm_sketch", "stats_buckets", "stats_histograms", "column_stats_usage"} {
			_, err = statsutil.Exec(sctx, "update mysql."+table+" set table_id = %? where table_id = %?", to, from)
			if err != nil {
				return err
			}
		}
		return nil
	}, statsutil.FlagWrapTxn)
}

func (h *Handle) getInitStateTableIDs(tblInfo *model.TableInfo) (ids []int64, err error) {
	pi := tblInfo.GetPartitionInfo()
	if pi == nil {
		return []int64{tblInfo.ID}, nil
	}
	ids = make([]int64, 0, len(pi.Definitions)+1)
	for _, def := range pi.Definitions {
		ids = append(ids, def.ID)
	}
	pruneMode, err := h.GetCurrentPruneMode()
	if err != nil {
		return nil, err
	}
	if variable.PartitionPruneMode(pruneMode) == variable.Dynamic {
		ids = append(ids, tblInfo.ID)
	}
	return ids, nil
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
			h.RecordHistoricalStatsMeta(physicalID, statsVer, StatsMetaHistorySourceSchemaChange)
		}
	}()

	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		startTS, err := statsutil.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}
		if _, err := statsutil.Exec(sctx, "insert into mysql.stats_meta (version, table_id) values(%?, %?)", startTS, physicalID); err != nil {
			return err
		}
		statsVer = startTS
		for _, col := range info.Columns {
			if _, err := statsutil.Exec(sctx, "insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%?, 0, %?, 0, %?)", physicalID, col.ID, startTS); err != nil {
				return err
			}
		}
		for _, idx := range info.Indices {
			if _, err := statsutil.Exec(sctx, "insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%?, 1, %?, 0, %?)", physicalID, idx.ID, startTS); err != nil {
				return err
			}
		}
		return nil
	}, statsutil.FlagWrapTxn)
}

// resetTableStats2KV resets the count to 0.
func (h *Handle) resetTableStats2KVForDrop(physicalID int64) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.RecordHistoricalStatsMeta(physicalID, statsVer, StatsMetaHistorySourceSchemaChange)
		}
	}()

	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		startTS, err := statsutil.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}
		if _, err := statsutil.Exec(sctx, "update mysql.stats_meta set version=%? where table_id =%?", startTS, physicalID); err != nil {
			return err
		}
		return nil
	}, statsutil.FlagWrapTxn)
}

// insertColStats2KV insert a record to stats_histograms with distinct_count 1 and insert a bucket to stats_buckets with default value.
// This operation also updates version.
func (h *Handle) insertColStats2KV(physicalID int64, colInfos []*model.ColumnInfo) (err error) {
	statsVer := uint64(0)
	defer func() {
		if err == nil && statsVer != 0 {
			h.RecordHistoricalStatsMeta(physicalID, statsVer, StatsMetaHistorySourceSchemaChange)
		}
	}()

	return h.callWithSCtx(func(sctx sessionctx.Context) error {
		startTS, err := statsutil.GetStartTS(sctx)
		if err != nil {
			return errors.Trace(err)
		}

		// First of all, we update the version.
		_, err = statsutil.Exec(sctx, "update mysql.stats_meta set version = %? where table_id = %?", startTS, physicalID)
		if err != nil {
			return err
		}
		statsVer = startTS
		// If we didn't update anything by last SQL, it means the stats of this table does not exist.
		if sctx.GetSessionVars().StmtCtx.AffectedRows() > 0 {
			// By this step we can get the count of this table, then we can sure the count and repeats of bucket.
			var rs sqlexec.RecordSet
			rs, err = statsutil.Exec(sctx, "select count from mysql.stats_meta where table_id = %?", physicalID)
			if err != nil {
				return err
			}
			defer terror.Call(rs.Close)
			req := rs.NewChunk(nil)
			err = rs.Next(context.Background(), req)
			if err != nil {
				return err
			}
			count := req.GetRow(0).GetInt64(0)
			for _, colInfo := range colInfos {
				value := types.NewDatum(colInfo.GetOriginDefaultValue())
				value, err = value.ConvertTo(sctx.GetSessionVars().StmtCtx, &colInfo.FieldType)
				if err != nil {
					return err
				}
				if value.IsNull() {
					// If the adding column has default value null, all the existing rows have null value on the newly added column.
					if _, err := statsutil.Exec(sctx, "insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, null_count) values (%?, %?, 0, %?, 0, %?)", startTS, physicalID, colInfo.ID, count); err != nil {
						return err
					}
				} else {
					// If this stats exists, we insert histogram meta first, the distinct_count will always be one.
					if _, err := statsutil.Exec(sctx, "insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count, tot_col_size) values (%?, %?, 0, %?, 1, %?)", startTS, physicalID, colInfo.ID, int64(len(value.GetBytes()))*count); err != nil {
						return err
					}
					value, err = value.ConvertTo(sctx.GetSessionVars().StmtCtx, types.NewFieldType(mysql.TypeBlob))
					if err != nil {
						return err
					}
					// There must be only one bucket for this new column and the value is the default value.
					if _, err := statsutil.Exec(sctx, "insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, repeats, count, lower_bound, upper_bound) values (%?, 0, %?, 0, %?, %?, %?, %?)", physicalID, colInfo.ID, count, count, value.GetBytes(), value.GetBytes()); err != nil {
						return err
					}
				}
			}
		}
		return nil
	}, statsutil.FlagWrapTxn)
}
