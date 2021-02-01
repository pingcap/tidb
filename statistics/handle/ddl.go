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
// See the License for the specific language governing permissions and
// limitations under the License.

package handle

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/sessionctx/variable"
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
		pruneMode := h.CurrentPruneMode()
		if pruneMode == variable.StaticOnly || pruneMode == variable.StaticButPrepareDynamic {
			for _, def := range t.PartInfo.Definitions {
				if err := h.insertTableStats2KV(t.TableInfo, def.ID); err != nil {
					return err
				}
			}
		}
		if pruneMode == variable.DynamicOnly || pruneMode == variable.StaticButPrepareDynamic {
			// TODO: need trigger full analyze
		}
	case model.ActionDropTablePartition:
		pruneMode := h.CurrentPruneMode()
		if pruneMode == variable.DynamicOnly || pruneMode == variable.StaticButPrepareDynamic {
			// TODO: need trigger full analyze
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
	pruneMode := h.CurrentPruneMode()
	if pruneMode == variable.StaticOnly || pruneMode == variable.StaticButPrepareDynamic {
		for _, def := range pi.Definitions {
			ids = append(ids, def.ID)
		}
	}
	if pruneMode == variable.DynamicOnly || pruneMode == variable.StaticButPrepareDynamic {
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
	exec.ExecuteInternal(ctx, "insert into mysql.stats_meta (version, table_id) values(%?, %?)", startTS, physicalID)
	for _, col := range info.Columns {
		if _, err := exec.ExecuteInternal(ctx, "insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%?, 0, %?, 0, %?", physicalID, col.ID, startTS); err != nil {
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
	// If we didn't update anything by last SQL, it means the stats of this table does not exist.
	if h.mu.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0 {
		// By this step we can get the count of this table, then we can sure the count and repeats of bucket.
		var rs []sqlexec.RecordSet
		rs, err = exec.ExecuteInternal(ctx, "select count from mysql.stats_meta where table_id = %?", physicalID)
		if len(rs) > 0 {
			defer terror.Call(rs[0].Close)
		}
		if err != nil {
			return
		}
		req := rs[0].NewChunk()
		err = rs[0].Next(ctx, req)
		if err != nil {
			return
		}
		count := req.GetRow(0).GetInt64(0)
		// sqls := make([]string, 0, len(colInfos))
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
		// return execSQLs(ctx, exec, sqls)
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
