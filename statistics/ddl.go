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

package statistics

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/types"
)

// HandleDDLEvent begins to process a ddl task.
func (h *Handle) HandleDDLEvent(t *ddl.Event) error {
	switch t.Tp {
	case ddl.TypeCreateTable:
		return h.insertTableStats2KV(t.TableInfo)
	case ddl.TypeCreateColumn:
		return h.insertColStats2KV(t.TableInfo.ID, t.ColumnInfo)
	case ddl.TypeDropColumn:
		return h.deleteHistStatsFromKV(t.TableInfo.ID, t.ColumnInfo.ID, 0)
	case ddl.TypeDropIndex:
		return h.deleteHistStatsFromKV(t.TableInfo.ID, t.IndexInfo.ID, 1)
	}
	return nil
}

// DDLEventCh returns ddl events channel in handle.
func (h *Handle) DDLEventCh() chan *ddl.Event {
	return h.ddlEventCh
}

// insertTableStats2KV inserts a record standing for a new table to stats_meta and inserts some records standing for the
// new columns and indices which belong to this table.
func (h *Handle) insertTableStats2KV(info *model.TableInfo) error {
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("insert into mysql.stats_meta (version, table_id) values(%d, %d)", h.ctx.Txn().StartTS(), info.ID))
	if err != nil {
		return errors.Trace(err)
	}
	for _, col := range info.Columns {
		_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%d, 0, %d, 0, %d)", info.ID, col.ID, h.ctx.Txn().StartTS()))
		if err != nil {
			return errors.Trace(err)
		}
	}
	for _, idx := range info.Indices {
		_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("insert into mysql.stats_histograms (table_id, is_index, hist_id, distinct_count, version) values(%d, 1, %d, 0, %d)", info.ID, idx.ID, h.ctx.Txn().StartTS()))
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
}

// insertColStats2KV insert a record to stats_histograms with distinct_count 1 and insert a bucket to stats_buckets with default value.
// This operation also updates version.
func (h *Handle) insertColStats2KV(tableID int64, colInfo *model.ColumnInfo) error {
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	// First of all, we update the version.
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d ", h.ctx.Txn().StartTS(), tableID))
	if err != nil {
		return errors.Trace(err)
	}
	// If we didn't update anything by last SQL, it means the stats of this table does not exist.
	if h.ctx.GetSessionVars().StmtCtx.AffectedRows() > 0 {
		// If this stats exists, we insert histogram meta first, the distinct_count will always be one.
		_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("insert into mysql.stats_histograms (version, table_id, is_index, hist_id, distinct_count) values (%d, %d, 0, %d, 1)", h.ctx.Txn().StartTS(), tableID, colInfo.ID))
		if err != nil {
			return errors.Trace(err)
		}
		// By this step we can get the count of this table, then we can sure the count and repeats of bucket.
		rs, err := h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("select count from mysql.stats_meta where table_id = %d", tableID))
		if err != nil {
			return errors.Trace(err)
		}
		row, err := rs[0].Next()
		if err != nil {
			return errors.Trace(err)
		}
		count := row.Data[0].GetInt64()
		valueBytes, err := codec.EncodeValue(nil, types.NewDatum(colInfo.DefaultValue))
		if err != nil {
			return errors.Trace(err)
		}
		// There must be only one bucket for this new column and the value is the default value.
		_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("insert into mysql.stats_buckets (table_id, is_index, hist_id, bucket_id, repeats, count, value) values (%d, 0, %d, 0, %d, %d, X'%X')", tableID, colInfo.ID, count, count, valueBytes))
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
}

// deleteHistStatsFromKV deletes all records about a column or an index and updates version.
func (h *Handle) deleteHistStatsFromKV(tableID int64, histID int64, isIndex int) error {
	_, err := h.ctx.(sqlexec.SQLExecutor).Execute("begin")
	if err != nil {
		return errors.Trace(err)
	}
	// First of all, we update the version. If this table doesn't exist, it won't have any problem. Because we cannot delete anything.
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d ", h.ctx.Txn().StartTS(), tableID))
	if err != nil {
		return errors.Trace(err)
	}
	// delete histogram meta
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("delete from mysql.stats_histograms where table_id = %d and hist_id = %d and is_index = %d", tableID, histID, isIndex))
	if err != nil {
		return errors.Trace(err)
	}
	// delete all buckets
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute(fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d and hist_id = %d and is_index = %d", tableID, histID, isIndex))
	if err != nil {
		return errors.Trace(err)
	}
	_, err = h.ctx.(sqlexec.SQLExecutor).Execute("commit")
	return errors.Trace(err)
}
