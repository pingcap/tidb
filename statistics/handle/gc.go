// Copyright 2018 PingCAP, Inc.
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
	"fmt"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/util/sqlexec"
)

// GCStats will garbage collect the useless stats info. For dropped tables, we will first update their version so that
// other tidb could know that table is deleted.
func (h *Handle) GCStats(is infoschema.InfoSchema, ddlLease time.Duration) error {
	// To make sure that all the deleted tables' schema and stats info have been acknowledged to all tidb,
	// we only garbage collect version before 10 lease.
	lease := mathutil.MaxInt64(int64(h.Lease()), int64(ddlLease))
	offset := DurationToTS(10 * time.Duration(lease))
	if h.LastUpdateVersion() < offset {
		return nil
	}
	sql := fmt.Sprintf("select table_id from mysql.stats_meta where version < %d", h.LastUpdateVersion()-offset)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	if err != nil {
		return errors.Trace(err)
	}
	for _, row := range rows {
		if err := h.gcTableStats(is, row.GetInt64(0)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (h *Handle) gcTableStats(is infoschema.InfoSchema, physicalID int64) error {
	sql := fmt.Sprintf("select is_index, hist_id from mysql.stats_histograms where table_id = %d", physicalID)
	rows, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
	if err != nil {
		return errors.Trace(err)
	}
	// The table has already been deleted in stats and acknowledged to all tidb,
	// we can safely remove the meta info now.
	if len(rows) == 0 {
		sql := fmt.Sprintf("delete from mysql.stats_meta where table_id = %d", physicalID)
		_, _, err := h.restrictedExec.ExecRestrictedSQL(sql)
		return errors.Trace(err)
	}
	h.mu.Lock()
	tbl, ok := h.getTableByPhysicalID(is, physicalID)
	h.mu.Unlock()
	if !ok {
		return errors.Trace(h.DeleteTableStatsFromKV(physicalID))
	}
	tblInfo := tbl.Meta()
	for _, row := range rows {
		isIndex, histID := row.GetInt64(0), row.GetInt64(1)
		find := false
		if isIndex == 1 {
			for _, idx := range tblInfo.Indices {
				if idx.ID == histID {
					find = true
					break
				}
			}
		} else {
			for _, col := range tblInfo.Columns {
				if col.ID == histID {
					find = true
					break
				}
			}
		}
		if !find {
			if err := h.deleteHistStatsFromKV(physicalID, histID, int(isIndex)); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// deleteHistStatsFromKV deletes all records about a column or an index and updates version.
func (h *Handle) deleteHistStatsFromKV(physicalID int64, histID int64, isIndex int) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(context.Background(), exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	sqls := make([]string, 0, 4)
	// First of all, we update the version. If this table doesn't exist, it won't have any problem. Because we cannot delete anything.
	sqls = append(sqls, fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d ", startTS, physicalID))
	// delete histogram meta
	sqls = append(sqls, fmt.Sprintf("delete from mysql.stats_histograms where table_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	// delete top n data
	sqls = append(sqls, fmt.Sprintf("delete from mysql.stats_top_n where table_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	// delete all buckets
	sqls = append(sqls, fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d and hist_id = %d and is_index = %d", physicalID, histID, isIndex))
	return execSQLs(context.Background(), exec, sqls)
}

// DeleteTableStatsFromKV deletes table statistics from kv.
func (h *Handle) DeleteTableStatsFromKV(physicalID int64) (err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	exec := h.mu.ctx.(sqlexec.SQLExecutor)
	_, err = exec.Execute(context.Background(), "begin")
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		err = finishTransaction(context.Background(), exec, err)
	}()
	txn, err := h.mu.ctx.Txn(true)
	if err != nil {
		return errors.Trace(err)
	}
	startTS := txn.StartTS()
	sqls := make([]string, 0, 3)
	// We only update the version so that other tidb will know that this table is deleted.
	sqls = append(sqls, fmt.Sprintf("update mysql.stats_meta set version = %d where table_id = %d ", startTS, physicalID))
	sqls = append(sqls, fmt.Sprintf("delete from mysql.stats_histograms where table_id = %d", physicalID))
	sqls = append(sqls, fmt.Sprintf("delete from mysql.stats_buckets where table_id = %d", physicalID))
	return execSQLs(context.Background(), exec, sqls)
}
