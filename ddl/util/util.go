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

package util

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

const (
	deleteRangesTable         = `gc_delete_range`
	doneDeleteRangesTable     = `gc_delete_range_done`
	loadDeleteRangeSQL        = `SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM mysql.%n WHERE ts < %?`
	recordDoneDeletedRangeSQL = `INSERT IGNORE INTO mysql.gc_delete_range_done SELECT * FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?`
	completeDeleteRangeSQL    = `DELETE FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?`
	updateDeleteRangeSQL      = `UPDATE mysql.gc_delete_range SET start_key = %? WHERE job_id = %? AND element_id = %? AND start_key = %?`
	deleteDoneRecordSQL       = `DELETE FROM mysql.gc_delete_range_done WHERE job_id = %? AND element_id = %?`
	loadGlobalVars            = `SELECT HIGH_PRIORITY variable_name, variable_value from mysql.global_variables where variable_name in (%?)`
)

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	JobID, ElementID int64
	StartKey, EndKey []byte
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() ([]byte, []byte) {
	return t.StartKey, t.EndKey
}

// LoadDeleteRanges loads delete range tasks from gc_delete_range table.
func LoadDeleteRanges(ctx sessionctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	return loadDeleteRangesFromTable(ctx, deleteRangesTable, safePoint)
}

// LoadDoneDeleteRanges loads deleted ranges from gc_delete_range_done table.
func LoadDoneDeleteRanges(ctx sessionctx.Context, safePoint uint64) (ranges []DelRangeTask, _ error) {
	return loadDeleteRangesFromTable(ctx, doneDeleteRangesTable, safePoint)
}

func loadDeleteRangesFromTable(ctx sessionctx.Context, table string, safePoint uint64) (ranges []DelRangeTask, _ error) {
	var buf strings.Builder
	sqlexec.MustFormatSQL(&buf, loadDeleteRangeSQL, table, safePoint)
	rss, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), buf.String())
	if len(rss) > 0 {
		defer terror.Call(rss[0].Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	rs := rss[0]
	req := rs.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	for {
		err = rs.Next(context.TODO(), req)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			break
		}

		for row := it.Begin(); row != it.End(); row = it.Next() {
			startKey, err := hex.DecodeString(row.GetString(2))
			if err != nil {
				return nil, errors.Trace(err)
			}
			endKey, err := hex.DecodeString(row.GetString(3))
			if err != nil {
				return nil, errors.Trace(err)
			}
			ranges = append(ranges, DelRangeTask{
				JobID:     row.GetInt64(0),
				ElementID: row.GetInt64(1),
				StartKey:  startKey,
				EndKey:    endKey,
			})
		}
	}
	return ranges, nil
}

// CompleteDeleteRange moves a record from gc_delete_range table to gc_delete_range_done table.
// NOTE: This function WILL NOT start and run in a new transaction internally.
func CompleteDeleteRange(ctx sessionctx.Context, dr DelRangeTask) error {
	var buf strings.Builder
	sqlexec.MustFormatSQL(&buf, recordDoneDeletedRangeSQL, dr.JobID, dr.ElementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), buf.String())
	if err != nil {
		return errors.Trace(err)
	}

	return RemoveFromGCDeleteRange(ctx, dr.JobID, dr.ElementID)
}

// RemoveFromGCDeleteRange is exported for ddl pkg to use.
func RemoveFromGCDeleteRange(ctx sessionctx.Context, jobID, elementID int64) error {
	var buf strings.Builder
	sqlexec.MustFormatSQL(&buf, completeDeleteRangeSQL, jobID, elementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), buf.String())
	return errors.Trace(err)
}

// DeleteDoneRecord removes a record from gc_delete_range_done table.
func DeleteDoneRecord(ctx sessionctx.Context, dr DelRangeTask) error {
	var buf strings.Builder
	sqlexec.MustFormatSQL(&buf, deleteDoneRecordSQL, dr.JobID, dr.ElementID)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), buf.String())
	return errors.Trace(err)
}

// UpdateDeleteRange is only for emulator.
func UpdateDeleteRange(ctx sessionctx.Context, dr DelRangeTask, newStartKey, oldStartKey kv.Key) error {
	newStartKeyHex := hex.EncodeToString(newStartKey)
	oldStartKeyHex := hex.EncodeToString(oldStartKey)
	var buf strings.Builder
	sqlexec.MustFormatSQL(&buf, updateDeleteRangeSQL, newStartKeyHex, dr.JobID, dr.ElementID, oldStartKeyHex)
	_, err := ctx.(sqlexec.SQLExecutor).Execute(context.TODO(), buf.String())
	return errors.Trace(err)
}

// LoadDDLReorgVars loads ddl reorg variable from mysql.global_variables.
func LoadDDLReorgVars(ctx sessionctx.Context) error {
	return LoadGlobalVars(ctx, []string{variable.TiDBDDLReorgWorkerCount, variable.TiDBDDLReorgBatchSize})
}

// LoadDDLVars loads ddl variable from mysql.global_variables.
func LoadDDLVars(ctx sessionctx.Context) error {
	return LoadGlobalVars(ctx, []string{variable.TiDBDDLErrorCountLimit})
}

// LoadGlobalVars loads global variable from mysql.global_variables.
func LoadGlobalVars(ctx sessionctx.Context, varNames []string) error {
	if sctx, ok := ctx.(sqlexec.RestrictedSQLExecutor); ok {
		var buf strings.Builder
		sqlexec.MustFormatSQL(&buf, loadGlobalVars, varNames)
		rows, _, err := sctx.ExecRestrictedSQL(ctx, buf.String())
		if err != nil {
			return errors.Trace(err)
		}
		for _, row := range rows {
			varName := row.GetString(0)
			varValue := row.GetString(1)
			variable.SetLocalSystemVar(varName, varValue)
		}
	}
	return nil
}
