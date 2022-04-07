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

package util

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/tikv/client-go/v2/tikvrpc"
	atomicutil "go.uber.org/atomic"
)

const (
	deleteRangesTable            = `gc_delete_range`
	doneDeleteRangesTable        = `gc_delete_range_done`
	loadDeleteRangeSQL           = `SELECT HIGH_PRIORITY job_id, element_id, start_key, end_key FROM mysql.%n WHERE ts < %?`
	recordDoneDeletedRangeSQL    = `INSERT IGNORE INTO mysql.gc_delete_range_done SELECT * FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?`
	completeDeleteRangeSQL       = `DELETE FROM mysql.gc_delete_range WHERE job_id = %? AND element_id = %?`
	completeDeleteMultiRangesSQL = `DELETE FROM mysql.gc_delete_range WHERE job_id = %? AND element_id in (` // + idList + ")"
	updateDeleteRangeSQL         = `UPDATE mysql.gc_delete_range SET start_key = %? WHERE job_id = %? AND element_id = %? AND start_key = %?`
	deleteDoneRecordSQL          = `DELETE FROM mysql.gc_delete_range_done WHERE job_id = %? AND element_id = %?`
	loadGlobalVars               = `SELECT HIGH_PRIORITY variable_name, variable_value from mysql.global_variables where variable_name in (` // + nameList + ")"
)

// DelRangeTask is for run delete-range command in gc_worker.
type DelRangeTask struct {
	JobID, ElementID int64
	StartKey, EndKey kv.Key
}

// Range returns the range [start, end) to delete.
func (t DelRangeTask) Range() (kv.Key, kv.Key) {
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
	rs, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), loadDeleteRangeSQL, table, safePoint)
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}

	req := rs.NewChunk(nil)
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
func CompleteDeleteRange(ctx sessionctx.Context, dr DelRangeTask) error {
	_, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "BEGIN")
	if err != nil {
		return errors.Trace(err)
	}

	_, err = ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), recordDoneDeletedRangeSQL, dr.JobID, dr.ElementID)
	if err != nil {
		return errors.Trace(err)
	}

	err = RemoveFromGCDeleteRange(ctx, dr.JobID, dr.ElementID)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), "COMMIT")
	return errors.Trace(err)
}

// RemoveFromGCDeleteRange is exported for ddl pkg to use.
func RemoveFromGCDeleteRange(ctx sessionctx.Context, jobID, elementID int64) error {
	_, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), completeDeleteRangeSQL, jobID, elementID)
	return errors.Trace(err)
}

// RemoveMultiFromGCDeleteRange is exported for ddl pkg to use.
func RemoveMultiFromGCDeleteRange(ctx context.Context, sctx sessionctx.Context, jobID int64, elementIDs []int64) error {
	var buf strings.Builder
	buf.WriteString(completeDeleteMultiRangesSQL)
	paramIDs := make([]interface{}, 0, 1+len(elementIDs))
	paramIDs = append(paramIDs, jobID)
	for i, elementID := range elementIDs {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("%?")
		paramIDs = append(paramIDs, elementID)
	}
	buf.WriteString(")")
	_, err := sctx.(sqlexec.SQLExecutor).ExecuteInternal(ctx, buf.String(), paramIDs...)
	return errors.Trace(err)
}

// DeleteDoneRecord removes a record from gc_delete_range_done table.
func DeleteDoneRecord(ctx sessionctx.Context, dr DelRangeTask) error {
	_, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), deleteDoneRecordSQL, dr.JobID, dr.ElementID)
	return errors.Trace(err)
}

// UpdateDeleteRange is only for emulator.
func UpdateDeleteRange(ctx sessionctx.Context, dr DelRangeTask, newStartKey, oldStartKey kv.Key) error {
	newStartKeyHex := hex.EncodeToString(newStartKey)
	oldStartKeyHex := hex.EncodeToString(oldStartKey)
	_, err := ctx.(sqlexec.SQLExecutor).ExecuteInternal(context.TODO(), updateDeleteRangeSQL, newStartKeyHex, dr.JobID, dr.ElementID, oldStartKeyHex)
	return errors.Trace(err)
}

// LoadDDLReorgVars loads ddl reorg variable from mysql.global_variables.
func LoadDDLReorgVars(ctx context.Context, sctx sessionctx.Context) error {
	// close issue #21391
	// variable.TiDBRowFormatVersion is used to encode the new row for column type change.
	return LoadGlobalVars(ctx, sctx, []string{variable.TiDBDDLReorgWorkerCount, variable.TiDBDDLReorgBatchSize, variable.TiDBRowFormatVersion})
}

// LoadDDLVars loads ddl variable from mysql.global_variables.
func LoadDDLVars(ctx sessionctx.Context) error {
	return LoadGlobalVars(context.Background(), ctx, []string{variable.TiDBDDLErrorCountLimit})
}

// LoadGlobalVars loads global variable from mysql.global_variables.
func LoadGlobalVars(ctx context.Context, sctx sessionctx.Context, varNames []string) error {
	if e, ok := sctx.(sqlexec.RestrictedSQLExecutor); ok {
		var buf strings.Builder
		buf.WriteString(loadGlobalVars)
		paramNames := make([]interface{}, 0, len(varNames))
		for i, name := range varNames {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("%?")
			paramNames = append(paramNames, name)
		}
		buf.WriteString(")")
		rows, _, err := e.ExecRestrictedSQL(ctx, nil, buf.String(), paramNames...)
		if err != nil {
			return errors.Trace(err)
		}
		for _, row := range rows {
			varName := row.GetString(0)
			varValue := row.GetString(1)
			if err = sctx.GetSessionVars().SetSystemVar(varName, varValue); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetTimeZone gets the session location's zone name and offset.
func GetTimeZone(sctx sessionctx.Context) (string, int) {
	loc := sctx.GetSessionVars().Location()
	name := loc.String()
	if name != "" {
		_, err := time.LoadLocation(name)
		if err == nil {
			return name, 0
		}
	}
	_, offset := time.Now().In(loc).Zone()
	return "UTC", offset
}

// enableEmulatorGC means whether to enable emulator GC. The default is enable.
// In some unit tests, we want to stop emulator GC, then wen can set enableEmulatorGC to 0.
var emulatorGCEnable = atomicutil.NewInt32(1)

// EmulatorGCEnable enables emulator gc. It exports for testing.
func EmulatorGCEnable() {
	emulatorGCEnable.Store(1)
}

// EmulatorGCDisable disables emulator gc. It exports for testing.
func EmulatorGCDisable() {
	emulatorGCEnable.Store(0)
}

// IsEmulatorGCEnable indicates whether emulator GC enabled. It exports for testing.
func IsEmulatorGCEnable() bool {
	return emulatorGCEnable.Load() == 1
}

var internalResourceGroupTag = []byte{0}

// GetInternalResourceGroupTaggerForTopSQL only use for testing.
func GetInternalResourceGroupTaggerForTopSQL() tikvrpc.ResourceGroupTagger {
	tagger := func(req *tikvrpc.Request) {
		req.ResourceGroupTag = internalResourceGroupTag
	}
	return tagger
}

// IsInternalResourceGroupTaggerForTopSQL use for testing.
func IsInternalResourceGroupTaggerForTopSQL(tag []byte) bool {
	return bytes.Equal(tag, internalResourceGroupTag)
}
