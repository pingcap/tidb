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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"slices"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
)

// DecodeRowValToChunk decodes row value into chunk checking row format used.
func DecodeRowValToChunk(sctx sessionctx.Context, schema *expression.Schema, tblInfo *model.TableInfo,
	handle kv.Handle, rowVal []byte, chk *chunk.Chunk, rd *rowcodec.ChunkDecoder) error {
	if rowcodec.IsNewFormat(rowVal) {
		return rd.DecodeToChunk(rowVal, 0, handle, chk)
	}
	return decodeOldRowValToChunk(sctx, schema, tblInfo, handle, rowVal, chk)
}

func decodeOldRowValToChunk(sctx sessionctx.Context, schema *expression.Schema, tblInfo *model.TableInfo, handle kv.Handle,
	rowVal []byte, chk *chunk.Chunk) error {
	pkCols := tables.TryGetCommonPkColumnIds(tblInfo)
	prefixColIDs := tables.PrimaryPrefixColumnIDs(tblInfo)
	colID2CutPos := make(map[int64]int, schema.Len())
	for _, col := range schema.Columns {
		if _, ok := colID2CutPos[col.ID]; !ok {
			colID2CutPos[col.ID] = len(colID2CutPos)
		}
	}
	cutVals, err := tablecodec.CutRowNew(rowVal, colID2CutPos)
	if err != nil {
		return err
	}
	if cutVals == nil {
		cutVals = make([][]byte, len(colID2CutPos))
	}
	decoder := codec.NewDecoder(chk, sctx.GetSessionVars().Location())
	for i, col := range schema.Columns {
		// fill the virtual column value after row calculation
		if col.VirtualExpr != nil {
			chk.AppendNull(i)
			continue
		}
		ok, err := tryDecodeFromHandle(tblInfo, i, col, handle, chk, decoder, pkCols, prefixColIDs)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		cutPos := colID2CutPos[col.ID]
		if len(cutVals[cutPos]) == 0 {
			colInfo := getColInfoByID(tblInfo, col.ID)
			d, err1 := table.GetColOriginDefaultValue(sctx.GetExprCtx(), colInfo)
			if err1 != nil {
				return err1
			}
			chk.AppendDatum(i, &d)
			continue
		}
		_, err = decoder.DecodeOne(cutVals[cutPos], i, col.RetType)
		if err != nil {
			return err
		}
	}
	return nil
}

func tryDecodeFromHandle(tblInfo *model.TableInfo, schemaColIdx int, col *expression.Column, handle kv.Handle, chk *chunk.Chunk,
	decoder *codec.Decoder, pkCols []int64, prefixColIDs []int64) (bool, error) {
	if tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.RetType.GetFlag()) {
		chk.AppendInt64(schemaColIdx, handle.IntValue())
		return true, nil
	}
	if col.ID == model.ExtraHandleID {
		chk.AppendInt64(schemaColIdx, handle.IntValue())
		return true, nil
	}
	if types.NeedRestoredData(col.RetType) {
		return false, nil
	}
	// Try to decode common handle.
	if mysql.HasPriKeyFlag(col.RetType.GetFlag()) {
		for i, hid := range pkCols {
			if col.ID == hid && notPKPrefixCol(hid, prefixColIDs) {
				_, err := decoder.DecodeOne(handle.EncodedCol(i), schemaColIdx, col.RetType)
				if err != nil {
					return false, errors.Trace(err)
				}
				return true, nil
			}
		}
	}
	return false, nil
}

func notPKPrefixCol(colID int64, prefixColIDs []int64) bool {
	return !slices.Contains(prefixColIDs, colID)
}

func getColInfoByID(tbl *model.TableInfo, colID int64) *model.ColumnInfo {
	for _, col := range tbl.Columns {
		if col.ID == colID {
			return col
		}
	}
	return nil
}

type runtimeStatsWithSnapshot struct {
	*txnsnapshot.SnapshotRuntimeStats
}

func (e *runtimeStatsWithSnapshot) String() string {
	if e.SnapshotRuntimeStats != nil {
		return e.SnapshotRuntimeStats.String()
	}
	return ""
}

// Clone implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Clone() execdetails.RuntimeStats {
	newRs := &runtimeStatsWithSnapshot{}
	if e.SnapshotRuntimeStats != nil {
		snapshotStats := e.SnapshotRuntimeStats.Clone()
		newRs.SnapshotRuntimeStats = snapshotStats
	}
	return newRs
}

// Merge implements the RuntimeStats interface.
func (e *runtimeStatsWithSnapshot) Merge(other execdetails.RuntimeStats) {
	tmp, ok := other.(*runtimeStatsWithSnapshot)
	if !ok {
		return
	}
	if tmp.SnapshotRuntimeStats != nil {
		if e.SnapshotRuntimeStats == nil {
			snapshotStats := tmp.SnapshotRuntimeStats.Clone()
			e.SnapshotRuntimeStats = snapshotStats
			return
		}
		e.SnapshotRuntimeStats.Merge(tmp.SnapshotRuntimeStats)
	}
}

// Tp implements the RuntimeStats interface.
func (*runtimeStatsWithSnapshot) Tp() int {
	return execdetails.TpRuntimeStatsWithSnapshot
}
