// Copyright 2015 PingCAP, Inc.
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

package ddl

import (
	"encoding/hex"
	"fmt"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)
func constructOneRowTableScanPB(
	physicalTableID int64,
	tblInfo *model.TableInfo,
	handleCols []*model.ColumnInfo,
	desc bool,
) *tipb.Executor {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, handleCols, false)
	tblScan.TableId = physicalTableID
	tblScan.Desc = desc
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}
}

func constructLimitPB(count uint64) *tipb.Executor {
	limitExec := &tipb.Limit{
		Limit: count,
	}
	return &tipb.Executor{Tp: tipb.ExecType_TypeLimit, Limit: limitExec}
}

func buildOneRowTableScanDAG(
	distSQLCtx *distsqlctx.DistSQLContext,
	tbl table.PhysicalTable,
	handleCols []*model.ColumnInfo,
	limit uint64,
	desc bool,
) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	_, timeZoneOffset := time.Now().In(time.UTC).Zone()
	dagReq.TimeZoneOffset = int64(timeZoneOffset)
	for i := range handleCols {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	dagReq.Flags |= model.FlagInSelectStmt

	tblScanExec := constructOneRowTableScanPB(tbl.GetPhysicalID(), tbl.Meta(), handleCols, desc)
	dagReq.Executors = append(dagReq.Executors, tblScanExec)
	dagReq.Executors = append(dagReq.Executors, constructLimitPB(limit))
	distsql.SetEncodeType(distSQLCtx, dagReq)
	return dagReq, nil
}

func getColumnsTypes(columns []*model.ColumnInfo) []*types.FieldType {
	colTypes := make([]*types.FieldType, 0, len(columns))
	for _, col := range columns {
		colTypes = append(colTypes, &col.FieldType)
	}
	return colTypes
}

// buildOneRowTableScan builds a table scan that only return one row upon tblInfo.
func buildOneRowTableScan(
	ctx *ReorgContext,
	store kv.Storage,
	startTS uint64,
	tbl table.PhysicalTable,
	handleCols []*model.ColumnInfo,
	limit uint64,
	desc bool,
) (distsql.SelectResult, error) {
	distSQLCtx := newDefaultReorgDistSQLCtx(store.GetClient(), contextutil.NewStaticWarnHandler(0))
	dagPB, err := buildOneRowTableScanDAG(distSQLCtx, tbl, handleCols, limit, desc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var b distsql.RequestBuilder
	var builder *distsql.RequestBuilder
	var ranges []*ranger.Range
	if tbl.Meta().IsCommonHandle {
		ranges = ranger.FullNotNullRange()
	} else {
		ranges = ranger.FullIntRange(false)
	}
	builder = b.SetHandleRanges(distSQLCtx, tbl.GetPhysicalID(), tbl.Meta().IsCommonHandle, ranges)
	builder.SetDAGRequest(dagPB).
		SetStartTS(startTS).
		SetKeepOrder(true).
		SetConcurrency(1).
		SetDesc(desc).
		SetResourceGroupTagger(ctx.getResourceGroupTaggerForTopSQL()).
		SetResourceGroupName(ctx.resourceGroupName)

	builder.Request.NotFillCache = true
	builder.Request.Priority = kv.PriorityLow
	builder.RequestSource.RequestSourceInternal = true
	builder.RequestSource.RequestSourceType = ctx.ddlJobSourceType()

	kvReq, err := builder.Build()
	if err != nil {
		return nil, errors.Trace(err)
	}

	result, err := distsql.Select(ctx.ddlJobCtx, distSQLCtx, kvReq, getColumnsTypes(handleCols))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return result, nil
}

// GetTableMaxHandle gets the max handle of a PhysicalTable.
func GetTableMaxHandle(ctx *ReorgContext, store kv.Storage, startTS uint64, tbl table.PhysicalTable) (maxHandle kv.Handle, emptyTable bool, err error) {
	tblInfo := tbl.Meta()
	handleCols := buildHandleCols(tbl)

	// build a desc scan of tblInfo, which limit is 1, we can use it to retrieve the last handle of the table.
	result, err := buildOneRowTableScan(ctx, store, startTS, tbl, handleCols, 1, true)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	defer terror.Call(result.Close)

	chk := chunk.New(getColumnsTypes(handleCols), 1, 1)
	err = result.Next(ctx.ddlJobCtx, chk)
	if err != nil {
		return nil, false, errors.Trace(err)
	}

	if chk.NumRows() == 0 {
		// empty table
		return nil, true, nil
	}
	row := chk.GetRow(0)
	if tblInfo.IsCommonHandle {
		pkIdx := tables.FindPrimaryIndex(tblInfo)
		maxHandle, err = buildCommonHandleFromChunkRow(time.UTC, tblInfo, pkIdx, handleCols, row)
		return maxHandle, false, err
	}
	return kv.IntHandle(row.GetInt64(0)), false, nil
}

// existsTableRow checks if there is at least one row in the specified table.
// In case of an error during the operation, it returns false along with the error.
func existsTableRow(ctx *ReorgContext, store kv.Storage, tbl table.PhysicalTable, startTS uint64) (bool, error) {
	found := false
	err := iterateSnapshotKeys(ctx, store, kv.PriorityLow, tbl.RecordPrefix(), startTS, nil, nil,
		func(_ kv.Handle, _ kv.Key, _ []byte) (bool, error) {
			found = true
			return false, nil
		})
	if err != nil {
		return false, errors.Trace(err)
	}
	return found, nil
}

func buildHandleCols(tbl table.PhysicalTable) []*model.ColumnInfo {
	var handleCols []*model.ColumnInfo
	var pkIdx *model.IndexInfo
	tblInfo := tbl.Meta()
	switch {
	case tblInfo.PKIsHandle:
		for _, col := range tbl.Meta().Columns {
			if mysql.HasPriKeyFlag(col.GetFlag()) {
				handleCols = []*model.ColumnInfo{col}
				break
			}
		}
	case tblInfo.IsCommonHandle:
		pkIdx = tables.FindPrimaryIndex(tblInfo)
		cols := tblInfo.Cols()
		for _, idxCol := range pkIdx.Columns {
			handleCols = append(handleCols, cols[idxCol.Offset])
		}
	default:
		handleCols = []*model.ColumnInfo{model.NewExtraHandleColInfo()}
	}
	return handleCols
}

func buildCommonHandleFromChunkRow(loc *time.Location, tblInfo *model.TableInfo, idxInfo *model.IndexInfo,
	cols []*model.ColumnInfo, row chunk.Row) (kv.Handle, error) {
	fieldTypes := make([]*types.FieldType, 0, len(cols))
	for _, col := range cols {
		fieldTypes = append(fieldTypes, &col.FieldType)
	}
	datumRow := row.GetDatumRow(fieldTypes)
	tablecodec.TruncateIndexValues(tblInfo, idxInfo, datumRow)

	var handleBytes []byte
	handleBytes, err := codec.EncodeKey(loc, nil, datumRow...)
	if err != nil {
		return nil, err
	}
	return kv.NewCommonHandle(handleBytes)
}

// getTableRange gets the start and end handle of a table (or partition).
func getTableRange(ctx *ReorgContext, store kv.Storage, tbl table.PhysicalTable, snapshotVer uint64, priority int) (startHandleKey, endHandleKey kv.Key, err error) {
	// Get the start handle of this partition.
	err = iterateSnapshotKeys(ctx, store, priority, tbl.RecordPrefix(), snapshotVer, nil, nil,
		func(_ kv.Handle, rowKey kv.Key, _ []byte) (bool, error) {
			startHandleKey = rowKey
			return false, nil
		})
	if err != nil {
		return startHandleKey, endHandleKey, errors.Trace(err)
	}
	maxHandle, isEmptyTable, err := GetTableMaxHandle(ctx, store, snapshotVer, tbl)
	if err != nil {
		return startHandleKey, nil, errors.Trace(err)
	}
	if maxHandle != nil {
		endHandleKey = tablecodec.EncodeRecordKey(tbl.RecordPrefix(), maxHandle).Next()
	}
	if isEmptyTable || endHandleKey.Cmp(startHandleKey) <= 0 {
		logutil.DDLLogger().Info("get noop table range",
			zap.String("table", fmt.Sprintf("%v", tbl.Meta())),
			zap.Int64("table/partition ID", tbl.GetPhysicalID()),
			zap.String("start key", hex.EncodeToString(startHandleKey)),
			zap.String("end key", hex.EncodeToString(endHandleKey)),
			zap.Bool("is empty table", isEmptyTable))
		if startHandleKey == nil {
			endHandleKey = nil
		} else {
			endHandleKey = startHandleKey.Next()
		}
	}
	return
}
