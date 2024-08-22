// Copyright 2022 PingCAP, Inc.
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
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	kvutil "github.com/tikv/client-go/v2/util"
)

func wrapInBeginRollback(se *sess.Session, f func(startTS uint64) error) error {
	err := se.Begin(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Rollback()
	var startTS uint64
	sessVars := se.GetSessionVars()
	sessVars.TxnCtxMu.Lock()
	startTS = sessVars.TxnCtx.StartTS
	sessVars.TxnCtxMu.Unlock()
	return f(startTS)
}

func buildTableScan(ctx context.Context, c *copr.CopContextBase, startTS uint64, start, end kv.Key) (distsql.SelectResult, error) {
	dagPB, err := buildDAGPB(c.ExprCtx, c.DistSQLCtx, c.PushDownFlags, c.TableInfo, c.ColumnInfos)
	if err != nil {
		return nil, err
	}

	var builder distsql.RequestBuilder
	kvReq, err := builder.
		SetDAGRequest(dagPB).
		SetStartTS(startTS).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(c.DistSQLCtx).
		SetConcurrency(1).
		Build()
	kvReq.RequestSource.RequestSourceInternal = true
	kvReq.RequestSource.RequestSourceType = getDDLRequestSource(model.ActionAddIndex)
	kvReq.RequestSource.ExplicitRequestSourceType = kvutil.ExplicitTypeDDL
	if err != nil {
		return nil, err
	}
	return distsql.Select(ctx, c.DistSQLCtx, kvReq, c.FieldTypes)
}

func fetchTableScanResult(
	ctx context.Context,
	copCtx *copr.CopContextBase,
	result distsql.SelectResult,
	chk *chunk.Chunk,
) (bool, error) {
	err := result.Next(ctx, chk)
	if err != nil {
		return false, errors.Trace(err)
	}
	if chk.NumRows() == 0 {
		return true, nil
	}
	err = table.FillVirtualColumnValue(
		copCtx.VirtualColumnsFieldTypes, copCtx.VirtualColumnsOutputOffsets,
		copCtx.ExprColumnInfos, copCtx.ColumnInfos, copCtx.ExprCtx, chk)
	return false, err
}

func completeErr(err error, idxInfo *model.IndexInfo) error {
	if expression.ErrInvalidJSONForFuncIndex.Equal(err) {
		err = expression.ErrInvalidJSONForFuncIndex.GenWithStackByArgs(idxInfo.Name.O)
	}
	return errors.Trace(err)
}

func getRestoreData(tblInfo *model.TableInfo, targetIdx, pkIdx *model.IndexInfo, handleDts []types.Datum) []types.Datum {
	if !collate.NewCollationEnabled() || !tblInfo.IsCommonHandle || tblInfo.CommonHandleVersion == 0 {
		return nil
	}
	if pkIdx == nil {
		return nil
	}
	for i, pkIdxCol := range pkIdx.Columns {
		pkCol := tblInfo.Columns[pkIdxCol.Offset]
		if !types.NeedRestoredData(&pkCol.FieldType) {
			// Since the handle data cannot be null, we can use SetNull to
			// indicate that this column does not need to be restored.
			handleDts[i].SetNull()
			continue
		}
		tables.TryTruncateRestoredData(&handleDts[i], pkCol, pkIdxCol, targetIdx)
		tables.ConvertDatumToTailSpaceCount(&handleDts[i], pkCol)
	}
	dtToRestored := handleDts[:0]
	for _, handleDt := range handleDts {
		if !handleDt.IsNull() {
			dtToRestored = append(dtToRestored, handleDt)
		}
	}
	return dtToRestored
}

func buildDAGPB(exprCtx exprctx.BuildContext, distSQLCtx *distsqlctx.DistSQLContext, pushDownFlags uint64, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.DAGRequest, error) {
	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(exprCtx.GetEvalCtx().Location())
	dagReq.Flags = pushDownFlags
	for i := range colInfos {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	execPB, err := constructTableScanPB(exprCtx, tblInfo, colInfos)
	if err != nil {
		return nil, err
	}
	dagReq.Executors = append(dagReq.Executors, execPB)
	distsql.SetEncodeType(distSQLCtx, dagReq)
	return dagReq, nil
}

func constructTableScanPB(ctx exprctx.BuildContext, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.Executor, error) {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, colInfos, false)
	tblScan.TableId = tblInfo.ID
	err := tables.SetPBColumnsDefaultValue(ctx, tblScan.Columns, colInfos)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}, err
}

// ExtractDatumByOffsets is exported for test.
func ExtractDatumByOffsets(ctx expression.EvalContext, row chunk.Row, offsets []int, expCols []*expression.Column, buf []types.Datum) []types.Datum {
	for i, offset := range offsets {
		c := expCols[offset]
		row.DatumWithBuffer(offset, c.GetType(ctx), &buf[i])
	}
	return buf
}

// BuildHandle is exported for test.
func BuildHandle(pkDts []types.Datum, tblInfo *model.TableInfo,
	pkInfo *model.IndexInfo, loc *time.Location, errCtx errctx.Context) (kv.Handle, error) {
	if tblInfo.IsCommonHandle {
		tablecodec.TruncateIndexValues(tblInfo, pkInfo, pkDts)
		handleBytes, err := codec.EncodeKey(loc, nil, pkDts...)
		err = errCtx.HandleError(err)
		if err != nil {
			return nil, err
		}
		return kv.NewCommonHandle(handleBytes)
	}
	return kv.IntHandle(pkDts[0].GetInt64()), nil
}
