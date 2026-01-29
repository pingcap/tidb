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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/copr"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/errctx"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/pingcap/tipb/go-tipb"
	kvutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const tableScanCopID = 1

func wrapInBeginRollback(se *sess.Session, f func(startTS uint64) error) error {
	err := se.Begin(context.Background())
	if err != nil {
		return errors.Trace(err)
	}
	defer se.Rollback()

	txn, err := se.Txn()
	if err != nil {
		return err
	}
	startTS := txn.StartTS()
	failpoint.InjectCall("wrapInBeginRollbackStartTS", startTS)
	return f(startTS)
}

func buildTableScan(ctx context.Context, c *copr.CopContextBase, distSQLCtx *distsqlctx.DistSQLContext, startTS uint64, start, end kv.Key, selectExpr expression.Expression) (distsql.SelectResult, bool, error) {
	dagPB, conditionPushed, err := buildDAGPB(ctx, c.ExprCtx, distSQLCtx, c.PushDownFlags, c.TableInfo, c.ColumnInfos, selectExpr)
	if err != nil {
		return nil, false, err
	}

	var builder distsql.RequestBuilder
	builder.
		SetDAGRequest(dagPB).
		SetStartTS(startTS).
		SetKeyRanges([]kv.KeyRange{{StartKey: start, EndKey: end}}).
		SetKeepOrder(true).
		SetFromSessionVars(distSQLCtx).
		SetConcurrency(1)
	if selectExpr != nil {
		// DDL will not push down to TiFlash currently, so we can just specify `kv.TiKV` here to make it clearer.
		builder.SetStoreType(kv.TiKV)
	}
	kvReq, err := builder.
		Build()
	kvReq.RequestSource.RequestSourceInternal = true
	kvReq.RequestSource.RequestSourceType = getDDLRequestSource(model.ActionAddIndex)
	kvReq.RequestSource.ExplicitRequestSourceType = kvutil.ExplicitTypeDDL
	if err != nil {
		return nil, conditionPushed, err
	}

	if distSQLCtx.RuntimeStatsColl == nil {
		result, err := distsql.Select(ctx, distSQLCtx, kvReq, c.FieldTypes)
		return result, conditionPushed, err
	}
	// The plan ID of the table scan is always `tableScanCopID`, so we can read the stats of `tableScanCopID` executor to know
	// how many rows have been scanned.
	//
	// The following logic assumes that the DAG has a structure like:
	// TableScan -> Executor1 -> Executor2 -> ... -> ExecutorN
	// So the plan IDs are assigned like:
	// TableScan: tableScanCopID
	// Executor1: tableScanCopID + 1
	// Executor2: tableScanCopID + 2
	// ...
	// ExecutorN: tableScanCopID + N
	copPlanIDs := make([]int, 0, 2)
	copPlanIDs = append(copPlanIDs, tableScanCopID)
	rootPlanID := tableScanCopID
	for i := range dagPB.Executors {
		if i == 0 {
			continue
		}
		copPlanIDs = append(copPlanIDs, tableScanCopID+i)
		rootPlanID = tableScanCopID + i
	}
	result, err := distsql.SelectWithRuntimeStats(ctx, distSQLCtx, kvReq, c.FieldTypes, copPlanIDs, rootPlanID)
	return result, conditionPushed, err
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

func buildDAGPB(ctx context.Context, exprCtx exprctx.BuildContext, distSQLCtx *distsqlctx.DistSQLContext, pushDownFlags uint64, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo, selectExpr expression.Expression) (*tipb.DAGRequest, bool, error) {
	conditionPushed := false

	dagReq := &tipb.DAGRequest{}
	dagReq.TimeZoneName, dagReq.TimeZoneOffset = timeutil.Zone(exprCtx.GetEvalCtx().Location())
	dagReq.Flags = pushDownFlags
	for i := range colInfos {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	tblScanPB, err := constructTableScanPB(exprCtx, tblInfo, colInfos)
	if err != nil {
		return nil, false, err
	}

	var selectionPB *tipb.Executor
	if selectExpr != nil {
		selectionPB, err = constructSelectionPB(exprCtx, selectExpr, distSQLCtx, tblScanPB)
	}

	// Now, the partial index doesn't support pushing down part of the condition.
	// So if we cannot push down the whole condition, we just ignore it.
	if err == nil && selectionPB != nil {
		conditionPushed = true
		dagReq.Executors = append(dagReq.Executors, tblScanPB, selectionPB)
	} else {
		if selectExpr != nil {
			selectExprStr := selectExpr.StringWithCtx(exprCtx.GetEvalCtx(), errors.RedactLogDisable)
			logutil.Logger(ctx).Info("fail to push down the selection expression for index condition",
				zap.String("table", tblInfo.Name.O),
				zap.String("expr", selectExprStr),
				zap.Error(err))
		}
		dagReq.Executors = append(dagReq.Executors, tblScanPB)
	}

	distsql.SetEncodeType(distSQLCtx, dagReq)
	collExec := true
	dagReq.CollectExecutionSummaries = &collExec
	return dagReq, conditionPushed, nil
}

func constructTableScanPB(ctx exprctx.BuildContext, tblInfo *model.TableInfo, colInfos []*model.ColumnInfo) (*tipb.Executor, error) {
	tblScan := tables.BuildTableScanFromInfos(tblInfo, colInfos, false)
	tblScan.TableId = tblInfo.ID
	err := tables.SetPBColumnsDefaultValue(ctx, tblScan.Columns, colInfos)
	return &tipb.Executor{Tp: tipb.ExecType_TypeTableScan, TblScan: tblScan}, err
}

func constructSelectionPB(ctx exprctx.BuildContext, expr expression.Expression, distSQLCtx *distsqlctx.DistSQLContext, child *tipb.Executor) (*tipb.Executor, error) {
	// Just use the default `variable.DefGroupConcatMaxLen`, it only affects the AGG functions, so it doesn't matter here.
	pc := expression.NewPushDownContext(ctx.GetEvalCtx(), distSQLCtx.Client, false, nil, nil, variable.DefGroupConcatMaxLen)
	// DDL will not push down to TiFlash currently, so we can just specify `kv.TiKV` here.
	// If we want to support TiFlash in the future, we need to try to push down to both TiKV and TiFlash.
	pushed, _ := expression.PushDownExprs(pc, []expression.Expression{expr}, kv.TiKV)
	if len(pushed) == 0 {
		// If no expression is pushed down, return nil to indicate that push down is not supported.
		return nil, errors.New("cannot push down the selection expression")
	}

	// As we have only one expression, the pushed expressions should be the same as the original expression.
	pbExpr, err := expression.ExpressionsToPBList(ctx.GetEvalCtx(), pushed, distSQLCtx.Client)
	if err != nil {
		return nil, err
	}

	return &tipb.Executor{
		Tp: tipb.ExecType_TypeSelection,
		Selection: &tipb.Selection{
			Conditions: pbExpr,
			Child:      child,
		},
	}, nil
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
