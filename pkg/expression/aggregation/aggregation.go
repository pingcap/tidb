// Copyright 2016 PingCAP, Inc.
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

package aggregation

import (
	"bytes"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tipb/go-tipb"
)

// Aggregation stands for aggregate functions.
type Aggregation interface {
	// Update during executing.
	Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error

	// GetPartialResult will called by coprocessor to get partial results. For avg function, partial results will return
	// sum and count values at the same time.
	GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum

	// GetResult will be called when all data have been processed.
	GetResult(evalCtx *AggEvaluateContext) types.Datum

	// CreateContext creates a new AggEvaluateContext for the aggregation function.
	CreateContext(ctx expression.EvalContext) *AggEvaluateContext

	// ResetContext resets the content of the evaluate context.
	ResetContext(ctx expression.EvalContext, evalCtx *AggEvaluateContext)
}

// NewDistAggFunc creates new Aggregate function for mock tikv.
func NewDistAggFunc(expr *tipb.Expr, fieldTps []*types.FieldType, ctx expression.BuildContext) (Aggregation, *AggFuncDesc, error) {
	args := make([]expression.Expression, 0, len(expr.Children))
	for _, child := range expr.Children {
		arg, err := expression.PBToExpr(ctx, child, fieldTps)
		if err != nil {
			return nil, nil, err
		}
		args = append(args, arg)
	}
	switch expr.Tp {
	case tipb.ExprType_Sum:
		aggF := newAggFunc(ast.AggFuncSum, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &sumFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	case tipb.ExprType_Count:
		aggF := newAggFunc(ast.AggFuncCount, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &countFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	case tipb.ExprType_Avg:
		aggF := newAggFunc(ast.AggFuncAvg, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &avgFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	case tipb.ExprType_GroupConcat:
		aggF := newAggFunc(ast.AggFuncGroupConcat, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &concatFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	case tipb.ExprType_Max:
		aggF := newAggFunc(ast.AggFuncMax, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &maxMinFunction{aggFunction: aggF, isMax: true, ctor: collate.GetCollator(args[0].GetType(ctx.GetEvalCtx()).GetCollate())}, aggF.AggFuncDesc, nil
	case tipb.ExprType_Min:
		aggF := newAggFunc(ast.AggFuncMin, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &maxMinFunction{aggFunction: aggF, ctor: collate.GetCollator(args[0].GetType(ctx.GetEvalCtx()).GetCollate())}, aggF.AggFuncDesc, nil
	case tipb.ExprType_First:
		aggF := newAggFunc(ast.AggFuncFirstRow, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &firstRowFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	case tipb.ExprType_Agg_BitOr:
		aggF := newAggFunc(ast.AggFuncBitOr, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &bitOrFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	case tipb.ExprType_Agg_BitXor:
		aggF := newAggFunc(ast.AggFuncBitXor, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &bitXorFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	case tipb.ExprType_Agg_BitAnd:
		aggF := newAggFunc(ast.AggFuncBitAnd, args, false)
		aggF.Mode = AggFunctionMode(*expr.AggFuncMode)
		return &bitAndFunction{aggFunction: aggF}, aggF.AggFuncDesc, nil
	}
	return nil, nil, errors.Errorf("Unknown aggregate function type %v", expr.Tp)
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	Ctx             expression.EvalContext
	DistinctChecker *distinctChecker
	Count           int64
	Value           types.Datum
	Buffer          *bytes.Buffer // Buffer is used for group_concat.
	GotFirstRow     bool          // It will check if the agg has met the first row key.
}

// AggFunctionMode stands for the aggregation function's mode.
type AggFunctionMode int

// |-----------------|--------------|--------------|
// | AggFunctionMode | input        | output       |
// |-----------------|--------------|--------------|
// | CompleteMode    | origin data  | final result |
// | FinalMode       | partial data | final result |
// | Partial1Mode    | origin data  | partial data |
// | Partial2Mode    | partial data | partial data |
// | DedupMode       | origin data  | origin data  |
// |-----------------|--------------|--------------|
const (
	CompleteMode AggFunctionMode = iota
	FinalMode
	Partial1Mode
	Partial2Mode
	DedupMode
)

// ToString show the agg mode.
func (a AggFunctionMode) ToString() string {
	switch a {
	case CompleteMode:
		return "complete"
	case FinalMode:
		return "final"
	case Partial1Mode:
		return "partial1"
	case Partial2Mode:
		return "partial2"
	case DedupMode:
		return "deduplicate"
	}
	return ""
}

type aggFunction struct {
	*AggFuncDesc
}

func newAggFunc(funcName string, args []expression.Expression, hasDistinct bool) aggFunction {
	agg := &AggFuncDesc{HasDistinct: hasDistinct}
	agg.Name = funcName
	agg.Args = args
	return aggFunction{AggFuncDesc: agg}
}

// CreateContext implements Aggregation interface.
func (af *aggFunction) CreateContext(ctx expression.EvalContext) *AggEvaluateContext {
	evalCtx := &AggEvaluateContext{Ctx: ctx}
	if af.HasDistinct {
		evalCtx.DistinctChecker = createDistinctChecker(ctx)
	}
	return evalCtx
}

func (af *aggFunction) ResetContext(ctx expression.EvalContext, evalCtx *AggEvaluateContext) {
	if af.HasDistinct {
		evalCtx.DistinctChecker = createDistinctChecker(ctx)
	}
	evalCtx.Ctx = ctx
	evalCtx.Value.SetNull()
}

func (af *aggFunction) updateSum(ctx types.Context, evalCtx *AggEvaluateContext, row chunk.Row) error {
	a := af.Args[0]
	value, err := a.Eval(evalCtx.Ctx, row)
	if err != nil {
		return err
	}
	if value.IsNull() {
		return nil
	}
	if af.HasDistinct {
		d, err1 := evalCtx.DistinctChecker.Check([]types.Datum{value})
		if err1 != nil {
			return err1
		}
		if !d {
			return nil
		}
	}
	evalCtx.Value, err = calculateSum(ctx, evalCtx.Value, value)
	if err != nil {
		return err
	}
	evalCtx.Count++
	return nil
}

// NeedCount indicates whether the aggregate function should record count.
func NeedCount(name string) bool {
	return name == ast.AggFuncCount || name == ast.AggFuncAvg
}

// NeedValue indicates whether the aggregate function should record value.
func NeedValue(name string) bool {
	switch name {
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncGroupConcat, ast.AggFuncBitOr, ast.AggFuncBitAnd, ast.AggFuncBitXor, ast.AggFuncApproxPercentile:
		return true
	default:
		return false
	}
}

// IsAllFirstRow checks whether functions in `aggFuncs` are all FirstRow.
func IsAllFirstRow(aggFuncs []*AggFuncDesc) bool {
	for _, fun := range aggFuncs {
		if fun.Name != ast.AggFuncFirstRow {
			return false
		}
	}
	return true
}

// CheckAggPushDown checks whether an agg function can be pushed to storage.
func CheckAggPushDown(ctx expression.EvalContext, aggFunc *AggFuncDesc, storeType kv.StoreType) bool {
	if len(aggFunc.OrderByItems) > 0 && aggFunc.Name != ast.AggFuncGroupConcat {
		return false
	}
	if aggFunc.Name == ast.AggFuncApproxPercentile {
		return false
	}
	ret := true
	switch storeType {
	case kv.TiFlash:
		ret = CheckAggPushFlash(ctx, aggFunc)
	case kv.TiKV:
		// TiKV does not support group_concat now
		ret = aggFunc.Name != ast.AggFuncGroupConcat
	}
	if ret {
		ret = expression.IsPushDownEnabled(strings.ToLower(aggFunc.Name), storeType)
	}
	return ret
}

// CheckAggPushFlash checks whether an agg function can be pushed to flash storage.
func CheckAggPushFlash(ctx expression.EvalContext, aggFunc *AggFuncDesc) bool {
	for _, arg := range aggFunc.Args {
		if arg.GetType(ctx).GetType() == mysql.TypeDuration {
			return false
		}
	}
	switch aggFunc.Name {
	case ast.AggFuncCount, ast.AggFuncMin, ast.AggFuncMax, ast.AggFuncFirstRow, ast.AggFuncApproxCountDistinct:
		return true
	case ast.AggFuncSum, ast.AggFuncAvg, ast.AggFuncGroupConcat:
		// Now tiflash doesn't support CastJsonAsReal and CastJsonAsString.
		return aggFunc.Args[0].GetType(ctx).GetType() != mysql.TypeJSON
	}
	return false
}
