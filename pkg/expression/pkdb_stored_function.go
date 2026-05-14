// Copyright 2026 PingCAP, Inc.
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

package expression

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression/sessionexpr"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
)

// StoredFuncClass is the class for stored function.
type StoredFuncClass struct {
	Name    [2]string
	RetType *types.FieldType
}

var (
	// Eval4StoredFunc is used to evaluate stored function.
	Eval4StoredFunc func(sctx sessionctx.Context, stmt any, node *ast.CallStmt) (*types.Datum, error)
	// GetCallStmt4StoredFuncExpr is used to get call statement plan for stored function expression.
	GetCallStmt4StoredFuncExpr func(ctx context.Context, sctx sessionctx.Context, node *ast.CallStmt) (any, error)
)

func (s *StoredFuncClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if !variable.TiDBEnableProcedureValue.Load() {
		return nil, exeerrors.ErrProcedureDisabled
	}
	funcName := s.Name[0] + "." + s.Name[1]
	bf, err := newBaseBuiltinFunc(ctx, funcName, args, s.RetType.Clone())
	if err != nil {
		return nil, err
	}
	callStmt := &ast.CallStmt{Procedure: &ast.FuncCallExpr{}, IsFunction: true}
	callStmt.Procedure.Schema = pmodel.NewCIStr(s.Name[0])
	callStmt.Procedure.FnName = pmodel.NewCIStr(s.Name[1])
	callStmt.Procedure.Args = make([]ast.ExprNode, len(args))
	ectx := ctx.GetEvalCtx().(*sessionexpr.EvalContext)
	callPlan, err := GetCallStmt4StoredFuncExpr(context.Background(), ectx.Sctx(), callStmt)
	if err != nil {
		return nil, err
	}

	// Stored function expressions should not be cached.
	ctx.SetSkipPlanCache("stored function should not be cached")
	sig := &storedFuncSig{
		baseBuiltinFunc: bf,
		sctx:            ectx.Sctx(),
		callPlan:        callPlan,
		callStmt:        callStmt,
	}
	return sig, nil
}

func (s *StoredFuncClass) verifyArgsByCount(_ int) error {
	return nil
}

type storedFuncSig struct {
	baseBuiltinFunc

	sctx     sessionctx.Context
	callPlan any
	callStmt *ast.CallStmt
}

func (b *storedFuncSig) Clone() builtinFunc {
	newSig := &storedFuncSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.sctx = b.sctx
	newSig.callPlan = nil
	if b.callStmt != nil && b.callStmt.Procedure != nil {
		callStmt := &ast.CallStmt{Procedure: &ast.FuncCallExpr{}, IsFunction: b.callStmt.IsFunction}
		callStmt.Procedure.Tp = b.callStmt.Procedure.Tp
		callStmt.Procedure.Schema = b.callStmt.Procedure.Schema
		callStmt.Procedure.FnName = b.callStmt.Procedure.FnName
		callStmt.Procedure.Args = make([]ast.ExprNode, len(b.args))
		newSig.callStmt = callStmt
	} else {
		newSig.callStmt = b.callStmt
	}
	return newSig
}

func (b *storedFuncSig) setArgs(ctx EvalContext, row chunk.Row) error {
	if !variable.TiDBEnableProcedureValue.Load() {
		return exeerrors.ErrProcedureDisabled
	}
	if b.callPlan == nil {
		callPlan, err := GetCallStmt4StoredFuncExpr(context.Background(), b.sctx, b.callStmt)
		if err != nil {
			return err
		}
		b.callPlan = callPlan
	}

	b.callStmt.Procedure.Args = make([]ast.ExprNode, len(b.args))
	for i, arg := range b.args {
		d, err := arg.Eval(ctx, row)
		if err != nil {
			return err
		}
		b.callStmt.Procedure.Args[i] = &driver.ValueExpr{
			Datum: d,
		}
	}
	return nil
}

func (b *storedFuncSig) evalDatum(ctx EvalContext, row chunk.Row) (*types.Datum, error) {
	vars := b.sctx.GetSessionVars()
	vars.LockStoredRoutineEval()
	defer vars.UnlockStoredRoutineEval()

	if err := b.setArgs(ctx, row); err != nil {
		return nil, err
	}
	return Eval4StoredFunc(b.sctx, b.callPlan, b.callStmt)
}

func (b *storedFuncSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	d, err := b.evalDatum(ctx, row)
	if err != nil {
		return "", false, err
	}
	if d.IsNull() {
		return "", true, nil
	}
	return d.GetString(), false, nil
}

func (b *storedFuncSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	d, err := b.evalDatum(ctx, row)
	if err != nil {
		return 0, false, err
	}
	if d.IsNull() {
		return 0, true, nil
	}
	return d.GetInt64(), false, nil
}

func (b *storedFuncSig) evalReal(ctx EvalContext, row chunk.Row) (float64, bool, error) {
	d, err := b.evalDatum(ctx, row)
	if err != nil {
		return 0, false, err
	}
	if d.IsNull() {
		return 0, true, nil
	}
	return d.GetFloat64(), false, nil
}

func (b *storedFuncSig) evalDecimal(ctx EvalContext, row chunk.Row) (*types.MyDecimal, bool, error) {
	d, err := b.evalDatum(ctx, row)
	if err != nil {
		return nil, false, err
	}
	if d.IsNull() {
		return nil, true, nil
	}
	return d.GetMysqlDecimal(), false, nil
}

func (b *storedFuncSig) evalTime(ctx EvalContext, row chunk.Row) (types.Time, bool, error) {
	d, err := b.evalDatum(ctx, row)
	if err != nil {
		return types.ZeroTime, false, err
	}
	if d.IsNull() {
		return types.ZeroTime, true, nil
	}
	return d.GetMysqlTime(), false, nil
}

func (b *storedFuncSig) evalDuration(ctx EvalContext, row chunk.Row) (val types.Duration, isNull bool, err error) {
	d, err := b.evalDatum(ctx, row)
	if err != nil {
		return types.Duration{}, false, err
	}
	if d.IsNull() {
		return types.Duration{}, true, nil
	}
	return d.GetMysqlDuration(), false, nil
}

func (b *storedFuncSig) evalJSON(ctx EvalContext, row chunk.Row) (val types.BinaryJSON, isNull bool, err error) {
	d, err := b.evalDatum(ctx, row)
	if err != nil {
		return types.BinaryJSON{}, false, err
	}
	if d.IsNull() {
		return types.BinaryJSON{}, true, nil
	}
	return d.GetMysqlJSON(), false, nil
}
