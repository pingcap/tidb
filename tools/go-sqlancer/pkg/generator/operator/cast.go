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

package operator

import (
	"fmt"
	"math/rand"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	// https://github.com/pingcap/tidb/blame/9ef4858ea525b8ffc1c7a7735b61461e7ad6a353/expression/constant.go#L219-L235
	castSigned = types.NewFn("CAST", 1, 1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 1 {
			panic("error param numbers")
		}
		value := v[0]
		e := parser_driver.ValueExpr{}
		if value.IsNull() {
			return value, nil
		}
		// we need set InSelectStmt to be true to disallow floatStr "0.6" round to 1 when cast signed
		statementContext := &stmtctx.StatementContext{AllowInvalidDate: true, InSelectStmt: true}
		statementContext.IgnoreTruncate.Store(true)
		res, err := value.ToInt64(statementContext)
		if err != nil {
			return e, errors.Trace(err)
		}
		e.SetInt64(res)
		return e, nil
	}, func(args ...uint64) (retType uint64, warnings bool, err error) {
		if len(args) != 1 {
			panic("require one param")
		}
		// we only accept int, float, string for the sake of simplicity
		switch args[0] {
		case types.TypeIntArg, types.TypeFloatArg:
			return types.TypeIntArg | types.TypeFloatArg, false, nil
		case types.TypeNonFormattedStringArg, types.TypeDatetimeLikeStringArg, types.TypeNumberLikeStringArg:
			return types.TypeIntArg | types.TypeFloatArg, true, nil
		default:
			return 0, false, errors.New("invalid type")
		}
	}, func(nodeGen types.TypedExprNodeGen, this types.OpFuncEval, retType uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		op := this.(*types.BaseOpFunc)
		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &retType)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, parser_driver.ValueExpr{},
				fmt.Errorf("cannot find valid param for type(%d) returned", retType)
		}
		args := argList[rand.Intn(len(argList))]
		expr, exprValue, err := nodeGen(args[0])
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		value, err := op.Eval(exprValue)
		if err != nil {
			return nil, parser_driver.ValueExpr{}, errors.Trace(err)
		}
		node := &ast.FuncCastExpr{
			Expr:         expr,
			Tp:           tidb_types.NewFieldType(mysql.TypeLonglong),
			FunctionType: ast.CastFunction,
		}
		return node, value, nil
	})
)

func init() {
	for _, f := range []types.OpFuncEval{castSigned} {
		util.RegisterToOpFnIndex(f)
	}
	// DONOT op on non-date format types
	for _, f := range []*types.Op{Lt, Gt, Le, Ge, Ne, Eq, LogicXor, LogicAnd, LogicOr} {
		BinaryOps.Add(f)
		util.RegisterToOpFnIndex(f)
	}
	UnaryOps.Add(Not)
	util.RegisterToOpFnIndex(Not)
	for _, f := range []types.OpFuncEval{between, in, isNull, nullEq, strCmp} {
		util.RegisterToOpFnIndex(f)
	}
	for _, f := range []types.OpFuncEval{Case, If, IfNull, NullIf} {
		util.RegisterToOpFnIndex(f)
	}
}
