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
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
)

var (
	// UnaryOps is a unary operation
	UnaryOps types.OpFuncMap = make(types.OpFuncMap)

	// Not is a Not operation
	Not = types.NewOp(opcode.Not, 1, 1, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		// evaluation
		if len(v) != 1 {
			panic("error param numbers")
		}
		a := v[0]
		e := parser_driver.ValueExpr{}
		boolA := util.ConvertToBoolOrNull(a)
		if boolA == -1 {
			e.SetValue(nil)
			return e, nil
		}
		if boolA == 1 {
			e.SetValue(false)
			return e, nil
		}
		e.SetValue(true)
		return e, nil
	}, func(args ...uint64) (uint64, bool, error) {
		// checking args validate?
		if len(args) != 1 {
			panic("require only one param")
		}
		arg := args[0]
		if arg&^(types.TypeDatetimeLikeArg|types.TypeNumberLikeArg) == 0 {
			return types.TypeIntArg | types.TypeFloatArg, false, nil
		}
		if arg&^(types.TypeNonFormattedStringArg) == 0 {
			return types.TypeIntArg | types.TypeFloatArg, true, nil
		}
		panic("unreachable")
	}, func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
		// generate op node and call cb to generate its child node
		val := parser_driver.ValueExpr{}
		op, ok := this.(*types.BaseOpFunc)
		if !ok {
			panic("should can transfer to BaseOpFunc")
		}

		argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
		if err != nil {
			return nil, val, errors.Trace(err)
		}
		if len(argList) == 0 {
			return nil, val,
				fmt.Errorf("cannot find valid param for type(%d) returned", ret)
		}
		firstArg := argList[rand.Intn(len(argList))][0]

		node := ast.UnaryOperationExpr{}
		node.Op = opcode.Not
		node.V, val, err = cb(firstArg)
		if err != nil {
			return nil, val, errors.Trace(err)
		}
		evalResult, err := op.Eval(val)
		if err != nil {
			return nil, val, errors.Trace(err)
		}
		return &node, evalResult, nil
	})
)
