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
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/types"
	"github.com/pingcap/tidb/tools/go-sqlancer/pkg/util"
	tidb_types "github.com/pingcap/tidb/types"
	parser_driver "github.com/pingcap/tidb/types/parser_driver"
	"go.uber.org/zap"
)

var (
	// BinaryOps is binary operation
	BinaryOps = make(types.OpFuncMap)

	// comparisionRetTypeGen stands for a comparison function that results a int value
	comparisionRetTypeGen = func(args ...uint64) (uint64, bool, error) {
		if len(args) != 2 {
			panic("require two params")
		}
		a, b := args[0], args[1]
		resultType := types.TypeIntArg | types.TypeFloatArg
		// because binary ops almost reflexive, we can just swap a and b then judge again
		for i := 0; i < 2; i++ {
			switch a {
			case types.TypeIntArg, types.TypeFloatArg:
				if b&^types.TypeNonFormattedStringArg == 0 {
					return resultType, true, nil
				}
				if b&^(types.TypeNumberLikeArg|types.TypeDatetimeLikeStringArg) == 0 {
					return resultType, false, nil
				}
			case types.TypeNonFormattedStringArg:
				if b&^types.TypeStringArg == 0 {
					return resultType, false, nil
				}
				if b&^types.TypeDatetimeArg == 0 {
					// return ERROR 1525
					return 0, false, errors.New("invalid type")
				}
				if b&^types.TypeNumberLikeStringArg == 0 {
					return resultType, false, nil
				}
			case types.TypeNumberLikeStringArg:
				if b&^(types.TypeNumberLikeStringArg|types.TypeDatetimeLikeStringArg) == 0 {
					return resultType, false, nil
				}
				if b&^types.TypeDatetimeArg == 0 {
					return 0, false, errors.New("invalid type")
				}
			case types.TypeDatetimeArg:
				if b&^types.TypeDatetimeLikeArg == 0 {
					return resultType, false, nil
				}
			case types.TypeDatetimeLikeStringArg:
				if b&^types.TypeDatetimeLikeStringArg == 0 {
					return resultType, false, nil
				}
			}
			a, b = b, a
		}
		log.L().Error("a and b are unexpected type", zap.Uint64("a", a), zap.Uint64("b", b))
		panic("unreachable")
	}

	defaultBinaryOpExprGen = func(opCode opcode.Op) types.TypedExprNodeGenSel {
		return func(cb types.TypedExprNodeGen, this types.OpFuncEval, ret uint64) (ast.ExprNode, parser_driver.ValueExpr, error) {
			// generate op node and call cb to generate its child node
			var valLeft, valRight parser_driver.ValueExpr
			op, ok := this.(*types.BaseOpFunc)
			if !ok {
				panic("should can transfer to BaseOpFunc")
			}

			argList, err := op.GetArgTable().Filter([]*uint64{nil}, &ret)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			if len(argList) == 0 {
				return nil, parser_driver.ValueExpr{},
					fmt.Errorf("cannot find valid param for type(%d) returned", ret)
			}
			args := argList[rand.Intn(len(argList))]
			firstArg := args[0]
			secondArg := args[1]

			node := ast.BinaryOperationExpr{}
			node.Op = opCode
			node.L, valLeft, err = cb(firstArg)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			node.R, valRight, err = cb(secondArg)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			evalResult, err := op.Eval(valLeft, valRight)
			if err != nil {
				return nil, parser_driver.ValueExpr{}, errors.Trace(err)
			}
			return &node, evalResult, nil
		}
	}

	// Gt is Gt operation
	Gt = types.NewOp(opcode.GT, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.CompareValueExpr(a, b) > 0)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.GT))

	// Lt is Lt operation
	Lt = types.NewOp(opcode.LT, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.CompareValueExpr(a, b) < 0)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.LT))

	// Ne is Ne operation
	Ne = types.NewOp(opcode.NE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.CompareValueExpr(a, b) != 0)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.NE))

	// Eq is Eq operation
	Eq = types.NewOp(opcode.EQ, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.CompareValueExpr(a, b) == 0)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.EQ))

	// Ge is Ge operation
	Ge = types.NewOp(opcode.GE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.CompareValueExpr(a, b) >= 0)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.GE))

	// Le is Le operation
	Le = types.NewOp(opcode.LE, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.CompareValueExpr(a, b) <= 0)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.LE))

	// LogicXor is LogicXor operation
	LogicXor = types.NewOp(opcode.LogicXor, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		if a.Kind() == tidb_types.KindNull || b.Kind() == tidb_types.KindNull {
			e.SetNull()
			return e, nil
		}
		e.SetValue(util.ConvertToBoolOrNull(a) != util.ConvertToBoolOrNull(b))
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.LogicXor))

	// LogicAnd is LogicAnd operation
	LogicAnd = types.NewOp(opcode.LogicAnd, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		boolA := util.ConvertToBoolOrNull(a)
		boolB := util.ConvertToBoolOrNull(b)
		if boolA*boolB == 0 {
			e.SetValue(false)
			return e, nil
		}
		if boolA == -1 || boolB == -1 {
			e.SetValue(nil)
			return e, nil
		}
		e.SetValue(true)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.LogicAnd))

	// LogicOr is LogicOr operation
	LogicOr = types.NewOp(opcode.LogicOr, 2, 2, func(v ...parser_driver.ValueExpr) (parser_driver.ValueExpr, error) {
		if len(v) != 2 {
			panic("error param numbers")
		}
		a, b := v[0], v[1]
		e := parser_driver.ValueExpr{}
		boolA := util.ConvertToBoolOrNull(a)
		boolB := util.ConvertToBoolOrNull(b)
		if boolA == 1 || boolB == 1 {
			e.SetValue(true)
			return e, nil
		}
		if boolA == -1 || boolB == -1 {
			e.SetValue(nil)
			return e, nil
		}
		e.SetValue(false)
		return e, nil
	}, comparisionRetTypeGen, defaultBinaryOpExprGen(opcode.LogicOr))
)
