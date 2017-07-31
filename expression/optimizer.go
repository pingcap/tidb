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
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"
	"strconv"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/util/types"
)

type exprOptimizeType int

const (
	noOpt                    = 0 // do not modify
	intColCmpDecimalConstOpt = 1 // optimize for int column and decimal constant
)

// isCmpFuncName returns whether funcName is compare operator.
// For now we only support EQ, GT, GE, LT, LE for optimize
func isCmpFuncName(funcName string) bool {
	switch funcName {
	case ast.EQ, ast.GT, ast.GE, ast.LT, ast.LE:
		return true
	}
	return false
}

// canConvertToDecimal returns whether a string Constant can convert to Decimal.
func canConvertToDecimal(value Expression) bool {
	constVal, ok := value.(*Constant)
	if !ok {
		return false
	}
	val, ok := constVal.Value.GetValue().(string)
	if !ok {
		return false
	}
	_, err := strconv.ParseFloat(val, 64)
	return err == nil
}

// isIntColCmpDecimalConst checks int Column compare with a Float Number constant
// Float Number means Decimal or String which can converted to Decimal.
func isIntColCmpDecimalConst(args ...Expression) bool {
	_, lconst := args[0].(*Constant)
	_, lcolumn := args[0].(*Column)
	_, rconst := args[1].(*Constant)
	_, rcolumn := args[1].(*Column)
	ltype, rtype := args[0].GetType().ToClass(), args[1].GetType().ToClass()
	if lconst && rcolumn {
		if rtype == types.ClassInt && ltype == types.ClassDecimal {
			return true
		}
		if rtype == types.ClassInt && ltype == types.ClassString && canConvertToDecimal(args[0]) {
			return true
		}
	} else if rconst && lcolumn {
		if ltype == types.ClassInt && rtype == types.ClassDecimal {
			return true
		}
		if ltype == types.ClassInt && rtype == types.ClassString && canConvertToDecimal(args[1]) {
			return true
		}
	}
	return false
}

// getBuiltinFuncOptimizeType returns optimize type.
// For now we just support int column cmp decimal constant optimization
func getScalarFuncOptimizeType(funcName string, args ...Expression) exprOptimizeType {
	if len(args) == 2 && isCmpFuncName(funcName) {
		if isIntColCmpDecimalConst(args...) {
			return intColCmpDecimalConstOpt
		}
	}
	return noOpt
}

// optimizeBuiltinFunc do optimize by given optimize type.
func optimizeScalarFunc(optimizeType exprOptimizeType, funcName string, args ...Expression) (string, []Expression) {
	fargs := make([]Expression, len(args))
	copy(fargs, args)
	switch optimizeType {
	case intColCmpDecimalConstOpt:
		return optimizeIntColumnCmpDecimalConstant(funcName, fargs...)
	default:
		panic("We should not get there! BUG!")
	}
}

func reverseOp(funcName string) string {
	switch funcName {
	case ast.GT:
		return ast.LT
	case ast.GE:
		return ast.LE
	case ast.LT:
		return ast.GT
	case ast.LE:
		return ast.GE
	default:
		return funcName
	}
}

func convertConstant2Float64(arg *Constant) (ret float64) {
	var err error
	switch val := arg.Value.GetValue().(type) {
	case string:
		ret, err = strconv.ParseFloat(val, 64)
		if err != nil {
			panic("BUG! we should not got error")
		}
		return
	case *types.MyDecimal:
		ret, err = val.ToFloat64()
		if err != nil {
			panic("BUG! we should not got error")
		}
		return
	default:
		panic("BUG! we should not got here")
	}
}

// optimizeIntColumnCmpDecimalConstant do optimize for int column cmp decimal constant expression.
// optimization rule:
//    a > 1.1  =>  a >= 2
//    a < 1.1  =>  a <= 1
//    a >= 1.1  =>  a >= 2
//    a <= 1.1  =>  a <= 1
//    a != 1.1  =>  0 == 1
func optimizeIntColumnCmpDecimalConstant(funcName string, args ...Expression) (string, []Expression) {
	var colArg, constArg Expression
	var needReverseOp = false
	if args[0].GetType().ToClass() == types.ClassInt {
		colArg = args[0]
		constArg = args[1]
	} else {
		colArg = args[1]
		constArg = args[0]
		needReverseOp = true
	}
	constVal := convertConstant2Float64(constArg.(*Constant))
	var op string
	if needReverseOp {
		op = reverseOp(funcName)
	} else {
		op = funcName
	}
	var retType = colArg.(*Column).RetType
	var intConstArg *Constant
	if !(constVal > math.Floor(constVal)) {
		// a > 1.0
		intConstArg = &Constant{
			Value:   types.NewDatum(int64(constVal)),
			RetType: retType,
		}
		return op, []Expression{colArg, intConstArg}
	}
	var cop string
	switch op {
	case ast.LT, ast.LE:
		// a  < 1.1  =>  a <= 1
		// a <= 1.1  =>  a <= 1
		cop = ast.LE
		rval := math.Floor(constVal)
		intConstArg = &Constant{
			Value:   types.NewDatum(int64(rval)),
			RetType: retType,
		}
	case ast.GT, ast.GE:
		// a  > 1.1  =>  a >= 2
		// a >= 1.1  =>  a >= 2
		cop = ast.GE
		rval := math.Ceil(constVal)
		intConstArg = &Constant{
			Value:   types.NewDatum(int64(rval)),
			RetType: retType,
		}
	case ast.EQ:
		// 0 == 1
		cop = ast.EQ
		intConstArg = Zero
		colArg = One
	}
	return cop, []Expression{colArg, intConstArg}
}
