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
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// ExtractColumns extracts all columns from an expression.
func ExtractColumns(expr Expression) (cols []*Column) {
	switch v := expr.(type) {
	case *Column:
		return []*Column{v}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			cols = append(cols, ExtractColumns(arg)...)
		}
	}
	return
}

// ColumnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func ColumnSubstitute(expr Expression, schema *Schema, newExprs []Expression) Expression {
	switch v := expr.(type) {
	case *Column:
		id := schema.ColumnIndex(v)
		if id == -1 {
			return v
		}
		return newExprs[id].Clone()
	case *ScalarFunction:
		if v.FuncName.L == ast.Cast {
			newFunc := v.Clone().(*ScalarFunction)
			newFunc.GetArgs()[0] = ColumnSubstitute(newFunc.GetArgs()[0], schema, newExprs)
			return newFunc
		}
		newArgs := make([]Expression, 0, len(v.GetArgs()))
		for _, arg := range v.GetArgs() {
			newArgs = append(newArgs, ColumnSubstitute(arg, schema, newExprs))
		}
		fun, _ := NewFunction(v.GetCtx(), v.FuncName.L, v.RetType, newArgs...)
		return fun
	}
	return expr
}

func datumsToConstants(datums []types.Datum) []Expression {
	constants := make([]Expression, 0, len(datums))
	for _, d := range datums {
		ft := kindToFieldType(d.Kind())
		ft.Flen, ft.Decimal = types.UnspecifiedLength, types.UnspecifiedLength
		constants = append(constants, &Constant{Value: d, RetType: &ft})
	}
	return constants
}

func primitiveValsToConstants(args []interface{}) []Expression {
	cons := datumsToConstants(types.MakeDatums(args...))
	for i, arg := range args {
		types.DefaultTypeForValue(arg, cons[i].GetType())
	}
	return cons
}

func kindToFieldType(kind byte) types.FieldType {
	ft := types.FieldType{}
	switch kind {
	case types.KindNull:
		ft.Tp = mysql.TypeNull
	case types.KindInt64:
		ft.Tp = mysql.TypeLonglong
	case types.KindUint64:
		ft.Tp = mysql.TypeLonglong
		ft.Flag |= mysql.UnsignedFlag
	case types.KindMinNotNull:
		ft.Tp = mysql.TypeLonglong
	case types.KindMaxValue:
		ft.Tp = mysql.TypeLonglong
	case types.KindFloat32:
		ft.Tp = mysql.TypeDouble
	case types.KindFloat64:
		ft.Tp = mysql.TypeDouble
	case types.KindString:
		ft.Tp = mysql.TypeVarString
	case types.KindBytes:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlEnum:
		ft.Tp = mysql.TypeEnum
	case types.KindMysqlSet:
		ft.Tp = mysql.TypeSet
	case types.KindInterface:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlDecimal:
		ft.Tp = mysql.TypeNewDecimal
	case types.KindMysqlDuration:
		ft.Tp = mysql.TypeDuration
	case types.KindMysqlTime:
		ft.Tp = mysql.TypeDatetime
	case types.KindBinaryLiteral:
		ft.Tp = mysql.TypeVarString
	case types.KindMysqlBit:
		ft.Tp = mysql.TypeBit
	}
	return ft
}

// getValidPrefix gets a prefix of string which can parsed to a number with base. the minimum base is 2 and the maximum is 36.
func getValidPrefix(s string, base int64) string {
	var (
		validLen int
		upper    rune
	)
	switch {
	case base >= 2 && base <= 9:
		upper = rune('0' + base)
	case base <= 36:
		upper = rune('A' + base - 10)
	default:
		return ""
	}
Loop:
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		switch {
		case unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c):
			c = unicode.ToUpper(c)
			if c < upper {
				validLen = i + 1
			} else {
				break Loop
			}
		case c == '+' || c == '-':
			if i != 0 {
				break Loop
			}
		default:
			break Loop
		}
	}
	if validLen > 1 && s[0] == '+' {
		return s[1:validLen]
	}
	return s[:validLen]
}

// SubstituteCorCol2Constant will substitute correlated column to constant value which it contains.
// If the args of one scalar function are all constant, we will substitute it to constant.
func SubstituteCorCol2Constant(expr Expression) (Expression, error) {
	switch x := expr.(type) {
	case *ScalarFunction:
		allConstant := true
		newArgs := make([]Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg, err := SubstituteCorCol2Constant(arg)
			if err != nil {
				return nil, errors.Trace(err)
			}
			_, ok := newArg.(*Constant)
			newArgs = append(newArgs, newArg)
			allConstant = allConstant && ok
		}
		if allConstant {
			val, err := x.Eval(nil)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return &Constant{Value: val, RetType: x.GetType()}, nil
		}
		var newSf Expression
		if x.FuncName.L == ast.Cast {
			newSf = NewCastFunc(x.RetType, newArgs[0], x.GetCtx())
		} else {
			newSf, _ = NewFunction(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
		}
		return newSf, nil
	case *CorrelatedColumn:
		return &Constant{Value: *x.Data, RetType: x.GetType()}, nil
	default:
		return x.Clone(), nil
	}
}

// ConvertCol2CorCol will convert the column in the condition which can be found in outerSchema to a correlated column whose
// Column is this column. And please make sure the outerSchema.Columns[i].Equal(corCols[i].Column)) holds when you call this.
func ConvertCol2CorCol(cond Expression, corCols []*CorrelatedColumn, outerSchema *Schema) Expression {
	switch x := cond.(type) {
	case *ScalarFunction:
		newArgs := make([]Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg := ConvertCol2CorCol(arg, corCols, outerSchema)
			newArgs = append(newArgs, newArg)
		}
		var newSf Expression
		if x.FuncName.L == ast.Cast {
			newSf = NewCastFunc(x.RetType, newArgs[0], x.GetCtx())
		} else {
			newSf, _ = NewFunction(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
		}
		return newSf
	case *Column:
		if pos := outerSchema.ColumnIndex(x); pos >= 0 {
			return corCols[pos]
		}
	}
	return cond
}

// timeZone2Duration converts timezone whose format should satisfy the regular condition
// `(^(+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^+13:00$)` to time.Duration.
func timeZone2Duration(tz string) time.Duration {
	sign := 1
	if strings.HasPrefix(tz, "-") {
		sign = -1
	}

	i := strings.Index(tz, ":")
	h, _ := strconv.Atoi(tz[1:i])
	m, _ := strconv.Atoi(tz[i+1:])
	return time.Duration(sign) * (time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
}

var oppositeOp = map[string]string{
	ast.LT: ast.GE,
	ast.GE: ast.LT,
	ast.GT: ast.LE,
	ast.LE: ast.GT,
	ast.EQ: ast.NE,
	ast.NE: ast.EQ,
}

// a op b is equal to b symmetricOp a
var symmetricOp = map[opcode.Op]opcode.Op{
	opcode.LT:     opcode.GT,
	opcode.GE:     opcode.LE,
	opcode.GT:     opcode.LT,
	opcode.LE:     opcode.GE,
	opcode.EQ:     opcode.EQ,
	opcode.NE:     opcode.NE,
	opcode.NullEQ: opcode.NullEQ,
}

// PushDownNot pushes the `not` function down to the expression's arguments.
func PushDownNot(expr Expression, not bool, ctx context.Context) Expression {
	if f, ok := expr.(*ScalarFunction); ok {
		switch f.FuncName.L {
		case ast.UnaryNot:
			return PushDownNot(f.GetArgs()[0], !not, f.GetCtx())
		case ast.LT, ast.GE, ast.GT, ast.LE, ast.EQ, ast.NE:
			if not {
				nf, _ := NewFunction(f.GetCtx(), oppositeOp[f.FuncName.L], f.GetType(), f.GetArgs()...)
				return nf
			}
			for i, arg := range f.GetArgs() {
				f.GetArgs()[i] = PushDownNot(arg, false, f.GetCtx())
			}
			return f
		case ast.LogicAnd:
			if not {
				args := f.GetArgs()
				for i, a := range args {
					args[i] = PushDownNot(a, true, f.GetCtx())
				}
				nf, _ := NewFunction(f.GetCtx(), ast.LogicOr, f.GetType(), args...)
				return nf
			}
			for i, arg := range f.GetArgs() {
				f.GetArgs()[i] = PushDownNot(arg, false, f.GetCtx())
			}
			return f
		case ast.LogicOr:
			if not {
				args := f.GetArgs()
				for i, a := range args {
					args[i] = PushDownNot(a, true, f.GetCtx())
				}
				nf, _ := NewFunction(f.GetCtx(), ast.LogicAnd, f.GetType(), args...)
				return nf
			}
			for i, arg := range f.GetArgs() {
				f.GetArgs()[i] = PushDownNot(arg, false, f.GetCtx())
			}
			return f
		}
	}
	if not {
		expr, _ = NewFunction(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), expr)
	}
	return expr
}
