// Copyright 2017 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestExpressionSemanticEqual(t *testing.T) {
	ctx := mock.NewContext()
	a := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}
	b := &Column{
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLong),
	}
	// order sensitive cases
	// a < b; b > a
	sf1 := newFunction(ast.LT, a, b)
	sf2 := newFunction(ast.GT, b, a)
	require.True(t, ExpressionsSemanticEqual(ctx, sf1, sf2))

	// a > b; b < a
	sf3 := newFunction(ast.GT, a, b)
	sf4 := newFunction(ast.LT, b, a)
	require.True(t, ExpressionsSemanticEqual(ctx, sf3, sf4))

	// a<=b; b>=a
	sf5 := newFunction(ast.LE, a, b)
	sf6 := newFunction(ast.GE, b, a)
	require.True(t, ExpressionsSemanticEqual(ctx, sf5, sf6))

	// a>=b; b<=a
	sf7 := newFunction(ast.GE, a, b)
	sf8 := newFunction(ast.LE, b, a)
	require.True(t, ExpressionsSemanticEqual(ctx, sf7, sf8))

	// not(a<b); a >= b
	sf9 := newFunction(ast.UnaryNot, sf1)
	require.True(t, ExpressionsSemanticEqual(ctx, sf9, sf7))

	// a < b; not(a>=b)
	sf10 := newFunction(ast.UnaryNot, sf7)
	require.True(t, ExpressionsSemanticEqual(ctx, sf1, sf10))

	// order insensitive cases
	// a + b; b + a
	p1 := newFunction(ast.Plus, a, b)
	p2 := newFunction(ast.Plus, b, a)
	require.True(t, ExpressionsSemanticEqual(ctx, p1, p2))

	// a * b; b * a
	m1 := newFunction(ast.Mul, a, b)
	m2 := newFunction(ast.Mul, b, a)
	require.True(t, ExpressionsSemanticEqual(ctx, m1, m2))

	// a = b; b = a
	e1 := newFunction(ast.EQ, a, b)
	e2 := newFunction(ast.EQ, b, a)
	require.True(t, ExpressionsSemanticEqual(ctx, e1, e2))

	// a = b AND b + a; a + b AND b = a
	a1 := newFunction(ast.LogicAnd, e1, p2)
	a2 := newFunction(ast.LogicAnd, p1, e2)
	require.True(t, ExpressionsSemanticEqual(ctx, a1, a2))

	// a * b OR a + b;  b + a OR b * a
	o1 := newFunction(ast.LogicOr, m1, p1)
	o2 := newFunction(ast.LogicOr, p2, m2)
	require.True(t, ExpressionsSemanticEqual(ctx, o1, o2))
}

func TestScalarFunction(t *testing.T) {
	ctx := mock.NewContext()
	a := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	sf := newFunction(ast.LT, a, NewOne())
	res, err := sf.MarshalJSON()
	require.NoError(t, err)
	require.EqualValues(t, []byte{0x22, 0x6c, 0x74, 0x28, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x23, 0x31, 0x2c, 0x20, 0x31, 0x29, 0x22}, res)
	require.False(t, sf.IsCorrelated())
	require.False(t, sf.ConstItem(ctx.GetSessionVars().StmtCtx))
	require.True(t, sf.Decorrelate(nil).Equal(ctx, sf))
	require.EqualValues(t, []byte{0x3, 0x4, 0x6c, 0x74, 0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x5, 0xbf, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, sf.HashCode(sc))

	sf = NewValuesFunc(ctx, 0, types.NewFieldType(mysql.TypeLonglong))
	newSf, ok := sf.Clone().(*ScalarFunction)
	require.True(t, ok)
	require.Equal(t, "values", newSf.FuncName.O)
	require.Equal(t, mysql.TypeLonglong, newSf.RetType.GetType())
	require.Equal(t, sf.Coercibility(), newSf.Coercibility())
	require.Equal(t, sf.Repertoire(), newSf.Repertoire())
	_, ok = newSf.Function.(*builtinValuesIntSig)
	require.True(t, ok)
}

func TestIssue23309(t *testing.T) {
	a := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}

	a.RetType.SetFlag(a.RetType.GetFlag() | mysql.NotNullFlag)
	null := NewNull()
	null.RetType = types.NewFieldType(mysql.TypeNull)
	sf, _ := newFunction(ast.NE, a, null).(*ScalarFunction)
	v, err := sf.GetArgs()[1].Eval(chunk.Row{})
	require.NoError(t, err)
	require.True(t, v.IsNull())
	require.False(t, mysql.HasNotNullFlag(sf.GetArgs()[1].GetType().GetFlag()))
}

func TestScalarFuncs2Exprs(t *testing.T) {
	ctx := mock.NewContext()
	a := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}
	sf0, _ := newFunction(ast.LT, a, NewZero()).(*ScalarFunction)
	sf1, _ := newFunction(ast.LT, a, NewOne()).(*ScalarFunction)

	funcs := []*ScalarFunction{sf0, sf1}
	exprs := ScalarFuncs2Exprs(funcs)
	for i := range exprs {
		require.True(t, exprs[i].Equal(ctx, funcs[i]))
	}
}
