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

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestExpressionSemanticEqual(t *testing.T) {
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
	sf1 := newFunctionWithMockCtx(ast.LT, a, b)
	sf2 := newFunctionWithMockCtx(ast.GT, b, a)
	require.True(t, ExpressionsSemanticEqual(sf1, sf2))

	// a > b; b < a
	sf3 := newFunctionWithMockCtx(ast.GT, a, b)
	sf4 := newFunctionWithMockCtx(ast.LT, b, a)
	require.True(t, ExpressionsSemanticEqual(sf3, sf4))

	// a<=b; b>=a
	sf5 := newFunctionWithMockCtx(ast.LE, a, b)
	sf6 := newFunctionWithMockCtx(ast.GE, b, a)
	require.True(t, ExpressionsSemanticEqual(sf5, sf6))

	// a>=b; b<=a
	sf7 := newFunctionWithMockCtx(ast.GE, a, b)
	sf8 := newFunctionWithMockCtx(ast.LE, b, a)
	require.True(t, ExpressionsSemanticEqual(sf7, sf8))

	// not(a<b); a >= b
	sf9 := newFunctionWithMockCtx(ast.UnaryNot, sf1)
	require.True(t, ExpressionsSemanticEqual(sf9, sf7))

	// a < b; not(a>=b)
	sf10 := newFunctionWithMockCtx(ast.UnaryNot, sf7)
	require.True(t, ExpressionsSemanticEqual(sf1, sf10))

	// order insensitive cases
	// a + b; b + a
	p1 := newFunctionWithMockCtx(ast.Plus, a, b)
	p2 := newFunctionWithMockCtx(ast.Plus, b, a)
	require.True(t, ExpressionsSemanticEqual(p1, p2))

	// a * b; b * a
	m1 := newFunctionWithMockCtx(ast.Mul, a, b)
	m2 := newFunctionWithMockCtx(ast.Mul, b, a)
	require.True(t, ExpressionsSemanticEqual(m1, m2))

	// a = b; b = a
	e1 := newFunctionWithMockCtx(ast.EQ, a, b)
	e2 := newFunctionWithMockCtx(ast.EQ, b, a)
	require.True(t, ExpressionsSemanticEqual(e1, e2))

	// a = b AND b + a; a + b AND b = a
	a1 := newFunctionWithMockCtx(ast.LogicAnd, e1, p2)
	a2 := newFunctionWithMockCtx(ast.LogicAnd, p1, e2)
	require.True(t, ExpressionsSemanticEqual(a1, a2))

	// a * b OR a + b;  b + a OR b * a
	o1 := newFunctionWithMockCtx(ast.LogicOr, m1, p1)
	o2 := newFunctionWithMockCtx(ast.LogicOr, p2, m2)
	require.True(t, ExpressionsSemanticEqual(o1, o2))
}

func TestScalarFunction(t *testing.T) {
	ctx := mock.NewContext()
	a := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}

	sf := newFunctionWithMockCtx(ast.LT, a, NewOne())
	res, err := sf.MarshalJSON()
	require.NoError(t, err)
	require.EqualValues(t, []byte{0x22, 0x6c, 0x74, 0x28, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x23, 0x31, 0x2c, 0x20, 0x31, 0x29, 0x22}, res)
	require.False(t, sf.IsCorrelated())
	require.Equal(t, ConstNone, sf.ConstLevel())
	require.True(t, sf.Decorrelate(nil).Equal(ctx, sf))
	require.EqualValues(t, []byte{0x3, 0x4, 0x6c, 0x74, 0x1, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x5, 0xbf, 0xf0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}, sf.HashCode())

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
	sf, _ := newFunctionWithMockCtx(ast.NE, a, null).(*ScalarFunction)
	v, err := sf.GetArgs()[1].Eval(mock.NewContext(), chunk.Row{})
	require.NoError(t, err)
	require.True(t, v.IsNull())

	ctx := createContext(t)
	require.False(t, mysql.HasNotNullFlag(sf.GetArgs()[1].GetType(ctx).GetFlag()))
}

func TestScalarFuncs2Exprs(t *testing.T) {
	ctx := mock.NewContext()
	a := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeDouble),
	}
	sf0, _ := newFunctionWithMockCtx(ast.LT, a, NewZero()).(*ScalarFunction)
	sf1, _ := newFunctionWithMockCtx(ast.LT, a, NewOne()).(*ScalarFunction)

	funcs := []*ScalarFunction{sf0, sf1}
	exprs := ScalarFuncs2Exprs(funcs)
	for i := range exprs {
		require.True(t, exprs[i].Equal(ctx, funcs[i]))
	}
}
