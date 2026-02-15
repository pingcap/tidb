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
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestDeepCopyExprNoSharedColumnFields(t *testing.T) {
	col := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		VirtualExpr: &Constant{
			Value:   types.NewBytesDatum([]byte("abc")),
			RetType: types.NewFieldType(mysql.TypeString),
		},
	}
	cloned := DeepCopyExpr(col).(*Column)
	require.NotSame(t, col, cloned)
	require.NotSame(t, col.RetType, cloned.RetType)
	require.NotSame(t, col.VirtualExpr, cloned.VirtualExpr)

	cloned.RetType.SetType(mysql.TypeDouble)
	cloned.VirtualExpr.(*Constant).Value.GetBytes()[0] = 'z'
	require.Equal(t, mysql.TypeLonglong, col.RetType.GetType())
	require.Equal(t, byte('a'), col.VirtualExpr.(*Constant).Value.GetBytes()[0])
}

func TestDeepCopyExprNoSharedCorrelatedData(t *testing.T) {
	data := types.NewBytesDatum([]byte("xyz"))
	col := &CorrelatedColumn{
		Column: Column{
			UniqueID: 2,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		},
		Data: &data,
	}
	cloned := DeepCopyExpr(col).(*CorrelatedColumn)
	require.NotSame(t, col, cloned)
	require.NotSame(t, col.RetType, cloned.RetType)
	require.NotSame(t, col.Data, cloned.Data)

	cloned.Data.GetBytes()[0] = 'q'
	require.Equal(t, byte('x'), col.Data.GetBytes()[0])
}

func TestDeepCopyExprNoSharedScalarFunctionFields(t *testing.T) {
	ctx := mock.NewContext()
	sf, ok := NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeLonglong),
		&Column{
			UniqueID: 1,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			VirtualExpr: &Constant{
				Value:   types.NewBytesDatum([]byte("abc")),
				RetType: types.NewFieldType(mysql.TypeString),
			},
		},
		NewOne(),
	).(*ScalarFunction)
	require.True(t, ok)

	cloned := DeepCopyExpr(sf).(*ScalarFunction)
	require.NotSame(t, sf, cloned)
	require.NotSame(t, sf.RetType, cloned.RetType)
	require.NotSame(t, sf.GetArgs()[0], cloned.GetArgs()[0])

	clonedCol := cloned.GetArgs()[0].(*Column)
	clonedCol.RetType.SetType(mysql.TypeDouble)
	clonedCol.VirtualExpr.(*Constant).Value.GetBytes()[0] = 'z'
	require.Equal(t, mysql.TypeLonglong, sf.GetArgs()[0].(*Column).RetType.GetType())
	require.Equal(t, byte('a'), sf.GetArgs()[0].(*Column).VirtualExpr.(*Constant).Value.GetBytes()[0])
}

func TestDeepCopySchemaNoSharedFields(t *testing.T) {
	col := &Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		VirtualExpr: &Constant{
			Value:   types.NewBytesDatum([]byte("abc")),
			RetType: types.NewFieldType(mysql.TypeString),
		},
	}
	schema := NewSchema(col)
	schema.SetKeys([]KeyInfo{{col}})
	schema.SetUniqueKeys([]KeyInfo{{col}})

	cloned := DeepCopySchema(schema)
	require.NotSame(t, schema, cloned)
	require.NotSame(t, schema.Columns[0], cloned.Columns[0])
	require.NotSame(t, schema.Columns[0].RetType, cloned.Columns[0].RetType)
	require.NotSame(t, schema.Columns[0].VirtualExpr, cloned.Columns[0].VirtualExpr)
	require.NotSame(t, schema.PKOrUK[0][0], cloned.PKOrUK[0][0])
	require.NotSame(t, schema.NullableUK[0][0], cloned.NullableUK[0][0])

	cloned.Columns[0].RetType.SetType(mysql.TypeDouble)
	cloned.Columns[0].VirtualExpr.(*Constant).Value.GetBytes()[0] = 'z'
	require.Equal(t, mysql.TypeLonglong, schema.Columns[0].RetType.GetType())
	require.Equal(t, byte('a'), schema.Columns[0].VirtualExpr.(*Constant).Value.GetBytes()[0])
}

func TestConstantCloneNoSharedMutableFields(t *testing.T) {
	con := &Constant{
		Value:   types.NewBytesDatum([]byte("abc")),
		RetType: types.NewFieldType(mysql.TypeEnum),
		DeferredExpr: &Constant{
			Value:   types.NewBytesDatum([]byte("def")),
			RetType: types.NewFieldType(mysql.TypeString),
		},
	}
	con.RetType.SetElems([]string{"a", "b"})

	cloned := con.Clone().(*Constant)
	require.NotSame(t, con, cloned)
	require.NotSame(t, con.RetType, cloned.RetType)
	require.NotSame(t, con.DeferredExpr, cloned.DeferredExpr)

	cloned.Value.GetBytes()[0] = 'z'
	cloned.RetType.SetElem(0, "zz")
	cloned.DeferredExpr.(*Constant).Value.GetBytes()[0] = 'q'
	require.Equal(t, byte('a'), con.Value.GetBytes()[0])
	require.Equal(t, "a", con.RetType.GetElem(0))
	require.Equal(t, byte('d'), con.DeferredExpr.(*Constant).Value.GetBytes()[0])
}

func TestScalarFunctionCloneNoSharedMutableFields(t *testing.T) {
	ctx := mock.NewContext()
	sf, ok := NewFunctionInternal(ctx, ast.Plus, types.NewFieldType(mysql.TypeLonglong),
		&Column{
			UniqueID: 1,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
			VirtualExpr: &Constant{
				Value:   types.NewBytesDatum([]byte("abc")),
				RetType: types.NewFieldType(mysql.TypeString),
			},
		},
		NewOne(),
	).(*ScalarFunction)
	require.True(t, ok)

	cloned := sf.Clone().(*ScalarFunction)
	require.NotSame(t, sf, cloned)
	require.NotSame(t, sf.RetType, cloned.RetType)
	require.Same(t, cloned.RetType, cloned.Function.getRetTp())
	require.NotSame(t, sf.GetArgs()[0], cloned.GetArgs()[0])

	clonedCol := cloned.GetArgs()[0].(*Column)
	clonedCol.RetType.SetType(mysql.TypeDouble)
	clonedCol.VirtualExpr.(*Constant).Value.GetBytes()[0] = 'z'
	require.Equal(t, mysql.TypeLonglong, sf.GetArgs()[0].(*Column).RetType.GetType())
	require.Equal(t, byte('a'), sf.GetArgs()[0].(*Column).VirtualExpr.(*Constant).Value.GetBytes()[0])
}
