// Copyright 2021 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestClone(t *testing.T) {
	ctx := mock.NewContext()
	col := &expression.Column{
		UniqueID: 0,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	desc, err := newBaseFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{col})
	require.NoError(t, err)
	cloned := desc.clone()
	require.True(t, desc.equal(ctx, cloned))

	col1 := &expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeVarchar),
	}
	cloned.Args[0] = col1

	require.Equal(t, col, desc.Args[0])
	require.False(t, desc.equal(ctx, cloned))
}

func TestBaseFunc_InferAggRetType(t *testing.T) {
	ctx := mock.NewContext()
	doubleType := types.NewFieldType(mysql.TypeDouble)
	bitType := types.NewFieldType(mysql.TypeBit)

	funcNames := []string{
		ast.AggFuncMax, ast.AggFuncMin,
	}
	dataTypes := []*types.FieldType{
		doubleType, bitType,
	}

	for _, dataType := range dataTypes {
		notNullType := dataType.Clone()
		notNullType.AddFlag(mysql.NotNullFlag)
		col := &expression.Column{
			UniqueID: 0,
			RetType:  notNullType,
		}
		for _, name := range funcNames {
			desc, err := newBaseFuncDesc(ctx, name, []expression.Expression{col})
			require.NoError(t, err)
			err = desc.TypeInfer(ctx)
			require.NoError(t, err)
			require.Equal(t, dataType, desc.RetTp)
		}
	}

	// GROUP_CONCAT should keep the non-binary collation from its non-binary inputs.
	{
		colType := types.NewFieldType(mysql.TypeVarString)
		colType.SetCharset(charset.CharsetUTF8MB4)
		colType.SetCollate("utf8mb4_0900_ai_ci")
		colType.DelFlag(mysql.BinaryFlag)
		col := &expression.Column{
			UniqueID: 1,
			RetType:  colType,
		}

		sepType := types.NewFieldType(mysql.TypeVarString)
		sepType.SetCharset(charset.CharsetUTF8MB4)
		sepType.SetCollate("utf8mb4_0900_ai_ci")
		sepType.DelFlag(mysql.BinaryFlag)
		sep := &expression.Constant{
			Value:   types.NewStringDatum(" "),
			RetType: sepType,
		}

		desc, err := newBaseFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{col, sep})
		require.NoError(t, err)
		require.Equal(t, charset.CharsetUTF8MB4, desc.RetTp.GetCharset())
		require.Equal(t, "utf8mb4_0900_ai_ci", desc.RetTp.GetCollate())
	}

	// GROUP_CONCAT should fall back to connection collation when separator literal has empty charset/collation metadata.
	{
		require.NoError(t, ctx.GetSessionVars().SetSystemVar(vardef.CharacterSetConnection, charset.CharsetUTF8MB4))
		require.NoError(t, ctx.GetSessionVars().SetSystemVar(vardef.CollationConnection, charset.CollationUTF8MB4))

		col := &expression.Column{
			UniqueID: 2,
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		}

		emptySepType := types.NewFieldType(mysql.TypeVarString)
		emptySep := &expression.Constant{
			Value:   types.NewStringDatum(","),
			RetType: emptySepType,
		}

		desc, err := newBaseFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{col, emptySep})
		require.NoError(t, err)
		require.Equal(t, charset.CharsetUTF8MB4, desc.RetTp.GetCharset())
		require.Equal(t, charset.CollationUTF8MB4, desc.RetTp.GetCollate())
	}

	// GROUP_CONCAT should still keep a non-binary input collation even when separator metadata is empty.
	{
		colType := types.NewFieldType(mysql.TypeVarString)
		colType.SetCharset(charset.CharsetUTF8MB4)
		colType.SetCollate("utf8mb4_0900_ai_ci")
		colType.DelFlag(mysql.BinaryFlag)
		col := &expression.Column{
			UniqueID: 3,
			RetType:  colType,
		}

		emptySepType := types.NewFieldType(mysql.TypeVarString)
		emptySep := &expression.Constant{
			Value:   types.NewStringDatum("|"),
			RetType: emptySepType,
		}

		desc, err := newBaseFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{col, emptySep})
		require.NoError(t, err)
		require.Equal(t, charset.CharsetUTF8MB4, desc.RetTp.GetCharset())
		require.Equal(t, "utf8mb4_0900_ai_ci", desc.RetTp.GetCollate())
	}
}

func TestTypeInfer4AvgSum(t *testing.T) {
	ctx := mock.NewContext()

	// sum(col)
	{
		argCol := &expression.Column{
			UniqueID: 0,
			RetType:  types.NewFieldType(mysql.TypeNewDecimal),
		}
		argColFlen := 15
		argCol.RetType.SetFlen(argColFlen)
		argCol.RetType.SetDecimal(2)

		avgFunc, err := newBaseFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{argCol})
		require.NoError(t, err)
		err = avgFunc.TypeInfer(ctx)
		require.NoError(t, err)

		partialSumFunc := avgFunc
		partialSumFunc.Name = ast.AggFuncSum
		err = partialSumFunc.TypeInfer4AvgSum(ctx.GetEvalCtx(), avgFunc.RetTp)
		require.NoError(t, err)

		require.Equal(t, partialSumFunc.RetTp.GetFlen(), argColFlen+22)
		require.Equal(t, partialSumFunc.RetTp.GetDecimal(), 2)
	}

	// sum(div(col/col))
	{
		divArgCol1 := &expression.Column{
			UniqueID: 0,
			RetType:  types.NewFieldType(mysql.TypeNewDecimal),
		}
		divArgCol1.RetType.SetFlen(20)
		divArgCol1.RetType.SetDecimal(0)

		divArgCol2 := &expression.Column{
			UniqueID: 0,
			RetType:  types.NewFieldType(mysql.TypeNewDecimal),
		}
		divArgCol2.RetType.SetFlen(5)
		divArgCol2.RetType.SetDecimal(0)

		mockDivRetType := types.NewFieldType(mysql.TypeUnspecified)
		divExpr, err := expression.NewFunction(ctx, ast.Div, mockDivRetType, divArgCol1, divArgCol2)
		require.NoError(t, err)
		require.Equal(t, divExpr.GetType(ctx.GetEvalCtx()).GetFlen(), 25)
		require.Equal(t, divExpr.GetType(ctx.GetEvalCtx()).GetDecimal(), 4)

		avgFunc, err := newBaseFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{divExpr})
		require.NoError(t, err)
		err = avgFunc.TypeInfer(ctx)
		require.NoError(t, err)
		require.Equal(t, avgFunc.RetTp.GetFlen(), 29)
		require.Equal(t, avgFunc.RetTp.GetDecimal(), 8)

		partialSumFunc := avgFunc
		partialSumFunc.Name = ast.AggFuncSum
		err = partialSumFunc.TypeInfer4AvgSum(ctx.GetEvalCtx(), avgFunc.RetTp)
		require.NoError(t, err)
		require.Equal(t, partialSumFunc.RetTp.GetFlen(), 51)
		require.Equal(t, partialSumFunc.RetTp.GetDecimal(), 8)
	}
}
