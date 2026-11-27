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

package aggregation

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// BIT is an unsigned integer type in MySQL, so SUM/AVG over a BIT column must
// infer DECIMAL rather than DOUBLE (issue #72740). SUM follows the integer
// width convention (flen+21) and AVG widens the actual BIT flen by
// div_precision_increment, both capped at the decimal limits.
func TestSumAvgBitRetType(t *testing.T) {
	ctx := mock.NewContext()
	for _, divPrec := range []int{4, 6} {
		ctx.GetSessionVars().DivPrecisionIncrement = divPrec
		for _, bitFlen := range []int{1, 8, 32, 64} {
			newBitCol := func() *expression.Column {
				tp := types.NewFieldType(mysql.TypeBit)
				tp.SetFlen(bitFlen)
				tp.SetDecimal(0)
				return &expression.Column{UniqueID: 1, RetType: tp}
			}
			label := fmt.Sprintf("BIT(%d) divPrec=%d", bitFlen, divPrec)

			sumDesc, err := newBaseFuncDesc(ctx, ast.AggFuncSum, []expression.Expression{newBitCol()})
			require.NoError(t, err)
			require.Equal(t, mysql.TypeNewDecimal, sumDesc.RetTp.GetType(), "SUM(%s)", label)
			require.Equal(t, min(bitFlen+21, mysql.MaxDecimalWidth), sumDesc.RetTp.GetFlen(), "SUM(%s)", label)
			require.Equal(t, 0, sumDesc.RetTp.GetDecimal(), "SUM(%s)", label)

			avgDesc, err := newBaseFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{newBitCol()})
			require.NoError(t, err)
			require.Equal(t, mysql.TypeNewDecimal, avgDesc.RetTp.GetType(), "AVG(%s)", label)
			require.Equal(t, min(bitFlen+divPrec, mysql.MaxDecimalWidth), avgDesc.RetTp.GetFlen(), "AVG(%s)", label)
			require.Equal(t, divPrec, avgDesc.RetTp.GetDecimal(), "AVG(%s)", label)

			// The argument must carry an explicit decimal cast so pushdown does
			// not send a raw BIT column with a DECIMAL result type.
			for _, desc := range []baseFuncDesc{sumDesc, avgDesc} {
				cast, ok := desc.Args[0].(*expression.ScalarFunction)
				require.True(t, ok, "%s: argument should be wrapped in a cast", desc.Name)
				require.Equal(t, ast.Cast, cast.FuncName.L)
				require.Equal(t, mysql.TypeNewDecimal, cast.GetType(ctx.GetEvalCtx()).GetType())
			}
		}
	}

	// Aggregates other than SUM/AVG keep the BIT argument untouched.
	ctx.GetSessionVars().DivPrecisionIncrement = vardef.DefDivPrecisionIncrement
	bitCol := &expression.Column{UniqueID: 2, RetType: types.NewFieldType(mysql.TypeBit)}
	for _, name := range []string{ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncCount} {
		desc, err := newBaseFuncDesc(ctx, name, []expression.Expression{bitCol})
		require.NoError(t, err)
		_, isCast := desc.Args[0].(*expression.ScalarFunction)
		require.False(t, isCast, "%s(BIT) argument must not be cast", name)
	}
}