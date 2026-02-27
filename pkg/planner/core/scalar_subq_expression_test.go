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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestScalarSubQueryExprCloneDeepCopy(t *testing.T) {
	evalCtx := &ScalarSubqueryEvalCtx{
		evaled:       true,
		outputColIDs: []int64{1},
		colsData:     []types.Datum{types.NewBytesDatum([]byte("abc"))},
	}
	expr := &ScalarSubQueryExpr{
		scalarSubqueryColID: 1,
		evalCtx:             evalCtx,
		evaled:              true,
		hashcode:            []byte{1, 2, 3},
		Constant: expression.Constant{
			Value:   types.NewBytesDatum([]byte("xyz")),
			RetType: types.NewFieldType(mysql.TypeVarString),
		},
	}

	cloned := expr.Clone().(*ScalarSubQueryExpr)
	require.NotSame(t, expr, cloned)
	require.NotSame(t, expr.evalCtx, cloned.evalCtx)
	require.NotSame(t, expr.RetType, cloned.RetType)
	require.NotSame(t, &expr.hashcode[0], &cloned.hashcode[0])

	cloned.evalCtx.outputColIDs[0] = 2
	require.Equal(t, int64(1), expr.evalCtx.outputColIDs[0])

	clonedEvalCtxData := cloned.evalCtx.colsData[0].GetBytes()
	clonedEvalCtxData[0] = 'z'
	require.Equal(t, byte('a'), expr.evalCtx.colsData[0].GetBytes()[0])

	clonedValue := cloned.Value.GetBytes()
	clonedValue[0] = 'q'
	require.Equal(t, byte('x'), expr.Value.GetBytes()[0])

	cloned.RetType.SetType(mysql.TypeBlob)
	require.Equal(t, mysql.TypeVarString, expr.RetType.GetType())
}
