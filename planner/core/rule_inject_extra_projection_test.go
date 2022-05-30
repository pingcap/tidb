// Copyright 2019 PingCAP, Inc.
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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestWrapCastForAggFuncs(t *testing.T) {
	aggNames := []string{ast.AggFuncSum}
	modes := []aggregation.AggFunctionMode{aggregation.CompleteMode,
		aggregation.FinalMode, aggregation.Partial1Mode, aggregation.Partial1Mode}
	retTypes := []byte{mysql.TypeLong, mysql.TypeNewDecimal, mysql.TypeDouble, mysql.TypeLonglong, mysql.TypeInt24}
	hasDistincts := []bool{true, false}

	aggFuncs := make([]*aggregation.AggFuncDesc, 0, 32)
	for _, hasDistinct := range hasDistincts {
		for _, name := range aggNames {
			for _, mode := range modes {
				for _, retType := range retTypes {
					sctx := mock.NewContext()
					aggFunc, err := aggregation.NewAggFuncDesc(sctx, name,
						[]expression.Expression{&expression.Constant{Value: types.Datum{}, RetType: types.NewFieldType(retType)}},
						hasDistinct)
					require.NoError(t, err)
					aggFunc.Mode = mode
					aggFuncs = append(aggFuncs, aggFunc)
				}
			}
		}
	}

	orgAggFuncs := make([]*aggregation.AggFuncDesc, 0, len(aggFuncs))
	for _, agg := range aggFuncs {
		orgAggFuncs = append(orgAggFuncs, agg.Clone())
	}

	wrapCastForAggFuncs(mock.NewContext(), aggFuncs)
	for i := range aggFuncs {
		if aggFuncs[i].Mode != aggregation.FinalMode && aggFuncs[i].Mode != aggregation.Partial2Mode {
			require.Equal(t, aggFuncs[i].Args[0].GetType().GetType(), aggFuncs[i].RetTp.GetType())
		} else {
			require.Equal(t, orgAggFuncs[i].Args[0].GetType().GetType(), aggFuncs[i].Args[0].GetType().GetType())
		}
	}
}
