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

package aggregation

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func genColumn(tp byte, id int64) *expression.Column {
	return &expression.Column{
		RetType: types.NewFieldType(tp),
		ID:      id,
		Index:   int(id),
	}
}

func TestAggFunc2Pb(t *testing.T) {
	ctx := mock.NewContext()
	client := new(mock.Client)

	funcNames := []string{ast.AggFuncSum, ast.AggFuncCount, ast.AggFuncAvg, ast.AggFuncGroupConcat, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow}
	funcTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeDouble),
		types.NewFieldType(mysql.TypeDouble),
	}

	jsons := []string{
		`{"tp":3002,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":%v}`,
		`{"tp":3001,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":0,"field_type":{"tp":8,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":%v}`,
		`{"tp":3003,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":%v}`,
		`{"tp":3007,"val":"AAAAAAAABAA=","children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":0,"field_type":{"tp":15,"flag":0,"flen":-1,"decimal":-1,"collate":46,"charset":"utf8mb4"},"has_distinct":%v}`,
		`{"tp":3005,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":%v}`,
		`{"tp":3004,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":%v}`,
		`{"tp":3006,"children":[{"tp":201,"val":"gAAAAAAAAAE=","sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":false}],"sig":0,"field_type":{"tp":5,"flag":0,"flen":-1,"decimal":-1,"collate":63,"charset":"binary"},"has_distinct":%v}`,
	}
	for i, funcName := range funcNames {
		for _, hasDistinct := range []bool{true, false} {
			args := []expression.Expression{genColumn(mysql.TypeDouble, 1)}
			aggFunc, err := NewAggFuncDesc(ctx, funcName, args, hasDistinct)
			require.NoError(t, err)
			aggFunc.RetTp = funcTypes[i]
			pbExpr := AggFuncToPBExpr(ctx, client, aggFunc)
			js, err := json.Marshal(pbExpr)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf(jsons[i], hasDistinct), string(js))
		}
	}
}
