// Copyright 2024 PingCAP, Inc.
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

package logicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/cascades/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestLogicalAggregationHash64Equals(t *testing.T) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	desc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{col}, true)
	require.Nil(t, err)
	la1 := &logicalop.LogicalAggregation{
		AggFuncs:           []*aggregation.AggFuncDesc{desc},
		GroupByItems:       []expression.Expression{col},
		PossibleProperties: [][]*expression.Column{{col}},
	}
	la2 := &logicalop.LogicalAggregation{
		AggFuncs:           []*aggregation.AggFuncDesc{desc},
		GroupByItems:       []expression.Expression{col},
		PossibleProperties: [][]*expression.Column{{col}},
	}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	la1.Hash64(hasher1)
	la2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())

	la2.GroupByItems = []expression.Expression{}
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	la2.GroupByItems = []expression.Expression{col}
	la2.PossibleProperties = [][]*expression.Column{{}}
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
}
