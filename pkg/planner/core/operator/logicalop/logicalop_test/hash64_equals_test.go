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

func TestLogicalExpandHash64Equals(t *testing.T) {
	col1 := &expression.Column{
		ID:      1,
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	col2 := &expression.Column{
		ID:      2,
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	expand1 := &logicalop.LogicalExpand{
		DistinctGroupByCol: []*expression.Column{col1},
		DistinctGbyExprs:   []expression.Expression{col1},
		DistinctSize:       1,
		RollupGroupingSets: nil,
		LevelExprs:         nil,
		GID:                col1,
		GPos:               col1,
	}
	expand2 := &logicalop.LogicalExpand{
		DistinctGroupByCol: []*expression.Column{col1},
		DistinctGbyExprs:   []expression.Expression{col1},
		DistinctSize:       1,
		RollupGroupingSets: nil,
		LevelExprs:         nil,
		GID:                col1,
		GPos:               col1,
	}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	expand1.Hash64(hasher1)
	expand2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.DistinctGroupByCol = []*expression.Column{col2}
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.DistinctGroupByCol = []*expression.Column{col1}
	expand2.DistinctGbyExprs = []expression.Expression{col2}
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.DistinctGbyExprs = []expression.Expression{col1}
	expand2.DistinctSize = 2
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.DistinctSize = 1
	expand2.RollupGroupingSets = expression.GroupingSets{expression.GroupingSet{expression.GroupingExprs{col1}}}
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.RollupGroupingSets = nil
	expand2.LevelExprs = [][]expression.Expression{{col1}}
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.LevelExprs = nil
	expand2.GID = col2
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.GID = col1
	expand2.GPos = col2
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	expand2.GPos = col1
	hasher2.Reset()
	expand2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
}

func TestLogicalApplyHash64Equals(t *testing.T) {
	col1 := &expression.Column{
		ID:      1,
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	col2 := &expression.Column{
		ID:      2,
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	col3 := &expression.Column{
		ID:      3,
		Index:   2,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	eq, err := expression.NewFunction(ctx, ast.EQ, types.NewFieldType(mysql.TypeLonglong), col1, col2)
	require.Nil(t, err)
	join := &logicalop.LogicalJoin{
		JoinType:        logicalop.InnerJoin,
		EqualConditions: []*expression.ScalarFunction{eq.(*expression.ScalarFunction)},
	}
	la1 := logicalop.LogicalApply{
		LogicalJoin: *join,
		CorCols:     []*expression.CorrelatedColumn{{Column: *col3}},
	}
	la2 := logicalop.LogicalApply{
		LogicalJoin: *join,
		CorCols:     []*expression.CorrelatedColumn{{Column: *col3}},
	}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	la1.Hash64(hasher1)
	la2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())

	la2.CorCols = []*expression.CorrelatedColumn{{Column: *col2}}
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	la2.CorCols = []*expression.CorrelatedColumn{{Column: *col3}}
	la2.NoDecorrelate = true
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	la2.NoDecorrelate = false
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
}

func TestLogicalJoinHash64Equals(t *testing.T) {
	col1 := &expression.Column{
		ID:      1,
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	col2 := &expression.Column{
		ID:      2,
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	ctx := mock.NewContext()
	eq, err := expression.NewFunction(ctx, ast.EQ, types.NewFieldType(mysql.TypeLonglong), col1, col2)
	require.Nil(t, err)
	la1 := &logicalop.LogicalJoin{
		JoinType:        logicalop.InnerJoin,
		EqualConditions: []*expression.ScalarFunction{eq.(*expression.ScalarFunction)},
	}
	la2 := &logicalop.LogicalJoin{
		JoinType:        logicalop.InnerJoin,
		EqualConditions: []*expression.ScalarFunction{eq.(*expression.ScalarFunction)},
	}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	la1.Hash64(hasher1)
	la2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())

	la2.JoinType = logicalop.AntiSemiJoin
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	la2.JoinType = logicalop.InnerJoin
	eq2, err2 := expression.NewFunction(ctx, ast.EQ, types.NewFieldType(mysql.TypeLonglong), col2, col1)
	require.Nil(t, err2)
	la2.EqualConditions = []*expression.ScalarFunction{eq2.(*expression.ScalarFunction)}
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	la2.EqualConditions = nil
	gt, err3 := expression.NewFunction(ctx, ast.EQ, types.NewFieldType(mysql.TypeLonglong), col1, col2)
	require.Nil(t, err3)
	la2.OtherConditions = []expression.Expression{gt}
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	la1.EqualConditions = []*expression.ScalarFunction{}
	la2.OtherConditions = nil
	la2.EqualConditions = nil
	hasher1.Reset()
	hasher2.Reset()
	la1.Hash64(hasher1)
	la2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())

	la2.EqualConditions = []*expression.ScalarFunction{}
	hasher2.Reset()
	la2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
}

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
