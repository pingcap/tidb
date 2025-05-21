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
<<<<<<< HEAD
=======
	require.False(t, la1.Equals(la2))
}

func MockFunc(sctx expression.EvalContext, lhsArg, rhsArg expression.Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	return 0, false, nil
}
func MockFunc2(sctx expression.EvalContext, lhsArg, rhsArg expression.Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	return 0, false, nil
}

func TestFrameBoundHash64Equals(t *testing.T) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	col2 := &expression.Column{
		Index:    1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	fb1 := &logicalop.FrameBound{
		Type:            ast.Preceding,
		UnBounded:       true,
		Num:             1,
		CalcFuncs:       []expression.Expression{col},
		CompareCols:     []expression.Expression{col},
		CmpFuncs:        []expression.CompareFunc{MockFunc},
		CmpDataType:     1,
		IsExplicitRange: false,
	}
	fb2 := &logicalop.FrameBound{
		Type:            ast.Preceding,
		UnBounded:       true,
		Num:             1,
		CalcFuncs:       []expression.Expression{col},
		CompareCols:     []expression.Expression{col},
		CmpFuncs:        []expression.CompareFunc{MockFunc},
		CmpDataType:     1,
		IsExplicitRange: false,
	}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	fb1.Hash64(hasher1)
	fb2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, fb1.Equals(fb2))

	fb2.Type = ast.CurrentRow
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))

	fb2.Type = ast.Preceding
	fb2.UnBounded = false
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))

	fb2.UnBounded = true
	fb2.Num = 2
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))

	fb2.Num = 1
	fb2.CalcFuncs = []expression.Expression{col2}
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))

	fb2.CalcFuncs = []expression.Expression{col}
	fb2.CompareCols = []expression.Expression{col2}
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))

	fb2.CompareCols = []expression.Expression{col}
	fb2.CmpFuncs = []expression.CompareFunc{MockFunc2}
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))

	fb2.CmpFuncs = []expression.CompareFunc{MockFunc}
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.True(t, fb1.Equals(fb2))
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())

	fb2.CmpDataType = 2
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))

	fb2.CmpDataType = 1
	fb2.IsExplicitRange = true
	hasher2.Reset()
	fb2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, fb1.Equals(fb2))
}

func TestWindowFrameHash64Equals(t *testing.T) {
	col := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	start := &logicalop.FrameBound{
		Type:            ast.Preceding,
		UnBounded:       true,
		Num:             1,
		CalcFuncs:       []expression.Expression{col},
		CompareCols:     []expression.Expression{col},
		CmpFuncs:        []expression.CompareFunc{MockFunc},
		CmpDataType:     1,
		IsExplicitRange: false,
	}
	end := start
	wf1 := &logicalop.WindowFrame{
		Type:  1,
		Start: start,
		End:   end,
	}
	wf2 := &logicalop.WindowFrame{
		Type:  1,
		Start: start,
		End:   end,
	}
	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	wf1.Hash64(hasher1)
	wf2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, wf1.Equals(wf2))

	wf2.Type = 2
	hasher2.Reset()
	wf2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, wf1.Equals(wf2))
}

func TestHandleColsHash64Equals(t *testing.T) {
	col1 := &expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	col2 := &expression.Column{
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	handles1 := util.NewCommonHandlesColsWithoutColsAlign(&model.TableInfo{ID: 1}, &model.IndexInfo{ID: 1}, []*expression.Column{col1, col2})
	handles2 := util.NewCommonHandlesColsWithoutColsAlign(&model.TableInfo{ID: 1}, &model.IndexInfo{ID: 1}, []*expression.Column{col1, col2})

	hasher1 := base.NewHashEqualer()
	hasher2 := base.NewHashEqualer()
	handles1.Hash64(hasher1)
	handles2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, handles1.Equals(handles2))

	handles2 = util.NewCommonHandlesColsWithoutColsAlign(&model.TableInfo{ID: 2}, &model.IndexInfo{ID: 1}, []*expression.Column{col1, col2})
	hasher2.Reset()
	handles2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, handles1.Equals(handles2))

	handles2 = util.NewCommonHandlesColsWithoutColsAlign(&model.TableInfo{ID: 1}, &model.IndexInfo{ID: 2}, []*expression.Column{col1, col2})
	hasher2.Reset()
	handles2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, handles1.Equals(handles2))

	handles2 = util.NewCommonHandlesColsWithoutColsAlign(&model.TableInfo{ID: 1}, &model.IndexInfo{ID: 1}, []*expression.Column{col2, col2})
	hasher2.Reset()
	handles2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, handles1.Equals(handles2))

	handles2 = util.NewCommonHandlesColsWithoutColsAlign(&model.TableInfo{ID: 1}, &model.IndexInfo{ID: 1}, []*expression.Column{col2, col1})
	hasher2.Reset()
	handles2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, handles1.Equals(handles2))

	handles2 = util.NewCommonHandlesColsWithoutColsAlign(&model.TableInfo{ID: 1}, &model.IndexInfo{ID: 1}, []*expression.Column{col1, col2})
	hasher2.Reset()
	handles2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, handles1.Equals(handles2))

	intH1 := util.NewIntHandleCols(col1)
	intH2 := util.NewIntHandleCols(col1)
	hasher1.Reset()
	hasher2.Reset()
	intH1.Hash64(hasher1)
	intH2.Hash64(hasher2)
	require.Equal(t, hasher1.Sum64(), hasher2.Sum64())
	require.True(t, handles1.Equals(handles2))

	intH2 = util.NewIntHandleCols(col2)
	hasher2.Reset()
	intH2.Hash64(hasher2)
	require.NotEqual(t, hasher1.Sum64(), hasher2.Sum64())
	require.False(t, intH1.Equals(intH2))
>>>>>>> 51659f35539 (*: remove `StatementContext` from `CommonHandleCols` to fix a bug caused by shallow clone in plan cache (#61182))
}
