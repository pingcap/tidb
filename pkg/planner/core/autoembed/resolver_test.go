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

package autoembed

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestResolveAutoEmbedInfo(t *testing.T) {
	sctx := mock.NewContext()
	ctx := sctx.GetPlanCtx()
	nextID := int64(1)

	newSourceWithDims := func(modelName string, dims int) (*logicalop.DataSource, *expression.Column) {
		t.Helper()
		textType := types.NewFieldType(mysql.TypeVarchar)
		vectorType := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
		vectorType.SetFlen(dims)
		tableID := nextID
		nextID++
		vectorID := nextID
		nextID++
		tblInfo := &model.TableInfo{
			ID:    tableID,
			Name:  ast.NewCIStr("t"),
			State: model.StatePublic,
			Columns: []*model.ColumnInfo{
				{
					ID:        1,
					Name:      ast.NewCIStr("text"),
					Offset:    0,
					State:     model.StatePublic,
					FieldType: *textType,
				},
				{
					ID:                  vectorID,
					Name:                ast.NewCIStr("vec"),
					Offset:              1,
					State:               model.StatePublic,
					FieldType:           *vectorType,
					GeneratedExprString: "embed_text('" + modelName + "', text, '{\"plus\":0.2}')",
					GeneratedStored:     true,
					Dependences:         map[string]struct{}{"text": {}},
				},
			},
		}
		tbl, err := tables.TableFromMeta(autoid.NewAllocators(false), tblInfo)
		require.NoError(t, err)
		col := &expression.Column{
			ID:       vectorID,
			UniqueID: nextID,
			RetType:  vectorType.Clone(),
		}
		nextID++
		ds := logicalop.DataSource{Table: tbl, TableInfo: tblInfo}.Init(ctx, 0)
		ds.SetSchema(expression.NewSchema(col))
		return ds, col
	}
	newSource := func(modelName string) (*logicalop.DataSource, *expression.Column) {
		return newSourceWithDims(modelName, 3)
	}

	assertModel := func(plan base.LogicalPlan, col *expression.Column, expected string) {
		t.Helper()
		info, ok := Resolve(plan, nil, nil, col, nil)
		require.True(t, ok)
		require.Equal(t, expected, info.ModelNameWithProvider)
		require.Equal(t, `{"plus":0.2}`, info.OptsInJSON)
	}
	assertRejected := func(plan base.LogicalPlan, col *expression.Column) {
		t.Helper()
		info, ok := Resolve(plan, nil, nil, col, nil)
		require.False(t, ok)
		require.Nil(t, info)
	}

	source, sourceCol := newSource("mock/json")
	assertModel(source, sourceCol, "mock/json")
	require.False(t, autoEmbedFieldTypesCompatible(sourceCol.RetType, types.NewFieldType(mysql.TypeLonglong)))
	nullableClone := sourceCol.RetType.Clone()
	nullableClone.DelFlag(mysql.NotNullFlag)
	nonNullClone := sourceCol.RetType.Clone()
	nonNullClone.AddFlag(mysql.NotNullFlag)
	require.True(t, autoEmbedFieldTypesCompatible(nullableClone, nonNullClone))

	projectedCol := &expression.Column{UniqueID: nextID, RetType: sourceCol.RetType.Clone()}
	nextID++
	projection := logicalop.LogicalProjection{Exprs: []expression.Expression{sourceCol}}.Init(ctx, 0)
	projection.SetSchema(expression.NewSchema(projectedCol))
	projection.SetChildren(source)
	assertModel(projection, projectedCol, "mock/json")
	snapshot := SnapshotSource(projection, nil)
	projection.SetChildren(logicalop.LogicalTableDual{}.Init(ctx, 0))
	snapshotResult := snapshot.resolve(projectedCol)
	require.True(t, snapshotResult.found)
	require.NotNil(t, snapshotResult.info)
	require.Equal(t, "mock/json", snapshotResult.info.ModelNameWithProvider)
	projection.SetChildren(source)

	derivedProjection := logicalop.LogicalProjection{Exprs: []expression.Expression{
		&expression.Constant{RetType: sourceCol.RetType.Clone()},
	}}.Init(ctx, 0)
	derivedProjection.SetSchema(expression.NewSchema(projectedCol.Clone().(*expression.Column)))
	derivedProjection.SetChildren(source)
	assertRejected(derivedProjection, derivedProjection.Schema().Columns[0])

	foldedDual := logicalop.LogicalTableDual{RowCount: 0}.Init(ctx, 0)
	foldedDual.SetSchema(source.Schema())
	state := NewBuildState(resolve.AutoEmbedConsumerPresent)
	state.RecordEmptyDual(foldedDual, source)
	info, ok := Resolve(foldedDual, nil, nil, sourceCol, state)
	require.True(t, ok)
	require.Equal(t, "mock/json", info.ModelNameWithProvider)

	sameSchemaDual := logicalop.LogicalTableDual{RowCount: 0}.Init(ctx, 0)
	sameSchemaDual.SetSchema(source.Schema())
	assertRejected(sameSchemaDual, sourceCol)
	info, ok = Resolve(sameSchemaDual, nil, nil, sourceCol, state)
	require.False(t, ok)
	require.Nil(t, info)

	chainDual := logicalop.LogicalTableDual{RowCount: 0}.Init(ctx, 0)
	chainDual.SetSchema(source.Schema())
	state.RecordEmptyDual(chainDual, foldedDual)
	info, ok = Resolve(chainDual, nil, nil, sourceCol, state)
	require.True(t, ok)
	require.Equal(t, "mock/json", info.ModelNameWithProvider)

	cycleDual := logicalop.LogicalTableDual{RowCount: 0}.Init(ctx, 0)
	cycleDual.SetSchema(source.Schema())
	state.RecordEmptyDual(cycleDual, cycleDual)
	info, ok = Resolve(cycleDual, nil, nil, sourceCol, state)
	require.False(t, ok)
	require.Nil(t, info)

	rowDual := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	rowDual.SetSchema(source.Schema())
	state.RecordEmptyDual(rowDual, source)
	info, ok = Resolve(rowDual, nil, nil, sourceCol, state)
	require.False(t, ok)
	require.Nil(t, info)

	foldedSnapshot := SnapshotSource(foldedDual, state)
	require.NotNil(t, foldedSnapshot)
	require.Equal(t, "mock/json", foldedSnapshot.resolve(sourceCol).info.ModelNameWithProvider)

	selection := logicalop.LogicalSelection{}.Init(ctx, 0)
	selection.SetChildren(projection)
	assertModel(selection, selection.Schema().Columns[0], "mock/json")

	firstRow, err := aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{sourceCol}, false)
	require.NoError(t, err)
	aggCol := sourceCol.Clone().(*expression.Column)
	aggCol.RetType = firstRow.RetTp
	agg := logicalop.LogicalAggregation{AggFuncs: []*aggregation.AggFuncDesc{firstRow}}.Init(ctx, 0)
	agg.SetSchema(expression.NewSchema(aggCol))
	agg.SetChildren(source)
	assertModel(agg, aggCol, "mock/json")

	count, err := aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{sourceCol}, false)
	require.NoError(t, err)
	derivedAggCol := &expression.Column{UniqueID: nextID, RetType: sourceCol.RetType.Clone()}
	nextID++
	derivedAgg := logicalop.LogicalAggregation{AggFuncs: []*aggregation.AggFuncDesc{count}}.Init(ctx, 0)
	derivedAgg.SetSchema(expression.NewSchema(derivedAggCol))
	derivedAgg.SetChildren(source)
	assertRejected(derivedAgg, derivedAggCol)

	sameSource, sameCol := newSource("mock/json")
	unionCol := &expression.Column{UniqueID: nextID, RetType: sourceCol.RetType.Clone()}
	nextID++
	union := logicalop.LogicalUnionAll{}.Init(ctx, 0)
	union.SetSchema(expression.NewSchema(unionCol))
	union.SetChildren(source, sameSource)
	assertModel(union, unionCol, "mock/json")
	unionFirstRow, err := aggregation.NewAggFuncDesc(ctx.GetExprCtx(), ast.AggFuncFirstRow, []expression.Expression{unionCol}, false)
	require.NoError(t, err)
	unionDistinctCol := unionCol.Clone().(*expression.Column)
	unionDistinctCol.RetType = unionFirstRow.RetTp
	unionDistinct := logicalop.LogicalAggregation{AggFuncs: []*aggregation.AggFuncDesc{unionFirstRow}}.Init(ctx, 0)
	unionDistinct.SetSchema(expression.NewSchema(unionDistinctCol))
	unionDistinct.SetChildren(union)
	assertRejected(unionDistinct, unionDistinctCol)

	setOperatorJoin := logicalop.LogicalJoin{JoinType: base.SemiJoin, FromSetOperator: true}.Init(ctx, 0)
	setOperatorJoin.SetSchema(agg.Schema())
	setOperatorJoin.SetChildren(agg, sameSource)
	assertRejected(setOperatorJoin, aggCol)
	ordinarySemiJoin := logicalop.LogicalJoin{JoinType: base.SemiJoin}.Init(ctx, 0)
	ordinarySemiJoin.SetSchema(agg.Schema())
	ordinarySemiJoin.SetChildren(agg, sameSource)
	assertModel(ordinarySemiJoin, aggCol, "mock/json")

	conflictingSource, _ := newSource("mock/other")
	conflictingUnion := logicalop.LogicalUnionAll{}.Init(ctx, 0)
	conflictingUnion.SetSchema(expression.NewSchema(unionCol.Clone().(*expression.Column)))
	conflictingUnion.SetChildren(source, conflictingSource)
	assertRejected(conflictingUnion, conflictingUnion.Schema().Columns[0])

	usingJoin := logicalop.LogicalJoin{JoinType: base.InnerJoin}.Init(ctx, 0)
	usingJoin.SetChildren(source, sameSource)
	usingJoin.SetSchema(expression.NewSchema(sourceCol))
	usingJoin.FullSchema = expression.NewSchema(sourceCol, sameCol)
	usingJoin.FullNames = types.NameSlice{
		&types.FieldName{ColName: ast.NewCIStr("vec")},
		&types.FieldName{ColName: ast.NewCIStr("vec"), Redundant: true},
	}
	usingJoin.RegisterRedundantColumnMapping(sameCol, sourceCol)
	assertModel(usingJoin, sameCol, "mock/json")
	missingUsingMap := logicalop.LogicalJoin{JoinType: base.InnerJoin}.Init(ctx, 0)
	missingUsingMap.SetChildren(source, sameSource)
	missingUsingMap.SetSchema(expression.NewSchema(sourceCol))
	missingUsingMap.FullSchema = expression.NewSchema(sourceCol, sameCol)
	missingUsingMap.FullNames = usingJoin.FullNames
	assertRejected(missingUsingMap, sourceCol)
	assertRejected(missingUsingMap, sameCol)

	conflictingCol := conflictingSource.Schema().Columns[0]
	conflictingJoin := logicalop.LogicalJoin{JoinType: base.InnerJoin}.Init(ctx, 0)
	conflictingJoin.SetChildren(source, conflictingSource)
	conflictingJoin.SetSchema(expression.NewSchema(sourceCol))
	conflictingJoin.FullSchema = expression.NewSchema(sourceCol, conflictingCol)
	conflictingJoin.FullNames = types.NameSlice{
		&types.FieldName{ColName: ast.NewCIStr("vec")},
		&types.FieldName{ColName: ast.NewCIStr("vec"), Redundant: true},
	}
	conflictingJoin.RegisterRedundantColumnMapping(conflictingCol, sourceCol)
	assertRejected(conflictingJoin, sourceCol)
	assertRejected(conflictingJoin, conflictingCol)

	differentTypeSource, differentTypeCol := newSourceWithDims("mock/json", 4)
	differentTypeJoin := logicalop.LogicalJoin{JoinType: base.InnerJoin}.Init(ctx, 0)
	differentTypeJoin.SetChildren(source, differentTypeSource)
	differentTypeJoin.SetSchema(expression.NewSchema(sourceCol))
	differentTypeJoin.FullSchema = expression.NewSchema(sourceCol, differentTypeCol)
	differentTypeJoin.FullNames = usingJoin.FullNames
	differentTypeJoin.RegisterRedundantColumnMapping(differentTypeCol, sourceCol)
	assertRejected(differentTypeJoin, sourceCol)
	assertRejected(differentTypeJoin, differentTypeCol)

	unknown := logicalop.LogicalMemTable{}.Init(ctx, 0)
	unknown.SetSchema(expression.NewSchema(sourceCol.Clone().(*expression.Column)))
	assertRejected(unknown, unknown.Schema().Columns[0])

	cycleCol := sourceCol.Clone().(*expression.Column)
	cycle := logicalop.LogicalProjection{Exprs: []expression.Expression{cycleCol}}.Init(ctx, 0)
	cycle.SetSchema(expression.NewSchema(cycleCol))
	cycle.SetChildren(cycle)
	assertRejected(cycle, cycleCol)
	assertRejected(nil, sourceCol)
}

func TestAutoEmbedConsumerClassificationAndBuildState(t *testing.T) {
	for name, expected := range map[string]string{
		ast.VecEmbedL1Distance:           ast.VecL1Distance,
		ast.VecEmbedL2Distance:           ast.VecL2Distance,
		ast.VecEmbedNegativeInnerProduct: ast.VecNegativeInnerProduct,
		ast.VecEmbedCosineDistance:       ast.VecCosineDistance,
	} {
		distance, ok := ConsumerDistanceFunction(name)
		require.True(t, ok)
		require.Equal(t, expected, distance)
		call := &ast.FuncCallExpr{FnName: ast.NewCIStr(name)}
		require.Equal(t, resolve.AutoEmbedConsumerPresent, ClassifyConsumerAST(call))
		require.Equal(t, resolve.AutoEmbedConsumerPresent, ClassifyAssignmentExprs([]*ast.Assignment{{Expr: call}}))
	}

	ordinary := &ast.FuncCallExpr{FnName: ast.NewCIStr(ast.VecL2Distance)}
	_, ok := ConsumerDistanceFunction(ordinary.FnName.L)
	require.False(t, ok)
	require.Equal(t, resolve.AutoEmbedConsumerAbsent, ClassifyConsumerAST(ordinary))
	require.Equal(t, resolve.AutoEmbedConsumerUnknown, ClassifyConsumerAST(nil))
	require.Equal(t, resolve.AutoEmbedConsumerAbsent, ClassifyAssignmentExprs(nil))
	require.Equal(t, resolve.AutoEmbedConsumerUnknown, ClassifyAssignmentExprs([]*ast.Assignment{nil}))

	require.Equal(t, resolve.AutoEmbedConsumerPresent,
		resolve.MergeAutoEmbedConsumerPresence(resolve.AutoEmbedConsumerUnknown, resolve.AutoEmbedConsumerPresent))
	require.Equal(t, resolve.AutoEmbedConsumerUnknown,
		resolve.MergeAutoEmbedConsumerPresence(resolve.AutoEmbedConsumerUnknown, resolve.AutoEmbedConsumerAbsent))
	require.Equal(t, resolve.AutoEmbedConsumerAbsent,
		resolve.MergeAutoEmbedConsumerPresence(resolve.AutoEmbedConsumerAbsent, resolve.AutoEmbedConsumerAbsent))
	require.True(t, resolve.AutoEmbedConsumerUnknown.NeedsLineage())
	require.True(t, resolve.AutoEmbedConsumerPresent.NeedsLineage())
	require.False(t, resolve.AutoEmbedConsumerAbsent.NeedsLineage())

	sctx := mock.NewContext()
	ctx := sctx.GetPlanCtx()
	source := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	empty := logicalop.LogicalTableDual{RowCount: 0}.Init(ctx, 0)
	absent := NewBuildState(resolve.AutoEmbedConsumerAbsent)
	absent.RecordEmptyDual(empty, source)
	_, found := absent.SourceOfEmptyDual(empty)
	require.False(t, found)
	allocs := testing.AllocsPerRun(100, func() {
		absent.ResetForBuild(resolve.AutoEmbedConsumerAbsent)
		absent.RecordEmptyDual(empty, source)
		absent.ResetForReuse()
	})
	require.Zero(t, allocs)

	unknown := NewBuildState(resolve.AutoEmbedConsumerUnknown)
	unknown.RecordEmptyDual(empty, source)
	recorded, found := unknown.SourceOfEmptyDual(empty)
	require.True(t, found)
	require.Same(t, source, recorded)
	unknown.ResetForReuse()
	_, found = unknown.SourceOfEmptyDual(empty)
	require.False(t, found)
	require.Equal(t, resolve.AutoEmbedConsumerUnknown, unknown.Presence())
}

func BenchmarkAutoEmbedConsumerGateAndSidecar(b *testing.B) {
	ordinary := &ast.FuncCallExpr{FnName: ast.NewCIStr(ast.VecL2Distance)}
	consumer := &ast.FuncCallExpr{FnName: ast.NewCIStr(ast.VecEmbedL2Distance)}
	b.Run("ClassifyAbsentAST", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if ClassifyConsumerAST(ordinary) != resolve.AutoEmbedConsumerAbsent {
				b.Fatal("unexpected classification")
			}
		}
	})
	b.Run("ClassifyPresentAST", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if ClassifyConsumerAST(consumer) != resolve.AutoEmbedConsumerPresent {
				b.Fatal("unexpected classification")
			}
		}
	})

	sctx := mock.NewContext()
	ctx := sctx.GetPlanCtx()
	source := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	empty := logicalop.LogicalTableDual{RowCount: 0}.Init(ctx, 0)
	b.Run("AbsentRecord", func(b *testing.B) {
		state := NewBuildState(resolve.AutoEmbedConsumerAbsent)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			state.ResetForBuild(resolve.AutoEmbedConsumerAbsent)
			state.RecordEmptyDual(empty, source)
		}
	})
	b.Run("PresentRecordAndLookup", func(b *testing.B) {
		state := NewBuildState(resolve.AutoEmbedConsumerPresent)
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			state.ResetForBuild(resolve.AutoEmbedConsumerPresent)
			state.RecordEmptyDual(empty, source)
			if _, ok := state.SourceOfEmptyDual(empty); !ok {
				b.Fatal("missing sidecar source")
			}
		}
	})
}
