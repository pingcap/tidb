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

package mvdeltamergeagg

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type mockSource struct {
	exec.BaseExecutor
	chunks []*chunk.Chunk
	idx    int
}

func newMockSource(ctx sessionctx.Context, fts []*types.FieldType, chks []*chunk.Chunk) *mockSource {
	cols := make([]*expression.Column, len(fts))
	for i := range fts {
		cols[i] = &expression.Column{Index: i, RetType: fts[i]}
	}
	return &mockSource{
		BaseExecutor: exec.NewBaseExecutor(ctx, expression.NewSchema(cols...), 0),
		chunks:       chks,
	}
}

func (m *mockSource) Open(context.Context) error {
	m.idx = 0
	return nil
}

func (m *mockSource) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if m.idx >= len(m.chunks) {
		return nil
	}
	req.SwapColumns(m.chunks[m.idx])
	m.idx++
	return nil
}

type collectWriter struct {
	results []*ChunkResult
}

func (w *collectWriter) WriteChunk(_ context.Context, result *ChunkResult) error {
	clone := &ChunkResult{
		Input:               result.Input.CopyConstruct(),
		ComputedCols:        make([]*chunk.Column, len(result.ComputedCols)),
		RowOps:              append([]RowOp(nil), result.RowOps...),
		UpdateTouchedBitmap: append([]uint8(nil), result.UpdateTouchedBitmap...),
		UpdateTouchedStride: result.UpdateTouchedStride,
		UpdateTouchedBitCnt: result.UpdateTouchedBitCnt,
	}
	for i := range result.ComputedCols {
		if result.ComputedCols[i] != nil {
			clone.ComputedCols[i] = result.ComputedCols[i].CopyConstruct(nil)
		}
	}
	w.results = append(w.results, clone)
	return nil
}

func TestCountAndNullableSum(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 2

	// [0]=delta_count(*), [1]=delta_sum(b), [2]=delta_count(b), [3]=group_key,
	// [4]=mv_count(*), [5]=mv_sum(b), [6]=mv_count(b)
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftDec := types.NewFieldType(mysql.TypeNewDecimal)
	fts := []*types.FieldType{ftInt, ftDec, ftInt, ftInt, ftInt, ftDec, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 3)

	chk.AppendInt64(0, 0)
	chk.AppendMyDecimal(1, types.NewDecFromInt(-10))
	chk.AppendInt64(2, -1)
	chk.AppendInt64(3, 1)
	chk.AppendInt64(4, 1)
	chk.AppendMyDecimal(5, types.NewDecFromInt(10))
	chk.AppendInt64(6, 1)

	chk.AppendInt64(0, -1)
	chk.AppendMyDecimal(1, types.NewDecFromInt(-5))
	chk.AppendInt64(2, -1)
	chk.AppendInt64(3, 2)
	chk.AppendInt64(4, 1)
	chk.AppendMyDecimal(5, types.NewDecFromInt(5))
	chk.AppendInt64(6, 1)

	chk.AppendInt64(0, 2)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 3)
	chk.AppendNull(4)
	chk.AppendNull(5)
	chk.AppendNull(6)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	countArg := &expression.Column{Index: 0, RetType: ftInt}
	countDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{countArg}, false)
	require.NoError(t, err)
	sumArg := &expression.Column{Index: 1, RetType: ftDec}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{4}, DependencyColID: []int{0}},
			{AggFunc: countDesc, ColID: []int{6}, DependencyColID: []int{2}},
			{AggFunc: sumDesc, ColID: []int{5}, DependencyColID: []int{1, 6}},
		},
		DeltaAggColCount: 3,
		WorkerCnt:        2,
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Len(t, writer.results, 1)
	res := writer.results[0]
	require.Len(t, res.RowOps, 3)
	require.Equal(t, RowOpUpdate, res.RowOps[0].Tp)
	require.Equal(t, RowOpDelete, res.RowOps[1].Tp)
	require.Equal(t, RowOpInsert, res.RowOps[2].Tp)

	countStarCol := res.ComputedCols[4]
	require.NotNil(t, countStarCol)
	require.Equal(t, int64(1), countStarCol.GetInt64(0))
	require.Equal(t, int64(0), countStarCol.GetInt64(1))
	require.Equal(t, int64(2), countStarCol.GetInt64(2))

	sumCol := res.ComputedCols[5]
	require.NotNil(t, sumCol)
	require.True(t, sumCol.IsNull(0))
	require.True(t, sumCol.IsNull(1))
	require.True(t, sumCol.IsNull(2))
}

func TestCountAndNonNullSumReal(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 2

	// [0]=delta_count(*), [1]=delta_sum(x), [2]=group_key, [3]=mv_count(*), [4]=mv_sum(x)
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftReal := types.NewFieldType(mysql.TypeDouble)
	fts := []*types.FieldType{ftInt, ftReal, ftInt, ftInt, ftReal}
	chk := chunk.NewChunkWithCapacity(fts, 3)

	chk.AppendInt64(0, 1)
	chk.AppendFloat64(1, 2.0)
	chk.AppendInt64(2, 1)
	chk.AppendInt64(3, 2)
	chk.AppendFloat64(4, 3.0)

	chk.AppendInt64(0, 1)
	chk.AppendFloat64(1, 1.5)
	chk.AppendInt64(2, 2)
	chk.AppendNull(3)
	chk.AppendNull(4)

	chk.AppendInt64(0, -1)
	chk.AppendFloat64(1, -8.0)
	chk.AppendInt64(2, 3)
	chk.AppendInt64(3, 1)
	chk.AppendFloat64(4, 8.0)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	sumArgTp := types.NewFieldType(mysql.TypeDouble)
	sumArgTp.AddFlag(mysql.NotNullFlag)
	sumArg := &expression.Column{Index: 1, RetType: sumArgTp}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{3}, DependencyColID: []int{0}},
			{AggFunc: sumDesc, ColID: []int{4}, DependencyColID: []int{1}},
		},
		DeltaAggColCount: 2,
		WorkerCnt:        2,
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Len(t, writer.results, 1)
	res := writer.results[0]
	require.Len(t, res.RowOps, 3)
	require.Equal(t, RowOpUpdate, res.RowOps[0].Tp)
	require.Equal(t, RowOpInsert, res.RowOps[1].Tp)
	require.Equal(t, RowOpDelete, res.RowOps[2].Tp)
	require.Equal(t, 1, res.UpdateTouchedStride)
	require.Equal(t, 2, res.UpdateTouchedBitCnt)
	require.Equal(t, []uint8{0x3}, res.UpdateTouchedBitmap)

	sumCol := res.ComputedCols[4]
	require.NotNil(t, sumCol)
	require.InDelta(t, 5.0, sumCol.GetFloat64(0), 1e-9)
	require.InDelta(t, 1.5, sumCol.GetFloat64(1), 1e-9)
	require.InDelta(t, 0.0, sumCol.GetFloat64(2), 1e-9)
}

func TestCountAndNonNullSumUnsignedInt(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 2

	// [0]=delta_count(*), [1]=delta_sum(x), [2]=mv_count(*), [3]=mv_sum(x unsigned)
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt.AddFlag(mysql.UnsignedFlag)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftUInt}
	chk := chunk.NewChunkWithCapacity(fts, 3)

	chk.AppendInt64(0, 0)
	chk.AppendInt64(1, -3)
	chk.AppendInt64(2, 1)
	chk.AppendUint64(3, 10)

	chk.AppendInt64(0, 1)
	chk.AppendInt64(1, 5)
	chk.AppendNull(2)
	chk.AppendNull(3)

	chk.AppendInt64(0, -1)
	chk.AppendInt64(1, -2)
	chk.AppendInt64(2, 1)
	chk.AppendUint64(3, 2)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	sumArgTp := types.NewFieldType(mysql.TypeLonglong)
	sumArgTp.AddFlag(mysql.NotNullFlag)
	sumArg := &expression.Column{Index: 1, RetType: sumArgTp}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{2}, DependencyColID: []int{0}},
			{AggFunc: sumDesc, ColID: []int{3}, DependencyColID: []int{1}},
		},
		DeltaAggColCount: 2,
		WorkerCnt:        2,
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Len(t, writer.results, 1)
	res := writer.results[0]
	require.Len(t, res.RowOps, 3)
	require.Equal(t, RowOpUpdate, res.RowOps[0].Tp)
	require.Equal(t, RowOpInsert, res.RowOps[1].Tp)
	require.Equal(t, RowOpDelete, res.RowOps[2].Tp)

	sumCol := res.ComputedCols[3]
	require.NotNil(t, sumCol)
	require.Equal(t, uint64(7), sumCol.GetUint64(0))
	require.Equal(t, uint64(5), sumCol.GetUint64(1))
	require.Equal(t, uint64(0), sumCol.GetUint64(2))
}

func TestCountAndNullableSumUnsignedInt(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 2

	// [0]=delta_count(*), [1]=delta_sum(b), [2]=delta_count(b), [3]=group_key,
	// [4]=mv_count(*), [5]=mv_sum(b unsigned), [6]=mv_count(b)
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt.AddFlag(mysql.UnsignedFlag)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftUInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 3)

	chk.AppendInt64(0, 0)
	chk.AppendInt64(1, -3)
	chk.AppendInt64(2, -1)
	chk.AppendInt64(3, 1)
	chk.AppendInt64(4, 1)
	chk.AppendUint64(5, 10)
	chk.AppendInt64(6, 2)

	chk.AppendInt64(0, 0)
	chk.AppendInt64(1, 0)
	chk.AppendInt64(2, -1)
	chk.AppendInt64(3, 2)
	chk.AppendInt64(4, 1)
	chk.AppendUint64(5, 5)
	chk.AppendInt64(6, 1)

	chk.AppendInt64(0, 1)
	chk.AppendInt64(1, 5)
	chk.AppendInt64(2, 2)
	chk.AppendInt64(3, 3)
	chk.AppendNull(4)
	chk.AppendNull(5)
	chk.AppendNull(6)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	countArg := &expression.Column{Index: 0, RetType: ftInt}
	countDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{countArg}, false)
	require.NoError(t, err)
	sumArg := &expression.Column{Index: 1, RetType: ftInt}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{4}, DependencyColID: []int{0}},
			{AggFunc: countDesc, ColID: []int{6}, DependencyColID: []int{2}},
			{AggFunc: sumDesc, ColID: []int{5}, DependencyColID: []int{1, 6}},
		},
		DeltaAggColCount: 3,
		WorkerCnt:        2,
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Len(t, writer.results, 1)
	res := writer.results[0]
	require.Len(t, res.RowOps, 3)
	require.Equal(t, RowOpUpdate, res.RowOps[0].Tp)
	require.Equal(t, RowOpUpdate, res.RowOps[1].Tp)
	require.Equal(t, RowOpInsert, res.RowOps[2].Tp)

	sumCol := res.ComputedCols[5]
	require.NotNil(t, sumCol)
	require.Equal(t, uint64(7), sumCol.GetUint64(0))
	require.True(t, sumCol.IsNull(1))
	require.Equal(t, uint64(5), sumCol.GetUint64(2))
}

func TestRejectSumUnsignedDelta(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 2

	// [0]=delta_count(*), [1]=delta_sum(x unsigned), [2]=mv_count(*), [3]=mv_sum(x signed)
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt.AddFlag(mysql.UnsignedFlag)
	fts := []*types.FieldType{ftInt, ftUInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 1)

	chk.AppendInt64(0, 0)
	chk.AppendUint64(1, 1<<63)
	chk.AppendInt64(2, 1)
	chk.AppendInt64(3, -(1<<63)+1)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	sumArgTp := types.NewFieldType(mysql.TypeLonglong)
	sumArgTp.AddFlag(mysql.NotNullFlag)
	sumArg := &expression.Column{Index: 1, RetType: sumArgTp}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{2}, DependencyColID: []int{0}},
			{AggFunc: sumDesc, ColID: []int{3}, DependencyColID: []int{1}},
		},
		DeltaAggColCount: 2,
		WorkerCnt:        2,
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	err = mergeExec.Open(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires signed delta dependency")
}

func TestRejectNullableSumCountDependencyFromDelta(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{
		ftInt, // [0] delta_count(*)
		ftInt, // [1] delta_sum(x)
		ftInt, // [2] delta_count(x) -- invalid for nullable sum dependency (must be final merged count)
		ftInt, // [3] mv_count(*)
		ftInt, // [4] mv_sum(x)
		ftInt, // [5] mv_count(x)
	}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	sumArg := &expression.Column{Index: 1, RetType: ftInt}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{3}, DependencyColID: []int{0}},
			{AggFunc: sumDesc, ColID: []int{4}, DependencyColID: []int{1, 2}},
		},
		DeltaAggColCount: 3,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "requires final COUNT(expr) from previously computed columns")
}

func TestRejectSumTypeMismatch(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftReal := types.NewFieldType(mysql.TypeDouble)
	// [0]=delta_count(*), [1]=delta_sum(x int), [2]=mv_count(*), [3]=mv_sum(x real)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftReal}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	sumArg := &expression.Column{Index: 1, RetType: ftInt}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{2}, DependencyColID: []int{0}},
			{AggFunc: sumDesc, ColID: []int{3}, DependencyColID: []int{1}},
		},
		DeltaAggColCount: 2,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "SUM mapping type mismatch")
}

func TestRejectCountTypeMismatch(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftReal := types.NewFieldType(mysql.TypeDouble)
	// [0]=delta_count(*) real (invalid), [1]=mv_count(*)
	fts := []*types.FieldType{ftReal, ftInt}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{1}, DependencyColID: []int{0}},
		},
		DeltaAggColCount: 1,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "COUNT mapping dependency eval type must be int")
}

func TestRejectCountUnsignedType(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt.AddFlag(mysql.UnsignedFlag)
	// [0]=delta_count(*) unsigned (invalid), [1]=mv_count(*)
	fts := []*types.FieldType{ftUInt, ftInt}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{1}, DependencyColID: []int{0}},
		},
		DeltaAggColCount: 1,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "COUNT mapping dependency type must be signed integer")
}

func TestRejectMinMaxForStage1(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	countArg := &expression.Column{Index: 0, RetType: ftInt}
	minDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMin, []expression.Expression{countArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{1}, DependencyColID: []int{0}},
			{AggFunc: minDesc, ColID: []int{2, 3}, DependencyColID: []int{0}},
		},
		DeltaAggColCount: 1,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "not implemented")
}

func TestRejectNilAggFunc(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt}
	src := newMockSource(sctx, fts, nil)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: nil, ColID: []int{1}, DependencyColID: []int{0}},
		},
	}
	err := mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "requires AggFunc")
}

func TestRejectFirstCountNotAllRows(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt}
	src := newMockSource(sctx, fts, nil)

	countArg := &expression.Column{Index: 0, RetType: ftInt}
	countDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{countArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countDesc, ColID: []int{1}, DependencyColID: []int{0}},
		},
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "must be COUNT(*)/COUNT(non-NULL constant)")
}

func TestAllowFirstCountConstTwo(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt}
	src := newMockSource(sctx, fts, nil)

	countTwoDesc, err := aggregation.NewAggFuncDesc(
		sctx.GetExprCtx(),
		ast.AggFuncCount,
		[]expression.Expression{&expression.Constant{
			Value:   types.NewIntDatum(2),
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}},
		false,
	)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countTwoDesc, ColID: []int{1}, DependencyColID: []int{0}},
		},
		DeltaAggColCount: 1,
	}
	require.NoError(t, mergeExec.Open(context.Background()))
	require.NoError(t, mergeExec.Close())
}

func TestRejectDependencyFromMVSide(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	sumArgTp := types.NewFieldType(mysql.TypeLonglong)
	sumArgTp.AddFlag(mysql.NotNullFlag)
	sumArg := &expression.Column{Index: 1, RetType: sumArgTp}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{1}, DependencyColID: []int{0}},
			// dependency col 2 is on MV side because DeltaAggColCount is 1.
			{AggFunc: sumDesc, ColID: []int{2}, DependencyColID: []int{2}},
		},
		DeltaAggColCount: 1,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "expect delta agg range")
}

func TestNoOpWhenAggValueUnchanged(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftReal := types.NewFieldType(mysql.TypeDouble)
	// [0]=delta_count(*), [1]=delta_sum(x), [2]=mv_count(*), [3]=mv_sum(x)
	fts := []*types.FieldType{ftInt, ftReal, ftInt, ftReal}
	chk := chunk.NewChunkWithCapacity(fts, 1)

	chk.AppendInt64(0, 0)
	chk.AppendFloat64(1, 0)
	chk.AppendInt64(2, 7)
	chk.AppendFloat64(3, 3.5)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	sumArgTp := types.NewFieldType(mysql.TypeDouble)
	sumArgTp.AddFlag(mysql.NotNullFlag)
	sumArg := &expression.Column{Index: 1, RetType: sumArgTp}
	sumDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncSum, []expression.Expression{sumArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{2}, DependencyColID: []int{0}},
			{AggFunc: sumDesc, ColID: []int{3}, DependencyColID: []int{1}},
		},
		DeltaAggColCount: 2,
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Len(t, writer.results, 1)
	require.Len(t, writer.results[0].RowOps, 1)
	require.Equal(t, RowOpNoOp, writer.results[0].RowOps[0].Tp)
	require.Equal(t, 1, writer.results[0].UpdateTouchedStride)
	require.Equal(t, 2, writer.results[0].UpdateTouchedBitCnt)
	require.Equal(t, uint8(0), writer.results[0].UpdateTouchedBitmap[0])
}

func TestMarkChangedRowsByColumnFloat32(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeFloat)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)

	chk.AppendFloat32(0, 1.5)
	chk.AppendFloat32(1, 1.5)

	chk.AppendFloat32(0, 2.5)
	chk.AppendFloat32(1, 2.0)

	chk.AppendNull(0)
	chk.AppendNull(1)

	chk.AppendNull(0)
	chk.AppendFloat32(1, 3.0)

	assertChangedMask(t, chk, ft, []bool{false, true, false, true})
}

func TestMarkChangedRowsByColumnString(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeVarString)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)

	chk.AppendString(0, "abc")
	chk.AppendString(1, "abc")

	chk.AppendString(0, "abc")
	chk.AppendString(1, "abd")

	chk.AppendNull(0)
	chk.AppendNull(1)

	chk.AppendNull(0)
	chk.AppendString(1, "x")

	assertChangedMask(t, chk, ft, []bool{false, true, false, true})
}

func TestMarkChangedRowsByColumnTime(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeDatetime)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)

	t1 := types.NewTime(types.FromDate(2026, 2, 1, 10, 0, 0, 0), mysql.TypeDatetime, 0)
	t2 := types.NewTime(types.FromDate(2026, 2, 1, 10, 0, 1, 0), mysql.TypeDatetime, 0)

	chk.AppendTime(0, t1)
	chk.AppendTime(1, t1)

	chk.AppendTime(0, t1)
	chk.AppendTime(1, t2)

	chk.AppendNull(0)
	chk.AppendNull(1)

	chk.AppendNull(0)
	chk.AppendTime(1, t1)

	assertChangedMask(t, chk, ft, []bool{false, true, false, true})
}

func TestMarkChangedRowsByColumnDuration(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeDuration)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)

	d1 := types.NewDuration(0, 0, 1, 0, 0)
	d2 := types.NewDuration(0, 0, 2, 0, 0)

	chk.AppendDuration(0, d1)
	chk.AppendDuration(1, d1)

	chk.AppendDuration(0, d1)
	chk.AppendDuration(1, d2)

	chk.AppendNull(0)
	chk.AppendNull(1)

	chk.AppendNull(0)
	chk.AppendDuration(1, d1)

	assertChangedMask(t, chk, ft, []bool{false, true, false, true})
}

func TestMarkChangedRowsByColumnJSON(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeJSON)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)

	j1 := types.CreateBinaryJSON(int64(1))
	j2 := types.CreateBinaryJSON(int64(2))

	chk.AppendJSON(0, j1)
	chk.AppendJSON(1, j1)

	chk.AppendJSON(0, j1)
	chk.AppendJSON(1, j2)

	chk.AppendNull(0)
	chk.AppendNull(1)

	chk.AppendNull(0)
	chk.AppendJSON(1, j1)

	assertChangedMask(t, chk, ft, []bool{false, true, false, true})
}

func TestMarkChangedRowsByColumnVector(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeTiDBVectorFloat32)
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)

	v1 := types.MustCreateVectorFloat32([]float32{1, 2})
	v2 := types.MustCreateVectorFloat32([]float32{1, 3})

	chk.AppendVectorFloat32(0, v1)
	chk.AppendVectorFloat32(1, v1)

	chk.AppendVectorFloat32(0, v1)
	chk.AppendVectorFloat32(1, v2)

	chk.AppendNull(0)
	chk.AppendNull(1)

	chk.AppendNull(0)
	chk.AppendVectorFloat32(1, v1)

	assertChangedMask(t, chk, ft, []bool{false, true, false, true})
}

func assertChangedMask(t *testing.T, chk *chunk.Chunk, ft *types.FieldType, expected []bool) {
	t.Helper()
	mask := make([]bool, chk.NumRows())
	err := markChangedRowsByColumn(mask, chk.Column(0), chk.Column(1), ft)
	require.NoError(t, err)
	require.Equal(t, expected, mask)
}
