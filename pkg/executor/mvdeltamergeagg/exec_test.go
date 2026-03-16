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
	"time"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
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

type mockSingleRowRecomputeExec struct {
	exec.BaseExecutor
	keyCols []*expression.CorrelatedColumn
	values  map[int64]int64
	done    bool
	calls   int
}

func (m *mockSingleRowRecomputeExec) Open(context.Context) error {
	m.done = false
	return nil
}

func (m *mockSingleRowRecomputeExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if m.done {
		return nil
	}
	m.calls++
	key := m.keyCols[0].Data.GetInt64()
	if v, ok := m.values[key]; ok {
		req.AppendInt64(0, v)
	} else {
		req.AppendNull(0)
	}
	m.done = true
	return nil
}

type mockSingleRowRecomputeExecMulti struct {
	exec.BaseExecutor
	keyCols []*expression.CorrelatedColumn
	values  map[int64][]int64
	done    bool
}

func (m *mockSingleRowRecomputeExecMulti) Open(context.Context) error {
	m.done = false
	return nil
}

func (m *mockSingleRowRecomputeExecMulti) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if m.done {
		return nil
	}
	key := m.keyCols[0].Data.GetInt64()
	if vs, ok := m.values[key]; ok {
		for _, v := range vs {
			req.AppendInt64(0, v)
		}
	}
	m.done = true
	return nil
}

type mockBatchRecomputeExec struct {
	exec.BaseExecutor
	rows [][2]int64
	idx  int
}

func (m *mockBatchRecomputeExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	for m.idx < len(m.rows) && req.NumRows() < req.Capacity() {
		req.AppendInt64(0, m.rows[m.idx][0])
		req.AppendInt64(1, m.rows[m.idx][1])
		m.idx++
	}
	return nil
}

type mockBatchRecomputeBuilder struct {
	sctx          sessionctx.Context
	retTp         []*types.FieldType
	values        map[int64]int64
	reverseOutput bool
	extraRows     [][2]int64
}

func (b *mockBatchRecomputeBuilder) Build(_ context.Context, req *MinMaxBatchBuildRequest) (exec.Executor, error) {
	rows := make([][2]int64, 0, len(req.LookupKeys))
	for _, key := range req.LookupKeys {
		k := key.Keys[0].GetInt64()
		rows = append(rows, [2]int64{k, b.values[k]})
	}
	rows = append(rows, b.extraRows...)
	if b.reverseOutput {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}
	cols := make([]*expression.Column, len(b.retTp))
	for i := range b.retTp {
		cols[i] = &expression.Column{Index: i, RetType: b.retTp[i]}
	}
	return &mockBatchRecomputeExec{
		BaseExecutor: exec.NewBaseExecutor(b.sctx, expression.NewSchema(cols...), 0),
		rows:         rows,
	}, nil
}

func TestCountAndNullableSum(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 2

	// Schema: [0] delta_count(*), [1] delta_sum(b), [2] delta_count(b), [3] group_key,
	// [4] mv_count(*), [5] mv_sum(b), [6] mv_count(b).
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

	// Schema: [0] delta_count(*), [1] delta_sum(x), [2] group_key, [3] mv_count(*), [4] mv_sum(x).
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

	// Schema: [0] delta_count(*), [1] delta_sum(x), [2] mv_count(*), [3] mv_sum(x unsigned).
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

	// Schema: [0] delta_count(*), [1] delta_sum(b), [2] delta_count(b), [3] group_key,
	// [4] mv_count(*), [5] mv_sum(b unsigned), [6] mv_count(b).
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

	// Schema: [0] delta_count(*), [1] delta_sum(x unsigned), [2] mv_count(*), [3] mv_sum(x signed).
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
		ftInt, // [2] delta_count(x), invalid for nullable SUM dependency.
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
	require.ErrorContains(t, err, "must come from previously computed columns")
}

func TestRejectMinMaxDeltaDependencyFromComputed(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{
		ftInt, // [0] delta_count(*)
		ftInt, // [1] delta_min_added(v)
		ftInt, // [2] delta_min_added_cnt(v)
		ftInt, // [3] delta_min_removed(v)
		ftInt, // [4] delta_min_removed_cnt(v)
		ftInt, // [5] group key
		ftInt, // [6] mv_count(*) output of mapping[0]
		ftInt, // [7] mv_min(v)
	}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	minArgTp := types.NewFieldType(mysql.TypeLonglong)
	minArgTp.AddFlag(mysql.NotNullFlag)
	minArg := &expression.Column{Index: 1, RetType: minArgTp}
	minDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMin, []expression.Expression{minArg}, false)
	require.NoError(t, err)

	for _, depIdx := range []int{0, 1, 2, 3} {
		deps := []int{1, 2, 3, 4}
		deps[depIdx] = 6 // force dependency to come from computed output col.
		mergeExec := &Exec{
			BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
			AggMappings: []Mapping{
				{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
				{
					AggFunc:         minDesc,
					ColID:           []int{7},
					DependencyColID: deps,
					MinMaxRecompute: &MinMaxRecomputeSpec{
						Strategy:            MinMaxRecomputeBatch,
						BatchResultColIdxes: []int{1},
					},
				},
			},
			DeltaAggColCount: 6,
			MinMaxRecompute: &MinMaxRecomputeExec{
				KeyInputColIDs: []int{5},
				BatchBuilder:   &mockBatchRecomputeBuilder{},
			},
		}
		err = mergeExec.Open(context.Background())
		require.ErrorContains(t, err, "must come from delta aggregation input")
	}
}

func TestRejectSumDeltaDependencyFromComputed(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{
		ftInt, // [0] delta_count(*)
		ftInt, // [1] delta_sum(x)
		ftInt, // [2] mv_count(*)
		ftInt, // [3] mv_sum(x)
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
			{AggFunc: countStarDesc, ColID: []int{2}, DependencyColID: []int{0}},
			// Dependency col 2 points to previously computed COUNT output, invalid for SUM delta dependency.
			{AggFunc: sumDesc, ColID: []int{3}, DependencyColID: []int{2}},
		},
		DeltaAggColCount: 2,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "SUM mapping delta dependency col 2: must come from delta aggregation input")
}

func TestRejectSumTypeMismatch(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftReal := types.NewFieldType(mysql.TypeDouble)
	// Schema: [0] delta_count(*), [1] delta_sum(x int), [2] mv_count(*), [3] mv_sum(x real).
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
	// Schema: [0] delta_count(*) real (invalid), [1] mv_count(*).
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
	require.ErrorContains(t, err, "COUNT mapping dependency: eval type must be int")
}

func TestRejectCountUnsignedType(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt := types.NewFieldType(mysql.TypeLonglong)
	ftUInt.AddFlag(mysql.UnsignedFlag)
	// Schema: [0] delta_count(*) unsigned (invalid), [1] mv_count(*).
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
	require.ErrorContains(t, err, "COUNT mapping dependency: type must be signed integer")
}

func TestRejectCountDependencyFromComputed(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{
		ftInt, // [0] delta_count(*)
		ftInt, // [1] delta_count(x)
		ftInt, // [2] mv_count(*)
		ftInt, // [3] mv_count(x)
	}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	countArg := &expression.Column{Index: 1, RetType: ftInt}
	countExprDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{countArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{2}, DependencyColID: []int{0}},
			// Dependency col 2 points to previously computed COUNT(*) output, invalid for COUNT delta dependency.
			{AggFunc: countExprDesc, ColID: []int{3}, DependencyColID: []int{2}},
		},
		DeltaAggColCount: 2,
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "COUNT mapping dependency col 2: must come from delta aggregation input")
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
	require.ErrorContains(t, err, "requires MinMaxRecompute metadata")
}

func TestMaxFastPathAndSingleRowRecompute(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	// Schema:
	// [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
	// [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
	// [6] mv_count(*), [7] mv_max(v).
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 3)

	// row0: fast update max 10 -> 12
	chk.AppendInt64(0, 0)
	chk.AppendInt64(1, 12)
	chk.AppendInt64(2, 1)
	chk.AppendNull(3)
	chk.AppendInt64(4, 0)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	// row1: fallback (remove old max 10), single-row recompute returns 9
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 2)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	// row2: no change
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendNull(3)
	chk.AppendInt64(4, 0)
	chk.AppendInt64(5, 3)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 5)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	keyDatum := new(types.Datum)
	keyCol := &expression.CorrelatedColumn{
		Column: expression.Column{Index: 0, RetType: ftInt},
		Data:   keyDatum,
	}
	recomputeExec := &mockSingleRowRecomputeExec{
		BaseExecutor: exec.NewBaseExecutor(
			sctx,
			expression.NewSchema(&expression.Column{Index: 0, RetType: ftInt}),
			0,
		),
		keyCols: []*expression.CorrelatedColumn{keyCol},
		values: map[int64]int64{
			2: 9,
		},
	}

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy: MinMaxRecomputeSingleRow,
					SingleRow: &MinMaxRecomputeSingleRowExec{
						Workers: []MinMaxRecomputeSingleRowWorker{
							{
								KeyCols: []*expression.CorrelatedColumn{keyCol},
								Exec:    recomputeExec,
							},
						},
					},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
		},
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
	require.Equal(t, RowOpNoOp, res.RowOps[2].Tp)

	maxCol := res.ComputedCols[7]
	require.NotNil(t, maxCol)
	require.Equal(t, int64(12), maxCol.GetInt64(0))
	require.Equal(t, int64(9), maxCol.GetInt64(1))
	require.Equal(t, int64(5), maxCol.GetInt64(2))
}

func TestRejectMaxSingleRowRecomputeMultipleRows(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	// Schema:
	// [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
	// [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
	// [6] mv_count(*), [7] mv_max(v).
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 1)

	// Force fallback recompute for key=1.
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	keyDatum := new(types.Datum)
	keyCol := &expression.CorrelatedColumn{
		Column: expression.Column{Index: 0, RetType: ftInt},
		Data:   keyDatum,
	}
	recomputeExec := &mockSingleRowRecomputeExecMulti{
		BaseExecutor: exec.NewBaseExecutor(
			sctx,
			expression.NewSchema(&expression.Column{Index: 0, RetType: ftInt}),
			0,
		),
		keyCols: []*expression.CorrelatedColumn{keyCol},
		values: map[int64][]int64{
			1: {9, 8},
		},
	}

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy: MinMaxRecomputeSingleRow,
					SingleRow: &MinMaxRecomputeSingleRowExec{
						Workers: []MinMaxRecomputeSingleRowWorker{
							{
								KeyCols: []*expression.CorrelatedColumn{keyCol},
								Exec:    recomputeExec,
							},
						},
					},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
		},
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	err = mergeExec.Next(context.Background(), outChk)
	require.ErrorContains(t, err, "returns more than one row")
	require.NoError(t, mergeExec.Close())
}

func TestMaxBatchRecompute(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	// Schema:
	// [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
	// [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
	// [6] mv_count(*), [7] mv_max(v).
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 2)

	// row0: fallback, batch recompute returns 8
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	// row1: fast update max 10 -> 12
	chk.AppendInt64(0, 0)
	chk.AppendInt64(1, 12)
	chk.AppendInt64(2, 1)
	chk.AppendNull(3)
	chk.AppendInt64(4, 0)
	chk.AppendInt64(5, 2)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	batchBuilder := &mockBatchRecomputeBuilder{
		sctx:  sctx,
		retTp: []*types.FieldType{ftInt, ftInt},
		values: map[int64]int64{
			1: 8,
			2: 12,
		},
	}
	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy:            MinMaxRecomputeBatch,
					BatchResultColIdxes: []int{1},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
			BatchBuilder:   batchBuilder,
		},
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Len(t, writer.results, 1)
	res := writer.results[0]
	require.Len(t, res.RowOps, 2)
	require.Equal(t, RowOpUpdate, res.RowOps[0].Tp)
	require.Equal(t, RowOpUpdate, res.RowOps[1].Tp)

	maxCol := res.ComputedCols[7]
	require.NotNil(t, maxCol)
	require.Equal(t, int64(8), maxCol.GetInt64(0))
	require.Equal(t, int64(12), maxCol.GetInt64(1))
}

func TestMaxBatchRecomputeWithOutOfOrderResultRows(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	// Schema:
	// [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
	// [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
	// [6] mv_count(*), [7] mv_max(v).
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 2)

	// row0: fallback, batch recompute returns 8.
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	// row1: fallback, batch recompute returns 15.
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 20)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 2)
	chk.AppendInt64(6, 4)
	chk.AppendInt64(7, 20)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	// Return rows in reverse key order to verify batch result order does not affect overrides.
	batchBuilder := &mockBatchRecomputeBuilder{
		sctx:          sctx,
		retTp:         []*types.FieldType{ftInt, ftInt},
		reverseOutput: true,
		values: map[int64]int64{
			1: 8,
			2: 15,
		},
	}
	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy:            MinMaxRecomputeBatch,
					BatchResultColIdxes: []int{1},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
			BatchBuilder:   batchBuilder,
		},
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Len(t, writer.results, 1)
	res := writer.results[0]
	require.Len(t, res.RowOps, 2)
	require.Equal(t, RowOpUpdate, res.RowOps[0].Tp)
	require.Equal(t, RowOpUpdate, res.RowOps[1].Tp)

	maxCol := res.ComputedCols[7]
	require.NotNil(t, maxCol)
	require.Equal(t, int64(8), maxCol.GetInt64(0))
	require.Equal(t, int64(15), maxCol.GetInt64(1))
}

func TestRejectMaxBatchRecomputeUnexpectedKey(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 1)

	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	batchBuilder := &mockBatchRecomputeBuilder{
		sctx:  sctx,
		retTp: []*types.FieldType{ftInt, ftInt},
		values: map[int64]int64{
			1: 8,
		},
		extraRows: [][2]int64{{999, 1}},
	}
	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy:            MinMaxRecomputeBatch,
					BatchResultColIdxes: []int{1},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
			BatchBuilder:   batchBuilder,
		},
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	err = mergeExec.Next(context.Background(), outChk)
	require.ErrorContains(t, err, "returns an unexpected key")
	require.NoError(t, mergeExec.Close())
}

func TestRejectMaxBatchRecomputeKeyTypeMismatch(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	// Schema:
	// [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
	// [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
	// [6] mv_count(*), [7] mv_max(v).
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftReal := types.NewFieldType(mysql.TypeDouble)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 1)

	// fallback path, must trigger batch recompute.
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	// key type mismatch: batch result key type is DOUBLE, input key type is BIGINT.
	batchBuilder := &mockBatchRecomputeBuilder{
		sctx:  sctx,
		retTp: []*types.FieldType{ftReal, ftInt},
		values: map[int64]int64{
			1: 8,
		},
	}
	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy:            MinMaxRecomputeBatch,
					BatchResultColIdxes: []int{1},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
			BatchBuilder:   batchBuilder,
		},
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	err = mergeExec.Next(context.Background(), outChk)
	require.ErrorContains(t, err, "min/max batch key type mismatch")
	require.NoError(t, mergeExec.Close())
}

func TestRejectMaxBatchRecomputeResultTypeMismatch(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	// Schema:
	// [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
	// [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
	// [6] mv_count(*), [7] mv_max(v).
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	ftReal := types.NewFieldType(mysql.TypeDouble)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 1)

	// fallback path, must trigger batch recompute.
	chk.AppendInt64(0, 0)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	// result type mismatch: output max(v) is BIGINT, but batch result col is DOUBLE.
	batchBuilder := &mockBatchRecomputeBuilder{
		sctx:  sctx,
		retTp: []*types.FieldType{ftInt, ftReal},
		values: map[int64]int64{
			1: 8,
		},
	}
	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy:            MinMaxRecomputeBatch,
					BatchResultColIdxes: []int{1},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
			BatchBuilder:   batchBuilder,
		},
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	err = mergeExec.Next(context.Background(), outChk)
	require.ErrorContains(t, err, "min/max batch result type mismatch")
	require.NoError(t, mergeExec.Close())
}

func TestMinMaxFallbackToCountStarWithoutFinalCountDependency(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().ExecutorConcurrency = 1

	// Schema:
	// [0] delta_count(*), [1] delta_max_added(v), [2] delta_max_added_cnt(v),
	// [3] delta_max_removed(v), [4] delta_max_removed_cnt(v), [5] group_key,
	// [6] mv_count(*), [7] mv_max(v).
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt, ftInt}
	chk := chunk.NewChunkWithCapacity(fts, 1)

	// Delete all rows in this group. MAX should short-circuit to NULL using fallback COUNT(*),
	// and must not trigger recompute.
	chk.AppendInt64(0, -3)
	chk.AppendNull(1)
	chk.AppendInt64(2, 0)
	chk.AppendInt64(3, 10)
	chk.AppendInt64(4, 1)
	chk.AppendInt64(5, 1)
	chk.AppendInt64(6, 3)
	chk.AppendInt64(7, 10)

	src := newMockSource(sctx, fts, []*chunk.Chunk{chk})
	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	keyDatum := new(types.Datum)
	keyCol := &expression.CorrelatedColumn{
		Column: expression.Column{Index: 0, RetType: ftInt},
		Data:   keyDatum,
	}
	recomputeExec := &mockSingleRowRecomputeExec{
		BaseExecutor: exec.NewBaseExecutor(
			sctx,
			expression.NewSchema(&expression.Column{Index: 0, RetType: ftInt}),
			0,
		),
		keyCols: []*expression.CorrelatedColumn{keyCol},
		values: map[int64]int64{
			1: 9,
		},
	}

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy: MinMaxRecomputeSingleRow,
					SingleRow: &MinMaxRecomputeSingleRowExec{
						Workers: []MinMaxRecomputeSingleRowWorker{
							{
								KeyCols: []*expression.CorrelatedColumn{keyCol},
								Exec:    recomputeExec,
							},
						},
					},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
		},
	}
	writer := &collectWriter{}
	mergeExec.Writer = writer

	require.NoError(t, mergeExec.Open(context.Background()))
	outChk := exec.NewFirstChunk(mergeExec)
	require.NoError(t, mergeExec.Next(context.Background(), outChk))
	require.NoError(t, mergeExec.Close())

	require.Equal(t, 0, recomputeExec.calls)
	require.Len(t, writer.results, 1)
	res := writer.results[0]
	require.Len(t, res.RowOps, 1)
	require.Equal(t, RowOpDelete, res.RowOps[0].Tp)

	maxCol := res.ComputedCols[7]
	require.NotNil(t, maxCol)
	require.True(t, maxCol.IsNull(0))
}

func TestRejectMinMaxFallbackToCountStarForNullableExpr(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{
		ftInt, // [0] delta_count(*)
		ftInt, // [1] delta_max_added(v)
		ftInt, // [2] delta_max_added_cnt(v)
		ftInt, // [3] delta_max_removed(v)
		ftInt, // [4] delta_max_removed_cnt(v)
		ftInt, // [5] group key
		ftInt, // [6] mv_count(*)
		ftInt, // [7] mv_max(v)
	}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	// RetType without NotNullFlag means nullable expression.
	maxArg := &expression.Column{Index: 1, RetType: ftInt}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy:            MinMaxRecomputeBatch,
					BatchResultColIdxes: []int{1},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
			BatchBuilder:   &mockBatchRecomputeBuilder{},
		},
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "max(nullable expr) requires final-count dependency")
}

func TestRejectMinMaxFinalCountForNonNullableExpr(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)
	fts := []*types.FieldType{
		ftInt, // [0] delta_count(*)
		ftInt, // [1] delta_max_added(v)
		ftInt, // [2] delta_max_added_cnt(v)
		ftInt, // [3] delta_max_removed(v)
		ftInt, // [4] delta_max_removed_cnt(v)
		ftInt, // [5] group key
		ftInt, // [6] mv_count(*)
		ftInt, // [7] mv_max(v)
	}
	src := newMockSource(sctx, fts, nil)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	maxArgTp := types.NewFieldType(mysql.TypeLonglong)
	maxArgTp.AddFlag(mysql.NotNullFlag)
	maxArg := &expression.Column{Index: 1, RetType: maxArgTp}
	maxDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMax, []expression.Expression{maxArg}, false)
	require.NoError(t, err)

	mergeExec := &Exec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0, src),
		AggMappings: []Mapping{
			{AggFunc: countStarDesc, ColID: []int{6}, DependencyColID: []int{0}},
			{
				AggFunc:         maxDesc,
				ColID:           []int{7},
				DependencyColID: []int{1, 2, 3, 4, 6},
				MinMaxRecompute: &MinMaxRecomputeSpec{
					Strategy:            MinMaxRecomputeBatch,
					BatchResultColIdxes: []int{1},
				},
			},
		},
		DeltaAggColCount: 6,
		WorkerCnt:        1,
		MinMaxRecompute: &MinMaxRecomputeExec{
			KeyInputColIDs: []int{5},
			BatchBuilder:   &mockBatchRecomputeBuilder{},
		},
	}
	err = mergeExec.Open(context.Background())
	require.ErrorContains(t, err, "max(non-nullable expr) must not set final-count dependency")
}

func TestRejectMinMaxRecomputeValidation(t *testing.T) {
	sctx := mock.NewContext()
	ftInt := types.NewFieldType(mysql.TypeLonglong)

	countStarDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncCount, []expression.Expression{expression.NewOne()}, false)
	require.NoError(t, err)
	minArg := &expression.Column{Index: 0, RetType: ftInt}
	minDesc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), ast.AggFuncMin, []expression.Expression{minArg}, false)
	require.NoError(t, err)

	cases := []struct {
		name          string
		fieldTypes    []*types.FieldType
		aggMappings   []Mapping
		minMax        *MinMaxRecomputeExec
		expectedError string
	}{
		{
			name:       "non-minmax mapping with recompute metadata",
			fieldTypes: []*types.FieldType{ftInt, ftInt},
			aggMappings: []Mapping{
				{
					AggFunc:         countStarDesc,
					ColID:           []int{1},
					DependencyColID: []int{0},
					MinMaxRecompute: &MinMaxRecomputeSpec{
						Strategy:            MinMaxRecomputeBatch,
						BatchResultColIdxes: []int{1},
					},
				},
			},
			minMax: &MinMaxRecomputeExec{
				KeyInputColIDs: []int{0},
				BatchBuilder:   &mockBatchRecomputeBuilder{},
			},
			expectedError: "set for non-MIN/MAX",
		},
		{
			name:       "batch strategy without builder",
			fieldTypes: []*types.FieldType{ftInt, ftInt, ftInt},
			aggMappings: []Mapping{
				{AggFunc: countStarDesc, ColID: []int{1}, DependencyColID: []int{0}},
				{
					AggFunc:         minDesc,
					ColID:           []int{2},
					DependencyColID: []int{0},
					MinMaxRecompute: &MinMaxRecomputeSpec{
						Strategy:            MinMaxRecomputeBatch,
						BatchResultColIdxes: []int{1},
					},
				},
			},
			minMax: &MinMaxRecomputeExec{
				KeyInputColIDs: []int{0},
			},
			expectedError: "batch strategy requires BatchBuilder",
		},
		{
			name:       "batch result index overlaps key prefix",
			fieldTypes: []*types.FieldType{ftInt, ftInt, ftInt},
			aggMappings: []Mapping{
				{AggFunc: countStarDesc, ColID: []int{1}, DependencyColID: []int{0}},
				{
					AggFunc:         minDesc,
					ColID:           []int{2},
					DependencyColID: []int{0},
					MinMaxRecompute: &MinMaxRecomputeSpec{
						Strategy:            MinMaxRecomputeBatch,
						BatchResultColIdxes: []int{0},
					},
				},
			},
			minMax: &MinMaxRecomputeExec{
				KeyInputColIDs: []int{0},
				BatchBuilder:   &mockBatchRecomputeBuilder{},
			},
			expectedError: "conflicts with key prefix",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := newMockSource(sctx, tc.fieldTypes, nil)
			mergeExec := &Exec{
				BaseExecutor:     exec.NewBaseExecutor(sctx, nil, 0, src),
				AggMappings:      tc.aggMappings,
				DeltaAggColCount: 1,
				MinMaxRecompute:  tc.minMax,
			}
			err := mergeExec.Open(context.Background())
			require.ErrorContains(t, err, tc.expectedError)
		})
	}
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
			// Dependency col 2 is on MV side (DeltaAggColCount = 1).
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
	// Schema: [0] delta_count(*), [1] delta_sum(x), [2] mv_count(*), [3] mv_sum(x).
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

func TestMarkUpdateTouchedRowsByColumnStringCollation(t *testing.T) {
	sctx := mock.NewContext()
	ft := types.NewFieldType(mysql.TypeVarString)
	ft.SetCharset("utf8mb4")
	ft.SetCollate("utf8mb4_general_ci")
	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft}, 4)

	chk.AppendString(0, "A")
	chk.AppendString(1, "a")

	chk.AppendString(0, "Ab")
	chk.AppendString(1, "ac")

	chk.AppendNull(0)
	chk.AppendNull(1)

	chk.AppendNull(0)
	chk.AppendString(1, "x")

	updateRows := []int{0, 1, 2, 3}
	updateChanged := make([]bool, len(updateRows))
	updateTouchedBitmap := make([]uint8, len(updateRows))
	err := markUpdateTouchedRowsByColumn(
		updateRows,
		updateChanged,
		updateTouchedBitmap,
		1,
		0,
		chk.Column(0),
		chk.Column(1),
		ft,
		sctx.GetSessionVars().StmtCtx.TypeCtx(),
	)
	require.NoError(t, err)
	require.Equal(t, []bool{true, true, false, true}, updateChanged)
	require.Equal(t, []uint8{1, 1, 0, 1}, updateTouchedBitmap)
}

func TestMarkUpdateTouchedRowsByColumnEnumSetUseDatumBinaryCompare(t *testing.T) {
	sctx := mock.NewContext()

	enumFT := types.NewFieldType(mysql.TypeEnum)
	enumFT.SetCharset("utf8mb4")
	enumFT.SetCollate("utf8mb4_general_ci")
	enumChk := chunk.NewChunkWithCapacity([]*types.FieldType{enumFT, enumFT}, 1)
	enumChk.AppendEnum(0, types.Enum{Name: "x", Value: 1})
	enumChk.AppendEnum(1, types.Enum{Name: "x", Value: 2})

	enumChanged := make([]bool, 1)
	enumBitmap := make([]uint8, 1)
	err := markUpdateTouchedRowsByColumn(
		[]int{0},
		enumChanged,
		enumBitmap,
		1,
		0,
		enumChk.Column(0),
		enumChk.Column(1),
		enumFT,
		sctx.GetSessionVars().StmtCtx.TypeCtx(),
	)
	require.NoError(t, err)
	require.Equal(t, []bool{false}, enumChanged)
	require.Equal(t, []uint8{0}, enumBitmap)

	setFT := types.NewFieldType(mysql.TypeSet)
	setFT.SetCharset("utf8mb4")
	setFT.SetCollate("utf8mb4_general_ci")
	setChk := chunk.NewChunkWithCapacity([]*types.FieldType{setFT, setFT}, 1)
	setChk.AppendSet(0, types.Set{Name: "a,b", Value: 3})
	setChk.AppendSet(1, types.Set{Name: "a,b", Value: 7})

	setChanged := make([]bool, 1)
	setBitmap := make([]uint8, 1)
	err = markUpdateTouchedRowsByColumn(
		[]int{0},
		setChanged,
		setBitmap,
		1,
		0,
		setChk.Column(0),
		setChk.Column(1),
		setFT,
		sctx.GetSessionVars().StmtCtx.TypeCtx(),
	)
	require.NoError(t, err)
	require.Equal(t, []bool{false}, setChanged)
	require.Equal(t, []uint8{0}, setBitmap)
}

func TestMVDeltaMergeAggRuntimeStatsString(t *testing.T) {
	stats := newMVDeltaMergeAggRuntimeStats(3)
	stats.fillFromPipelineStats(&mvDeltaMergeAggPipelineStats{
		readerTime:      15 * time.Millisecond,
		writerTime:      8 * time.Millisecond,
		mergeWorkerTime: []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 0},
	})
	s := stats.String()
	require.Contains(t, s, "mv_delta_merge_agg")
	require.Contains(t, s, "total:3")
	require.Contains(t, s, "active:2")
	require.Contains(t, s, "min:10ms")
	require.Contains(t, s, "max:20ms")
	require.Contains(t, s, "avg:15ms")
	require.Contains(t, s, "reader:15ms")
	require.Contains(t, s, "writer:{time:8ms")
	require.Contains(t, s, "chunks:0")
	require.Contains(t, s, "row_ops:0")
}

func TestMVDeltaMergeAggRuntimeStatsMergeAndClone(t *testing.T) {
	left := newMVDeltaMergeAggRuntimeStats(2)
	left.fillFromPipelineStats(&mvDeltaMergeAggPipelineStats{
		readerTime:      3 * time.Millisecond,
		writerTime:      4 * time.Millisecond,
		mergeWorkerTime: []time.Duration{5 * time.Millisecond, 7 * time.Millisecond},
		writerDetail: mvDeltaMergeAggWriterStats{
			chunks:     1,
			rowOps:     10,
			insertRows: 3,
			updateRows: 5,
			deleteRows: 2,
		},
	})
	right := newMVDeltaMergeAggRuntimeStats(3)
	right.fillFromPipelineStats(&mvDeltaMergeAggPipelineStats{
		readerTime:      2 * time.Millisecond,
		writerTime:      1 * time.Millisecond,
		mergeWorkerTime: []time.Duration{1 * time.Millisecond, 2 * time.Millisecond, 9 * time.Millisecond},
		writerDetail: mvDeltaMergeAggWriterStats{
			chunks:     2,
			rowOps:     7,
			insertRows: 1,
			updateRows: 4,
			deleteRows: 2,
		},
	})

	left.Merge(right)
	require.Equal(t, 5*time.Millisecond, left.readerTime)
	require.Equal(t, 5*time.Millisecond, left.writerTime)
	require.Equal(t, []time.Duration{
		6 * time.Millisecond,
		9 * time.Millisecond,
		9 * time.Millisecond,
	}, left.mergeWorkerTime)
	require.Equal(t, int64(3), left.writerDetail.chunks)
	require.Equal(t, int64(17), left.writerDetail.rowOps)
	require.Equal(t, int64(4), left.writerDetail.insertRows)
	require.Equal(t, int64(9), left.writerDetail.updateRows)
	require.Equal(t, int64(4), left.writerDetail.deleteRows)

	cloned, ok := left.Clone().(*mvDeltaMergeAggRuntimeStats)
	require.True(t, ok)
	require.Equal(t, left.readerTime, cloned.readerTime)
	require.Equal(t, left.writerTime, cloned.writerTime)
	require.Equal(t, left.mergeWorkerTime, cloned.mergeWorkerTime)
	require.Equal(t, left.writerDetail, cloned.writerDetail)
	require.Equal(t, execdetails.TpMVDeltaMergeAggRuntimeStats, left.Tp())
}
