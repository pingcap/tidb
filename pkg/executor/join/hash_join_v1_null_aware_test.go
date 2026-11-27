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

package join

import (
	"context"
	"math/rand"
	"sort"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

// NOTE: These tests manually construct HashJoinV1Exec and a reference evaluator because
// the planner does not yet emit null-aware LeftOuterSemiJoin plans. Once planner support
// is in place, this test scaffolding can be simplified (e.g. replaced with SQL-level tests).

type chunkSliceExec struct {
	exec.BaseExecutor
	chunks []*chunk.Chunk
	cursor int
}

func newChunkSliceExec(ctx sessionctx.Context, fields []*types.FieldType, chunks []*chunk.Chunk) *chunkSliceExec {
	schema := buildSchema(fields)
	for i, col := range schema.Columns {
		col.Index = i
	}
	return &chunkSliceExec{
		BaseExecutor: exec.NewBaseExecutor(ctx, schema, 0),
		chunks:       chunks,
	}
}

func (e *chunkSliceExec) Open(context.Context) error {
	e.cursor = 0
	return nil
}

func (e *chunkSliceExec) Next(_ context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= len(e.chunks) {
		return nil
	}
	src := e.chunks[e.cursor]
	e.cursor++
	for i := range src.NumRows() {
		req.AppendRow(src.GetRow(i))
	}
	return nil
}

func (e *chunkSliceExec) Close() error {
	return nil
}

type hashJoinV1NAConfig struct {
	ctx             sessionctx.Context
	joinType        base.JoinType
	leftTypes       []*types.FieldType
	rightTypes      []*types.FieldType
	leftNAKeyIdx    []int
	rightNAKeyIdx   []int
	probeKeyTypes   []*types.FieldType
	buildKeyTypes   []*types.FieldType
	outerFilter     expression.CNFExprs
	otherConditions expression.CNFExprs
	childrenUsed    [][]int
	concurrency     uint
	spill           bool
}

func buildChunksFromRows(t *testing.T, fields []*types.FieldType, rows [][]any, chunkSize int) []*chunk.Chunk {
	t.Helper()
	if len(rows) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = len(rows)
	}
	var chunks []*chunk.Chunk
	for start := 0; start < len(rows); start += chunkSize {
		end := min(start+chunkSize, len(rows))
		chk := chunk.New(fields, end-start, max(end-start, 1))
		for _, r := range rows[start:end] {
			require.Len(t, r, len(fields))
			for colIdx, val := range r {
				if val == nil {
					chk.AppendNull(colIdx)
					continue
				}
				switch fields[colIdx].GetType() {
				case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeTiny:
					chk.AppendInt64(colIdx, val.(int64))
				case mysql.TypeVarchar, mysql.TypeString:
					chk.AppendString(colIdx, val.(string))
				default:
					t.Fatalf("unsupported test field type %v", fields[colIdx].GetType())
				}
			}
		}
		chunks = append(chunks, chk)
	}
	return chunks
}

func buildHashJoinV1NAExec(cfg *hashJoinV1NAConfig, leftChunks, rightChunks []*chunk.Chunk) *HashJoinV1Exec {
	concurrency := cfg.concurrency
	if concurrency == 0 {
		concurrency = 2
	}
	leftExec := newChunkSliceExec(cfg.ctx, cfg.leftTypes, leftChunks)
	rightExec := newChunkSliceExec(cfg.ctx, cfg.rightTypes, rightChunks)

	outTypes := make([]*types.FieldType, 0, len(cfg.leftTypes)+1)
	if cfg.childrenUsed != nil {
		for _, idx := range cfg.childrenUsed[0] {
			outTypes = append(outTypes, cfg.leftTypes[idx])
		}
	} else {
		outTypes = append(outTypes, cfg.leftTypes...)
	}
	outTypes = append(outTypes, types.NewFieldType(mysql.TypeTiny))
	outSchema := buildSchema(outTypes)

	probeKeyTypes := cfg.probeKeyTypes
	if probeKeyTypes == nil {
		probeKeyTypes = make([]*types.FieldType, len(cfg.leftNAKeyIdx))
		for i, colIdx := range cfg.leftNAKeyIdx {
			probeKeyTypes[i] = cfg.leftTypes[colIdx].Clone()
		}
	}
	buildKeyTypes := cfg.buildKeyTypes
	if buildKeyTypes == nil {
		buildKeyTypes = make([]*types.FieldType, len(cfg.rightNAKeyIdx))
		for i, colIdx := range cfg.rightNAKeyIdx {
			buildKeyTypes[i] = cfg.rightTypes[colIdx].Clone()
		}
	}

	e := &HashJoinV1Exec{
		BaseExecutor:          exec.NewBaseExecutor(cfg.ctx, outSchema, 1, leftExec, rightExec),
		ProbeSideTupleFetcher: &ProbeSideTupleFetcherV1{},
		ProbeWorkers:          make([]*ProbeWorkerV1, concurrency),
		BuildWorker:           &BuildWorkerV1{},
		HashJoinCtxV1: &HashJoinCtxV1{
			IsOuterJoin: cfg.joinType.IsOuterJoin(),
			ProbeTypes:  probeKeyTypes,
			BuildTypes:  buildKeyTypes,
			OuterFilter: cfg.outerFilter,
		},
	}
	e.HashJoinCtxV1.SessCtx = cfg.ctx
	e.HashJoinCtxV1.JoinType = cfg.joinType
	e.HashJoinCtxV1.Concurrency = concurrency
	e.HashJoinCtxV1.ChunkAllocPool = e.AllocPool
	e.HashJoinCtxV1.IsNullAware = true

	e.ProbeSideTupleFetcher.ProbeSideExec = leftExec
	e.BuildWorker.BuildSideExec = rightExec
	e.BuildWorker.BuildNAKeyColIdx = cfg.rightNAKeyIdx
	e.BuildWorker.HashJoinCtx = e.HashJoinCtxV1

	defaultValues := make([]types.Datum, len(cfg.rightTypes))
	for i := range concurrency {
		worker := &ProbeWorkerV1{
			HashJoinCtx:      e.HashJoinCtxV1,
			ProbeNAKeyColIdx: cfg.leftNAKeyIdx,
			Joiner: NewJoiner(
				cfg.ctx, cfg.joinType, false, defaultValues,
				cfg.otherConditions, cfg.leftTypes, cfg.rightTypes, cfg.childrenUsed, true,
			),
		}
		worker.WorkerID = i
		e.ProbeWorkers[i] = worker
	}
	return e
}

func runHashJoinV1NAExec(t *testing.T, cfg *hashJoinV1NAConfig, leftChunks, rightChunks []*chunk.Chunk) ([]chunk.Row, bool) {
	t.Helper()
	if cfg.spill {
		memTracker := memory.NewTracker(-1, 1)
		cfg.ctx.GetSessionVars().MemTracker = memTracker
		cfg.ctx.GetSessionVars().StmtCtx.MemTracker.AttachTo(memTracker)
	}
	e := buildHashJoinV1NAExec(cfg, leftChunks, rightChunks)
	tmpCtx := context.Background()
	require.NoError(t, e.Open(tmpCtx))
	defer func() {
		require.NoError(t, e.Close())
	}()

	var chunks []*chunk.Chunk
	for {
		chk := exec.NewFirstChunk(e)
		require.NoError(t, e.Next(tmpCtx, chk))
		if chk.NumRows() == 0 {
			break
		}
		chunks = append(chunks, chk)
	}
	spilled := e.RowContainer != nil && e.RowContainer.AlreadySpilledSafeForTest()
	return sortRows(chunks, exec.RetTypes(e)), spilled
}

// evalNullAwareOuterSemiRef computes the exact SQL 3-valued reference output for
// null-aware LeftOuterSemiJoin (IN) and AntiLeftOuterSemiJoin (NOT IN).
func evalNullAwareOuterSemiRef(
	t *testing.T,
	cfg *hashJoinV1NAConfig,
	leftChunks, rightChunks []*chunk.Chunk,
) []chunk.Row {
	t.Helper()
	isAnti := cfg.joinType == base.AntiLeftOuterSemiJoin
	require.True(t, cfg.joinType == base.LeftOuterSemiJoin || isAnti)

	probeKeyTypes := cfg.probeKeyTypes
	if probeKeyTypes == nil {
		probeKeyTypes = make([]*types.FieldType, len(cfg.leftNAKeyIdx))
		for i, colIdx := range cfg.leftNAKeyIdx {
			probeKeyTypes[i] = cfg.leftTypes[colIdx].Clone()
		}
	}
	buildKeyTypes := cfg.buildKeyTypes
	if buildKeyTypes == nil {
		buildKeyTypes = make([]*types.FieldType, len(cfg.rightNAKeyIdx))
		for i, colIdx := range cfg.rightNAKeyIdx {
			buildKeyTypes[i] = cfg.rightTypes[colIdx].Clone()
		}
	}

	var lUsed []int
	outTypes := make([]*types.FieldType, 0, len(cfg.leftTypes)+1)
	if cfg.childrenUsed != nil {
		lUsed = cfg.childrenUsed[0]
		for _, idx := range lUsed {
			outTypes = append(outTypes, cfg.leftTypes[idx])
		}
	} else {
		outTypes = append(outTypes, cfg.leftTypes...)
	}
	outTypes = append(outTypes, types.NewFieldType(mysql.TypeTiny))

	shallowTypes := make([]*types.FieldType, 0, len(cfg.leftTypes)+len(cfg.rightTypes))
	shallowTypes = append(shallowTypes, cfg.leftTypes...)
	shallowTypes = append(shallowTypes, cfg.rightTypes...)
	shallowRow := chunk.MutRowFromTypes(shallowTypes)

	resChk := chunk.New(outTypes, 16, 1024)
	evalCtx := cfg.ctx.GetExprCtx().GetEvalCtx()
	typeCtx := cfg.ctx.GetSessionVars().StmtCtx.TypeCtx()
	var selected []bool

	for _, lChk := range leftChunks {
		var err error
		selected, err = expression.VectorizedFilter(
			evalCtx, cfg.ctx.GetSessionVars().EnableVectorizedExpression,
			cfg.outerFilter, chunk.NewIterator4Chunk(lChk), selected,
		)
		require.NoError(t, err)

		for lIdx := range lChk.NumRows() {
			lRow := lChk.GetRow(lIdx)
			lWide := resChk.AppendRowByColIdxs(lRow, lUsed)
			if !selected[lIdx] {
				if isAnti {
					resChk.AppendInt64(lWide, 1)
				} else {
					resChk.AppendInt64(lWide, 0)
				}
				continue
			}

			// Evaluate SQL 3-valued IN across all right rows that satisfy OtherConditions:
			//   true  if any valid right row has all NA keys equal (non-null);
			//   null  else if any valid right row has no false NA-key comparison (at least one null);
			//   false otherwise.
			foundTrue := false
			foundNull := false
			for _, rChk := range rightChunks {
				for rIdx := range rChk.NumRows() {
					rRow := rChk.GetRow(rIdx)
					if len(cfg.otherConditions) > 0 {
						shallowRow.ShallowCopyPartialRow(0, lRow)
						shallowRow.ShallowCopyPartialRow(len(cfg.leftTypes), rRow)
						valid, _, err := expression.EvalBool(evalCtx, cfg.otherConditions, shallowRow.ToRow())
						require.NoError(t, err)
						if !valid {
							continue
						}
					}

					rowFalse := false
					rowNull := false
					for k := range cfg.leftNAKeyIdx {
						lCol, rCol := cfg.leftNAKeyIdx[k], cfg.rightNAKeyIdx[k]
						if lRow.IsNull(lCol) || rRow.IsNull(rCol) {
							rowNull = true
							continue
						}
						eq, err := codec.EqualChunkRow(
							typeCtx,
							lRow, []*types.FieldType{probeKeyTypes[k]}, []int{lCol},
							rRow, []*types.FieldType{buildKeyTypes[k]}, []int{rCol},
						)
						require.NoError(t, err)
						if !eq {
							rowFalse = true
							break
						}
					}
					if rowFalse {
						continue
					}
					if !rowNull {
						foundTrue = true
						break
					}
					foundNull = true
				}
				if foundTrue {
					break
				}
			}

			switch {
			case foundTrue:
				if isAnti {
					resChk.AppendInt64(lWide, 0)
				} else {
					resChk.AppendInt64(lWide, 1)
				}
			case foundNull:
				resChk.AppendNull(lWide)
			default:
				if isAnti {
					resChk.AppendInt64(lWide, 1)
				} else {
					resChk.AppendInt64(lWide, 0)
				}
			}
		}
	}
	return sortRows([]*chunk.Chunk{resChk}, outTypes)
}

func formatOutputRows(rows []chunk.Row, fieldTypes []*types.FieldType) [][]any {
	out := make([][]any, len(rows))
	for i, r := range rows {
		vals := make([]any, len(fieldTypes))
		for colIdx, ft := range fieldTypes {
			if r.IsNull(colIdx) {
				vals[colIdx] = nil
				continue
			}
			switch ft.GetType() {
			case mysql.TypeLonglong, mysql.TypeLong, mysql.TypeTiny:
				vals[colIdx] = r.GetInt64(colIdx)
			case mysql.TypeVarchar, mysql.TypeString:
				vals[colIdx] = r.GetString(colIdx)
			}
		}
		out[i] = vals
	}
	return out
}

func sortExpectedTuples(rows [][]any) [][]any {
	copied := make([][]any, len(rows))
	copy(copied, rows)
	sort.Slice(copied, func(i, j int) bool {
		for col := range copied[i] {
			a, b := copied[i][col], copied[j][col]
			if a == nil && b == nil {
				continue
			}
			if a == nil {
				return true
			}
			if b == nil {
				return false
			}
			switch av := a.(type) {
			case int64:
				bv := b.(int64)
				if av != bv {
					return av < bv
				}
			case string:
				bv := b.(string)
				if av != bv {
					return av < bv
				}
			}
		}
		return false
	})
	return copied
}

func TestHashJoinV1NullAwareLeftOuterSemiJoinSingleKey(t *testing.T) {
	intType := types.NewFieldType(mysql.TypeLonglong)
	flagType := types.NewFieldType(mysql.TypeTiny)
	fields := []*types.FieldType{intType}
	outFields := []*types.FieldType{intType, flagType}

	cases := []struct {
		name     string
		left     [][]any
		right    [][]any
		wantSemi [][]any
		wantAnti [][]any
	}{
		{
			name:     "empty build side",
			left:     [][]any{{int64(1)}, {int64(2)}, {nil}},
			right:    nil,
			wantSemi: [][]any{{int64(1), int64(0)}, {int64(2), int64(0)}, {nil, int64(0)}},
			wantAnti: [][]any{{int64(1), int64(1)}, {int64(2), int64(1)}, {nil, int64(1)}},
		},
		{
			name:     "non-null build only",
			left:     [][]any{{int64(1)}, {int64(2)}, {int64(3)}, {nil}},
			right:    [][]any{{int64(1)}, {int64(3)}},
			wantSemi: [][]any{{int64(1), int64(1)}, {int64(2), int64(0)}, {int64(3), int64(1)}, {nil, nil}},
			wantAnti: [][]any{{int64(1), int64(0)}, {int64(2), int64(1)}, {int64(3), int64(0)}, {nil, nil}},
		},
		{
			name:     "null-only build side",
			left:     [][]any{{int64(1)}, {nil}},
			right:    [][]any{{nil}},
			wantSemi: [][]any{{int64(1), nil}, {nil, nil}},
			wantAnti: [][]any{{int64(1), nil}, {nil, nil}},
		},
		{
			name:     "same-key match takes priority over null bucket",
			left:     [][]any{{int64(1)}, {int64(2)}, {nil}},
			right:    [][]any{{nil}, {int64(1)}, {nil}, {int64(1)}},
			wantSemi: [][]any{{int64(1), int64(1)}, {int64(2), nil}, {nil, nil}},
			wantAnti: [][]any{{int64(1), int64(0)}, {int64(2), nil}, {nil, nil}},
		},
		{
			name:     "empty probe side",
			left:     nil,
			right:    [][]any{{int64(1)}, {nil}},
			wantSemi: [][]any{},
			wantAnti: [][]any{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			leftChunks := buildChunksFromRows(t, fields, tc.left, 2)
			rightChunks := buildChunksFromRows(t, fields, tc.right, 2)

			semiCfg := &hashJoinV1NAConfig{
				ctx:           defaultCtx(),
				joinType:      base.LeftOuterSemiJoin,
				leftTypes:     fields,
				rightTypes:    fields,
				leftNAKeyIdx:  []int{0},
				rightNAKeyIdx: []int{0},
				concurrency:   2,
			}
			semiGot, _ := runHashJoinV1NAExec(t, semiCfg, leftChunks, rightChunks)
			require.Equal(t, sortExpectedTuples(tc.wantSemi), formatOutputRows(semiGot, outFields))
			checkResults(t, outFields, semiGot, evalNullAwareOuterSemiRef(t, semiCfg, leftChunks, rightChunks))

			antiCfg := &hashJoinV1NAConfig{
				ctx:           defaultCtx(),
				joinType:      base.AntiLeftOuterSemiJoin,
				leftTypes:     fields,
				rightTypes:    fields,
				leftNAKeyIdx:  []int{0},
				rightNAKeyIdx: []int{0},
				concurrency:   2,
			}
			antiGot, _ := runHashJoinV1NAExec(t, antiCfg, leftChunks, rightChunks)
			require.Equal(t, sortExpectedTuples(tc.wantAnti), formatOutputRows(antiGot, outFields))
			checkResults(t, outFields, antiGot, evalNullAwareOuterSemiRef(t, antiCfg, leftChunks, rightChunks))
		})
	}
}

func TestHashJoinV1NullAwareLeftOuterSemiJoinMultiKey(t *testing.T) {
	intType := types.NewFieldType(mysql.TypeLonglong)
	flagType := types.NewFieldType(mysql.TypeTiny)
	fields := []*types.FieldType{intType, intType}
	outFields := []*types.FieldType{intType, intType, flagType}

	probeRows := [][]any{
		{int64(1), int64(2)},
		{int64(1), int64(9)},
		{int64(3), int64(2)},
		{int64(3), int64(9)},
		{int64(1), nil},
		{int64(3), nil},
		{nil, int64(2)},
		{nil, int64(9)},
		{nil, nil},
	}

	buildSets := []struct {
		name  string
		right [][]any
	}{
		{"empty", nil},
		{"non-null only", [][]any{{int64(1), int64(2)}, {int64(2), int64(3)}}},
		{"partial null first key only", [][]any{{int64(1), nil}}},
		{"partial null disjoint columns", [][]any{{int64(1), nil}, {nil, int64(3)}}},
		{"all null row", [][]any{{nil, nil}}},
		{
			"mixed exact and partial null",
			[][]any{
				{int64(1), int64(2)},
				{int64(1), nil},
				{int64(4), nil},
				{nil, int64(2)},
				{nil, int64(5)},
			},
		},
	}

	for _, bs := range buildSets {
		t.Run(bs.name, func(t *testing.T) {
			leftChunks := buildChunksFromRows(t, fields, probeRows, 3)
			rightChunks := buildChunksFromRows(t, fields, bs.right, 2)
			cfg := &hashJoinV1NAConfig{
				ctx:           defaultCtx(),
				joinType:      base.LeftOuterSemiJoin,
				leftTypes:     fields,
				rightTypes:    fields,
				leftNAKeyIdx:  []int{0, 1},
				rightNAKeyIdx: []int{0, 1},
				concurrency:   3,
			}
			got, _ := runHashJoinV1NAExec(t, cfg, leftChunks, rightChunks)
			want := evalNullAwareOuterSemiRef(t, cfg, leftChunks, rightChunks)
			checkResults(t, outFields, got, want)
		})
	}
}

func TestHashJoinV1NullAwareLeftOuterSemiJoinConditionsAndFilters(t *testing.T) {
	ctx := defaultCtx()
	intType := types.NewFieldType(mysql.TypeLonglong)
	flagType := types.NewFieldType(mysql.TypeTiny)
	// Left schema:  [id, key, c, d]
	// Right schema: [key, c]
	leftTypes := []*types.FieldType{intType, intType, intType, intType}
	rightTypes := []*types.FieldType{intType, intType}
	outFields := []*types.FieldType{intType, intType, intType, intType, flagType}

	// OtherConditions: left.c (col 2) < right.c (col 5 in shallow row [l0,l1,l2,l3,r0,r1])
	otherCond, err := expression.NewFunction(
		ctx.GetExprCtx(), ast.LT, types.NewFieldType(mysql.TypeTiny),
		&expression.Column{Index: 2, RetType: intType},
		&expression.Column{Index: 5, RetType: intType},
	)
	require.NoError(t, err)

	// OuterFilter: left.d (col 3) > 0
	outerFilter, err := expression.NewFunction(
		ctx.GetExprCtx(), ast.GT, types.NewFieldType(mysql.TypeTiny),
		&expression.Column{Index: 3, RetType: intType},
		expression.NewZero(),
	)
	require.NoError(t, err)

	leftRows := [][]any{
		// id=1: key=1 matches same-key (1, 100) AND null-bucket (NULL, 100) -> 1
		{int64(1), int64(1), int64(50), int64(1)},
		// id=2: key=2 same-key (2, 10) fails 50 < 10, null-bucket (NULL, 100) passes -> NULL
		{int64(2), int64(2), int64(50), int64(1)},
		// id=3: key=2 with c=200 fails all rows (10 and 100) -> 0
		{int64(3), int64(2), int64(200), int64(1)},
		// id=4: key=1 with c=NULL makes otherCond evaluate to NULL for all right rows -> 0
		{int64(4), int64(1), nil, int64(1)},
		// id=5: key=NULL, null-bucket (NULL, 100) fails 150 < 100, hash-table (3, 200) passes -> NULL
		{int64(5), nil, int64(150), int64(1)},
		// id=6: key=NULL, c=500 fails all rows in both buckets -> 0
		{int64(6), nil, int64(500), int64(1)},
		// id=7: rejected by OuterFilter (d=0) despite matching key=1 -> 0
		{int64(7), int64(1), int64(50), int64(0)},
		// id=8: rejected by OuterFilter (d=NULL) despite key=NULL -> 0
		{int64(8), nil, int64(50), nil},
	}
	rightRows := [][]any{
		{int64(1), int64(100)},
		{int64(2), int64(10)},
		{int64(2), nil},
		{int64(3), int64(200)},
		{nil, int64(100)},
		{nil, nil},
	}

	leftChunks := buildChunksFromRows(t, leftTypes, leftRows, 3)
	rightChunks := buildChunksFromRows(t, rightTypes, rightRows, 2)
	cfg := &hashJoinV1NAConfig{
		ctx:             ctx,
		joinType:        base.LeftOuterSemiJoin,
		leftTypes:       leftTypes,
		rightTypes:      rightTypes,
		leftNAKeyIdx:    []int{1},
		rightNAKeyIdx:   []int{0},
		outerFilter:     expression.CNFExprs{outerFilter},
		otherConditions: expression.CNFExprs{otherCond},
		concurrency:     2,
	}

	got, _ := runHashJoinV1NAExec(t, cfg, leftChunks, rightChunks)
	want := evalNullAwareOuterSemiRef(t, cfg, leftChunks, rightChunks)
	checkResults(t, outFields, got, want)

	// Explicitly assert the expected flag column per row id.
	wantFlags := map[int64]any{
		1: int64(1),
		2: nil,
		3: int64(0),
		4: int64(0),
		5: nil,
		6: int64(0),
		7: int64(0),
		8: int64(0),
	}
	for _, r := range formatOutputRows(got, outFields) {
		id := r[0].(int64)
		require.Equal(t, wantFlags[id], r[4], "unexpected flag for row id=%d", id)
	}
}

func TestHashJoinV1NullAwareLeftOuterSemiJoinInlineProjChunkingSpillAndCollation(t *testing.T) {
	intType := types.NewFieldType(mysql.TypeLonglong)
	flagType := types.NewFieldType(mysql.TypeTiny)

	t.Run("inline projection, small chunks, concurrency, and disk spill", func(t *testing.T) {
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/executor/join/testRowContainerSpill", "return(true)")
		prevSpill := vardef.EnableTmpStorageOnOOM.Swap(true)
		defer vardef.EnableTmpStorageOnOOM.Store(prevSpill)

		leftTypes := []*types.FieldType{intType, intType, intType}
		rightTypes := []*types.FieldType{intType, intType, intType}

		rng := rand.New(rand.NewSource(42))
		genVal := func() any {
			v := rng.Intn(6)
			if v == 0 {
				return nil
			}
			return int64(v)
		}

		leftRows := make([][]any, 60)
		for i := range leftRows {
			leftRows[i] = []any{int64(i), genVal(), genVal()}
		}
		rightRows := make([][]any, 40)
		for i := range rightRows {
			rightRows[i] = []any{genVal(), genVal(), genVal()}
		}

		for _, spill := range []bool{false, true} {
			for _, childrenUsed := range [][][]int{
				nil,
				{{2, 0}, {}},
				{{}, {}},
			} {
				ctx := defaultCtx()
				ctx.GetSessionVars().InitChunkSize = 3
				ctx.GetSessionVars().MaxChunkSize = 3

				// OtherCondition: left.col2 < right.col2 (cols 2 and 5 in shallow row)
				otherCond, err := expression.NewFunction(
					ctx.GetExprCtx(), ast.LT, types.NewFieldType(mysql.TypeTiny),
					&expression.Column{Index: 2, RetType: intType},
					&expression.Column{Index: 5, RetType: intType},
				)
				require.NoError(t, err)

				leftChunks := buildChunksFromRows(t, leftTypes, leftRows, 3)
				rightChunks := buildChunksFromRows(t, rightTypes, rightRows, 3)

				cfg := &hashJoinV1NAConfig{
					ctx:             ctx,
					joinType:        base.LeftOuterSemiJoin,
					leftTypes:       leftTypes,
					rightTypes:      rightTypes,
					leftNAKeyIdx:    []int{1, 2},
					rightNAKeyIdx:   []int{0, 1},
					otherConditions: expression.CNFExprs{otherCond},
					childrenUsed:    childrenUsed,
					concurrency:     4,
					spill:           spill,
				}
				var outFields []*types.FieldType
				if childrenUsed != nil {
					for _, idx := range childrenUsed[0] {
						outFields = append(outFields, leftTypes[idx])
					}
				} else {
					outFields = append(outFields, leftTypes...)
				}
				outFields = append(outFields, flagType)

				got, spilled := runHashJoinV1NAExec(t, cfg, leftChunks, rightChunks)
				require.Equal(t, spill, spilled)
				want := evalNullAwareOuterSemiRef(t, cfg, leftChunks, rightChunks)
				checkResults(t, outFields, got, want)
			}
		}
	})

	t.Run("case-insensitive collation", func(t *testing.T) {
		ciStrType := types.NewFieldTypeBuilder().
			SetType(mysql.TypeVarchar).
			SetCharset("utf8mb4").
			SetCollate("utf8mb4_general_ci").
			BuildP()
		fields := []*types.FieldType{ciStrType, ciStrType}
		outFields := []*types.FieldType{ciStrType, ciStrType, flagType}

		leftRows := [][]any{
			{"a", "b"}, // exact ci match with ("A", "B") -> 1
			{"c", "d"}, // ci match with null-bucket ("C", NULL) -> NULL
			{"x", "d"}, // no match with ("A","B") or ("C",NULL) -> 0
			{"a", nil}, // matches ("A", "B") on col 0 -> NULL
			{"z", nil}, // fails col 0 on all build rows -> 0
		}
		rightRows := [][]any{
			{"A", "B"},
			{"C", nil},
		}

		leftChunks := buildChunksFromRows(t, fields, leftRows, 2)
		rightChunks := buildChunksFromRows(t, fields, rightRows, 2)
		cfg := &hashJoinV1NAConfig{
			ctx:           defaultCtx(),
			joinType:      base.LeftOuterSemiJoin,
			leftTypes:     fields,
			rightTypes:    fields,
			leftNAKeyIdx:  []int{0, 1},
			rightNAKeyIdx: []int{0, 1},
			concurrency:   2,
		}
		got, _ := runHashJoinV1NAExec(t, cfg, leftChunks, rightChunks)
		want := evalNullAwareOuterSemiRef(t, cfg, leftChunks, rightChunks)
		checkResults(t, outFields, got, want)
		require.Equal(t, sortExpectedTuples([][]any{
			{"a", "b", int64(1)},
			{"c", "d", nil},
			{"x", "d", int64(0)},
			{"a", nil, nil},
			{"z", nil, int64(0)},
		}), formatOutputRows(got, outFields))
	})
}
