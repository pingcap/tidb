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

package join

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func defaultCtx() sessionctx.Context {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = vardef.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = vardef.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, ctx.GetSessionVars().MemQuotaQuery)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	ctx.GetSessionVars().SnapshotTS = uint64(1)
	ctx.BindDomainAndSchValidator(domain.NewMockDomain(), nil)
	return ctx
}

func TestJoinerOtherConditionChunkUsesInitChunkSize(t *testing.T) {
	lfields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	rfields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
	fields := append(append([]*types.FieldType{}, lfields...), rfields...)
	conditions := []expression.Expression{expression.NewOne()}
	defaultInner := []types.Datum{types.NewIntDatum(0)}

	for _, joinType := range []base.JoinType{base.InnerJoin, base.LeftOuterJoin, base.RightOuterJoin} {
		ctx := defaultCtx()
		initChunkSize := 8
		ctx.GetSessionVars().InitChunkSize = initChunkSize
		maxChunkSize := ctx.GetSessionVars().MaxChunkSize
		joiner := NewJoiner(ctx, joinType, false, defaultInner, conditions, lfields, rfields, nil, false)
		base := joinerBaseForTest(t, joiner)
		require.NotNil(t, base.chk)
		require.Equal(t, initChunkSize, base.chk.Capacity())

		cloned := joiner.Clone()
		clonedBase := joinerBaseForTest(t, cloned)
		require.NotNil(t, clonedBase.chk)
		require.Equal(t, initChunkSize, clonedBase.chk.Capacity())

		outerRow := genTestChunk(maxChunkSize, 1, lfields).GetRow(0)
		innerChk := genTestChunk(maxChunkSize, initChunkSize+1, rfields)
		result := chunk.New(fields, maxChunkSize, maxChunkSize)
		iter := chunk.NewIterator4Chunk(innerChk)
		iter.Begin()
		_, _, err := joiner.TryToMatchInners(outerRow, iter, result)
		require.NoError(t, err)
		require.Equal(t, initChunkSize+1, result.NumRows())
	}
}

func joinerBaseForTest(t *testing.T, joiner Joiner) *baseJoiner {
	switch j := joiner.(type) {
	case *innerJoiner:
		return &j.baseJoiner
	case *leftOuterJoiner:
		return &j.baseJoiner
	case *rightOuterJoiner:
		return &j.baseJoiner
	default:
		t.Fatalf("unexpected joiner type %T", joiner)
	}
	return nil
}

func TestJoinerJoinedRows(t *testing.T) {
	t.Run("joined rows on outer panic", func(t *testing.T) {
		fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
		for _, kind := range []base.JoinType{base.SemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin, base.AntiSemiJoin} {
			for _, panicAt := range []string{"append", "iterator-next"} {
				if kind == base.AntiSemiJoin && panicAt == "append" {
					// Anti-semi matches produce no output, so there is no append to fail.
					continue
				}
				t.Run(kind.String()+"/"+panicAt, func(t *testing.T) {
					j := NewJoiner(defaultCtx(), kind, false, []types.Datum{types.NewDatum(nil)}, nil, fields, fields, nil, false)
					j.resetJoinedRows(true)
					input := genTestChunk(4, 4, fields)
					var iter chunk.Iterator = chunk.NewIterator4Chunk(input)
					// An empty schema makes the first output append panic.
					outputFields := []*types.FieldType{}
					if panicAt == "iterator-next" {
						iter = &joinedRowsPanicIterator{iter}
						outputFields = append(fields, fields...)
					}
					iter.Begin()
					result := chunk.New(outputFields, 4, 4)
					match := func() { _, _ = j.TryToMatchOuters(iter, input.GetRow(0), result, nil) }
					if panicAt == "iterator-next" {
						require.PanicsWithValue(t, "iterator panic", match)
					} else {
						require.Panics(t, match)
					}
					stats := execdetails.NewRuntimeStatsColl(nil)
					j.registerJoinedRows(stats, 1)
					rows, found := stats.GetRootJoinedRows(1)
					require.True(t, found)
					require.Equal(t, int64(1), rows)
				})
			}
		}
	})

	t.Run("joined rows before residual filtering", func(t *testing.T) {
		fields := []*types.FieldType{types.NewFieldType(mysql.TypeLong)}
		for _, joinType := range []base.JoinType{
			base.InnerJoin, base.LeftOuterJoin, base.RightOuterJoin, base.SemiJoin,
			base.AntiSemiJoin, base.LeftOuterSemiJoin, base.AntiLeftOuterSemiJoin,
		} {
			for _, isNA := range []bool{false, true} {
				for _, tc := range []struct {
					name         string
					conditions   []expression.Expression
					wantSemiRows int64
				}{
					{"none", nil, 1},
					{"true", []expression.Expression{expression.NewOne()}, 1},
					{"false", []expression.Expression{expression.NewZero()}, 4},
					{"null", []expression.Expression{expression.NewNull()}, 4},
				} {
					t.Run(joinType.String()+"/null-aware="+strconv.FormatBool(isNA)+"/"+tc.name, func(t *testing.T) {
						joiner := NewJoiner(defaultCtx(), joinType, false, []types.Datum{types.NewDatum(nil)}, tc.conditions, fields, fields, nil, isNA)
						joiner.resetJoinedRows(true)
						outer := genTestChunk(4, 1, fields).GetRow(0)
						inners := genTestChunk(4, 4, fields)
						iter := chunk.NewIterator4Chunk(inners)
						iter.Begin()
						result := chunk.New(append(fields, fields...), 4, 4)
						result.SetRequiredRows(2, 4)
						for iter.Current() != iter.End() {
							result.Reset()
							_, _, err := joiner.TryToMatchInners(outer, iter, result, LeftNotNullRightNotNull)
							require.NoError(t, err)
						}
						// Null-extension and anti/semi output must not add candidate pairs.
						result.Reset()
						joiner.OnMissMatch(false, outer, result)
						stats := execdetails.NewRuntimeStatsColl(nil)
						joiner.registerJoinedRows(stats, 1)
						rows, found := stats.GetRootJoinedRows(1)
						require.True(t, found)
						want := int64(4)
						if joinType != base.InnerJoin && joinType != base.LeftOuterJoin && joinType != base.RightOuterJoin {
							want = tc.wantSemiRows // Semi joins stop after the first matching candidate.
						}
						require.Equal(t, want, rows)

						// Publishing twice or opening an empty execution cannot duplicate prior work.
						joiner.registerJoinedRows(stats, 1)
						joiner.resetJoinedRows(true)
						joiner.registerJoinedRows(stats, 1)
						rows, _ = stats.GetRootJoinedRows(1)
						require.Equal(t, want, rows)

						if !isNA {
							// Building the outer side must count every evaluated outer pair.
							joiner.resetJoinedRows(true)
							iter.Begin()
							for iter.Current() != iter.End() {
								result.Reset()
								_, err := joiner.TryToMatchOuters(iter, outer, result, nil)
								require.NoError(t, err)
							}
							joiner.registerJoinedRows(stats, 2)
							rows, found = stats.GetRootJoinedRows(2)
							require.True(t, found)
							require.Equal(t, int64(4), rows)
						}
					})
				}
			}
		}
	})
}

func TestRequiredRows(t *testing.T) {
	joinTypes := []base.JoinType{base.InnerJoin, base.LeftOuterJoin, base.RightOuterJoin}
	lTypes := [][]byte{
		{mysql.TypeLong},
		{mysql.TypeFloat},
		{mysql.TypeLong, mysql.TypeFloat},
	}
	rTypes := lTypes

	convertTypes := func(mysqlTypes []byte) []*types.FieldType {
		fieldTypes := make([]*types.FieldType, 0, len(mysqlTypes))
		for _, t := range mysqlTypes {
			fieldTypes = append(fieldTypes, types.NewFieldType(t))
		}
		return fieldTypes
	}

	for _, joinType := range joinTypes {
		for _, ltype := range lTypes {
			for _, rtype := range rTypes {
				maxChunkSize := defaultCtx().GetSessionVars().MaxChunkSize
				lfields := convertTypes(ltype)
				rfields := convertTypes(rtype)
				outerRow := genTestChunk(maxChunkSize, 1, lfields).GetRow(0)
				innerChk := genTestChunk(maxChunkSize, maxChunkSize, rfields)
				var defaultInner []types.Datum
				for i, f := range rfields {
					defaultInner = append(defaultInner, innerChk.GetRow(0).GetDatum(i, f))
				}
				joiner := NewJoiner(defaultCtx(), joinType, false, defaultInner, nil, lfields, rfields, nil, false)

				fields := make([]*types.FieldType, 0, len(lfields)+len(rfields))
				fields = append(fields, rfields...)
				fields = append(fields, lfields...)
				result := chunk.New(fields, maxChunkSize, maxChunkSize)

				for range 10 {
					required := rand.Int()%maxChunkSize + 1
					result.SetRequiredRows(required, maxChunkSize)
					result.Reset()
					it := chunk.NewIterator4Chunk(innerChk)
					it.Begin()
					_, _, err := joiner.TryToMatchInners(outerRow, it, result)
					require.NoError(t, err)
					require.Equal(t, required, result.NumRows())
				}
			}
		}
	}
}

func genTestChunk(maxChunkSize int, numRows int, fields []*types.FieldType) *chunk.Chunk {
	chk := chunk.New(fields, maxChunkSize, maxChunkSize)
	for numRows > 0 {
		numRows--
		for col, field := range fields {
			switch field.GetType() {
			case mysql.TypeLong:
				chk.AppendInt64(col, 0)
			case mysql.TypeFloat:
				chk.AppendFloat32(col, 0)
			default:
				panic("not support")
			}
		}
	}
	return chk
}

// Inject an iterator failure after the first candidate has been processed.
type joinedRowsPanicIterator struct{ chunk.Iterator }

func (*joinedRowsPanicIterator) Next() chunk.Row { panic("iterator panic") }

type errIterator struct {
	chunk.Iterator
	err error
}

func (e *errIterator) Error() error { return e.err }

func TestNullAwareLeftOuterSemiJoiner(t *testing.T) {
	ctx := defaultCtx()
	intType := types.NewFieldType(mysql.TypeLonglong)
	flagType := types.NewFieldType(mysql.TypeTiny)
	lfields := []*types.FieldType{intType, intType}
	rfields := []*types.FieldType{intType, intType}
	outFields := []*types.FieldType{intType, intType, flagType}

	makeRows := func(fields []*types.FieldType, rows [][]any) *chunk.Chunk {
		chk := chunk.New(fields, max(len(rows), 1), max(len(rows), 1))
		for _, r := range rows {
			for colIdx, v := range r {
				if v == nil {
					chk.AppendNull(colIdx)
				} else {
					chk.AppendInt64(colIdx, v.(int64))
				}
			}
		}
		return chk
	}

	t.Run("no other conditions", func(t *testing.T) {
		j := NewJoiner(ctx, base.LeftOuterSemiJoin, false, nil, nil, lfields, rfields, nil, true)
		_, ok := j.(*nullAwareLeftOuterSemiJoiner)
		require.True(t, ok)
		require.Equal(t, base.LeftOuterSemiJoin, JoinerType(j))
		require.True(t, j.isSemiJoinWithoutCondition())

		cloned := j.Clone()
		_, ok = cloned.(*nullAwareLeftOuterSemiJoiner)
		require.True(t, ok)
		require.Equal(t, base.LeftOuterSemiJoin, JoinerType(cloned))

		outerChk := makeRows(lfields, [][]any{{int64(10), int64(20)}})
		outerRow := outerChk.GetRow(0)
		innerChk := makeRows(rfields, [][]any{{int64(10), int64(30)}, {int64(10), int64(40)}})
		emptyChk := chunk.New(rfields, 1, 1)

		// Empty inners yields unmatched and appends nothing.
		res := chunk.New(outFields, 8, 8)
		emptyIter := chunk.NewIterator4Chunk(emptyChk)
		emptyIter.Begin()
		matched, hasNull, err := j.TryToMatchInners(outerRow, emptyIter, res, LeftNotNullRightNotNull)
		require.NoError(t, err)
		require.False(t, matched)
		require.False(t, hasNull)
		require.Equal(t, 0, res.NumRows())

		cases := []struct {
			opt      NAAJType
			wantNull bool
			wantVal  int64
		}{
			{LeftNotNullRightNotNull, false, 1},
			{LeftNotNullRightHasNull, true, 0},
			{LeftHasNullRightHasNull, true, 0},
			{LeftHasNullRightNotNull, true, 0},
		}
		for _, tc := range cases {
			res.Reset()
			iter := chunk.NewIterator4Chunk(innerChk)
			iter.Begin()
			matched, hasNull, err := j.TryToMatchInners(outerRow, iter, res, tc.opt)
			require.NoError(t, err)
			require.True(t, matched)
			require.False(t, hasNull)
			require.Equal(t, iter.End(), iter.Current())
			require.Equal(t, 1, res.NumRows())
			row := res.GetRow(0)
			require.Equal(t, int64(10), row.GetInt64(0))
			require.Equal(t, int64(20), row.GetInt64(1))
			require.Equal(t, tc.wantNull, row.IsNull(2))
			if !tc.wantNull {
				require.Equal(t, tc.wantVal, row.GetInt64(2))
			}
		}

		// OnMissMatch always appends (outer, 0) regardless of hasNull flag.
		res.Reset()
		j.OnMissMatch(false, outerRow, res)
		j.OnMissMatch(true, outerRow, res)
		require.Equal(t, 2, res.NumRows())
		for i := range 2 {
			row := res.GetRow(i)
			require.Equal(t, int64(10), row.GetInt64(0))
			require.Equal(t, int64(20), row.GetInt64(1))
			require.False(t, row.IsNull(2))
			require.Equal(t, int64(0), row.GetInt64(2))
		}
	})

	t.Run("with other conditions and inline projection", func(t *testing.T) {
		// Condition: lhs.col1 < rhs.col1 (indices 1 and 3 in shallow row [lhs0, lhs1, rhs0, rhs1]).
		cond, err := expression.NewFunction(
			ctx.GetExprCtx(), ast.LT, types.NewFieldType(mysql.TypeTiny),
			&expression.Column{Index: 1, RetType: intType},
			&expression.Column{Index: 3, RetType: intType},
		)
		require.NoError(t, err)

		// Inline projection keeps only lhs.col0 (lhs.col1 is pruned from output but used in cond).
		childrenUsed := [][]int{{0}, {}}
		j := NewJoiner(ctx, base.LeftOuterSemiJoin, false, nil, []expression.Expression{cond}, lfields, rfields, childrenUsed, true)
		require.False(t, j.isSemiJoinWithoutCondition())

		projOutFields := []*types.FieldType{intType, flagType}
		outerRow := makeRows(lfields, [][]any{{int64(7), int64(50)}}).GetRow(0)

		// First inner evaluates to false (50 < 10), second to NULL (50 < NULL), third to true (50 < 60).
		inners := makeRows(rfields, [][]any{
			{int64(7), int64(10)},
			{int64(7), nil},
			{int64(7), int64(60)},
			{int64(7), int64(70)},
		})
		res := chunk.New(projOutFields, 4, 4)
		iter := chunk.NewIterator4Chunk(inners)
		iter.Begin()
		matched, hasNull, err := j.TryToMatchInners(outerRow, iter, res, LeftNotNullRightNotNull)
		require.NoError(t, err)
		require.True(t, matched)
		require.False(t, hasNull)
		require.Equal(t, iter.End(), iter.Current())
		require.Equal(t, 1, res.NumRows())
		require.Equal(t, int64(7), res.GetRow(0).GetInt64(0))
		require.Equal(t, int64(1), res.GetRow(0).GetInt64(1))

		// When all inners yield false or NULL in other conditions, returns matched=false, hasNull=false.
		allNullOrFalseInners := makeRows(rfields, [][]any{
			{int64(7), int64(10)},
			{int64(7), nil},
		})
		res.Reset()
		iter2 := chunk.NewIterator4Chunk(allNullOrFalseInners)
		iter2.Begin()
		matched, hasNull, err = j.TryToMatchInners(outerRow, iter2, res, LeftNotNullRightHasNull)
		require.NoError(t, err)
		require.False(t, matched)
		require.False(t, hasNull)
		require.Equal(t, 0, res.NumRows())

		// Empty lUsed outputs only the match flag column.
		jEmptyLUsed := NewJoiner(ctx, base.LeftOuterSemiJoin, false, nil, nil, lfields, rfields, [][]int{{}, {}}, true)
		flagOnlyRes := chunk.New([]*types.FieldType{flagType}, 4, 4)
		iter3 := chunk.NewIterator4Chunk(inners)
		iter3.Begin()
		matched, _, err = jEmptyLUsed.TryToMatchInners(outerRow, iter3, flagOnlyRes, LeftNotNullRightNotNull)
		require.NoError(t, err)
		require.True(t, matched)
		jEmptyLUsed.OnMissMatch(false, outerRow, flagOnlyRes)
		require.Equal(t, 2, flagOnlyRes.NumRows())
		require.Equal(t, int64(1), flagOnlyRes.GetRow(0).GetInt64(0))
		require.Equal(t, int64(0), flagOnlyRes.GetRow(1).GetInt64(0))

		// Iterator error is propagated when no inner row matches.
		wantErr := errors.New("mock iter error")
		errIter := &errIterator{Iterator: chunk.NewIterator4Chunk(allNullOrFalseInners), err: wantErr}
		errIter.Begin()
		matched, hasNull, err = j.TryToMatchInners(outerRow, errIter, res, LeftNotNullRightNotNull)
		require.ErrorIs(t, err, wantErr)
		require.False(t, matched)
		require.False(t, hasNull)
	})
}
