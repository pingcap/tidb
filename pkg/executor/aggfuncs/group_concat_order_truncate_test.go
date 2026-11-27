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

package aggfuncs_test

import (
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// orderConcatResult feeds vals (in the given arrival order) into a
// group_concat(... ORDER BY <col> [DESC] SEPARATOR sep) aggregate with the
// given group_concat_max_len and returns the final string.
func orderConcatResult(t *testing.T, vals []string, sep string, maxLen uint64, desc bool) string {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(vardef.GroupConcatMaxLen, strconv.FormatUint(maxLen, 10)))
	return orderConcatResultWithCtx(t, ctx, vals, sep, desc)
}

func orderConcatResultWithCtx(t *testing.T, ctx *mock.Context, vals []string, sep string, desc bool) string {
	sc := ctx.GetSessionVars().StmtCtx
	sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(true))
	ft := types.NewFieldType(mysql.TypeString)
	args := []expression.Expression{
		&expression.Column{RetType: ft, Index: 0},
		&expression.Constant{Value: types.NewStringDatum(sep), RetType: types.NewFieldType(mysql.TypeString)},
	}
	d, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, args, false)
	require.NoError(t, err)
	d.OrderByItems = []*util.ByItems{{Expr: args[0], Desc: desc}}
	f := aggfuncs.Build(ctx, d, 0)
	pr, _ := f.AllocPartialResult()

	chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, len(vals))
	for _, v := range vals {
		chk.AppendString(0, v)
	}
	iter := chunk.NewIterator4Chunk(chk)
	for row := iter.Begin(); row != iter.End(); row = iter.Next() {
		_, err := f.UpdatePartialResult(ctx, []chunk.Row{row}, pr)
		require.NoError(t, err)
	}
	out := chunk.NewChunkWithCapacity([]*types.FieldType{d.RetTp}, 1)
	require.NoError(t, f.AppendFinalResult2Chunk(ctx, pr, out))
	require.Equal(t, 1, out.NumRows())
	if out.GetRow(0).IsNull(0) {
		return ""
	}
	return out.GetRow(0).GetString(0)
}

// oracleConcat is MySQL's semantics: sort all values, join with sep, cut the
// joined string at maxLen bytes.
func oracleConcat(vals []string, sep string, maxLen uint64, desc bool) string {
	s := append([]string(nil), vals...)
	sort.SliceStable(s, func(i, j int) bool {
		if desc {
			return s[i] > s[j]
		}
		return s[i] < s[j]
	})
	var b strings.Builder
	for i, v := range s {
		if i > 0 {
			b.WriteString(sep)
		}
		b.WriteString(v)
	}
	r := b.String()
	if uint64(len(r)) > maxLen {
		r = r[:maxLen]
	}
	return r
}

// TestGroupConcatOrderTruncateIssue71926 reproduces
// https://github.com/pingcap/tidb/issues/71926: 600 rows (100 x 'a',
// 300 x 'aaa', 200 x 'b'), group_concat_max_len=1024, separator ','.
// MySQL 9.7 returns 307 elements of which 100 are 'a' and 206 are 'aaa'.
func TestGroupConcatOrderTruncateIssue71926(t *testing.T) {
	vals := make([]string, 0, 600)
	for n := 1; n <= 600; n++ {
		switch {
		case n%6 < 3:
			vals = append(vals, "aaa")
		case n%6 == 3:
			vals = append(vals, "a")
		default:
			vals = append(vals, "b")
		}
	}
	got := orderConcatResult(t, vals, ",", 1024, false)
	require.Equal(t, oracleConcat(vals, ",", 1024, false), got)
	require.Equal(t, 1024, len(got))

	elems := strings.Split(got, ",")
	a, aaa := 0, 0
	for _, e := range elems {
		switch e {
		case "a":
			a++
		case "aaa":
			aaa++
		}
	}
	require.Equal(t, 307, len(elems))
	require.Equal(t, 100, a)
	require.Equal(t, 206, aaa)
}

// The separator itself may be cut: 'aaa---bbb---ccc' with max_len 10 keeps
// 'aaa---bbb-'.
func TestGroupConcatOrderTruncatePartialSep(t *testing.T) {
	vals := []string{"aaa", "bbb", "ccc"}
	got := orderConcatResult(t, vals, "---", 10, false)
	require.Equal(t, "aaa---bbb-", got)
}

// With an empty separator the cut lands inside a value, not a separator.
func TestGroupConcatOrderTruncateEmptySep(t *testing.T) {
	vals := []string{"aaa", "bbb", "cccccc"}
	got := orderConcatResult(t, vals, "", 10, false)
	require.Equal(t, "aaabbbcccc", got)
}

// A single value longer than max_len is cut to max_len with no leading sep.
func TestGroupConcatOrderTruncateSingleLongValue(t *testing.T) {
	got := orderConcatResult(t, []string{"abcdefghij"}, ",", 4, false)
	require.Equal(t, "abcd", got)
}

func TestGroupConcatOrderTruncateHugeValue(t *testing.T) {
	huge := strings.Repeat("x", 1<<20)
	require.Equal(t, strings.Repeat("x", 1024), orderConcatResult(t, []string{huge}, ",", 1024, false))
	require.Equal(t, "a,xxxxxx", orderConcatResult(t, []string{"a", huge}, ",", 8, false))
	require.Equal(t, "a---", orderConcatResult(t, []string{"a", huge}, "-----", 4, false))
}

func TestGroupConcatOrderTruncateDesc(t *testing.T) {
	vals := []string{"a", "bb", "ccc", "dddd"}
	got := orderConcatResult(t, vals, ",", 8, true)
	require.Equal(t, "dddd,ccc", got)
	require.Equal(t, oracleConcat(vals, ",", 8, true), got)
}

// A partial result reused for a second group must not inherit truncation
// state from the first group.
func TestGroupConcatOrderTruncateResetBetweenGroups(t *testing.T) {
	ctx := mock.NewContext()
	require.NoError(t, ctx.GetSessionVars().SetSystemVar(vardef.GroupConcatMaxLen, "10"))
	sc := ctx.GetSessionVars().StmtCtx
	sc.SetTypeFlags(sc.TypeFlags().WithTruncateAsWarning(true))
	ft := types.NewFieldType(mysql.TypeString)
	args := []expression.Expression{
		&expression.Column{RetType: ft, Index: 0},
		&expression.Constant{Value: types.NewStringDatum("---"), RetType: types.NewFieldType(mysql.TypeString)},
	}
	d, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, args, false)
	require.NoError(t, err)
	d.OrderByItems = []*util.ByItems{{Expr: args[0], Desc: false}}
	f := aggfuncs.Build(ctx, d, 0)
	pr, _ := f.AllocPartialResult()
	feed := func(vals []string) string {
		chk := chunk.NewChunkWithCapacity([]*types.FieldType{ft}, len(vals))
		for _, v := range vals {
			chk.AppendString(0, v)
		}
		iter := chunk.NewIterator4Chunk(chk)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			_, err := f.UpdatePartialResult(ctx, []chunk.Row{row}, pr)
			require.NoError(t, err)
		}
		out := chunk.NewChunkWithCapacity([]*types.FieldType{d.RetTp}, 1)
		require.NoError(t, f.AppendFinalResult2Chunk(ctx, pr, out))
		return out.GetRow(0).GetString(0)
	}
	require.Equal(t, "aaa---bbb-", feed([]string{"aaa", "bbb", "ccc"}))
	f.ResetPartialResult(pr)
	require.Equal(t, "x---y", feed([]string{"x", "y"}))
}

// Rows arriving out of order after the budget already overflowed must not
// displace the boundary row that the cut falls into.
func TestGroupConcatOrderTruncateOutOfOrderArrival(t *testing.T) {
	vals := []string{"zzzzzzzz", "mmmmm", "a"}
	got := orderConcatResult(t, vals, ",", 10, false)
	require.Equal(t, "a,mmmmm,zz", got)
	require.Equal(t, oracleConcat(vals, ",", 10, false), got)
}

// Randomized cross-check against the sort-join-cut oracle, with shuffled
// arrival order, both directions, and separators of varying width.
func TestGroupConcatOrderTruncateProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(71926))
	seps := []string{"", ",", " ", "---"}
	for iter := 0; iter < 300; iter++ {
		n := 1 + rng.Intn(40)
		seen := make(map[string]bool, n)
		vals := make([]string, 0, n)
		for len(vals) < n {
			v := randString(rng, 1+rng.Intn(6))
			if seen[v] {
				continue
			}
			seen[v] = true
			vals = append(vals, v)
		}
		sep := seps[rng.Intn(len(seps))]
		maxLen := uint64(4 + rng.Intn(60))
		desc := rng.Intn(2) == 0
		rng.Shuffle(len(vals), func(i, j int) { vals[i], vals[j] = vals[j], vals[i] })
		got := orderConcatResult(t, vals, sep, maxLen, desc)
		require.Equalf(t, oracleConcat(vals, sep, maxLen, desc), got,
			"iter=%d vals=%v sep=%q maxLen=%d desc=%v", iter, vals, sep, maxLen, desc)
	}
}

func randString(rng *rand.Rand, n int) string {
	const letters = "abcxyz"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}