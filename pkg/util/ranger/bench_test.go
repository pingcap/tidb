// Copyright 2021 PingCAP, Inc.
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

package ranger_test

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

const (
	benchmarkLongInListSize      = 1024
	benchmarkCartesianInListSize = 64
)

var (
	benchmarkDetachRangeResult *ranger.DetachRangeResult
	benchmarkBuiltRanges       ranger.Ranges
)

type indexRangeBenchmarkInput struct {
	sctx       sessionctx.Context
	conds      []expression.Expression
	cols       []*expression.Column
	lengths    []int
	rangeCount int
	rangeBytes int64
}

type columnRangeBenchmarkInput struct {
	sctx       sessionctx.Context
	conds      []expression.Expression
	tp         *types.FieldType
	colLen     int
	rangeCount int
	rangeBytes int64
}

func BenchmarkDetachCondAndBuildRangeForIndex(b *testing.B) {
	input := prepareIndexRangeBenchmarkInput(b, `
CREATE TABLE t (
    id bigint(20) NOT NULL,
    flag tinyint(1) NOT NULL,
    start_time bigint(20) NOT NULL,
    end_time bigint(20) NOT NULL,
    org_id bigint(20) NOT NULL,
    work_type varchar(20) CHARACTER SET latin1 DEFAULT NULL,
    KEY idx (work_type, flag, org_id, end_time)
)`,
		queryForLongInListValues(benchmarkLongInListSize, "SELECT * FROM test.t WHERE flag = false AND work_type = 'ART' AND start_time < 1437233819011 AND org_id IN (%s)"),
	)

	b.ReportAllocs()
	reportRangeMetrics(b, input.rangeCount, input.rangeBytes)
	b.ResetTimer()
	for range b.N {
		res, err := ranger.DetachCondAndBuildRangeForIndex(input.sctx.GetRangerCtx(), input.conds, input.cols, input.lengths, 0)
		require.NoError(b, err)
		benchmarkDetachRangeResult = res
	}
	b.StopTimer()
}

func BenchmarkDetachCondAndBuildRangeForIndexCartesianFanout(b *testing.B) {
	input := prepareIndexRangeBenchmarkInput(b, `
CREATE TABLE t (
    a bigint NOT NULL,
    b bigint NOT NULL,
    c bigint NOT NULL,
    d bigint,
    KEY idx (a, b, c)
)`,
		queryForCartesianInListValues(
			benchmarkCartesianInListSize,
			benchmarkCartesianInListSize,
			"SELECT * FROM test.t WHERE a IN (%s) AND b IN (%s) AND c >= 10 AND c <= 20",
		),
	)

	b.ReportAllocs()
	reportRangeMetrics(b, input.rangeCount, input.rangeBytes)
	b.ResetTimer()
	for range b.N {
		res, err := ranger.DetachCondAndBuildRangeForIndex(input.sctx.GetRangerCtx(), input.conds, input.cols, input.lengths, 0)
		require.NoError(b, err)
		benchmarkDetachRangeResult = res
	}
	b.StopTimer()
}

func BenchmarkBuildColumnRangeForLongInList(b *testing.B) {
	input := prepareColumnRangeBenchmarkInput(b, `
CREATE TABLE t (
    a bigint NOT NULL,
    b bigint,
    KEY idx (a)
)`,
		queryForLongInListValues(benchmarkLongInListSize, "SELECT * FROM test.t WHERE a IN (%s)"),
	)

	b.ReportAllocs()
	reportRangeMetrics(b, input.rangeCount, input.rangeBytes)
	b.ResetTimer()
	for range b.N {
		ranges, access, remained, err := ranger.BuildColumnRange(input.conds, input.sctx.GetRangerCtx(), input.tp, input.colLen, 0)
		require.NoError(b, err)
		require.NotEmpty(b, access)
		require.Empty(b, remained)
		benchmarkBuiltRanges = ranges
	}
	b.StopTimer()
}

func BenchmarkBuildColumnRangeForTimestampLongInList(b *testing.B) {
	input := prepareColumnRangeBenchmarkInput(b, `
CREATE TABLE t (
    ts timestamp NOT NULL,
    b bigint,
    KEY idx (ts)
)`,
		queryForLongTimestampInListValues(benchmarkLongInListSize, "SELECT * FROM test.t WHERE ts IN (%s)"),
		"SET time_zone = '+00:00'",
	)

	b.ReportAllocs()
	reportRangeMetrics(b, input.rangeCount, input.rangeBytes)
	b.ResetTimer()
	for range b.N {
		ranges, access, remained, err := ranger.BuildColumnRange(input.conds, input.sctx.GetRangerCtx(), input.tp, input.colLen, 0)
		require.NoError(b, err)
		require.NotEmpty(b, access)
		require.Empty(b, remained)
		benchmarkBuiltRanges = ranges
	}
	b.StopTimer()
}

func prepareIndexRangeBenchmarkInput(b *testing.B, createTableSQL, query string) indexRangeBenchmarkInput {
	b.Helper()

	store := testkit.CreateMockStore(b)
	testKit := testkit.NewTestKit(b, store)
	testKit.MustExec("USE test")
	testKit.MustExec("DROP TABLE IF EXISTS t")
	testKit.MustExec(createTableSQL)

	sctx := testKit.Session().(sessionctx.Context)
	selection := getSelectionFromQuery(b, sctx, query)
	dataSource := selection.Children()[0].(*logicalop.DataSource)
	conds := normalizedSelectionConds(sctx, selection.Conditions)
	cols, lengths := plannerutil.IndexInfo2PrefixCols(dataSource.TableInfo.Columns, selection.Schema().Columns, dataSource.TableInfo.Indices[0])
	require.NotNil(b, cols)

	fullResult, err := ranger.DetachCondAndBuildRangeForIndex(sctx.GetRangerCtx(), conds, cols, lengths, 0)
	require.NoError(b, err)
	require.NotEmpty(b, fullResult.Ranges)

	return indexRangeBenchmarkInput{
		sctx:       sctx,
		conds:      conds,
		cols:       cols,
		lengths:    lengths,
		rangeCount: len(fullResult.Ranges),
		rangeBytes: fullResult.Ranges.MemUsage(),
	}
}

func prepareColumnRangeBenchmarkInput(b *testing.B, createTableSQL, query string, setupSQL ...string) columnRangeBenchmarkInput {
	b.Helper()

	store := testkit.CreateMockStore(b)
	testKit := testkit.NewTestKit(b, store)
	testKit.MustExec("USE test")
	testKit.MustExec("DROP TABLE IF EXISTS t")
	testKit.MustExec(createTableSQL)
	for _, sql := range setupSQL {
		testKit.MustExec(sql)
	}

	sctx := testKit.Session().(sessionctx.Context)
	selection := getSelectionFromQuery(b, sctx, query)
	dataSource := selection.Children()[0].(*logicalop.DataSource)
	col := expression.ColInfo2Col(selection.Schema().Columns, dataSource.TableInfo.Columns[0])
	conds, filters := ranger.DetachCondsForColumn(sctx.GetRangerCtx(), normalizedSelectionConds(sctx, selection.Conditions), col)
	require.Empty(b, filters)

	ranges, access, remained, err := ranger.BuildColumnRange(conds, sctx.GetRangerCtx(), col.RetType, types.UnspecifiedLength, 0)
	require.NoError(b, err)
	require.NotEmpty(b, ranges)
	require.NotEmpty(b, access)
	require.Empty(b, remained)

	return columnRangeBenchmarkInput{
		sctx:       sctx,
		conds:      conds,
		tp:         col.RetType,
		colLen:     types.UnspecifiedLength,
		rangeCount: len(ranges),
		rangeBytes: ranges.MemUsage(),
	}
}

func normalizedSelectionConds(sctx sessionctx.Context, conds []expression.Expression) []expression.Expression {
	normalized := make([]expression.Expression, len(conds))
	for i, cond := range conds {
		normalized[i] = expression.PushDownNot(sctx.GetExprCtx(), cond)
	}
	return normalized
}

func queryForLongInListValues(size int, pattern string) string {
	return strings.Replace(pattern, "%s", benchmarkIntValuesSQL(size, 1), 1)
}

func queryForLongTimestampInListValues(size int, pattern string) string {
	return strings.Replace(pattern, "%s", benchmarkTimestampValuesSQL(size, 1), 1)
}

func queryForCartesianInListValues(leftSize, rightSize int, pattern string) string {
	leftValues := benchmarkIntValuesSQL(leftSize, 1)
	rightValues := benchmarkIntValuesSQL(rightSize, 2)
	query := strings.Replace(pattern, "%s", leftValues, 1)
	return strings.Replace(query, "%s", rightValues, 1)
}

func benchmarkIntValuesSQL(size int, seed int64) string {
	values := make([]int64, size)
	for i := 0; i < size; i++ {
		values[i] = int64(i+1)*1000003 + seed
	}

	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(values), func(i, j int) {
		values[i], values[j] = values[j], values[i]
	})

	var builder strings.Builder
	builder.Grow(len(values) * 16)
	for i, value := range values {
		if i > 0 {
			builder.WriteByte(',')
		}
		builder.WriteString(strconv.FormatInt(value, 10))
	}
	return builder.String()
}

func benchmarkTimestampValuesSQL(size int, seed int64) string {
	offsets := make([]int, size)
	for i := range offsets {
		offsets[i] = i
	}

	rng := rand.New(rand.NewSource(seed))
	rng.Shuffle(len(offsets), func(i, j int) {
		offsets[i], offsets[j] = offsets[j], offsets[i]
	})

	var builder strings.Builder
	builder.Grow(len(offsets) * 34)
	for i, offset := range offsets {
		if i > 0 {
			builder.WriteByte(',')
		}
		seconds := offset * 37
		hour := seconds / 3600
		minute := seconds % 3600 / 60
		second := seconds % 60
		fmt.Fprintf(&builder, "TIMESTAMP '2025-01-01 %02d:%02d:%02d'", hour, minute, second)
	}
	return builder.String()
}

func reportRangeMetrics(b *testing.B, rangeCount int, rangeBytes int64) {
	b.ReportMetric(float64(rangeCount), "ranges/op")
	b.ReportMetric(float64(rangeBytes), "range-bytes/op")
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkBuildColumnRangeForLongInList,
		BenchmarkBuildColumnRangeForTimestampLongInList,
		BenchmarkDetachCondAndBuildRangeForIndex,
		BenchmarkDetachCondAndBuildRangeForIndexCartesianFanout,
	)
}
