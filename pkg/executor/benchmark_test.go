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

package executor

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/executor/aggfuncs"
	"github.com/pingcap/tidb/pkg/executor/aggregate"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/executor/sortexec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/disk"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/mock"
	"go.uber.org/zap/zapcore"
)

var (
	_          exec.Executor     = &testutil.MockDataSource{}
	_          base.PhysicalPlan = &testutil.MockDataPhysicalPlan{}
	wideString                   = strings.Repeat("x", 5*1024)
)

func buildHashAggExecutor(ctx sessionctx.Context, src exec.Executor, schema *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression) exec.Executor {
	plan := new(core.PhysicalHashAgg)
	plan.AggFuncs = aggFuncs
	plan.GroupByItems = groupItems
	plan.SetSchema(schema)
	plan.Init(ctx.GetPlanCtx(), nil, 0)
	plan.SetChildren(nil)
	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	hashAgg := exec.(*aggregate.HashAggExec)
	hashAgg.SetChildren(0, src)
	return exec
}

func buildStreamAggExecutor(ctx sessionctx.Context, srcExec exec.Executor, schema *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression, concurrency int, dataSourceSorted bool) exec.Executor {
	src := testutil.BuildMockDataPhysicalPlan(ctx, srcExec)

	sg := new(core.PhysicalStreamAgg)
	sg.AggFuncs = aggFuncs
	sg.GroupByItems = groupItems
	sg.SetSchema(schema)
	sg.Init(ctx.GetPlanCtx(), nil, 0)

	var tail base.PhysicalPlan = sg
	// if data source is not sorted, we have to attach sort, to make the input of stream-agg sorted
	if !dataSourceSorted {
		byItems := make([]*util.ByItems, 0, len(sg.GroupByItems))
		for _, col := range sg.GroupByItems {
			byItems = append(byItems, &util.ByItems{Expr: col, Desc: false})
		}
		sortPP := &core.PhysicalSort{ByItems: byItems}
		sortPP.SetChildren(src)
		sg.SetChildren(sortPP)
		tail = sortPP
	} else {
		sg.SetChildren(src)
	}

	var (
		plan     base.PhysicalPlan
		splitter core.PartitionSplitterType = core.PartitionHashSplitterType
	)
	if concurrency > 1 {
		if dataSourceSorted {
			splitter = core.PartitionRangeSplitterType
		}
		plan = core.PhysicalShuffle{
			Concurrency:  concurrency,
			Tails:        []base.PhysicalPlan{tail},
			DataSources:  []base.PhysicalPlan{src},
			SplitterType: splitter,
			ByItemArrays: [][]expression.Expression{sg.GroupByItems},
		}.Init(ctx.GetPlanCtx(), nil, 0)
		plan.SetChildren(sg)
	} else {
		plan = sg
	}

	b := newExecutorBuilder(ctx, nil)
	return b.build(plan)
}

func buildAggExecutor(b *testing.B, testCase *testutil.AggTestCase, child exec.Executor) exec.Executor {
	ctx := testCase.Ctx
	if testCase.ExecType == "stream" {
		if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBStreamAggConcurrency, fmt.Sprintf("%v", testCase.Concurrency)); err != nil {
			b.Fatal(err)
		}
	} else {
		if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggFinalConcurrency, fmt.Sprintf("%v", testCase.Concurrency)); err != nil {
			b.Fatal(err)
		}
		if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggPartialConcurrency, fmt.Sprintf("%v", testCase.Concurrency)); err != nil {
			b.Fatal(err)
		}
	}

	childCols := testCase.Columns()
	schema := expression.NewSchema(childCols...)
	groupBy := []expression.Expression{childCols[1]}
	aggFunc, err := aggregation.NewAggFuncDesc(testCase.Ctx.GetExprCtx(), testCase.AggFunc, []expression.Expression{childCols[0]}, testCase.HasDistinct)
	if err != nil {
		b.Fatal(err)
	}
	aggFuncs := []*aggregation.AggFuncDesc{aggFunc}

	var aggExec exec.Executor
	switch testCase.ExecType {
	case "hash":
		aggExec = buildHashAggExecutor(testCase.Ctx, child, schema, aggFuncs, groupBy)
	case "stream":
		aggExec = buildStreamAggExecutor(testCase.Ctx, child, schema, aggFuncs, groupBy, testCase.Concurrency, testCase.DataSourceSorted)
	default:
		b.Fatal("not implement")
	}
	return aggExec
}

func benchmarkAggExecWithCase(b *testing.B, casTest *testutil.AggTestCase) {
	if err := casTest.Ctx.GetSessionVars().SetSystemVar(variable.TiDBStreamAggConcurrency, fmt.Sprintf("%v", casTest.Concurrency)); err != nil {
		b.Fatal(err)
	}

	cols := casTest.Columns()
	dataSource := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(cols...),
		Ndvs:       []int{0, casTest.GroupByNDV},
		Orders:     []bool{false, casTest.DataSourceSorted},
		Rows:       casTest.Rows,
		Ctx:        casTest.Ctx,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new agg-executor
		aggExec := buildAggExecutor(b, casTest, dataSource)
		tmpCtx := context.Background()
		chk := exec.NewFirstChunk(aggExec)
		dataSource.PrepareChunks()

		b.StartTimer()
		if err := aggExec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := aggExec.Next(tmpCtx, chk); err != nil {
				b.Fatal(b)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := aggExec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkShuffleStreamAggRows(b *testing.B) {
	b.ReportAllocs()
	sortTypes := []bool{false, true}
	rows := []int{10000, 100000, 1000000, 10000000}
	concurrencies := []int{1, 2, 4, 8}
	for _, row := range rows {
		for _, con := range concurrencies {
			for _, sorted := range sortTypes {
				cas := testutil.DefaultAggTestCase("stream")
				cas.Rows = row
				cas.DataSourceSorted = sorted
				cas.Concurrency = con
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkAggExecWithCase(b, cas)
				})
			}
		}
	}
}

func BenchmarkHashAggRows(b *testing.B) {
	rows := []int{100000, 1000000, 10000000}
	concurrencies := []int{1, 4, 8, 15, 20, 30, 40}
	for _, row := range rows {
		for _, con := range concurrencies {
			cas := testutil.DefaultAggTestCase("hash")
			cas.Rows = row
			cas.Concurrency = con
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkAggExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkAggGroupByNDV(b *testing.B) {
	NDVs := []int{10, 100, 1000, 10000, 100000, 1000000, 10000000}
	for _, NDV := range NDVs {
		for _, exec := range []string{"hash", "stream"} {
			cas := testutil.DefaultAggTestCase(exec)
			cas.GroupByNDV = NDV
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkAggExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkAggConcurrency(b *testing.B) {
	concs := []int{1, 4, 8, 15, 20, 30, 40}
	for _, con := range concs {
		for _, exec := range []string{"hash", "stream"} {
			cas := testutil.DefaultAggTestCase(exec)
			cas.Concurrency = con
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkAggExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkAggDistinct(b *testing.B) {
	rows := []int{100000, 1000000, 10000000}
	distincts := []bool{false, true}
	for _, row := range rows {
		for _, exec := range []string{"hash", "stream"} {
			for _, distinct := range distincts {
				cas := testutil.DefaultAggTestCase(exec)
				cas.Rows = row
				cas.HasDistinct = distinct
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkAggExecWithCase(b, cas)
				})
			}
		}
	}
}

func buildWindowExecutor(ctx sessionctx.Context, windowFunc string, funcs int, frame *core.WindowFrame, srcExec exec.Executor, schema *expression.Schema, partitionBy []*expression.Column, concurrency int, dataSourceSorted bool) exec.Executor {
	src := testutil.BuildMockDataPhysicalPlan(ctx, srcExec)
	win := new(core.PhysicalWindow)
	win.WindowFuncDescs = make([]*aggregation.WindowFuncDesc, 0)
	winSchema := schema.Clone()
	for i := 0; i < funcs; i++ {
		var args []expression.Expression
		switch windowFunc {
		case ast.WindowFuncNtile:
			args = append(args, &expression.Constant{Value: types.NewUintDatum(2)})
		case ast.WindowFuncNthValue:
			args = append(args, partitionBy[0], &expression.Constant{Value: types.NewUintDatum(2)})
		case ast.AggFuncSum:
			args = append(args, src.Schema().Columns[0])
		case ast.AggFuncAvg:
			args = append(args, src.Schema().Columns[0])
		case ast.AggFuncBitXor:
			args = append(args, src.Schema().Columns[0])
		case ast.AggFuncMax, ast.AggFuncMin:
			args = append(args, src.Schema().Columns[0])
		default:
			args = append(args, partitionBy[0])
		}
		desc, _ := aggregation.NewWindowFuncDesc(ctx.GetExprCtx(), windowFunc, args, false)

		win.WindowFuncDescs = append(win.WindowFuncDescs, desc)
		winSchema.Append(&expression.Column{
			UniqueID: 10 + (int64)(i),
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		})
	}
	for _, col := range partitionBy {
		win.PartitionBy = append(win.PartitionBy, property.SortItem{Col: col})
	}
	win.Frame = frame
	win.OrderBy = nil

	win.SetSchema(winSchema)
	win.Init(ctx.GetPlanCtx(), nil, 0)

	var tail base.PhysicalPlan = win
	if !dataSourceSorted {
		byItems := make([]*util.ByItems, 0, len(partitionBy))
		for _, col := range partitionBy {
			byItems = append(byItems, &util.ByItems{Expr: col, Desc: false})
		}
		sort := &core.PhysicalSort{ByItems: byItems}
		sort.SetChildren(src)
		win.SetChildren(sort)
		tail = sort
	} else {
		win.SetChildren(src)
	}

	var plan base.PhysicalPlan
	if concurrency > 1 {
		byItems := make([]expression.Expression, 0, len(win.PartitionBy))
		for _, item := range win.PartitionBy {
			byItems = append(byItems, item.Col)
		}

		plan = core.PhysicalShuffle{
			Concurrency:  concurrency,
			Tails:        []base.PhysicalPlan{tail},
			DataSources:  []base.PhysicalPlan{src},
			SplitterType: core.PartitionHashSplitterType,
			ByItemArrays: [][]expression.Expression{byItems},
		}.Init(ctx.GetPlanCtx(), nil, 0)
		plan.SetChildren(win)
	} else {
		plan = win
	}

	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	return exec
}

func benchmarkWindowExecWithCase(b *testing.B, casTest *testutil.WindowTestCase) {
	ctx := casTest.Ctx
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBWindowConcurrency, fmt.Sprintf("%v", casTest.Concurrency)); err != nil {
		b.Fatal(err)
	}
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBEnablePipelinedWindowFunction, fmt.Sprintf("%v", casTest.Pipelined)); err != nil {
		b.Fatal(err)
	}

	cols := casTest.Columns
	dataSource := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(cols...),
		Ndvs:       []int{0, casTest.Ndv, 0, 0},
		Orders:     []bool{false, casTest.DataSourceSorted, false, false},
		Rows:       casTest.Rows,
		Ctx:        casTest.Ctx,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new window-executor
		childCols := casTest.Columns
		schema := expression.NewSchema(childCols...)
		windowExec := buildWindowExecutor(casTest.Ctx, casTest.WindowFunc, casTest.NumFunc, casTest.Frame, dataSource, schema, childCols[1:2], casTest.Concurrency, casTest.DataSourceSorted)
		tmpCtx := context.Background()
		chk := exec.NewFirstChunk(windowExec)
		dataSource.PrepareChunks()

		b.StartTimer()
		if err := windowExec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := windowExec.Next(tmpCtx, chk); err != nil {
				b.Fatal(b)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := windowExec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func baseBenchmarkWindowRows(b *testing.B, pipelined int) {
	b.ReportAllocs()
	rows := []int{1000, 100000}
	ndvs := []int{1, 10, 1000}
	concs := []int{1, 2, 4}
	for _, row := range rows {
		for _, ndv := range ndvs {
			for _, con := range concs {
				cas := testutil.DefaultWindowTestCase()
				cas.Rows = row
				cas.Ndv = ndv
				cas.Concurrency = con
				cas.DataSourceSorted = false
				cas.WindowFunc = ast.WindowFuncRowNumber // cheapest
				cas.Pipelined = pipelined
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkWindowExecWithCase(b, cas)
				})
			}
		}
	}
}

func BenchmarkWindowRows(b *testing.B) {
	baseBenchmarkWindowRows(b, 0)
	baseBenchmarkWindowRows(b, 1)
}

func baseBenchmarkWindowFunctions(b *testing.B, pipelined int) {
	b.ReportAllocs()
	windowFuncs := []string{
		// ast.WindowFuncRowNumber,
		// ast.WindowFuncRank,
		// ast.WindowFuncDenseRank,
		// ast.WindowFuncCumeDist,
		// ast.WindowFuncPercentRank,
		// ast.WindowFuncNtile,
		// ast.WindowFuncLead,
		ast.WindowFuncLag,
		// ast.WindowFuncFirstValue,
		// ast.WindowFuncLastValue,
		// ast.WindowFuncNthValue,
	}
	concs := []int{1, 4}
	for _, windowFunc := range windowFuncs {
		for _, con := range concs {
			cas := testutil.DefaultWindowTestCase()
			cas.Rows = 100000
			cas.Ndv = 1000
			cas.Concurrency = con
			cas.DataSourceSorted = false
			cas.WindowFunc = windowFunc
			cas.Pipelined = pipelined
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkWindowExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkWindowFunctions(b *testing.B) {
	baseBenchmarkWindowFunctions(b, 0)
	baseBenchmarkWindowFunctions(b, 1)
}

func baseBenchmarkWindowFunctionsWithFrame(b *testing.B, pipelined int) {
	b.ReportAllocs()
	windowFuncs := []string{
		ast.WindowFuncRowNumber,
		ast.AggFuncBitXor,
	}
	numFuncs := []int{1, 5}
	frames := []*core.WindowFrame{
		{Type: ast.Rows, Start: &core.FrameBound{UnBounded: true}, End: &core.FrameBound{Type: ast.CurrentRow}},
	}
	sortTypes := []bool{false, true}
	concs := []int{1, 2, 3, 4, 5, 6}
	for i, windowFunc := range windowFuncs {
		for _, sorted := range sortTypes {
			for _, numFunc := range numFuncs {
				for _, con := range concs {
					cas := testutil.DefaultWindowTestCase()
					cas.Rows = 100000
					cas.Ndv = 1000
					cas.Concurrency = con
					cas.DataSourceSorted = sorted
					cas.WindowFunc = windowFunc
					cas.NumFunc = numFunc
					if i < len(frames) {
						cas.Frame = frames[i]
					}
					cas.Pipelined = pipelined
					b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
						benchmarkWindowExecWithCase(b, cas)
					})
				}
			}
		}
	}
}

func BenchmarkWindowFunctionsWithFrame(b *testing.B) {
	baseBenchmarkWindowFunctionsWithFrame(b, 0)
	baseBenchmarkWindowFunctionsWithFrame(b, 1)
}

func baseBenchmarkWindowFunctionsAggWindowProcessorAboutFrame(b *testing.B, pipelined int) {
	b.ReportAllocs()
	windowFunc := ast.AggFuncMax
	frame := &core.WindowFrame{Type: ast.Rows, Start: &core.FrameBound{UnBounded: true}, End: &core.FrameBound{UnBounded: true}}
	cas := testutil.DefaultWindowTestCase()
	cas.Rows = 10000
	cas.Ndv = 10
	cas.Concurrency = 1
	cas.DataSourceSorted = false
	cas.WindowFunc = windowFunc
	cas.NumFunc = 1
	cas.Frame = frame
	cas.Pipelined = pipelined
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkWindowExecWithCase(b, cas)
	})
}

func BenchmarkWindowFunctionsAggWindowProcessorAboutFrame(b *testing.B) {
	baseBenchmarkWindowFunctionsAggWindowProcessorAboutFrame(b, 0)
	baseBenchmarkWindowFunctionsAggWindowProcessorAboutFrame(b, 1)
}

func baseBenchmarkWindowFunctionsWithSlidingWindow(b *testing.B, frameType ast.FrameType, pipelined int) {
	b.ReportAllocs()
	windowFuncs := []struct {
		aggFunc     string
		aggColTypes byte
	}{
		{ast.AggFuncSum, mysql.TypeFloat},
		{ast.AggFuncSum, mysql.TypeNewDecimal},
		{ast.AggFuncCount, mysql.TypeLong},
		{ast.AggFuncAvg, mysql.TypeFloat},
		{ast.AggFuncAvg, mysql.TypeNewDecimal},
		{ast.AggFuncBitXor, mysql.TypeLong},
		{ast.AggFuncMax, mysql.TypeLong},
		{ast.AggFuncMax, mysql.TypeFloat},
		{ast.AggFuncMin, mysql.TypeLong},
		{ast.AggFuncMin, mysql.TypeFloat},
	}
	row := 100000
	ndv := 100
	frame := &core.WindowFrame{
		Type:  frameType,
		Start: &core.FrameBound{Type: ast.Preceding, Num: 10},
		End:   &core.FrameBound{Type: ast.Following, Num: 10},
	}
	for _, windowFunc := range windowFuncs {
		cas := testutil.DefaultWindowTestCase()
		cas.Ctx.GetSessionVars().WindowingUseHighPrecision = false
		cas.Rows = row
		cas.Ndv = ndv
		cas.WindowFunc = windowFunc.aggFunc
		cas.Frame = frame
		cas.Columns[0].RetType.SetType(windowFunc.aggColTypes)
		cas.Pipelined = pipelined
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkWindowExecWithCase(b, cas)
		})
	}
}

func BenchmarkWindowFunctionsWithSlidingWindow(b *testing.B) {
	baseBenchmarkWindowFunctionsWithSlidingWindow(b, ast.Rows, 0)
	baseBenchmarkWindowFunctionsWithSlidingWindow(b, ast.Ranges, 0)
	baseBenchmarkWindowFunctionsWithSlidingWindow(b, ast.Rows, 1)
	baseBenchmarkWindowFunctionsWithSlidingWindow(b, ast.Ranges, 1)
}

type hashJoinTestCase struct {
	rows               int
	cols               []*types.FieldType
	concurrency        int
	ctx                sessionctx.Context
	keyIdx             []int
	joinType           core.JoinType
	disk               bool
	useOuterToBuild    bool
	rawData            string
	childrenUsedSchema [][]bool
}

func (tc hashJoinTestCase) columns() []*expression.Column {
	ret := make([]*expression.Column, 0)
	for i, t := range tc.cols {
		column := &expression.Column{Index: i, RetType: t, UniqueID: tc.ctx.GetSessionVars().AllocPlanColumnID()}
		ret = append(ret, column)
	}
	return ret
}

func (tc hashJoinTestCase) String() string {
	return fmt.Sprintf("(rows:%v, cols:%v, concurency:%v, joinKeyIdx: %v, disk:%v)",
		tc.rows, tc.cols, tc.concurrency, tc.keyIdx, tc.disk)
}

func defaultHashJoinTestCase(cols []*types.FieldType, joinType core.JoinType, useOuterToBuild bool) *hashJoinTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	ctx.GetSessionVars().SetIndexLookupJoinConcurrency(4)
	tc := &hashJoinTestCase{rows: 100000, concurrency: 4, ctx: ctx, keyIdx: []int{0, 1}, rawData: wideString}
	tc.cols = cols
	tc.useOuterToBuild = useOuterToBuild
	tc.joinType = joinType
	return tc
}

func prepareResolveIndices(joinSchema, lSchema, rSchema *expression.Schema, joinType core.JoinType) *expression.Schema {
	colsNeedResolving := joinSchema.Len()
	// The last output column of this two join is the generated column to indicate whether the row is matched or not.
	if joinType == core.LeftOuterSemiJoin || joinType == core.AntiLeftOuterSemiJoin {
		colsNeedResolving--
	}
	mergedSchema := expression.MergeSchema(lSchema, rSchema)
	// To avoid that two plan shares the same column slice.
	shallowColSlice := make([]*expression.Column, joinSchema.Len())
	copy(shallowColSlice, joinSchema.Columns)
	joinSchema = expression.NewSchema(shallowColSlice...)
	foundCnt := 0
	// Here we want to resolve all join schema columns directly as a merged schema, and you know same name
	// col in join schema should be separately redirected to corresponded same col in child schema. But two
	// column sets are **NOT** always ordered, see comment: https://github.com/pingcap/tidb/pull/45831#discussion_r1481031471
	// we are using mapping mechanism instead of moving j forward.
	marked := make([]bool, mergedSchema.Len())
	for i := 0; i < colsNeedResolving; i++ {
		findIdx := -1
		for j := 0; j < len(mergedSchema.Columns); j++ {
			if !joinSchema.Columns[i].EqualColumn(mergedSchema.Columns[j]) || marked[j] {
				continue
			}
			// resolve to a same unique id one, and it not being marked.
			findIdx = j
			break
		}
		if findIdx != -1 {
			// valid one.
			joinSchema.Columns[i] = joinSchema.Columns[i].Clone().(*expression.Column)
			joinSchema.Columns[i].Index = findIdx
			marked[findIdx] = true
			foundCnt++
		}
	}
	return joinSchema
}

func prepare4HashJoin(testCase *hashJoinTestCase, innerExec, outerExec exec.Executor) *join.HashJoinExec {
	if testCase.useOuterToBuild {
		innerExec, outerExec = outerExec, innerExec
	}
	cols0 := innerExec.Schema().Columns
	cols1 := outerExec.Schema().Columns

	joinSchema := expression.NewSchema()
	if testCase.childrenUsedSchema != nil {
		for i, used := range testCase.childrenUsedSchema[0] {
			if used {
				joinSchema.Append(cols0[i])
			}
		}
		for i, used := range testCase.childrenUsedSchema[1] {
			if used {
				joinSchema.Append(cols1[i])
			}
		}
	} else {
		joinSchema.Append(cols0...)
		joinSchema.Append(cols1...)
	}
	// todo: need systematic way to protect.
	// physical join should resolveIndices to get right schema column index.
	// otherwise, markChildrenUsedColsForTest will fail below.
	joinSchema = prepareResolveIndices(joinSchema, innerExec.Schema(), outerExec.Schema(), core.InnerJoin)

	joinKeysColIdx := make([]int, 0, len(testCase.keyIdx))
	joinKeysColIdx = append(joinKeysColIdx, testCase.keyIdx...)
	probeKeysColIdx := make([]int, 0, len(testCase.keyIdx))
	probeKeysColIdx = append(probeKeysColIdx, testCase.keyIdx...)
	e := &join.HashJoinExec{
		BaseExecutor: exec.NewBaseExecutor(testCase.ctx, joinSchema, 5, innerExec, outerExec),
		HashJoinCtx: &join.HashJoinCtx{
			SessCtx:         testCase.ctx,
			JoinType:        testCase.joinType, // 0 for InnerJoin, 1 for LeftOutersJoin, 2 for RightOuterJoin
			IsOuterJoin:     false,
			UseOuterToBuild: testCase.useOuterToBuild,
			Concurrency:     uint(testCase.concurrency),
			ProbeTypes:      exec.RetTypes(outerExec),
			BuildTypes:      exec.RetTypes(innerExec),
			ChunkAllocPool:  chunk.NewEmptyAllocator(),
		},
		ProbeSideTupleFetcher: &join.ProbeSideTupleFetcher{
			ProbeSideExec: outerExec,
		},
		ProbeWorkers: make([]*join.ProbeWorker, testCase.concurrency),
		BuildWorker: &join.BuildWorker{
			BuildKeyColIdx: joinKeysColIdx,
			BuildSideExec:  innerExec,
		},
	}

	childrenUsedSchema := markChildrenUsedColsForTest(testCase.ctx, e.Schema(), e.Children(0).Schema(), e.Children(1).Schema())
	defaultValues := make([]types.Datum, e.BuildWorker.BuildSideExec.Schema().Len())
	lhsTypes, rhsTypes := exec.RetTypes(innerExec), exec.RetTypes(outerExec)
	for i := uint(0); i < e.Concurrency; i++ {
		e.ProbeWorkers[i] = &join.ProbeWorker{
			WorkerID:    i,
			HashJoinCtx: e.HashJoinCtx,
			Joiner: join.NewJoiner(testCase.ctx, e.JoinType, true, defaultValues,
				nil, lhsTypes, rhsTypes, childrenUsedSchema, false),
			ProbeKeyColIdx: probeKeysColIdx,
		}
	}
	e.BuildWorker.HashJoinCtx = e.HashJoinCtx
	memLimit := int64(-1)
	if testCase.disk {
		memLimit = 1
	}
	t := memory.NewTracker(-1, memLimit)
	t.SetActionOnExceed(nil)
	t2 := disk.NewTracker(-1, -1)
	e.Ctx().GetSessionVars().MemTracker = t
	e.Ctx().GetSessionVars().StmtCtx.MemTracker.AttachTo(t)
	e.Ctx().GetSessionVars().DiskTracker = t2
	e.Ctx().GetSessionVars().StmtCtx.DiskTracker.AttachTo(t2)
	return e
}

// markChildrenUsedColsForTest compares each child with the output schema, and mark
// each column of the child is used by output or not.
func markChildrenUsedColsForTest(ctx sessionctx.Context, outputSchema *expression.Schema, childSchemas ...*expression.Schema) (childrenUsed [][]int) {
	childrenUsed = make([][]int, 0, len(childSchemas))
	markedOffsets := make(map[int]int)
	for originalIdx, col := range outputSchema.Columns {
		markedOffsets[col.Index] = originalIdx
	}
	prefixLen := 0
	type intPair struct {
		first  int
		second int
	}
	// for example here.
	// left child schema: [col11]
	// right child schema: [col21, col22]
	// output schema is [col11, col22, col21], if not records the original derived order after physical resolve index.
	// the lused will be [0], the rused will be [0,1], while the actual order is dismissed, [1,0] is correct for rused.
	for _, childSchema := range childSchemas {
		usedIdxPair := make([]intPair, 0, len(childSchema.Columns))
		for i := range childSchema.Columns {
			if originalIdx, ok := markedOffsets[prefixLen+i]; ok {
				usedIdxPair = append(usedIdxPair, intPair{first: originalIdx, second: i})
			}
		}
		// sort the used idxes according their original indexes derived after resolveIndex.
		slices.SortFunc(usedIdxPair, func(a, b intPair) int {
			return cmp.Compare(a.first, b.first)
		})
		usedIdx := make([]int, 0, len(childSchema.Columns))
		for _, one := range usedIdxPair {
			usedIdx = append(usedIdx, one.second)
		}
		childrenUsed = append(childrenUsed, usedIdx)
		prefixLen += childSchema.Len()
	}
	return
}

func benchmarkHashJoinExecWithCase(b *testing.B, casTest *hashJoinTestCase) {
	opt1 := testutil.MockDataSourceParameters{
		Rows: casTest.rows,
		Ctx:  casTest.ctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeVarString:
				return casTest.rawData
			case mysql.TypeDouble:
				return float64(row)
			default:
				panic("not implement")
			}
		},
	}
	opt2 := opt1
	opt1.DataSchema = expression.NewSchema(casTest.columns()...)
	opt2.DataSchema = expression.NewSchema(casTest.columns()...)
	dataSource1 := testutil.BuildMockDataSource(opt1)
	dataSource2 := testutil.BuildMockDataSource(opt2)
	// Test spill result.
	benchmarkHashJoinExec(b, casTest, dataSource1, dataSource2, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkHashJoinExec(b, casTest, dataSource1, dataSource2, false)
	}
}

func benchmarkHashJoinExec(b *testing.B, casTest *hashJoinTestCase, opt1, opt2 *testutil.MockDataSource, testResult bool) {
	b.StopTimer()
	executor := prepare4HashJoin(casTest, opt1, opt2)
	tmpCtx := context.Background()
	chk := exec.NewFirstChunk(executor)
	opt1.PrepareChunks()
	opt2.PrepareChunks()

	totalRow := 0
	b.StartTimer()
	if err := executor.Open(tmpCtx); err != nil {
		b.Fatal(err)
	}
	for {
		if err := executor.Next(tmpCtx, chk); err != nil {
			b.Fatal(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		totalRow += chk.NumRows()
	}

	if testResult {
		time.Sleep(200 * time.Millisecond)
		if spilled := executor.RowContainer.AlreadySpilledSafeForTest(); spilled != casTest.disk {
			b.Fatal("wrong usage with disk:", spilled, casTest.disk)
		}
	}

	if err := executor.Close(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
	if totalRow == 0 {
		b.Fatal("totalRow == 0")
	}
}

func BenchmarkHashJoinInlineProjection(b *testing.B) {
	cols := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
	}

	b.ReportAllocs()

	{
		cas := defaultHashJoinTestCase(cols, 0, false)
		cas.keyIdx = []int{0}
		cas.childrenUsedSchema = [][]bool{
			{false, true},
			{false, false},
		}
		b.Run("InlineProjection:ON", func(b *testing.B) {
			benchmarkHashJoinExecWithCase(b, cas)
		})
	}

	{
		cas := defaultHashJoinTestCase(cols, 0, false)
		cas.keyIdx = []int{0}
		b.Run("InlineProjection:OFF", func(b *testing.B) {
			benchmarkHashJoinExecWithCase(b, cas)
		})
	}
}

func BenchmarkHashJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	cols := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
	}

	b.ReportAllocs()
	cas := defaultHashJoinTestCase(cols, 0, false)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	cas.disk = true
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	// Replace the wide string column with double column
	cols = []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
	}

	cas = defaultHashJoinTestCase(cols, 0, false)
	cas.keyIdx = []int{0}
	cas.rows = 5
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas = defaultHashJoinTestCase(cols, 0, false)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cols = []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
	}
	cas = defaultHashJoinTestCase(cols, 0, false)
	cas.keyIdx = []int{0}
	cas.disk = true
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})
}

func BenchmarkOuterHashJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	cols := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
	}

	b.ReportAllocs()
	cas := defaultHashJoinTestCase(cols, 2, true)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	cas.disk = true
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	// Replace the wide string column with double column
	cols = []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeDouble),
	}

	cas = defaultHashJoinTestCase(cols, 2, true)
	cas.keyIdx = []int{0}
	cas.rows = 5
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas = defaultHashJoinTestCase(cols, 2, true)
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})

	cas.keyIdx = []int{0}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkHashJoinExecWithCase(b, cas)
	})
}

func benchmarkBuildHashTableForList(b *testing.B, casTest *hashJoinTestCase) {
	opt := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(casTest.columns()...),
		Rows:       casTest.rows,
		Ctx:        casTest.ctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeVarString:
				return casTest.rawData
			default:
				panic("not implement")
			}
		},
	}
	dataSource1 := testutil.BuildMockDataSource(opt)
	dataSource2 := testutil.BuildMockDataSource(opt)

	dataSource1.PrepareChunks()
	benchmarkBuildHashTable(b, casTest, dataSource1, dataSource2, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkBuildHashTable(b, casTest, dataSource1, dataSource2, false)
	}
}

func benchmarkBuildHashTable(b *testing.B, casTest *hashJoinTestCase, dataSource1, dataSource2 *testutil.MockDataSource, testResult bool) {
	b.StopTimer()
	exec := prepare4HashJoin(casTest, dataSource1, dataSource2)
	tmpCtx := context.Background()
	if err := exec.Open(tmpCtx); err != nil {
		b.Fatal(err)
	}
	exec.Prepared = true

	innerResultCh := make(chan *chunk.Chunk, len(dataSource1.Chunks))
	for _, chk := range dataSource1.Chunks {
		innerResultCh <- chk
	}
	close(innerResultCh)

	b.StartTimer()
	if err := exec.BuildWorker.BuildHashTableForList(innerResultCh); err != nil {
		b.Fatal(err)
	}

	if testResult {
		time.Sleep(200 * time.Millisecond)
		if exec.RowContainer.AlreadySpilledSafeForTest() != casTest.disk {
			b.Fatal("wrong usage with disk")
		}
	}

	if err := exec.Close(); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func BenchmarkBuildHashTableForList(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	cols := []*types.FieldType{
		types.NewFieldType(mysql.TypeLonglong),
		types.NewFieldType(mysql.TypeVarString),
	}

	b.ReportAllocs()
	cas := defaultHashJoinTestCase(cols, 0, false)
	rows := []int{10, 100000}
	keyIdxs := [][]int{{0, 1}, {0}}
	disks := []bool{false, true}
	for _, row := range rows {
		for _, keyIdx := range keyIdxs {
			for _, disk := range disks {
				cas.rows = row
				cas.keyIdx = keyIdx
				cas.disk = disk
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkBuildHashTableForList(b, cas)
				})
			}
		}
	}
}

type IndexJoinTestCase struct {
	OuterRows       int
	InnerRows       int
	Concurrency     int
	Ctx             sessionctx.Context
	OuterJoinKeyIdx []int
	InnerJoinKeyIdx []int
	OuterHashKeyIdx []int
	InnerHashKeyIdx []int
	InnerIdx        []int
	NeedOuterSort   bool
	RawData         string
}

func (tc IndexJoinTestCase) Columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
	}
}

func defaultIndexJoinTestCase() *IndexJoinTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().SnapshotTS = 1
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	tc := &IndexJoinTestCase{
		OuterRows:       100000,
		InnerRows:       variable.DefMaxChunkSize * 100,
		Concurrency:     4,
		Ctx:             ctx,
		OuterJoinKeyIdx: []int{0, 1},
		InnerJoinKeyIdx: []int{0, 1},
		OuterHashKeyIdx: []int{0, 1},
		InnerHashKeyIdx: []int{0, 1},
		InnerIdx:        []int{0, 1},
		RawData:         wideString,
	}
	return tc
}

func (tc IndexJoinTestCase) String() string {
	return fmt.Sprintf("(outerRows:%v, innerRows:%v, concurency:%v, outerJoinKeyIdx: %v, innerJoinKeyIdx: %v, NeedOuterSort:%v)",
		tc.OuterRows, tc.InnerRows, tc.Concurrency, tc.OuterJoinKeyIdx, tc.InnerJoinKeyIdx, tc.NeedOuterSort)
}
func (tc IndexJoinTestCase) GetMockDataSourceOptByRows(rows int) testutil.MockDataSourceParameters {
	return testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(tc.Columns()...),
		Rows:       rows,
		Ctx:        tc.Ctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeDouble:
				return float64(row)
			case mysql.TypeVarString:
				return tc.RawData
			default:
				panic("not implement")
			}
		},
	}
}

func prepare4IndexInnerHashJoin(tc *IndexJoinTestCase, outerDS *testutil.MockDataSource, innerDS *testutil.MockDataSource) (exec.Executor, error) {
	outerCols, innerCols := tc.Columns(), tc.Columns()
	joinSchema := expression.NewSchema(outerCols...)
	joinSchema.Append(innerCols...)
	leftTypes, rightTypes := exec.RetTypes(outerDS), exec.RetTypes(innerDS)
	defaultValues := make([]types.Datum, len(innerCols))
	colLens := make([]int, len(innerCols))
	for i := range colLens {
		colLens[i] = types.UnspecifiedLength
	}
	keyOff2IdxOff := make([]int, len(tc.OuterJoinKeyIdx))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = i
	}

	readerBuilder, err := newExecutorBuilder(tc.Ctx, nil).
		newDataReaderBuilder(&mockPhysicalIndexReader{e: innerDS})
	if err != nil {
		return nil, err
	}

	e := &join.IndexLookUpJoin{
		BaseExecutor: exec.NewBaseExecutor(tc.Ctx, joinSchema, 1, outerDS),
		OuterCtx: join.OuterCtx{
			RowTypes: leftTypes,
			KeyCols:  tc.OuterJoinKeyIdx,
			HashCols: tc.OuterHashKeyIdx,
		},
		InnerCtx: join.InnerCtx{
			ReaderBuilder: readerBuilder,
			RowTypes:      rightTypes,
			ColLens:       colLens,
			KeyCols:       tc.InnerJoinKeyIdx,
			HashCols:      tc.InnerHashKeyIdx,
		},
		WorkerWg:      new(sync.WaitGroup),
		Joiner:        join.NewJoiner(tc.Ctx, 0, false, defaultValues, nil, leftTypes, rightTypes, nil, false),
		IsOuterJoin:   false,
		KeyOff2IdxOff: keyOff2IdxOff,
		LastColHelper: nil,
	}
	e.JoinResult = exec.NewFirstChunk(e)
	return e, nil
}

func prepare4IndexOuterHashJoin(tc *IndexJoinTestCase, outerDS *testutil.MockDataSource, innerDS *testutil.MockDataSource) (exec.Executor, error) {
	e, err := prepare4IndexInnerHashJoin(tc, outerDS, innerDS)
	if err != nil {
		return nil, err
	}
	idxHash := &join.IndexNestedLoopHashJoin{IndexLookUpJoin: *e.(*join.IndexLookUpJoin)}
	concurrency := tc.Concurrency
	idxHash.Joiners = make([]join.Joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.Joiners[i] = e.(*join.IndexLookUpJoin).Joiner.Clone()
	}
	return idxHash, nil
}

func prepare4IndexMergeJoin(tc *IndexJoinTestCase, outerDS *testutil.MockDataSource, innerDS *testutil.MockDataSource) (exec.Executor, error) {
	outerCols, innerCols := tc.Columns(), tc.Columns()
	joinSchema := expression.NewSchema(outerCols...)
	joinSchema.Append(innerCols...)
	outerJoinKeys := make([]*expression.Column, 0, len(tc.OuterJoinKeyIdx))
	innerJoinKeys := make([]*expression.Column, 0, len(tc.InnerJoinKeyIdx))
	for _, keyIdx := range tc.OuterJoinKeyIdx {
		outerJoinKeys = append(outerJoinKeys, outerCols[keyIdx])
	}
	for _, keyIdx := range tc.InnerJoinKeyIdx {
		innerJoinKeys = append(innerJoinKeys, innerCols[keyIdx])
	}
	leftTypes, rightTypes := exec.RetTypes(outerDS), exec.RetTypes(innerDS)
	defaultValues := make([]types.Datum, len(innerCols))
	colLens := make([]int, len(innerCols))
	for i := range colLens {
		colLens[i] = types.UnspecifiedLength
	}
	keyOff2IdxOff := make([]int, len(outerJoinKeys))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = i
	}

	compareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	outerCompareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	for i := range outerJoinKeys {
		compareFuncs = append(compareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], innerJoinKeys[i]))
		outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], outerJoinKeys[i]))
	}

	readerBuilder, err := newExecutorBuilder(tc.Ctx, nil).
		newDataReaderBuilder(&mockPhysicalIndexReader{e: innerDS})
	if err != nil {
		return nil, err
	}

	e := &join.IndexLookUpMergeJoin{
		BaseExecutor: exec.NewBaseExecutor(tc.Ctx, joinSchema, 2, outerDS),
		OuterMergeCtx: join.OuterMergeCtx{
			RowTypes:      leftTypes,
			KeyCols:       tc.OuterJoinKeyIdx,
			JoinKeys:      outerJoinKeys,
			NeedOuterSort: tc.NeedOuterSort,
			CompareFuncs:  outerCompareFuncs,
		},
		InnerMergeCtx: join.InnerMergeCtx{
			ReaderBuilder: readerBuilder,
			RowTypes:      rightTypes,
			JoinKeys:      innerJoinKeys,
			ColLens:       colLens,
			KeyCols:       tc.InnerJoinKeyIdx,
			CompareFuncs:  compareFuncs,
		},
		WorkerWg:      new(sync.WaitGroup),
		IsOuterJoin:   false,
		KeyOff2IdxOff: keyOff2IdxOff,
		LastColHelper: nil,
	}
	concurrency := e.Ctx().GetSessionVars().IndexLookupJoinConcurrency()
	joiners := make([]join.Joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		joiners[i] = join.NewJoiner(tc.Ctx, 0, false, defaultValues, nil, leftTypes, rightTypes, nil, false)
	}
	e.Joiners = joiners
	return e, nil
}

type indexJoinType int8

const (
	indexInnerHashJoin indexJoinType = iota
	indexOuterHashJoin
	indexMergeJoin
)

func benchmarkIndexJoinExecWithCase(
	b *testing.B,
	tc *IndexJoinTestCase,
	outerDS *testutil.MockDataSource,
	innerDS *testutil.MockDataSource,
	execType indexJoinType,
) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var executor exec.Executor
		var err error
		switch execType {
		case indexInnerHashJoin:
			executor, err = prepare4IndexInnerHashJoin(tc, outerDS, innerDS)
		case indexOuterHashJoin:
			executor, err = prepare4IndexOuterHashJoin(tc, outerDS, innerDS)
		case indexMergeJoin:
			executor, err = prepare4IndexMergeJoin(tc, outerDS, innerDS)
		}

		if err != nil {
			b.Fatal(err)
		}

		tmpCtx := context.Background()
		chk := exec.NewFirstChunk(executor)
		outerDS.PrepareChunks()
		innerDS.PrepareChunks()

		b.StartTimer()
		if err = executor.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := executor.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := executor.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkIndexJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	b.ReportAllocs()
	tc := defaultIndexJoinTestCase()
	outerOpt := tc.GetMockDataSourceOptByRows(tc.OuterRows)
	innerOpt := tc.GetMockDataSourceOptByRows(tc.InnerRows)
	outerDS := testutil.BuildMockDataSourceWithIndex(outerOpt, tc.InnerIdx)
	innerDS := testutil.BuildMockDataSourceWithIndex(innerOpt, tc.InnerIdx)

	tc.NeedOuterSort = true
	b.Run(fmt.Sprintf("index merge join need outer sort %v", tc), func(b *testing.B) {
		benchmarkIndexJoinExecWithCase(b, tc, outerDS, innerDS, indexMergeJoin)
	})

	tc.NeedOuterSort = false
	b.Run(fmt.Sprintf("index merge join %v", tc), func(b *testing.B) {
		benchmarkIndexJoinExecWithCase(b, tc, outerDS, innerDS, indexMergeJoin)
	})

	b.Run(fmt.Sprintf("index inner hash join %v", tc), func(b *testing.B) {
		benchmarkIndexJoinExecWithCase(b, tc, outerDS, innerDS, indexInnerHashJoin)
	})

	b.Run(fmt.Sprintf("index outer hash join %v", tc), func(b *testing.B) {
		benchmarkIndexJoinExecWithCase(b, tc, outerDS, innerDS, indexOuterHashJoin)
	})
}

type mergeJoinTestCase struct {
	IndexJoinTestCase
	childrenUsedSchema [][]bool
}

func prepareMergeJoinExec(tc *mergeJoinTestCase, joinSchema *expression.Schema, leftExec, rightExec exec.Executor, defaultValues []types.Datum,
	compareFuncs []expression.CompareFunc, innerJoinKeys []*expression.Column, outerJoinKeys []*expression.Column) *join.MergeJoinExec {
	// only benchmark inner join
	mergeJoinExec := &join.MergeJoinExec{
		StmtCtx:      tc.Ctx.GetSessionVars().StmtCtx,
		BaseExecutor: exec.NewBaseExecutor(tc.Ctx, joinSchema, 3, leftExec, rightExec),
		CompareFuncs: compareFuncs,
		IsOuterJoin:  false,
	}

	var usedIdx [][]int
	if tc.childrenUsedSchema != nil {
		usedIdx = make([][]int, 0, len(tc.childrenUsedSchema))
		for _, childSchema := range tc.childrenUsedSchema {
			used := make([]int, 0, len(childSchema))
			for idx, one := range childSchema {
				if one {
					used = append(used, idx)
				}
			}
			usedIdx = append(usedIdx, used)
		}
	}

	mergeJoinExec.Joiner = join.NewJoiner(
		tc.Ctx,
		0,
		false,
		defaultValues,
		nil,
		exec.RetTypes(leftExec),
		exec.RetTypes(rightExec),
		usedIdx,
		false,
	)

	mergeJoinExec.InnerTable = &join.MergeJoinTable{
		IsInner:    true,
		ChildIndex: 1,
		JoinKeys:   innerJoinKeys,
	}

	mergeJoinExec.OuterTable = &join.MergeJoinTable{
		ChildIndex: 0,
		Filters:    nil,
		JoinKeys:   outerJoinKeys,
	}

	return mergeJoinExec
}

func prepare4MergeJoin(tc *mergeJoinTestCase, innerDS, outerDS *testutil.MockDataSource, sorted bool, concurrency int) exec.Executor {
	outerCols, innerCols := tc.Columns(), tc.Columns()

	joinSchema := expression.NewSchema()
	if tc.childrenUsedSchema != nil {
		for i, used := range tc.childrenUsedSchema[0] {
			if used {
				joinSchema.Append(outerCols[i])
			}
		}
		for i, used := range tc.childrenUsedSchema[1] {
			if used {
				joinSchema.Append(innerCols[i])
			}
		}
	} else {
		joinSchema.Append(outerCols...)
		joinSchema.Append(innerCols...)
	}

	outerJoinKeys := make([]*expression.Column, 0, len(tc.OuterJoinKeyIdx))
	innerJoinKeys := make([]*expression.Column, 0, len(tc.InnerJoinKeyIdx))
	for _, keyIdx := range tc.OuterJoinKeyIdx {
		outerJoinKeys = append(outerJoinKeys, outerCols[keyIdx])
	}
	for _, keyIdx := range tc.InnerJoinKeyIdx {
		innerJoinKeys = append(innerJoinKeys, innerCols[keyIdx])
	}
	compareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	for i := range outerJoinKeys {
		compareFuncs = append(compareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], innerJoinKeys[i]))
	}

	defaultValues := make([]types.Datum, len(innerCols))

	var leftExec, rightExec exec.Executor
	if sorted {
		leftSortExec := &sortexec.SortExec{
			BaseExecutor: exec.NewBaseExecutor(tc.Ctx, innerDS.Schema(), 3, innerDS),
			ByItems:      make([]*util.ByItems, 0, len(tc.InnerJoinKeyIdx)),
			ExecSchema:   innerDS.Schema(),
		}
		for _, key := range innerJoinKeys {
			leftSortExec.ByItems = append(leftSortExec.ByItems, &util.ByItems{Expr: key})
		}
		leftExec = leftSortExec

		rightSortExec := &sortexec.SortExec{
			BaseExecutor: exec.NewBaseExecutor(tc.Ctx, outerDS.Schema(), 4, outerDS),
			ByItems:      make([]*util.ByItems, 0, len(tc.OuterJoinKeyIdx)),
			ExecSchema:   outerDS.Schema(),
		}
		for _, key := range outerJoinKeys {
			rightSortExec.ByItems = append(rightSortExec.ByItems, &util.ByItems{Expr: key})
		}
		rightExec = rightSortExec
	} else {
		leftExec = innerDS
		rightExec = outerDS
	}

	var e exec.Executor
	if concurrency == 1 {
		e = prepareMergeJoinExec(tc, joinSchema, leftExec, rightExec, defaultValues, compareFuncs, innerJoinKeys, outerJoinKeys)
	} else {
		// build dataSources
		dataSources := []exec.Executor{leftExec, rightExec}
		// build splitters
		innerByItems := make([]expression.Expression, 0, len(innerJoinKeys))
		for _, innerJoinKey := range innerJoinKeys {
			innerByItems = append(innerByItems, innerJoinKey)
		}
		outerByItems := make([]expression.Expression, 0, len(outerJoinKeys))
		for _, outerJoinKey := range outerJoinKeys {
			outerByItems = append(outerByItems, outerJoinKey)
		}
		splitters := []partitionSplitter{
			&partitionHashSplitter{
				byItems:    innerByItems,
				numWorkers: concurrency,
			},
			&partitionHashSplitter{
				byItems:    outerByItems,
				numWorkers: concurrency,
			},
		}
		// build ShuffleMergeJoinExec
		shuffle := &ShuffleExec{
			BaseExecutor: exec.NewBaseExecutor(tc.Ctx, joinSchema, 4),
			concurrency:  concurrency,
			dataSources:  dataSources,
			splitters:    splitters,
		}

		// build workers, only benchmark inner join
		shuffle.workers = make([]*shuffleWorker, shuffle.concurrency)
		for i := range shuffle.workers {
			leftReceiver := shuffleReceiver{
				BaseExecutor: exec.NewBaseExecutor(tc.Ctx, leftExec.Schema(), 0),
			}
			rightReceiver := shuffleReceiver{
				BaseExecutor: exec.NewBaseExecutor(tc.Ctx, rightExec.Schema(), 0),
			}
			w := &shuffleWorker{
				receivers: []*shuffleReceiver{&leftReceiver, &rightReceiver},
			}
			w.childExec = prepareMergeJoinExec(tc, joinSchema, &leftReceiver, &rightReceiver, defaultValues, compareFuncs, innerJoinKeys, outerJoinKeys)

			shuffle.workers[i] = w
		}
		e = shuffle
	}

	return e
}

func newMergeJoinBenchmark(numOuterRows, numInnerDup, numInnerRedundant int) (tc *mergeJoinTestCase, innerDS, outerDS *testutil.MockDataSource) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().SnapshotTS = 1
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)

	numInnerRows := numOuterRows*numInnerDup + numInnerRedundant
	itc := &IndexJoinTestCase{
		OuterRows:       numOuterRows,
		InnerRows:       numInnerRows,
		Concurrency:     4,
		Ctx:             ctx,
		OuterJoinKeyIdx: []int{0, 1},
		InnerJoinKeyIdx: []int{0, 1},
		OuterHashKeyIdx: []int{0, 1},
		InnerHashKeyIdx: []int{0, 1},
		InnerIdx:        []int{0, 1},
		RawData:         wideString,
	}
	tc = &mergeJoinTestCase{*itc, nil}
	outerOpt := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(tc.Columns()...),
		Rows:       numOuterRows,
		Ctx:        tc.Ctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeDouble:
				return float64(row)
			case mysql.TypeVarString:
				return tc.RawData
			default:
				panic("not implement")
			}
		},
	}

	innerOpt := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(tc.Columns()...),
		Rows:       numInnerRows,
		Ctx:        tc.Ctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			row = row / numInnerDup
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeDouble:
				return float64(row)
			case mysql.TypeVarString:
				return tc.RawData
			default:
				panic("not implement")
			}
		},
	}

	innerDS = testutil.BuildMockDataSource(innerOpt)
	outerDS = testutil.BuildMockDataSource(outerOpt)

	return
}

type mergeJoinType int8

const (
	innerMergeJoin mergeJoinType = iota
)

func benchmarkMergeJoinExecWithCase(b *testing.B, tc *mergeJoinTestCase, innerDS, outerDS *testutil.MockDataSource, joinType mergeJoinType) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var executor exec.Executor
		switch joinType {
		case innerMergeJoin:
			executor = prepare4MergeJoin(tc, innerDS, outerDS, true, 2)
		}

		tmpCtx := context.Background()
		chk := exec.NewFirstChunk(executor)
		outerDS.PrepareChunks()
		innerDS.PrepareChunks()

		b.StartTimer()
		if err := executor.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := executor.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := executor.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkMergeJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)
	b.ReportAllocs()

	totalRows := 300000

	innerDupAndRedundant := [][]int{
		{1, 0},
		{100, 0},
		{10000, 0},
		{1, 30000},
	}

	childrenUsedSchemas := [][][]bool{
		nil,
		{
			{true, false, false},
			{false, true, false},
		},
	}

	for _, params := range innerDupAndRedundant {
		numInnerDup, numInnerRedundant := params[0], params[1]
		for _, childrenUsedSchema := range childrenUsedSchemas {
			tc, innerDS, outerDS := newMergeJoinBenchmark(totalRows/numInnerDup, numInnerDup, numInnerRedundant)
			inlineProj := false
			if childrenUsedSchema != nil {
				inlineProj = true
				tc.childrenUsedSchema = childrenUsedSchema
			}

			b.Run(fmt.Sprintf("merge join %v InlineProj:%v", tc, inlineProj), func(b *testing.B) {
				benchmarkMergeJoinExecWithCase(b, tc, outerDS, innerDS, innerMergeJoin)
			})
		}
	}
}

func benchmarkLimitExec(b *testing.B, cas *testutil.LimitCase) {
	opt := testutil.MockDataSourceParameters{
		DataSchema: expression.NewSchema(cas.Columns()...),
		Rows:       cas.Rows,
		Ctx:        cas.Ctx,
	}
	dataSource := testutil.BuildMockDataSource(opt)
	var exe exec.Executor
	limit := &LimitExec{
		BaseExecutor: exec.NewBaseExecutor(cas.Ctx, dataSource.Schema(), 4, dataSource),
		begin:        uint64(cas.Offset),
		end:          uint64(cas.Offset + cas.Count),
	}
	if cas.UsingInlineProjection {
		if len(cas.ChildUsedSchema) > 0 {
			limit.columnIdxsUsedByChild = make([]int, 0, len(cas.ChildUsedSchema))
			for i, used := range cas.ChildUsedSchema {
				if used {
					limit.columnIdxsUsedByChild = append(limit.columnIdxsUsedByChild, i)
				}
			}
		}
		exe = limit
	} else {
		columns := cas.Columns()
		usedCols := make([]*expression.Column, 0, len(columns))
		exprs := make([]expression.Expression, 0, len(columns))
		for i, used := range cas.ChildUsedSchema {
			if used {
				usedCols = append(usedCols, columns[i])
				exprs = append(exprs, columns[i])
			}
		}
		proj := &ProjectionExec{
			BaseExecutor:  exec.NewBaseExecutor(cas.Ctx, expression.NewSchema(usedCols...), 0, limit),
			numWorkers:    1,
			evaluatorSuit: expression.NewEvaluatorSuite(exprs, false),
		}
		exe = proj
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpCtx := context.Background()
		chk := exec.NewFirstChunk(exe)
		dataSource.PrepareChunks()

		b.StartTimer()
		if err := exe.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exe.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := exe.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkLimitExec(b *testing.B) {
	b.ReportAllocs()
	cas := testutil.DefaultLimitTestCase()
	usingInlineProjection := []bool{false, true}
	for _, inlineProjection := range usingInlineProjection {
		cas.UsingInlineProjection = inlineProjection
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkLimitExec(b, cas)
		})
	}
}

func BenchmarkReadLastLinesOfHugeLine(b *testing.B) {
	// step 1. initial a huge line log file
	hugeLine := make([]byte, 1024*1024*10)
	for i := range hugeLine {
		hugeLine[i] = 'a' + byte(i%26)
	}
	fileName := "tidb.log"
	err := os.WriteFile(fileName, hugeLine, 0644)
	if err != nil {
		b.Fatal(err)
	}
	file, err := os.OpenFile(fileName, os.O_RDONLY, os.ModePerm)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		file.Close()
		os.Remove(fileName)
	}()
	stat, _ := file.Stat()
	filesize := stat.Size()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, n, err := readLastLines(context.Background(), file, filesize)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(hugeLine) {
			b.Fatalf("len %v, expected: %v", n, len(hugeLine))
		}
	}
}

func BenchmarkAggPartialResultMapperMemoryUsage(b *testing.B) {
	b.ReportAllocs()
	type testCase struct {
		rowNum int
	}
	cases := []testCase{
		{
			rowNum: 0,
		},
		{
			rowNum: 100,
		},
		{
			rowNum: 10000,
		},
		{
			rowNum: 1000000,
		},
		{
			rowNum: 851968, // 6.5 * (1 << 17)
		},
		{
			rowNum: 851969, // 6.5 * (1 << 17) + 1
		},
		{
			rowNum: 425984, // 6.5 * (1 << 16)
		},
		{
			rowNum: 425985, // 6.5 * (1 << 16) + 1
		},
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("MapRows %v", c.rowNum), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				aggMap := make(aggfuncs.AggPartialResultMapper)
				tempSlice := make([]aggfuncs.PartialResult, 10)
				for num := 0; num < c.rowNum; num++ {
					aggMap[strconv.Itoa(num)] = tempSlice
				}
			}
		})
	}
}

func BenchmarkPipelinedRowNumberWindowFunctionExecution(b *testing.B) {
	b.ReportAllocs()
}

func BenchmarkCompleteInsertErr(b *testing.B) {
	b.ReportAllocs()
	col := &model.ColumnInfo{
		Name:      model.NewCIStr("a"),
		FieldType: *types.NewFieldType(mysql.TypeBlob),
	}
	err := types.ErrWarnDataOutOfRange
	for n := 0; n < b.N; n++ {
		completeInsertErr(col, nil, 0, err)
	}
}

func BenchmarkCompleteLoadErr(b *testing.B) {
	b.ReportAllocs()
	col := &model.ColumnInfo{
		Name: model.NewCIStr("a"),
	}
	err := types.ErrDataTooLong
	for n := 0; n < b.N; n++ {
		completeLoadErr(col, 0, err)
	}
}
