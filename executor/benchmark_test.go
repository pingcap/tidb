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
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/zap/zapcore"
)

var (
	_          Executor          = &mockDataSource{}
	_          core.PhysicalPlan = &mockDataPhysicalPlan{}
	wideString                   = strings.Repeat("x", 5*1024)
)

type mockDataSourceParameters struct {
	schema      *expression.Schema
	genDataFunc func(row int, typ *types.FieldType) interface{}
	ndvs        []int  // number of distinct values on columns[i] and zero represents no limit
	orders      []bool // columns[i] should be ordered if orders[i] is true
	rows        int    // number of rows the DataSource should output
	ctx         sessionctx.Context
}

type mockDataSource struct {
	baseExecutor
	p        mockDataSourceParameters
	genData  []*chunk.Chunk
	chunks   []*chunk.Chunk
	chunkPtr int
}

type mockDataPhysicalPlan struct {
	MockPhysicalPlan
	schema *expression.Schema
	exec   Executor
}

func (mp *mockDataPhysicalPlan) GetExecutor() Executor {
	return mp.exec
}

func (mp *mockDataPhysicalPlan) Schema() *expression.Schema {
	return mp.schema
}

func (mp *mockDataPhysicalPlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return "mockData_0"
	})
}

func (mp *mockDataPhysicalPlan) ID() int {
	return 0
}

func (mp *mockDataPhysicalPlan) Stats() *property.StatsInfo {
	return nil
}

func (mp *mockDataPhysicalPlan) SelectBlockOffset() int {
	return 0
}

func buildMockDataPhysicalPlan(ctx sessionctx.Context, srcExec Executor) *mockDataPhysicalPlan {
	return &mockDataPhysicalPlan{
		schema: srcExec.Schema(),
		exec:   srcExec,
	}
}

func (mds *mockDataSource) genColDatums(col int) (results []interface{}) {
	typ := mds.retFieldTypes[col]
	order := false
	if col < len(mds.p.orders) {
		order = mds.p.orders[col]
	}
	rows := mds.p.rows
	NDV := 0
	if col < len(mds.p.ndvs) {
		NDV = mds.p.ndvs[col]
	}
	results = make([]interface{}, 0, rows)
	if NDV == 0 {
		if mds.p.genDataFunc == nil {
			for i := 0; i < rows; i++ {
				results = append(results, mds.randDatum(typ))
			}
		} else {
			for i := 0; i < rows; i++ {
				results = append(results, mds.p.genDataFunc(i, typ))
			}
		}
	} else {
		datumSet := make(map[string]bool, NDV)
		datums := make([]interface{}, 0, NDV)
		for len(datums) < NDV {
			d := mds.randDatum(typ)
			str := fmt.Sprintf("%v", d)
			if datumSet[str] {
				continue
			}
			datumSet[str] = true
			datums = append(datums, d)
		}

		for i := 0; i < rows; i++ {
			results = append(results, datums[rand.Intn(NDV)])
		}
	}

	if order {
		sort.Slice(results, func(i, j int) bool {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return results[i].(int64) < results[j].(int64)
			case mysql.TypeDouble:
				return results[i].(float64) < results[j].(float64)
			case mysql.TypeVarString:
				return results[i].(string) < results[j].(string)
			default:
				panic("not implement")
			}
		})
	}

	return
}

func (mds *mockDataSource) randDatum(typ *types.FieldType) interface{} {
	switch typ.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong:
		return int64(rand.Int())
	case mysql.TypeFloat:
		return rand.Float32()
	case mysql.TypeDouble:
		return rand.Float64()
	case mysql.TypeNewDecimal:
		var d types.MyDecimal
		return d.FromInt(int64(rand.Int()))
	case mysql.TypeVarString:
		buff := make([]byte, 10)
		rand.Read(buff)
		return base64.RawURLEncoding.EncodeToString(buff)
	default:
		panic("not implement")
	}
}

func (mds *mockDataSource) prepareChunks() {
	mds.chunks = make([]*chunk.Chunk, len(mds.genData))
	for i := range mds.chunks {
		mds.chunks[i] = mds.genData[i].CopyConstruct()
	}
	mds.chunkPtr = 0
}

func (mds *mockDataSource) Next(ctx context.Context, req *chunk.Chunk) error {
	if mds.chunkPtr >= len(mds.chunks) {
		req.Reset()
		return nil
	}
	dataChk := mds.chunks[mds.chunkPtr]
	dataChk.SwapColumns(req)
	mds.chunkPtr++
	return nil
}

func buildMockDataSource(opt mockDataSourceParameters) *mockDataSource {
	baseExec := newBaseExecutor(opt.ctx, opt.schema, 0)
	m := &mockDataSource{baseExec, opt, nil, nil, 0}
	rTypes := retTypes(m)
	colData := make([][]interface{}, len(rTypes))
	for i := 0; i < len(rTypes); i++ {
		colData[i] = m.genColDatums(i)
	}

	m.genData = make([]*chunk.Chunk, (m.p.rows+m.maxChunkSize-1)/m.maxChunkSize)
	for i := range m.genData {
		m.genData[i] = chunk.NewChunkWithCapacity(retTypes(m), m.maxChunkSize)
	}

	for i := 0; i < m.p.rows; i++ {
		idx := i / m.maxChunkSize
		retTypes := retTypes(m)
		for colIdx := 0; colIdx < len(rTypes); colIdx++ {
			switch retTypes[colIdx].GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				m.genData[idx].AppendInt64(colIdx, colData[colIdx][i].(int64))
			case mysql.TypeFloat:
				m.genData[idx].AppendFloat32(colIdx, colData[colIdx][i].(float32))
			case mysql.TypeDouble:
				m.genData[idx].AppendFloat64(colIdx, colData[colIdx][i].(float64))
			case mysql.TypeNewDecimal:
				m.genData[idx].AppendMyDecimal(colIdx, colData[colIdx][i].(*types.MyDecimal))
			case mysql.TypeVarString:
				m.genData[idx].AppendString(colIdx, colData[colIdx][i].(string))
			default:
				panic("not implement")
			}
		}
	}
	return m
}

func buildMockDataSourceWithIndex(opt mockDataSourceParameters, index []int) *mockDataSource {
	opt.orders = make([]bool, len(opt.schema.Columns))
	for _, idx := range index {
		opt.orders[idx] = true
	}
	return buildMockDataSource(opt)
}

// aggTestCase has a fixed schema (aggCol Double, groupBy LongLong).
type aggTestCase struct {
	execType         string // "hash" or "stream"
	aggFunc          string // sum, avg, count ....
	groupByNDV       int    // the number of distinct group-by keys
	hasDistinct      bool
	rows             int
	concurrency      int
	dataSourceSorted bool
	ctx              sessionctx.Context
}

func (a aggTestCase) columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

func (a aggTestCase) String() string {
	return fmt.Sprintf("(execType:%v, aggFunc:%v, ndv:%v, hasDistinct:%v, rows:%v, concurrency:%v, sorted:%v)",
		a.execType, a.aggFunc, a.groupByNDV, a.hasDistinct, a.rows, a.concurrency, a.dataSourceSorted)
}

func defaultAggTestCase(exec string) *aggTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	return &aggTestCase{exec, ast.AggFuncSum, 1000, false, 10000000, 4, true, ctx}
}

func buildHashAggExecutor(ctx sessionctx.Context, src Executor, schema *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression) Executor {
	plan := new(core.PhysicalHashAgg)
	plan.AggFuncs = aggFuncs
	plan.GroupByItems = groupItems
	plan.SetSchema(schema)
	plan.Init(ctx, nil, 0)
	plan.SetChildren(nil)
	b := newExecutorBuilder(ctx, nil, nil, oracle.GlobalTxnScope)
	exec := b.build(plan)
	hashAgg := exec.(*HashAggExec)
	hashAgg.children[0] = src
	return exec
}

func buildStreamAggExecutor(ctx sessionctx.Context, srcExec Executor, schema *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression, concurrency int, dataSourceSorted bool) Executor {
	src := buildMockDataPhysicalPlan(ctx, srcExec)

	sg := new(core.PhysicalStreamAgg)
	sg.AggFuncs = aggFuncs
	sg.GroupByItems = groupItems
	sg.SetSchema(schema)
	sg.Init(ctx, nil, 0)

	var tail core.PhysicalPlan = sg
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
		plan     core.PhysicalPlan
		splitter core.PartitionSplitterType = core.PartitionHashSplitterType
	)
	if concurrency > 1 {
		if dataSourceSorted {
			splitter = core.PartitionRangeSplitterType
		}
		plan = core.PhysicalShuffle{
			Concurrency:  concurrency,
			Tails:        []core.PhysicalPlan{tail},
			DataSources:  []core.PhysicalPlan{src},
			SplitterType: splitter,
			ByItemArrays: [][]expression.Expression{sg.GroupByItems},
		}.Init(ctx, nil, 0)
		plan.SetChildren(sg)
	} else {
		plan = sg
	}

	b := newExecutorBuilder(ctx, nil, nil, oracle.GlobalTxnScope)
	return b.build(plan)
}

func buildAggExecutor(b *testing.B, testCase *aggTestCase, child Executor) Executor {
	ctx := testCase.ctx
	if testCase.execType == "stream" {
		if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBStreamAggConcurrency, fmt.Sprintf("%v", testCase.concurrency)); err != nil {
			b.Fatal(err)
		}
	} else {
		if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggFinalConcurrency, fmt.Sprintf("%v", testCase.concurrency)); err != nil {
			b.Fatal(err)
		}
		if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggPartialConcurrency, fmt.Sprintf("%v", testCase.concurrency)); err != nil {
			b.Fatal(err)
		}
	}

	childCols := testCase.columns()
	schema := expression.NewSchema(childCols...)
	groupBy := []expression.Expression{childCols[1]}
	aggFunc, err := aggregation.NewAggFuncDesc(testCase.ctx, testCase.aggFunc, []expression.Expression{childCols[0]}, testCase.hasDistinct)
	if err != nil {
		b.Fatal(err)
	}
	aggFuncs := []*aggregation.AggFuncDesc{aggFunc}

	var aggExec Executor
	switch testCase.execType {
	case "hash":
		aggExec = buildHashAggExecutor(testCase.ctx, child, schema, aggFuncs, groupBy)
	case "stream":
		aggExec = buildStreamAggExecutor(testCase.ctx, child, schema, aggFuncs, groupBy, testCase.concurrency, testCase.dataSourceSorted)
	default:
		b.Fatal("not implement")
	}
	return aggExec
}

func benchmarkAggExecWithCase(b *testing.B, casTest *aggTestCase) {
	if err := casTest.ctx.GetSessionVars().SetSystemVar(variable.TiDBStreamAggConcurrency, fmt.Sprintf("%v", casTest.concurrency)); err != nil {
		b.Fatal(err)
	}

	cols := casTest.columns()
	dataSource := buildMockDataSource(mockDataSourceParameters{
		schema: expression.NewSchema(cols...),
		ndvs:   []int{0, casTest.groupByNDV},
		orders: []bool{false, casTest.dataSourceSorted},
		rows:   casTest.rows,
		ctx:    casTest.ctx,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new agg-executor
		aggExec := buildAggExecutor(b, casTest, dataSource)
		tmpCtx := context.Background()
		chk := newFirstChunk(aggExec)
		dataSource.prepareChunks()

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
				cas := defaultAggTestCase("stream")
				cas.rows = row
				cas.dataSourceSorted = sorted
				cas.concurrency = con
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
			cas := defaultAggTestCase("hash")
			cas.rows = row
			cas.concurrency = con
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
			cas := defaultAggTestCase(exec)
			cas.groupByNDV = NDV
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
			cas := defaultAggTestCase(exec)
			cas.concurrency = con
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
				cas := defaultAggTestCase(exec)
				cas.rows = row
				cas.hasDistinct = distinct
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkAggExecWithCase(b, cas)
				})
			}
		}
	}
}

func buildWindowExecutor(ctx sessionctx.Context, windowFunc string, funcs int, frame *core.WindowFrame, srcExec Executor, schema *expression.Schema, partitionBy []*expression.Column, concurrency int, dataSourceSorted bool) Executor {
	src := buildMockDataPhysicalPlan(ctx, srcExec)
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
		desc, _ := aggregation.NewWindowFuncDesc(ctx, windowFunc, args, false)

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
	win.Init(ctx, nil, 0)

	var tail core.PhysicalPlan = win
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

	var plan core.PhysicalPlan
	if concurrency > 1 {
		byItems := make([]expression.Expression, 0, len(win.PartitionBy))
		for _, item := range win.PartitionBy {
			byItems = append(byItems, item.Col)
		}

		plan = core.PhysicalShuffle{
			Concurrency:  concurrency,
			Tails:        []plannercore.PhysicalPlan{tail},
			DataSources:  []plannercore.PhysicalPlan{src},
			SplitterType: core.PartitionHashSplitterType,
			ByItemArrays: [][]expression.Expression{byItems},
		}.Init(ctx, nil, 0)
		plan.SetChildren(win)
	} else {
		plan = win
	}

	b := newExecutorBuilder(ctx, nil, nil, oracle.GlobalTxnScope)
	exec := b.build(plan)
	return exec
}

// windowTestCase has a fixed schema (col Double, partitionBy LongLong, rawData VarString(16), col LongLong).
type windowTestCase struct {
	windowFunc       string
	numFunc          int // The number of windowFuncs. Default: 1.
	frame            *core.WindowFrame
	ndv              int // the number of distinct group-by keys
	rows             int
	concurrency      int
	pipelined        int
	dataSourceSorted bool
	ctx              sessionctx.Context
	rawDataSmall     string
	columns          []*expression.Column // the columns of mock schema
}

func (a windowTestCase) String() string {
	return fmt.Sprintf("(func:%v, aggColType:%s, numFunc:%v, ndv:%v, rows:%v, sorted:%v, concurrency:%v, pipelined:%v)",
		a.windowFunc, a.columns[0].RetType, a.numFunc, a.ndv, a.rows, a.dataSourceSorted, a.concurrency, a.pipelined)
}

func defaultWindowTestCase() *windowTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	return &windowTestCase{ast.WindowFuncRowNumber, 1, nil, 1000, 10000000, 1, 0, true, ctx, strings.Repeat("x", 16),
		[]*expression.Column{
			{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
			{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
			{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
			{Index: 3, RetType: types.NewFieldType(mysql.TypeLonglong)},
		}}
}

func benchmarkWindowExecWithCase(b *testing.B, casTest *windowTestCase) {
	ctx := casTest.ctx
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBWindowConcurrency, fmt.Sprintf("%v", casTest.concurrency)); err != nil {
		b.Fatal(err)
	}
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBEnablePipelinedWindowFunction, fmt.Sprintf("%v", casTest.pipelined)); err != nil {
		b.Fatal(err)
	}

	cols := casTest.columns
	dataSource := buildMockDataSource(mockDataSourceParameters{
		schema: expression.NewSchema(cols...),
		ndvs:   []int{0, casTest.ndv, 0, 0},
		orders: []bool{false, casTest.dataSourceSorted, false, false},
		rows:   casTest.rows,
		ctx:    casTest.ctx,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new window-executor
		childCols := casTest.columns
		schema := expression.NewSchema(childCols...)
		windowExec := buildWindowExecutor(casTest.ctx, casTest.windowFunc, casTest.numFunc, casTest.frame, dataSource, schema, childCols[1:2], casTest.concurrency, casTest.dataSourceSorted)
		tmpCtx := context.Background()
		chk := newFirstChunk(windowExec)
		dataSource.prepareChunks()

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
				cas := defaultWindowTestCase()
				cas.rows = row
				cas.ndv = ndv
				cas.concurrency = con
				cas.dataSourceSorted = false
				cas.windowFunc = ast.WindowFuncRowNumber // cheapest
				cas.pipelined = pipelined
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
			cas := defaultWindowTestCase()
			cas.rows = 100000
			cas.ndv = 1000
			cas.concurrency = con
			cas.dataSourceSorted = false
			cas.windowFunc = windowFunc
			cas.pipelined = pipelined
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
					cas := defaultWindowTestCase()
					cas.rows = 100000
					cas.ndv = 1000
					cas.concurrency = con
					cas.dataSourceSorted = sorted
					cas.windowFunc = windowFunc
					cas.numFunc = numFunc
					if i < len(frames) {
						cas.frame = frames[i]
					}
					cas.pipelined = pipelined
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
	cas := defaultWindowTestCase()
	cas.rows = 10000
	cas.ndv = 10
	cas.concurrency = 1
	cas.dataSourceSorted = false
	cas.windowFunc = windowFunc
	cas.numFunc = 1
	cas.frame = frame
	cas.pipelined = pipelined
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
		cas := defaultWindowTestCase()
		cas.ctx.GetSessionVars().WindowingUseHighPrecision = false
		cas.rows = row
		cas.ndv = ndv
		cas.windowFunc = windowFunc.aggFunc
		cas.frame = frame
		cas.columns[0].RetType.SetType(windowFunc.aggColTypes)
		cas.pipelined = pipelined
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

func prepare4HashJoin(testCase *hashJoinTestCase, innerExec, outerExec Executor) *HashJoinExec {
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

	joinKeys := make([]*expression.Column, 0, len(testCase.keyIdx))
	for _, keyIdx := range testCase.keyIdx {
		joinKeys = append(joinKeys, cols0[keyIdx])
	}
	probeKeys := make([]*expression.Column, 0, len(testCase.keyIdx))
	for _, keyIdx := range testCase.keyIdx {
		probeKeys = append(probeKeys, cols1[keyIdx])
	}
	e := &HashJoinExec{
		baseExecutor:      newBaseExecutor(testCase.ctx, joinSchema, 5, innerExec, outerExec),
		concurrency:       uint(testCase.concurrency),
		joinType:          testCase.joinType, // 0 for InnerJoin, 1 for LeftOutersJoin, 2 for RightOuterJoin
		isOuterJoin:       false,
		buildKeys:         joinKeys,
		probeKeys:         probeKeys,
		buildSideExec:     innerExec,
		probeSideExec:     outerExec,
		buildSideEstCount: float64(testCase.rows),
		useOuterToBuild:   testCase.useOuterToBuild,
	}

	childrenUsedSchema := markChildrenUsedCols(e.Schema(), e.children[0].Schema(), e.children[1].Schema())
	defaultValues := make([]types.Datum, e.buildSideExec.Schema().Len())
	lhsTypes, rhsTypes := retTypes(innerExec), retTypes(outerExec)
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(testCase.ctx, e.joinType, true, defaultValues,
			nil, lhsTypes, rhsTypes, childrenUsedSchema)
	}
	memLimit := int64(-1)
	if testCase.disk {
		memLimit = 1
	}
	t := memory.NewTracker(-1, memLimit)
	t.SetActionOnExceed(nil)
	t2 := disk.NewTracker(-1, -1)
	e.ctx.GetSessionVars().StmtCtx.MemTracker = t
	e.ctx.GetSessionVars().StmtCtx.DiskTracker = t2
	return e
}

func benchmarkHashJoinExecWithCase(b *testing.B, casTest *hashJoinTestCase) {
	opt1 := mockDataSourceParameters{
		rows: casTest.rows,
		ctx:  casTest.ctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
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
	opt1.schema = expression.NewSchema(casTest.columns()...)
	opt2.schema = expression.NewSchema(casTest.columns()...)
	dataSource1 := buildMockDataSource(opt1)
	dataSource2 := buildMockDataSource(opt2)
	// Test spill result.
	benchmarkHashJoinExec(b, casTest, dataSource1, dataSource2, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkHashJoinExec(b, casTest, dataSource1, dataSource2, false)
	}
}

func benchmarkHashJoinExec(b *testing.B, casTest *hashJoinTestCase, opt1, opt2 *mockDataSource, testResult bool) {
	b.StopTimer()
	exec := prepare4HashJoin(casTest, opt1, opt2)
	tmpCtx := context.Background()
	chk := newFirstChunk(exec)
	opt1.prepareChunks()
	opt2.prepareChunks()

	totalRow := 0
	b.StartTimer()
	if err := exec.Open(tmpCtx); err != nil {
		b.Fatal(err)
	}
	for {
		if err := exec.Next(tmpCtx, chk); err != nil {
			b.Fatal(err)
		}
		if chk.NumRows() == 0 {
			break
		}
		totalRow += chk.NumRows()
	}

	if testResult {
		time.Sleep(200 * time.Millisecond)
		if spilled := exec.rowContainer.alreadySpilledSafeForTest(); spilled != casTest.disk {
			b.Fatal("wrong usage with disk:", spilled, casTest.disk)
		}
	}

	if err := exec.Close(); err != nil {
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
	opt := mockDataSourceParameters{
		schema: expression.NewSchema(casTest.columns()...),
		rows:   casTest.rows,
		ctx:    casTest.ctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
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
	dataSource1 := buildMockDataSource(opt)
	dataSource2 := buildMockDataSource(opt)

	dataSource1.prepareChunks()
	benchmarkBuildHashTable(b, casTest, dataSource1, dataSource2, true)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkBuildHashTable(b, casTest, dataSource1, dataSource2, false)
	}
}

func benchmarkBuildHashTable(b *testing.B, casTest *hashJoinTestCase, dataSource1, dataSource2 *mockDataSource, testResult bool) {
	b.StopTimer()
	exec := prepare4HashJoin(casTest, dataSource1, dataSource2)
	tmpCtx := context.Background()
	if err := exec.Open(tmpCtx); err != nil {
		b.Fatal(err)
	}
	exec.prepared = true

	innerResultCh := make(chan *chunk.Chunk, len(dataSource1.chunks))
	for _, chk := range dataSource1.chunks {
		innerResultCh <- chk
	}
	close(innerResultCh)

	b.StartTimer()
	if err := exec.buildHashTableForList(innerResultCh); err != nil {
		b.Fatal(err)
	}

	if testResult {
		time.Sleep(200 * time.Millisecond)
		if exec.rowContainer.alreadySpilledSafeForTest() != casTest.disk {
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

type indexJoinTestCase struct {
	outerRows       int
	innerRows       int
	concurrency     int
	ctx             sessionctx.Context
	outerJoinKeyIdx []int
	innerJoinKeyIdx []int
	outerHashKeyIdx []int
	innerHashKeyIdx []int
	innerIdx        []int
	needOuterSort   bool
	rawData         string
}

func (tc indexJoinTestCase) columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 2, RetType: types.NewFieldType(mysql.TypeVarString)},
	}
}

func defaultIndexJoinTestCase() *indexJoinTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().SnapshotTS = 1
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)
	tc := &indexJoinTestCase{
		outerRows:       100000,
		innerRows:       variable.DefMaxChunkSize * 100,
		concurrency:     4,
		ctx:             ctx,
		outerJoinKeyIdx: []int{0, 1},
		innerJoinKeyIdx: []int{0, 1},
		outerHashKeyIdx: []int{0, 1},
		innerHashKeyIdx: []int{0, 1},
		innerIdx:        []int{0, 1},
		rawData:         wideString,
	}
	return tc
}

func (tc indexJoinTestCase) String() string {
	return fmt.Sprintf("(outerRows:%v, innerRows:%v, concurency:%v, outerJoinKeyIdx: %v, innerJoinKeyIdx: %v, NeedOuterSort:%v)",
		tc.outerRows, tc.innerRows, tc.concurrency, tc.outerJoinKeyIdx, tc.innerJoinKeyIdx, tc.needOuterSort)
}
func (tc indexJoinTestCase) getMockDataSourceOptByRows(rows int) mockDataSourceParameters {
	return mockDataSourceParameters{
		schema: expression.NewSchema(tc.columns()...),
		rows:   rows,
		ctx:    tc.ctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeDouble:
				return float64(row)
			case mysql.TypeVarString:
				return tc.rawData
			default:
				panic("not implement")
			}
		},
	}
}

func prepare4IndexInnerHashJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) (Executor, error) {
	outerCols, innerCols := tc.columns(), tc.columns()
	joinSchema := expression.NewSchema(outerCols...)
	joinSchema.Append(innerCols...)
	leftTypes, rightTypes := retTypes(outerDS), retTypes(innerDS)
	defaultValues := make([]types.Datum, len(innerCols))
	colLens := make([]int, len(innerCols))
	for i := range colLens {
		colLens[i] = types.UnspecifiedLength
	}
	keyOff2IdxOff := make([]int, len(tc.outerJoinKeyIdx))
	for i := range keyOff2IdxOff {
		keyOff2IdxOff[i] = i
	}

	readerBuilder, err := newExecutorBuilder(tc.ctx, nil, nil, oracle.GlobalTxnScope).
		newDataReaderBuilder(&mockPhysicalIndexReader{e: innerDS})
	if err != nil {
		return nil, err
	}

	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, 1, outerDS),
		outerCtx: outerCtx{
			rowTypes: leftTypes,
			keyCols:  tc.outerJoinKeyIdx,
			hashCols: tc.outerHashKeyIdx,
		},
		innerCtx: innerCtx{
			readerBuilder: readerBuilder,
			rowTypes:      rightTypes,
			colLens:       colLens,
			keyCols:       tc.innerJoinKeyIdx,
			hashCols:      tc.innerHashKeyIdx,
		},
		workerWg:      new(sync.WaitGroup),
		joiner:        newJoiner(tc.ctx, 0, false, defaultValues, nil, leftTypes, rightTypes, nil),
		isOuterJoin:   false,
		keyOff2IdxOff: keyOff2IdxOff,
		lastColHelper: nil,
	}
	e.joinResult = newFirstChunk(e)
	return e, nil
}

func prepare4IndexOuterHashJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) (Executor, error) {
	e, err := prepare4IndexInnerHashJoin(tc, outerDS, innerDS)
	if err != nil {
		return nil, err
	}
	idxHash := &IndexNestedLoopHashJoin{IndexLookUpJoin: *e.(*IndexLookUpJoin)}
	concurrency := tc.concurrency
	idxHash.joiners = make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.joiners[i] = e.(*IndexLookUpJoin).joiner.Clone()
	}
	return idxHash, nil
}

func prepare4IndexMergeJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) (Executor, error) {
	outerCols, innerCols := tc.columns(), tc.columns()
	joinSchema := expression.NewSchema(outerCols...)
	joinSchema.Append(innerCols...)
	outerJoinKeys := make([]*expression.Column, 0, len(tc.outerJoinKeyIdx))
	innerJoinKeys := make([]*expression.Column, 0, len(tc.innerJoinKeyIdx))
	for _, keyIdx := range tc.outerJoinKeyIdx {
		outerJoinKeys = append(outerJoinKeys, outerCols[keyIdx])
	}
	for _, keyIdx := range tc.innerJoinKeyIdx {
		innerJoinKeys = append(innerJoinKeys, innerCols[keyIdx])
	}
	leftTypes, rightTypes := retTypes(outerDS), retTypes(innerDS)
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

	readerBuilder, err := newExecutorBuilder(tc.ctx, nil, nil, oracle.GlobalTxnScope).
		newDataReaderBuilder(&mockPhysicalIndexReader{e: innerDS})
	if err != nil {
		return nil, err
	}

	e := &IndexLookUpMergeJoin{
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, 2, outerDS),
		outerMergeCtx: outerMergeCtx{
			rowTypes:      leftTypes,
			keyCols:       tc.outerJoinKeyIdx,
			joinKeys:      outerJoinKeys,
			needOuterSort: tc.needOuterSort,
			compareFuncs:  outerCompareFuncs,
		},
		innerMergeCtx: innerMergeCtx{
			readerBuilder: readerBuilder,
			rowTypes:      rightTypes,
			joinKeys:      innerJoinKeys,
			colLens:       colLens,
			keyCols:       tc.innerJoinKeyIdx,
			compareFuncs:  compareFuncs,
		},
		workerWg:      new(sync.WaitGroup),
		isOuterJoin:   false,
		keyOff2IdxOff: keyOff2IdxOff,
		lastColHelper: nil,
	}
	concurrency := e.ctx.GetSessionVars().IndexLookupJoinConcurrency()
	joiners := make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		joiners[i] = newJoiner(tc.ctx, 0, false, defaultValues, nil, leftTypes, rightTypes, nil)
	}
	e.joiners = joiners
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
	tc *indexJoinTestCase,
	outerDS *mockDataSource,
	innerDS *mockDataSource,
	execType indexJoinType,
) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var exec Executor
		var err error
		switch execType {
		case indexInnerHashJoin:
			exec, err = prepare4IndexInnerHashJoin(tc, outerDS, innerDS)
		case indexOuterHashJoin:
			exec, err = prepare4IndexOuterHashJoin(tc, outerDS, innerDS)
		case indexMergeJoin:
			exec, err = prepare4IndexMergeJoin(tc, outerDS, innerDS)
		}

		if err != nil {
			b.Fatal(err)
		}

		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		outerDS.prepareChunks()
		innerDS.prepareChunks()

		b.StartTimer()
		if err = exec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exec.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := exec.Close(); err != nil {
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
	outerOpt := tc.getMockDataSourceOptByRows(tc.outerRows)
	innerOpt := tc.getMockDataSourceOptByRows(tc.innerRows)
	outerDS := buildMockDataSourceWithIndex(outerOpt, tc.innerIdx)
	innerDS := buildMockDataSourceWithIndex(innerOpt, tc.innerIdx)

	tc.needOuterSort = true
	b.Run(fmt.Sprintf("index merge join need outer sort %v", tc), func(b *testing.B) {
		benchmarkIndexJoinExecWithCase(b, tc, outerDS, innerDS, indexMergeJoin)
	})

	tc.needOuterSort = false
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
	indexJoinTestCase
	childrenUsedSchema [][]bool
}

func prepareMergeJoinExec(tc *mergeJoinTestCase, joinSchema *expression.Schema, leftExec, rightExec Executor, defaultValues []types.Datum,
	compareFuncs []expression.CompareFunc, innerJoinKeys []*expression.Column, outerJoinKeys []*expression.Column) *MergeJoinExec {
	// only benchmark inner join
	mergeJoinExec := &MergeJoinExec{
		stmtCtx:      tc.ctx.GetSessionVars().StmtCtx,
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, 3, leftExec, rightExec),
		compareFuncs: compareFuncs,
		isOuterJoin:  false,
	}

	mergeJoinExec.joiner = newJoiner(
		tc.ctx,
		0,
		false,
		defaultValues,
		nil,
		retTypes(leftExec),
		retTypes(rightExec),
		tc.childrenUsedSchema,
	)

	mergeJoinExec.innerTable = &mergeJoinTable{
		isInner:    true,
		childIndex: 1,
		joinKeys:   innerJoinKeys,
	}

	mergeJoinExec.outerTable = &mergeJoinTable{
		childIndex: 0,
		filters:    nil,
		joinKeys:   outerJoinKeys,
	}

	return mergeJoinExec
}

func prepare4MergeJoin(tc *mergeJoinTestCase, innerDS, outerDS *mockDataSource, sorted bool, concurrency int) Executor {
	outerCols, innerCols := tc.columns(), tc.columns()

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

	outerJoinKeys := make([]*expression.Column, 0, len(tc.outerJoinKeyIdx))
	innerJoinKeys := make([]*expression.Column, 0, len(tc.innerJoinKeyIdx))
	for _, keyIdx := range tc.outerJoinKeyIdx {
		outerJoinKeys = append(outerJoinKeys, outerCols[keyIdx])
	}
	for _, keyIdx := range tc.innerJoinKeyIdx {
		innerJoinKeys = append(innerJoinKeys, innerCols[keyIdx])
	}
	compareFuncs := make([]expression.CompareFunc, 0, len(outerJoinKeys))
	for i := range outerJoinKeys {
		compareFuncs = append(compareFuncs, expression.GetCmpFunction(nil, outerJoinKeys[i], innerJoinKeys[i]))
	}

	defaultValues := make([]types.Datum, len(innerCols))

	var leftExec, rightExec Executor
	if sorted {
		leftSortExec := &SortExec{
			baseExecutor: newBaseExecutor(tc.ctx, innerDS.schema, 3, innerDS),
			ByItems:      make([]*util.ByItems, 0, len(tc.innerJoinKeyIdx)),
			schema:       innerDS.schema,
		}
		for _, key := range innerJoinKeys {
			leftSortExec.ByItems = append(leftSortExec.ByItems, &util.ByItems{Expr: key})
		}
		leftExec = leftSortExec

		rightSortExec := &SortExec{
			baseExecutor: newBaseExecutor(tc.ctx, outerDS.schema, 4, outerDS),
			ByItems:      make([]*util.ByItems, 0, len(tc.outerJoinKeyIdx)),
			schema:       outerDS.schema,
		}
		for _, key := range outerJoinKeys {
			rightSortExec.ByItems = append(rightSortExec.ByItems, &util.ByItems{Expr: key})
		}
		rightExec = rightSortExec
	} else {
		leftExec = innerDS
		rightExec = outerDS
	}

	var e Executor
	if concurrency == 1 {
		e = prepareMergeJoinExec(tc, joinSchema, leftExec, rightExec, defaultValues, compareFuncs, innerJoinKeys, outerJoinKeys)
	} else {
		// build dataSources
		dataSources := []Executor{leftExec, rightExec}
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
			baseExecutor: newBaseExecutor(tc.ctx, joinSchema, 4),
			concurrency:  concurrency,
			dataSources:  dataSources,
			splitters:    splitters,
		}

		// build workers, only benchmark inner join
		shuffle.workers = make([]*shuffleWorker, shuffle.concurrency)
		for i := range shuffle.workers {
			leftReceiver := shuffleReceiver{
				baseExecutor: newBaseExecutor(tc.ctx, leftExec.Schema(), 0),
			}
			rightReceiver := shuffleReceiver{
				baseExecutor: newBaseExecutor(tc.ctx, rightExec.Schema(), 0),
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

func newMergeJoinBenchmark(numOuterRows, numInnerDup, numInnerRedundant int) (tc *mergeJoinTestCase, innerDS, outerDS *mockDataSource) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().SnapshotTS = 1
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	ctx.GetSessionVars().StmtCtx.DiskTracker = disk.NewTracker(-1, -1)

	numInnerRows := numOuterRows*numInnerDup + numInnerRedundant
	itc := &indexJoinTestCase{
		outerRows:       numOuterRows,
		innerRows:       numInnerRows,
		concurrency:     4,
		ctx:             ctx,
		outerJoinKeyIdx: []int{0, 1},
		innerJoinKeyIdx: []int{0, 1},
		outerHashKeyIdx: []int{0, 1},
		innerHashKeyIdx: []int{0, 1},
		innerIdx:        []int{0, 1},
		rawData:         wideString,
	}
	tc = &mergeJoinTestCase{*itc, nil}
	outerOpt := mockDataSourceParameters{
		schema: expression.NewSchema(tc.columns()...),
		rows:   numOuterRows,
		ctx:    tc.ctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeDouble:
				return float64(row)
			case mysql.TypeVarString:
				return tc.rawData
			default:
				panic("not implement")
			}
		},
	}

	innerOpt := mockDataSourceParameters{
		schema: expression.NewSchema(tc.columns()...),
		rows:   numInnerRows,
		ctx:    tc.ctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			row = row / numInnerDup
			switch typ.GetType() {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeDouble:
				return float64(row)
			case mysql.TypeVarString:
				return tc.rawData
			default:
				panic("not implement")
			}
		},
	}

	innerDS = buildMockDataSource(innerOpt)
	outerDS = buildMockDataSource(outerOpt)

	return
}

type mergeJoinType int8

const (
	innerMergeJoin mergeJoinType = iota
)

func benchmarkMergeJoinExecWithCase(b *testing.B, tc *mergeJoinTestCase, innerDS, outerDS *mockDataSource, joinType mergeJoinType) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var exec Executor
		switch joinType {
		case innerMergeJoin:
			exec = prepare4MergeJoin(tc, innerDS, outerDS, true, 2)
		}

		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		outerDS.prepareChunks()
		innerDS.prepareChunks()

		b.StartTimer()
		if err := exec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exec.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := exec.Close(); err != nil {
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

type sortCase struct {
	rows       int
	orderByIdx []int
	ndvs       []int
	ctx        sessionctx.Context
}

func (tc sortCase) columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

func (tc sortCase) String() string {
	return fmt.Sprintf("(rows:%v, orderBy:%v, ndvs: %v)", tc.rows, tc.orderByIdx, tc.ndvs)
}

func defaultSortTestCase() *sortCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	tc := &sortCase{rows: 300000, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	return tc
}

func benchmarkSortExec(b *testing.B, cas *sortCase) {
	opt := mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
		ndvs:   cas.ndvs,
	}
	dataSource := buildMockDataSource(opt)
	exec := &SortExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, 4, dataSource),
		ByItems:      make([]*util.ByItems, 0, len(cas.orderByIdx)),
		schema:       dataSource.schema,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &util.ByItems{Expr: cas.columns()[idx]})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		dataSource.prepareChunks()

		b.StartTimer()
		if err := exec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exec.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := exec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkSortExec(b *testing.B) {
	b.ReportAllocs()
	cas := defaultSortTestCase()
	// all random data
	cas.ndvs = []int{0, 0}
	cas.orderByIdx = []int{0, 1}
	b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
		benchmarkSortExec(b, cas)
	})

	ndvs := []int{1, 10000}
	for _, ndv := range ndvs {
		cas.ndvs = []int{ndv, 0}
		cas.orderByIdx = []int{0, 1}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})

		cas.ndvs = []int{ndv, 0}
		cas.orderByIdx = []int{0}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})

		cas.ndvs = []int{ndv, 0}
		cas.orderByIdx = []int{1}
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkSortExec(b, cas)
		})
	}
}

type limitCase struct {
	rows                  int
	offset                int
	count                 int
	childUsedSchema       []bool
	usingInlineProjection bool
	ctx                   sessionctx.Context
}

func (tc limitCase) columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

func (tc limitCase) String() string {
	return fmt.Sprintf("(rows:%v, offset:%v, count:%v, inline_projection:%v)",
		tc.rows, tc.offset, tc.count, tc.usingInlineProjection)
}

func defaultLimitTestCase() *limitCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	tc := &limitCase{
		rows:                  30000,
		offset:                10000,
		count:                 10000,
		childUsedSchema:       []bool{false, true},
		usingInlineProjection: false,
		ctx:                   ctx,
	}
	return tc
}

func benchmarkLimitExec(b *testing.B, cas *limitCase) {
	opt := mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
	}
	dataSource := buildMockDataSource(opt)
	var exec Executor
	limit := &LimitExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, 4, dataSource),
		begin:        uint64(cas.offset),
		end:          uint64(cas.offset + cas.count),
	}
	if cas.usingInlineProjection {
		if len(cas.childUsedSchema) > 0 {
			limit.columnIdxsUsedByChild = make([]int, 0, len(cas.childUsedSchema))
			for i, used := range cas.childUsedSchema {
				if used {
					limit.columnIdxsUsedByChild = append(limit.columnIdxsUsedByChild, i)
				}
			}
		}
		exec = limit
	} else {
		columns := cas.columns()
		usedCols := make([]*expression.Column, 0, len(columns))
		exprs := make([]expression.Expression, 0, len(columns))
		for i, used := range cas.childUsedSchema {
			if used {
				usedCols = append(usedCols, columns[i])
				exprs = append(exprs, columns[i])
			}
		}
		proj := &ProjectionExec{
			baseExecutor:  newBaseExecutor(cas.ctx, expression.NewSchema(usedCols...), 0, limit),
			numWorkers:    1,
			evaluatorSuit: expression.NewEvaluatorSuite(exprs, false),
		}
		exec = proj
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		dataSource.prepareChunks()

		b.StartTimer()
		if err := exec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := exec.Next(tmpCtx, chk); err != nil {
				b.Fatal(err)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		if err := exec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
	}
}

func BenchmarkLimitExec(b *testing.B) {
	b.ReportAllocs()
	cas := defaultLimitTestCase()
	usingInlineProjection := []bool{false, true}
	for _, inlineProjection := range usingInlineProjection {
		cas.usingInlineProjection = inlineProjection
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
				aggMap := make(aggPartialResultMapper)
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
