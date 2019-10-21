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
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/planner/property"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/stringutil"
	"go.uber.org/zap/zapcore"
)

var (
	_ Executor = &mockDataSource{}
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
	if mds.p.genDataFunc != nil {
		for i := 0; i < rows; i++ {
			results = append(results, mds.p.genDataFunc(i, typ))
		}
	} else if NDV == 0 {
		for i := 0; i < rows; i++ {
			results = append(results, mds.randDatum(typ))
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
			switch typ.Tp {
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
	switch typ.Tp {
	case mysql.TypeLong, mysql.TypeLonglong:
		return int64(rand.Int())
	case mysql.TypeDouble:
		return rand.Float64()
	case mysql.TypeVarString:
		return rawData
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
	baseExec := newBaseExecutor(opt.ctx, opt.schema, nil)
	m := &mockDataSource{baseExec, opt, nil, nil, 0}
	types := retTypes(m)
	colData := make([][]interface{}, len(types))
	for i := 0; i < len(types); i++ {
		colData[i] = m.genColDatums(i)
	}

	m.genData = make([]*chunk.Chunk, (m.p.rows+m.maxChunkSize-1)/m.maxChunkSize)
	for i := range m.genData {
		m.genData[i] = chunk.NewChunkWithCapacity(retTypes(m), m.maxChunkSize)
	}

	for i := 0; i < m.p.rows; i++ {
		idx := i / m.maxChunkSize
		retTypes := retTypes(m)
		for colIdx := 0; colIdx < len(types); colIdx++ {
			switch retTypes[colIdx].Tp {
			case mysql.TypeLong, mysql.TypeLonglong:
				m.genData[idx].AppendInt64(colIdx, colData[colIdx][i].(int64))
			case mysql.TypeDouble:
				m.genData[idx].AppendFloat64(colIdx, colData[colIdx][i].(float64))
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

type aggTestCase struct {
	// The test table's schema is fixed (aggCol Double, groupBy LongLong).
	execType    string // "hash" or "stream"
	aggFunc     string // sum, avg, count ....
	groupByNDV  int    // the number of distinct group-by keys
	hasDistinct bool
	rows        int
	concurrency int
	ctx         sessionctx.Context
}

func (a aggTestCase) columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

func (a aggTestCase) String() string {
	return fmt.Sprintf("(execType:%v, aggFunc:%v, ndv:%v, hasDistinct:%v, rows:%v, concurrency:%v)",
		a.execType, a.aggFunc, a.groupByNDV, a.hasDistinct, a.rows, a.concurrency)
}

func defaultAggTestCase(exec string) *aggTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	return &aggTestCase{exec, ast.AggFuncSum, 1000, false, 10000000, 4, ctx}
}

func buildHashAggExecutor(ctx sessionctx.Context, src Executor, schema *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression) Executor {
	plan := new(core.PhysicalHashAgg)
	plan.AggFuncs = aggFuncs
	plan.GroupByItems = groupItems
	plan.SetSchema(schema)
	plan.Init(ctx, nil, 0)
	plan.SetChildren(nil)
	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	hashAgg := exec.(*HashAggExec)
	hashAgg.children[0] = src
	return exec
}

func buildStreamAggExecutor(ctx sessionctx.Context, src Executor, schema *expression.Schema,
	aggFuncs []*aggregation.AggFuncDesc, groupItems []expression.Expression) Executor {
	plan := new(core.PhysicalStreamAgg)
	plan.AggFuncs = aggFuncs
	plan.GroupByItems = groupItems
	plan.SetSchema(schema)
	plan.Init(ctx, nil, 0)
	plan.SetChildren(nil)
	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	streamAgg := exec.(*StreamAggExec)
	streamAgg.children[0] = src
	return exec
}

func buildAggExecutor(b *testing.B, testCase *aggTestCase, child Executor) Executor {
	ctx := testCase.ctx
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggFinalConcurrency, fmt.Sprintf("%v", testCase.concurrency)); err != nil {
		b.Fatal(err)
	}
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggPartialConcurrency, fmt.Sprintf("%v", testCase.concurrency)); err != nil {
		b.Fatal(err)
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
		aggExec = buildStreamAggExecutor(testCase.ctx, child, schema, aggFuncs, groupBy)
	default:
		b.Fatal("not implement")
	}
	return aggExec
}

func benchmarkAggExecWithCase(b *testing.B, casTest *aggTestCase) {
	cols := casTest.columns()
	orders := []bool{false, casTest.execType == "stream"}
	dataSource := buildMockDataSource(mockDataSourceParameters{
		schema: expression.NewSchema(cols...),
		ndvs:   []int{0, casTest.groupByNDV},
		orders: orders,
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

func BenchmarkAggRows(b *testing.B) {
	rows := []int{100000, 1000000, 10000000}
	concurrencies := []int{1, 4, 8, 15, 20, 30, 40}
	for _, row := range rows {
		for _, con := range concurrencies {
			for _, exec := range []string{"hash", "stream"} {
				if exec == "stream" && con > 1 {
					continue
				}
				cas := defaultAggTestCase(exec)
				cas.rows = row
				cas.concurrency = con
				b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
					benchmarkAggExecWithCase(b, cas)
				})
			}
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
			if exec == "stream" && con > 1 {
				continue
			}
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

func buildWindowExecutor(ctx sessionctx.Context, windowFunc string, src Executor, schema *expression.Schema, partitionBy []*expression.Column) Executor {
	plan := new(core.PhysicalWindow)

	var args []expression.Expression
	switch windowFunc {
	case ast.WindowFuncNtile:
		args = append(args, &expression.Constant{Value: types.NewUintDatum(2)})
	case ast.WindowFuncNthValue:
		args = append(args, partitionBy[0], &expression.Constant{Value: types.NewUintDatum(2)})
	default:
		args = append(args, partitionBy[0])
	}
	desc, _ := aggregation.NewWindowFuncDesc(ctx, windowFunc, args)
	plan.WindowFuncDescs = []*aggregation.WindowFuncDesc{desc}
	for _, col := range partitionBy {
		plan.PartitionBy = append(plan.PartitionBy, property.Item{Col: col})
	}
	plan.OrderBy = nil
	plan.SetSchema(schema)
	plan.Init(ctx, nil, 0)
	plan.SetChildren(nil)
	b := newExecutorBuilder(ctx, nil)
	exec := b.build(plan)
	window := exec.(*WindowExec)
	window.children[0] = src
	return exec
}

type windowTestCase struct {
	// The test table's schema is fixed (col Double, partitionBy LongLong, rawData VarString(5128), col LongLong).
	windowFunc string
	ndv        int // the number of distinct group-by keys
	rows       int
	ctx        sessionctx.Context
}

var rawData = strings.Repeat("x", 5*1024)

func (a windowTestCase) columns() []*expression.Column {
	rawDataTp := new(types.FieldType)
	types.DefaultTypeForValue(rawData, rawDataTp)
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 2, RetType: rawDataTp},
		{Index: 3, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
}

func (a windowTestCase) String() string {
	return fmt.Sprintf("(func:%v, ndv:%v, rows:%v)",
		a.windowFunc, a.ndv, a.rows)
}

func defaultWindowTestCase() *windowTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	return &windowTestCase{ast.WindowFuncRowNumber, 1000, 10000000, ctx}
}

func benchmarkWindowExecWithCase(b *testing.B, casTest *windowTestCase) {
	cols := casTest.columns()
	dataSource := buildMockDataSource(mockDataSourceParameters{
		schema: expression.NewSchema(cols...),
		ndvs:   []int{0, casTest.ndv, 0, 0},
		orders: []bool{false, true, false, false},
		rows:   casTest.rows,
		ctx:    casTest.ctx,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new window-executor
		childCols := casTest.columns()
		schema := expression.NewSchema(childCols...)
		windowExec := buildWindowExecutor(casTest.ctx, casTest.windowFunc, dataSource, schema, childCols[1:2])
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

func BenchmarkWindowRows(b *testing.B) {
	b.ReportAllocs()
	rows := []int{1000, 100000}
	ndvs := []int{10, 1000}
	for _, row := range rows {
		for _, ndv := range ndvs {
			cas := defaultWindowTestCase()
			cas.rows = row
			cas.ndv = ndv
			cas.windowFunc = ast.WindowFuncRowNumber // cheapest
			b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
				benchmarkWindowExecWithCase(b, cas)
			})
		}
	}
}

func BenchmarkWindowFunctions(b *testing.B) {
	b.ReportAllocs()
	windowFuncs := []string{
		ast.WindowFuncRowNumber,
		ast.WindowFuncRank,
		ast.WindowFuncDenseRank,
		ast.WindowFuncCumeDist,
		ast.WindowFuncPercentRank,
		ast.WindowFuncNtile,
		ast.WindowFuncLead,
		ast.WindowFuncLag,
		ast.WindowFuncFirstValue,
		ast.WindowFuncLastValue,
		ast.WindowFuncNthValue,
	}
	for _, windowFunc := range windowFuncs {
		cas := defaultWindowTestCase()
		cas.rows = 100000
		cas.ndv = 1000
		cas.windowFunc = windowFunc
		b.Run(fmt.Sprintf("%v", cas), func(b *testing.B) {
			benchmarkWindowExecWithCase(b, cas)
		})
	}
}

type hashJoinTestCase struct {
	rows        int
	concurrency int
	ctx         sessionctx.Context
	keyIdx      []int
	disk        bool
}

func (tc hashJoinTestCase) columns() []*expression.Column {
	return []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeVarString)},
	}
}

func (tc hashJoinTestCase) String() string {
	return fmt.Sprintf("(rows:%v, concurency:%v, joinKeyIdx: %v, disk:%v)",
		tc.rows, tc.concurrency, tc.keyIdx, tc.disk)
}

func defaultHashJoinTestCase() *hashJoinTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefInitChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(nil, -1)
	ctx.GetSessionVars().IndexLookupJoinConcurrency = 4
	tc := &hashJoinTestCase{rows: 100000, concurrency: 4, ctx: ctx, keyIdx: []int{0, 1}}
	return tc
}

func prepare4HashJoin(testCase *hashJoinTestCase, innerExec, outerExec Executor) *HashJoinExec {
	cols0 := testCase.columns()
	cols1 := testCase.columns()
	joinSchema := expression.NewSchema(cols0...)
	joinSchema.Append(cols1...)
	joinKeys := make([]*expression.Column, 0, len(testCase.keyIdx))
	for _, keyIdx := range testCase.keyIdx {
		joinKeys = append(joinKeys, cols0[keyIdx])
	}
	e := &HashJoinExec{
		baseExecutor:  newBaseExecutor(testCase.ctx, joinSchema, stringutil.StringerStr("HashJoin"), innerExec, outerExec),
		concurrency:   uint(testCase.concurrency),
		joinType:      0, // InnerJoin
		isOuterJoin:   false,
		innerKeys:     joinKeys,
		outerKeys:     joinKeys,
		innerExec:     innerExec,
		outerExec:     outerExec,
		innerEstCount: float64(testCase.rows),
	}
	defaultValues := make([]types.Datum, e.innerExec.Schema().Len())
	lhsTypes, rhsTypes := retTypes(innerExec), retTypes(outerExec)
	e.joiners = make([]joiner, e.concurrency)
	for i := uint(0); i < e.concurrency; i++ {
		e.joiners[i] = newJoiner(testCase.ctx, e.joinType, true, defaultValues,
			nil, lhsTypes, rhsTypes)
	}
	memLimit := int64(-1)
	if testCase.disk {
		memLimit = 1
	}
	t := memory.NewTracker(stringutil.StringerStr("root of prepare4HashJoin"), memLimit)
	t.SetActionOnExceed(nil)
	e.ctx.GetSessionVars().StmtCtx.MemTracker = t
	return e
}

func benchmarkHashJoinExecWithCase(b *testing.B, casTest *hashJoinTestCase) {
	opt := mockDataSourceParameters{
		schema: expression.NewSchema(casTest.columns()...),
		rows:   casTest.rows,
		ctx:    casTest.ctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			switch typ.Tp {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeVarString:
				return rawData
			default:
				panic("not implement")
			}
		},
	}
	dataSource1 := buildMockDataSource(opt)
	dataSource2 := buildMockDataSource(opt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		exec := prepare4HashJoin(casTest, dataSource1, dataSource2)
		tmpCtx := context.Background()
		chk := newFirstChunk(exec)
		dataSource1.prepareChunks()
		dataSource2.prepareChunks()

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
		if exec.rowContainer.alreadySpilled() != casTest.disk {
			b.Fatal("wrong usage with disk")
		}
	}
}

func BenchmarkHashJoinExec(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	b.ReportAllocs()
	cas := defaultHashJoinTestCase()
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

	cas.keyIdx = []int{0}
	cas.disk = true
	cas.rows = 1000
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
			switch typ.Tp {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeVarString:
				return rawData
			default:
				panic("not implement")
			}
		},
	}
	dataSource1 := buildMockDataSource(opt)
	dataSource2 := buildMockDataSource(opt)

	dataSource1.prepareChunks()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
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

		if err := exec.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if exec.rowContainer.alreadySpilled() != casTest.disk {
			b.Fatal("wrong usage with disk")
		}
	}
}

func BenchmarkBuildHashTableForList(b *testing.B) {
	lvl := log.GetLevel()
	log.SetLevel(zapcore.ErrorLevel)
	defer log.SetLevel(lvl)

	b.ReportAllocs()
	cas := defaultHashJoinTestCase()
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
	innerIdx        []int
	needOuterSort   bool
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
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(nil, -1)
	tc := &indexJoinTestCase{
		outerRows:       100000,
		innerRows:       variable.DefMaxChunkSize * 100,
		concurrency:     4,
		ctx:             ctx,
		outerJoinKeyIdx: []int{0, 1},
		innerJoinKeyIdx: []int{0, 1},
		innerIdx:        []int{0, 1},
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
			switch typ.Tp {
			case mysql.TypeLong, mysql.TypeLonglong:
				return int64(row)
			case mysql.TypeDouble:
				return float64(row)
			case mysql.TypeVarString:
				return rawData
			default:
				panic("not implement")
			}
		},
	}
}

func prepare4IndexInnerHashJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) Executor {
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
	e := &IndexLookUpJoin{
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, stringutil.StringerStr("IndexInnerHashJoin"), outerDS),
		outerCtx: outerCtx{
			rowTypes: leftTypes,
			keyCols:  tc.outerJoinKeyIdx,
		},
		innerCtx: innerCtx{
			readerBuilder: &dataReaderBuilder{Plan: &mockPhysicalIndexReader{e: innerDS}, executorBuilder: newExecutorBuilder(tc.ctx, nil)},
			rowTypes:      rightTypes,
			colLens:       colLens,
			keyCols:       tc.innerJoinKeyIdx,
		},
		workerWg:      new(sync.WaitGroup),
		joiner:        newJoiner(tc.ctx, 0, false, defaultValues, nil, leftTypes, rightTypes),
		isOuterJoin:   false,
		keyOff2IdxOff: keyOff2IdxOff,
		lastColHelper: nil,
	}
	e.joinResult = newFirstChunk(e)
	return e
}

func prepare4IndexOuterHashJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) Executor {
	e := prepare4IndexInnerHashJoin(tc, outerDS, innerDS).(*IndexLookUpJoin)
	idxHash := &IndexNestedLoopHashJoin{IndexLookUpJoin: *e}
	concurrency := tc.concurrency
	idxHash.joiners = make([]joiner, concurrency)
	for i := 0; i < concurrency; i++ {
		idxHash.joiners[i] = e.joiner.Clone()
	}
	return idxHash
}

func prepare4IndexMergeJoin(tc *indexJoinTestCase, outerDS *mockDataSource, innerDS *mockDataSource) Executor {
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
		compareFuncs = append(compareFuncs, expression.GetCmpFunction(outerJoinKeys[i], innerJoinKeys[i]))
		outerCompareFuncs = append(outerCompareFuncs, expression.GetCmpFunction(outerJoinKeys[i], outerJoinKeys[i]))
	}
	e := &IndexLookUpMergeJoin{
		baseExecutor: newBaseExecutor(tc.ctx, joinSchema, stringutil.StringerStr("IndexMergeJoin"), outerDS),
		outerMergeCtx: outerMergeCtx{
			rowTypes:      leftTypes,
			keyCols:       tc.outerJoinKeyIdx,
			joinKeys:      outerJoinKeys,
			needOuterSort: tc.needOuterSort,
			compareFuncs:  outerCompareFuncs,
		},
		innerMergeCtx: innerMergeCtx{
			readerBuilder: &dataReaderBuilder{Plan: &mockPhysicalIndexReader{e: innerDS}, executorBuilder: newExecutorBuilder(tc.ctx, nil)},
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
	joiners := make([]joiner, e.ctx.GetSessionVars().IndexLookupJoinConcurrency)
	for i := 0; i < e.ctx.GetSessionVars().IndexLookupJoinConcurrency; i++ {
		joiners[i] = newJoiner(tc.ctx, 0, false, defaultValues, nil, leftTypes, rightTypes)
	}
	e.joiners = joiners
	return e
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
		switch execType {
		case indexInnerHashJoin:
			exec = prepare4IndexInnerHashJoin(tc, outerDS, innerDS)
		case indexOuterHashJoin:
			exec = prepare4IndexOuterHashJoin(tc, outerDS, innerDS)
		case indexMergeJoin:
			exec = prepare4IndexMergeJoin(tc, outerDS, innerDS)
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
