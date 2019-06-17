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
	"testing"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

var (
	_ Executor = &mockDataSource{}
)

type mockDataSourceParameters struct {
	schema *expression.Schema
	ndvs   []int  // number of distinct values on columns[i] and zero represents no limit
	orders []bool // columns[i] should be ordered if orders[i] is true
	rows   int    // number of rows the DataSource should output
	ctx    sessionctx.Context
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
	order := mds.p.orders[col]
	rows := mds.p.rows
	NDV := mds.p.ndvs[col]
	results = make([]interface{}, 0, rows)
	if NDV == 0 {
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

func (mds *mockDataSource) Next(ctx context.Context, req *chunk.RecordBatch) error {
	if mds.chunkPtr >= len(mds.chunks) {
		req.Reset()
		return nil
	}
	dataChk := mds.chunks[mds.chunkPtr]
	dataChk.SwapColumns(req.Chunk)
	mds.chunkPtr++
	return nil
}

func buildMockDataSource(opt mockDataSourceParameters) *mockDataSource {
	baseExec := newBaseExecutor(opt.ctx, opt.schema, nil)
	m := &mockDataSource{baseExec, opt, nil, nil, 0}
	types := m.retTypes()
	colData := make([][]interface{}, len(types))
	for i := 0; i < len(types); i++ {
		colData[i] = m.genColDatums(i)
	}

	m.genData = make([]*chunk.Chunk, (m.p.rows+m.initCap-1)/m.initCap)
	for i := range m.genData {
		m.genData[i] = chunk.NewChunkWithCapacity(m.retTypes(), m.ctx.GetSessionVars().MaxChunkSize)
	}

	for i := 0; i < m.p.rows; i++ {
		idx := i / m.maxChunkSize
		retTypes := m.retTypes()
		for colIdx := 0; colIdx < len(types); colIdx++ {
			switch retTypes[colIdx].Tp {
			case mysql.TypeLong, mysql.TypeLonglong:
				m.genData[idx].AppendInt64(colIdx, colData[colIdx][i].(int64))
			case mysql.TypeDouble:
				m.genData[idx].AppendFloat64(colIdx, colData[colIdx][i].(float64))
			default:
				panic("not implement")
			}
		}
	}
	return m
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
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)}}
}

func (a aggTestCase) String() string {
	return fmt.Sprintf("(execType:%v, aggFunc:%v, groupByNDV:%v, hasDistinct:%v, rows:%v, concruuency:%v)",
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
	plan.Init(ctx, nil)
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
	plan.Init(ctx, nil)
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
	aggFunc := aggregation.NewAggFuncDesc(testCase.ctx, testCase.aggFunc, []expression.Expression{childCols[0]}, testCase.hasDistinct)
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
		chk := aggExec.newFirstChunk()
		dataSource.prepareChunks()

		b.StartTimer()
		if err := aggExec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		batch := chunk.NewRecordBatch(chk)
		for {
			if err := aggExec.Next(tmpCtx, batch); err != nil {
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
