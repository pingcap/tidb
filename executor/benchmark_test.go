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
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
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
	schema *expression.Schema // schema
	ndvs   []int              // number of distinct values on columns[i] and zero represents no limit
	orders []bool             // columns[i] should be ordered if orders[i] is true
	rows   int                // number of rows the DataSource should output
	ctx    sessionctx.Context
}

type mockDataSource struct {
	baseExecutor
	p        mockDataSourceParameters
	colData  [][]interface{}
	chunks   []*chunk.Chunk
	chunkPtr int
}

func (mds *mockDataSource) genColDatums(typ *types.FieldType, order bool, rows, NDV int) (results []interface{}) {
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
	case mysql.TypeLonglong:
		return int64(rand.Int())
	case mysql.TypeLong:
		return int32(rand.Int())
	case mysql.TypeDouble:
		return rand.Float64()
	default:
		panic("not implement")
	}
}

func (mds *mockDataSource) prepareChunks() {
	mds.chunks = make([]*chunk.Chunk, (mds.p.rows+mds.initCap-1)/mds.initCap)
	for i := range mds.chunks {
		mds.chunks[i] = mds.newFirstChunk()
	}

	for i := 0; i < mds.p.rows; i++ {
		idx := i / mds.initCap
		types := mds.retTypes()
		for colIdx := 0; colIdx < len(types); colIdx++ {
			switch types[colIdx].Tp {
			case mysql.TypeLong:
				mds.chunks[idx].AppendInt64(colIdx, int64(mds.colData[colIdx][i].(int32)))
			case mysql.TypeLonglong:
				mds.chunks[idx].AppendInt64(colIdx, mds.colData[colIdx][i].(int64))
			case mysql.TypeDouble:
				mds.chunks[idx].AppendFloat64(colIdx, mds.colData[colIdx][i].(float64))
			default:
				panic("not implement")
			}
		}
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
	baseExec := newBaseExecutor(opt.ctx, opt.schema, "")
	m := &mockDataSource{baseExec, opt, nil, nil, 0}
	types := m.retTypes()
	m.colData = make([][]interface{}, len(types))
	for i := 0; i < len(types); i++ {
		m.colData[i] = m.genColDatums(types[i], m.p.orders[i], m.p.rows, m.p.ndvs[i])
	}
	return m
}

type aggExecutorParameters struct {
	ctx          sessionctx.Context
	schema       *expression.Schema
	child        Executor
	aggFuncs     []*aggregation.AggFuncDesc
	groupByItems []expression.Expression
	concurrency  int
}

func buildHashAggExecutor(v *aggExecutorParameters) Executor {
	sessionVars := v.ctx.GetSessionVars()
	e := &HashAggExec{
		baseExecutor:    newBaseExecutor(v.ctx, v.schema, "", v.child),
		sc:              sessionVars.StmtCtx,
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(v.aggFuncs)),
		GroupByItems:    v.groupByItems,
	}
	if len(v.groupByItems) != 0 || aggregation.IsAllFirstRow(v.aggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(e.retTypes(), 1)
	}
	for _, aggDesc := range v.aggFuncs {
		if aggDesc.HasDistinct {
			e.isUnparallelExec = true
		}
	}
	if finalCon, partialCon := sessionVars.HashAggFinalConcurrency, sessionVars.HashAggPartialConcurrency; finalCon <= 0 || partialCon <= 0 || finalCon == 1 && partialCon == 1 {
		e.isUnparallelExec = true
	}
	partialOrdinal := 0
	for i, aggDesc := range v.aggFuncs {
		if e.isUnparallelExec {
			e.PartialAggFuncs = append(e.PartialAggFuncs, aggfuncs.Build(v.ctx, aggDesc, i))
		} else {
			ordinal := []int{partialOrdinal}
			partialOrdinal++
			if aggDesc.Name == ast.AggFuncAvg {
				ordinal = append(ordinal, partialOrdinal+1)
				partialOrdinal++
			}
			partialAggDesc, finalDesc := aggDesc.Split(ordinal)
			partialAggFunc := aggfuncs.Build(v.ctx, partialAggDesc, i)
			finalAggFunc := aggfuncs.Build(v.ctx, finalDesc, i)
			e.PartialAggFuncs = append(e.PartialAggFuncs, partialAggFunc)
			e.FinalAggFuncs = append(e.FinalAggFuncs, finalAggFunc)
			if partialAggDesc.Name == ast.AggFuncGroupConcat {
				finalAggFunc.(interface{ SetTruncated(t *int32) }).SetTruncated(
					partialAggFunc.(interface{ GetTruncated() *int32 }).GetTruncated(),
				)
			}
		}
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}
	return e
}

func buildStreamAggExecutor(v *aggExecutorParameters) Executor {
	e := &StreamAggExec{
		baseExecutor: newBaseExecutor(v.ctx, v.schema, "", v.child),
		groupChecker: newGroupChecker(v.ctx.GetSessionVars().StmtCtx, v.groupByItems),
		aggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.aggFuncs)),
	}
	if len(v.groupByItems) != 0 || aggregation.IsAllFirstRow(v.aggFuncs) {
		e.defaultVal = nil
	} else {
		e.defaultVal = chunk.NewChunkWithCapacity(e.retTypes(), 1)
	}
	for i, aggDesc := range v.aggFuncs {
		aggFunc := aggfuncs.Build(v.ctx, aggDesc, i)
		e.aggFuncs = append(e.aggFuncs, aggFunc)
		if e.defaultVal != nil {
			value := aggDesc.GetDefaultValue()
			e.defaultVal.AppendDatum(i, &value)
		}
	}

	return e
}

type aggTestCase struct {
	// The test table's schema is fixed (aggCol Double, groupBy Long).
	exec        string // "hash" or "stream"
	aggFunc     string // sum, avg, count ....
	groupByNDV  int    // the number of distinct group-by keys
	hasDistinct bool
	rows        int
	concurrency int
	ctx         sessionctx.Context
}

func (a aggTestCase) String() string {
	return fmt.Sprintf("(%v|%v|%v|%v|%v|%v)",
		a.exec, a.aggFunc, a.hasDistinct, a.rows, a.groupByNDV, a.concurrency)
}

func defaultAggTestCase(exec string) *aggTestCase {
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = 1024
	ctx.GetSessionVars().MaxChunkSize = 1024
	return &aggTestCase{exec, ast.AggFuncSum, 1000, false, 10000000, 4, ctx}
}

func buildAggDataSource(b *testing.B, cas *aggTestCase) *mockDataSource {
	cols := []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}
	orders := []bool{false, cas.exec == "stream"}
	child := buildMockDataSource(mockDataSourceParameters{
		schema: expression.NewSchema(cols...),
		ndvs:   []int{0, cas.groupByNDV},
		orders: orders,
		rows:   cas.rows,
		ctx:    cas.ctx,
	})
	return child
}

func buildAggExecutor(b *testing.B, cas *aggTestCase, child Executor) Executor {
	childCols := []*expression.Column{
		{Index: 0, RetType: types.NewFieldType(mysql.TypeDouble)},
		{Index: 1, RetType: types.NewFieldType(mysql.TypeLonglong)},
	}

	ctx := cas.ctx
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggFinalConcurrency, fmt.Sprintf("%v", cas.concurrency)); err != nil {
		b.Fatal(err)
	}
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggPartialConcurrency, fmt.Sprintf("%v", cas.concurrency)); err != nil {
		b.Fatal(err)
	}

	aggFunc := aggregation.NewAggFuncDesc(ctx, cas.aggFunc, []expression.Expression{childCols[0]}, cas.hasDistinct)
	p := &aggExecutorParameters{
		ctx:          ctx,
		schema:       expression.NewSchema(childCols...),
		child:        child,
		aggFuncs:     []*aggregation.AggFuncDesc{aggFunc},
		groupByItems: []expression.Expression{childCols[1]},
	}

	var agg Executor
	switch cas.exec {
	case "hash":
		agg = buildHashAggExecutor(p)
	case "stream":
		agg = buildStreamAggExecutor(p)
	default:
		b.Fatal("not implement")
	}

	return agg
}

func benchmarkAggExecWithCase(b *testing.B, cas *aggTestCase) {
	dataSource := buildAggDataSource(b, cas)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new agg-executor
		aggExec := buildAggExecutor(b, cas, dataSource)
		tmpCtx := context.Background()
		chk := aggExec.newFirstChunk()
		dataSource.prepareChunks()

		b.StartTimer()
		if err := aggExec.Open(tmpCtx); err != nil {
			b.Fatal(err)
		}
		for {
			if err := aggExec.Next(tmpCtx, chunk.NewRecordBatch(chk)); err != nil {
				b.Fatal(b)
			}
			if chk.NumRows() == 0 {
				break
			}
		}

		b.StopTimer()
		if err := aggExec.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggRows(b *testing.B) {
	rows := []int{100000, 1000000, 10000000}
	concs := []int{1, 4, 8, 15, 20, 30, 40}
	for _, row := range rows {
		for _, con := range concs {
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
