package executor

import (
	"context"
	"fmt"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"math/rand"
	"sort"
	"testing"
)

type mockDataSourceParameters struct {
	types     []*types.FieldType // type of columns
	NDVs      []int              // number of distinct values and zero is no limit
	orders    []bool
	rows      int
	chunkSize int
	ctx       *stmtctx.StatementContext
}

type mockDataSrouce struct {
	p           mockDataSourceParameters
	orgChunks   []*chunk.Chunk
	toUseChunks []*chunk.Chunk
	chunkPtr    int
}

func (mds *mockDataSrouce) genData() {
	colDatums := make([][]types.Datum, len(mds.p.types))
	for i := 0; i < len(mds.p.types); i++ {
		colDatums[i] = mds.genColDatums(mds.p.types[i], mds.p.orders[i], mds.p.rows, mds.p.NDVs[i])
	}
	mds.orgChunks = make([]*chunk.Chunk, (mds.p.rows+mds.p.chunkSize-1)/mds.p.chunkSize)
	for i := range mds.orgChunks {
		mds.orgChunks[i] = mds.newFirstChunk()
	}

	for i := 0; i < mds.p.rows; i++ {
		row := make([]types.Datum, len(mds.p.types))
		for colIdx := 0; colIdx < len(mds.p.types); colIdx++ {
			row[colIdx] = colDatums[colIdx][i]
		}

		idx := i / mds.p.chunkSize
		mds.orgChunks[idx].AppendRow(chunk.MutRowFromDatums(row).ToRow())
	}
}

func (mds *mockDataSrouce) genColDatums(typ *types.FieldType, order bool, n, NDV int) (results []types.Datum) {
	defer func() {
		if order {
			sort.Slice(results, func(i, j int) bool {
				cmp, _ := results[i].CompareDatum(mds.p.ctx, &results[j])
				return cmp < 0
			})
		}
	}()

	results = make([]types.Datum, 0, n)
	if NDV == 0 {
		for i := 0; i < n; i++ {
			results = append(results, mds.randDatum(typ))
		}
		return
	}

	datumSet := make(map[string]bool, NDV)
	datums := make([]types.Datum, 0, NDV)
	for len(datums) < NDV {
		d := mds.randDatum(typ)
		str, err := d.ToString()
		if err != nil {
			panic("TODO")
		}
		if datumSet[str] {
			continue
		}
		datumSet[str] = true
		datums = append(datums, d)
	}

	for i := 0; i < n; i++ {
		results = append(results, datums[rand.Intn(NDV)])
	}
	return
}

func (mds *mockDataSrouce) randDatum(typ *types.FieldType) types.Datum {
	var d types.Datum
	switch typ.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		d.SetInt64(int64(rand.Int() % 10000))
	case mysql.TypeFloat:
		d.SetFloat32(rand.Float32())
	case mysql.TypeDouble:
		d.SetFloat64(rand.Float64())
	default:
		panic("not implement")
	}
	return d
}

func (mds *mockDataSrouce) prepare() {
	mds.toUseChunks = make([]*chunk.Chunk, len(mds.orgChunks))
	for i := range mds.toUseChunks {
		mds.toUseChunks[i] = mds.orgChunks[i].CopyTo(mds.toUseChunks[i])
	}
	mds.chunkPtr = 0
}

func (mds *mockDataSrouce) Open(context.Context) error {
	return nil
}

func (mds *mockDataSrouce) Next(ctx context.Context, chk *chunk.Chunk) error {
	if mds.chunkPtr >= len(mds.toUseChunks) {
		chk.Reset()
		return nil
	}
	dataChk := mds.toUseChunks[mds.chunkPtr]
	dataChk.SwapColumns(chk)
	mds.chunkPtr++

	return nil
}

func (mds *mockDataSrouce) Close() error {
	return nil
}

func (mds *mockDataSrouce) Schema() *expression.Schema {
	return nil
}

func (mds *mockDataSrouce) retTypes() []*types.FieldType {
	return mds.p.types
}

func (mds *mockDataSrouce) newFirstChunk() *chunk.Chunk {
	return chunk.New(mds.p.types, mds.p.chunkSize, mds.p.chunkSize)
}

func buildMockDataSource(opt mockDataSourceParameters) *mockDataSrouce {
	m := &mockDataSrouce{opt, nil, nil, 0}
	m.genData()
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
	// TODO(zhangyuanjia): reuse executorBuilder.buildHashAgg instead of copying it
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
		StmtCtx:      v.ctx.GetSessionVars().StmtCtx,
		aggFuncs:     make([]aggfuncs.AggFunc, 0, len(v.aggFuncs)),
		GroupByItems: v.groupByItems,
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
	exec        string // "hash" or "stream"
	aggFunc     string // sum, avg, count ....
	hasDistinct bool
	rows        int
	groupByNDV  int // the number of distinct group-by keys
	concurrency int
}

func buildAggDataSrouce(b *testing.B, cas *aggTestCase) *mockDataSrouce {
	fieldTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeLong),
	}
	cols := make([]*expression.Column, len(fieldTypes))
	for i := range cols {
		cols[i] = &expression.Column{Index: i, RetType: fieldTypes[i]}
	}
	ctx := core.MockContext()

	chunkSize := 1 << 10
	orders := make([]bool, len(fieldTypes))
	if cas.exec == "stream" {
		orders[1] = true
	}
	child := buildMockDataSource(mockDataSourceParameters{
		types:     fieldTypes,
		NDVs:      []int{0, cas.groupByNDV},
		orders:    orders,
		rows:      cas.rows,
		chunkSize: chunkSize,
		ctx:       ctx.GetSessionVars().StmtCtx,
	})
	return child
}

func buildAggExecutor(b *testing.B, cas *aggTestCase, child Executor) Executor {
	childTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeLong),
	}
	childCols := make([]*expression.Column, len(childTypes))
	for i := range childCols {
		childCols[i] = &expression.Column{Index: i, RetType: childTypes[i]}
	}
	ctx := core.MockContext()

	sumFunc := aggregation.NewAggFuncDesc(ctx, ast.AggFuncSum, []expression.Expression{childCols[0]}, cas.hasDistinct)
	groupBy := []expression.Expression{childCols[1]}

	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggFinalConcurrency, fmt.Sprintf("%v", cas.concurrency)); err != nil {
		b.Fatal(err)
	}
	if err := ctx.GetSessionVars().SetSystemVar(variable.TiDBHashAggPartialConcurrency, fmt.Sprintf("%v", cas.concurrency)); err != nil {
		b.Fatal(err)
	}

	p := &aggExecutorParameters{
		ctx:          ctx,
		schema:       expression.NewSchema(childCols...),
		child:        child,
		aggFuncs:     []*aggregation.AggFuncDesc{sumFunc},
		groupByItems: groupBy,
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
	dataSource := buildAggDataSrouce(b, cas)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer() // prepare a new agg-executor
		aggExec := buildAggExecutor(b, cas, dataSource)
		tmpCtx := context.Background()
		chk := aggExec.newFirstChunk()
		dataSource.prepare()

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

		b.StopTimer()
		if err := aggExec.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkHashAgg(b *testing.B) {
	cases := []*aggTestCase{
		{
			exec:        "hash",
			aggFunc:     ast.AggFuncSum,
			hasDistinct: false,
			rows:        10000,
			groupByNDV:  100,
			concurrency: 4,
		},
		{
			exec:        "hash",
			aggFunc:     ast.AggFuncSum,
			hasDistinct: false,
			rows:        100000,
			groupByNDV:  100,
			concurrency: 4,
		},
		{
			exec:        "hash",
			aggFunc:     ast.AggFuncSum,
			hasDistinct: false,
			rows:        1000000,
			groupByNDV:  100,
			concurrency: 4,
		},
	}

	for i, cas := range cases {
		b.Run(fmt.Sprintf("case-%d", i), func(b *testing.B) {
			benchmarkAggExecWithCase(b, cas)
		})
	}
}
