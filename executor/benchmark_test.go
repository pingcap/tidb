package executor

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/planner/core"
	"math/rand"
	"testing"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type mockDataSourceOption struct {
	types         []*types.FieldType // type of columns
	cardinalities []int              // cardinality of columns
	rows          int
	initChunkSize int
	maxChunkSize  int
}

type mockDataSrouce struct {
	opt      mockDataSourceOption
	chunks   *chunk.List
	chunkPtr int
}

func (mds *mockDataSrouce) genData() {
	mds.chunks = chunk.NewList(mds.opt.types, mds.opt.initChunkSize, mds.opt.maxChunkSize)

	colDatums := make([][]types.Datum, len(mds.opt.types))
	for i := 0; i < len(mds.opt.types); i++ {
		colDatums[i] = mds.genDatums(mds.opt.types[i], mds.opt.rows, mds.opt.cardinalities[i])
	}

	for i := 0; i < mds.opt.rows; i++ {
		row := make([]types.Datum, len(mds.opt.types))
		for colIdx := 0; colIdx < len(mds.opt.types); colIdx++ {
			row[colIdx] = colDatums[colIdx][i]
		}
		mds.chunks.AppendRow(chunk.MutRowFromDatums(row).ToRow())
	}
}

func (mds *mockDataSrouce) genDatums(typ *types.FieldType, n, cardinality int) []types.Datum {
	if cardinality == 0 {
		results := make([]types.Datum, 0, n)
		for i := 0; i < n; i++ {
			results = append(results, mds.randDatum(typ))
		}
		return results
	}

	datumSet := make(map[string]bool, cardinality)
	datums := make([]types.Datum, 0, cardinality)
	for len(datums) < cardinality {
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

	results := make([]types.Datum, 0, n)
	for i := 0; i < n; i++ {
		results = append(results, datums[rand.Intn(cardinality)])
	}
	return results
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

func (mds *mockDataSrouce) Open(context.Context) error {
	mds.genData()
	return nil
}

func (mds *mockDataSrouce) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if mds.chunkPtr >= mds.chunks.NumChunks() {
		return nil
	}
	dataChk := mds.chunks.GetChunk(mds.chunkPtr)
	chk.SwapColumns(dataChk)
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
	return mds.opt.types
}

func (mds *mockDataSrouce) newFirstChunk() *chunk.Chunk {
	return chunk.New(mds.opt.types, mds.opt.initChunkSize, mds.opt.maxChunkSize)
}

func buildMockDataSource(opt mockDataSourceOption) *mockDataSrouce {
	return &mockDataSrouce{opt, nil, 0}
}

type hashAggExecutorParameters struct {
	ctx          sessionctx.Context
	schema       *expression.Schema
	child        Executor
	aggFuncs     []*aggregation.AggFuncDesc
	groupByItems []expression.Expression
}

func newColumnWithType(id int, t *types.FieldType) *expression.Column {
	return &expression.Column{
		UniqueID: int64(id),
		ColName:  model.NewCIStr(fmt.Sprint(id)),
		TblName:  model.NewCIStr("t"),
		DBName:   model.NewCIStr("test"),
		RetType:  t,
	}
}

func buildHashAggExecutor(v *hashAggExecutorParameters) Executor {
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

type aggTestCase struct {
	exec        string // "hash" or "stream"
	aggFunc     string // sum, avg, count ....
	rows        int
	groupByCard int // the number of distinct group-by keys
}

type aggTestResult struct {
	*aggTestCase

	cost time.Duration
}

func report(results []*aggTestResult) {
	fmt.Println("Exec\tAggFunc\tRows\tGroupByColCard\tCost")
	for _, r := range results {
		fmt.Printf("%v\t%v\t%v\t%v\t%v\n", r.exec, r.aggFunc, r.rows, r.groupByCard, r.cost)
	}
	fmt.Println()
}

func doTest(t *testing.T, cas *aggTestCase) *aggTestResult {
	childTypes := []*types.FieldType{
		types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeLong),
	}
	childCols := make([]*expression.Column, len(childTypes))
	for i := range childCols {
		childCols[i] = &expression.Column{Index: i, RetType: childTypes[i]}
	}

	chunkSize := 1 << 10
	child := buildMockDataSource(mockDataSourceOption{
		types:         childTypes,
		cardinalities: []int{0, cas.groupByCard},
		rows:          cas.rows,
		initChunkSize: chunkSize,
		maxChunkSize:  chunkSize,
	})

	if cas.exec != "hash" {
		t.Fatal("not implement")
	}

	ctx := core.MockContext()
	sumFunc := aggregation.NewAggFuncDesc(ctx, ast.AggFuncSum, []expression.Expression{childCols[0]}, false)
	frFunc := aggregation.NewAggFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{childCols[1]}, false)
	groupBy := []expression.Expression{childCols[1]}

	p := &hashAggExecutorParameters{
		ctx:          ctx,
		schema:       expression.NewSchema(childCols...),
		child:        child,
		aggFuncs:     []*aggregation.AggFuncDesc{sumFunc, frFunc},
		groupByItems: groupBy,
	}

	agg := buildHashAggExecutor(p)

	if err := agg.Open(context.Background()); err != nil {
		t.Fatal(err)
	}
	buf := agg.newFirstChunk()
	begin := time.Now()
	for {
		err := agg.Next(context.Background(), buf)
		if err != nil {
			t.Fatal(err)
		}
		if buf.NumRows() == 0 {
			break
		}
	}
	cost := time.Now().Sub(begin)

	return &aggTestResult{
		cas,
		cost,
	}
}

func TestDoTest(t *testing.T) {
	aggExecs := []string{"hash"}
	aggFuncs := []string{ast.AggFuncSum, ast.AggFuncAvg}
	aggDataSizes := []struct {
		rows        int
		groupByCard int
	}{
		{1000000, 10},
		{1000000, 100},
		{1000000, 1000},
		{1000000, 10000},
	}

	var testCases []*aggTestCase
	for _, exec := range aggExecs {
		for _, aggFunc := range aggFuncs {
			for _, dataSize := range aggDataSizes {
				testCases = append(testCases, &aggTestCase{
					exec:        exec,
					aggFunc:     aggFunc,
					rows:        dataSize.rows,
					groupByCard: dataSize.groupByCard,
				})
			}
		}
	}

	var testResults []*aggTestResult
	for _, cas := range testCases {
		fmt.Println(">>>> test ", cas)
		testResults = append(testResults, doTest(t, cas))
	}

	report(testResults)
}
