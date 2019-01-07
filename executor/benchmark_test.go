package executor

import (
	"context"
	"math/rand"
	"testing"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor/aggfuncs"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
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
		colDatums[i] = mds.genDatums(mds.opt.types[i], mds.opt.cardinalities[i])
	}

	for i := 0; i < mds.opt.rows; i++ {
		row := make([]types.Datum, len(mds.opt.types))
		for colIdx := 0; colIdx < len(mds.opt.types); colIdx++ {
			card := mds.opt.cardinalities[colIdx]
			row[colIdx] = colDatums[colIdx][rand.Intn(card)]
		}
		mds.chunks.AppendRow(chunk.MutRowFromDatums(row).ToRow())
	}
}

func (mds *mockDataSrouce) genDatums(typ *types.FieldType, cardinality int) []types.Datum {
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
	return datums
}

func (mds *mockDataSrouce) randDatum(typ *types.FieldType) types.Datum {
	var d types.Datum
	switch typ.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		d.SetInt64(int64(rand.Int()))
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

func newMockDataSource(opt mockDataSourceOption) *mockDataSrouce {
	return &mockDataSrouce{opt, nil, 0}
}

func newHashAggExecutor(ctx sessionctx.Context, schema *expression.Schema, child Executor,
	aggFuncs []*aggregation.AggFuncDesc, groupByItems []expression.Expression) Executor {
	e := &HashAggExec{
		baseExecutor:    newBaseExecutor(ctx, schema, "", child),
		sc:              &stmtctx.StatementContext{},
		PartialAggFuncs: make([]aggfuncs.AggFunc, 0, len(aggFuncs)),
		GroupByItems:    groupByItems,
	}

	return e
}

func BenchmarkTestZYJ(b *testing.B) {
	b.ReportAllocs()
}
