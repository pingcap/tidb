package executor

import (
	"testing"

	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"golang.org/x/net/context"
)

type mockExec struct {
	baseExecutor
	chk []*chunk.Chunk

	i int
	chkIdx int
}

const (
	partialConcurrency = 4
	numberRows = 1000000
)


func (e *mockExec) Open(ctx context.Context) error {
	if e.chk == nil {
		e.chk = make([]*chunk.Chunk, partialConcurrency*2)
	}
	e.i = 0
	e.chkIdx = 0
	for i := 0; i < partialConcurrency*2; i++ {
		if e.chk[i] == nil {
			e.chk[i] = chunk.NewChunkWithCapacity([]*types.FieldType{types.NewFieldType(mysql.TypeLong)}, 1000)
		}
		for j := int64(0); j < 1000; j++ {
			e.chk[i].AppendInt64(0, j)
		}
	}
	return nil
}

func (e *mockExec) Close() error {
	for _, chk := range e.chk {
		chk.Reset()
	}
	return nil
}

func (e *mockExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	if e.i < numberRows/1000 {
		if chk.NumRows() == 0{
			chk.SwapColumns(e.chk[e.chkIdx])
			e.chkIdx++
		}
		e.i++
	}else{
		chk.SwapColumns(e.chk[0])
	}

	return nil
}

func BenchmarkParallelAgg(b *testing.B) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().HashAggPartialConcurrency = partialConcurrency
	ctx.GetSessionVars().HashAggFinalConcurrency = 4

	retCol0 := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLong),
	}
	mockTableReaderExec := &mockExec{
		baseExecutor: newBaseExecutor(ctx, expression.NewSchema(retCol0), "mockTable", nil),
	}
	arg := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLong),
	}
	avgDesc := aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{arg}, false)
	avgDesc.Mode = aggregation.Partial1Mode
	avgFunc := avgDesc.GetAggFunc()

	retCol1 := &expression.Column{
		Index:   0,
		RetType: avgDesc.RetTp,
	}
	hashAggExec := &HashAggExec{
		baseExecutor:       newBaseExecutor(ctx, expression.NewSchema(retCol1), "mockHashExec", mockTableReaderExec),
		AggFuncs:           []aggregation.Aggregation{avgFunc},
		doesUnparallelExec: true,
	}

	bgCtx := context.Background()
	hashAggExec.Open(bgCtx)

	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		hashAggExec.Close()
		hashAggExec.Open(bgCtx)
		origChk := chunk.NewChunkWithCapacity([]*types.FieldType{avgDesc.RetTp}, 1)
		b.StartTimer()

		hashAggExec.Next(bgCtx, origChk)
	}
	b.StopTimer()
	hashAggExec.Close()
	b.StartTimer()
}
