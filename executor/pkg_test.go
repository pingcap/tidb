package executor

import (
	"context"
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
)

var _ = Suite(&pkgTestSuite{})

type pkgTestSuite struct {
}

type MockExec struct {
	baseExecutor

	Rows      []chunk.MutRow
	curRowIdx int
}

func (m *MockExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	colTypes := m.retTypes()
	for ; m.curRowIdx < len(m.Rows) && chk.NumRows() < chk.Capacity(); m.curRowIdx++ {
		curRow := m.Rows[m.curRowIdx]
		for i := 0; i < curRow.Len(); i++ {
			curDatum := curRow.ToRow().GetDatum(i, colTypes[i])
			chk.AppendDatum(i, &curDatum)
		}
	}
	return nil
}

func (m *MockExec) Close() error {
	m.curRowIdx = 0
	return nil
}

func (m *MockExec) Open(ctx context.Context) error {
	m.curRowIdx = 0
	return nil
}

func (s *pkgTestSuite) TestNestedLoopApply(c *C) {
	ctx := context.Background()
	sctx := mock.NewContext()
	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	col1 := &expression.Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLong)}
	con := &expression.Constant{Value: types.NewDatum(6), RetType: types.NewFieldType(mysql.TypeLong)}
	outerSchema := expression.NewSchema(col0)
	outerExec := &MockExec{
		baseExecutor: newBaseExecutor(sctx, outerSchema, ""),
		Rows: []chunk.MutRow{
			chunk.MutRowFromDatums(types.MakeDatums(1)),
			chunk.MutRowFromDatums(types.MakeDatums(2)),
			chunk.MutRowFromDatums(types.MakeDatums(3)),
			chunk.MutRowFromDatums(types.MakeDatums(4)),
			chunk.MutRowFromDatums(types.MakeDatums(5)),
			chunk.MutRowFromDatums(types.MakeDatums(6)),
		}}
	innerSchema := expression.NewSchema(col1)
	innerExec := &MockExec{
		baseExecutor: newBaseExecutor(sctx, innerSchema, ""),
		Rows: []chunk.MutRow{
			chunk.MutRowFromDatums(types.MakeDatums(1)),
			chunk.MutRowFromDatums(types.MakeDatums(2)),
			chunk.MutRowFromDatums(types.MakeDatums(3)),
			chunk.MutRowFromDatums(types.MakeDatums(4)),
			chunk.MutRowFromDatums(types.MakeDatums(5)),
			chunk.MutRowFromDatums(types.MakeDatums(6)),
		}}
	outerFilter := expression.NewFunctionInternal(sctx, ast.LT, types.NewFieldType(mysql.TypeTiny), col0, con)
	innerFilter := outerFilter.Clone()
	otherFilter := expression.NewFunctionInternal(sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col0, col1)
	joiner := newJoiner(sctx, plannercore.InnerJoin, false,
		make([]types.Datum, innerExec.Schema().Len()), []expression.Expression{otherFilter}, outerExec.retTypes(), innerExec.retTypes())
	joinSchema := expression.NewSchema(col0, col1)
	join := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(sctx, joinSchema, ""),
		outerExec:    outerExec,
		innerExec:    innerExec,
		outerFilter:  []expression.Expression{outerFilter},
		innerFilter:  []expression.Expression{innerFilter},
		joiner:       joiner,
	}
	join.innerList = chunk.NewList(innerExec.retTypes(), innerExec.initCap, innerExec.maxChunkSize)
	join.innerChunk = innerExec.newFirstChunk()
	join.outerChunk = outerExec.newFirstChunk()
	joinChk := join.newFirstChunk()
	it := chunk.NewIterator4Chunk(joinChk)
	for rowIdx := 1; ; {
		err := join.Next(ctx, joinChk)
		c.Check(err, IsNil)
		if joinChk.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			correctResult := fmt.Sprintf("%v %v", rowIdx, rowIdx)
			obtainedResult := fmt.Sprintf("%v %v", row.GetInt64(0), row.GetInt64(1))
			c.Check(obtainedResult, Equals, correctResult)
			rowIdx++
		}
	}
}

func (s *pkgTestSuite) TestMoveInfoSchemaToFront(c *C) {
	dbss := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"A", "B", "C", "INFORMATION_SCHEMA"},
		{"A", "B", "INFORMATION_SCHEMA", "a"},
		{"INFORMATION_SCHEMA"},
		{"A", "B", "C", "INFORMATION_SCHEMA", "a", "b"},
	}
	wanted := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"INFORMATION_SCHEMA", "A", "B", "C"},
		{"INFORMATION_SCHEMA", "A", "B", "a"},
		{"INFORMATION_SCHEMA"},
		{"INFORMATION_SCHEMA", "A", "B", "C", "a", "b"},
	}

	for _, dbs := range dbss {
		moveInfoSchemaToFront(dbs)
	}

	for i, dbs := range wanted {
		c.Check(len(dbss[i]), Equals, len(dbs))
		for j, db := range dbs {
			c.Check(dbss[i][j], Equals, db)
		}
	}
}
