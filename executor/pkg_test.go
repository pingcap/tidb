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
var _ = SerialSuites(&pkgTestSerialSuite{})

type pkgTestSuite struct {
}

type pkgTestSerialSuite struct {
}

func (s *pkgTestSuite) TestNestedLoopApply(c *C) {
	ctx := context.Background()
	sctx := mock.NewContext()
	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	col1 := &expression.Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLong)}
	con := &expression.Constant{Value: types.NewDatum(6), RetType: types.NewFieldType(mysql.TypeLong)}
	outerSchema := expression.NewSchema(col0)
	outerExec := buildMockDataSource(mockDataSourceParameters{
		schema: outerSchema,
		rows:   6,
		ctx:    sctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			return int64(row + 1)
		},
	})
	outerExec.prepareChunks()

	innerSchema := expression.NewSchema(col1)
	innerExec := buildMockDataSource(mockDataSourceParameters{
		schema: innerSchema,
		rows:   6,
		ctx:    sctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			return int64(row + 1)
		},
	})
	innerExec.prepareChunks()

	outerFilter := expression.NewFunctionInternal(sctx, ast.LT, types.NewFieldType(mysql.TypeTiny), col0, con)
	innerFilter := outerFilter.Clone()
	otherFilter := expression.NewFunctionInternal(sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col0, col1)
	joiner := newJoiner(sctx, plannercore.InnerJoin, false,
		make([]types.Datum, innerExec.Schema().Len()), []expression.Expression{otherFilter},
		retTypes(outerExec), retTypes(innerExec), nil)
	joinSchema := expression.NewSchema(col0, col1)
	join := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(sctx, joinSchema, nil),
		outerExec:    outerExec,
		innerExec:    innerExec,
		outerFilter:  []expression.Expression{outerFilter},
		innerFilter:  []expression.Expression{innerFilter},
		joiner:       joiner,
		ctx:          sctx,
	}
	join.innerList = chunk.NewList(retTypes(innerExec), innerExec.initCap, innerExec.maxChunkSize)
	join.innerChunk = newFirstChunk(innerExec)
	join.outerChunk = newFirstChunk(outerExec)
	joinChk := newFirstChunk(join)
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
