package executor

import (
	"context"
	"fmt"
	"testing"

	"github.com/klauspost/cpuid"
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/spaolacci/murmur3"
)

var _ = Suite(&pkgTestSuite{})

type pkgTestSuite struct {
}

type MockExec struct {
	baseExecutor

	Rows      []chunk.MutRow
	curRowIdx int
}

func (m *MockExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	req.Reset()
	colTypes := m.retTypes()
	for ; m.curRowIdx < len(m.Rows) && req.NumRows() < req.Capacity(); m.curRowIdx++ {
		curRow := m.Rows[m.curRowIdx]
		for i := 0; i < curRow.Len(); i++ {
			curDatum := curRow.ToRow().GetDatum(i, colTypes[i])
			req.AppendDatum(i, &curDatum)
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
		baseExecutor: newBaseExecutor(sctx, outerSchema, nil),
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
		baseExecutor: newBaseExecutor(sctx, innerSchema, nil),
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
		baseExecutor: newBaseExecutor(sctx, joinSchema, nil),
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
		err := join.Next(ctx, chunk.NewRecordBatch(joinChk))
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

func prepareOneColChildExec(sctx sessionctx.Context, rowCount int) Executor {
	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	schema := expression.NewSchema(col0)
	exec := &MockExec{
		baseExecutor: newBaseExecutor(sctx, schema, nil),
		Rows:         make([]chunk.MutRow, rowCount)}
	for i := 0; i < len(exec.Rows); i++ {
		exec.Rows[i] = chunk.MutRowFromDatums(types.MakeDatums(i % 10))
	}
	return exec
}

func buildExec4RadixHashJoin(sctx sessionctx.Context, rowCount int) *RadixHashJoinExec {
	childExec0 := prepareOneColChildExec(sctx, rowCount)
	childExec1 := prepareOneColChildExec(sctx, rowCount)

	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	col1 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	joinSchema := expression.NewSchema(col0, col1)
	hashJoinExec := &HashJoinExec{
		baseExecutor:   newBaseExecutor(sctx, joinSchema, stringutil.StringerStr("HashJoin"), childExec0, childExec1),
		concurrency:    4,
		joinType:       0, // InnerJoin
		innerIdx:       0,
		innerKeys:      []*expression.Column{{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}},
		innerKeyColIdx: []int{0},
		outerKeys:      []*expression.Column{{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}},
		outerKeyColIdx: []int{0},
		innerExec:      childExec0,
		outerExec:      childExec1,
	}
	return &RadixHashJoinExec{HashJoinExec: hashJoinExec}
}

func (s *pkgTestSuite) TestRadixPartition(c *C) {
	sctx := mock.NewContext()
	hashJoinExec := buildExec4RadixHashJoin(sctx, 200)
	sv := sctx.GetSessionVars()
	originL2CacheSize, originEnableRadixJoin, originMaxChunkSize := sv.L2CacheSize, sv.EnableRadixJoin, sv.MaxChunkSize
	sv.L2CacheSize = 100
	sv.EnableRadixJoin = true
	// FIXME: use initChunkSize when join support initChunkSize.
	sv.MaxChunkSize = 100
	defer func() {
		sv.L2CacheSize, sv.EnableRadixJoin, sv.MaxChunkSize = originL2CacheSize, originEnableRadixJoin, originMaxChunkSize
	}()
	sv.StmtCtx.MemTracker = memory.NewTracker(stringutil.StringerStr("RootMemTracker"), variable.DefTiDBMemQuotaHashJoin)

	ctx := context.Background()
	err := hashJoinExec.Open(ctx)
	c.Assert(err, IsNil)

	hashJoinExec.fetchInnerRows(ctx)
	c.Assert(hashJoinExec.innerResult.GetMemTracker().BytesConsumed(), Equals, int64(14400))

	hashJoinExec.evalRadixBit()
	// ceil(log_2(14400/(100*3/4))) = 8
	c.Assert(len(hashJoinExec.innerParts), Equals, 256)
	radixBits := hashJoinExec.radixBits
	c.Assert(radixBits, Equals, uint32(0x00ff))

	err = hashJoinExec.partitionInnerRows()
	c.Assert(err, IsNil)
	totalRowCnt := 0
	for i, part := range hashJoinExec.innerParts {
		if part == nil {
			continue
		}
		totalRowCnt += part.NumRows()
		iter := chunk.NewIterator4Chunk(part)
		keyBuf := make([]byte, 0, 64)
		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			hasNull, keyBuf, err := hashJoinExec.getJoinKeyFromChkRow(false, row, keyBuf)
			c.Assert(err, IsNil)
			c.Assert(hasNull, IsFalse)
			joinHash := murmur3.Sum32(keyBuf)
			c.Assert(joinHash&radixBits, Equals, uint32(i)&radixBits)
		}
	}
	c.Assert(totalRowCnt, Equals, 200)

	for _, ch := range hashJoinExec.outerResultChs {
		close(ch)
	}
	for _, ch := range hashJoinExec.joinChkResourceCh {
		close(ch)
	}
	err = hashJoinExec.Close()
	c.Assert(err, IsNil)
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

func BenchmarkPartitionInnerRows(b *testing.B) {
	sctx := mock.NewContext()
	hashJoinExec := buildExec4RadixHashJoin(sctx, 1500000)
	sv := sctx.GetSessionVars()
	originL2CacheSize, originEnableRadixJoin, originMaxChunkSize := sv.L2CacheSize, sv.EnableRadixJoin, sv.MaxChunkSize
	sv.L2CacheSize = cpuid.CPU.Cache.L2
	sv.EnableRadixJoin = true
	sv.MaxChunkSize = 1024
	defer func() {
		sv.L2CacheSize, sv.EnableRadixJoin, sv.MaxChunkSize = originL2CacheSize, originEnableRadixJoin, originMaxChunkSize
	}()
	sv.StmtCtx.MemTracker = memory.NewTracker(stringutil.StringerStr("RootMemTracker"), variable.DefTiDBMemQuotaHashJoin)

	ctx := context.Background()
	hashJoinExec.Open(ctx)
	hashJoinExec.fetchInnerRows(ctx)
	hashJoinExec.evalRadixBit()
	b.ResetTimer()
	hashJoinExec.concurrency = 16
	hashJoinExec.maxChunkSize = 1024
	hashJoinExec.initCap = 1024
	for i := 0; i < b.N; i++ {
		hashJoinExec.partitionInnerRows()
		hashJoinExec.innerRowPrts = hashJoinExec.innerRowPrts[:0]
	}
}

func (s *pkgTestSuite) TestParallelBuildHashTable4RadixJoin(c *C) {
	sctx := mock.NewContext()
	hashJoinExec := buildExec4RadixHashJoin(sctx, 200)

	sv := sctx.GetSessionVars()
	sv.L2CacheSize = 100
	sv.EnableRadixJoin = true
	sv.MaxChunkSize = 100
	sv.StmtCtx.MemTracker = memory.NewTracker(stringutil.StringerStr("RootMemTracker"), variable.DefTiDBMemQuotaHashJoin)

	ctx := context.Background()
	err := hashJoinExec.Open(ctx)
	c.Assert(err, IsNil)

	hashJoinExec.partitionInnerAndBuildHashTables(ctx)
	innerParts := hashJoinExec.innerParts
	c.Assert(len(hashJoinExec.hashTables), Equals, len(innerParts))
	for i := 0; i < len(innerParts); i++ {
		if innerParts[i] == nil {
			c.Assert(hashJoinExec.hashTables[i], IsNil)
		} else {
			c.Assert(hashJoinExec.hashTables[i], NotNil)
		}
	}
}
