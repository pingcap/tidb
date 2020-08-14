// Copyright 2016 PingCAP, Inc.
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
	"crypto/tls"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/expression"
	plannerutil "github.com/pingcap/tidb/planner/util"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
)

var _ = Suite(&testExecSuite{})
var _ = SerialSuites(&testExecSerialSuite{})

// Note: it's a tricky way to export the `inspectionSummaryRules` and `inspectionRules` for unit test but invisible for normal code
var (
	InspectionSummaryRules = inspectionSummaryRules
	InspectionRules        = inspectionRules
)

type testExecSuite struct {
}

type testExecSerialSuite struct {
}

// mockSessionManager is a mocked session manager which is used for test.
type mockSessionManager struct {
	PS []*util.ProcessInfo
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *mockSessionManager) ShowProcessList() map[uint64]*util.ProcessInfo {
	ret := make(map[uint64]*util.ProcessInfo)
	for _, item := range msm.PS {
		ret[item.ID] = item
	}
	return ret
}

func (msm *mockSessionManager) GetProcessInfo(id uint64) (*util.ProcessInfo, bool) {
	for _, item := range msm.PS {
		if item.ID == id {
			return item, true
		}
	}
	return &util.ProcessInfo{}, false
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager) Kill(cid uint64, query bool) {

}

func (msm *mockSessionManager) UpdateTLSConfig(cfg *tls.Config) {
}

func (s *testExecSuite) TestShowProcessList(c *C) {
	// Compose schema.
	names := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
	ftypes := []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
		mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString}
	schema := buildSchema(names, ftypes)

	// Compose a mocked session manager.
	ps := make([]*util.ProcessInfo, 0, 1)
	pi := &util.ProcessInfo{
		ID:      0,
		User:    "test",
		Host:    "127.0.0.1",
		DB:      "test",
		Command: 't',
		State:   1,
		Info:    "",
	}
	ps = append(ps, pi)
	sm := &mockSessionManager{
		PS: ps,
	}
	sctx := mock.NewContext()
	sctx.SetSessionManager(sm)
	sctx.GetSessionVars().User = &auth.UserIdentity{Username: "test"}

	// Compose executor.
	e := &ShowExec{
		baseExecutor: newBaseExecutor(sctx, schema, nil, 0),
		Tp:           ast.ShowProcessList,
	}

	ctx := context.Background()
	err := e.Open(ctx)
	c.Assert(err, IsNil)

	chk := newFirstChunk(e)
	it := chunk.NewIterator4Chunk(chk)
	// Run test and check results.
	for _, p := range ps {
		err = e.Next(context.Background(), chk)
		c.Assert(err, IsNil)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			c.Assert(row.GetUint64(0), Equals, p.ID)
		}
	}
	err = e.Next(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 0)
	err = e.Close()
	c.Assert(err, IsNil)
}

func buildSchema(names []string, ftypes []byte) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	for i := range names {
		col := &expression.Column{
			UniqueID: int64(i),
		}
		// User varchar as the default return column type.
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 && ftypes[0] != mysql.TypeUnspecified {
			tp = ftypes[0]
		}
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.Collate = types.DefaultCharsetForType(tp)
		col.RetType = fieldType
		schema.Append(col)
	}
	return schema
}

func (s *testExecSuite) TestBuildKvRangesForIndexJoinWithoutCwc(c *C) {
	indexRanges := make([]*ranger.Range, 0, 6)
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 2))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 3, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 2, 1, 1))

	joinKeyRows := make([]*indexJoinLookUpContent, 0, 5)
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(1, 1)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(1, 2)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(2, 1)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(2, 2)})
	joinKeyRows = append(joinKeyRows, &indexJoinLookUpContent{keys: generateDatumSlice(2, 3)})

	keyOff2IdxOff := []int{1, 3}
	ctx := mock.NewContext()
	kvRanges, err := buildKvRangesForIndexJoin(ctx, 0, 0, joinKeyRows, indexRanges, keyOff2IdxOff, nil)
	c.Assert(err, IsNil)
	// Check the kvRanges is in order.
	for i, kvRange := range kvRanges {
		c.Assert(kvRange.StartKey.Cmp(kvRange.EndKey) < 0, IsTrue)
		if i > 0 {
			c.Assert(kvRange.StartKey.Cmp(kvRanges[i-1].EndKey) >= 0, IsTrue)
		}
	}
}

func generateIndexRange(vals ...int64) *ranger.Range {
	lowDatums := generateDatumSlice(vals...)
	highDatums := make([]types.Datum, len(vals))
	copy(highDatums, lowDatums)
	return &ranger.Range{LowVal: lowDatums, HighVal: highDatums}
}

func generateDatumSlice(vals ...int64) []types.Datum {
	datums := make([]types.Datum, len(vals))
	for i, val := range vals {
		datums[i].SetInt64(val)
	}
	return datums
}

func (s *testExecSuite) TestGetFieldsFromLine(c *C) {
	tests := []struct {
		input    string
		expected []string
	}{
		{
			`"1","a string","100.20"`,
			[]string{"1", "a string", "100.20"},
		},
		{
			`"2","a string containing a , comma","102.20"`,
			[]string{"2", "a string containing a , comma", "102.20"},
		},
		{
			`"3","a string containing a \" quote","102.20"`,
			[]string{"3", "a string containing a \" quote", "102.20"},
		},
		{
			`"4","a string containing a \", quote and comma","102.20"`,
			[]string{"4", "a string containing a \", quote and comma", "102.20"},
		},
		// Test some escape char.
		{
			`"\0\b\n\r\t\Z\\\  \c\'\""`,
			[]string{string([]byte{0, '\b', '\n', '\r', '\t', 26, '\\', ' ', ' ', 'c', '\'', '"'})},
		},
		// Test mixed.
		{
			`"123",456,"\t7890",abcd`,
			[]string{"123", "456", "\t7890", "abcd"},
		},
	}

	ldInfo := LoadDataInfo{
		FieldsInfo: &ast.FieldsClause{
			Enclosed:   '"',
			Terminated: ",",
		},
	}

	for _, test := range tests {
		got, err := ldInfo.getFieldsFromLine([]byte(test.input))
		c.Assert(err, IsNil, Commentf("failed: %s", test.input))
		assertEqualStrings(c, got, test.expected)
	}

	_, err := ldInfo.getFieldsFromLine([]byte(`1,a string,100.20`))
	c.Assert(err, IsNil)
}

func assertEqualStrings(c *C, got []field, expect []string) {
	c.Assert(len(got), Equals, len(expect))
	for i := 0; i < len(got); i++ {
		c.Assert(string(got[i].str), Equals, expect[i])
	}
}

func (s *testExecSerialSuite) TestSortSpillDisk(c *C) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMUseTmpStorage = true
		conf.MemQuotaQuery = 1
	})
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill", "return(true)"), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/executor/testSortedRowContainerSpill"), IsNil)
	}()
	ctx := mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, -1)
	cas := &sortCase{rows: 2048, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt := mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
		ndvs:   cas.ndvs,
	}
	dataSource := buildMockDataSource(opt)
	exec := &SortExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, stringutil.StringerStr("sort"), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schema:       dataSource.schema,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.columns()[idx]})
	}
	tmpCtx := context.Background()
	chk := newFirstChunk(exec)
	dataSource.prepareChunks()
	err := exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition and all data in memory.
	c.Assert(len(exec.partitionList), Equals, 1)
	c.Assert(exec.partitionList[0].AlreadySpilledSafe(), Equals, false)
	c.Assert(exec.partitionList[0].NumRow(), Equals, 2048)
	err = exec.Close()
	c.Assert(err, IsNil)

	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 1)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test 2 partitions and all data in disk.
	// Now spilling is in parallel.
	// Maybe the second add() will called before spilling, depends on
	// Golang goroutine scheduling. So the result has two possibilities.
	if len(exec.partitionList) == 2 {
		c.Assert(len(exec.partitionList), Equals, 2)
		c.Assert(exec.partitionList[0].AlreadySpilledSafe(), Equals, true)
		c.Assert(exec.partitionList[1].AlreadySpilledSafe(), Equals, true)
		c.Assert(exec.partitionList[0].NumRow(), Equals, 1024)
		c.Assert(exec.partitionList[1].NumRow(), Equals, 1024)
	} else {
		c.Assert(len(exec.partitionList), Equals, 1)
		c.Assert(exec.partitionList[0].AlreadySpilledSafe(), Equals, true)
		c.Assert(exec.partitionList[0].NumRow(), Equals, 2048)
	}

	err = exec.Close()
	c.Assert(err, IsNil)

	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 24000)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Test only 1 partition but spill disk.
	c.Assert(len(exec.partitionList), Equals, 1)
	c.Assert(exec.partitionList[0].AlreadySpilledSafe(), Equals, true)
	c.Assert(exec.partitionList[0].NumRow(), Equals, 2048)
	err = exec.Close()
	c.Assert(err, IsNil)

	// Test partition nums.
	ctx = mock.NewContext()
	ctx.GetSessionVars().InitChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().MaxChunkSize = variable.DefMaxChunkSize
	ctx.GetSessionVars().StmtCtx.MemTracker = memory.NewTracker(-1, 16864*50)
	ctx.GetSessionVars().StmtCtx.MemTracker.Consume(16864 * 45)
	cas = &sortCase{rows: 20480, orderByIdx: []int{0, 1}, ndvs: []int{0, 0}, ctx: ctx}
	opt = mockDataSourceParameters{
		schema: expression.NewSchema(cas.columns()...),
		rows:   cas.rows,
		ctx:    cas.ctx,
		ndvs:   cas.ndvs,
	}
	dataSource = buildMockDataSource(opt)
	exec = &SortExec{
		baseExecutor: newBaseExecutor(cas.ctx, dataSource.schema, stringutil.StringerStr("sort"), 0, dataSource),
		ByItems:      make([]*plannerutil.ByItems, 0, len(cas.orderByIdx)),
		schema:       dataSource.schema,
	}
	for _, idx := range cas.orderByIdx {
		exec.ByItems = append(exec.ByItems, &plannerutil.ByItems{Expr: cas.columns()[idx]})
	}
	tmpCtx = context.Background()
	chk = newFirstChunk(exec)
	dataSource.prepareChunks()
	err = exec.Open(tmpCtx)
	c.Assert(err, IsNil)
	for {
		err = exec.Next(tmpCtx, chk)
		c.Assert(err, IsNil)
		if chk.NumRows() == 0 {
			break
		}
	}
	// Don't spill too many partitions.
	c.Assert(len(exec.partitionList) <= 4, IsTrue)
	err = exec.Close()
	c.Assert(err, IsNil)
}
