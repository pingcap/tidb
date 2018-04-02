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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/ranger"
	"golang.org/x/net/context"
)

var _ = Suite(&testExecSuite{})

type testExecSuite struct {
}

// mockSessionManager is a mocked session manager which is used for test.
type mockSessionManager struct {
	PS []util.ProcessInfo
}

// ShowProcessList implements the SessionManager.ShowProcessList interface.
func (msm *mockSessionManager) ShowProcessList() []util.ProcessInfo {
	return msm.PS
}

// Kill implements the SessionManager.Kill interface.
func (msm *mockSessionManager) Kill(cid uint64, query bool) {

}

func (s *testExecSuite) TestShowProcessList(c *C) {
	// Compose schema.
	names := []string{"Id", "User", "Host", "db", "Command", "Time", "State", "Info"}
	ftypes := []byte{mysql.TypeLonglong, mysql.TypeVarchar, mysql.TypeVarchar,
		mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLong, mysql.TypeVarchar, mysql.TypeString}
	schema := buildSchema(names, ftypes)

	// Compose a mocked session manager.
	ps := make([]util.ProcessInfo, 0, 1)
	pi := util.ProcessInfo{
		ID:      0,
		User:    "test",
		Host:    "127.0.0.1",
		DB:      "test",
		Command: "select * from t",
		State:   1,
		Info:    "",
	}
	ps = append(ps, pi)
	sm := &mockSessionManager{
		PS: ps,
	}
	sctx := mock.NewContext()
	sctx.SetSessionManager(sm)

	// Compose executor.
	e := &ShowExec{
		baseExecutor: newBaseExecutor(sctx, schema, ""),
		Tp:           ast.ShowProcessList,
	}

	ctx := context.Background()
	err := e.Open(ctx)
	c.Assert(err, IsNil)

	chk := e.newChunk()
	it := chunk.NewIterator4Chunk(chk)
	// Run test and check results.
	for _, p := range ps {
		err = e.NextChunk(context.Background(), chk)
		c.Assert(err, IsNil)
		for row := it.Begin(); row != it.End(); row = it.Next() {
			c.Assert(row.GetUint64(0), Equals, p.ID)
		}
	}
	err = e.NextChunk(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(chk.NumRows(), Equals, 0)
	err = e.Close()
	c.Assert(err, IsNil)
}

func buildSchema(names []string, ftypes []byte) *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	for i := range names {
		col := &expression.Column{
			ColName: model.NewCIStr(names[i]),
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

func (s *testExecSuite) TestBuildKvRangesForIndexJoin(c *C) {
	indexRanges := make([]*ranger.NewRange, 0, 6)
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 2, 1, 2))
	indexRanges = append(indexRanges, generateIndexRange(1, 1, 3, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 1, 1, 1))
	indexRanges = append(indexRanges, generateIndexRange(2, 1, 2, 1, 1))

	joinKeyRows := make([][]types.Datum, 0, 5)
	joinKeyRows = append(joinKeyRows, generateDatumSlice(1, 1))
	joinKeyRows = append(joinKeyRows, generateDatumSlice(1, 2))
	joinKeyRows = append(joinKeyRows, generateDatumSlice(2, 1))
	joinKeyRows = append(joinKeyRows, generateDatumSlice(2, 2))
	joinKeyRows = append(joinKeyRows, generateDatumSlice(2, 3))

	keyOff2IdxOff := []int{1, 3}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	kvRanges, err := buildKvRangesForIndexJoin(sc, 0, 0, joinKeyRows, indexRanges, keyOff2IdxOff)
	c.Assert(err, IsNil)
	// Check the kvRanges is in order.
	for i, kvRange := range kvRanges {
		c.Assert(kvRange.StartKey.Cmp(kvRange.EndKey) < 0, IsTrue)
		if i > 0 {
			c.Assert(kvRange.StartKey.Cmp(kvRanges[i-1].EndKey) >= 0, IsTrue)
		}
	}
}

func generateIndexRange(vals ...int64) *ranger.NewRange {
	lowDatums := generateDatumSlice(vals...)
	highDatums := make([]types.Datum, len(vals))
	copy(highDatums, lowDatums)
	return &ranger.NewRange{LowVal: lowDatums, HighVal: highDatums}
}

func generateDatumSlice(vals ...int64) []types.Datum {
	datums := make([]types.Datum, len(vals))
	for i, val := range vals {
		datums[i].SetInt64(val)
	}
	return datums
}
