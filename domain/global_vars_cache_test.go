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

package domain

import (
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stmtsummary"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testGVCSuite{})

type testGVCSuite struct{}

func (gvcSuite *testGVCSuite) TestSimple(c *C) {
	defer testleak.AfterTest(c)()
	testleak.BeforeTest()

	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	ddlLease := 50 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, mockFactory)
	err = dom.Init(ddlLease, sysMockFactory)
	c.Assert(err, IsNil)
	defer dom.Close()

	// Get empty global vars cache.
	gvc := dom.GetGlobalVarsCache()
	succ, rows, fields := gvc.Get()
	c.Assert(succ, IsFalse)
	c.Assert(rows, IsNil)
	c.Assert(fields, IsNil)
	// Get a variable from global vars cache.
	rf := getResultField("c", 1, 0)
	rf1 := getResultField("c1", 2, 1)
	ft := &types.FieldType{
		Tp:      mysql.TypeString,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
	ft1 := &types.FieldType{
		Tp:      mysql.TypeString,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
	ck := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft1}, 1024)
	ck.AppendString(0, "variable1")
	ck.AppendString(1, "value1")
	row := ck.GetRow(0)
	gvc.Update([]chunk.Row{row}, []*ast.ResultField{rf, rf1})
	succ, rows, fields = gvc.Get()
	c.Assert(succ, IsTrue)
	c.Assert(rows[0], Equals, row)
	c.Assert(fields, DeepEquals, []*ast.ResultField{rf, rf1})
	// Disable the cache.
	gvc.Disable()
	succ, rows, fields = gvc.Get()
	c.Assert(succ, IsFalse)
	c.Assert(rows[0], Equals, row)
	c.Assert(fields, DeepEquals, []*ast.ResultField{rf, rf1})
}

func getResultField(colName string, id, offset int) *ast.ResultField {
	return &ast.ResultField{
		Column: &model.ColumnInfo{
			Name:   model.NewCIStr(colName),
			ID:     int64(id),
			Offset: offset,
			FieldType: types.FieldType{
				Tp:      mysql.TypeString,
				Charset: charset.CharsetUTF8,
				Collate: charset.CollationUTF8,
			},
		},
		TableAsName: model.NewCIStr("tbl"),
		DBName:      model.NewCIStr("test"),
	}
}

func (gvcSuite *testGVCSuite) TestConcurrentOneFlight(c *C) {
	defer testleak.AfterTest(c)()
	testleak.BeforeTest()
	gvc := &GlobalVariableCache{}
	succ, rows, fields := gvc.Get()
	c.Assert(succ, IsFalse)
	c.Assert(rows, IsNil)
	c.Assert(fields, IsNil)

	// Get a variable from global vars cache.
	rf := getResultField("c", 1, 0)
	rf1 := getResultField("c1", 2, 1)
	ft := &types.FieldType{
		Tp:      mysql.TypeString,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
	ft1 := &types.FieldType{
		Tp:      mysql.TypeString,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
	ckLow := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft1}, 1024)
	val := "fromStorage"
	val1 := "fromStorage1"
	ckLow.AppendString(0, val)
	ckLow.AppendString(1, val1)

	// Let cache become invalid, and try concurrent load
	counter := int32(0)
	waitToStart := new(sync.WaitGroup)
	waitToStart.Add(1)
	gvc.lastModify = time.Now().Add(time.Duration(-10) * time.Second)
	loadFunc := func() ([]chunk.Row, []*ast.ResultField, error) {
		time.Sleep(100 * time.Millisecond)
		atomic.AddInt32(&counter, 1)
		return []chunk.Row{ckLow.GetRow(0)}, []*ast.ResultField{rf, rf1}, nil
	}
	wg := new(sync.WaitGroup)
	worker := 100
	resArray := make([]loadResult, worker)
	for i := 0; i < worker; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			waitToStart.Wait()
			resRow, resField, _ := gvc.LoadGlobalVariables(loadFunc)
			resArray[idx].rows = resRow
			resArray[idx].fields = resField
		}(i)
	}
	waitToStart.Done()
	wg.Wait()
	succ, rows, fields = gvc.Get()
	c.Assert(counter, Equals, int32(1))
	c.Assert(resArray[0].rows[0].GetString(0), Equals, val)
	c.Assert(resArray[0].rows[0].GetString(1), Equals, val1)
	for i := 0; i < worker; i++ {
		c.Assert(resArray[0].rows[0], Equals, resArray[i].rows[0])
		c.Assert(resArray[i].rows[0].GetString(0), Equals, val)
		c.Assert(resArray[i].rows[0].GetString(1), Equals, val1)
	}
	// Validate cache
	c.Assert(succ, IsTrue)
	c.Assert(rows[0], Equals, resArray[0].rows[0])
	c.Assert(fields, DeepEquals, []*ast.ResultField{rf, rf1})
}

func (gvcSuite *testGVCSuite) TestCheckEnableStmtSummary(c *C) {
	defer testleak.AfterTest(c)()
	testleak.BeforeTest()

	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()
	ddlLease := 50 * time.Millisecond
	dom := NewDomain(store, ddlLease, 0, mockFactory)
	err = dom.Init(ddlLease, sysMockFactory)
	c.Assert(err, IsNil)
	defer dom.Close()

	gvc := dom.GetGlobalVarsCache()

	rf := getResultField("c", 1, 0)
	rf1 := getResultField("c1", 2, 1)
	ft := &types.FieldType{
		Tp:      mysql.TypeString,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}
	ft1 := &types.FieldType{
		Tp:      mysql.TypeString,
		Charset: charset.CharsetBin,
		Collate: charset.CollationBin,
	}

	stmtsummary.StmtSummaryByDigestMap.SetEnabled("0", false)
	ck := chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft1}, 1024)
	ck.AppendString(0, variable.TiDBEnableStmtSummary)
	ck.AppendString(1, "1")
	row := ck.GetRow(0)
	gvc.Update([]chunk.Row{row}, []*ast.ResultField{rf, rf1})
	c.Assert(stmtsummary.StmtSummaryByDigestMap.Enabled(), Equals, true)

	ck = chunk.NewChunkWithCapacity([]*types.FieldType{ft, ft1}, 1024)
	ck.AppendString(0, variable.TiDBEnableStmtSummary)
	ck.AppendString(1, "0")
	row = ck.GetRow(0)
	gvc.Update([]chunk.Row{row}, []*ast.ResultField{rf, rf1})
	c.Assert(stmtsummary.StmtSummaryByDigestMap.Enabled(), Equals, false)
}
