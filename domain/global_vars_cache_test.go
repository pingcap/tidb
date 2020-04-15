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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/v4/sessionctx/variable"
	"github.com/pingcap/tidb/v4/store/mockstore"
	"github.com/pingcap/tidb/v4/types"
	"github.com/pingcap/tidb/v4/util/chunk"
	"github.com/pingcap/tidb/v4/util/stmtsummary"
	"github.com/pingcap/tidb/v4/util/testleak"
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
