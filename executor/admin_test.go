// Copyright 2018 PingCAP, Inc.
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

package executor_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"golang.org/x/net/context"
)

func (s *testSuite) TestAdminCheckIndexRange(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists check_index_test;`)
	tk.MustExec(`create table check_index_test (a int, b varchar(10), index a_b (a, b), index b (b))`)
	tk.MustExec(`insert check_index_test values (3, "ab"),(2, "cd"),(1, "ef"),(-1, "hi")`)
	result := tk.MustQuery("admin check index check_index_test a_b (2, 4);")
	result.Check(testkit.Rows("1 ef 3", "2 cd 2"))

	result = tk.MustQuery("admin check index check_index_test a_b (3, 5);")
	result.Check(testkit.Rows("-1 hi 4", "1 ef 3"))

	tk.MustExec("use mysql")
	result = tk.MustQuery("admin check index test.check_index_test a_b (2, 3), (4, 5);")
	result.Check(testkit.Rows("-1 hi 4", "2 cd 2"))
}

func findIndexByName(idxName string, indices []*model.IndexInfo) *model.IndexInfo {
	for _, idx := range indices {
		if idx.Name.L == idxName {
			return idx
		}
	}
	return nil
}

func (s *testSuite) TestAdminRecoverIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (NULL, NULL)")

	r := tk.MustQuery("admin recover index admin_test c1")
	r.Check(testkit.Rows("0 3"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("0 3"))

	tk.MustExec("admin check index admin_test c1")
	tk.MustExec("admin check index admin_test c2")

	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key(c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 1), (2, 2), (3, 3), (10, 10), (20, 20)")
	// pk is handle, no additional unique index, no way to recover
	_, err := tk.Exec("admin recover index admin_test c1")
	// err:index is not found
	c.Assert(err, NotNil)

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("0 5"))
	tk.MustExec("admin check index admin_test c2")

	// Make some corrupted index.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo := findIndexByName("c2", tblInfo.Indices)
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sc := s.ctx.GetSessionVars().StmtCtx
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(1), 1)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err, NotNil)
	c.Assert(executor.ErrAdminCheckTable.Equal(err), IsTrue)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("4"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("1 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("5"))
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(10), 10)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)
	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("1 5"))
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")

	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(1), 1)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(2), 2)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(3), 3)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(10), 10)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(20), 20)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("0"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test")
	r.Check(testkit.Rows("5"))

	r = tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("5 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("5"))

	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")
}

func (s *testSuite) TestAdminRecoverIndex1(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	sc := s.ctx.GetSessionVars().StmtCtx
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 varchar(255), c2 int, c3 int default 1, primary key(c1), unique key(c2))")
	tk.MustExec("insert admin_test (c1, c2) values ('1', 1), ('2', 2), ('3', 3), ('10', 10), ('20', 20)")

	r := tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("5"))

	is := s.domain.InfoSchema()
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo := findIndexByName("primary", tblInfo.Indices)
	c.Assert(idxInfo, NotNil)
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums("1"), 1)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums("2"), 2)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums("3"), 3)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums("10"), 4)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("admin recover index admin_test `primary`")
	r.Check(testkit.Rows("4 5"))

	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("5"))

	tk.MustExec("admin check table admin_test")
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check index admin_test `primary`")
}

func (s *testSuite) TestAdminCleanupIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, primary key (c1), unique key(c2), key (c3))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 2), (3, 4), (-5, NULL)")
	tk.MustExec("insert admin_test (c1, c3) values (7, 100), (9, 100), (11, NULL)")

	// pk is handle, no need to cleanup
	_, err := tk.Exec("admin cleanup index admin_test `primary`")
	c.Assert(err, NotNil)
	r := tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("0"))
	r = tk.MustQuery("admin cleanup index admin_test c3")
	r.Check(testkit.Rows("0"))

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo2 := findIndexByName("c2", tblInfo.Indices)
	indexOpr2 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo2)
	idxInfo3 := findIndexByName("c3", tblInfo.Indices)
	indexOpr3 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo3)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(1), -100)
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(6), 100)
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(8), 100)
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(nil), 101)
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(nil), 102)
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn, types.MakeDatums(6), 200)
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn, types.MakeDatums(6), -200)
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn, types.MakeDatums(8), -200)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("11"))
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("5"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("6"))
	tk.MustExec("admin check index admin_test c2")

	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c3")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
	r.Check(testkit.Rows("9"))
	r = tk.MustQuery("admin cleanup index admin_test c3")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
	r.Check(testkit.Rows("6"))
	tk.MustExec("admin check index admin_test c3")

	tk.MustExec("admin check table admin_test")
}

func (s *testSuite) TestAdminCleanupIndexPKNotHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int, primary key (c1, c2))")
	tk.MustExec("insert admin_test (c1, c2) values (1, 2), (3, 4), (-5, 5)")

	r := tk.MustQuery("admin cleanup index admin_test `primary`")
	r.Check(testkit.Rows("0"))

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo := findIndexByName("primary", tblInfo.Indices)
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(7, 10), -100)
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(4, 6), 100)
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(-7, 4), 101)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test `primary`")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("6"))
	r = tk.MustQuery("admin cleanup index admin_test `primary`")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(`primary`)")
	r.Check(testkit.Rows("3"))
	tk.MustExec("admin check index admin_test `primary`")
	tk.MustExec("admin check table admin_test")
}

func (s *testSuite) TestAdminCleanupIndexMore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, unique key (c1, c2), key (c2))")
	tk.MustExec("insert admin_test values (1, 2), (3, 4), (5, 6)")

	tk.MustExec("admin cleanup index admin_test c1")
	tk.MustExec("admin cleanup index admin_test c2")

	// Make some dangling index.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)

	tblInfo := tbl.Meta()
	idxInfo1 := findIndexByName("c1", tblInfo.Indices)
	indexOpr1 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo1)
	idxInfo2 := findIndexByName("c2", tblInfo.Indices)
	indexOpr2 := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo2)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	for i := 0; i < 2000; i++ {
		c1 := int64(2*i + 7)
		c2 := int64(2*i + 8)
		_, err = indexOpr1.Create(s.ctx, txn, types.MakeDatums(c1, c2), c1)
		c.Assert(err, IsNil)
		_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(c2), c1)
		c.Assert(err, IsNil)
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c1")
	c.Assert(err, NotNil)
	err = tk.ExecToErr("admin check index admin_test c2")
	c.Assert(err, NotNil)
	r := tk.MustQuery("SELECT COUNT(*) FROM admin_test")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c1)")
	r.Check(testkit.Rows("2003"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("2003"))
	r = tk.MustQuery("admin cleanup index admin_test c1")
	r.Check(testkit.Rows("2000"))
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("2000"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c1)")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("3"))
	tk.MustExec("admin check index admin_test c1")
	tk.MustExec("admin check index admin_test c2")
	tk.MustExec("admin check table admin_test")
}

func (s *testSuite) TestAdminCheckTableFailed(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 varchar(255) default '1', primary key(c1), key(c3), unique key(c2), key(c2, c3))")
	tk.MustExec("insert admin_test (c1, c2, c3) values (-10, -20, 'y'), (-1, -10, 'z'), (1, 11, 'a'), (2, 12, 'b'), (5, 15, 'c'), (10, 20, 'd'), (20, 30, 'e')")

	// Make some corrupted index. Build the index information.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[1]
	indexOpr := tables.NewIndex(tblInfo.ID, tblInfo, idxInfo)
	sc := s.ctx.GetSessionVars().StmtCtx
	tk.Se.GetSessionVars().IndexLookupSize = 3
	tk.Se.GetSessionVars().MaxChunkSize = 3

	// Reduce one row of index.
	// Table count > index count.
	// Index c2 is missing 11.
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(-10), -1)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err.Error(), Equals,
		"[executor:8003]admin_test err:[admin:1]index:<nil> != record:&admin.RecordData{Handle:-1, Values:[]types.Datum{types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:-10, b:[]uint8(nil), x:interface {}(nil)}}}")
	c.Assert(executor.ErrAdminCheckTable.Equal(err), IsTrue)
	r := tk.MustQuery("admin recover index admin_test c2")
	r.Check(testkit.Rows("1 7"))
	tk.MustExec("admin check table admin_test")

	// Add one row of index.
	// Table count < index count.
	// Index c2 has one more values ​​than table data: 0, and the handle 0 hasn't correlative record.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(0), 0)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err.Error(), Equals, "handle 0, index:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:0, b:[]uint8(nil), x:interface {}(nil)} != record:<nil>")

	// Add one row of index.
	// Table count < index count.
	// Index c2 has two more values ​​than table data: 10, 13, and these handles have correlative record.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(0), 0)
	c.Assert(err, IsNil)
	// Make sure the index value "19" is smaller "21". Then we scan to "19" before "21".
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(19), 10)
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(13), 2)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err.Error(), Equals, "col c2, handle 2, index:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:13, b:[]uint8(nil), x:interface {}(nil)} != record:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:12, b:[]uint8(nil), x:interface {}(nil)}")

	// Table count = index count.
	// Two indices have the same handle.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(13), 2)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(12), 2)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err.Error(), Equals, "col c2, handle 10, index:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:19, b:[]uint8(nil), x:interface {}(nil)} != record:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:20, b:[]uint8(nil), x:interface {}(nil)}")

	// Table count = index count.
	// Index c2 has one line of data is 19, the corresponding table data is 20.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(12), 2)
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(20), 10)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	err = tk.ExecToErr("admin check table admin_test")
	c.Assert(err.Error(), Equals, "col c2, handle 10, index:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:19, b:[]uint8(nil), x:interface {}(nil)} != record:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:20, b:[]uint8(nil), x:interface {}(nil)}")

	// Recover records.
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(19), 10)
	c.Assert(err, IsNil)
	_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(20), 10)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	tk.MustExec("admin check table admin_test")
}

func (s *testSuite) TestAdminCheckPartitionTableFailed(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test_p")
	tk.MustExec("set @@tidb_enable_table_partition = 1")
	tk.MustExec("create table admin_test_p (c1 int key,c2 int,c3 int,index idx(c2)) partition by range (c1) (" +
		"partition p0 values less than (1)," +
		"partition p1 values less than (2)," +
		"partition p2 values less than (3)," +
		"partition p3 values less than (4)," +
		"partition p4 values less than (5)," +
		"partition p5 values less than (maxvalue))")
	tk.MustExec("insert admin_test_p (c1, c2, c3) values (0,0,0), (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)")
	tk.MustExec("admin check table admin_test_p")

	// Make some corrupted index. Build the index information.
	s.ctx = mock.NewContext()
	s.ctx.Store = s.store
	is := s.domain.InfoSchema()
	dbName := model.NewCIStr("test")
	tblName := model.NewCIStr("admin_test_p")
	tbl, err := is.TableByName(dbName, tblName)
	c.Assert(tbl, NotNil)
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	idxInfo := tblInfo.Indices[0]
	sc := s.ctx.GetSessionVars().StmtCtx
	tk.Se.GetSessionVars().IndexLookupSize = 3
	tk.Se.GetSessionVars().MaxChunkSize = 3

	// Reduce one row of index on partitions.
	// Table count > index count.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		err = indexOpr.Delete(sc, txn, types.MakeDatums(i), int64(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		err = tk.ExecToErr("admin check table admin_test_p")
		c.Assert(err.Error(), Equals, fmt.Sprintf("[executor:8003]admin_test_p err:[admin:1]index:<nil> != record:&admin.RecordData{Handle:%d, Values:[]types.Datum{types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:%d, b:[]uint8(nil), x:interface {}(nil)}}}", i, i))
		c.Assert(executor.ErrAdminCheckTable.Equal(err), IsTrue)
		// TODO: fix admin recover for partition table.
		//r := tk.MustQuery("admin recover index admin_test_p idx")
		//r.Check(testkit.Rows("0 0"))
		//tk.MustExec("admin check table admin_test_p")
		// Manual recover index.
		txn, err = s.store.Begin()
		c.Assert(err, IsNil)
		_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(i), int64(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test_p")
	}

	// Add one row of index on partitions.
	// Table count < index count.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(i+8), int64(i+8))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		err = tk.ExecToErr("admin check table admin_test_p")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("handle %d, index:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:%d, b:[]uint8(nil), x:interface {}(nil)} != record:<nil>", i+8, i+8))
		// TODO: fix admin recover for partition table.
		txn, err = s.store.Begin()
		c.Assert(err, IsNil)
		err = indexOpr.Delete(sc, txn, types.MakeDatums(i+8), int64(i+8))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test_p")
	}

	// Table count = index count, but the index value was wrong.
	for i := 0; i <= 5; i++ {
		partitionIdx := i % len(tblInfo.GetPartitionInfo().Definitions)
		indexOpr := tables.NewIndex(tblInfo.GetPartitionInfo().Definitions[partitionIdx].ID, tblInfo, idxInfo)
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)
		_, err = indexOpr.Create(s.ctx, txn, types.MakeDatums(i+8), int64(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
		err = tk.ExecToErr("admin check table admin_test_p")
		c.Assert(err, NotNil)
		c.Assert(err.Error(), Equals, fmt.Sprintf("col c2, handle %d, index:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:%d, b:[]uint8(nil), x:interface {}(nil)} != record:types.Datum{k:0x1, collation:0x0, decimal:0x0, length:0x0, i:%d, b:[]uint8(nil), x:interface {}(nil)}", i, i+8, i))
		// TODO: fix admin recover for partition table.
		txn, err = s.store.Begin()
		c.Assert(err, IsNil)
		err = indexOpr.Delete(sc, txn, types.MakeDatums(i+8), int64(i))
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		tk.MustExec("admin check table admin_test_p")
	}
}

func (s *testSuite) TestAdminCheckTable(c *C) {
	// test NULL value.
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_null (
		a int(11) NOT NULL,
		c int(11) NOT NULL,
		PRIMARY KEY (a, c),
		KEY idx_a (a)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin`)

	tk.MustExec(`insert into test_null(a, c) values(2, 2);`)
	tk.MustExec(`ALTER TABLE test_null ADD COLUMN b int NULL DEFAULT '1795454803' AFTER a;`)
	tk.MustExec(`ALTER TABLE test_null add index b(b);`)
	tk.MustExec("ADMIN CHECK TABLE test_null")

	// Fix unflatten issue in CheckExec.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`create table test (
		a time,
 		PRIMARY KEY (a)
 		);`)

	tk.MustExec(`insert into test set a='12:10:36';`)
	tk.MustExec(`admin check table test`)

	// Test decimal
	tk.MustExec(`drop table if exists test`)
	tk.MustExec("CREATE TABLE test (  a decimal, PRIMARY KEY (a));")
	tk.MustExec("insert into test set a=10;")
	tk.MustExec("admin check table test;")

	// Test timestamp type check table.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`create table test ( a  TIMESTAMP, primary key(a) );`)
	tk.MustExec(`insert into test set a='2015-08-10 04:18:49';`)
	tk.MustExec(`admin check table test;`)

	// Test partitioned table.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`set @@tidb_enable_table_partition = 1`)
	tk.MustExec(`create table test (
		      a int not null,
		      c int not null,
		      primary key (a, c),
		      key idx_a (a)) partition by range (c) (
		      partition p1 values less than (1),
		      partition p2 values less than (4),
		      partition p3 values less than (7),
		      partition p4 values less than (11))`)
	for i := 1; i <= 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into test values (%d, %d);", i, i))
	}
	tk.MustExec(`admin check table test;`)

	// Test index in virtual generated column.
	tk.MustExec(`drop table if exists test`)
	tk.MustExec(`create table test ( b json , c int as (JSON_EXTRACT(b,'$.d')), index idxc(c));`)
	tk.MustExec(`INSERT INTO test set b='{"d": 100}';`)
	tk.MustExec(`admin check table test;`)
	// Test prefix index.
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (
	  			ID CHAR(32) NOT NULL,
	  			name CHAR(32) NOT NULL,
	  			value CHAR(255),
	  			INDEX indexIDname (ID(8),name(8)));`)
	tk.MustExec(`INSERT INTO t VALUES ('keyword','urlprefix','text/ /text');`)
	tk.MustExec(`admin check table t;`)

	tk.MustExec("use mysql")
	tk.MustExec(`admin check table test.t;`)
	err := tk.ExecToErr("admin check table t")
	c.Assert(err, NotNil)

	// test add index on time type column which have default value
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`CREATE TABLE t1 (c2 YEAR, PRIMARY KEY (c2))`)
	tk.MustExec(`INSERT INTO t1 SET c2 = '1912'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c3 TIMESTAMP NULL DEFAULT '1976-08-29 16:28:11'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c4 DATE      NULL DEFAULT '1976-08-29'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c5 TIME      NULL DEFAULT '16:28:11'`)
	tk.MustExec(`ALTER TABLE t1 ADD COLUMN c6 YEAR      NULL DEFAULT '1976'`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx1 (c2, c3,c4,c5,c6)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx2 (c2)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx3 (c3)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx4 (c4)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx5 (c5)`)
	tk.MustExec(`ALTER TABLE t1 ADD INDEX idx6 (c6)`)
	tk.MustExec(`admin check table t1`)

	// Test add index on decimal column.
	tk.MustExec(`drop table if exists td1;`)
	tk.MustExec(`CREATE TABLE td1 (c2 INT NULL DEFAULT '70');`)
	tk.MustExec(`INSERT INTO td1 SET c2 = '5';`)
	tk.MustExec(`ALTER TABLE td1 ADD COLUMN c4 DECIMAL(12,8) NULL DEFAULT '213.41598062';`)
	tk.MustExec(`ALTER TABLE td1 ADD INDEX id2 (c4) ;`)
	tk.MustExec(`ADMIN CHECK TABLE td1;`)

	// Test add not null column, then add index.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1 (a int);`)
	tk.MustExec(`insert into t1 set a=2;`)
	tk.MustExec(`alter table t1 add column b timestamp not null;`)
	tk.MustExec(`alter table t1 add index(b);`)
	tk.MustExec(`admin check table t1;`)

	// Test for index with change decimal precision.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec(`create table t1 (a decimal(2,1), index(a))`)
	tk.MustExec(`insert into t1 set a='1.9'`)
	err = tk.ExecToErr(`alter table t1 modify column a decimal(3,2);`)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:203]unsupported modify column can't change decimal column precision")
	tk.MustExec(`delete from t1;`)
	tk.MustExec(`admin check table t1;`)
}

func (s *testSuite) TestAdminCheckPrimaryIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint unsigned primary key, b int, c int, index idx(a, b));")
	tk.MustExec("insert into t values(1, 1, 1), (9223372036854775807, 2, 2);")
	tk.MustExec("admin check index t idx;")
}

func (s *testSuite) TestAdminShowNextID(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	step := int64(10)
	autoIDStep := autoid.GetStep()
	autoid.SetStep(step)
	defer autoid.SetStep(autoIDStep)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, c int)")
	// Start handle is 1.
	r := tk.MustQuery("admin show t next_row_id")
	r.Check(testkit.Rows("test t _tidb_rowid 1"))
	// Row ID is step + 1.
	tk.MustExec("insert into t values(1, 1)")
	r = tk.MustQuery("admin show t next_row_id")
	r.Check(testkit.Rows("test t _tidb_rowid 11"))
	// Row ID is original + step.
	for i := 0; i < int(step); i++ {
		tk.MustExec("insert into t values(10000, 1)")
	}
	r = tk.MustQuery("admin show t next_row_id")
	r.Check(testkit.Rows("test t _tidb_rowid 21"))

	// test for a table with the primary key
	tk.MustExec("create table tt(id int primary key auto_increment, c int)")
	// Start handle is 1.
	r = tk.MustQuery("admin show tt next_row_id")
	r.Check(testkit.Rows("test tt id 1"))
	// After rebasing auto ID, row ID is 20 + step + 1.
	tk.MustExec("insert into tt values(20, 1)")
	r = tk.MustQuery("admin show tt next_row_id")
	r.Check(testkit.Rows("test tt id 31"))
	// test for renaming the table
	tk.MustExec("create database test1")
	tk.MustExec("rename table test.tt to test1.tt")
	tk.MustExec("use test1")
	r = tk.MustQuery("admin show tt next_row_id")
	r.Check(testkit.Rows("test1 tt id 31"))
	tk.MustExec("insert test1.tt values ()")
	r = tk.MustQuery("admin show tt next_row_id")
	r.Check(testkit.Rows("test1 tt id 41"))
}
