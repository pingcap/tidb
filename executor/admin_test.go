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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
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
	indexOpr := tables.NewIndex(tblInfo, idxInfo)
	sc := s.ctx.GetSessionVars().StmtCtx
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	err = indexOpr.Delete(sc, txn, types.MakeDatums(1), 1)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	_, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("admin check index admin_test c2")
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

	_, err = tk.Exec("admin check index admin_test c2")
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

	_, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("admin check index admin_test c2")
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
	indexOpr := tables.NewIndex(tblInfo, idxInfo)

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
	indexOpr2 := tables.NewIndex(tblInfo, idxInfo2)
	idxInfo3 := findIndexByName("c3", tblInfo.Indices)
	indexOpr3 := tables.NewIndex(tblInfo, idxInfo3)

	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(1), -100)
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(6), 100)
	c.Assert(err, IsNil)
	_, err = indexOpr2.Create(s.ctx, txn, types.MakeDatums(nil), 101)
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn, types.MakeDatums(6), 200)
	c.Assert(err, IsNil)
	_, err = indexOpr3.Create(s.ctx, txn, types.MakeDatums(6), -200)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	_, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("admin check index admin_test c2")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("9"))
	r = tk.MustQuery("admin cleanup index admin_test c2")
	r.Check(testkit.Rows("3"))
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c2)")
	r.Check(testkit.Rows("6"))
	tk.MustExec("admin check index admin_test c2")

	_, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("admin check index admin_test c3")
	c.Assert(err, NotNil)
	r = tk.MustQuery("SELECT COUNT(*) FROM admin_test USE INDEX(c3)")
	r.Check(testkit.Rows("8"))
	r = tk.MustQuery("admin cleanup index admin_test c3")
	r.Check(testkit.Rows("2"))
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
	indexOpr := tables.NewIndex(tblInfo, idxInfo)

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

	_, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("admin check index admin_test `primary`")
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
	indexOpr1 := tables.NewIndex(tblInfo, idxInfo1)
	idxInfo2 := findIndexByName("c2", tblInfo.Indices)
	indexOpr2 := tables.NewIndex(tblInfo, idxInfo2)

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

	_, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
	_, err = tk.Exec("admin check index admin_test c1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("admin check index admin_test c2")
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
}
