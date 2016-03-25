// Copyright 2015 PingCAP, Inc.
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
	"strings"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/inspectkv"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
}

func (s *testSuite) SetUpSuite(c *C) {
	store, err := tidb.NewStore("memory://test/test")
	c.Assert(err, IsNil)
	s.store = store
}

func (s *testSuite) TearDownSuite(c *C) {
	s.store.Close()
}

func (s *testSuite) TestAdmin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")
	r, err := tk.Exec("admin show ddl")
	c.Assert(err, IsNil)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 6)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	ddlInfo, err := inspectkv.GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetInt64(), Equals, ddlInfo.SchemaVer)
	rowOwnerInfos := strings.Split(row.Data[1].GetString(), ",")
	ownerInfos := strings.Split(ddlInfo.Owner.String(), ",")
	c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	c.Assert(row.Data[2].GetString(), Equals, "")
	bgInfo, err := inspectkv.GetBgDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.Data[3].GetInt64(), Equals, bgInfo.SchemaVer)
	rowOwnerInfos = strings.Split(row.Data[4].GetString(), ",")
	ownerInfos = strings.Split(bgInfo.Owner.String(), ",")
	c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	c.Assert(row.Data[5].GetString(), Equals, "")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)

	// check table test
	tk.MustExec("create table admin_test1 (c1 int, c2 int default 1, index (c1))")
	tk.MustExec("insert admin_test1 (c1) values (21),(22)")
	r, err = tk.Exec("admin check table admin_test, admin_test1")
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	// error table name
	r, err = tk.Exec("admin check table admin_test_error")
	c.Assert(err, NotNil)
	// different index values
	domain, err := domain.NewDomain(s.store, 1*time.Second)
	c.Assert(err, IsNil)
	is := domain.InfoSchema()
	c.Assert(is, NotNil)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	c.Assert(tb.Indices(), HasLen, 1)
	err = tb.Indices()[0].X.Create(txn, types.MakeDatums(int64(10)), 1)
	c.Assert(err, IsNil)
	err = txn.Commit()
	c.Assert(err, IsNil)
	r, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
}

func (s *testSuite) TestPrepared(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists prepare_test")
	tk.MustExec("create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1)")
	tk.MustExec("insert prepare_test (c1) values (1),(2),(NULL)")

	tk.MustExec(`prepare stmt_test_1 from 'select id from prepare_test where id > ?'; set @a = 1; execute stmt_test_1 using @a;`)
	tk.MustExec(`prepare stmt_test_2 from 'select 1'`)
	// Prepare multiple statement is not allowed.
	_, err := tk.Exec(`prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'`)
	c.Assert(executor.ErrPrepareMulti.Equal(err), IsTrue)
	// The variable count does not match.
	_, err = tk.Exec(`prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?'; set @a = 1; execute stmt_test_4 using @a;`)
	c.Assert(executor.ErrWrongParamCount.Equal(err), IsTrue)
	// Prepare and deallocate prepared statement immediately.
	tk.MustExec(`prepare stmt_test_5 from 'select id from prepare_test where id > ?'; deallocate prepare stmt_test_5;`)

	// Statement not found.
	_, err = tk.Exec("deallocate prepare stmt_test_5")
	c.Assert(executor.ErrStmtNotFound.Equal(err), IsTrue)

	// The `stmt_test5` should not be found.
	_, err = tk.Exec(`set @a = 1; execute stmt_test_5 using @a;`)
	c.Assert(executor.ErrStmtNotFound.Equal(err), IsTrue)

	// Use parameter marker with argument will run prepared statement.
	result := tk.MustQuery("select distinct c1, c2 from prepare_test where c1 = ?", 1)
	result.Check([][]interface{}{{1, nil}})

	// Call Session PrepareStmt directly to get stmtId.
	stmtId, _, _, err := tk.Se.PrepareStmt("select c1, c2 from prepare_test where c1 = ?")
	c.Assert(err, IsNil)
	_, err = tk.Se.ExecutePreparedStmt(stmtId, 1)
	c.Assert(err, IsNil)

	// Make schema change.
	tk.Exec("create table prepare2 (a int)")

	// Should success as the changed schema do not affect the prepared statement.
	_, err = tk.Se.ExecutePreparedStmt(stmtId, 1)
	c.Assert(err, IsNil)

	// Drop a column so the prepared statement become invalid.
	tk.MustExec("alter table prepare_test drop column c2")

	// There should be schema changed error.
	_, err = tk.Se.ExecutePreparedStmt(stmtId, 1)
	c.Assert(executor.ErrSchemaChanged.Equal(err), IsTrue)

	// Coverage.
	exec := &executor.ExecuteExec{}
	exec.Fields()
	exec.Next()
	exec.Close()
}

func (s *testSuite) fillData(tk *testkit.TestKit, table string) {
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("create table %s(id int not null default 1, name varchar(255), PRIMARY KEY(id));", table))

	// insert data
	tk.MustExec(fmt.Sprintf("insert INTO %s VALUES (1, \"hello\");", table))
	tk.CheckExecResult(1, 0)
	tk.MustExec(fmt.Sprintf("insert into %s values (2, \"hello\");", table))
	tk.CheckExecResult(1, 0)
}

func (s *testSuite) TestDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.fillData(tk, "delete_test")

	tk.MustExec(`update delete_test set name = "abc" where id = 2;`)
	tk.CheckExecResult(1, 0)

	tk.MustExec(`delete from delete_test where id = 2 limit 1;`)
	tk.CheckExecResult(1, 0)

	// Test delete with false condition
	tk.MustExec(`delete from delete_test where 0;`)
	tk.CheckExecResult(0, 0)

	tk.MustExec("insert into delete_test values (2, 'abc')")
	tk.MustExec(`delete from delete_test where delete_test.id = 2 limit 1`)
	tk.CheckExecResult(1, 0)

	// Select data
	tk.MustExec("begin")
	rows := tk.MustQuery(`SELECT * from delete_test limit 2;`)
	rowStr := fmt.Sprintf("%v %v", "1", []byte("hello"))
	rows.Check(testkit.Rows(rowStr))
	tk.MustExec("commit")

	tk.MustExec(`delete from delete_test ;`)
	tk.CheckExecResult(1, 0)
}

func (s *testSuite) fillDataMultiTable(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	// Create and fill table t1
	tk.MustExec("create table t1 (id int, data int);")
	tk.MustExec("insert into t1 values (11, 121), (12, 122), (13, 123);")
	tk.CheckExecResult(3, 0)
	// Create and fill table t2
	tk.MustExec("create table t2 (id int, data int);")
	tk.MustExec("insert into t2 values (11, 221), (22, 222), (23, 223);")
	tk.CheckExecResult(3, 0)
	// Create and fill table t3
	tk.MustExec("create table t3 (id int, data int);")
	tk.MustExec("insert into t3 values (11, 321), (22, 322), (23, 323);")
	tk.CheckExecResult(3, 0)
}

func (s *testSuite) TestMultiTableDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	s.fillDataMultiTable(tk)

	tk.MustExec(`delete t1, t2 from t1 inner join t2 inner join t3 where t1.id=t2.id and t2.id=t3.id;`)
	tk.CheckExecResult(2, 0)

	// Select data
	r := tk.MustQuery("select * from t3")
	c.Assert(r.Rows(), HasLen, 3)
}

func (s *testSuite) TestQualifedDelete(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (c1 int, c2 int, index (c1))")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert into t1 values (1, 1), (2, 2)")

	// delete with index
	tk.MustExec("delete from t1 where t1.c1 = 1")
	tk.CheckExecResult(1, 0)

	// delete with no index
	tk.MustExec("delete from t1 where t1.c2 = 2")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("select * from t1")
	c.Assert(r.Rows(), HasLen, 0)

	_, err := tk.Exec("delete from t1 as a where a.c1 = 1")
	c.Assert(err, NotNil)

	tk.MustExec("insert into t1 values (1, 1), (2, 2)")
	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete t1, t2 from t1 join t2 where t1.c1 = t2.c2")
	tk.CheckExecResult(3, 0)

	tk.MustExec("insert into t2 values (2, 1), (3,1)")
	tk.MustExec("delete a, b from t1 as a join t2 as b where a.c2 = b.c1")
	tk.CheckExecResult(2, 0)

	_, err = tk.Exec("delete t1, t2 from t1 as a join t2 as b where a.c2 = b.c1")
	c.Assert(err, NotNil)

	tk.MustExec("drop table t1, t2")
}

func (s *testSuite) TestInsert(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists insert_test;create table insert_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `insert insert_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)

	errInsertSelectSQL := `insert insert_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSetSQL := `insert insert_test set c1 = 3;`
	tk.MustExec(insertSetSQL)

	errInsertSelectSQL = `insert insert_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errInsertSelectSQL = `insert insert_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSelectSQL := `create table insert_test_1 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test;`
	tk.MustExec(insertSelectSQL)

	insertSelectSQL = `create table insert_test_2 (id int, c1 int);`
	tk.MustExec(insertSelectSQL)
	insertSelectSQL = `insert insert_test_1 select id, c1 from insert_test union select id * 10, c1 * 10 from insert_test;`
	tk.MustExec(insertSelectSQL)

	errInsertSelectSQL = `insert insert_test_1 select c1 from insert_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errInsertSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	insertSQL := `insert into insert_test (id, c2) values (1, 1) on duplicate key update c2=10;`
	tk.MustExec(insertSQL)

	insertSQL = `insert into insert_test (id, c2) values (1, 1) on duplicate key update insert_test.c2=10;`
	tk.MustExec(insertSQL)

	_, err = tk.Exec(`insert into insert_test (id, c2) values(1, 1) on duplicate key update t.c2 = 10`)
	c.Assert(err, NotNil)
}

func (s *testSuite) TestInsertAutoInc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	createSQL := `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)

	insertSQL := `insert into insert_autoinc_test(c1) values (1), (2)`
	tk.MustExec(insertSQL)
	tk.MustExec("begin")
	r := tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 := fmt.Sprintf("%v %v", "1", "1")
	rowStr2 := fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (5,5)`
	tk.MustExec(insertSQL)
	insertSQL = `insert into insert_autoinc_test(c1) values (6)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 := fmt.Sprintf("%v %v", "5", "5")
	rowStr4 := fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(id, c1) values (3,3)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 := fmt.Sprintf("%v %v", "3", "3")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4))
	tk.MustExec("commit")

	tk.MustExec("begin")
	insertSQL = `insert into insert_autoinc_test(c1) values (7)`
	tk.MustExec(insertSQL)
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 := fmt.Sprintf("%v %v", "7", "7")
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr5, rowStr3, rowStr4, rowStr6))
	tk.MustExec("commit")

	// issue-962
	createSQL = `drop table if exists insert_autoinc_test; create table insert_autoinc_test (id int primary key auto_increment, c1 int);`
	tk.MustExec(createSQL)
	insertSQL = `insert into insert_autoinc_test(id, c1) values (0.3, 1)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr1 = fmt.Sprintf("%v %v", "1", "1")
	r.Check(testkit.Rows(rowStr1))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (-0.3, 2)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr2 = fmt.Sprintf("%v %v", "2", "2")
	r.Check(testkit.Rows(rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (-3.3, 3)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr3 = fmt.Sprintf("%v %v", "-3", "3")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (4.3, 4)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr4 = fmt.Sprintf("%v %v", "4", "4")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4))
	insertSQL = `insert into insert_autoinc_test(c1) values (5)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr5 = fmt.Sprintf("%v %v", "5", "5")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5))
	insertSQL = `insert into insert_autoinc_test(id, c1) values (null, 6)`
	tk.MustExec(insertSQL)
	r = tk.MustQuery("select * from insert_autoinc_test;")
	rowStr6 = fmt.Sprintf("%v %v", "6", "6")
	r.Check(testkit.Rows(rowStr3, rowStr1, rowStr2, rowStr4, rowStr5, rowStr6))
}

func (s *testSuite) TestReplace(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists replace_test;
    create table replace_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)
	testSQL = `replace replace_test (c1) values (1),(2),(NULL);`
	tk.MustExec(testSQL)

	errReplaceSQL := `replace replace_test (c1) values ();`
	tk.MustExec("begin")
	_, err := tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (c1, c2) values (1,2),(1);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test (xxx) values (3);`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSQL = `replace replace_test_xxx (c1) values ();`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSetSQL := `replace replace_test set c1 = 3;`
	tk.MustExec(replaceSetSQL)

	errReplaceSetSQL := `replace replace_test set c1 = 4, c1 = 5;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	errReplaceSetSQL = `replace replace_test set xxx = 6;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSetSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceSelectSQL := `create table replace_test_1 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test;`
	tk.MustExec(replaceSelectSQL)

	replaceSelectSQL = `create table replace_test_2 (id int, c1 int);`
	tk.MustExec(replaceSelectSQL)
	replaceSelectSQL = `replace replace_test_1 select id, c1 from replace_test union select id * 10, c1 * 10 from replace_test;`
	tk.MustExec(replaceSelectSQL)

	errReplaceSelectSQL := `replace replace_test_1 select c1 from replace_test;`
	tk.MustExec("begin")
	_, err = tk.Exec(errReplaceSelectSQL)
	c.Assert(err, NotNil)
	tk.MustExec("rollback")

	replaceUniqueIndexSQL := `create table replace_test_3 (c1 int, c2 int, UNIQUE INDEX (c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))
	replaceUniqueIndexSQL = `replace into replace_test_3 set c1=1, c2=1;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(2))

	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_3 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replaceUniqueIndexSQL = `create table replace_test_4 (c1 int, c2 int, c3 int, UNIQUE INDEX (c1, c2));`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	replaceUniqueIndexSQL = `replace into replace_test_4 set c2=NULL;`
	tk.MustExec(replaceUniqueIndexSQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	replacePrimaryKeySQL := `create table replace_test_5 (c1 int, c2 int, c3 int, PRIMARY KEY (c1, c2));`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	replacePrimaryKeySQL = `replace into replace_test_5 set c1=1, c2=2;`
	tk.MustExec(replacePrimaryKeySQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(1))

	// For Issue989
	issue989SQL := `CREATE TABLE tIssue989 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue989SQL)
	issue989SQL = `insert into tIssue989 (a, b) values (1, 2);`
	tk.MustExec(issue989SQL)
	issue989SQL = `replace into tIssue989(a, b) values (111, 2);`
	tk.MustExec(issue989SQL)
	r := tk.MustQuery("select * from tIssue989;")
	r.Check(testkit.Rows("111 2"))

	// For Issue1012
	issue1012SQL := `CREATE TABLE tIssue1012 (a int, b int, PRIMARY KEY(a), UNIQUE KEY(b));`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `insert into tIssue1012 (a, b) values (1, 2);`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `insert into tIssue1012 (a, b) values (2, 1);`
	tk.MustExec(issue1012SQL)
	issue1012SQL = `replace into tIssue1012(a, b) values (1, 1);`
	tk.MustExec(issue1012SQL)
	c.Assert(int64(tk.Se.AffectedRows()), Equals, int64(3))
	r = tk.MustQuery("select * from tIssue1012;")
	r.Check(testkit.Rows("1 1"))
}

func (s *testSuite) TestSelectWithoutFrom(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("begin")
	r := tk.MustQuery("select 1 + 2*3")
	r.Check(testkit.Rows("7"))
	tk.MustExec("commit")

	tk.MustExec("begin")
	r = tk.MustQuery(`select _utf8"string";`)
	r.Check(testkit.Rows("string"))
	tk.MustExec("commit")
}

func (s *testSuite) TestSelectOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_order_test")

	tk.MustExec("begin")
	// Test star field
	r := tk.MustQuery("select * from select_order_test where id = 1 order by id limit 1 offset 0;")
	rowStr := fmt.Sprintf("%v %v", 1, []byte("hello"))
	r.Check(testkit.Rows(rowStr))
	tk.MustExec("commit")

	tk.MustExec("begin")
	// Test multiple field
	r = tk.MustQuery("select id, name from select_order_test where id = 1 group by id, name limit 1 offset 0;")
	rowStr = fmt.Sprintf("%v %v", 1, []byte("hello"))
	r.Check(testkit.Rows(rowStr))
	tk.MustExec("commit")

	tk.MustExec("drop table select_order_test")
}

func (s *testSuite) TestSelectDistinct(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_distinct_test")

	tk.MustExec("begin")
	r := tk.MustQuery("select distinct name from select_distinct_test;")
	rowStr := fmt.Sprintf("%v", []byte("hello"))
	r.Check(testkit.Rows(rowStr))
	tk.MustExec("commit")

	tk.MustExec("drop table select_distinct_test")
}

func (s *testSuite) TestSelectHaving(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_having_test")

	tk.MustExec("begin")
	r := tk.MustQuery("select id, name from select_having_test where id in (1,3) having name like 'he%';")
	rowStr := fmt.Sprintf("%v %v", 1, []byte("hello"))
	r.Check(testkit.Rows(rowStr))
	tk.MustExec("commit")

	tk.MustExec("drop table select_having_test")
}

func (s *testSuite) TestSelectErrorRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("begin")
	_, err := tk.Exec("select row(1, 1) from test")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test group by row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test order by row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test having row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select (select 1, 1) from test;")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test group by (select 1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test order by (select 1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test having (select 1, 1);")
	c.Assert(err, NotNil)

	tk.MustExec("commit")
}

func (s *testSuite) TestUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "update_test")

	updateStr := `UPDATE update_test SET name = "abc" where id > 0;`
	tk.MustExec(updateStr)
	tk.CheckExecResult(2, 0)

	// select data
	tk.MustExec("begin")
	r := tk.MustQuery(`SELECT * from update_test limit 2;`)
	rowStr1 := fmt.Sprintf("%v %v", 1, []byte("abc"))
	rowStr2 := fmt.Sprintf("%v %v", 2, []byte("abc"))
	r.Check(testkit.Rows(rowStr1, rowStr2))
	tk.MustExec("commit")

	tk.MustExec(`UPDATE update_test SET name = "foo"`)
	tk.CheckExecResult(2, 0)

	// table option is auto-increment
	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), primary key(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	tk.MustExec("update update_test set id = 8 where name = 'aa'")
	tk.MustExec("insert into update_test(name) values ('bb')")
	tk.MustExec("commit")
	tk.MustExec("begin")
	r = tk.MustQuery("select * from update_test;")
	rowStr1 = fmt.Sprintf("%v %v", 8, []byte("aa"))
	rowStr2 = fmt.Sprintf("%v %v", 9, []byte("bb"))
	r.Check(testkit.Rows(rowStr1, rowStr2))
	tk.MustExec("commit")

	tk.MustExec("begin")
	tk.MustExec("drop table if exists update_test;")
	tk.MustExec("commit")
	tk.MustExec("begin")
	tk.MustExec("create table update_test(id int not null auto_increment, name varchar(255), index(id))")
	tk.MustExec("insert into update_test(name) values ('aa')")
	_, err := tk.Exec("update update_test set id = null where name = 'aa'")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), DeepEquals, "Column 'id' cannot be null")

	tk.MustExec("drop table update_test")
}

func (s *testSuite) fillMultiTableForUpdate(tk *testkit.TestKit) {
	// Create and fill table items
	tk.MustExec("CREATE TABLE items (id int, price TEXT);")
	tk.MustExec(`insert into items values (11, "items_price_11"), (12, "items_price_12"), (13, "items_price_13");`)
	tk.CheckExecResult(3, 0)
	// Create and fill table month
	tk.MustExec("CREATE TABLE month (mid int, mprice TEXT);")
	tk.MustExec(`insert into month values (11, "month_price_11"), (22, "month_price_22"), (13, "month_price_13");`)
	tk.CheckExecResult(3, 0)
}

func (s *testSuite) TestMultipleTableUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillMultiTableForUpdate(tk)

	tk.MustExec(`UPDATE items, month  SET items.price=month.mprice WHERE items.id=month.mid;`)
	tk.MustExec("begin")
	r := tk.MustQuery("SELECT * FROM items")
	rowStr1 := fmt.Sprintf("%v %v", 11, []byte("month_price_11"))
	rowStr2 := fmt.Sprintf("%v %v", 12, []byte("items_price_12"))
	rowStr3 := fmt.Sprintf("%v %v", 13, []byte("month_price_13"))
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("commit")

	// Single-table syntax but with multiple tables
	tk.MustExec(`UPDATE items join month on items.id=month.mid SET items.price=month.mid;`)
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	rowStr1 = fmt.Sprintf("%v %v", 11, []byte("11"))
	rowStr2 = fmt.Sprintf("%v %v", 12, []byte("items_price_12"))
	rowStr3 = fmt.Sprintf("%v %v", 13, []byte("13"))
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("commit")

	// JoinTable with alias table name.
	tk.MustExec(`UPDATE items T0 join month T1 on T0.id=T1.mid SET T0.price=T1.mprice;`)
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM items")
	rowStr1 = fmt.Sprintf("%v %v", 11, []byte("month_price_11"))
	rowStr2 = fmt.Sprintf("%v %v", 12, []byte("items_price_12"))
	rowStr3 = fmt.Sprintf("%v %v", 13, []byte("month_price_13"))
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("commit")
}

// For https://github.com/pingcap/tidb/issues/345
func (s *testSuite) TestIssue345(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1 (c1 int);`)
	tk.MustExec(`create table t2 (c2 int);`)
	tk.MustExec(`insert into t1 values (1);`)
	tk.MustExec(`insert into t2 values (2);`)
	tk.MustExec(`update t1, t2 set t1.c1 = 2, t2.c2 = 1;`)
	tk.MustExec(`update t1, t2 set c1 = 2, c2 = 1;`)
	tk.MustExec(`update t1 as a, t2 as b set a.c1 = 2, b.c2 = 1;`)

	// Check t1 content
	tk.MustExec("begin")
	r := tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("2"))
	tk.MustExec("commit")
	// Check t2 content
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("1"))
	tk.MustExec("commit")

	tk.MustExec(`update t1 as a, t2 as t1 set a.c1 = 1, t1.c2 = 2;`)
	// Check t1 content
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("1"))
	tk.MustExec("commit")
	// Check t2 content
	tk.MustExec("begin")
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("2"))

	_, err := tk.Exec(`update t1 as a, t2 set t1.c1 = 10;`)
	c.Assert(err, NotNil)

	tk.MustExec("commit")
}

func (s *testSuite) TestMultiUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// fix https://github.com/pingcap/tidb/issues/369
	testSQL := `
		DROP TABLE IF EXISTS t1, t2;
		create table t1 (c int);
		create table t2 (c varchar(256));
		insert into t1 values (1), (2);
		insert into t2 values ("a"), ("b");
		update t1, t2 set t1.c = 10, t2.c = "abc";`
	tk.MustExec(testSQL)

	// fix https://github.com/pingcap/tidb/issues/376
	testSQL = `DROP TABLE IF EXISTS t1, t2;
		create table t1 (c1 int);
		create table t2 (c2 int);
		insert into t1 values (1), (2);
		insert into t2 values (1), (2);
		update t1, t2 set t1.c1 = 10, t2.c2 = 2 where t2.c2 = 1;`
	tk.MustExec(testSQL)

	r := tk.MustQuery("select * from t1")
	r.Check(testkit.Rows("10", "10"))
}

func (s *testSuite) TestUnion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	testSQL := `select 1 union select 0;`
	tk.MustExec(testSQL)

	testSQL = `drop table if exists union_test; create table union_test(id int);`
	tk.MustExec(testSQL)

	testSQL = `drop table if exists union_test;`
	tk.MustExec(testSQL)
	testSQL = `create table union_test(id int);`
	tk.MustExec(testSQL)
	testSQL = `insert union_test values (1),(2); select id from union_test union select 1;`
	tk.MustExec(testSQL)

	testSQL = `select id from union_test union select id from union_test;`
	tk.MustExec("begin")
	r := tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1", "2"))

	r = tk.MustQuery("select 1 union all select 1")
	r.Check(testkit.Rows("1", "1"))

	r = tk.MustQuery("select 1 union all select 1 union select 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 union (select 2) limit 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 union (select 2) limit 1, 1")
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery("select id from union_test union all (select 1) order by id desc")
	r.Check(testkit.Rows("2", "1", "1"))

	r = tk.MustQuery("select id as a from union_test union (select 1) order by a desc")
	r.Check(testkit.Rows("2", "1"))

	r = tk.MustQuery(`select null union select "abc"`)
	rowStr1 := fmt.Sprintf("%v", nil)
	r.Check(testkit.Rows(rowStr1, "abc"))

	r = tk.MustQuery(`select "abc" union select 1`)
	r.Check(testkit.Rows("abc", "1"))

	tk.MustExec("commit")
}

func (s *testSuite) TestTablePKisHandleScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int PRIMARY KEY AUTO_INCREMENT)")
	tk.MustExec("insert t values (),()")
	tk.MustExec("insert t values (-100),(0)")

	cases := []struct {
		sql    string
		result [][]interface{}
	}{
		{
			"select * from t",
			testkit.Rows("-100", "1", "2", "3"),
		},
		{
			"select * from t where a = 1",
			testkit.Rows("1"),
		},
		{
			"select * from t where a != 1",
			testkit.Rows("-100", "2", "3"),
		},
		{
			"select * from t where a >= '1.1'",
			testkit.Rows("2", "3"),
		},
		{
			"select * from t where a < '1.1'",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a > '-100.1' and a < 2",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a is null",
			testkit.Rows(),
		}, {
			"select * from t where a is true",
			testkit.Rows("-100", "1", "2", "3"),
		}, {
			"select * from t where a is false",
			testkit.Rows(),
		},
		{
			"select * from t where a in (1, 2)",
			testkit.Rows("1", "2"),
		},
		{
			"select * from t where a between 1 and 2",
			testkit.Rows("1", "2"),
		},
	}

	for _, ca := range cases {
		result := tk.MustQuery(ca.sql)
		result.Check(ca.result)
	}
}

func (s *testSuite) TestJoin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int)")
	tk.MustExec("insert t values (1)")
	cases := []struct {
		sql    string
		result [][]interface{}
	}{
		{
			"select 1 from t as a left join t as b on 0",
			testkit.Rows("1"),
		},
		{
			"select 1 from t as a join t as b on 1",
			testkit.Rows("1"),
		},
	}
	for _, ca := range cases {
		result := tk.MustQuery(ca.sql)
		result.Check(ca.result)
	}
}
