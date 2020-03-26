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

package executor_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/meta/autoid"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

func (s *testSuite3) TestTruncateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists truncate_test;`)
	tk.MustExec(`create table truncate_test (a int)`)
	tk.MustExec(`insert truncate_test values (1),(2),(3)`)
	result := tk.MustQuery("select * from truncate_test")
	result.Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("truncate table truncate_test")
	result = tk.MustQuery("select * from truncate_test")
	result.Check(nil)
}

// TestInTxnExecDDLFail tests the following case:
//  1. Execute the SQL of "begin";
//  2. A SQL that will fail to execute;
//  3. Execute DDL.
func (s *testSuite3) TestInTxnExecDDLFail(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (i int key);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1);")
	_, err := tk.Exec("truncate table t;")
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'PRIMARY'")
	result := tk.MustQuery("select count(*) from t")
	result.Check(testkit.Rows("1"))
}

func (s *testSuite3) TestCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Test create an exist database
	_, err := tk.Exec("CREATE database test")
	c.Assert(err, NotNil)

	// Test create an exist table
	tk.MustExec("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	_, err = tk.Exec("CREATE TABLE create_test (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	c.Assert(err, NotNil)

	// Test "if not exist"
	tk.MustExec("CREATE TABLE if not exists test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// Testcase for https://github.com/pingcap/tidb/issues/312
	tk.MustExec(`create table issue312_1 (c float(24));`)
	tk.MustExec(`create table issue312_2 (c float(25));`)
	rs, err := tk.Exec(`desc issue312_1`)
	c.Assert(err, IsNil)
	ctx := context.Background()
	req := rs.NewChunk()
	it := chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		c.Assert(err1, IsNil)
		if req.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			c.Assert(row.GetString(1), Equals, "float")
		}
	}
	rs, err = tk.Exec(`desc issue312_2`)
	c.Assert(err, IsNil)
	req = rs.NewChunk()
	it = chunk.NewIterator4Chunk(req)
	for {
		err1 := rs.Next(ctx, req)
		c.Assert(err1, IsNil)
		if req.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			c.Assert(req.GetRow(0).GetString(1), Equals, "double")
		}
	}

	// test multiple collate specified in column when create.
	tk.MustExec("drop table if exists test_multiple_column_collate;")
	tk.MustExec("create table test_multiple_column_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	t, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("test_multiple_column_collate"))
	c.Assert(err, IsNil)
	c.Assert(t.Cols()[0].Charset, Equals, "utf8")
	c.Assert(t.Cols()[0].Collate, Equals, "utf8_general_ci")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().Collate, Equals, "utf8mb4_bin")

	tk.MustExec("drop table if exists test_multiple_column_collate;")
	tk.MustExec("create table test_multiple_column_collate (a char(1) charset utf8 collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	t, err = domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("test_multiple_column_collate"))
	c.Assert(err, IsNil)
	c.Assert(t.Cols()[0].Charset, Equals, "utf8")
	c.Assert(t.Cols()[0].Collate, Equals, "utf8_general_ci")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().Collate, Equals, "utf8mb4_bin")

	// test Err case for multiple collate specified in column when create.
	tk.MustExec("drop table if exists test_err_multiple_collate;")
	_, err = tk.Exec("create table test_err_multiple_collate (a char(1) charset utf8mb4 collate utf8_unicode_ci collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ddl.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8_unicode_ci", "utf8mb4").Error())

	tk.MustExec("drop table if exists test_err_multiple_collate;")
	_, err = tk.Exec("create table test_err_multiple_collate (a char(1) collate utf8_unicode_ci collate utf8mb4_general_ci) charset utf8mb4 collate utf8mb4_bin")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ddl.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8mb4_general_ci", "utf8").Error())

	// table option is auto-increment
	tk.MustExec("drop table if exists create_auto_increment_test;")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 999;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('cc')")
	r := tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Rows("999 aa", "1000 bb", "1001 cc"))
	tk.MustExec("drop table create_auto_increment_test")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 1999;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('cc')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Rows("1999 aa", "2000 bb", "2001 cc"))
	tk.MustExec("drop table create_auto_increment_test")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), key(id)) auto_increment = 1000;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	r.Check(testkit.Rows("1000 aa"))
}

func (s *testSuite3) TestCreateView(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	//create an source table
	tk.MustExec("CREATE TABLE source_table (id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	//test create a exist view
	tk.MustExec("CREATE VIEW view_t AS select id , name from source_table")
	defer tk.MustExec("DROP VIEW IF EXISTS view_t")
	_, err := tk.Exec("CREATE VIEW view_t AS select id , name from source_table")
	c.Assert(err.Error(), Equals, "[schema:1050]Table 'test.view_t' already exists")
	//create view on nonexistent table
	_, err = tk.Exec("create view v1 (c,d) as select a,b from t1")
	c.Assert(err.Error(), Equals, "[schema:1146]Table 'test.t1' doesn't exist")
	//simple view
	tk.MustExec("create table t1 (a int ,b int)")
	tk.MustExec("insert into t1 values (1,2), (1,3), (2,4), (2,5), (3,10)")
	//view with colList and SelectFieldExpr
	tk.MustExec("create view v1 (c) as select b+1 from t1")
	//view with SelectFieldExpr
	tk.MustExec("create view v2 as select b+1 from t1")
	//view with SelectFieldExpr and AsName
	tk.MustExec("create view v3 as select b+1 as c from t1")
	//view with colList , SelectField and AsName
	tk.MustExec("create view v4 (c) as select b+1 as d from t1")
	//view with select wild card
	tk.MustExec("create view v5 as select * from t1")
	tk.MustExec("create view v6 (c,d) as select * from t1")
	_, err = tk.Exec("create view v7 (c,d,e) as select * from t1")
	c.Assert(err.Error(), Equals, ddl.ErrViewWrongList.Error())
	//drop multiple views in a statement
	tk.MustExec("drop view v1,v2,v3,v4,v5,v6")
	//view with variable
	tk.MustExec("create view v1 (c,d) as select a,b+@@global.max_user_connections from t1")
	_, err = tk.Exec("create view v1 (c,d) as select a,b from t1 where a = @@global.max_user_connections")
	c.Assert(err.Error(), Equals, "[schema:1050]Table 'test.v1' already exists")
	tk.MustExec("drop view v1")
	//view with different col counts
	_, err = tk.Exec("create view v1 (c,d,e) as select a,b from t1 ")
	c.Assert(err.Error(), Equals, ddl.ErrViewWrongList.Error())
	_, err = tk.Exec("create view v1 (c) as select a,b from t1 ")
	c.Assert(err.Error(), Equals, ddl.ErrViewWrongList.Error())
	//view with or_replace flag
	tk.MustExec("drop view if exists v1")
	tk.MustExec("create view v1 (c,d) as select a,b from t1")
	tk.MustExec("create or replace view v1 (c,d) as select a,b from t1 ")
	tk.MustExec("create table if not exists t1 (a int ,b int)")
	_, err = tk.Exec("create or replace view t1 as select * from t1")
	c.Assert(err.Error(), Equals, ddl.ErrWrongObject.GenWithStackByArgs("test", "t1", "VIEW").Error())
	// create view using prepare
	tk.MustExec(`prepare stmt from "create view v10 (x) as select 1";`)
	tk.MustExec("execute stmt")

	// create view on union
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("drop view if exists v")
	_, err = tk.Exec("create view v as select * from t1 union select * from t2")
	c.Assert(terror.ErrorEqual(err, infoschema.ErrTableNotExists), IsTrue)
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	tk.MustExec("insert into t1 values(1,2), (1,1), (1,2)")
	tk.MustExec("insert into t2 values(1,1),(1,3)")
	tk.MustExec("create definer='root'@'localhost' view v as select * from t1 union select * from t2")
	tk.MustQuery("select * from v").Sort().Check(testkit.Rows("1 1", "1 2", "1 3"))
	tk.MustExec("alter table t1 drop column a")
	_, err = tk.Exec("select * from v")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrViewInvalid), IsTrue)
	tk.MustExec("alter table t1 add column a int")
	tk.MustQuery("select * from v").Sort().Check(testkit.Rows("1 1", "1 3", "<nil> 1", "<nil> 2"))
	tk.MustExec("alter table t1 drop column a")
	tk.MustExec("alter table t2 drop column b")
	_, err = tk.Exec("select * from v")
	c.Assert(terror.ErrorEqual(err, plannercore.ErrViewInvalid), IsTrue)
	tk.MustExec("drop view v")

	tk.MustExec("create view v as (select * from t1)")
	tk.MustExec("drop view v")
	tk.MustExec("create view v as (select * from t1 union select * from t2)")
	tk.MustExec("drop view v")
}

func (s *testSuite3) TestCreateViewWithOverlongColName(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	defer tk.MustExec("drop table t")
	tk.MustExec("create view v as select distinct'" + strings.Repeat("a", 65) + "', " +
		"max('" + strings.Repeat("b", 65) + "'), " +
		"'cccccccccc', '" + strings.Repeat("d", 65) + "';")
	resultCreateStmt := "CREATE ALGORITHM=UNDEFINED DEFINER=``@`` SQL SECURITY DEFINER VIEW `v` (`name_exp_1`, `name_exp_2`, `cccccccccc`, `name_exp_4`) AS SELECT DISTINCT '" + strings.Repeat("a", 65) + "',MAX('" + strings.Repeat("b", 65) + "'),'cccccccccc','" + strings.Repeat("d", 65) + "'"
	tk.MustQuery("select * from v")
	tk.MustQuery("select name_exp_1, name_exp_2, cccccccccc, name_exp_4 from v")
	tk.MustQuery("show create view v").Check(testkit.Rows("v " + resultCreateStmt + "  "))
	tk.MustExec("drop view v;")
	tk.MustExec(resultCreateStmt)

	tk.MustExec("drop view v ")
	tk.MustExec("create definer='root'@'localhost' view v as select 'a', '" + strings.Repeat("b", 65) + "' from t " +
		"union select '" + strings.Repeat("c", 65) + "', " +
		"count(distinct '" + strings.Repeat("b", 65) + "', " +
		"'c');")
	resultCreateStmt = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`, `name_exp_2`) AS SELECT 'a','" + strings.Repeat("b", 65) + "' FROM `test`.`t` UNION SELECT '" + strings.Repeat("c", 65) + "',COUNT(DISTINCT '" + strings.Repeat("b", 65) + "', 'c')"
	tk.MustQuery("select * from v")
	tk.MustQuery("select a, name_exp_2 from v")
	tk.MustQuery("show create view v").Check(testkit.Rows("v " + resultCreateStmt + "  "))
	tk.MustExec("drop view v;")
	tk.MustExec(resultCreateStmt)

	tk.MustExec("drop view v ")
	tk.MustExec("create definer='root'@'localhost' view v as select 'a' as '" + strings.Repeat("b", 65) + "' from t;")
	tk.MustQuery("select * from v")
	tk.MustQuery("select name_exp_1 from v")
	resultCreateStmt = "CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`name_exp_1`) AS SELECT 'a' AS `" + strings.Repeat("b", 65) + "` FROM `test`.`t`"
	tk.MustQuery("show create view v").Check(testkit.Rows("v " + resultCreateStmt + "  "))
	tk.MustExec("drop view v;")
	tk.MustExec(resultCreateStmt)

	tk.MustExec("drop view v ")
	err := tk.ExecToErr("create view v(`" + strings.Repeat("b", 65) + "`) as select a from t;")
	c.Assert(err.Error(), Equals, "[ddl:1059]Identifier name 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' is too long")
}

func (s *testSuite3) TestCreateDropDatabase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists drop_test;")
	tk.MustExec("drop database if exists drop_test;")
	tk.MustExec("create database drop_test;")
	tk.MustExec("use drop_test;")
	tk.MustExec("drop database drop_test;")
	_, err := tk.Exec("drop table t;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())
	err = tk.ExecToErr("select * from t;")
	c.Assert(err.Error(), Equals, plannercore.ErrNoDB.Error())

	_, err = tk.Exec("drop database mysql")
	c.Assert(err, NotNil)
}

func (s *testSuite3) TestCreateDropTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists drop_test (a int)")
	tk.MustExec("drop table if exists drop_test")
	tk.MustExec("create table drop_test (a int)")
	tk.MustExec("drop table drop_test")

	_, err := tk.Exec("drop table mysql.gc_delete_range")
	c.Assert(err, NotNil)
}

func (s *testSuite3) TestCreateDropView(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create or replace view drop_test as select 1,2")

	_, err := tk.Exec("drop table drop_test")
	c.Assert(err.Error(), Equals, "[schema:1051]Unknown table 'test.drop_test'")

	_, err = tk.Exec("drop view if exists drop_test")
	c.Assert(err, IsNil)

	_, err = tk.Exec("drop view mysql.gc_delete_range")
	c.Assert(err.Error(), Equals, "Drop tidb system table 'mysql.gc_delete_range' is forbidden")

	_, err = tk.Exec("drop view drop_test")
	c.Assert(err.Error(), Equals, "[schema:1051]Unknown table 'test.drop_test'")

	tk.MustExec("create table t_v(a int)")
	_, err = tk.Exec("drop view t_v")
	c.Assert(err.Error(), Equals, "[ddl:1347]'test.t_v' is not VIEW")

	tk.MustExec("create table t_v1(a int, b int);")
	tk.MustExec("create table t_v2(a int, b int);")
	tk.MustExec("create view v as select * from t_v1;")
	tk.MustExec("create or replace view v  as select * from t_v2;")
	tk.MustQuery("select * from information_schema.views where table_name ='v';").Check(
		testkit.Rows("def test v SELECT `test`.`t_v2`.`a`,`test`.`t_v2`.`b` FROM `test`.`t_v2` CASCADED NO @ DEFINER utf8mb4 utf8mb4_bin"))
}

func (s *testSuite3) TestCreateDropIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists drop_test (a int)")
	tk.MustExec("create index idx_a on drop_test (a)")
	tk.MustExec("drop index idx_a on drop_test")
	tk.MustExec("drop table drop_test")
}

func (s *testSuite3) TestAlterTableAddColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists alter_test (c1 int)")
	tk.MustExec("insert into alter_test values(1)")
	tk.MustExec("alter table alter_test add column c2 timestamp default current_timestamp")
	time.Sleep(1 * time.Millisecond)
	now := time.Now().Add(-time.Duration(1 * time.Millisecond)).Format(types.TimeFormat)
	r, err := tk.Exec("select c2 from alter_test")
	c.Assert(err, IsNil)
	req := r.NewChunk()
	err = r.Next(context.Background(), req)
	c.Assert(err, IsNil)
	row := req.GetRow(0)
	c.Assert(row.Len(), Equals, 1)
	c.Assert(now, GreaterEqual, row.GetTime(0).String())
	r.Close()
	tk.MustExec("alter table alter_test add column c3 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustQuery("select c3 from alter_test").Check(testkit.Rows("CURRENT_TIMESTAMP"))
	tk.MustExec("create or replace view alter_view as select c1,c2 from alter_test")
	_, err = tk.Exec("alter table alter_view add column c4 varchar(50)")
	c.Assert(err.Error(), Equals, ddl.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustExec("drop view alter_view")
}

func (s *testSuite3) TestAddNotNullColumnNoDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table nn (c1 int)")
	tk.MustExec("insert nn values (1), (2)")
	tk.MustExec("alter table nn add column c2 int not null")

	tbl, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("nn"))
	c.Assert(err, IsNil)
	col2 := tbl.Meta().Columns[1]
	c.Assert(col2.DefaultValue, IsNil)
	c.Assert(col2.OriginDefaultValue, Equals, "0")

	tk.MustQuery("select * from nn").Check(testkit.Rows("1 0", "2 0"))
	_, err = tk.Exec("insert nn (c1) values (3)")
	c.Check(err, NotNil)
	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert nn (c1) values (3)")
	tk.MustQuery("select * from nn").Check(testkit.Rows("1 0", "2 0", "3 0"))
}

func (s *testSuite3) TestAlterTableModifyColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(c1 int, c2 varchar(10), c3 bit)")
	_, err := tk.Exec("alter table mc modify column c1 short")
	c.Assert(err, NotNil)
	tk.MustExec("alter table mc modify column c1 bigint")

	_, err = tk.Exec("alter table mc modify column c2 blob")
	c.Assert(err, NotNil)

	_, err = tk.Exec("alter table mc modify column c2 varchar(8)")
	c.Assert(err, NotNil)
	tk.MustExec("alter table mc modify column c2 varchar(11)")
	tk.MustExec("alter table mc modify column c2 text(13)")
	tk.MustExec("alter table mc modify column c2 text")
	tk.MustExec("alter table mc modify column c3 bit")
	result := tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `c1` bigint(20) DEFAULT NULL,\n  `c2` text DEFAULT NULL,\n  `c3` bit(1) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)
	tk.MustExec("create or replace view alter_view as select c1,c2 from mc")
	_, err = tk.Exec("alter table alter_view modify column c2 text")
	c.Assert(err.Error(), Equals, ddl.ErrWrongObject.GenWithStackByArgs("test", "alter_view", "BASE TABLE").Error())
	tk.MustExec("drop view alter_view")

	// test multiple collate modification in column.
	tk.MustExec("drop table if exists modify_column_multiple_collate")
	tk.MustExec("create table modify_column_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	_, err = tk.Exec("alter table modify_column_multiple_collate modify column a char(1) collate utf8mb4_bin;")
	c.Assert(err, IsNil)
	t, err := domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("modify_column_multiple_collate"))
	c.Assert(err, IsNil)
	c.Assert(t.Cols()[0].Charset, Equals, "utf8mb4")
	c.Assert(t.Cols()[0].Collate, Equals, "utf8mb4_bin")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().Collate, Equals, "utf8mb4_bin")

	tk.MustExec("drop table if exists modify_column_multiple_collate;")
	tk.MustExec("create table modify_column_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	_, err = tk.Exec("alter table modify_column_multiple_collate modify column a char(1) charset utf8mb4 collate utf8mb4_bin;")
	c.Assert(err, IsNil)
	t, err = domain.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("modify_column_multiple_collate"))
	c.Assert(err, IsNil)
	c.Assert(t.Cols()[0].Charset, Equals, "utf8mb4")
	c.Assert(t.Cols()[0].Collate, Equals, "utf8mb4_bin")
	c.Assert(t.Meta().Charset, Equals, "utf8mb4")
	c.Assert(t.Meta().Collate, Equals, "utf8mb4_bin")

	// test Err case for multiple collate modification in column.
	tk.MustExec("drop table if exists err_modify_multiple_collate;")
	tk.MustExec("create table err_modify_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	_, err = tk.Exec("alter table err_modify_multiple_collate modify column a char(1) charset utf8mb4 collate utf8_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ddl.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8_bin", "utf8mb4").Error())

	tk.MustExec("drop table if exists err_modify_multiple_collate;")
	tk.MustExec("create table err_modify_multiple_collate (a char(1) collate utf8_bin collate utf8_general_ci) charset utf8mb4 collate utf8mb4_bin")
	_, err = tk.Exec("alter table err_modify_multiple_collate modify column a char(1) collate utf8_bin collate utf8mb4_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, ddl.ErrCollationCharsetMismatch.GenWithStackByArgs("utf8mb4_bin", "utf8").Error())

}

func (s *testSuite3) TestDefaultDBAfterDropCurDB(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	testSQL := `create database if not exists test_db CHARACTER SET latin1 COLLATE latin1_swedish_ci;`
	tk.MustExec(testSQL)

	testSQL = `use test_db;`
	tk.MustExec(testSQL)
	tk.MustQuery(`select database();`).Check(testkit.Rows("test_db"))
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Rows("latin1"))
	tk.MustQuery(`select @@collation_database;`).Check(testkit.Rows("latin1_swedish_ci"))

	testSQL = `drop database test_db;`
	tk.MustExec(testSQL)
	tk.MustQuery(`select database();`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Rows("utf8"))
	tk.MustQuery(`select @@collation_database;`).Check(testkit.Rows("utf8_unicode_ci"))
}

func (s *testSuite3) TestRenameTable(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func() {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create database rename3")
	tk.MustExec("create table rename1.t (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t values ()")
	tk.MustExec("rename table rename1.t to rename2.t")
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustExec("drop database rename1")
	tk.MustExec("insert rename2.t values ()")
	tk.MustExec("rename table rename2.t to rename3.t")
	tk.MustExec("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Rows("1", "5001", "10001"))
	// Make sure the drop old database doesn't affect the rename3.t's operations.
	tk.MustExec("drop database rename2")
	tk.MustExec("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Rows("1", "5001", "10001", "10002"))
	tk.MustExec("drop database rename3")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create table rename1.t (a int primary key auto_increment)")
	tk.MustExec("rename table rename1.t to rename2.t1")
	tk.MustExec("insert rename2.t1 values ()")
	result := tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Rows("1"))
	// Make sure the drop old database doesn't affect the t1's operations.
	tk.MustExec("drop database rename1")
	tk.MustExec("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Rows("1", "2"))
	// Rename a table to another table in the same database.
	tk.MustExec("rename table rename2.t1 to rename2.t2")
	tk.MustExec("insert rename2.t2 values ()")
	result = tk.MustQuery("select * from rename2.t2")
	result.Check(testkit.Rows("1", "2", "5001"))
	tk.MustExec("drop database rename2")

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create table rename1.t (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t values ()")
	tk.MustExec("rename table rename1.t to rename2.t1")
	// Make sure the value is greater than autoid.step.
	tk.MustExec("insert rename2.t1 values (100000)")
	tk.MustExec("insert rename2.t1 values ()")
	result = tk.MustQuery("select * from rename2.t1")
	result.Check(testkit.Rows("1", "100000", "100001"))
	_, err := tk.Exec("insert rename1.t values ()")
	c.Assert(err, NotNil)
	tk.MustExec("drop database rename1")
	tk.MustExec("drop database rename2")
}

func (s *testSuite3) TestColumnCharsetAndCollate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	dbName := "col_charset_collate"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tests := []struct {
		colType     string
		charset     string
		collates    string
		exptCharset string
		exptCollate string
		errMsg      string
	}{
		{
			colType:     "varchar(10)",
			charset:     "charset utf8",
			collates:    "collate utf8_bin",
			exptCharset: "utf8",
			exptCollate: "utf8_bin",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset utf8mb4",
			collates:    "",
			exptCharset: "utf8mb4",
			exptCollate: "utf8mb4_bin",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset utf16",
			collates:    "",
			exptCharset: "",
			exptCollate: "",
			errMsg:      "Unknown charset utf16",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset latin1",
			collates:    "",
			exptCharset: "latin1",
			exptCollate: "latin1_bin",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset binary",
			collates:    "",
			exptCharset: "binary",
			exptCollate: "binary",
			errMsg:      "",
		},
		{
			colType:     "varchar(10)",
			charset:     "charset ascii",
			collates:    "",
			exptCharset: "ascii",
			exptCollate: "ascii_bin",
			errMsg:      "",
		},
	}
	sctx := tk.Se.(sessionctx.Context)
	dm := domain.GetDomain(sctx)
	for i, tt := range tests {
		tblName := fmt.Sprintf("t%d", i)
		sql := fmt.Sprintf("create table %s (a %s %s %s)", tblName, tt.colType, tt.charset, tt.collates)
		if tt.errMsg == "" {
			tk.MustExec(sql)
			is := dm.InfoSchema()
			c.Assert(is, NotNil)

			tb, err := is.TableByName(model.NewCIStr(dbName), model.NewCIStr(tblName))
			c.Assert(err, IsNil)
			c.Assert(tb.Meta().Columns[0].Charset, Equals, tt.exptCharset, Commentf(sql))
			c.Assert(tb.Meta().Columns[0].Collate, Equals, tt.exptCollate, Commentf(sql))
		} else {
			_, err := tk.Exec(sql)
			c.Assert(err, NotNil, Commentf(sql))
		}
	}
	tk.MustExec("drop database " + dbName)
}

func (s *testSuite3) TestTooLargeIdentifierLength(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// for database.
	dbName1, dbName2 := strings.Repeat("a", mysql.MaxDatabaseNameLength), strings.Repeat("a", mysql.MaxDatabaseNameLength+1)
	tk.MustExec(fmt.Sprintf("create database %s", dbName1))
	tk.MustExec(fmt.Sprintf("drop database %s", dbName1))
	_, err := tk.Exec(fmt.Sprintf("create database %s", dbName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", dbName2))

	// for table.
	tk.MustExec("use test")
	tableName1, tableName2 := strings.Repeat("b", mysql.MaxTableNameLength), strings.Repeat("b", mysql.MaxTableNameLength+1)
	tk.MustExec(fmt.Sprintf("create table %s(c int)", tableName1))
	tk.MustExec(fmt.Sprintf("drop table %s", tableName1))
	_, err = tk.Exec(fmt.Sprintf("create table %s(c int)", tableName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", tableName2))

	// for column.
	tk.MustExec("drop table if exists t;")
	columnName1, columnName2 := strings.Repeat("c", mysql.MaxColumnNameLength), strings.Repeat("c", mysql.MaxColumnNameLength+1)
	tk.MustExec(fmt.Sprintf("create table t(%s int)", columnName1))
	tk.MustExec("drop table t")
	_, err = tk.Exec(fmt.Sprintf("create table t(%s int)", columnName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", columnName2))

	// for index.
	tk.MustExec("create table t(c int);")
	indexName1, indexName2 := strings.Repeat("d", mysql.MaxIndexIdentifierLen), strings.Repeat("d", mysql.MaxIndexIdentifierLen+1)
	tk.MustExec(fmt.Sprintf("create index %s on t(c)", indexName1))
	tk.MustExec(fmt.Sprintf("drop index %s on t", indexName1))
	_, err = tk.Exec(fmt.Sprintf("create index %s on t(c)", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", indexName2))

	// for create table with index.
	tk.MustExec("drop table t;")
	_, err = tk.Exec(fmt.Sprintf("create table t(c int, index %s(c));", indexName2))
	c.Assert(err.Error(), Equals, fmt.Sprintf("[ddl:1059]Identifier name '%s' is too long", indexName2))
}

func (s *testSuite3) TestShardRowIDBits(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("create table t (a int) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert t values (%d)", i))
	}
	dom := domain.GetDomain(tk.Se)
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	assertCountAndShard := func(t table.Table, expectCount int) {
		var hasShardedID bool
		var count int
		c.Assert(tk.Se.NewTxn(context.Background()), IsNil)
		err = t.IterRecords(tk.Se, t.FirstKey(), nil, func(h int64, rec []types.Datum, cols []*table.Column) (more bool, err error) {
			c.Assert(h, GreaterEqual, int64(0))
			first8bits := h >> 56
			if first8bits > 0 {
				hasShardedID = true
			}
			count++
			return true, nil
		})
		c.Assert(err, IsNil)
		c.Assert(count, Equals, expectCount)
		c.Assert(hasShardedID, IsTrue)
	}

	assertCountAndShard(tbl, 100)

	// After PR 10759, shard_row_id_bits is supported with tables with auto_increment column.
	tk.MustExec("create table auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustExec("alter table auto shard_row_id_bits = 5")
	tk.MustExec("drop table auto")
	tk.MustExec("create table auto (id int not null auto_increment unique) shard_row_id_bits = 0")
	tk.MustExec("alter table auto shard_row_id_bits = 5")
	tk.MustExec("drop table auto")
	tk.MustExec("create table auto (id int not null auto_increment unique)")
	tk.MustExec("alter table auto shard_row_id_bits = 5")
	tk.MustExec("drop table auto")
	tk.MustExec("create table auto (id int not null auto_increment unique) shard_row_id_bits = 4")
	tk.MustExec("alter table auto shard_row_id_bits = 0")
	tk.MustExec("drop table auto")

	// After PR 10759, shard_row_id_bits is not supported with pk_is_handle tables.
	err = tk.ExecToErr("create table auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 4")
	c.Assert(err.Error(), Equals, "[ddl:207]unsupported shard_row_id_bits for table with primary key as row id.")
	tk.MustExec("create table auto (id int not null auto_increment primary key, b int) shard_row_id_bits = 0")
	err = tk.ExecToErr("alter table auto shard_row_id_bits = 5")
	c.Assert(err.Error(), Equals, "[ddl:207]unsupported shard_row_id_bits for table with primary key as row id.")
	tk.MustExec("alter table auto shard_row_id_bits = 0")

	// Hack an existing table with shard_row_id_bits and primary key as handle
	db, ok := dom.InfoSchema().SchemaByName(model.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("auto"))
	tblInfo := tbl.Meta()
	tblInfo.ShardRowIDBits = 5
	tblInfo.MaxShardRowIDBits = 5

	kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, tblInfo), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustExec("insert auto(b) values (1), (3), (5)")
	tk.MustQuery("select id from auto order by id").Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("alter table auto shard_row_id_bits = 0")
	tk.MustExec("drop table auto")

	// Test shard_row_id_bits with auto_increment column
	tk.MustExec("create table auto (a int, b int auto_increment unique) shard_row_id_bits = 15")
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf("insert auto(a) values (%d)", i))
	}
	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("auto"))
	assertCountAndShard(tbl, 100)
	prevB, err := strconv.Atoi(tk.MustQuery("select b from auto where a=0").Rows()[0][0].(string))
	c.Assert(err, IsNil)
	for i := 1; i < 100; i++ {
		b, err := strconv.Atoi(tk.MustQuery(fmt.Sprintf("select b from auto where a=%d", i)).Rows()[0][0].(string))
		c.Assert(err, IsNil)
		c.Assert(b, Greater, prevB)
		prevB = b
	}

	// Test overflow
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int) shard_row_id_bits = 15")
	defer tk.MustExec("drop table if exists t1")

	tbl, err = dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	maxID := 1<<(64-15-1) - 1
	err = tbl.RebaseAutoID(tk.Se, int64(maxID)-1, false)
	c.Assert(err, IsNil)
	tk.MustExec("insert into t1 values(1)")

	// continue inserting will fail.
	_, err = tk.Exec("insert into t1 values(2)")
	c.Assert(autoid.ErrAutoincReadFailed.Equal(err), IsTrue, Commentf("err:%v", err))
	_, err = tk.Exec("insert into t1 values(3)")
	c.Assert(autoid.ErrAutoincReadFailed.Equal(err), IsTrue, Commentf("err:%v", err))
}

func (s *testSuite3) TestMaxHandleAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	tk.MustExec(fmt.Sprintf("insert into t values(%v, 1)", math.MaxInt64))
	tk.MustExec(fmt.Sprintf("insert into t values(%v, 1)", math.MinInt64))
	tk.MustExec("alter table t add index idx_b(b)")
	tk.MustExec("admin check table t")

	tk.MustExec("create table t1(a bigint UNSIGNED PRIMARY KEY, b int)")
	tk.MustExec(fmt.Sprintf("insert into t1 values(%v, 1)", uint64(math.MaxUint64)))
	tk.MustExec(fmt.Sprintf("insert into t1 values(%v, 1)", 0))
	tk.MustExec("alter table t1 add index idx_b(b)")
	tk.MustExec("admin check table t1")
}

func (s *testSuite3) TestSetDDLReorgWorkerCnt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(variable.DefTiDBDDLReorgWorkerCount))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 1")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(1))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.Exec("set @@global.tidb_ddl_reorg_worker_cnt = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgWorkerCounter(), Equals, int32(100))
	_, err = tk.Exec("set @@global.tidb_ddl_reorg_worker_cnt = -1")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongValueForVar), IsTrue, Commentf("err %v", err))

	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	res := tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))

	res = tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))
	tk.MustExec("set @@global.tidb_ddl_reorg_worker_cnt = 100")
	res = tk.MustQuery("select @@global.tidb_ddl_reorg_worker_cnt")
	res.Check(testkit.Rows("100"))
}

func (s *testSuite3) TestSetDDLReorgBatchSize(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(variable.DefTiDBDDLReorgBatchSize))

	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '1'"))
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(variable.MinDDLReorgBatchSize))
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_reorg_batch_size = %v", variable.MaxDDLReorgBatchSize+1))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '%d'", variable.MaxDDLReorgBatchSize+1)))
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(variable.MaxDDLReorgBatchSize))
	_, err = tk.Exec("set @@global.tidb_ddl_reorg_batch_size = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 100")
	err = ddlutil.LoadDDLReorgVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLReorgBatchSize(), Equals, int32(100))
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_reorg_batch_size value: '-1'"))

	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 100")
	res := tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows("100"))

	res = tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows(fmt.Sprintf("%v", 100)))
	tk.MustExec("set @@global.tidb_ddl_reorg_batch_size = 1000")
	res = tk.MustQuery("select @@global.tidb_ddl_reorg_batch_size")
	res.Check(testkit.Rows("1000"))
}

func (s *testSuite3) TestIllegalFunctionCall4GeneratedColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Test create an exist database
	_, err := tk.Exec("CREATE database test")
	c.Assert(err, NotNil)

	_, err = tk.Exec("create table t1 (b double generated always as (rand()) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.Exec("create table t1 (a varchar(64), b varchar(1024) generated always as (load_file(a)) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.Exec("create table t1 (a datetime generated always as (curdate()) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create table t1 (a datetime generated always as (current_time()) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create table t1 (a datetime generated always as (current_timestamp()) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create table t1 (a datetime, b varchar(10) generated always as (localtime()) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	_, err = tk.Exec("create table t1 (a varchar(1024) generated always as (uuid()) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("a").Error())

	_, err = tk.Exec("create table t1 (a varchar(1024), b varchar(1024) generated always as (is_free_lock(a)) virtual);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("b").Error())

	tk.MustExec("create table t1 (a bigint not null primary key auto_increment, b bigint, c bigint as (b + 1));")

	_, err = tk.Exec("alter table t1 add column d varchar(1024) generated always as (database());")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("d").Error())

	tk.MustExec("alter table t1 add column d bigint generated always as (b + 1); ")

	_, err = tk.Exec("alter table t1 modify column d bigint generated always as (connection_id());")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("d").Error())

	_, err = tk.Exec("alter table t1 change column c cc bigint generated always as (connection_id());")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnFunctionIsNotAllowed.GenWithStackByArgs("cc").Error())
}

func (s *testSuite3) TestGeneratedColumnRelatedDDL(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// Test create an exist database
	_, err := tk.Exec("CREATE database test")
	c.Assert(err, NotNil)

	_, err = tk.Exec("create table t1 (a bigint not null primary key auto_increment, b bigint as (a + 1));")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs("b").Error())

	tk.MustExec("create table t1 (a bigint not null primary key auto_increment, b bigint, c bigint as (b + 1));")

	_, err = tk.Exec("alter table t1 add column d bigint generated always as (a + 1);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs("d").Error())

	tk.MustExec("alter table t1 add column d bigint generated always as (b + 1);")

	_, err = tk.Exec("alter table t1 modify column d bigint generated always as (a + 1);")
	c.Assert(err.Error(), Equals, ddl.ErrGeneratedColumnRefAutoInc.GenWithStackByArgs("d").Error())

	_, err = tk.Exec("alter table t1 add column e bigint as (z + 1);")
	c.Assert(err.Error(), Equals, ddl.ErrBadField.GenWithStackByArgs("z", "generated column function").Error())

	tk.MustExec("drop table t1;")
}

func (s *testSuite3) TestSetDDLErrorCountLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	err := ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(variable.DefTiDBDDLErrorCountLimit))

	tk.MustExec("set @@global.tidb_ddl_error_count_limit = -1")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_ddl_error_count_limit value: '-1'"))
	err = ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(0))
	tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_error_count_limit = %v", uint64(math.MaxInt64)+1))
	tk.MustQuery("show warnings;").Check(testkit.Rows(fmt.Sprintf("Warning 1292 Truncated incorrect tidb_ddl_error_count_limit value: '%d'", uint64(math.MaxInt64)+1)))
	err = ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(math.MaxInt64))
	_, err = tk.Exec("set @@global.tidb_ddl_error_count_limit = invalid_val")
	c.Assert(terror.ErrorEqual(err, variable.ErrWrongTypeForVar), IsTrue, Commentf("err %v", err))
	tk.MustExec("set @@global.tidb_ddl_error_count_limit = 100")
	err = ddlutil.LoadDDLVars(tk.Se)
	c.Assert(err, IsNil)
	c.Assert(variable.GetDDLErrorCountLimit(), Equals, int64(100))
	res := tk.MustQuery("select @@global.tidb_ddl_error_count_limit")
	res.Check(testkit.Rows("100"))
}

// Test issue #9205, fix the precision problem for time type default values
// See https://github.com/pingcap/tidb/issues/9205 for details
func (s *testSuite3) TestIssue9205(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(c time DEFAULT '12:12:12.8');`)
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
	tk.MustExec(`alter table t add column c1 time default '12:12:12.000000';`)
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '12:12:12'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec(`alter table t alter column c1 set default '2019-02-01 12:12:10.4';`)
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '12:12:10'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec(`alter table t modify c1 time DEFAULT '770:12:12.000000';`)
	tk.MustQuery("show create table `t`").Check(testutil.RowsWithSep("|",
		""+
			"t CREATE TABLE `t` (\n"+
			"  `c` time DEFAULT '12:12:13',\n"+
			"  `c1` time DEFAULT '770:12:12'\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

func (s *testSuite3) TestCheckDefaultFsp(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)

	_, err := tk.Exec("create table t (  tt timestamp default now(1));")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("create table t (  tt timestamp(1) default current_timestamp);")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("create table t (  tt timestamp(1) default now(2));")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'tt'")

	tk.MustExec("create table t (  tt timestamp(1) default now(1));")
	tk.MustExec("create table t2 (  tt timestamp default current_timestamp());")
	tk.MustExec("create table t3 (  tt timestamp default current_timestamp(0));")

	_, err = tk.Exec("alter table t add column ttt timestamp default now(2);")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'ttt'")

	_, err = tk.Exec("alter table t add column ttt timestamp(5) default current_timestamp;")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'ttt'")

	_, err = tk.Exec("alter table t add column ttt timestamp(5) default now(2);")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'ttt'")

	_, err = tk.Exec("alter table t modify column tt timestamp(1) default now();")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("alter table t modify column tt timestamp(4) default now(5);")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'tt'")

	_, err = tk.Exec("alter table t change column tt tttt timestamp(4) default now(5);")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'tttt'")

	_, err = tk.Exec("alter table t change column tt tttt timestamp(1) default now();")
	c.Assert(err.Error(), Equals, "[ddl:1067]Invalid default value for 'tttt'")
}

func (s *testSuite3) TestTimestampMinDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tdv;")
	tk.MustExec("create table tdv(a int);")
	tk.MustExec("ALTER TABLE tdv ADD COLUMN ts timestamp DEFAULT '1970-01-01 08:00:01';")
}
