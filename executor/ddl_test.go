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
	"fmt"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testSuite) TestTruncateTable(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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

func (s *testSuite) TestCreateTable(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	for {
		row, err1 := rs.Next()
		c.Assert(err1, IsNil)
		if row == nil {
			break
		}
		c.Assert(row.Data[1].GetString(), Equals, "float")
	}
	rs, err = tk.Exec(`desc issue312_2`)
	c.Assert(err, IsNil)
	for {
		row, err1 := rs.Next()
		c.Assert(err1, IsNil)
		if row == nil {
			break
		}
		c.Assert(row.Data[1].GetString(), Equals, "double")
	}

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

func (s *testSuite) TestCreateDropDatabase(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists drop_test;")
	tk.MustExec("drop database if exists drop_test;")
	tk.MustExec("create database drop_test;")
	tk.MustExec("drop database drop_test;")
}

func (s *testSuite) TestCreateDropTable(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists drop_test (a int)")
	tk.MustExec("drop table if exists drop_test")
	tk.MustExec("create table drop_test (a int)")
	tk.MustExec("drop table drop_test")
}

func (s *testSuite) TestCreateDropIndex(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists drop_test (a int)")
	tk.MustExec("create index idx_a on drop_test (a)")
	tk.MustExec("drop index idx_a on drop_test")
	tk.MustExec("drop table drop_test")
}

func (s *testSuite) TestAlterTableAddColumn(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists alter_test (c1 int)")
	tk.MustExec("insert into alter_test values(1)")
	tk.MustExec("alter table alter_test add column c2 timestamp default current_timestamp")
	time.Sleep(1 * time.Second)
	now := time.Now().Add(-time.Duration(1 * time.Second)).Format(types.TimeFormat)
	r, err := tk.Exec("select c2 from alter_test")
	c.Assert(err, IsNil)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(len(row.Data), Equals, 1)
	t := row.Data[0].GetMysqlTime()
	c.Assert(now, GreaterEqual, t.String())
	tk.MustExec("alter table alter_test add column c3 varchar(50) default 'CURRENT_TIMESTAMP'")
	tk.MustQuery("select c3 from alter_test").Check(testkit.Rows("CURRENT_TIMESTAMP"))
}

func (s *testSuite) TestAddNotNullColumnNoDefault(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table nn (c1 int)")
	tk.MustExec("insert nn values (1), (2)")
	tk.MustExec("alter table nn add column c2 int not null")

	tbl, err := sessionctx.GetDomain(tk.Se).InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("nn"))
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

func (s *testSuite) TestAlterTableModifyColumn(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists mc (c1 int, c2 varchar(10))")
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
	result := tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `c1` bigint(21) DEFAULT NULL,\n  `c2` text DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	c.Assert(createSQL, Equals, expected)
}

func (s *testSuite) TestDefaultDBAfterDropCurDB(c *C) {
	defer testleak.AfterTest(c)()
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

func (s *testSuite) TestRenameTable(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("create database rename1")
	tk.MustExec("create database rename2")
	tk.MustExec("create database rename3")

	tk.MustExec("create table rename1.t (a int primary key auto_increment)")
	tk.MustExec("insert rename1.t values ()")
	tk.MustExec("rename table rename1.t to rename2.t")
	tk.MustExec("insert rename2.t values ()")
	tk.MustExec("rename table rename2.t to rename3.t")
	tk.MustExec("insert rename3.t values ()")
	tk.MustQuery("select * from rename3.t").Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop database rename1")
	tk.MustExec("drop database rename2")
	tk.MustExec("drop database rename3")
}

func (s *testSuite) TestUnsupportedCharset(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	dbName := "unsupported_charset"
	tk.MustExec("create database " + dbName)
	tk.MustExec("use " + dbName)
	tests := []struct {
		charset string
		valid   bool
	}{
		{"charset UTF8 collate UTF8_bin", true},
		{"charset utf8mb4", true},
		{"charset utf16", false},
		{"charset latin1", true},
		{"charset binary", true},
		{"charset ascii", true},
	}
	for i, tt := range tests {
		sql := fmt.Sprintf("create table t%d (a varchar(10) %s)", i, tt.charset)
		if tt.valid {
			tk.MustExec(sql)
		} else {
			_, err := tk.Exec(sql)
			c.Assert(err, NotNil, Commentf(sql))
		}
	}
	tk.MustExec("drop database " + dbName)
}
