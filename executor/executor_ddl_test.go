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

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestTruncateTable(c *C) {
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
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
		row, err2 := rs.Next()
		c.Assert(err2, IsNil)
		if row == nil {
			break
		}
		c.Assert(row.Data[1].GetString(), Equals, "float")
	}
	rs, err = tk.Exec(`desc issue312_2`)
	c.Assert(err, IsNil)
	for {
		row, err2 := rs.Next()
		c.Assert(err2, IsNil)
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
	rowStr1 := fmt.Sprintf("%v %v", 999, []byte("aa"))
	rowStr2 := fmt.Sprintf("%v %v", 1000, []byte("bb"))
	rowStr3 := fmt.Sprintf("%v %v", 1001, []byte("cc"))
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("drop table create_auto_increment_test")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), primary key(id)) auto_increment = 1999;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('bb')")
	tk.MustExec("insert into create_auto_increment_test (name) values ('cc')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	rowStr1 = fmt.Sprintf("%v %v", 1999, []byte("aa"))
	rowStr2 = fmt.Sprintf("%v %v", 2000, []byte("bb"))
	rowStr3 = fmt.Sprintf("%v %v", 2001, []byte("cc"))
	r.Check(testkit.Rows(rowStr1, rowStr2, rowStr3))
	tk.MustExec("drop table create_auto_increment_test")
	tk.MustExec("create table create_auto_increment_test (id int not null auto_increment, name varchar(255), key(id)) auto_increment = 1000;")
	tk.MustExec("insert into create_auto_increment_test (name) values ('aa')")
	r = tk.MustQuery("select * from create_auto_increment_test;")
	rowStr1 = fmt.Sprintf("%v %v", 1000, []byte("aa"))
	r.Check(testkit.Rows(rowStr1))
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

func (s *testSuite) TestAlterTable(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table if not exists alter_test (c1 int)")
	tk.MustExec("alter table alter_test add column c2 int")
}
