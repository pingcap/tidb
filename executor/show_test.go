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
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

func (s *testSuite) TestShow(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists show_test`
	tk.MustExec(testSQL)
	testSQL = `create table SHOW_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int comment "c1_comment", c2 int, c3 int default 1) ENGINE=InnoDB AUTO_INCREMENT=28934 DEFAULT CHARSET=utf8 COMMENT "table_comment";`
	tk.MustExec(testSQL)

	testSQL = "show columns from show_test;"
	result := tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 4)

	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row := result.Rows()[0]
	// For issue https://github.com/pingcap/tidb/issues/1061
	expectedRow := []interface{}{
		"SHOW_test", "CREATE TABLE `SHOW_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `c1` int(11) DEFAULT NULL COMMENT 'c1_comment',\n  `c2` int(11) DEFAULT NULL,\n  `c3` int(11) DEFAULT '1',\n PRIMARY KEY (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=28934 COMMENT='table_comment'"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	// For issue https://github.com/pingcap/tidb/issues/1918
	testSQL = `create table ptest(
		a int primary key, 
		b double NOT NULL DEFAULT 2.0, 
		c varchar(10) NOT NULL, 
		d time unique,
		e timestamp NULL
	);`
	tk.MustExec(testSQL)
	testSQL = "show create table ptest;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"ptest", "CREATE TABLE `ptest` (\n  `a` int(11) NOT NULL,\n  `b` double NOT NULL DEFAULT '2.0',\n  `c` varchar(10) NOT NULL,\n  `d` time DEFAULT NULL,\n  `e` timestamp NULL DEFAULT NULL,\n PRIMARY KEY (`a`),\n  UNIQUE KEY `d` (`d`)\n) ENGINE=InnoDB"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	testSQL = "SHOW VARIABLES LIKE 'character_set_results';"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)

	// Test case for index type and comment
	tk.MustExec(`create table show_index (id int, c int, primary key (id), index cIdx using hash (c) comment "index_comment_for_cIdx");`)
	testSQL = "SHOW index from show_index;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 2)
	expectedRow = []interface{}{
		"show_index", int64(0), "PRIMARY", int64(1), "id", "utf8_bin",
		int64(0), nil, nil, "", "BTREE", "", ""}
	row = result.Rows()[0]
	c.Check(row, HasLen, len(expectedRow))
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}
	expectedRow = []interface{}{
		"show_index", int64(1), "cIdx", int64(1), "c", "utf8_bin",
		int64(0), nil, nil, "YES", "HASH", "", "index_comment_for_cIdx"}
	row = result.Rows()[1]
	c.Check(row, HasLen, len(expectedRow))
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	// For show like with escape
	testSQL = `show tables like 'show\_test'`
	result = tk.MustQuery(testSQL)
	rows := result.Rows()
	c.Check(rows, HasLen, 1)
	c.Check(rows[0], DeepEquals, []interface{}{"SHOW_test"})

	var ss stats
	variable.RegisterStatistics(ss)
	testSQL = "show status like 'character_set_results';"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), NotNil)

	tk.MustQuery("SHOW PROCEDURE STATUS WHERE Db='test'").Check(testkit.Rows())
	tk.MustQuery("SHOW TRIGGERS WHERE Trigger ='test'").Check(testkit.Rows())
	tk.MustQuery("SHOW processlist;").Check(testkit.Rows())
	tk.MustQuery("SHOW EVENTS WHERE Db = 'test'").Check(testkit.Rows())
	// Test show create database
	testSQL = `create database show_test_DB`
	tk.MustExec(testSQL)
	testSQL = "show create database show_test_DB;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"show_test_DB", "CREATE DATABASE `show_test_DB` /* !40100 DEFAULT CHARACTER SET utf8 */"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	tk.MustExec("use show_test_DB")
	result = tk.MustQuery("SHOW index from show_index from test where Column_name = 'c'")
	c.Check(result.Rows(), HasLen, 1)
}

type stats struct {
}

func (s stats) GetScope(status string) variable.ScopeFlag { return variable.DefaultScopeFlag }

func (s stats) Stats() (map[string]interface{}, error) {
	m := make(map[string]interface{})
	var a, b interface{}
	b = "123"
	m["test_interface_nil"] = a
	m["test_interface"] = b
	m["test_interface_slice"] = []interface{}{"a", "b", "c"}
	return m, nil
}

func (s *testSuite) TestForeignKeyInShowCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists show_test`
	tk.MustExec(testSQL)
	testSQL = `drop table if exists t1`
	tk.MustExec(testSQL)
	testSQL = `CREATE TABLE t1 (id int PRIMARY KEY AUTO_INCREMENT)`
	tk.MustExec(testSQL)

	testSQL = "create table show_test (`id` int PRIMARY KEY AUTO_INCREMENT, FOREIGN KEY `Fk` (`id`) REFERENCES `t1` (`a`) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB"
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result := tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row := result.Rows()[0]
	expectedRow := []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n PRIMARY KEY (`id`),\n  CONSTRAINT `Fk` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE CASCADE\n) ENGINE=InnoDB"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	testSQL = "alter table show_test drop foreign key `fk`"
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n PRIMARY KEY (`id`)\n) ENGINE=InnoDB"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	testSQL = "ALTER TABLE SHOW_TEST ADD CONSTRAINT `Fk` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE CASCADE\n "
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n PRIMARY KEY (`id`),\n  CONSTRAINT `Fk` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE CASCADE\n) ENGINE=InnoDB"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

}
