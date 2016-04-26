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
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists show_test`
	tk.MustExec(testSQL)
	testSQL = `create table show_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int comment "c1_comment", c2 int, c3 int default 1) ENGINE=InnoDB AUTO_INCREMENT=28934 DEFAULT CHARSET=utf8 COMMENT "table_comment";`
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
		"show_test", "CREATE TABLE `show_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n  `c1` int(11) DEFAULT NULL COMMENT 'c1_comment',\n  `c2` int(11) DEFAULT NULL,\n  `c3` int(11) DEFAULT '1',\n PRIMARY KEY (`id`) \n) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=28934 COMMENT='table_comment'"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	testSQL = "SHOW VARIABLES LIKE 'character_set_results';"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)

	// Test case for index type and comment
	tk.MustExec(`create table show_index (c int, index cIdx using hash (c) comment "index_comment_for_cIdx");`)
	testSQL = "SHOW index from show_index;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	expectedRow = []interface{}{
		"show_index", int64(1), "cIdx", int64(1), "c", "utf8_bin",
		int64(0), nil, nil, "YES", "HASH", "", "index_comment_for_cIdx"}
	row = result.Rows()[0]
	c.Check(row, HasLen, len(expectedRow))
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	// For show like with escape
	testSQL = `show tables like 'show\_test'`
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)

	var ss statistics
	variable.RegisterStatistics(ss)
	testSQL = "show status like 'character_set_results';"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), NotNil)
}

type statistics struct {
}

func (s statistics) GetScope(status string) variable.ScopeFlag { return variable.DefaultScopeFlag }

func (s statistics) Stats() (map[string]interface{}, error) {
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

	testSQL = "create table show_test (`id` int PRIMARY KEY AUTO_INCREMENT, FOREIGN KEY `fk` (`id`) REFERENCES `t1` (`a`) ON DELETE CASCADE ON UPDATE CASCADE) ENGINE=InnoDB"
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result := tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row := result.Rows()[0]
	expectedRow := []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n PRIMARY KEY (`id`) \n  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE CASCADE\n) ENGINE=InnoDB"}
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
		"show_test", "CREATE TABLE `show_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n PRIMARY KEY (`id`) \n) ENGINE=InnoDB"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

	testSQL = "ALTER TABLE SHOW_TEST ADD CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE CASCADE\n "
	tk.MustExec(testSQL)
	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	row = result.Rows()[0]
	expectedRow = []interface{}{
		"show_test", "CREATE TABLE `show_test` (\n  `id` int(11) NOT NULL AUTO_INCREMENT,\n PRIMARY KEY (`id`) \n  CONSTRAINT `fk` FOREIGN KEY (`id`) REFERENCES `t1` (`id`) ON DELETE CASCADE ON UPDATE CASCADE\n) ENGINE=InnoDB"}
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}

}
