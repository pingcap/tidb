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
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestShow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `drop table if exists show_test`
	tk.MustExec(testSQL)
	testSQL = `create table show_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	tk.MustExec(testSQL)

	testSQL = "show columns from show_test;"
	result := tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 4)

	testSQL = "show create table show_test;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)

	testSQL = "SHOW VARIABLES LIKE 'character_set_results';"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)

	// Test case for index type and comment
	tk.MustExec(`create table show_index (c int, index cIdx using hash (c) comment "index_comment_for_cIdx");`)
	testSQL = "SHOW index from show_index;"
	result = tk.MustQuery(testSQL)
	c.Check(result.Rows(), HasLen, 1)
	expectedRow := []interface{}{
		"show_index", int64(1), "cIdx", int64(1), "c", "utf8_bin",
		int64(0), nil, nil, "YES", "HASH", "", "index_comment_for_cIdx"}
	row := result.Rows()[0]
	c.Check(row, HasLen, len(expectedRow))
	for i, r := range row {
		c.Check(r, Equals, expectedRow[i])
	}
}
