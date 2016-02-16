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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestCharsetDatabase(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	testSQL := `create database if not exists cd_test_utf8 CHARACTER SET utf8 COLLATE utf8_bin;`
	tk.MustExec(testSQL)

	testSQL = `create database if not exists cd_test_latin1 CHARACTER SET latin1 COLLATE latin1_swedish_ci;`
	tk.MustExec(testSQL)

	testSQL = `use cd_test_utf8;`
	tk.MustExec(testSQL)
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Rows("utf8"))
	tk.MustQuery(`select @@collation_database;`).Check(testkit.Rows("utf8_bin"))

	testSQL = `use cd_test_latin1;`
	tk.MustExec(testSQL)
	tk.MustQuery(`select @@character_set_database;`).Check(testkit.Rows("latin1"))
	tk.MustQuery(`select @@collation_database;`).Check(testkit.Rows("latin1_swedish_ci"))
}

func (s *testSuite) TestSet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	testSQL := "SET @a = 1;"
	tk.MustExec(testSQL)

	testSQL = `SET @a = "1";`
	tk.MustExec(testSQL)

	testSQL = "SET @a = null;"
	tk.MustExec(testSQL)

	testSQL = "SET @@global.autocommit = 1;"
	tk.MustExec(testSQL)

	testSQL = "SET @@global.autocommit = null;"
	tk.MustExec(testSQL)

	testSQL = "SET @@autocommit = 1;"
	tk.MustExec(testSQL)

	testSQL = "SET @@autocommit = null;"
	tk.MustExec(testSQL)

	errTestSql := "SET @@date_format = 1;"
	_, err := tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@rewriter_enabled = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET xxx = abcd;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@global.a = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)

	errTestSql = "SET @@global.timestamp = 1;"
	_, err = tk.Exec(errTestSql)
	c.Assert(err, NotNil)
}

func (s *testSuite) TestSetCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`SET NAMES latin1`)

	ctx := tk.Se.(context.Context)
	sessionVars := variable.GetSessionVars(ctx)
	for _, v := range variable.SetNamesVariables {
		c.Assert(sessionVars.Systems[v] != "utf8", IsTrue)
	}
	tk.MustExec(`SET NAMES utf8`)
	for _, v := range variable.SetNamesVariables {
		c.Assert(sessionVars.Systems[v], Equals, "utf8")
	}
	c.Assert(sessionVars.Systems[variable.CollationConnection], Equals, "utf8_general_ci")
}
