// Copyright 2021 PingCAP, Inc.
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

package parser_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	_ "github.com/pingcap/tidb/types/parser_driver"
	utilparser "github.com/pingcap/tidb/util/parser"
)

var _ = Suite(&testASTSuite{})

type testASTSuite struct {
}

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testASTSuite) TestSimpleCases(c *C) {
	tests := []struct {
		sql string
		db  string
		ans string
	}{
		{
			sql: "insert into t values(1, 2)",
			db:  "test",
			ans: "insert into test.t values(1, 2)",
		},
		{
			sql: "insert into mydb.t values(1, 2)",
			db:  "test",
			ans: "insert into mydb.t values(1, 2)",
		},
		{
			sql: "insert into t(a, b) values(1, 2)",
			db:  "test",
			ans: "insert into test.t(a, b) values(1, 2)",
		},
		{
			sql: "insert into value value(2, 3)",
			db:  "test",
			ans: "insert into test.value value(2, 3)",
		},
	}

	for _, t := range tests {
		p := parser.New()
		stmt, err := p.ParseOneStmt(t.sql, "", "")
		c.Assert(err, IsNil)
		ans, ok := utilparser.SimpleCases(stmt, t.db, t.sql)
		c.Assert(ok, IsTrue)
		c.Assert(t.ans, Equals, ans)
	}
}
