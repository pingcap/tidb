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

package stmts_test

import (
	"bytes"
	"fmt"

	. "github.com/pingcap/check"
)

type mockFormatter struct {
	bytes.Buffer
}

func newMockFormatter() *mockFormatter {
	return &mockFormatter{}
}

func (f *mockFormatter) Format(format string, args ...interface{}) (n int, errno error) {
	data := fmt.Sprintf(format, args...)
	return f.Write([]byte(data))
}

func (s *testStmtSuite) TestGetColDefaultValue(c *C) {
	testSQL := `drop table if exists helper_test;`
	mustExec(c, s.testDB, testSQL)
	testSQL = `create table helper_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int not null, c2 timestamp, c3 int default 1);`
	mustExec(c, s.testDB, testSQL)

	testSQL = " insert helper_test (c1) values (1);"
	mustExec(c, s.testDB, testSQL)

	errTestSQL := `insert helper_test (c3) values (0);`
	tx := mustBegin(c, s.testDB)
	_, err := tx.Exec(errTestSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	testSQL = `drop table if exists helper_test;`
	mustExec(c, s.testDB, testSQL)
	testSQL = `create table helper_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 datetime, c3 int default 1);`
	mustExec(c, s.testDB, testSQL)

	testSQL = " insert helper_test (c1) values (1);"
	mustExec(c, s.testDB, testSQL)

	testSQL = `drop table if exists helper_test;`
	mustExec(c, s.testDB, testSQL)
	testSQL = `create table helper_test (c1 enum("a"), c2 enum("b", "e") not null, c3 enum("c") default "c", c4 enum("d") default "d" not null);`
	mustExec(c, s.testDB, testSQL)

	testSQL = "insert into helper_test values();"
	mustExec(c, s.testDB, testSQL)

	row := s.testDB.QueryRow("select * from helper_test")
	var (
		v1 interface{}
		v2 interface{}
		v3 interface{}
		v4 interface{}
	)

	err = row.Scan(&v1, &v2, &v3, &v4)
	c.Assert(err, IsNil)
	c.Assert(v1, IsNil)
	c.Assert(v2, Equals, "b")
	c.Assert(v3, Equals, "c")
	c.Assert(v4, Equals, "d")

}
