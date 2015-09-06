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
	testSQL := `drop table if exists helper_test;
    create table helper_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int not null, c2 timestamp, c3 int default 1);`
	mustExec(c, s.testDB, testSQL)

	testSQL = " insert helper_test (c1) values (1);"
	mustExec(c, s.testDB, testSQL)

	errTestSQL := `insert helper_test (c3) values (0);`
	tx := mustBegin(c, s.testDB)
	_, err := tx.Exec(errTestSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	testSQL = `drop table if exists helper_test;
    create table helper_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 datetime, c3 int default 1);`
	mustExec(c, s.testDB, testSQL)

	testSQL = " insert helper_test (c1) values (1);"
	mustExec(c, s.testDB, testSQL)
}
