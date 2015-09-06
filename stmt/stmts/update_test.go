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
	"database/sql"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
)

func (s *testStmtSuite) TestUpdate(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+s.dbName)
	c.Assert(err, IsNil)

	s.fillData(testDB, c)

	updateStr := `UPDATE test SET name = "abc" where id > 0;`

	// Test compile
	stmtList, err := tidb.Compile(updateStr)
	c.Assert(err, IsNil)

	str := stmtList[0].OriginText()
	c.Assert(0, Less, len(str))

	r := mustExec(c, testDB, updateStr)
	checkResult(c, r, 2, 0)

	// select data
	tx := mustBegin(c, testDB)
	rows, err := tx.Query(s.selectSql)
	c.Assert(err, IsNil)

	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		c.Assert(name, Equals, "abc")
	}

	rows.Close()
	mustCommit(c, tx)

	// Should use index
	strs := s.queryStrings(testDB, `explain `+updateStr, c)
	var useIndex bool
	for _, str := range strs {
		if strings.Index(str, "index") > 0 {
			useIndex = true
		}
	}

	if !useIndex {
		c.Fatal(strs)
	}
	// Should not use index
	strs = s.queryStrings(testDB, `explain UPDATE test SET name = "abc"`, c)
	useIndex = false
	for _, str := range strs {
		if strings.Index(str, "index") > 0 {
			useIndex = true
		}
	}

	if useIndex {
		c.Fatal(strs)
	}

	// Test update without index
	r = mustExec(c, testDB, `explain UPDATE test SET name = "abc"`)
	checkResult(c, r, 0, 0)

	r = mustExec(c, testDB, `UPDATE test SET name = "foo"`)
	checkResult(c, r, 2, 0)
}
