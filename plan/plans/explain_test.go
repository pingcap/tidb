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

package plans_test

import (
	"database/sql"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
)

type testExplainSuit struct{}

var _ = Suite(&testExplainSuit{})

func mustExplain(c *C, currDB *sql.DB, s string) string {
	tx := mustBegin(c, currDB)
	r, err := tx.Query(s)
	c.Assert(err, IsNil)
	cols, _ := r.Columns()
	l := len(cols)
	c.Assert(l, Greater, 0)
	res := make([]interface{}, l)
	for i := 0; i < l; i++ {
		res[i] = &sql.RawBytes{}
	}
	line := ""
	for r.Next() {
		err := r.Scan(res...)
		c.Assert(err, IsNil)
		line += string(*res[0].(*sql.RawBytes)) + "\n"
	}
	return line
}

func (t *testExplainSuit) TestExplain(c *C) {
	testDB, err := sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"/ex/ex")
	c.Assert(err, IsNil)
	mustExec(c, testDB, "create table tt(id int);")
	mustExec(c, testDB, "create table tt2(id int, KEY i_id(id));")
	mustExec(c, testDB, "insert into tt values(1);")
	mustExec(c, testDB, "insert into tt2 values(1);")

	s := mustExplain(c, testDB, "explain select * from tt")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select * from tt2 where id > 0")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select * from tt2 where id > 0 limit 1 offset 0;")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select * from tt limit 1;")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select count(*) from tt2")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select * from tt left join tt2 on tt.id = tt2.id;")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select distinct(id) from tt;")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select * from tt where id > 0 and id <0;")
	c.Assert(len(s), Greater, 0)
	s = mustExplain(c, testDB, "explain select * from tt order by id desc;")
	c.Assert(len(s), Greater, 0)
}
