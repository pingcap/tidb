// Copyright 2018 PingCAP, Inc.
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

func (s *testSuite) TestPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table point (id int primary key, c int, d varchar(10), unique c_d (c, d))")
	tk.MustExec("insert point values (1, 1, 'a')")
	tk.MustExec("insert point values (2, 2, 'b')")
	tk.MustQuery("select * from point where id = 1 and c = 0").Check(testkit.Rows())
	tk.MustQuery("select * from point where id < 0 and c = 1 and d = 'b'").Check(testkit.Rows())
	result, err := tk.Exec("select id as ident from point where id = 1")
	c.Assert(err, IsNil)
	fields := result.Fields()
	c.Assert(fields[0].ColumnAsName.O, Equals, "ident")

	tk.MustExec("CREATE TABLE tab3(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);")
	tk.MustExec("CREATE UNIQUE INDEX idx_tab3_0 ON tab3 (col4);")
	tk.MustExec("INSERT INTO tab3 VALUES(0,854,111.96,'mguub',711,966.36,'snwlo');")
	tk.MustQuery("SELECT ALL * FROM tab3 WHERE col4 = 85;").Check(testkit.Rows())
}
