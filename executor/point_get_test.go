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

func (s *testSuite1) TestPointGet(c *C) {
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
	result.Close()

	tk.MustExec("CREATE TABLE tab3(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col2 TEXT, col3 INTEGER, col4 FLOAT, col5 TEXT);")
	tk.MustExec("CREATE UNIQUE INDEX idx_tab3_0 ON tab3 (col4);")
	tk.MustExec("INSERT INTO tab3 VALUES(0,854,111.96,'mguub',711,966.36,'snwlo');")
	tk.MustQuery("SELECT ALL * FROM tab3 WHERE col4 = 85;").Check(testkit.Rows())

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint primary key, b bigint, c bigint);`)
	tk.MustExec(`insert into t values(1, NULL, NULL), (2, NULL, 2), (3, 3, NULL), (4, 4, 4), (5, 6, 7);`)
	tk.MustQuery(`select * from t where a = 1;`).Check(testkit.Rows(
		`1 <nil> <nil>`,
	))
	tk.MustQuery(`select * from t where a = 2;`).Check(testkit.Rows(
		`2 <nil> 2`,
	))
	tk.MustQuery(`select * from t where a = 3;`).Check(testkit.Rows(
		`3 3 <nil>`,
	))
	tk.MustQuery(`select * from t where a = 4;`).Check(testkit.Rows(
		`4 4 4`,
	))
	tk.MustQuery(`select a, a, b, a, b, c, b, c, c from t where a = 5;`).Check(testkit.Rows(
		`5 5 6 5 6 7 6 7 7`,
	))
}

func (s *testSuite1) TestPointGetCharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) primary key, b char(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	// Test truncate without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	// Test truncate with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// // Test CHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a char(2) binary primary key, b char(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(` `))

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a b`))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(` `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(` `))
}

func (s *testSuite1) TestPointGetVarcharPK(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a varchar(2) primary key, b varchar(2));`)
	tk.MustExec(`insert into t values("aa", "bb");`)

	// Test truncate without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	// Test truncate with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "aa";`).Check(testkit.Rows(`aa bb`))
	tk.MustPointGet(`select * from t where a = "aab";`).Check(testkit.Rows())

	tk.MustExec(`truncate table t;`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows())
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows())

	// // Test VARCHAR BINARY.
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a varchar(2) binary primary key, b varchar(2));`)
	tk.MustExec(`insert into t values("  ", "  ");`)
	tk.MustExec(`insert into t values("a ", "b ");`)

	// Test trailing spaces without sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(`     `))

	// Test trailing spaces with sql mode `PAD_CHAR_TO_FULL_LENGTH`.
	// `PAD_CHAR_TO_FULL_LENGTH` should not affect the result.
	tk.MustExec(`set @@sql_mode="PAD_CHAR_TO_FULL_LENGTH";`)
	tk.MustPointGet(`select * from t where a = "a";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = "a  ";`).Check(testkit.Rows(`a  b `))
	tk.MustPointGet(`select * from t where a = " ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "  ";`).Check(testkit.Rows(`     `))
	tk.MustPointGet(`select * from t where a = "   ";`).Check(testkit.Rows(`     `))
}
