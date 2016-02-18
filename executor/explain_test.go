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

func (s *testSuite) TestExplain(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c1 int primary key, c2 int, index c2 (c2))")
	tk.MustExec("create table t2 (c1 int unique, c2 int)")

	cases := []struct {
		sql    string
		result []string
	}{
		{
			"select * from t1",
			[]string{
				"1 | SIMPLE | t1 | ALL | <nil> | <nil> | <nil> | <nil> | 0 | <nil>",
			},
		},
		{
			"select * from t1 order by c2",
			[]string{
				"1 | SIMPLE | t1 | index | c2 | c2 | <nil> | <nil> | 0 | <nil>",
			},
		},
		{
			"select * from t2 order by c2",
			[]string{
				"1 | SIMPLE | t2 | ALL | <nil> | <nil> | <nil> | <nil> | 0 | Using Filesort",
			},
		},
		{
			"select * from t1 where t1.c1 > 0",
			[]string{
				"1 | SIMPLE | t1 | range | PRIMARY | PRIMARY | 8 | <nil> | 0 | Using Where",
			},
		},
		{
			"select * from t1 where t1.c1 = 1",
			[]string{
				"1 | SIMPLE | t1 | const | PRIMARY | PRIMARY | 8 | <nil> | 0 | Using Where",
			},
		},
		{
			"select * from t1 where t1.c2 = 1",
			[]string{
				"1 | SIMPLE | t1 | range | c2 | c2 | -1 | <nil> | 0 | Using Where",
			},
		},
		{
			"select * from t1 left join t2 on t1.c2 = t2.c1 where t1.c1 > 1",
			[]string{
				"1 | SIMPLE | t1 | range | PRIMARY | PRIMARY | 8 | <nil> | 0 | Using Where",
				"1 | SIMPLE | t2 | eq_ref | c1 | c1 | -1 | <nil> | 0 | Using Where",
			},
		},
		{
			"update t1 set t1.c2 = 2 where t1.c1 = 1",
			[]string{
				"1 | SIMPLE | t1 | const | PRIMARY | PRIMARY | 8 | <nil> | 0 | Using Where",
			},
		},
		{
			"delete from t1 where t1.c2 = 1",
			[]string{
				"1 | SIMPLE | t1 | range | c2 | c2 | -1 | <nil> | 0 | Using Where",
			},
		},
	}
	for _, ca := range cases {
		result := tk.MustQuery("explain " + ca.sql)
		result.Check(testkit.RowsWithSep(" | ", ca.result...))
	}
}
