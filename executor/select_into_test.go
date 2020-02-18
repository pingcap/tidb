// Copyright 2020 PingCAP, Inc.
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
	"fmt"
	"io/ioutil"
	"os"
	"path"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func cmpAndRm(expected, outfile string, c *C) {
	content, err := ioutil.ReadFile(outfile)
	c.Assert(err, IsNil)
	c.Assert(string(content), Equals, expected)
	c.Assert(os.Remove(outfile), IsNil)
}

func (s *testSuite1) TestSelectIntoOutfileFromTable(c *C) {
	tmpDir := os.TempDir()
	outfile := path.Join(tmpDir, "select-into-outfile.data")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, j json)")
	tk.MustExec("insert into t values (1, 1.1, 0.1, 'a', '2000-01-01', '01:01:01', '[1]')")
	tk.MustExec("insert into t values (2, 2.2, 0.2, 'b', '2000-02-02', '02:02:02', '[1,2]')")
	tk.MustExec("insert into t values (null, null, null, null, '2000-03-03', '03:03:03', '[1,2,3]')")
	tk.MustExec("insert into t values (4, 4.4, 0.4, 'd', null, null, null)")

	tk.MustExec(fmt.Sprintf("select * from t into outfile '%v'", outfile))
	cmpAndRm(`1	1.1	0.10000	a	2000-01-01 00:00:00	2001-01-01 00:00:00	[1]
2	2.2	0.20000	b	2000-02-02 00:00:00	2002-02-02 00:00:00	[1, 2]
\N	\N	\N	\N	2000-03-03 00:00:00	2003-03-03 00:00:00	[1, 2, 3]
4	4.4	0.40000	d	\N	\N	\N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select * from t into outfile '%v' fields terminated by ',' enclosed by '\"' escaped by '#'", outfile))
	cmpAndRm(`"1","1.1","0.10000","a","2000-01-01 00:00:00","2001-01-01 00:00:00","[1]"
"2","2.2","0.20000","b","2000-02-02 00:00:00","2002-02-02 00:00:00","[1, 2]"
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","[1, 2, 3]"
"4","4.4","0.40000","d",#N,#N,#N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select * from t into outfile '%v' fields terminated by ',' optionally enclosed by '\"' escaped by '#'", outfile))
	cmpAndRm(`1,1.1,0.10000,"a","2000-01-01 00:00:00","2001-01-01 00:00:00","[1]"
2,2.2,0.20000,"b","2000-02-02 00:00:00","2002-02-02 00:00:00","[1, 2]"
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","[1, 2, 3]"
4,4.4,0.40000,"d",#N,#N,#N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select * from t into outfile '%v' fields terminated by ',' optionally enclosed by '\"' escaped by '#' lines terminated by '<<<\n'", outfile))
	cmpAndRm(`1,1.1,0.10000,"a","2000-01-01 00:00:00","2001-01-01 00:00:00","[1]"<<<
2,2.2,0.20000,"b","2000-02-02 00:00:00","2002-02-02 00:00:00","[1, 2]"<<<
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","[1, 2, 3]"<<<
4,4.4,0.40000,"d",#N,#N,#N<<<
`, outfile, c)
}

func (s *testSuite1) TestSelectIntoOutfileConstant(c *C) {
	tmpDir := os.TempDir()
	outfile := path.Join(tmpDir, "select-into-outfile.data")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(fmt.Sprintf("select 1, 2, 3, '4', '5', '6', 7.7, 8.8, 9.9, null into outfile '%v'", outfile)) // test constants
	cmpAndRm(`1	2	3	4	5	6	7.7	8.8	9.9	\N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select 1e10, 1e20, 1.234567e8, 0.000123e3, 1.01234567890123456789, 123456789e-10 into outfile '%v'", outfile))
	cmpAndRm(`10000000000	1e20	123456700	0.123	1.01234567890123456789	0.0123456789
`, outfile, c)
}
