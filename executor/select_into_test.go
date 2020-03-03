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
	"path/filepath"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testkit"
)

func cmpAndRm(expected, outfile string, c *C) {
	content, err := ioutil.ReadFile(outfile)
	c.Assert(err, IsNil)
	c.Assert(string(content), Equals, expected)
	c.Assert(os.Remove(outfile), IsNil)
}

func (s *testSuite1) TestSelectIntoFileExists(c *C) {
	outfile := filepath.Join(os.TempDir(), fmt.Sprintf("TestSelectIntoFileExists-%v.data", time.Now().Nanosecond()))
	defer func() {
		c.Assert(os.Remove(outfile), IsNil)
	}()
	tk := testkit.NewTestKit(c, s.store)
	sql := fmt.Sprintf("select 1 into outfile %q", outfile)
	tk.MustExec(sql)
	err := tk.ExecToErr(sql)
	c.Assert(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "already exists") ||
		strings.Contains(err.Error(), "file exists"), IsTrue, Commentf("err: %v", err))
	c.Assert(strings.Contains(err.Error(), outfile), IsTrue)
}

func (s *testSuite1) TestSelectIntoOutfileFromTable(c *C) {
	tmpDir := os.TempDir()
	outfile := filepath.Join(tmpDir, "select-into-outfile.data")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (i int, r real, d decimal(10, 5), s varchar(100), dt datetime, ts timestamp, du time, j json)")
	tk.MustExec("insert into t values (1, 1.1, 0.1, 'a', '2000-01-01', '01:01:01', '01:01:01', '[1]')")
	tk.MustExec("insert into t values (2, 2.2, 0.2, 'b', '2000-02-02', '02:02:02', '02:02:02', '[1,2]')")
	tk.MustExec("insert into t values (null, null, null, null, '2000-03-03', '03:03:03', '03:03:03', '[1,2,3]')")
	tk.MustExec("insert into t values (4, 4.4, 0.4, 'd', null, null, null, null)")

	tk.MustExec(fmt.Sprintf("select * from t into outfile %q", outfile))
	cmpAndRm(`1	1.1	0.10000	a	2000-01-01 00:00:00	2001-01-01 00:00:00	01:01:01	[1]
2	2.2	0.20000	b	2000-02-02 00:00:00	2002-02-02 00:00:00	02:02:02	[1, 2]
\N	\N	\N	\N	2000-03-03 00:00:00	2003-03-03 00:00:00	03:03:03	[1, 2, 3]
4	4.4	0.40000	d	\N	\N	\N	\N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select * from t into outfile %q fields terminated by ',' enclosed by '\"' escaped by '#'", outfile))
	cmpAndRm(`"1","1.1","0.10000","a","2000-01-01 00:00:00","2001-01-01 00:00:00","01:01:01","[1]"
"2","2.2","0.20000","b","2000-02-02 00:00:00","2002-02-02 00:00:00","02:02:02","[1, 2]"
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","03:03:03","[1, 2, 3]"
"4","4.4","0.40000","d",#N,#N,#N,#N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select * from t into outfile %q fields terminated by ',' optionally enclosed by '\"' escaped by '#'", outfile))
	cmpAndRm(`1,1.1,0.10000,"a","2000-01-01 00:00:00","2001-01-01 00:00:00","01:01:01","[1]"
2,2.2,0.20000,"b","2000-02-02 00:00:00","2002-02-02 00:00:00","02:02:02","[1, 2]"
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","03:03:03","[1, 2, 3]"
4,4.4,0.40000,"d",#N,#N,#N,#N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select * from t into outfile %q fields terminated by ',' optionally enclosed by '\"' escaped by '#' lines terminated by '<<<\n'", outfile))
	cmpAndRm(`1,1.1,0.10000,"a","2000-01-01 00:00:00","2001-01-01 00:00:00","01:01:01","[1]"<<<
2,2.2,0.20000,"b","2000-02-02 00:00:00","2002-02-02 00:00:00","02:02:02","[1, 2]"<<<
#N,#N,#N,#N,"2000-03-03 00:00:00","2003-03-03 00:00:00","03:03:03","[1, 2, 3]"<<<
4,4.4,0.40000,"d",#N,#N,#N,#N<<<
`, outfile, c)
}

func (s *testSuite1) TestSelectIntoOutfileConstant(c *C) {
	tmpDir := os.TempDir()
	outfile := filepath.Join(tmpDir, "select-into-outfile.data")
	tk := testkit.NewTestKit(c, s.store)
	// On windows the outfile name looks like "C:\Users\genius\AppData\Local\Temp\select-into-outfile.data",
	// fmt.Sprintf("%q") is used otherwise the string become
	// "C:UsersgeniusAppDataLocalTempselect-into-outfile.data".
	tk.MustExec(fmt.Sprintf("select 1, 2, 3, '4', '5', '6', 7.7, 8.8, 9.9, null into outfile %q", outfile)) // test constants
	cmpAndRm(`1	2	3	4	5	6	7.7	8.8	9.9	\N
`, outfile, c)

	tk.MustExec(fmt.Sprintf("select 1e10, 1e20, 1.234567e8, 0.000123e3, 1.01234567890123456789, 123456789e-10 into outfile %q", outfile))
	cmpAndRm(`10000000000	1e20	123456700	0.123	1.01234567890123456789	0.0123456789
`, outfile, c)
}

func (s *testSuite1) TestDumpReal(c *C) {
	cases := []struct {
		val    float64
		dec    int
		result string
	}{
		{1.2, 1, "1.2"},
		{1.2, 2, "1.20"},
		{2, 2, "2.00"},
		{2.333, types.UnspecifiedLength, "2.333"},
		{1e14, types.UnspecifiedLength, "100000000000000"},
		{1e15, types.UnspecifiedLength, "1e15"},
		{1e-15, types.UnspecifiedLength, "0.000000000000001"},
		{1e-16, types.UnspecifiedLength, "1e-16"},
	}
	for _, testCase := range cases {
		tp := types.NewFieldType(mysql.TypeDouble)
		tp.Decimal = testCase.dec
		_, buf := executor.DumpRealOutfile(nil, nil, testCase.val, tp)
		c.Assert(string(buf), Equals, testCase.result)
	}
}
