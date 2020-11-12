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

func randomSelectFilePath(testName string) string {
	return filepath.Join(os.TempDir(), fmt.Sprintf("select-into-%v-%v.data", testName, time.Now().Nanosecond()))
}

func (s *testSuite1) TestSelectIntoFileExists(c *C) {
	outfile := randomSelectFilePath("TestSelectIntoFileExists")
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

func (s *testSuite1) TestSelectIntoOutfileTypes(c *C) {
	outfile := randomSelectFilePath("TestSelectIntoOutfileTypes")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` ( `a` bit(10) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("INSERT INTO `t` VALUES (_binary '\\0'), (_binary '\\1'), (_binary '\\2'), (_binary '\\3');")
	tk.MustExec(fmt.Sprintf("SELECT * FROM t INTO OUTFILE %q", outfile))
	cmpAndRm("\x00\x00\n\x001\n\x002\n\x003\n", outfile, c)

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (col ENUM ('value1','value2','value3'));")
	tk.MustExec("INSERT INTO t values ('value1'), ('value2');")
	tk.MustExec(fmt.Sprintf("SELECT * FROM t INTO OUTFILE %q", outfile))
	cmpAndRm("value1\nvalue2\n", outfile, c)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t ( v json);")
	tk.MustExec(`insert into t values ('{"id": 1, "name": "aaa"}'), ('{"id": 2, "name": "xxx"}');`)
	tk.MustExec(fmt.Sprintf("SELECT * FROM t INTO OUTFILE %q", outfile))
	cmpAndRm(`{"id": 1, "name": "aaa"}
{"id": 2, "name": "xxx"}
`, outfile, c)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (v tinyint unsigned)")
	tk.MustExec("insert into t values (0), (1)")
	tk.MustExec(fmt.Sprintf("SELECT * FROM t INTO OUTFILE %q", outfile))
	cmpAndRm(`0
1
`, outfile, c)
}

func (s *testSuite1) TestSelectIntoOutfileFromTable(c *C) {
	outfile := randomSelectFilePath("TestSelectIntoOutfileFromTable")
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
	outfile := randomSelectFilePath("TestSelectIntoOutfileConstant")
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

func (s *testSuite1) TestDeliminators(c *C) {
	outfile := randomSelectFilePath("TestDeliminators")
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("CREATE TABLE `tx` (`a` varbinary(20) DEFAULT NULL,`b` int DEFAULT NULL)")
	err := tk.ExecToErr(fmt.Sprintf("select * from `tx` into outfile %q fields enclosed by '\"\"'", outfile))
	// enclosed by must be a single character
	c.Check(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "Field separator argument is not what is expected"), IsTrue, Commentf("err: %v", err))
	err = tk.ExecToErr(fmt.Sprintf("select * from `tx` into outfile %q fields escaped by 'gg'", outfile))
	// so does escaped by
	c.Check(err, NotNil)
	c.Assert(strings.Contains(err.Error(), "Field separator argument is not what is expected"), IsTrue, Commentf("err: %v", err))

	// since the above two test cases failed, it should not has outfile remained on disk
	_, err = os.Stat(outfile)
	c.Check(os.IsNotExist(err), IsTrue, Commentf("err: %v", err))

	tk.MustExec("insert into tx values (NULL, NULL);\n")
	tk.MustExec(fmt.Sprintf("select * from `tx` into outfile %q fields escaped by ''", outfile))
	// if escaped by is set as empty, then NULL should not be escaped
	cmpAndRm("NULL\tNULL\n", outfile, c)

	tk.MustExec("delete from tx")
	tk.MustExec("insert into tx values ('d\",\"e\",', 3), ('\\\\', 2)")
	tk.MustExec(fmt.Sprintf("select * from `tx` into outfile %q FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'", outfile))
	// enclosed by character & escaped by characters should be escaped, no matter what
	cmpAndRm("\"d\\\",\\\"e\\\",\",\"3\"\n\"\\\\\",\"2\"\n", outfile, c)

	tk.MustExec("delete from tx")
	tk.MustExec("insert into tx values ('a\tb', 1)")
	tk.MustExec(fmt.Sprintf("select * from `tx` into outfile %q FIELDS TERMINATED BY ',' ENCLOSED BY '\"' escaped by '\t' LINES TERMINATED BY '\\n'", outfile))
	// enclosed by character & escaped by characters should be escaped, no matter what
	cmpAndRm("\"a\t\tb\",\"1\"\n", outfile, c)

	tk.MustExec("delete from tx")
	tk.MustExec(`insert into tx values ('d","e",', 1)`)
	tk.MustExec(`insert into tx values (unhex("00"), 2)`)
	tk.MustExec(`insert into tx values ("\r\n\b\Z\t", 3)`)
	tk.MustExec(`insert into tx values (null, 4)`)
	tk.MustExec(fmt.Sprintf("select * from `tx` into outfile %q FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n'", outfile))
	// line terminator will be escaped
	cmpAndRm("\"d\\\",\\\"e\\\",\",\"1\"\n"+"\"\\0\",\"2\"\n"+"\"\r\\\n\b\032\t\",\"3\"\n"+"\\N,\"4\"\n", outfile, c)

	tk.MustExec("create table tb (s char(10), b bit(48), bb blob(6))")
	tk.MustExec("insert into tb values ('\\0\\b\\n\\r\\t\\Z', _binary '\\0\\b\\n\\r\\t\\Z', unhex('00080A0D091A'))")
	tk.MustExec(fmt.Sprintf("select * from tb into outfile %q", outfile))
	// bit type won't be escaped (verified on MySQL)
	cmpAndRm("\\0\b\\\n\r\\\t\032\t"+"\000\b\n\r\t\032\t"+"\\0\b\\\n\r\\\t\032\n", outfile, c)

	tk.MustExec("create table zero (a varchar(10), b varchar(10), c varchar(10))")
	tk.MustExec("insert into zero values (unhex('00'), _binary '\\0', '\\0')")
	tk.MustExec(fmt.Sprintf("select * from zero into outfile %q", outfile))
	// zero will always be escaped
	cmpAndRm("\\0\t\\0\t\\0\n", outfile, c)
	tk.MustExec(fmt.Sprintf("select * from zero into outfile %q fields enclosed by '\"'", outfile))
	// zero will always be escaped, including when being enclosed
	cmpAndRm("\"\\0\"\t\"\\0\"\t\"\\0\"\n", outfile, c)

	tk.MustExec("create table tt (a char(10), b char(10), c char(10))")
	tk.MustExec("insert into tt values ('abcd', 'abcd', 'abcd')")
	tk.MustExec(fmt.Sprintf("select * from tt into outfile %q fields terminated by 'a-' lines terminated by 'b--'", outfile))
	// when not escaped, the first character of both terminators will be escaped
	cmpAndRm("\\a\\bcda-\\a\\bcda-\\a\\bcdb--", outfile, c)
	tk.MustExec(fmt.Sprintf("select * from tt into outfile %q fields terminated by 'a-' enclosed by '\"' lines terminated by 'b--'", outfile))
	// when escaped, only line terminator's first character will be escaped
	cmpAndRm("\"a\\bcd\"a-\"a\\bcd\"a-\"a\\bcd\"b--", outfile, c)
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
