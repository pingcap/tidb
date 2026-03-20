// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestArrayType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set global pkdb_extra_data_type = on")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_extra_data_type = off")
	})
	tk.MustExec("create table t (a array, b json);")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` array DEFAULT NULL,\n" +
		"  `b` json DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("desc t").Check(testkit.Rows("a array YES  <nil> ", "b json YES  <nil> "))

	// Test for 1D array.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (v array);")
	tk.MustExec("insert into t values ('[10, 20, 30]');")
	tk.MustQuery("select array_element(v, 0) from t;").Check(testkit.Rows("10"))
	tk.MustQuery("select array_element(v, 5) from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select array_element(v, -1) from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select array_element(v, 0, 1) from t;").Check(testkit.Rows("<nil>"))
	err := tk.QueryToErr("select array_element(v, 'a') from t;")
	require.Equal(t, "invalid array index: a", err.Error())

	// Test for 2D array.
	tk.MustExec("delete from t;")
	tk.MustExec("insert into t values ('[[1,2], [3,4]]');")
	tk.MustQuery("select array_element(v, 0) from t;").Check(testkit.Rows("[1, 2]"))
	tk.MustQuery("select array_element(v, 0, 0) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("select array_element(v, 1, 0) from t;").Check(testkit.Rows("3"))

	// Non-array input returns input value.
	tk.MustQuery(`select array_element(cast('{"name": "a"}' as json), 0)`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select array_element(cast('1' as json), 0)`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select array_element(cast('1' as json), 1)`).Check(testkit.Rows("<nil>"))

	// Test valid array value when inserting.
	sqls := []string{
		"insert into t values ('1:2');",
		"insert into t values ('a');",
		"insert into t values ('1');",
		`insert into t values ('{"name": "a", "age": 10}');`, // valid json, but not array
	}
	for _, sql := range sqls {
		err := tk.ExecToErr(sql)
		require.Error(t, err, sql)
		require.Equal(t, "[types:8801]Invalid ARRAY value", err.Error(), sql)
	}
}

func TestXMLType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set global pkdb_extra_data_type = on")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_extra_data_type = off")
	})
	tk.MustExec("create table t (a int, b xml);")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` xml DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("desc t").Check(testkit.Rows("a int(11) YES  <nil> ", "b xml YES  <nil> "))
	tk.MustExec("insert into t values (1, '<root>v1</root>'), (2, '<root>v2</root>'), (3, '<root>v3</root>');")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 <root>v1</root>", "2 <root>v2</root>", "3 <root>v3</root>"))

	// test built-in function extractvalue
	cases := []struct {
		xml    string
		path   string
		result string
	}{
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a",
			result: "a1 a2",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a/b",
			result: "b1 b2",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a/b/c",
			result: "c1",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a/@aa1",
			result: "aa1",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a/@aa2",
			result: "aa2",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a/@*",
			result: "aa1 aa2",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "//*",
			result: "a1 b1 c1 b2 a2",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a//*",
			result: "b1 c1 b2",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a/b/c/ancestor::*",
			result: "a1 b1 b2 a2",
		},
		{
			xml:    `<a aa1="aa1" aa2="aa2">a1<b ba1="ba1">b1<c>c1</c>b2</b>a2</a>`,
			path:   "/a/b/c/ancestor-or-self::*",
			result: "a1 b1 c1 b2 a2",
		},
		{
			xml:    `<book id="1" category="database"><title>TiDB-Guide</title><author>Someone</author><price>39.9</price></book>`,
			path:   "//book/title",
			result: "TiDB-Guide",
		},
		{
			xml:    `<book id="1" category="database"><title>TiDB-Guide</title></book>`,
			path:   "//book/@category",
			result: "database",
		},
		{
			xml: `<library>
				<section>
					<book><title>Go101</title></book>
					<book><title>Go Concurrency</title></book>
				</section>
			  </library>`,
			path:   "//library/section/book[2]/title",
			result: "Go Concurrency",
		},
		{
			xml: `<library>
				<book id="1" category="database"><title>TiDB-Guide</title></book>
				<book id="2" category="programming"><title>Go 101</title></book>
			  </library>`,
			path:   `//book[@category="programming"]/title`,
			result: "Go 101",
		},
		{
			xml: `<library>
				<book><title>Go 101</title><author>Tom</author></book>
				<book><title>TiDB-Guide</title><author>Someone</author></book>
			  </library>`,
			path:   `//book[author="Someone"]/title`,
			result: "TiDB-Guide",
		},
		{
			xml: `<books>
				<book><title>Book A</title><price>20</price></book>
				<book><title>Book B</title><price>50</price></book>
			  </books>`,
			path:   "//book[price>30]/title",
			result: "Book B",
		},
		{
			xml: `<authors>
				<author>Tom</author>
				<author>Jerry</author>
			  </authors>`,
			path:   "//author",
			result: "Tom Jerry",
		},
		{
			xml:    `<data>hello</data>`,
			path:   "/data",
			result: "hello",
		},
		{
			xml:    `<root><item>1</item></root>`,
			path:   "//notfound",
			result: "",
		},
		{
			xml: `<users>
				<user id="1" role="admin"><name>Alice</name></user>
				<user id="2" role="guest"><name>Bob</name></user>
			  </users>`,
			path:   `//user[@role="guest"]/name`,
			result: "Bob",
		},
		{
			xml:    `<note><to>Tove</to><from>Jani</from></note>`,
			path:   "//note/to/text()",
			result: "Tove",
		},
		{
			xml: `<servers>
				<server name="db1" port="4000" />
				<server name="db2" port="5000" />
			  </servers>`,
			path:   `//server[@port="5000"]/@name`,
			result: "db2",
		},
		{
			xml:    `<a><b>v</b></a>`,
			path:   "/a",
			result: "",
		},
		{
			xml:    `<a><![CDATA[x]]><b>v</b><![CDATA[y]]></a>`,
			path:   "/a",
			result: "x y",
		},
		{
			xml:    `<a>a1<b>v</b>a2</a>`,
			path:   "/a/text()",
			result: "a1 a2",
		},
		{
			xml:    `<a>a</a>`,
			path:   `/a | /a/text()`,
			result: "a",
		},
		{
			xml:    `<a><b>b1</b><b>b2</b></a>`,
			path:   `string-length("x")`,
			result: "1",
		},
		{
			xml:    `<a><b>B</b></a>`,
			path:   `string-length(/a)`,
			result: "0",
		},
		{
			xml:    `<ns:element xmlns:ns="myns"/>`,
			path:   `count(ns:element)`,
			result: "1",
		},
	}
	tk.MustExec("delete from t;")
	for i, ca := range cases {
		sql := fmt.Sprintf("insert into t values ( %d, '%s');", i, ca.xml)
		tk.MustExec(sql)
		tk.MustQuery(fmt.Sprintf("select b from t where a = %d;", i)).Check(testkit.Rows(ca.xml))
		tk.MustQuery(fmt.Sprintf("select extractvalue(b, '%s') from t where a = %d;", ca.path, i)).Check(testkit.Rows(ca.result))
		tk.MustQuery(fmt.Sprintf("select extractvalue('%s', '%s')", ca.xml, ca.path)).Check(testkit.Rows(ca.result))
	}

	tk.MustExec(`SET @xml = '<a>\n  <b>v</b>\n</a>'`)
	rows := tk.MustQuery(`SELECT EXTRACTVALUE(@xml, '/a/text()')`).Rows()
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1)
	require.Equal(t, "\n   \n", rows[0][0])
	tk.MustQuery(`SELECT HEX(EXTRACTVALUE(@xml, '/a/text()'))`).Check(testkit.Rows("0A2020200A"))
	tk.MustQuery(`SELECT LENGTH(EXTRACTVALUE(@xml, '/a/text()'))`).Check(testkit.Rows("5"))

	tk.MustQuery(`select extractvalue('<a><b></a>', '/a')`).Check(testkit.Rows("<nil>"))
	warnings := tk.MustQuery("show warnings").Rows()
	require.Len(t, warnings, 1)
	require.Equal(t, "Warning", warnings[0][0])
	require.Equal(t, "1525", warnings[0][1])
	require.Contains(t, warnings[0][2], "Incorrect XML value")

	err := tk.QueryToErr(`select extractvalue('<a>1</a>', '/a[')`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "1105")
	require.Contains(t, err.Error(), "XPATH syntax error: '/a['")

	// Test valid xml value when inserting.
	values := []string{
		"<root>boss</root>",
		"<root><element>value</element></root>",
		`<root><item>123</item></root>`,
		`<a><b><c></c></b></a>`,
		`<note><to>Tove</to><from>Jani</from></note>`,
		`<?xml version="1.0"?><data></data>`,
	}
	for i, v := range values {
		tk.MustExec("delete from t;")
		sql := fmt.Sprintf("insert into t values ( %d, '%s');", i, v)
		tk.MustExec(sql)
		tk.MustQuery("select * from t;").Check(testkit.Rows(fmt.Sprintf("%d %s", i, v)))
	}

	// Test invalid xml value when inserting.
	values = []string{
		`<root><item>123</root>`,
		`<root><item></root></item>`,
		`<root attr="unterminated></root>`,
		`<root><item>1<item></root>`,
		"<a>",
		"<element>value",
		"<root><element>value</element>",
	}
	for i, v := range values {
		tk.MustExec("delete from t;")
		sql := fmt.Sprintf("insert into t values ( %d, '%s');", i+1, v)
		err := tk.ExecToErr(sql)
		require.Error(t, err, sql)
		require.Equal(t, "[types:8801]Invalid XML value", err.Error(), sql)
	}
}

func TestIntervalYearToMonthType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set global pkdb_extra_data_type = on")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_extra_data_type = off")
	})

	// Test create table with interval year to month
	tk.MustExec("create table t (date_t interval year(3) to month, id int key, name varchar(10))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `date_t` interval year(3) to month DEFAULT NULL,\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `name` varchar(10) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("desc t").Check(testkit.Rows(
		"date_t interval year(3) to month YES  <nil> ",
		"id int(11) NO PRI <nil> ",
		"name varchar(10) YES  <nil> ",
	))

	// Test insert values
	tk.MustExec("insert into t values('2-11',1,'ZTE'),('10-4',2,'zte'),('1-0',9,'ZTe'),('-1-1',12,'Zte');")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"2-11 1 ZTE",
		"10-4 2 zte",
		"1-0 9 ZTe",
		"-1-1 12 Zte",
	))

	// tk.MustExecToErr("insert into t values('10-13',10,'ZTE')")

	// Test select with condition
	tk.MustQuery("select * from t where date_t='2-11'").Check(testkit.Rows("2-11 1 ZTE"))

	// Test update
	tk.MustExec("update t set date_t='20-11' where id=2")
	tk.MustQuery("select * from t where id=2").Check(testkit.Rows("20-11 2 zte"))

	// Test delete
	tk.MustExec("delete from t where id=1")
	tk.MustQuery("select * from t").Check(testkit.Rows(
		"20-11 2 zte",
		"1-0 9 ZTe",
		"-1-1 12 Zte",
	))

	// Test DATE + INTERVAL operations

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int key, date_col date)")

	// Test DATE_ADD with positive interval
	tk.MustExec("insert into t1 values(12, date_add('20121212', interval '1-1' year to month))")

	// Test DATE_SUB with positive interval
	tk.MustExec("insert into t1 values(13, date_sub('20121212', interval '1-1' year to month))")

	// Test DATE_ADD with negative interval
	tk.MustExec("insert into t1 values(14, date_add('20121212', interval '-1-1' year to month))")

	// Verify results
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows(
		"12 2014-01-12",
		"13 2011-11-12",
		"14 2011-11-12",
	))

	// Test Invalid Input
	invalidCases := []string{
		"invalid",
		"88-18",
		"1000-1",
		"1-12",
		"1-123",
		"",
		"- 1-1",
	}
	for i, v := range invalidCases {
		err := tk.ExecToErr(fmt.Sprintf("insert into t values('%s',%d,'test')", v, 100+i))
		require.EqualError(t, err, "[types:8801]Invalid Interval value", v)
	}

	// Test drop table
	tk.MustExec("drop table t")
	tk.MustExec("drop table t1")
}

func TestIntervalDayToSecondType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set global pkdb_extra_data_type = on")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_extra_data_type = off")
	})

	// Test create table with interval day to second
	tk.MustExec("create table t (itv interval day(3) to second, id int key, name varchar(10));")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `itv` interval day(3) to second DEFAULT NULL,\n" +
		"  `id` int(11) NOT NULL,\n" +
		"  `name` varchar(10) DEFAULT NULL,\n" +
		"  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustQuery("desc t").Check(testkit.Rows(
		"itv interval day(3) to second YES  <nil> ",
		"id int(11) NO PRI <nil> ",
		"name varchar(10) YES  <nil> ",
	))

	// Test insert values + canonicalize.
	tk.MustExec("insert into t values ('2 11:04:05',1,'a'), (' 02  3:4:5 ',2,'b'), ('-1 1:1:1',3,'c');")
	tk.MustQuery("select itv, id, name from t order by id").Check(testkit.Rows(
		"2 11:04:05 1 a",
		"2 03:04:05 2 b",
		"-1 01:01:01 3 c",
	))

	// Test select with condition.
	tk.MustQuery("select * from t where itv='2 11:04:05'").Check(testkit.Rows("2 11:04:05 1 a"))

	// Test update.
	tk.MustExec("update t set itv='0 0:0:0' where id=1")
	tk.MustQuery("select itv from t where id=1").Check(testkit.Rows("0 00:00:00"))

	// Test DATE_ADD/DATE_SUB with DAY TO SECOND time unit.
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int key, dt datetime)")
	tk.MustExec("insert into t1 values (1, date_add('2012-12-12 00:00:00', interval '1 01:01:01' day to second));")
	tk.MustExec("insert into t1 values (2, date_sub('2012-12-12 00:00:00', interval '1 01:01:01' day to second));")
	tk.MustExec("insert into t1 values (3, date_add('2012-12-12 00:00:00', interval '-1 01:01:01' day to second));")
	tk.MustQuery("select * from t1 order by id").Check(testkit.Rows(
		"1 2012-12-13 01:01:01",
		"2 2012-12-10 22:58:59",
		"3 2012-12-10 22:58:59",
	))

	// Test invalid input.
	invalidCases := []string{
		"invalid",
		"",
		"- 1 01:01:01",
		"1 24:00:00",
		"1 00:60:00",
		"1 00:00:60",
		"1000 00:00:00",
		"1 00:00",
		"1 00:00:00.1",
	}
	for i, v := range invalidCases {
		err := tk.ExecToErr(fmt.Sprintf("insert into t values('%s',%d,'x')", v, 100+i))
		require.EqualError(t, err, "[types:8801]Invalid Interval value", v)
	}

	tk.MustExec("drop table t")
	tk.MustExec("drop table t1")
}

func TestPKDBExtraDataType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set global pkdb_extra_data_type = off")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_extra_data_type = off")
	})
	tk.MustExec("create table t (a array, b xml, c interval year(3) to month, d interval day(3) to second);")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` json DEFAULT NULL,\n" +
		"  `b` longblob DEFAULT NULL,\n" +
		"  `c` int(3) DEFAULT NULL,\n" +
		"  `d` int(3) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t;")

	tk.MustExec("set global pkdb_extra_data_type = on")
	tk.MustExec("create table t (a array, b xml, c interval year(3) to month, d interval day(3) to second);")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` array DEFAULT NULL,\n" +
		"  `b` xml DEFAULT NULL,\n" +
		"  `c` interval year(3) to month DEFAULT NULL,\n" +
		"  `d` interval day(3) to second DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}
