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

	// test built-in function xpath
	cases := []struct {
		xml    string
		path   string
		result string
	}{
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
			result: "Tom,Jerry",
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
	}
	tk.MustExec("delete from t;")
	for i, ca := range cases {
		sql := fmt.Sprintf("insert into t values ( %d, '%s');", i, ca.xml)
		tk.MustExec(sql)
		tk.MustQuery(fmt.Sprintf("select b from t where a = %d;", i)).Check(testkit.Rows(ca.xml))
		tk.MustQuery(fmt.Sprintf("select xpath(b, '%s') from t where a = %d;", ca.path, i)).Check(testkit.Rows(ca.result))
		tk.MustQuery(fmt.Sprintf("select xpath('%s', '%s')", ca.xml, ca.path)).Check(testkit.Rows(ca.result))
	}

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

func TestPKDBExtraDataType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set global pkdb_extra_data_type = off")
	t.Cleanup(func() {
		tk.MustExec("set global pkdb_extra_data_type = off")
	})
	tk.MustExec("create table t (a array, b xml);")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` json DEFAULT NULL,\n" +
		"  `b` longblob DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t;")

	tk.MustExec("set global pkdb_extra_data_type = on")
	tk.MustExec("create table t (a array, b xml);")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` array DEFAULT NULL,\n" +
		"  `b` xml DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}
