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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestArrayType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
