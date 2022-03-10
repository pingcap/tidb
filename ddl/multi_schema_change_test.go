// Copyright 2022 PingCAP, Inc.
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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestMultiSchemaChangeAddColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	// Test add multiple columns in multiple specs.
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column b int default 2, add column c int default 3;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))

	// Test add multiple columns in one spec.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column (b int default 2, c int default 3);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))

	// Test referencing previous column in multi-schema change is not supported.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int);")
	tk.MustGetErrCode("alter table t add column b int after a, add column c int after b", errno.ErrBadField)
	tk.MustGetErrCode("alter table t add column c int after b, add column b int", errno.ErrBadField)

	// Test add multiple columns with different position.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec(`alter table t
        add column d int default 4 first,
        add column e int default 5 after b,
        add column f int default 6 after b;`)
	tk.MustQuery("select * from t;").Check(testkit.Rows("4 1 2 6 5 3"))

	// Test [if not exists] for adding columns.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column b int default 2, add column if not exists a int;")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1060 Duplicate column name 'a'"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2"))
}

func TestMultiSchemaChangeDropColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	tk.MustExec("create table t (a int, b int);")
	tk.MustGetErrCode("alter table t drop column a, drop column b;", errno.ErrCantRemoveAllFields)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, d int, e int);")
	tk.MustExec("insert into t values (1, 2, 3, 4, 5);")
	tk.MustExec("alter table t drop column a, drop column d, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 5"))
}

func TestMultiSchemaChangeAddDropColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1;")

	// [a, b] -> [+c, -a, +d, -b] -> [c, d]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column c int default 3, drop column a, add column d int default 4, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4"))

	// [a, b] -> [-a, -b, +c, +d] -> [c, d]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t drop column a, drop column b, add column c int default 3, add column d int default 4;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("3 4"))

	// [a, b] -> [+c after a, +d first, -a, -b] -> [d, c]
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, b int default 2);")
	tk.MustExec("insert into t values ();")
	// Note that MariaDB does not support this: Unknown column 'a' in 't'.
	// Since TiDB's implementation is snapshot + reasonable cascading, this is supported.
	tk.MustExec("alter table t add column c int default 3 after a, add column d int default 4 first, drop column a, drop column b;")
	tk.MustQuery("select * from t;").Check(testkit.Rows("4 3"))
}

func TestMultiSchemaChangeOperateSameColumn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.tidb_enable_change_multi_schema = 1")

	tk.MustExec("create table t (a int, c int)")
	_, err := tk.Exec("alter table t add column b int default 2, add column b int default 3")
	require.Equal(t, err.Error(), "[ddl:8200]Unsupported operate same column 'b'")
	_, err = tk.Exec("alter table t drop column a, drop column a")
	require.Equal(t, err.Error(), "[ddl:8200]Unsupported operate same column 'a'")
}
