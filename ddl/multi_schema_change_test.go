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
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (1);")
	tk.MustExec("alter table t add column (b int default 2, c int default 3);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int);")
	tk.MustExec("insert into t values (1, 2, 3);")
	tk.MustExec("alter table t add column (d int default 4, e int default 5);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 4 5"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1);")
	tk.MustExec("insert into t values ();")
	tk.MustExec("alter table t add column if not exists (b int default 2, c int default 3);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3"))
	tk.MustExec("alter table t add column if not exists (c int default 3, d int default 4);")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Note 1060 Duplicate column name 'c'"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 4"))

	// Test referencing previous column in multi-schema change is not supported.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int);")
	tk.MustGetErrCode("alter table t add column b int after a, add column c int after b", errno.ErrBadField)
	tk.MustGetErrCode("alter table t add column c int after b, add column b int", errno.ErrBadField)

	// Test add multiple columns with different position.
	tk.MustExec("drop table if exists t;")
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

	// Test add generate column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustExec("alter table t add column c double default 3.0, add column d double as (a + b);")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2 3 3"))

	// Test add columns with same name
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int default 1, c int default 4);")
	tk.MustGetErrCode("alter table t add column b int default 2, add column b int default 3", errno.ErrUnsupportedDDLOperation)

	// Test add generate column dependents on a modifying column
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustGetErrCode("alter table t modify column b double, add column c double as (a + b);", errno.ErrUnsupportedDDLOperation)
}
