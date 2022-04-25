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
//

package ddl_test

import (
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// This test file contains tests that test the expected or unexpected DDL error.
// For expected error, we use SQL to check it.
// For unexpected error, we mock a SQL job to check it.

func TestTableError(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table testDrop(a int)")
	// Schema ID is wrong, so dropping table is failed.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId", `return(-1)`))
	_, err := tk.Exec("drop table testDrop")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId"))

	// Table ID is wrong, so dropping table is failed.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockModifyJobTableId", `return(-1)`))
	_, err = tk.Exec("drop table testDrop")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockModifyJobTableId"))

	// Args is wrong, so creating table is failed.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockModifyJobArg", `return(true)`))
	_, err = tk.Exec("create table test.t1(a int)")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockModifyJobArg"))

	// Table exists, so creating table is failed.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId", `return(-1)`))
	_, err = tk.Exec("create table test.t1(a int)")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId"))
	// Table exists, so creating table is failed.
	tk.MustExec("create table test.t2(a int)")
	tk.MustGetErrCode("create table test.t2(a int)", errno.ErrTableExists)
}

func TestViewError(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	// Args is wrong, so creating view is failed.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockModifyJobArg", `return(true)`))
	_, err := tk.Exec("create view v as select * from t")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockModifyJobArg"))
}

func TestForeignKeyError(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table t1 (a int, FOREIGN KEY fk(a) REFERENCES t(a))")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId", `return(-1)`))
	_, err := tk.Exec("alter table t1 add foreign key idx(a) REFERENCES t(a)")
	require.Error(t, err)
	_, err = tk.Exec("alter table t1 drop index fk")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId"))
}

func TestIndexError(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t add index a(a)")

	// Schema ID is wrong.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId", `return(-1)`))
	_, err := tk.Exec("alter table t add index idx(a)")
	require.Error(t, err)
	_, err = tk.Exec("alter table t1 drop a")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId"))

	// for adding index
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockModifyJobArg", `return(true)`))
	_, err = tk.Exec("alter table t add index idx(a)")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop index a")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockModifyJobArg"))
}

func TestColumnError(t *testing.T) {
	store, clean := testkit.CreateMockStoreWithSchemaLease(t, testLease)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, aa int, ab int)")
	tk.MustExec("alter table t add index a(a)")

	// Invalid schema ID.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId", `return(-1)`))
	_, err := tk.Exec("alter table t add column ta int")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa")
	require.Error(t, err)
	_, err = tk.Exec("alter table t add column ta int, add column tb int")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa, drop column ab")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId"))

	// Invalid table ID.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockModifyJobTableId", `return(-1)`))
	_, err = tk.Exec("alter table t add column ta int")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa")
	require.Error(t, err)
	_, err = tk.Exec("alter table t add column ta int, add column tb int")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa, drop column ab")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockModifyJobTableId"))

	// Invalid argument.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/MockModifyJobArg", `return(true)`))
	_, err = tk.Exec("alter table t add column ta int")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa")
	require.Error(t, err)
	_, err = tk.Exec("alter table t add column ta int, add column tb int")
	require.Error(t, err)
	_, err = tk.Exec("alter table t drop column aa, drop column ab")
	require.Error(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/MockModifyJobArg"))

	tk.MustGetErrCode("alter table t add column c int after c5", errno.ErrBadField)
	tk.MustGetErrCode("alter table t drop column c5", errno.ErrCantDropFieldOrKey)
	tk.MustGetErrCode("alter table t add column c int after c5, add column d int", errno.ErrBadField)
	tk.MustGetErrCode("alter table t drop column ab, drop column c5", errno.ErrCantDropFieldOrKey)
}

func TestCreateDatabaseError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId", `return(-1)`))
	tk.MustExec("create database db1;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockModifyJobSchemaId"))
}

func TestRenameViewOverDifferentSchemaError(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	//init
	tk.MustExec("use test")
	tk.MustExec("drop database if exists test_2;")
	tk.MustExec("drop table if exists table_1;")
	tk.MustExec("drop view if exists view_1;")

	tk.MustExec("create database test_2;")
	tk.MustExec("create table table_1 (a int);")
	tk.MustExec("create view view_1 as select a from table_1;")

	//different schema
	tk.MustGetErrCode("rename table test.view_1 to test_2.view_1;", errno.ErrForbidSchemaChange)
	tk.MustGetErrMsg("rename table test.view_1 to test_2.view_1;",
		infoschema.ErrForbidSchemaChange.GenWithStackByArgs("test", "test_2").Error(),
	)
	tk.MustGetErrMsg("rename table test.view_1 to test_2.view_1;",
		"[schema:1450]Changing schema from 'test' to 'test_2' is not allowed.",
	)

	//same schema
	tk.MustExec("rename table test.view_1 to test.view_1000;")

}
