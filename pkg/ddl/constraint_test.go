// Copyright 2023-2023 PingCAP, Inc.
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
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/util/callback"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestAlterConstraintAddDrop(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t (a int check(a>1), b int, constraint a_b check(a<b))")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	_, err := tk1.Exec("insert into t (a, b) values(2, 3)")
	require.NoError(t, err)
	_, err = tk1.Exec("insert into t (a, b) values(3, 4)")
	require.NoError(t, err)
	_, err = tk1.Exec("insert into t (a, b) values(4, 3)")
	require.Error(t, err)

	var checkErr error
	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteOnly {
			_, checkErr = tk1.Exec("insert into t (a, b) values(5,6) ")
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	d.SetHook(callback)
	tk.MustExec("alter table t add constraint cc check ( b < 5 )")
	require.Errorf(t, err, "[table:3819]Check constraint 'cc' is violated.")

	tk.MustExec("alter table t drop constraint cc")
	require.Errorf(t, err, "[table:3819]Check constraint 'cc' is violated.")
	tk.MustExec("drop table if exists t")
}

func TestAlterAddConstraintStateChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t (a int)")

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("insert into t values(12)")

	var checkErr error
	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	onJobUpdatedExportedFunc := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteReorganization {
			tk1.MustQuery(fmt.Sprintf("select count(1) from `%s`.`%s` where not %s limit 1", "test", "t", "a > 10")).Check(testkit.Rows("0"))
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			originCons := tableCommon.Constraints
			tableCommon.Constraints = []*table.Constraint{}
			// insert data
			tk1.MustExec("insert into t values(1)")
			// recover
			tableCommon.Constraints = originCons
			tableCommon.WritableConstraint()
		}
	}

	//StatNone  StateWriteReorganization
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/mockVerifyRemainDataSuccess", "return(true)"))
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	d.SetHook(callback)
	tk.MustExec("alter table t add constraint c0 check ( a > 10)")
	tk.MustQuery("select * from t").Check(testkit.Rows("12", "1"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL,\nCONSTRAINT `c0` CHECK ((`a` > 10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table t drop constraint c0")
	tk.MustExec("delete from t where a = 1")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockVerifyRemainDataSuccess"))
}

func TestAlterAddConstraintStateChange1(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t (a int)")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("insert into t values(12)")

	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StatNone -> StateWriteOnly
	onJobUpdatedExportedFunc1 := func(job *model.Job) {
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteOnly {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			originCons := tableCommon.Constraints
			tableCommon.Constraints = []*table.Constraint{}
			// insert data
			tk1.MustExec("insert into t values(1)")
			// recover
			tableCommon.Constraints = originCons
			tableCommon.WritableConstraint()
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc1)
	d.SetHook(callback)
	tk.MustGetErrMsg("alter table t add constraint c1 check ( a > 10)", "[ddl:3819]Check constraint 'c1' is violated.")
	tk.MustQuery("select * from t").Check(testkit.Rows("12", "1"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("delete from t where a = 1")
}

func TestAlterAddConstraintStateChange2(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t (a int)")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("insert into t values(12)")

	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StateWriteOnly -> StateWriteReorganization
	onJobUpdatedExportedFunc2 := func(job *model.Job) {
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteReorganization {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			tableCommon.Constraints[0].State = model.StateWriteOnly
			// insert data
			tk1.MustGetErrMsg("insert into t values(1)", "[table:3819]Check constraint 'c2' is violated.")
			// recover
			tableCommon.Constraints[0].State = model.StateWriteReorganization
			tableCommon.WritableConstraint()
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc2)
	d.SetHook(callback)
	tk.MustExec("alter table t add constraint c2 check ( a > 10)")
	tk.MustQuery("select * from t").Check(testkit.Rows("12"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL,\nCONSTRAINT `c2` CHECK ((`a` > 10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table t drop constraint c2")
}

func TestAlterAddConstraintStateChange3(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t (a int)")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("insert into t values(12)")

	addCheckDone := false
	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StateWriteReorganization -> StatePublic
	onJobUpdatedExportedFunc3 := func(job *model.Job) {
		if job.Type != model.ActionAddCheckConstraint || job.TableName != "t" {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StatePublic && job.IsDone() {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			tableCommon.Constraints[0].State = model.StateWriteReorganization
			// insert data
			tk1.MustGetErrMsg("insert into t values(1)", "[table:3819]Check constraint 'c3' is violated.")
			// recover
			tableCommon.Constraints[0].State = model.StatePublic
			tableCommon.WritableConstraint()
			addCheckDone = true
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc3)
	d.SetHook(callback)
	tk.MustExec("alter table t add constraint c3 check ( a > 10)")
	// Issue TiDB#48123.
	for i := 0; i <= 100; i++ {
		if addCheckDone {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	tk.MustQuery("select * from t").Check(testkit.Rows("12"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL,\nCONSTRAINT `c3` CHECK ((`a` > 10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func TestAlterEnforcedConstraintStateChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t (a int, constraint c1 check (a > 10) not enforced)")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("insert into t values(12)")

	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StateWriteReorganization -> StatePublic
	onJobUpdatedExportedFunc3 := func(job *model.Job) {
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteReorganization {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			tableCommon.Constraints[0].State = model.StateWriteOnly
			// insert data
			tk1.MustGetErrMsg("insert into t values(1)", "[table:3819]Check constraint 'c1' is violated.")
			// recover
			tableCommon.Constraints[0].State = model.StateWriteReorganization
			tableCommon.WritableConstraint()
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc3)
	d.SetHook(callback)
	tk.MustExec("alter table t alter constraint c1 enforced")
	tk.MustQuery("select * from t").Check(testkit.Rows("12"))
}
