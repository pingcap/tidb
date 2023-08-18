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
	"sort"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl/util/callback"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestCreateTableWithCheckConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	// Test column-type check constraint.
	tk.MustExec("create table t(a int not null check(a>0))")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	require.Equal(t, 1, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints
	require.Equal(t, int64(1), constrs[0].ID)
	require.True(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[0].Name)
	require.Equal(t, "`a` > 0", constrs[0].ExprString)

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a bigint key constraint my_constr check(a<10), b int constraint check(b > 1) not enforced)")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, 2, len(constrs))
	require.Equal(t, int64(1), constrs[0].ID)
	require.True(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("my_constr"), constrs[0].Name)
	require.Equal(t, "`a` < 10", constrs[0].ExprString)

	require.Equal(t, int64(2), constrs[1].ID)
	require.True(t, constrs[1].InColumn)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("b"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`b` > 1", constrs[1].ExprString)

	// Test table-type check constraint.
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int constraint check(a > 1) not enforced, constraint my_constr check(a < 10))")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, 2, len(constrs))
	// table-type check constraint.
	require.Equal(t, int64(1), constrs[0].ID)
	require.False(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("my_constr"), constrs[0].Name)
	require.Equal(t, "`a` < 10", constrs[0].ExprString)

	// column-type check constraint.
	require.Equal(t, int64(2), constrs[1].ID)
	require.True(t, constrs[1].InColumn)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`a` > 1", constrs[1].ExprString)

	// Test column-type check constraint fail on dependency.
	tk.MustExec("drop table t")
	_, err := tk.Exec("create table t(a int not null check(b>0))")
	require.Errorf(t, err, "[ddl:3813]Column check constraint 't_chk_1' references other column.")

	_, err = tk.Exec("create table t(a int not null check(b>a))")
	require.Errorf(t, err, "[ddl:3813]Column check constraint 't_chk_1' references other column.")

	_, err = tk.Exec("create table t(a int not null check(a>0), b int, constraint check(c>b))")
	require.Errorf(t, err, "[ddl:3820]Check constraint 't_chk_1' refers to non-existing column 'c'.")

	tk.MustExec("create table t(a int not null check(a>0), b int, constraint check(a>b))")
	tk.MustExec("drop table t")

	tk.MustExec("create table t(a int not null check(a > '12345'))")
	tk.MustExec("drop table t")

	tk.MustExec("create table t(a int not null primary key check(a > '12345'))")
	tk.MustExec("drop table t")

	tk.MustExec("create table t(a varchar(10) not null primary key check(a > '12345'))")
	tk.MustExec("drop table t")
}

func TestAlterTableAddCheckConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("create table t(a int not null check(a>0))")
	// Add constraint with name.
	tk.MustExec("alter table t add constraint haha check(a<10)")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints
	require.Equal(t, int64(2), constrs[1].ID)
	require.False(t, constrs[1].InColumn)
	require.True(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("haha"), constrs[1].Name)
	require.Equal(t, "`a` < 10", constrs[1].ExprString)

	// Add constraint without name.
	tk.MustExec("alter table t add constraint check(a<11) not enforced")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 1, len(constraintTable.Meta().Columns))
	require.Equal(t, 3, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, int64(3), constrs[2].ID)
	require.False(t, constrs[2].InColumn)
	require.False(t, constrs[2].Enforced)
	require.Equal(t, "t", constrs[2].Table.L)
	require.Equal(t, model.StatePublic, constrs[2].State)
	require.Equal(t, 1, len(constrs[2].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[2].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_2"), constrs[2].Name)
	require.Equal(t, "`a` < 11", constrs[2].ExprString)

	// Add constraint with the name has already existed.
	_, err := tk.Exec("alter table t add constraint haha check(a)")
	require.Errorf(t, err, "[schema:3822]Duplicate check constraint name 'haha'.")

	// Add constraint with the unknown column.
	_, err = tk.Exec("alter table t add constraint check(b)")
	require.Errorf(t, err, "[ddl:3820]Check constraint 't_chk_3' refers to non-existing column 'b'.")

	tk.MustExec("alter table t add constraint check(a*2 < a+1) not enforced")
	// test add check constraint verify remain data
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (2), (3)")
	tk.MustGetErrMsg("alter table t add constraint check(a < 2)", "[ddl:3819]Check constraint 't_chk_1' is violated.")
	tk.MustGetErrMsg("alter table t add constraint check(a < 2) not enforced", "[ddl:3819]Check constraint 't_chk_2' is violated.")
}

func TestAlterTableDropCheckConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("create table t(a int not null check(a>0), b int, constraint haha check(a < b), check(a<b+1))")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 3, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints

	require.Equal(t, int64(1), constrs[0].ID)
	require.False(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 2, len(constrs[0].ConstraintCols))
	sort.Slice(constrs[0].ConstraintCols, func(i, j int) bool {
		return constrs[0].ConstraintCols[i].L < constrs[0].ConstraintCols[j].L
	})
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("b"), constrs[0].ConstraintCols[1])
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.Equal(t, "`a` < `b`", constrs[0].ExprString)

	require.Equal(t, int64(2), constrs[1].ID)
	require.False(t, constrs[1].InColumn)
	require.True(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 2, len(constrs[1].ConstraintCols))
	sort.Slice(constrs[1].ConstraintCols, func(i, j int) bool {
		return constrs[1].ConstraintCols[i].L < constrs[1].ConstraintCols[j].L
	})
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("b"), constrs[1].ConstraintCols[1])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`a` < `b` + 1", constrs[1].ExprString)

	// Column check constraint will be appended to the table constraint list.
	// So the offset will be the last one, so is the id.
	require.Equal(t, int64(3), constrs[2].ID)
	require.True(t, constrs[2].InColumn)
	require.True(t, constrs[2].Enforced)
	require.Equal(t, "t", constrs[2].Table.L)
	require.Equal(t, model.StatePublic, constrs[2].State)
	require.Equal(t, 1, len(constrs[2].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[2].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_2"), constrs[2].Name)
	require.Equal(t, "`a` > 0", constrs[2].ExprString)

	// Drop a non-exist constraint
	_, err := tk.Exec("alter table t drop constraint not_exist_constraint")
	require.Errorf(t, err, "[ddl:3940]Constraint 'not_exist_constraint' does not exist")

	tk.MustExec("alter table t drop constraint haha")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[0].Name)
	require.Equal(t, model.NewCIStr("t_chk_2"), constrs[1].Name)

	tk.MustExec("alter table t drop constraint t_chk_2")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 1, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[0].Name)

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int check(a > 0))")
	tk.MustGetErrMsg("insert into t values(0)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("alter table t drop constraint t_chk_1")
	tk.MustExec("insert into t values(0)")
}

func TestAlterTableAlterCheckConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("create table t(a int not null check(a>0) not enforced, b int, constraint haha check(a < b))")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints

	require.Equal(t, int64(1), constrs[0].ID)
	require.False(t, constrs[0].InColumn)
	require.True(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 2, len(constrs[0].ConstraintCols))
	sort.Slice(constrs[0].ConstraintCols, func(i, j int) bool {
		return constrs[0].ConstraintCols[i].L < constrs[0].ConstraintCols[j].L
	})
	require.Equal(t, model.NewCIStr("a"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("b"), constrs[0].ConstraintCols[1])
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.Equal(t, "`a` < `b`", constrs[0].ExprString)

	require.Equal(t, int64(2), constrs[1].ID)
	require.True(t, constrs[1].InColumn)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, "t", constrs[1].Table.L)
	require.Equal(t, model.StatePublic, constrs[1].State)
	require.Equal(t, 1, len(constrs[1].ConstraintCols))
	require.Equal(t, model.NewCIStr("a"), constrs[1].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)
	require.Equal(t, "`a` > 0", constrs[1].ExprString)

	// Alter constraint alter constraint with unknown name.
	_, err := tk.Exec("alter table t alter constraint unknown not enforced")
	require.Errorf(t, err, "[ddl:3940]Constraint 'unknown' does not exist")

	// Alter table alter constraint with user name.
	tk.MustExec("alter table t alter constraint haha not enforced")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.False(t, constrs[0].Enforced)
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.False(t, constrs[1].Enforced)
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)

	// Alter table alter constraint with generated name.
	tk.MustExec("alter table t alter constraint t_chk_1 enforced")
	constraintTable = external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.False(t, constrs[0].Enforced)
	require.Equal(t, model.NewCIStr("haha"), constrs[0].Name)
	require.True(t, constrs[1].Enforced)
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[1].Name)

	// Alter table alter constraint will violate check.
	// Here a=1, b=0 doesn't satisfy "a < b" constraint.
	// Since "a<b" is not enforced, so the insert will success.

	//tk.MustExec("insert into t values(1, 0)")
	//_, err = tk.Exec("alter table t alter constraint haha enforced")
	//require.Errorf(t, err, "[table:3819]Check constraint 'haha' is violated."))
}

func TestDropColumnWithCheckConstraints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int check(a > 0), b int, c int, check(c > 0), check (b > c))")
	tk.MustExec("alter table t drop column a")
	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 2, len(constraintTable.Meta().Constraints))
	tk.MustGetErrMsg("alter table t drop column b", "[ddl:3959]Check constraint 't_chk_2' uses column 'b', hence column cannot be dropped or renamed.")
	tk.MustGetErrMsg("alter table t rename column c to c1", "[ddl:3959]Check constraint 't_chk_1' uses column 'c', hence column cannot be dropped or renamed.")
}

func TestCheckConstraintsNotEnforcedWorks(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int check(a > 0), b int check(b < 10) not enforced, c int)")
	tk.MustExec("alter table t alter constraint t_chk_1 not enforced")
	tk.MustExec("insert into t values(-1, 1, 0)")
	tk.MustExec("alter table t alter constraint t_chk_2 enforced")
	tk.MustGetErrMsg("insert into t values(-1, 11, 0)", "[table:3819]Check constraint 't_chk_2' is violated.")
	tk.MustExec("alter table t add check(c = 0)")
	tk.MustGetErrMsg("insert into t values(-1, 1, 1)", "[table:3819]Check constraint 't_chk_3' is violated.")
	tk.MustExec("alter table t alter constraint t_chk_3 not enforced")
	tk.MustExec("insert into t values(-1, 1, 0)")
}

func TestUnsupportedCheckConstraintsExprWhenCreateTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	// functions
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + NOW() > '2011-11-21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: now.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + CURRENT_TIMESTAMP() > '2011-11-21 01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_timestamp.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + CURRENT_TIMESTAMP > '2011-11-21 01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_timestamp.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 DATETIME CHECK (f1 + CURDATE() > '2011-11-21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: curdate.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 DATETIME CHECK (f1 + CURTIME() > '23:11:21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: curtime.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + CURRENT_DATE() > '2011-11-21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_date.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + CURRENT_DATE > '2011-11-21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_date.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + CURRENT_TIME() > '01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_time.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + CURRENT_TIME > '01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_time.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 DATETIME CHECK (f1 + LOCALTIME() > '23:11:21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtime.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 DATETIME CHECK (f1 + LOCALTIME > '23:11:21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtime.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + LOCALTIMESTAMP() > '2011-11-21 01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtimestamp.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + LOCALTIMESTAMP > '2011-11-21 01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtimestamp.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + UNIX_TIMESTAMP() > '2011-11-21 01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: unix_timestamp.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 DATETIME CHECK (f1 + UTC_DATE() > '2011-11-21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: utc_date.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 TIMESTAMP CHECK (f1 + UTC_TIMESTAMP() > '2011-11-21 01:02:03'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: utc_timestamp.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 DATETIME CHECK (f1 + UTC_TIME() > '23:11:21'));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: utc_time.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 INT CHECK (f1 + CONNECTION_ID() < 929));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: connection_id.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(32) CHECK (CURRENT_USER() != a));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_user.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(32) CHECK (CURRENT_USER != a));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_user.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(32) CHECK (SESSION_USER() != a));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: session_user.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(32) CHECK (VERSION() != a));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: version.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(1024), b INT CHECK (b + FOUND_ROWS() > 2000));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: found_rows.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a INT CHECK ((a + LAST_INSERT_ID()) < 929));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: last_insert_id.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(32) CHECK (SYSTEM_USER() != a));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: system_user.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(32) CHECK (USER() != a));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: user.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 FLOAT CHECK (f1 + RAND() < 929.929));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: rand.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a INT CHECK (a + ROW_COUNT() > 1000));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: row_count.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(1024), b VARCHAR(1024) CHECK (GET_LOCK(b,10) != 0));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: get_lock.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(1024), b VARCHAR(1024) CHECK (IS_FREE_LOCK(b) != 0));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: is_free_lock.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(1024), b VARCHAR(1024) CHECK (IS_USED_LOCK(b) != 0));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: is_used_lock.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(1024), b VARCHAR(1024) CHECK (RELEASE_LOCK(b) != 0));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: release_lock.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a VARCHAR(1024), b VARCHAR(1024), CHECK (RELEASE_ALL_LOCKS() != 0));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: release_all_locks.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 VARCHAR(1024), f2 VARCHAR(1024) CHECK (LOAD_FILE(f2) != NULL));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: load_file.")
	tk.MustGetErrMsg("CREATE TABLE t1 (id CHAR(40) CHECK(UUID() != id));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: uuid.")
	tk.MustGetErrMsg("CREATE TABLE t1 (id INT CHECK(UUID_SHORT() != id));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: uuid_short.")
	tk.MustGetErrMsg("CREATE TABLE t1 (id INT CHECK(SLEEP(id) != 0));", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: sleep.")
	// udf var and system var
	tk.MustExec("set @a = 1")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 int CHECK (f1 > @a))", "[ddl:3816]An expression of a check constraint 't1_chk_1' cannot refer to a user or system variable.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 int CHECK (f1 > @@session.tidb_mem_quota_query))", "[ddl:3816]An expression of a check constraint 't1_chk_1' cannot refer to a user or system variable.")
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 int CHECK (f1 > @@global.tidb_mem_quota_query))", "[ddl:3816]An expression of a check constraint 't1_chk_1' cannot refer to a user or system variable.")
	// auto-increment column
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 int primary key auto_increment, f2 int, CHECK (f1 != f2));", "[ddl:3818]Check constraint 't1_chk_1' cannot refer to an auto-increment column.")
	// default
	tk.MustGetErrMsg("CREATE TABLE t1 (f1 INT CHECK (f1 = default(f1)))", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: default.")
	// subquery
	tk.MustGetErrMsg("CREATE TABLE t1 (id INT CHECK (id != (SELECT 1)));", "[ddl:3815]An expression of a check constraint 't1_chk_1' contains disallowed function.")
	tk.MustGetErrMsg("CREATE TABLE t1 (a int check(a in (SELECT COALESCE(NULL, 1, 1))))", "[ddl:3815]An expression of a check constraint 't1_chk_1' contains disallowed function.")
}

func TestUnsupportedCheckConstraintsExprWhenAlterTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t1(f1 TIMESTAMP, f2 DATETIME, f3 INT, f4 VARCHAR(32), f5 FLOAT, f6 CHAR(40), f7 INT PRIMARY KEY AUTO_INCREMENT)")
	// functions
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + NOW() > '2011-11-21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: now.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + CURRENT_TIMESTAMP() > '2011-11-21 01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_timestamp.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + CURRENT_TIMESTAMP > '2011-11-21 01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_timestamp.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f2 + CURDATE() > '2011-11-21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: curdate.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f2 + CURTIME() > '23:11:21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: curtime.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + CURRENT_DATE() > '2011-11-21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_date.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + CURRENT_DATE > '2011-11-21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_date.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + CURRENT_TIME() > '01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_time.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + CURRENT_TIME > '01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_time.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f2 + LOCALTIME() > '23:11:21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtime.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f2 + LOCALTIME > '23:11:21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtime.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + LOCALTIMESTAMP() > '2011-11-21 01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtimestamp.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + LOCALTIMESTAMP > '2011-11-21 01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: localtimestamp.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + UNIX_TIMESTAMP() > '2011-11-21 01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: unix_timestamp.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f2 + UTC_DATE() > '2011-11-21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: utc_date.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 + UTC_TIMESTAMP() > '2011-11-21 01:02:03');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: utc_timestamp.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f2 + UTC_TIME() > '23:11:21');", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: utc_time.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f3 + CONNECTION_ID() < 929);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: connection_id.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (CURRENT_USER() != f4);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_user.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (CURRENT_USER != f4);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: current_user.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (SESSION_USER() != f4);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: session_user.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (VERSION() != f4);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: version.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f3 + FOUND_ROWS() > 2000);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: found_rows.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK ((f3 + LAST_INSERT_ID()) < 929);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: last_insert_id.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (SYSTEM_USER() != f4);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: system_user.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (USER() != f4);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: user.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f5 + RAND() < 929.929);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: rand.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f3 + ROW_COUNT() > 1000);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: row_count.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (GET_LOCK(f4,10) != 0);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: get_lock.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (IS_FREE_LOCK(f4) != 0);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: is_free_lock.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (IS_USED_LOCK(f4) != 0);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: is_used_lock.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (RELEASE_LOCK(f4) != 0);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: release_lock.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (RELEASE_ALL_LOCKS() != 0);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: release_all_locks.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (LOAD_FILE(f4) != NULL);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: load_file.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK(UUID() != f6);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: uuid.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK(UUID_SHORT() != f3);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: uuid_short.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK(SLEEP(f3) != 0);", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: sleep.")
	// udf var and system var
	tk.MustExec("set @a = 1")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f3 > @a)", "[ddl:3816]An expression of a check constraint 't1_chk_1' cannot refer to a user or system variable.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f3 > @@session.tidb_mem_quota_query)", "[ddl:3816]An expression of a check constraint 't1_chk_1' cannot refer to a user or system variable.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f3 > @@global.tidb_mem_quota_query)", "[ddl:3816]An expression of a check constraint 't1_chk_1' cannot refer to a user or system variable.")
	// auto-increment column
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f7 != f3);", "[ddl:3818]Check constraint 't1_chk_1' cannot refer to an auto-increment column.")
	// default
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f1 = default(f1))", "[ddl:3814]An expression of a check constraint 't1_chk_1' contains disallowed function: default.")
	// subquery
	tk.MustGetErrMsg("ALTER TABLE t1 ADD CHECK (f3 != (SELECT 1));", "[ddl:3815]An expression of a check constraint 't1_chk_1' contains disallowed function.")
	tk.MustGetErrMsg("ALTER TABLE t1 ADD check (f3 in (SELECT COALESCE(NULL, 1, 1)))", "[ddl:3815]An expression of a check constraint 't1_chk_1' contains disallowed function.")
}

func TestNameInCreateTableLike(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int check(a > 10), b int constraint bbb check(b > 5), c int, check(c < 0))")
	tk.MustExec("create table s like t")

	constraintTable := external.GetTableByName(t, tk, "test", "s")
	require.Equal(t, 3, len(constraintTable.Meta().Columns))
	require.Equal(t, 3, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints
	require.Equal(t, model.NewCIStr("s_chk_1"), constrs[0].Name)
	require.Equal(t, model.NewCIStr("s_chk_2"), constrs[1].Name)
	require.Equal(t, model.NewCIStr("s_chk_3"), constrs[2].Name)
	tk.MustGetErrMsg("insert into s(a) values(1)", "[table:3819]Check constraint 's_chk_2' is violated.")
	tk.MustGetErrMsg("insert into s(b) values(2)", "[table:3819]Check constraint 's_chk_3' is violated.")
	tk.MustGetErrMsg("insert into s(c) values(3)", "[table:3819]Check constraint 's_chk_1' is violated.")

	tk.MustExec("alter table s add check(a > 0)")
	tk.MustExec("alter table s add constraint aaa check(a > 0)")
	constraintTable = external.GetTableByName(t, tk, "test", "s")
	require.Equal(t, 5, len(constraintTable.Meta().Constraints))
	constrs = constraintTable.Meta().Constraints
	require.Equal(t, model.NewCIStr("s_chk_4"), constrs[3].Name)
	require.Equal(t, model.NewCIStr("aaa"), constrs[4].Name)
}

func TestInsertUpdateIgnoreWarningMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int check(a > 10))")
	tk.MustGetErrMsg("insert into t values(1),(11),(15)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("insert ignore into t values(1),(11),(15)")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 3819 Check constraint 't_chk_1' is violated."))
	tk.MustQuery("select a from t").Check(testkit.Rows("11]\n[15"))

	tk.MustExec("update ignore t set a = a-2")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 3819 Check constraint 't_chk_1' is violated."))
	tk.MustQuery("select a from t").Check(testkit.Rows("11]\n[13"))
}

func TestCheckConstraintForeignKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int, b int, index(a), index(a, b))")
	tk.MustGetErrMsg("create table s(a int, check (a > 0), foreign key (a) references t(a) on update cascade)",
		"[ddl:3823]Column 'a' cannot be used in a check constraint 's_chk_1': needed in a foreign key constraint referential action.")
	tk.MustGetErrMsg("create table s(a int, b int,  check (a > 0), foreign key (a, b) references t(a, b) on update cascade)",
		"[ddl:3823]Column 'a' cannot be used in a check constraint 's_chk_1': needed in a foreign key constraint referential action.")

	tk.MustExec("create table t1(a int, foreign key (a) references t(a) on update cascade)")
	tk.MustGetErrMsg("alter table t1 add check ( a > 0 )",
		"[ddl:3823]Column 'a' cannot be used in a check constraint 't1_chk_1': needed in a foreign key constraint referential action.")
	tk.MustExec("create table t2(a int, b int, foreign key (a, b) references t(a, b) on update cascade)")
	tk.MustGetErrMsg("alter table t2 add check ( a > 0 )",
		"[ddl:3823]Column 'a' cannot be used in a check constraint 't2_chk_1': needed in a foreign key constraint referential action.")
}

func TestCheckConstrainNonBoolExpr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustGetErrMsg("create table t(a int, check(a))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(1))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check('1'))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int check(a))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(1+1))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(1/2))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(a + 1/2))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int check(a + 1/2))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(true + 1))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(abs(1)))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(length(a)))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(length(1)))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(floor(1.1)))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check(mod(3, 2)))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")
	tk.MustGetErrMsg("create table t(a int, check('true'))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_1'.")

	tk.MustExec("create table t(a int, check(true))")
	tk.MustGetErrMsg("alter table t add check(a)", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(1)", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check('1')", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(1/2)", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(a + 1/2)", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(true + 1)", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(abs(1))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(length(a))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(length(1))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(length(1))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check(mod(3, 2))", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
	tk.MustGetErrMsg("alter table t add check('true')", "[ddl:3812]An expression of non-boolean type specified to a check constraint 't_chk_2'.")
}

func TestAlterAddCheckConstrainColumnBadErr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int)")
	tk.MustGetErrMsg("alter table t add check(b > 0)", "[ddl:1054]Unknown column 'b' in 'check constraint t_chk_1 expression'")
}

func TestCheckConstraintBoolExpr(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a json, b varchar(20))")
	tk.MustExec("alter table t add check (JSON_VALID(a))")
	tk.MustExec("alter table t add check (REGEXP_LIKE(b,'@'))")
}

func TestCheckConstraintNameMaxLength(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	var str64, str65 string
	for i := 0; i < 64; i++ {
		str64 += "a"
	}
	str65 = str64 + "a"
	// create table
	tk.MustExec(fmt.Sprintf("create table t(a int constraint %s check(a > 0))", str64))
	tk.MustExec("drop table t")
	tk.MustExec(fmt.Sprintf("create table t(a int, constraint %s check(a > 0))", str64))
	tk.MustExec("drop table t")
	tk.MustGetErrMsg(fmt.Sprintf("create table t(a int constraint %s check(a > 0))", str65), "[ddl:1059]Identifier name 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' is too long")
	tk.MustGetErrMsg(fmt.Sprintf("create table t(a int, constraint %s check(a > 0))", str65), "[ddl:1059]Identifier name 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' is too long")
	tk.MustGetErrMsg(fmt.Sprintf("create table %s(a int check(a > 0))", str64), "[ddl:1059]Identifier name 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_chk_1' is too long")
	tk.MustGetErrMsg(fmt.Sprintf("create table %s(a int, check(a > 0))", str64), "[ddl:1059]Identifier name 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_chk_1' is too long")
	// alter table
	tk.MustExec("create table t(a int)")
	tk.MustGetErrMsg(fmt.Sprintf("alter table t add constraint %s check(a > 0)", str65), "[ddl:1059]Identifier name 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' is too long")
	tk.MustExec(fmt.Sprintf("alter table t add constraint %s check(a > 0)", str64))
	tk.MustExec("drop table t")
	tk.MustExec(fmt.Sprintf("create table %s(a int)", str64))
	tk.MustGetErrMsg(fmt.Sprintf("alter table %s add check(a > 0)", str64), "[ddl:1059]Identifier name 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_chk_1' is too long")
}

func TestCheckConstraintNameCaseAndAccentSensitivity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int)")
	tk.MustExec("alter table t add constraint `cafe` check(a > 0)")
	tk.MustGetErrMsg("alter table t add constraint `CAFE` check(a > 0)", "[schema:3822]Duplicate check constraint name 'cafe'.")
	tk.MustExec("alter table t add constraint `café` check(a > 0)")
}

func TestCheckConstraintEvaluated(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int check(a > 0))")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("create table s(a int)")
	tk.MustExec("insert into s values(-1)")
	// insert
	tk.MustGetErrMsg("insert into t values(-1)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("insert into t values(1)")
	tk.MustGetErrMsg("insert into t(a) values(-1)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustGetErrMsg("insert into t set a = -1", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("insert into t set a = 1")
	tk.MustGetErrMsg("insert into t(a) select -1", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("insert into t(a) select 1")
	tk.MustGetErrMsg("insert into t(a) select a from s", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("insert into t(a) select a + 2 from s")
	// update
	tk.MustGetErrMsg("update t set a = -1", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("update t set a = 1")
	tk.MustGetErrMsg("update t set a = a-1", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("update t set a = a+1")
	tk.MustGetErrMsg("update t set a = -1 where a > 0", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("update t set a = 1 where a > 0")
	tk.MustGetErrMsg("update t set a = (select a from s where a < 0) where a > 0", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("update t set a = (select a + 2 from s where a < 0) where a > 0")
	tk.MustGetErrMsg("update t as a, s as b set a.a=b.a", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("update t as a, s as b set a.a=b.a + 2")
	// replace
	tk.MustGetErrMsg("replace into t(a) values(-1)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("replace into t(a) values(1)")
	tk.MustGetErrMsg("replace into t(a) select -1", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("replace into t(a) select 1")
	tk.MustGetErrMsg("replace into t set a = -1", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("replace into t set a = 1")
}

func TestGenerateColumnCheckConstraint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t( a int, b int as (a+1), CHECK (b > 0));")
	tk.MustExec("insert into t(a) values(0)")
	tk.MustGetErrMsg("insert into t(a) values(-2)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("alter table t alter constraint t_chk_1 not enforced")
	tk.MustExec("insert into t(a) values(-2)")

	constraintTable := external.GetTableByName(t, tk, "test", "t")
	require.Equal(t, 2, len(constraintTable.Meta().Columns))
	require.Equal(t, 1, len(constraintTable.Meta().Constraints))
	constrs := constraintTable.Meta().Constraints
	require.Equal(t, int64(1), constrs[0].ID)
	require.False(t, constrs[0].InColumn)
	require.False(t, constrs[0].Enforced)
	require.Equal(t, "t", constrs[0].Table.L)
	require.Equal(t, model.StatePublic, constrs[0].State)
	require.Equal(t, 1, len(constrs[0].ConstraintCols))
	require.Equal(t, model.NewCIStr("b"), constrs[0].ConstraintCols[0])
	require.Equal(t, model.NewCIStr("t_chk_1"), constrs[0].Name)
	require.Equal(t, "`b` > 0", constrs[0].ExprString)

	tk.MustExec("alter table t drop constraint t_chk_1")
	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int as (a+1))")
	tk.MustExec("alter table t add check(b > 0)")
	tk.MustExec("insert into t(a) values(0)")
	tk.MustGetErrMsg("insert into t(a) values(-2)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("alter table t alter constraint t_chk_1 not enforced")
	tk.MustExec("insert into t(a) values(-2)")
	tk.MustExec("alter table t drop constraint t_chk_1")
	tk.MustGetErrMsg("alter table t add check(b > 0)", "[ddl:3819]Check constraint 't_chk_1' is violated.")
}

func TestTemporaryTableCheckConstraint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int, CHECK (a < -999));")
	// local temporary table
	tk.MustExec("drop temporary table if exists t;")
	tk.MustExec("create temporary table t(a int, CHECK (a > 0));")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustGetErrMsg("insert into t(a) values(-2)", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("drop temporary table t")
	tk.MustExec("create temporary table t(a int, CHECK (a > 0) not enforced);")
	tk.MustExec("insert into t values(-1)")
	// global temporary table
	tk.MustExec("create global temporary table tt(a int, check(a > 0)) on commit delete rows")
	tk.MustExec("insert into tt(a) values(1)")
	tk.MustGetErrMsg("insert into tt(a) values(-2)", "[table:3819]Check constraint 'tt_chk_1' is violated.")
	tk.MustExec("alter table tt alter constraint tt_chk_1 not enforced")
	tk.MustExec("insert into tt(a) values(-2)")
	tk.MustExec("alter table tt drop constraint tt_chk_1")
}

func TestCheckConstraintWithPrepareInsertCheckConstraint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int CHECK (a != 0));")
	tk.MustExec("prepare stmt from 'insert into t values(?)'")
	tk.MustExec("set @a = 1")
	tk.MustExec("execute stmt using @a")
	tk.MustExec("set @a = 0")
	tk.MustGetErrMsg("execute stmt using @a", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("deallocate prepare stmt")
}

func TestCheckConstraintOnDuplicateKeyUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int primary key, b int, check (b > 0))")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustGetErrMsg("insert into t values(1, -10) on duplicate key update b = -1", "[table:3819]Check constraint 't_chk_1' is violated.")
	tk.MustExec("insert ignore into t values(1, -10) on duplicate key update b = -1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1"))
	tk.MustExec("create table s(a int primary key, check (a > 0))")
	tk.MustExec("insert into s values(1)")
	tk.MustGetErrMsg("insert into s values(1) on duplicate key update a = -1", "[table:3819]Check constraint 's_chk_1' is violated.")
	tk.MustExec("insert ignore into s values(1) on duplicate key update a = -1")
	tk.MustQuery("select * from s").Check(testkit.Rows("1"))
}

func TestCheckConstraintOnInsert(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP DATABASE IF EXISTS test_insert_check_constraint;")
	tk.MustExec("CREATE DATABASE test_insert_check_constraint;")
	tk.MustExec("USE test_insert_check_constraint;")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("CREATE TABLE t1 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0));")
	tk.MustGetErrMsg("insert into t1 values (2, 2)", "[table:3819]Check constraint 't1_chk_1' is violated.")
	tk.MustGetErrMsg("insert into t1 values (9, 2)", "[table:3819]Check constraint 't1_chk_2' is violated.")
	tk.MustGetErrMsg("insert into t1 values (14, -4)", "[table:3819]Check constraint 'c2_positive' is violated.")
	tk.MustGetErrMsg("insert into t1(c1) values (9)", "[table:3819]Check constraint 't1_chk_2' is violated.")
	tk.MustGetErrMsg("insert into t1(c2) values (-3)", "[table:3819]Check constraint 'c2_positive' is violated.")
	tk.MustExec("insert into t1 values (14, 4)")
	tk.MustExec("insert into t1 values (null, 4)")
	tk.MustExec("insert into t1 values (13, null)")
	tk.MustExec("insert into t1 values (null, null)")
	tk.MustExec("insert into t1(c1) values (null)")
	tk.MustExec("insert into t1(c2) values (null)")

	// Test generated column with check constraint.
	tk.MustExec("CREATE TABLE t2 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0), c3 int as (c1 + c2) check(c3 > 15));")
	tk.MustGetErrMsg("insert into t2(c1, c2) values (11, 1)", "[table:3819]Check constraint 't2_chk_3' is violated.")
	tk.MustExec("insert into t2(c1, c2) values (12, 7)")
}

func TestCheckConstraintOnUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP DATABASE IF EXISTS test_update_check_constraint;")
	tk.MustExec("CREATE DATABASE test_update_check_constraint;")
	tk.MustExec("USE test_update_check_constraint;")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("CREATE TABLE t1 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0));")
	tk.MustExec("insert into t1 values (11, 12), (12, 13), (13, 14), (14, 15), (15, 16);")
	tk.MustGetErrMsg("update t1 set c2 = -c2;", "[table:3819]Check constraint 'c2_positive' is violated.")
	tk.MustGetErrMsg("update t1 set c2 = c1;", "[table:3819]Check constraint 't1_chk_1' is violated.")
	tk.MustGetErrMsg("update t1 set c1 = c1 - 10;", "[table:3819]Check constraint 't1_chk_2' is violated.")
	tk.MustGetErrMsg("update t1 set c2 = -10 where c2 = 12;", "[table:3819]Check constraint 'c2_positive' is violated.")

	// Test generated column with check constraint.
	tk.MustExec("CREATE TABLE t2 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0), c3 int as (c1 + c2) check(c3 > 15));")
	tk.MustExec("insert into t2(c1, c2) values (11, 12), (12, 13), (13, 14), (14, 15), (15, 16);")
	tk.MustGetErrMsg("update t2 set c2 = c2 - 10;", "[table:3819]Check constraint 't2_chk_3' is violated.")
	tk.MustExec("update t2 set c2 = c2 - 5;")
}

func TestCheckConstraintOnUpdateWithPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("DROP DATABASE IF EXISTS test_update_check_constraint_hash;")
	tk.MustExec("CREATE DATABASE test_update_check_constraint_hash;")
	tk.MustExec("USE test_update_check_constraint_hash;")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")

	tk.MustExec("CREATE TABLE t1 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0)) partition by hash(c2) partitions 5;")
	tk.MustExec("insert into t1 values (11, 12), (12, 13), (13, 14), (14, 15), (15, 16);")
	tk.MustGetErrMsg("update t1 set c2 = -c2;", "[table:3819]Check constraint 'c2_positive' is violated.")
	tk.MustGetErrMsg("update t1 set c2 = c1;", "[table:3819]Check constraint 't1_chk_1' is violated.")
	tk.MustGetErrMsg("update t1 set c1 = c1 - 10;", "[table:3819]Check constraint 't1_chk_2' is violated.")
	tk.MustGetErrMsg("update t1 set c2 = -10 where c2 = 12;", "[table:3819]Check constraint 'c2_positive' is violated.")

	// Test generated column with check constraint.
	tk.MustExec("CREATE TABLE t2 (CHECK (c1 <> c2), c1 INT CHECK (c1 > 10), c2 INT CONSTRAINT c2_positive CHECK (c2 > 0), c3 int as (c1 + c2) check(c3 > 15)) partition by hash(c2) partitions 5;")
	tk.MustExec("insert into t2(c1, c2) values (11, 12), (12, 13), (13, 14), (14, 15), (15, 16);")
	tk.MustGetErrMsg("update t2 set c2 = c2 - 10;", "[table:3819]Check constraint 't2_chk_3' is violated.")
	tk.MustExec("update t2 set c2 = c2 - 5;")
}

func TestShowCheckConstraint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	// Create table with check constraint
	tk.MustExec("create table t(a int check (a>1), b int, constraint my_constr check(a<10))")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"CONSTRAINT `my_constr` CHECK ((`a` < 10)),\n" +
		"CONSTRAINT `t_chk_1` CHECK ((`a` > 1))\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Alter table add constraint.
	tk.MustExec("alter table t add constraint my_constr2 check (a<b) not enforced")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"CONSTRAINT `my_constr` CHECK ((`a` < 10)),\n" +
		"CONSTRAINT `t_chk_1` CHECK ((`a` > 1)),\n" +
		"CONSTRAINT `my_constr2` CHECK ((`a` < `b`)) /*!80016 NOT ENFORCED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Alter table drop constraint.
	tk.MustExec("alter table t drop constraint t_chk_1")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"CONSTRAINT `my_constr` CHECK ((`a` < 10)),\n" +
		"CONSTRAINT `my_constr2` CHECK ((`a` < `b`)) /*!80016 NOT ENFORCED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table if exists t")
}

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
			tableCommon.WritableConstraints = []*table.Constraint{}
			tableCommon.Constraints = []*table.Constraint{}
			// insert data
			tk1.MustExec("insert into t values(1)")
			// recover
			tableCommon.Constraints = originCons
			tableCommon.WritableConstraint()
		}
	}

	//StatNone  StateWriteReorganization
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/ddl/mockVerifyRemainDataSuccess", "return(true)"))
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc)
	d.SetHook(callback)
	tk.MustExec("alter table t add constraint c0 check ( a > 10)")
	tk.MustQuery("select * from t").Check(testkit.Rows("12", "1"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL,\nCONSTRAINT `c0` CHECK ((`a` > 10))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table t drop constraint c0")
	tk.MustExec("delete from t where a = 1")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/ddl/mockVerifyRemainDataSuccess"))
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

	var checkErr error
	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StatNone -> StateWriteOnly
	onJobUpdatedExportedFunc1 := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteOnly {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			originCons := tableCommon.Constraints
			tableCommon.WritableConstraints = []*table.Constraint{}
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

	var checkErr error
	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StateWriteOnly -> StateWriteReorganization
	onJobUpdatedExportedFunc2 := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteReorganization {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			tableCommon.Constraints[0].State = model.StateWriteOnly
			tableCommon.WritableConstraints = []*table.Constraint{}
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

	var checkErr error
	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StateWriteReorganization -> StatePublic
	onJobUpdatedExportedFunc3 := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StatePublic {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			tableCommon.Constraints[0].State = model.StateWriteReorganization
			tableCommon.WritableConstraints = []*table.Constraint{}
			// insert data
			tk1.MustGetErrMsg("insert into t values(1)", "[table:3819]Check constraint 'c3' is violated.")
			// recover
			tableCommon.Constraints[0].State = model.StatePublic
			tableCommon.WritableConstraint()
		}
	}
	callback.OnJobUpdatedExported.Store(&onJobUpdatedExportedFunc3)
	d.SetHook(callback)
	tk.MustExec("alter table t add constraint c3 check ( a > 10)")
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

	var checkErr error
	d := dom.DDL()
	originalCallback := d.GetHook()
	callback := &callback.TestDDLCallback{}
	// StateWriteReorganization -> StatePublic
	onJobUpdatedExportedFunc3 := func(job *model.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteReorganization {
			// set constraint state
			constraintTable := external.GetTableByName(t, tk1, "test", "t")
			tableCommon, ok := constraintTable.(*tables.TableCommon)
			require.True(t, ok)
			tableCommon.Constraints[0].State = model.StateWriteOnly
			tableCommon.WritableConstraints = []*table.Constraint{}
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

func TestIssue44689(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	for _, expr := range []string{"true", "false"} {
		tk.MustExec("DROP TABLE IF EXISTS t0, t1, t2")
		tk.MustExec(fmt.Sprintf("CREATE TABLE t0(c1 NUMERIC CHECK(%s))", expr))

		tk.MustExec(fmt.Sprintf("CREATE TABLE t1(c1 NUMERIC, CHECK(%s))", expr))

		tk.MustExec("CREATE TABLE t2(c1 NUMERIC)")
		tk.MustExec(fmt.Sprintf("ALTER TABLE t2 ADD CONSTRAINT CHECK(%s)", expr))
	}
}

func TestCheckConstraintSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int check(a > 0))")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 the switch of check constraint is off"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	tk.MustExec("drop table t")
	tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
	tk.MustExec("create table t(a int check(a > 0))")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL,\nCONSTRAINT `t_chk_1` CHECK ((`a` > 0))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("alter table t add constraint chk check(true)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("alter table t alter constraint chk not enforced")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("alter table t drop constraint chk")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("set @@global.tidb_enable_check_constraint = 0")
	tk.MustExec("alter table t drop constraint t_chk_1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 the switch of check constraint is off"))
	tk.MustExec("alter table t alter constraint t_chk_1 not enforced")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 the switch of check constraint is off"))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n  `a` int(11) DEFAULT NULL,\nCONSTRAINT `t_chk_1` CHECK ((`a` > 0))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}
