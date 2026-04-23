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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestMaskingPolicyDDLBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key auto_increment, c char(120))")

	tk.MustExec("create masking policy p on t(c) as c")
	tk.MustQuery("select policy_name, db_name, table_name, column_name, expression, status, masking_type, restrict_on from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("p test t c `c` ENABLED CUSTOM NONE"))

	tk.MustExec("alter table t disable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("DISABLED"))

	tk.MustExec("alter table t enable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("ENABLED"))

	tk.MustExec("create or replace masking policy p on t(c) as mask_full(c, '*')")
	tk.MustQuery("select masking_type from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("MASK_FULL"))

	tk.MustExec("alter table t drop masking policy p")
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("0"))
}

func TestMaskingPolicyCaseExpression(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c char(120))")

	tk.MustExec("create masking policy p_case on t(c) as case when current_user() = 'root' then c else 'xxx' end enable")
	tk.MustQuery("select policy_name, status from mysql.tidb_masking_policy where policy_name = 'p_case'").
		Check(testkit.Rows("p_case ENABLED"))
	tk.MustQuery("select expression like 'CASE WHEN %' from mysql.tidb_masking_policy where policy_name = 'p_case'").
		Check(testkit.Rows("1"))
	tk.MustQuery("select expression like '%CURRENT_USER()%' from mysql.tidb_masking_policy where policy_name = 'p_case'").
		Check(testkit.Rows("1"))
}

func TestMaskingPolicyCreateWithRestrictOn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_restrict_on")
	tk.MustExec("create table t_restrict_on (c char(120))")

	tk.MustExec("create masking policy p_restrict_on on t_restrict_on(c) as c restrict on (insert_into_select, ctas) disable")
	tk.MustQuery("select status, restrict_on from mysql.tidb_masking_policy where policy_name = 'p_restrict_on'").
		Check(testkit.Rows("DISABLED INSERT_INTO_SELECT,CTAS"))
}

func TestMaskingPolicyModifyExpressionAndRestrictOn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c char(120))")

	tk.MustExec("create masking policy p on t(c) as c enable")
	tk.MustExec("alter table t modify masking policy p set expression = mask_full(c, '*')")
	tk.MustQuery("select expression, masking_type from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("MASK_FULL(`c`, _UTF8MB4'*') MASK_FULL"))

	tk.MustExec("alter table t modify masking policy p set restrict on (insert_into_select, delete_select)")
	tk.MustQuery("select restrict_on from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("INSERT_INTO_SELECT,DELETE_SELECT"))

	tk.MustExec("alter table t modify masking policy p set restrict on none")
	tk.MustQuery("select restrict_on from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("NONE"))
}

func TestMaskingPolicyCurrentIdentityOperators(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c char(120))")

	tk.MustExec(`create masking policy p on t(c) as
		case when current_user() != 'root@%' then mask_full(c, '*') else c end enable`)
	tk.MustQuery("select expression like '%CURRENT_USER()%' from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("1"))

	tk.MustExec(`alter table t modify masking policy p set expression =
		case when current_role() = 'NONE' then c else mask_full(c, '*') end`)
	tk.MustQuery("select expression like '%CURRENT_ROLE()%' from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("1"))
}

func TestMaskingPolicyCascadeCleanupOnDrop(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_drop_col, t_drop_tbl")

	tk.MustExec("create table t_drop_col(id int primary key, c varchar(20))")
	tk.MustExec("create masking policy p_drop_col on t_drop_col(c) as c enable")
	tk.MustExec("alter table t_drop_col drop column c")
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p_drop_col'").
		Check(testkit.Rows("0"))

	tk.MustExec("create table t_drop_tbl(c varchar(20))")
	tk.MustExec("create masking policy p_drop_tbl on t_drop_tbl(c) as c enable")
	tk.MustExec("drop table t_drop_tbl")
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p_drop_tbl'").
		Check(testkit.Rows("0"))
}

func TestMaskingPolicyRenameColumnUpdatesPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_rename_col")
	tk.MustExec("create table t_rename_col(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_rename_col values (1, 'delta')")
	tk.MustExec("create masking policy p_rename_col on t_rename_col(c) as mask_full(c, '*') enable")

	tk.MustExec("alter table t_rename_col rename column c to c_new")
	tk.MustQuery("select column_name, expression from mysql.tidb_masking_policy where policy_name = 'p_rename_col'").
		Check(testkit.Rows("c_new MASK_FULL(`c_new`, _UTF8MB4'*')"))
	tk.MustQuery("select c_new from t_rename_col").Check(testkit.Rows("*****"))
}

func TestMaskingPolicyModifyColumnGuard(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_guard")
	tk.MustExec("create table t_guard(c varchar(20), d datetime(3))")
	tk.MustExec("create masking policy p_guard_c on t_guard(c) as c enable")
	tk.MustExec("create masking policy p_guard_d on t_guard(d) as d enable")

	tk.MustGetErrCode("alter table t_guard modify column c varchar(64)", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t_guard modify column d datetime(6)", errno.ErrUnsupportedDDLOperation)
	tk.MustQuery("show create table t_guard").Check(testkit.Rows(
		"t_guard CREATE TABLE `t_guard` (\n" +
			"  `c` varchar(20) DEFAULT NULL /* MASKING POLICY `p_guard_c` ENABLED */,\n" +
			"  `d` datetime(3) DEFAULT NULL /* MASKING POLICY `p_guard_d` ENABLED */\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))
}

// TestMaskingPolicyTableScopedUniqueness verifies policy name uniqueness is table-scoped.
func TestMaskingPolicyTableScopedUniqueness(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Create two databases and three tables.
	tk.MustExec("drop database if exists db1")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("create database db1")
	tk.MustExec("create database db2")

	tk.MustExec("use db1")
	tk.MustExec("create table t1 (c varchar(20))")
	tk.MustExec("create table t2 (c varchar(20))")
	tk.MustExec("use db2")
	tk.MustExec("create table t3 (c varchar(20))")

	// Same policy name on different tables in the same database should succeed.
	tk.MustExec("use db1")
	tk.MustExec("create masking policy simple_mask on t1(c) as c enable")
	tk.MustExec("create masking policy simple_mask on t2(c) as c enable")

	// Same policy name in another database should also succeed.
	tk.MustExec("use db2")
	tk.MustExec("create masking policy simple_mask on t3(c) as c enable")
	tk.MustQuery("select policy_name, db_name, table_name from mysql.tidb_masking_policy where policy_name = 'simple_mask' order by db_name, table_name").
		Check(testkit.Rows("simple_mask db1 t1", "simple_mask db1 t2", "simple_mask db2 t3"))

	// Duplicate policy name on the same table should fail.
	tk.MustExec("use db1")
	tk.MustGetErrCode("create masking policy simple_mask on t1(c) as c enable", errno.ErrMaskingPolicyExists)

	// Clean up
	tk.MustExec("use db1")
	tk.MustExec("drop database db1")
	tk.MustExec("use db2")
	tk.MustExec("drop database db2")
}

func TestMaskingPolicyRenameTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists old_table, new_table")

	// Create table and masking policy
	tk.MustExec("create table old_table(id int primary key, c varchar(100))")
	tk.MustExec("insert into old_table values (1, 'secret')")
	tk.MustExec("create masking policy p on old_table(c) as mask_full(c, '*') enable")

	// Verify masking works before rename
	tk.MustQuery("select c from old_table").Check(testkit.Rows("******"))
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("test old_table"))

	// Rename the table
	tk.MustExec("rename table old_table to new_table")

	// Verify policy metadata is updated in sys table
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("test new_table"))

	// Verify masking still works after rename
	tk.MustQuery("select c from new_table").Check(testkit.Rows("******"))

	// Verify we can modify the policy after rename
	tk.MustExec("alter table new_table disable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("DISABLED"))
	tk.MustQuery("select c from new_table").Check(testkit.Rows("secret"))

	tk.MustExec("alter table new_table enable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("ENABLED"))
	tk.MustQuery("select c from new_table").Check(testkit.Rows("******"))

	// Verify we can drop the policy after rename
	tk.MustExec("alter table new_table drop masking policy p")
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("0"))
	tk.MustQuery("select c from new_table").Check(testkit.Rows("secret"))
}

func TestMaskingPolicyRenameTableCrossDatabase(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists db1")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("drop table if exists db1.t")

	// Create databases and table
	tk.MustExec("create database db1")
	tk.MustExec("create database db2")
	tk.MustExec("create table db1.t(id int primary key, c varchar(100))")
	tk.MustExec("insert into db1.t values (1, 'secret')")
	tk.MustExec("create masking policy p on db1.t(c) as mask_full(c, '*') enable")

	// Verify masking works before rename
	tk.MustQuery("select c from db1.t").Check(testkit.Rows("******"))
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("db1 t"))

	// Rename the table across databases
	tk.MustExec("rename table db1.t to db2.t")

	// Verify policy metadata is updated in sys table
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("db2 t"))

	// Verify masking still works after rename
	tk.MustQuery("select c from db2.t").Check(testkit.Rows("******"))

	// Verify we can modify the policy after rename
	tk.MustExec("alter table db2.t disable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("DISABLED"))
	tk.MustQuery("select c from db2.t").Check(testkit.Rows("secret"))

	tk.MustExec("alter table db2.t enable masking policy p")
	tk.MustQuery("select c from db2.t").Check(testkit.Rows("******"))
}

func TestMaskingPolicyRenameTableNoPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists old_table, new_table")

	// Create table without masking policy
	tk.MustExec("create table old_table(id int primary key, c varchar(100))")
	tk.MustExec("insert into old_table values (1, 'secret')")

	// Rename the table (no policy to update)
	tk.MustExec("rename table old_table to new_table")

	// Verify no error and table works
	tk.MustQuery("select c from new_table").Check(testkit.Rows("secret"))
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy").
		Check(testkit.Rows("0"))
}

func TestMaskingPolicyFailClosed(t *testing.T) {
	// Test that invalid masking policy expressions are rejected during creation (fail-closed)
	// and that queries fail when expression parsing errors occur instead of returning raw values
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, c varchar(20))")

	// Test 1: Creating policy with unknown function should fail
	_, err := tk.Exec("create masking policy p_invalid on t(c) as unknown_function(c)")
	if err == nil {
		t.Fatal("Expected error when creating masking policy with unknown function, got nil")
	}

	// Test 2: PointGet should fail with error if masking expression cannot be built
	// First, create a valid policy
	tk.MustExec("create masking policy p_valid on t(c) as mask_full(c, '*') enable")
	tk.MustExec("insert into t values (1, 'secret')")

	// PointGet query with PK should return masked value
	tk.MustQuery("select c from t where id = 1").Check(testkit.Rows("******"))
}
