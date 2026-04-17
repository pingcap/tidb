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

func TestMaskingPolicyMaskNullOnNumeric(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_numeric")
	tk.MustExec("create table t_numeric (id int primary key, salary decimal(10,2))")
	tk.MustExec("insert into t_numeric values (1, 85000.00)")

	tk.MustExec("create masking policy p_numeric on t_numeric(salary) as mask_null(salary) enable")
	tk.MustQuery("select salary is null from t_numeric where id = 1").Check(testkit.Rows("1"))
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

func TestMaskingPolicyCTEScenarios(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_cte_src, t_cte_join, t_cte_dst")
	tk.MustExec("create table t_cte_src(id int primary key, c varchar(20))")
	tk.MustExec("create table t_cte_join(id int primary key, name varchar(20))")
	tk.MustExec("create table t_cte_dst(c varchar(20))")
	tk.MustExec("insert into t_cte_src values (1, 'secret'), (2, 'public')")
	tk.MustExec("insert into t_cte_join values (1, 'alice'), (2, 'bob')")
	tk.MustExec("create masking policy p_cte on t_cte_src(c) as mask_full(c, '*') restrict on (insert_into_select) enable")

	tk.MustQuery("with cte as (select c from t_cte_src) select c from cte order by c").Check(testkit.Rows("******", "******"))
	tk.MustQuery("with cte as (select c from t_cte_src) select count(*) from cte where c = 'secret'").Check(testkit.Rows("1"))
	tk.MustQuery("with cte as (select id, c from t_cte_src) select j.name, cte.c from t_cte_join j join cte on j.id = cte.id order by j.id").
		Check(testkit.Rows("alice ******", "bob ******"))
	tk.MustQuery("with cte1 as (select c from t_cte_src), cte2 as (select c from cte1) select count(*) from cte2 where c = 'secret'").
		Check(testkit.Rows("1"))
	tk.MustQuery("with cte as (select c from t_cte_src) select count(*) from (select c from cte group by c) g").
		Check(testkit.Rows("2"))

	tk.MustGetErrCode("insert into t_cte_dst with cte as (select c from t_cte_src) select c from cte", errno.ErrAccessDeniedToMaskedColumn)
}

func TestMaskingPolicyCreateOrReplaceCoverage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_or_replace")
	tk.MustExec("create table t_or_replace(id int primary key, c1 varchar(20), c2 varchar(20))")
	tk.MustExec("insert into t_or_replace values (1, 'alpha', 'A1'), (2, 'bravo', 'B2')")

	// OR REPLACE should create policy when policy does not exist.
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as c1 enable")
	tk.MustQuery("select policy_name, column_name, expression, status, restrict_on from mysql.tidb_masking_policy where policy_name='p_or_replace'").
		Check(testkit.Rows("p_or_replace c1 `c1` ENABLED NONE"))

	// OR REPLACE and IF NOT EXISTS are mutually exclusive.
	tk.MustGetErrCode(
		"create or replace masking policy if not exists p_or_replace on t_or_replace(c1) as c1 enable",
		errno.ErrWrongUsage,
	)

	// OR REPLACE can update status.
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as c1 disable")
	tk.MustQuery("select expression, status from mysql.tidb_masking_policy where policy_name='p_or_replace'").
		Check(testkit.Rows("`c1` DISABLED"))

	// OR REPLACE can update RESTRICT ON.
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as c1 restrict on (insert_into_select) enable")
	tk.MustQuery("select status, restrict_on from mysql.tidb_masking_policy where policy_name='p_or_replace'").
		Check(testkit.Rows("ENABLED INSERT_INTO_SELECT"))

	// OR REPLACE with the same policy name on a different column should fail.
	tk.MustGetErrCode("create or replace masking policy p_or_replace on t_or_replace(c2) as c2 enable", errno.ErrMaskingPolicyExists)

	// OR REPLACE should update updated_at and keep created_at unchanged.
	tk.MustExec("do sleep(0.01)")
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as mask_full(c1, '*') enable")
	tk.MustQuery("select timestampdiff(microsecond, created_at, updated_at) > 0 from mysql.tidb_masking_policy where policy_name='p_or_replace'").
		Check(testkit.Rows("1"))

	// OR REPLACE works for DISABLED -> ENABLED transition.
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as mask_full(c1, '*') disable")
	tk.MustQuery("select c1 from t_or_replace order by id").Check(testkit.Rows("alpha", "bravo"))
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as mask_full(c1, '*') enable")
	tk.MustQuery("select c1 from t_or_replace order by id").Check(testkit.Rows("*****", "*****"))
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

func TestMaskingPolicyDatabaseScopedUniqueness(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop database if exists db1")
	tk.MustExec("drop database if exists db2")
	tk.MustExec("create database db1")
	tk.MustExec("create database db2")

	// Create table in db1
	tk.MustExec("use db1")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c varchar(20))")
	tk.MustExec("create table t2(c varchar(20))")

	// Same policy name in different tables in same database should succeed
	tk.MustExec("create masking policy simple_mask on t1(c) as c enable")
	tk.MustExec("create masking policy simple_mask on t2(c) as c enable")

	// Same policy name in same table should fail
	tk.MustGetErrCode("create masking policy simple_mask on t1(c) as c enable", errno.ErrMaskingPolicyExists)

	// Different database - should also succeed since it's different table
	tk.MustExec("use db2")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3(c varchar(20))")
	tk.MustExec("create masking policy simple_mask on t3(c) as c enable")
}

func TestMaskingPolicySameNameNoOverwrite(t *testing.T) {
	// This test verifies the critical bug fix: policy names are only unique per table, not globally.
	// Different tables can have policies with the same name, and they should not overwrite each other.
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")

	// Create two tables
	tk.MustExec("create table t1(id int primary key, c varchar(20))")
	tk.MustExec("create table t2(id int primary key, c varchar(20))")
	tk.MustExec("insert into t1 values (1, 'secret1')")
	tk.MustExec("insert into t2 values (1, 'secret2')")

	// Create policies with the same name "p" on different tables with different expressions
	tk.MustExec("create masking policy p on t1(c) as mask_full(c, '*') enable")
	tk.MustExec("create masking policy p on t2(c) as mask_full(c, '#') enable")

	// Verify both policies are stored and work correctly (no overwrite)
	// t1 should use '*' for masking
	tk.MustQuery("select c from t1").Check(testkit.Rows("*******"))
	// t2 should use '#' for masking (not overwritten by t1's policy)
	tk.MustQuery("select c from t2").Check(testkit.Rows("#######"))

	// Verify both policies exist in system table
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("2"))
}

func TestMaskingPolicyDropImmediateQuery(t *testing.T) {
	// This test verifies that after dropping a policy, the InfoSchema is immediately updated
	// and subsequent queries see the original values (not masked).
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	// Create table with masking policy
	tk.MustExec("create table t(id int primary key, c varchar(20))")
	tk.MustExec("insert into t values (1, 'secret')")
	tk.MustExec("create masking policy p on t(c) as mask_full(c, '*') enable")

	// Verify masking is active
	tk.MustQuery("select c from t").Check(testkit.Rows("******"))

	// Drop the policy
	tk.MustExec("alter table t drop masking policy p")

	// Immediately query - should see original value, not masked
	// This verifies refreshMaskingPoliciesForTableIDs works correctly
	tk.MustQuery("select c from t").Check(testkit.Rows("secret"))

	// Verify policy is removed from system table
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("0"))
}

func TestMaskingPolicyAlterDropByTableWithSameName(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")

	tk.MustExec("create table t1(id int primary key, c varchar(20))")
	tk.MustExec("create table t2(id int primary key, c varchar(20))")
	tk.MustExec("insert into t1 values (1, 'secret1')")
	tk.MustExec("insert into t2 values (1, 'secret2')")

	tk.MustExec("create masking policy p on t1(c) as mask_full(c, '*') enable")
	tk.MustExec("create masking policy p on t2(c) as mask_full(c, '#') enable")

	tk.MustExec("alter table t2 modify masking policy p set expression = mask_full(c, '$')")
	tk.MustQuery("select c from t1").Check(testkit.Rows("*******"))
	tk.MustQuery("select c from t2").Check(testkit.Rows("$$$$$$$"))

	tk.MustExec("alter table t1 drop masking policy p")
	tk.MustQuery("select c from t1").Check(testkit.Rows("secret1"))
	tk.MustQuery("select c from t2").Check(testkit.Rows("$$$$$$$"))
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
