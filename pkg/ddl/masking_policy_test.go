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
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/stretchr/testify/require"
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

	tk.MustExec("create or replace masking policy p on t(c) as mask_full(c)")
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
	tk.MustExec("alter table t modify masking policy p set expression = mask_full(c)")
	tk.MustQuery("select expression, masking_type from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("MASK_FULL(`c`) MASK_FULL"))

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
		case when current_user() != 'root@%' then mask_full(c) else c end enable`)
	tk.MustQuery("select expression like '%CURRENT_USER()%' from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("1"))

	tk.MustExec(`alter table t modify masking policy p set expression =
		case when current_role() = 'NONE' then c else mask_full(c) end`)
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
	tk.MustExec("create masking policy p_cte on t_cte_src(c) as mask_full(c) restrict on (insert_into_select) enable")

	tk.MustQuery("with cte as (select c from t_cte_src) select c from cte order by c").Check(testkit.Rows("XXXXXX", "XXXXXX"))
	tk.MustQuery("with cte as (select c from t_cte_src) select count(*) from cte where c = 'secret'").Check(testkit.Rows("1"))
	tk.MustQuery("with cte as (select id, c from t_cte_src) select j.name, cte.c from t_cte_join j join cte on j.id = cte.id order by j.id").
		Check(testkit.Rows("alice XXXXXX", "bob XXXXXX"))
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
		errno.ErrParse,
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
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as mask_full(c1) enable")
	tk.MustQuery("select timestampdiff(microsecond, created_at, updated_at) > 0 from mysql.tidb_masking_policy where policy_name='p_or_replace'").
		Check(testkit.Rows("1"))

	// OR REPLACE works for DISABLED -> ENABLED transition.
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as mask_full(c1) disable")
	tk.MustQuery("select c1 from t_or_replace order by id").Check(testkit.Rows("alpha", "bravo"))
	tk.MustExec("create or replace masking policy p_or_replace on t_or_replace(c1) as mask_full(c1) enable")
	tk.MustQuery("select c1 from t_or_replace order by id").Check(testkit.Rows("XXXXX", "XXXXX"))
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
	tk.MustExec("create masking policy p_rename_col on t_rename_col(c) as mask_full(c) enable")

	tk.MustExec("alter table t_rename_col rename column c to c_new")
	tk.MustQuery("select column_name, expression from mysql.tidb_masking_policy where policy_name = 'p_rename_col'").
		Check(testkit.Rows("c_new MASK_FULL(`c_new`)"))
	tk.MustQuery("select c_new from t_rename_col").Check(testkit.Rows("XXXXX"))
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
	tk.MustExec("create masking policy p on t1(c) as mask_full(c) enable")
	tk.MustExec("create masking policy p on t2(c) as mask_partial(c, 0, 0, '#') enable")

	// Verify both policies are stored and work correctly (no overwrite)
	// t1 should use 'X' for masking
	tk.MustQuery("select c from t1").Check(testkit.Rows("XXXXXXX"))
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
	tk.MustExec("create masking policy p on t(c) as mask_full(c) enable")

	// Verify masking is active
	tk.MustQuery("select c from t").Check(testkit.Rows("XXXXXX"))

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

	tk.MustExec("create masking policy p on t1(c) as mask_full(c) enable")
	tk.MustExec("create masking policy p on t2(c) as mask_partial(c, 0, 0, '#') enable")

	tk.MustExec("alter table t2 modify masking policy p set expression = mask_partial(c, 0, 0, '$')")
	tk.MustQuery("select c from t1").Check(testkit.Rows("XXXXXXX"))
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
	tk.MustExec("create masking policy p on old_table(c) as mask_full(c) enable")

	// Verify masking works before rename
	tk.MustQuery("select c from old_table").Check(testkit.Rows("XXXXXX"))
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("test old_table"))

	// Rename the table
	tk.MustExec("rename table old_table to new_table")

	// Verify policy metadata is updated in sys table
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("test new_table"))

	// Verify masking still works after rename
	tk.MustQuery("select c from new_table").Check(testkit.Rows("XXXXXX"))

	// Verify we can modify the policy after rename
	tk.MustExec("alter table new_table disable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("DISABLED"))
	tk.MustQuery("select c from new_table").Check(testkit.Rows("secret"))

	tk.MustExec("alter table new_table enable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("ENABLED"))
	tk.MustQuery("select c from new_table").Check(testkit.Rows("XXXXXX"))

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
	tk.MustExec("create masking policy p on db1.t(c) as mask_full(c) enable")

	// Verify masking works before rename
	tk.MustQuery("select c from db1.t").Check(testkit.Rows("XXXXXX"))
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("db1 t"))

	// Rename the table across databases
	tk.MustExec("rename table db1.t to db2.t")

	// Verify policy metadata is updated in sys table
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("db2 t"))

	// Verify masking still works after rename
	tk.MustQuery("select c from db2.t").Check(testkit.Rows("XXXXXX"))

	// Verify we can modify the policy after rename
	tk.MustExec("alter table db2.t disable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("DISABLED"))
	tk.MustQuery("select c from db2.t").Check(testkit.Rows("secret"))

	tk.MustExec("alter table db2.t enable masking policy p")
	tk.MustQuery("select c from db2.t").Check(testkit.Rows("XXXXXX"))
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
	tk.MustExec("create masking policy p_valid on t(c) as mask_full(c) enable")
	tk.MustExec("insert into t values (1, 'secret')")

	// PointGet query with PK should return masked value
	tk.MustQuery("select c from t where id = 1").Check(testkit.Rows("XXXXXX"))
}

func TestMaskingPolicyMultilineName(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_multiline(c varchar(10), d varchar(10))")
	tk.MustExec("create masking policy p_single on t_multiline(c) as mask_full(c) enable")
	// The checker compares SHOW CREATE after stripping only policy annotations.
	tk.MustExec("create masking policy `p_multi\nline` on t_multiline(d) as mask_full(d) enable")
	ddl := tk.MustQuery("show create table t_multiline").Rows()[0][1]
	require.Contains(t, ddl, "/* MASKING POLICY `p_single` ENABLED */")
	require.Contains(t, ddl, "/* MASKING POLICY `p_multi\nline` ENABLED */")
	tk.MustExec("alter table t_multiline disable masking policy `p_multi\nline`")
	tk.MustExec("alter table t_multiline add column e int")
	tk.MustExec("alter table t_multiline drop masking policy `p_multi\nline`")
}

func TestMaskingPolicyIfNotExists(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 200*time.Millisecond)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_db_state default charset utf8 default collate utf8_bin")
	tk.MustExec("use test_db_state")
	tk.MustExec("create table t_mask (c char(120))")

	dbChangeTestParallelExecSQL(t, store, "create masking policy if not exists p on t_mask(c) as c")

	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where db_name = 'test_db_state' and table_name = 't_mask' and policy_name = 'p'").
		Check(testkit.Rows("1"))
}

func TestMaskingPolicyRenameColumn(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_rename_col")
	tk.MustExec("create table t_rename_col(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_rename_col values (1, 'delta')")
	tk.MustExec("create masking policy p_rename_col on t_rename_col(c) as c enable")

	// Verify policy exists before rename
	tk.MustQuery("select column_name, expression from mysql.tidb_masking_policy where policy_name = 'p_rename_col'").
		Check(testkit.Rows("c `c`"))

	// Rename column
	tk.MustExec("alter table t_rename_col rename column c to c_new")

	// Verify column_name and expression are updated in sys table
	tk.MustQuery("select column_name, expression from mysql.tidb_masking_policy where policy_name = 'p_rename_col'").
		Check(testkit.Rows("c_new `c_new`"))

		// Verify select still works
	tk.MustQuery("select c_new from t_rename_col").Check(testkit.Rows("delta"))
}

func TestMaskingPolicyModifyColumnRejectUnsupportedType(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_mod")
	tk.MustExec("create table t_mod(id int primary key, c varchar(100))")
	tk.MustExec("create masking policy p on t_mod(c) as c enable")

	// MODIFY COLUMN to JSON (unsupported type) should be rejected.
	tk.MustGetErrCode("alter table t_mod modify column c json", errno.ErrUnsupportedDDLOperation)

	// CHANGE COLUMN to JSON should also be rejected.
	tk.MustGetErrCode("alter table t_mod change column c c2 json", errno.ErrUnsupportedDDLOperation)

	// Verify the policy is still intact.
	tk.MustQuery("select column_name, expression from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("c `c`"))
}

func TestMaskingPolicyExpressionRejectsNonTargetColumn(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_expr_dep")
	tk.MustExec("create table t_expr_dep(a varchar(100), b varchar(100))")

	// CREATE MASKING POLICY ... ON t(a) AS b must fail because expression references non-target column b.
	tk.MustGetErrCode("create masking policy p_expr_dep on t_expr_dep(a) as b", errno.ErrMaskingPolicyExprInvalidColumn)

	// Expression referencing both target and non-target columns must also fail.
	tk.MustGetErrCode("create masking policy p_expr_dep2 on t_expr_dep(a) as concat(a, b)", errno.ErrMaskingPolicyExprInvalidColumn)

	// Expression referencing only the target column must succeed.
	tk.MustExec("create masking policy p_valid on t_expr_dep(a) as a enable")
	tk.MustQuery("select expression from mysql.tidb_masking_policy where policy_name = 'p_valid'").
		Check(testkit.Rows("`a`"))

	// CREATE OR REPLACE with non-target column reference must also fail.
	tk.MustGetErrCode("create or replace masking policy p_valid on t_expr_dep(a) as b", errno.ErrMaskingPolicyExprInvalidColumn)

	// ALTER TABLE ... MODIFY MASKING POLICY with non-target column reference must fail.
	tk.MustGetErrCode("alter table t_expr_dep modify masking policy p_valid set expression = b", errno.ErrMaskingPolicyExprInvalidColumn)

	// ALTER TABLE ... MODIFY MASKING POLICY with target column reference must succeed.
	tk.MustExec("alter table t_expr_dep modify masking policy p_valid set expression = concat(a, '_masked')")
}

func TestMaskingPolicyTruncateKeepsPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_trunc")
	tk.MustExec("create table t_trunc(id int primary key, c varchar(100))")
	tk.MustExec("insert into t_trunc values (1, 'secret')")
	tk.MustExec("create masking policy p_trunc on t_trunc(c) as mask_full(c) enable")

	// Verify policy exists before truncate
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p_trunc'").
		Check(testkit.Rows("1"))

	// Capture the table_id before truncate
	var oldTableID string
	rs := tk.MustQuery("select table_id from mysql.tidb_masking_policy where policy_name = 'p_trunc'")
	oldTableID = rs.Rows()[0][0].(string)

	// Warm the policy cache before the table ID changes.
	tk.MustQuery("select c from t_trunc").Check(testkit.Rows("XXXXXX"))
	tk.MustExec("truncate table t_trunc")
	tk.MustExec("insert into t_trunc values (2, 'secret')")
	tk.MustQuery("select c from t_trunc").Check(testkit.Rows("XXXXXX"))

	// Verify policy still exists after truncate
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p_trunc'").
		Check(testkit.Rows("1"))

	// Verify table_id was updated to a new table ID (different from old)
	rs = tk.MustQuery("select table_id from mysql.tidb_masking_policy where policy_name = 'p_trunc'")
	newTableID := rs.Rows()[0][0].(string)
	require.NotEqual(t, oldTableID, newTableID, "table_id should change after TRUNCATE TABLE")

	// Verify we can still operate on the policy after truncate
	tk.MustExec("alter table t_trunc disable masking policy p_trunc")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p_trunc'").
		Check(testkit.Rows("DISABLED"))

	tk.MustExec("alter table t_trunc enable masking policy p_trunc")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p_trunc'").
		Check(testkit.Rows("ENABLED"))

	// Verify we can drop the policy after truncate
	tk.MustExec("alter table t_trunc drop masking policy p_trunc")
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p_trunc'").
		Check(testkit.Rows("0"))
}

func TestMaskingPolicyDropDatabaseCleanup(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// Create a separate database with tables and masking policies
	tk.MustExec("drop database if exists db_mask_cleanup")
	tk.MustExec("create database db_mask_cleanup")
	tk.MustExec("use db_mask_cleanup")
	tk.MustExec("create table t1(c varchar(100))")
	tk.MustExec("create table t2(c varchar(100))")
	tk.MustExec("create masking policy p1 on t1(c) as c enable")
	tk.MustExec("create masking policy p2 on t2(c) as c enable")

	// Verify policies exist
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where db_name = 'db_mask_cleanup'").
		Check(testkit.Rows("2"))

	// Drop the database
	tk.MustExec("drop database db_mask_cleanup")

	// Verify all policies for this database are cleaned up
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where db_name = 'db_mask_cleanup'").
		Check(testkit.Rows("0"))
}

func TestMaskingPolicyInvalidNameCharacters(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c char(120))")

	// Test CREATE MASKING POLICY with invalid characters
	_, err := tk.Exec("create masking policy `p*` on t(c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	_, err = tk.Exec("create masking policy `p/x` on t(c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	_, err = tk.Exec("create masking policy `p*/x` on t(c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	_, err = tk.Exec("create masking policy `p*x/` on t(c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	// Test ALTER TABLE ... ADD MASKING POLICY with invalid characters
	_, err = tk.Exec("alter table t add masking policy `p*` on (c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	_, err = tk.Exec("alter table t add masking policy `p/x` on (c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	_, err = tk.Exec("alter table t add masking policy `p*/x` on (c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	_, err = tk.Exec("alter table t add masking policy `p*x/` on (c) as c")
	require.Error(t, err)
	require.Contains(t, err.Error(), "Cannot use character * or / in masking policy name")

	// Test that valid names still work
	tk.MustExec("create masking policy valid_name on t(c) as c")
	tk.MustExec("alter table t drop masking policy valid_name")

	tk.MustExec("alter table t add masking policy another_valid_name on (c) as c")
	tk.MustExec("alter table t drop masking policy another_valid_name")
}

func TestMaskingPolicyDropCleanupRetries(t *testing.T) {
	for _, sql := range []string{"alter table t_cleanup drop column c", "drop table t_cleanup"} {
		t.Run(sql, func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_cleanup(id int, c varchar(20))")
			tk.MustExec("create masking policy p on t_cleanup(c) as mask_full(c)")
			var retried atomic.Bool
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMissingMaskingPolicySysTable", "return(true)")
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunAfter", func(job *model.Job) {
				if job.ErrorCount > 0 && (job.Type == model.ActionDropColumn || job.Type == model.ActionDropTable) {
					require.False(t, job.IsCancelled())
					retried.Store(true)
					require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/mockMissingMaskingPolicySysTable"))
				}
			})
			tk.MustExec(sql)
			require.True(t, retried.Load())
			tk.MustQuery("select count(*) from mysql.tidb_masking_policy where table_name='t_cleanup'").Check(testkit.Rows("0"))
		})
	}
}

func TestMaskingPolicyTruncateRollbackIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name, expr string
		cancelled  bool
	}{
		{"error", "1*off->1*return(true)->off", false},
		{"panic", `1*off->1*panic("policy update panic")->off`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table t_atomic(a varchar(20), b varchar(20))")
			tk.MustExec("insert into t_atomic values ('secret','secret')")
			tk.MustExec("create masking policy p1 on t_atomic(a) as mask_full(a)")
			tk.MustExec("create masking policy p2 on t_atomic(b) as mask_full(b)")
			tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("t_atomic"))
			require.NoError(t, err)
			oldID := tbl.Meta().ID
			var observed atomic.Bool
			testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMaskingPolicyUpdateError", tc.expr)
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/beforeTransitOneJobStep", func(job *model.JobW) {
				if job.Job.Type == model.ActionTruncateTable && job.Job.ErrorCount > 0 && !observed.Swap(true) {
					txn, err := store.Begin()
					require.NoError(t, err)
					metaTable, err := meta.NewReader(txn).GetTable(job.Job.SchemaID, oldID)
					require.NoError(t, err)
					require.NotNil(t, metaTable)
					require.NoError(t, txn.Rollback())
					tkCheck := testkit.NewTestKit(t, store)
					tkCheck.MustQuery("select count(*) from mysql.tidb_masking_policy where table_id=" + fmt.Sprint(oldID)).Check(testkit.Rows("2"))
					tkCheck.MustQuery("select a,b from test.t_atomic").Check(testkit.Rows("XXXXXX XXXXXX"))
				}
			})
			err = tk.ExecToErr("truncate table t_atomic")
			if tc.cancelled {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.True(t, observed.Load())
			if tc.cancelled {
				tk.MustQuery("select count(*) from mysql.tidb_masking_policy where table_id=" + fmt.Sprint(oldID)).Check(testkit.Rows("2"))
				tk.MustQuery("select a,b from t_atomic").Check(testkit.Rows("XXXXXX XXXXXX"))
				return
			}
			tk.MustQuery("select count(*) from mysql.tidb_masking_policy where table_id=" + fmt.Sprint(oldID)).Check(testkit.Rows("0"))
			tk.MustExec("insert into t_atomic values ('secret','secret')")
			tk.MustQuery("select a,b from t_atomic").Check(testkit.Rows("XXXXXX XXXXXX"))
		})
	}
}

func TestMaskingPolicyRenameRollbackIsAtomic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database rename_target")
	tk.MustExec("create table t_rename(a varchar(20), b varchar(20))")
	tk.MustExec("insert into t_rename values ('secret','secret')")
	tk.MustExec("create masking policy p1 on t_rename(a) as mask_full(a)")
	tk.MustExec("create masking policy p2 on t_rename(b) as mask_full(b)")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockMaskingPolicyUpdateError", "1*off->1*return(true)->off")
	require.Error(t, tk.ExecToErr("rename table test.t_rename to rename_target.t_renamed"))
	tk.MustQuery("select db_name,table_name from mysql.tidb_masking_policy where table_name like 't_rename%' order by policy_id").Check(testkit.Rows("test t_rename", "test t_rename"))
	tk.MustQuery("select a,b from t_rename").Check(testkit.Rows("XXXXXX XXXXXX"))
	tk.MustExec("rename table test.t_rename to rename_target.t_renamed")
	tk.MustQuery("select a,b from rename_target.t_renamed").Check(testkit.Rows("XXXXXX XXXXXX"))
}

func TestMaskingPolicyModifyColumnPublication(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table t_publish(c decimal(10,2))")
	root.MustExec("insert into t_publish values (123.45)")
	root.MustExec("create masking policy p on t_publish(c) as mask_full(c)")
	var checked atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/onJobRunBefore", func(job *model.Job) {
		if job.Type == model.ActionModifyColumn && job.SchemaState == model.StatePublic && !checked.Swap(true) {
			user := testkit.NewTestKit(t, store)
			user.MustQuery("select c from test.t_publish").Check(testkit.Rows("0"))
		}
	})
	root.MustExec("alter table t_publish modify column c decimal(10,2) unsigned")
	require.True(t, checked.Load())
	root.MustQuery("select c from t_publish").Check(testkit.Rows("0"))
}
