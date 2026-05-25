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
	"time"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
)

func TestMaskingPolicyDDLBasic(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
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

	tk.MustExec("create or replace masking policy p on t(c) as concat(c, '_x')")
	tk.MustQuery("select expression like 'CONCAT(%', masking_type from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("1 CUSTOM"))

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

func TestMaskingPolicyRenameTable(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists old_table, new_table")

	// Create table and masking policy
	tk.MustExec("create table old_table(id int primary key, c varchar(100))")
	tk.MustExec("insert into old_table values (1, 'secret')")
	tk.MustExec("create masking policy p on old_table(c) as c enable")

	// Verify policy metadata before rename
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("test old_table"))

	// Rename the table
	tk.MustExec("rename table old_table to new_table")

	// Verify policy metadata is updated in sys table
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("test new_table"))

	// Verify we can drop the policy after rename
	tk.MustExec("alter table new_table drop masking policy p")
	tk.MustQuery("select count(*) from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("0"))
	tk.MustQuery("select c from new_table").Check(testkit.Rows("secret"))
}

func TestMaskingPolicyRenameTableCrossDatabase(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists db1")
	tk.MustExec("drop database if exists db2")

	// Create databases and table
	tk.MustExec("create database db1")
	tk.MustExec("create database db2")
	tk.MustExec("create table db1.t(id int primary key, c varchar(100))")
	tk.MustExec("insert into db1.t values (1, 'secret')")
	tk.MustExec("create masking policy p on db1.t(c) as c enable")

	// Verify before rename
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("db1 t"))

	// Rename the table across databases
	tk.MustExec("rename table db1.t to db2.t")

	// Verify policy metadata is updated
	tk.MustQuery("select db_name, table_name from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("db2 t"))

	// Cleanup
	tk.MustExec("drop table db2.t")
	tk.MustExec("drop database db1")
	tk.MustExec("drop database db2")
}

func TestMaskingPolicyRenameTableNoPolicy(t *testing.T) {
	store := testkit.CreateMockStore(t, mockstore.WithDDLChecker())
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
