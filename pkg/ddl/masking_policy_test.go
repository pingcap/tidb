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
		Check(testkit.Rows("p test t c `c` ENABLE CUSTOM NONE"))

	tk.MustExec("alter table t disable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("DISABLE"))

	tk.MustExec("alter table t enable masking policy p")
	tk.MustQuery("select status from mysql.tidb_masking_policy where policy_name = 'p'").
		Check(testkit.Rows("ENABLE"))

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
		Check(testkit.Rows("p_case ENABLE"))
	tk.MustQuery("select expression like 'CASE WHEN %' from mysql.tidb_masking_policy where policy_name = 'p_case'").
		Check(testkit.Rows("1"))
	tk.MustQuery("select expression like '%CURRENT_USER()%' from mysql.tidb_masking_policy where policy_name = 'p_case'").
		Check(testkit.Rows("1"))
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
