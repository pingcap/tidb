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

package executor_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestUpdateUnqualifiedSetSingleTableTarget checks multi-table UPDATE with an unqualified
// column that exists on only one table: only that table should be an UPDATE target for
// same-table-FROM correlation (collectUpdateTargetTableIDs resolves via FROM names).
func TestUpdateUnqualifiedSetSingleTableTarget(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists upt1, upt2")
	tk.MustExec("create table upt1 (a int)")
	tk.MustExec("create table upt2 (b int, w int)")
	tk.MustExec("insert into upt1 values (1)")
	tk.MustExec("insert into upt2 values (1, 2)")

	tk.MustExec("update upt1, upt2 set w=5")
	tk.MustQuery("select w from upt2").Check(testkit.Rows("5"))
	tk.MustQuery("select a from upt1").Check(testkit.Rows("1"))
}

// TestUpdateSelfJoinExistsUsesCorrelatedSameTable EXPLAIN: self-join UPDATE with EXISTS (FROM same base table)
// should use correlated TableDual for the inner FROM matching one outer alias.
func TestUpdateSelfJoinExistsUsesCorrelatedSameTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists sjc")
	tk.MustExec("create table sjc (id int primary key, v int)")

	rows := tk.MustQuery(`explain format='brief' update sjc x join sjc y on x.id=y.id set x.v=1
		where exists (select 1 from sjc where sjc.id = x.id)`).Rows()
	var b strings.Builder
	for _, r := range rows {
		b.WriteString(r[0].(string))
		b.WriteByte('\n')
	}
	require.Contains(t, b.String(), "TableDual")
}

// TestDeleteMultiTableInnerFromNonTargetNoCorrelatedTableDual: table only in FROM, not in DELETE list,
// should not use correlated single-row FROM for that table.
func TestDeleteMultiTableInnerFromNonTargetNoCorrelatedTableDual(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists dmt1, dmt2")
	tk.MustExec("create table dmt1 (id int primary key, v int)")
	tk.MustExec("create table dmt2 (id int primary key, v int)")

	rows := tk.MustQuery(`explain format='brief' delete dmt1 from dmt1 join dmt2 on dmt1.id=dmt2.id
		where exists (select 1 from dmt2 where dmt2.v = dmt1.v)`).Rows()
	var plan strings.Builder
	for _, r := range rows {
		plan.WriteString(r[0].(string))
		plan.WriteByte('\n')
	}
	s := strings.ToLower(plan.String())
	require.NotContains(t, s, "tabledual", "inner FROM dmt2 is not a delete target; expect full scan, not correlated TableDual")
}
