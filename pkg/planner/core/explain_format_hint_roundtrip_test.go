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

package core_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

const explainFormatHintDerivedTableSQL = `select *
from t1
join t2 on t1.a = t2.a
join (
    select a, b, row_number() over(partition by a order by b desc) as rn
    from t3
) dt on t2.a = dt.a
where dt.rn = 1`

const explainFormatHintNestedDerivedTableSQL = `select *
from t1
join (
    select a, b
    from (select a, b from t2 limit 100) d2
) dt on t1.a = dt.a
join t3 on dt.a = t3.a`

const explainFormatHintMixedQueryBlockLeadingSQL = `select /*+ leading(t2, t1) */ *
from t1
join t2 on t1.a = t2.a
where t2.b in (
    select a
    from t3
    where t3.b = 1
)
and t1.c = 1`

const explainFormatHintMultipleMixedQueryBlockLeadingSQL = `update /*+ leading(t2, t1, t4@sel_2) */ t1
join t2 on t1.a = t2.a
set t1.a = 1
where t2.b in (
    select a
    from t3
    where t3.b = 1
)
and t1.b in (
    select b
    from t4
    where t4.b = 1
)
and t1.c = 1`

func prepareExplainFormatHintDerivedTableAliasTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int, b int, key(a))")
	tk.MustExec("create table t2(a int, b int, key(a))")
	tk.MustExec("create table t3(a int, b int, key(a, b))")
	return tk
}

func prepareExplainFormatHintMixedLeadingTestKit(t *testing.T) *testkit.TestKit {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.MustExec("create table t1(pk int, a int, b int, c int, primary key(pk), key k_a(a), key k_bc(b, c))")
	tk.MustExec("create table t2(a int, b int, c int, key k_a(a), key k_bc(b, c))")
	tk.MustExec("create table t3(a int, b int, c int, key k_a(a), key k_bc(b, c))")
	tk.MustExec("create table t4(a int, b int, c int, key k_a(a), key k_bc(b, c))")
	return tk
}

func TestExplainFormatHintRecoverableForDerivedTableAlias(t *testing.T) {
	tk := prepareExplainFormatHintDerivedTableAliasTestKit(t)

	t.Run("simple derived table", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintDerivedTableSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`t1`, `test`.`t2`, `test`.`dt`)")

		replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from t1 join t2 on t1.a = t2.a join (select a, b, row_number() over(partition by a order by b desc) as rn from t3) dt on t2.a = dt.a where dt.rn = 1", hints)).Rows()[0][0]
		require.Contains(t, replayedHints, "leading(`test`.`t1`, `test`.`t2`, `test`.`dt`)")
		require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
	})

	t.Run("nested derived table keeps outer alias on replay", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintNestedDerivedTableSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")

		replayedHints := tk.MustQuery(fmt.Sprintf("explain format='hint' select /*+ %s */ * from t1 join (select a, b from (select a, b from t2 limit 100) d2) dt on t1.a = dt.a join t3 on dt.a = t3.a", hints)).Rows()[0][0]
		require.Contains(t, replayedHints, "leading(`test`.`t1`, `test`.`dt`, `test`.`t3`)")
		require.Empty(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings())
	})
}

func TestExplainFormatHintGeneratesMixedQueryBlockLeading(t *testing.T) {
	tk := prepareExplainFormatHintMixedLeadingTestKit(t)

	t.Run("single inner query block", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintMixedQueryBlockLeadingSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`t2`, `test`.`t1`, `test`.`t3`@`sel_2`)")
	})

	t.Run("multiple inner query blocks", func(t *testing.T) {
		hints := tk.MustQuery("explain format='hint' " + explainFormatHintMultipleMixedQueryBlockLeadingSQL).Rows()[0][0]
		require.Contains(t, hints, "leading(`test`.`t2`, `test`.`t1`, `test`.`t4`@`sel_2`, `test`.`t3`@`sel_1`)")
	})
}
