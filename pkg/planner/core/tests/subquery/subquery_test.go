// Copyright 2025 PingCAP, Inc.
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

package subquery

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestCollateSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int, col varchar(100), key ix(col)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("create table t1(id varchar(100)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	samePlan := testkit.Rows(
		"IndexHashJoin root  inner join, inner:IndexLookUp, outer key:Column, inner key:test.t.col, equal cond:eq(Column, test.t.col)",
		"├─HashAgg(Build) root  group by:Column, funcs:firstrow(Column)->Column",
		"│ └─TableReader root  data:HashAgg",
		"│   └─HashAgg cop[tikv]  group by:cast(test.t1.id, var_string(100)), ",
		"│     └─Selection cop[tikv]  not(isnull(cast(test.t1.id, var_string(100))))",
		"│       └─TableFullScan cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─IndexLookUp(Probe) root  ",
		"  ├─Selection(Build) cop[tikv]  not(isnull(test.t.col))",
		"  │ └─IndexRangeScan cop[tikv] table:t, index:ix(col) range: decided by [eq(test.t.col, Column)], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) cop[tikv] table:t keep order:false, stats:pseudo")
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='utf8_bin';`)
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
	tk.MustExec(`set collation_connection='latin1_bin';`)
	tk.MustQuery(`explain format="plan_tree" select * from t use index(ix) where col in (select cast(id as char) from t1);`).
		Check(samePlan)
}

func TestInSubqueryIncompatibleCollationUsesInnerDrivenJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table p (id int primary key, uuid char(36) not null, unique key uk_uuid (uuid)) default charset=utf8 collate=utf8_unicode_ci")
	tk.MustExec("create table m (id int primary key, user_id int not null, p_id int, key idx_user (user_id))")
	tk.MustExec("create table t (id int primary key, p_uuid varchar(36) not null, unique key uk_p_uuid (p_uuid)) default charset=utf8mb4 collate=utf8mb4_0900_ai_ci")
	tk.MustExec("create table t_ci (id int primary key, p_uuid varchar(36) not null, unique key uk_p_uuid (p_uuid)) default charset=utf8mb4 collate=utf8mb4_unicode_ci")
	tk.MustExec("insert into p values (1, '00000001-0000-4000-8000-000000000001'), (2, '00000002-0000-4000-8000-000000000002'), (3, '00000003-0000-4000-8000-000000000003')")
	tk.MustExec("insert into m values (1, 4242, 1)")
	tk.MustExec("insert into t values (1, '00000001-0000-4000-8000-000000000001'), (2, '00000002-0000-4000-8000-000000000002'), (3, '00000003-0000-4000-8000-000000000003')")
	values := make([]string, 1000)
	for i := range values {
		id := i + 4
		values[i] = fmt.Sprintf("(%d, '%036d')", id, id)
	}
	tk.MustExec("insert into t values " + strings.Join(values, ","))
	tk.MustExec("insert into t_ci select * from t")
	tk.MustExec("analyze table p")
	tk.MustExec("analyze table m")
	tk.MustExec("analyze table t")
	tk.MustExec("analyze table t_ci")

	query := `select t.id from %s as t where t.p_uuid in (select p.uuid from p join m on p.id = m.p_id where m.user_id = 4242)`
	for _, table := range []string{"t", "t_ci"} {
		tk.MustQuery(fmt.Sprintf(`explain format="plan_tree" `+query, table)).
			CheckContain("index:uk_p_uuid")
	}
}
