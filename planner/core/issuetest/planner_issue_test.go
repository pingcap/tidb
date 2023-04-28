// Copyright 2022 PingCAP, Inc.
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

package issuetest

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

// It's a case for index merge's order prop push down.
func TestIssue43178(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE aa311c3c (
		57fd8d09 year(4) DEFAULT '1913',
		afbdd7c3 char(220) DEFAULT 'gakkl6occ0yd2jmhi2qxog8szibtcqwxyxmga3hp4ktszjplmg3rjvu8v6lgn9q6hva2lekhw6napjejbut6svsr8q2j8w8rc551e5vq',
		43b06e99 date NOT NULL DEFAULT '3403-10-08',
		b80b3746 tinyint(4) NOT NULL DEFAULT '34',
		6302d8ac timestamp DEFAULT '2004-04-01 18:21:18',
		PRIMARY KEY (43b06e99,b80b3746) /*T![clustered_index] CLUSTERED */,
		KEY 3080c821 (57fd8d09,43b06e99,b80b3746),
		KEY a9af33a4 (57fd8d09,b80b3746,43b06e99),
		KEY 464b386e (b80b3746),
		KEY 19dc3c2d (57fd8d09)
	      ) ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin COMMENT='320f8401'`)
	// Should not panic
	tk.MustExec("explain select  /*+ use_index_merge( `aa311c3c` ) */   `aa311c3c`.`43b06e99` as r0 , `aa311c3c`.`6302d8ac` as r1 from `aa311c3c` where IsNull( `aa311c3c`.`b80b3746` ) or not( `aa311c3c`.`57fd8d09` >= '2008' )   order by r0,r1 limit 95")
}

// It's a case for Columns in tableScan and indexScan with double reader
func TestIssue43461(t *testing.T) {
	store, domain := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, c int, index b(b), index b_c(b, c)) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t")

	stmt, err := parser.New().ParseOneStmt("select * from t use index(b) where b > 1 order by b limit 1", "", "")
	require.NoError(t, err)

	p, _, err := planner.Optimize(context.TODO(), tk.Session(), stmt, domain.InfoSchema())
	require.NoError(t, err)
	require.NotNil(t, p)

	idxLookUpPlan, ok := p.(*core.PhysicalLimit).Children()[0].(*core.PhysicalProjection).Children()[0].(*core.PhysicalIndexLookUpReader)
	require.True(t, ok)

	is := idxLookUpPlan.IndexPlans[0].(*core.PhysicalIndexScan)
	ts := idxLookUpPlan.TablePlans[0].(*core.PhysicalTableScan)

	require.NotEqual(t, is.Columns, ts.Columns)
}
