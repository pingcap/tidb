// Copyright 2023 PingCAP, Inc.
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

package expressionrewriter

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestInExprRewriteBug(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tb0 (c0 bigint unsigned not null);")
	tk.MustQuery("explain format='plan_tree' select 1 from tb0 where ((true not in (select 1)) not in (tb0.c0));").Check(testkit.Rows(
		`Projection root  1->Column#5`,
		`└─Selection root  ne(Column#4, test.tb0.c0)`,
		`  └─HashJoin root  CARTESIAN anti left outer semi join, left side:TableReader`,
		`    ├─Projection(Build) root  0->Column#9`,
		`    │ └─TableDual root  rows:1`,
		`    └─TableReader(Probe) root  data:TableFullScan`,
		`      └─TableFullScan cop[tikv] table:tb0 keep order:false, stats:pseudo`))
}
