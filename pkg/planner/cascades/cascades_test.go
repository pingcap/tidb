// Copyright 2024 PingCAP, Inc.
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

package cascades_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestCascadesDrive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().SetEnableCascadesPlanner(true)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int not null, b int not null, key(a,b))")
	tk.MustExec("insert into t1 values(1,1),(1,2),(2,1),(2,2),(1,1)")

	// simple select for quick debug of memo, the normal test case is in tests/planner/cascades/integration.test.
	tk.MustQuery("select 1").Check(testkit.Rows("1"))
	tk.MustQuery("explain select 1").Check(testkit.Rows(""+
		"Projection_3 1.00 root  1->Column#1",
		"└─TableDual_4 1.00 root  rows:1"))
}
