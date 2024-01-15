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

package plancache

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestWarningWithDisablePlanCacheStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int) partition by hash(a) partitions 4;")
	tk.MustExec("analyze table t;")
	tk.MustExec("prepare st from 'select * from t';")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query accesses partitioned tables is un-cacheable"))
	tk.MustExec("execute st;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query accesses partitioned tables is un-cacheable"))
	tk.MustExec("execute st;")
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query accesses partitioned tables is un-cacheable"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}
