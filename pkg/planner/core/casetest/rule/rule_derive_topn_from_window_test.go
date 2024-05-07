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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

type Input []string

// TiFlash cases. TopN pushed down to storage only when no partition by.
func TestPushDerivedTopnFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())

	tk.MustExec("set tidb_opt_derive_topn=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(b,a))")
	coretestsdk.SetTiFlashReplica(t, dom, "test", "t")
	tk.MustExec("set tidb_enforce_mpp=1")
	tk.MustExec("set @@session.tidb_allow_mpp=ON;")
	var input Input
	var output []struct {
		SQL  string
		Plan []string
	}
	suiteData := GetDerivedTopNSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}
