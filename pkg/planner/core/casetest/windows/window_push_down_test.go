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

package windows

import (
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

type Input []string
type Output []struct {
	SQL  string
	Plan []string
	Warn []string
}

func testWithData(t *testing.T, tk *testkit.TestKit, input Input, output Output) {
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		// c.Assert(len(output[i].Warn), Equals, 0)
	}
}

// Test WindowFuncDesc.CanPushDownToTiFlash
func TestWindowFunctionDescCanPushDown(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists employee")
		tk.MustExec("create table employee (empid int, deptid int, salary decimal(10,2))")
		testkit.SetTiFlashReplica(t, dom, "test", "employee")

		var input Input
		var output Output
		suiteData := getWindowPushDownSuiteData()
		suiteData.LoadTestCases(t, &input, &output, cascades, caller)
		testWithData(t, tk, input, output)
	})
}

func TestWindowPushDownPlans(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists employee")
		tk.MustExec("create table employee (empid int, deptid int, salary decimal(10,2))")
		testkit.SetTiFlashReplica(t, dom, "test", "employee")

		var input Input
		var output Output
		suiteData := getWindowPushDownSuiteData()
		suiteData.LoadTestCases(t, &input, &output, cascades, caller)
		testWithData(t, tk, input, output)
	})
}

func TestWindowPlanWithOtherOperators(t *testing.T) {
	t.Run("preserve nominal sort order", func(t *testing.T) {
		tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
		tk.MustExec("use test")
		tk.MustExec("set tidb_window_concurrency=4")
		tk.MustExec("create table wm(v int,g int)")
		tk.MustExec("insert into wm values(1,1),(2,1),(5,1),(3,2),(3,2)")
		query := "select v,count(v) over (partition by g order by v) c from wm"
		require.NotContains(t, tk.MustQuery("explain format='brief' "+query+" order by g,v").String(), "Shuffle")
		for range 10 {
			tk.MustQuery(query + " order by g,v").Check(testkit.Rows("1 1", "2 2", "5 3", "3 2", "3 2"))
		}
		tk.MustQuery(query + " order by g desc,v").Check(testkit.Rows("3 2", "3 2", "1 1", "2 2", "5 3"))
		require.Contains(t, tk.MustQuery("explain format='brief' "+query).String(), "Shuffle")
	})
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists employee")
		tk.MustExec("create table employee (empid int, deptid int, salary decimal(10,2))")
		testkit.SetTiFlashReplica(t, dom, "test", "employee")

		var input Input
		var output Output
		suiteData := getWindowPushDownSuiteData()
		suiteData.LoadTestCases(t, &input, &output, cascades, caller)
		testWithData(t, tk, input, output)

		tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("create table t1(c1 varchar(32), c2 datetime, c3 bigint, c4 varchar(64));")
		tk.MustExec("create table t2(b2 varchar(64));")
		tk.MustExec("set tidb_enforce_mpp=1;")
		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testkit.SetTiFlashReplica(t, dom, "test", "t2")

		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/CheckMPPWindowSchemaLength", "return"))
		defer func() {
			require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/CheckMPPWindowSchemaLength"))
		}()
		tk.MustExec("explain format = 'plan_tree' select /* issue:34765 */ count(*) from (select row_number() over (partition by c1 order by c2) num from (select * from t1 left join t2 on t1.c4 = t2.b2) tem2 ) tx where num = 1;")
	})
}
