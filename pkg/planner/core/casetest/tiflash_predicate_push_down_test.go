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

package casetest

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/stretchr/testify/require"
)

func TestTiFlashLateMaterialization(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t1;")
		testKit.MustExec("create table t1 (a int, b int, c int, t time, index idx(a, b, c, t))")
		testKit.MustExec("insert into t1 values(1,1,1,'08:00:00'), (2,2,2,'09:00:00'), (3,3,3,'10:00:00'), (4,4,4,'11:00:00')")
		for range 13 {
			testKit.MustExec("insert into t1(a,b,c,t) select a,b,c,t from t1;")
		}
		h := dom.StatsHandle()
		require.NoError(t, h.DumpStatsDeltaToKV(true))
		testKit.MustExec("analyze table t1 all columns;")
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual `tiflash` replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		// Enable late materialization.
		testKit.MustExec("set @@tidb_opt_enable_late_materialization = on")
		testKit.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		planNormalizedSuiteData := GetPlanNormalizedSuiteData()
		planNormalizedSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testKit.Session().GetSessionVars().PlanID.Store(0)
			testKit.MustExec(tt)
			info := testKit.Session().ShowProcess()
			require.NotNil(t, info)
			p, ok := info.Plan.(base.Plan)
			require.True(t, ok)
			normalized, digest := core.NormalizePlan(p)

			// test the new normalization code
			flat := core.FlattenPhysicalPlan(p, false)
			newNormalized, newDigest := core.NormalizeFlatPlan(flat)
			require.Equal(t, normalized, newNormalized)
			require.Equal(t, digest, newDigest)

			normalizedPlan, err := plancodec.DecodeNormalizedPlan(normalized)
			normalizedPlanRows := getPlanRows(normalizedPlan)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = normalizedPlanRows
			})
			require.Equal(t, output[i].Plan, normalizedPlanRows, tt)
		}
	})
}

func TestInvertedIndex(t *testing.T) {
	testkit.RunTestUnderCascadesAndDomainWithSchemaLease(t, 600*time.Millisecond, []mockstore.MockTiKVStoreOption{mockstore.WithMockTiFlash(2)}, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")

		tiflash := infosync.NewMockTiFlash()
		infosync.SetMockTiFlash(tiflash)
		defer func() {
			tiflash.Lock()
			tiflash.StatusServer.Close()
			tiflash.Unlock()
		}()
		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

		testKit.MustExec("drop table if exists t1;")
		testKit.MustExec("create table t1 (a int, b int, c int, t time, columnar index idx_a (a) using inverted, columnar index idx_b (b) using inverted)")
		testKit.MustExec("insert into t1 values(1,1,1,'08:00:00'), (2,2,2,'09:00:00'), (3,3,3,'10:00:00'), (4,4,4,'11:00:00')")
		for range 13 {
			testKit.MustExec("insert into t1(a,b,c,t) select a,b,c,t from t1;")
		}
		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testKit.MustExec("analyze table t1 all columns;")
		testKit.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		planNormalizedSuiteData := GetPlanNormalizedSuiteData()
		planNormalizedSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testKit.Session().GetSessionVars().PlanID.Store(0)
			testKit.MustExec(tt)
			info := testKit.Session().ShowProcess()
			require.NotNil(t, info)
			p, ok := info.Plan.(base.Plan)
			require.True(t, ok)
			normalized, digest := core.NormalizePlan(p)

			// test the new normalization code
			flat := core.FlattenPhysicalPlan(p, false)
			newNormalized, newDigest := core.NormalizeFlatPlan(flat)
			require.Equal(t, normalized, newNormalized)
			require.Equal(t, digest, newDigest)

			normalizedPlan, err := plancodec.DecodeNormalizedPlan(normalized)
			normalizedPlanRows := getPlanRows(normalizedPlan)
			require.NoError(t, err)
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = normalizedPlanRows
			})
			compareStringSlice(t, normalizedPlanRows, output[i].Plan)
		}
	})
}
