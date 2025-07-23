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
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int, c int, t time, index idx(a, b, c, t))")
	tk.MustExec("insert into t1 values(1,1,1,'08:00:00'), (2,2,2,'09:00:00'), (3,3,3,'10:00:00'), (4,4,4,'11:00:00')")
	for range 13 {
		tk.MustExec("insert into t1(a,b,c,t) select a,b,c,t from t1;")
	}
	tk.MustExec("analyze table t1 all columns;")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// Create virtual `tiflash` replica info.
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	// Enable late materialization.
	tk.MustExec("set @@tidb_opt_enable_late_materialization = on")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planNormalizedSuiteData := GetPlanNormalizedSuiteData()
	planNormalizedSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		tk.Session().GetSessionVars().PlanID.Store(0)
		tk.MustExec(tt)
		info := tk.Session().ShowProcess()
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
		require.Equal(t, normalizedPlanRows, output[i].Plan, tt)
	}
}

func TestInvertedIndex(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 600*time.Millisecond, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1 (a int, b int, c int, t time, columnar index idx_a (a) using inverted, columnar index idx_b (b) using inverted)")
	tk.MustExec("insert into t1 values(1,1,1,'08:00:00'), (2,2,2,'09:00:00'), (3,3,3,'10:00:00'), (4,4,4,'11:00:00')")
	for range 13 {
		tk.MustExec("insert into t1(a,b,c,t) select a,b,c,t from t1;")
	}
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	tk.MustExec("analyze table t1 all columns;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planNormalizedSuiteData := GetPlanNormalizedSuiteData()
	planNormalizedSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		tk.Session().GetSessionVars().PlanID.Store(0)
		tk.MustExec(tt)
		info := tk.Session().ShowProcess()
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
}

func TestCaseWhenPreparedStatementBug(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t0(c0 DECIMAL);")
	tk.MustExec("INSERT INTO t0 VALUES (0);")

	normalResult := tk.MustQuery("SELECT * FROM t0 WHERE CAST((CASE t0.c0 WHEN t0.c0 THEN CAST(t0.c0 AS TIME) ELSE NULL END ) AS DATE);")
	t.Logf("Normal query result: %v", normalResult.Rows())

	caseResult := tk.MustQuery("SELECT CAST((CASE t0.c0 WHEN t0.c0 THEN CAST(t0.c0 AS TIME) ELSE NULL END ) AS DATE) FROM t0;")
	t.Logf("Normal CASE result: %v", caseResult.Rows())

	tk.MustExec("SET @b = NULL;")
	tk.MustExec("PREPARE prepare_query FROM 'SELECT * FROM t0 WHERE CAST((CASE t0.c0 WHEN t0.c0 THEN CAST(t0.c0 AS TIME) ELSE ? END ) AS DATE)';")
	preparedResult := tk.MustQuery("EXECUTE prepare_query USING @b;")
	t.Logf("Prepared statement result: %v", preparedResult.Rows())

	tk.MustExec("PREPARE prepare_case FROM 'SELECT CAST((CASE t0.c0 WHEN t0.c0 THEN CAST(t0.c0 AS TIME) ELSE ? END ) AS DATE) FROM t0';")
	preparedCaseResult := tk.MustQuery("EXECUTE prepare_case USING @b;")
	t.Logf("Prepared CASE result: %v", preparedCaseResult.Rows())

	require.Equal(t, len(normalResult.Rows()), len(preparedResult.Rows()), "Results should have the same number of rows")
}
