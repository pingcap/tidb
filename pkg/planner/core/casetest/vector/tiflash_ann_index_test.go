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

package vector

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/internal"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func getPlanRows(planStr string) []string {
	planStr = strings.Replace(planStr, "\t", " ", -1)
	return strings.Split(planStr, "\n")
}

func TestVectorIndexProtobufMatch(t *testing.T) {
	require.EqualValues(t, tipb.VectorDistanceMetric_INNER_PRODUCT.String(), model.DistanceMetricInnerProduct)
	require.EqualValues(t, tipb.VectorIndexKind_HNSW.String(), model.VectorIndexKindHNSW)
}

func TestTiFlashANNIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3) comment 'hnsw(distance=cosine)',
			a int,
			b int,
			c vector(3),
			d vector
		)
	`)
	tk.MustExec(`
		insert into t1 values
			('[1,1,1]', 1, 1, '[1,1,1]', '[1,1,1]'),
			('[2,2,2]', 2, 2, '[2,2,2]', '[2,2,2]'),
			('[3,3,3]', 3, 3, '[3,3,3]', '[3,3,3]')
	`)
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into t1(vec, a, b, c, d) select vec, a, b, c, d from t1")
	}
	tk.MustExec("analyze table t1")
	internal.SetTiFlashReplica(t, dom, "test", "t1")

	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetANNIndexSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestTiFlashANNIndexForPartition(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3) comment 'hnsw(distance=cosine)',
			a int, b int,
			store_id int
		) PARTITION BY RANGE COLUMNS(store_id) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (200),
			PARTITION p2 VALUES LESS THAN (MAXVALUE)
		);
	`)
	tk.MustExec("insert into t1 values('[1,1,1]', 1, 1, 50), ('[2,2,2]', 2, 2, 150), ('[3,3,3]', 3, 3, 250)")
	for i := 0; i < 14; i++ {
		tk.MustExec("insert into t1(vec, a, b, store_id) select vec, a, b, store_id from t1")
	}
	tk.MustExec("analyze table t1")
	internal.SetTiFlashReplica(t, dom, "test", "t1")

	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetANNIndexSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
	}
}

func TestANNIndexNormalizedPlan(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlan := func() ([]string, string) {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(core.Plan)
		require.True(t, ok)
		plan, digest := core.NormalizePlan(p)

		// test the new normalization code
		flat := core.FlattenPhysicalPlan(p, false)
		newNormalized, newDigest := core.NormalizeFlatPlan(flat)
		require.Equal(t, plan, newNormalized)
		require.Equal(t, digest, newDigest)

		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		normalizedPlanRows := getPlanRows(normalizedPlan)
		require.NoError(t, err)

		return normalizedPlanRows, digest.String()
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`
		create table t (
			vec vector(3) comment 'hnsw(distance=cosine)'
		)
	`)
	tk.MustExec(`
		insert into t values
			('[1,1,1]'),
			('[2,2,2]'),
			('[3,3,3]')
	`)

	tk.MustExec("analyze table t")
	internal.SetTiFlashReplica(t, dom, "test", "t")

	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	tk.MustExec("explain select * from t order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	p1, d1 := getNormalizedPlan()
	require.Equal(t, []string{
		" Projection                    root         test.t.vec",
		" └─TopN                        root         ?",
		"   └─Projection                root         test.t.vec, vec_cosine_distance(test.t.vec, ?)",
		"     └─TableReader             root         ",
		"       └─ExchangeSender        cop[tiflash] ",
		"         └─Projection          cop[tiflash] test.t.vec",
		"           └─TopN              cop[tiflash] ?",
		"             └─Projection      cop[tiflash] test.t.vec, vec_cosine_distance(test.t.vec, ?)",
		"               └─TableFullScan cop[tiflash] table:t, range:[?,?], annIndex:COSINE(test.t.vec..[?], limit:?), keep order:false",
	}, p1)

	tk.MustExec("explain select * from t order by vec_cosine_distance(vec, '[1,2,3]') limit 3")
	_, d2 := getNormalizedPlan()

	tk.MustExec("explain select * from t order by vec_cosine_distance(vec, '[]') limit 3")
	_, d3 := getNormalizedPlan()

	// Projection differs, so that normalized plan should differ.
	tk.MustExec("explain select * from t order by vec_cosine_distance('[1,2,3]', vec) limit 3")
	_, dx1 := getNormalizedPlan()

	require.Equal(t, d1, d2)
	require.Equal(t, d1, d3)
	require.NotEqual(t, d1, dx1)
}
