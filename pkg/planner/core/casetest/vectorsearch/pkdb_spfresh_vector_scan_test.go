// Copyright 2026 PingCAP, Inc.
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

package vectorsearch

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/pkg/config"
	ingesttestutil "github.com/pingcap/tidb/pkg/ddl/ingest/testutil"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func mockSPFreshBootstrapEndpoint(t *testing.T) string {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/mockSPFreshBuildTransport", "return(true)")
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/spfresh/build" && r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "unexpected SPFresh request method", http.StatusBadRequest)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		switch r.URL.Path {
		case "/spfresh/build-lifecycle":
			w.WriteHeader(http.StatusNoContent)
		case "/spfresh/bootstrap":
			w.WriteHeader(http.StatusOK)
		case "/spfresh/search":
			resp, err := proto.Marshal(&tipb.SPFreshSearchResponse{
				Result: &tipb.SPFreshSearchResponse_Success{
					Success: &tipb.SPFreshSearchResult{Rows: []*tipb.SPFreshSearchRow{}, Stats: &tipb.SPFreshSearchStats{}},
				},
			})
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/x-protobuf")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(resp)
		default:
			http.Error(w, "unexpected SPFresh request", http.StatusBadRequest)
		}
	}))
	t.Cleanup(server.Close)
	return server.URL
}

func TestSPFreshVectorScanExplainNormalized(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockBootstrapEmptySPFreshIndexes", `return(true)`)
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlanRows := func() []string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		plan, _ := plannercore.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		require.NoError(t, err)
		return getPlanRows(normalizedPlan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh")
	tk.MustExec("create table t_spfresh(vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh ((vec_cosine_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh values ('[1,1,1]'), ('[2,2,2]'), ('[3,3,3]')")

	tk.MustExec("explain select * from t_spfresh order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	joined := strings.Join(rows, "\n")
	require.True(t, strings.Contains(joined, "SPFreshVectorScan"), joined)
	require.True(t, strings.Contains(joined, "spfresh:COSINE(vec..[?], limit:?)"), joined)

	tk.MustExec("begin")
	staleReadTS := tk.MustQuery("select @@tidb_current_ts").Rows()[0][0].(string)
	tk.MustExec("commit")
	tk.MustExec("set @spfresh_stale_read_ts=tidb_parse_tso(" + staleReadTS + ")")
	tk.MustExec("explain select * from t_spfresh as of timestamp @spfresh_stale_read_ts order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows = getNormalizedPlanRows()
	joined = strings.Join(rows, "\n")
	require.True(t, strings.Contains(joined, "SPFreshVectorScan"), joined)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 SPFresh search does not support specified read timestamp"))

	tk.MustExec("begin")
	tk.MustExec("explain select * from t_spfresh order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows = getNormalizedPlanRows()
	joined = strings.Join(rows, "\n")
	require.True(t, strings.Contains(joined, "SPFreshVectorScan"), joined)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 SPFresh search does not support repeatable reads in explicit transactions"))
	tk.MustExec("rollback")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = nil
	})
	tk.MustExec("explain select * from t_spfresh order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows = getNormalizedPlanRows()
	joined = strings.Join(rows, "\n")
	require.False(t, strings.Contains(joined, "SPFreshVectorScan"), joined)
}

func TestSPFreshVectorScanExplainNormalizedIdOnly(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlanRows := func() []string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		plan, _ := plannercore.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		require.NoError(t, err)
		return getPlanRows(normalizedPlan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_id")
	tk.MustExec("create table t_spfresh_id(id int, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_id ((vec_cosine_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_id values (1, '[1,1,1]'), (2, '[2,2,2]'), (3, '[3,3,3]')")

	tk.MustExec("explain select id from t_spfresh_id order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

func TestSPFreshVectorScanFilteredTopNPushesSupportedFilter(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	explainBrief := func(sql string) string {
		rows := tk.MustQuery(sql).Rows()
		lines := make([]string, 0, len(rows))
		for _, row := range rows {
			parts := make([]string, 0, len(row))
			for _, col := range row {
				parts = append(parts, col.(string))
			}
			lines = append(lines, strings.Join(parts, " "))
		}
		return strings.Join(lines, "\n")
	}
	requireNormalFilteredPlan := func(plan string) {
		require.NotContains(t, plan, "SPFreshVectorScan", plan)
		require.Contains(t, plan, "TopN", plan)
		require.Contains(t, plan, "Projection", plan)
		require.Contains(t, plan, "Selection", plan)
		require.Contains(t, plan, "TableFullScan", plan)
		require.Contains(t, plan, "vec_cosine_distance", plan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_filter")
	tk.MustExec("create table t_spfresh_filter(id int not null, score int, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_filter ((vec_cosine_distance(vec))) using spfresh storing (id)")
	tk.MustExec("insert into t_spfresh_filter values (1, 10, '[1,1,1]'), (2, 20, '[2,2,2]'), (3, 30, '[3,3,3]')")

	unfiltered := explainBrief("explain format = 'brief' select * from t_spfresh_filter order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	require.Contains(t, unfiltered, "SPFreshVectorScan", unfiltered)

	filteredWhere := explainBrief("explain format = 'brief' select * from t_spfresh_filter where id > 1 order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	require.Contains(t, filteredWhere, "SPFreshVectorScan", filteredWhere)
	require.Contains(t, filteredWhere, "filter_cols:id", filteredWhere)

	filteredInWithNull := explainBrief("explain format = 'brief' select * from t_spfresh_filter where id in (2, null) order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	require.Contains(t, filteredInWithNull, "SPFreshVectorScan", filteredInWithNull)
	require.Contains(t, filteredInWithNull, "filter_cols:id", filteredInWithNull)

	filteredHaving := explainBrief("explain format = 'brief' select id, vec_cosine_distance(vec, '[0,0,0]') as d from t_spfresh_filter having id > 1 order by d limit 1")
	require.Contains(t, filteredHaving, "SPFreshVectorScan", filteredHaving)
	require.Contains(t, filteredHaving, "filter_cols:id", filteredHaving)

	filteredDistanceWhere := explainBrief("explain format = 'brief' select * from t_spfresh_filter where vec_cosine_distance(vec, '[0,0,0]') > 0 order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	requireNormalFilteredPlan(filteredDistanceWhere)

	filteredDistanceHaving := explainBrief("explain format = 'brief' select id, vec_cosine_distance(vec, '[0,0,0]') as d from t_spfresh_filter having d > 0 order by d limit 1")
	requireNormalFilteredPlan(filteredDistanceHaving)

	unsupported := explainBrief("explain format = 'brief' select * from t_spfresh_filter where score > 1 order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	requireNormalFilteredPlan(unsupported)
}

func TestSPFreshVectorScanL2AndNonVectorTopNFallback(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	explainBrief := func(sql string) string {
		rows := tk.MustQuery(sql).Rows()
		lines := make([]string, 0, len(rows))
		for _, row := range rows {
			parts := make([]string, 0, len(row))
			for _, col := range row {
				parts = append(parts, col.(string))
			}
			lines = append(lines, strings.Join(parts, " "))
		}
		return strings.Join(lines, "\n")
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_l2")
	tk.MustExec("create table t_spfresh_l2(id int primary key, score int, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_l2 ((vec_l2_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_l2 values (1, 30, '[1,1,1]'), (2, 20, '[2,2,2]'), (3, 10, '[3,3,3]')")

	l2Plan := explainBrief("explain format = 'brief' select id from t_spfresh_l2 order by vec_l2_distance(vec, '[0,0,0]') limit 2")
	require.Contains(t, l2Plan, "SPFreshVectorScan", l2Plan)
	require.Contains(t, l2Plan, "spfresh:L2", l2Plan)

	nonVectorTopN := explainBrief("explain format = 'brief' select id from t_spfresh_l2 order by score limit 2")
	require.NotContains(t, nonVectorTopN, "SPFreshVectorScan", nonVectorTopN)
	require.Contains(t, nonVectorTopN, "TopN", nonVectorTopN)
	require.Contains(t, nonVectorTopN, "TableFullScan", nonVectorTopN)

	for _, sql := range []string{
		"select id from t_spfresh_l2 order by vec_l2_distance(vec, '[0,0,0]') limit 1025",
		"select id from t_spfresh_l2 order by vec_l2_distance(vec, '[0,0,0]') limit 100 offset 1000",
	} {
		plan := explainBrief("explain format = 'brief' " + sql)
		require.Contains(t, plan, "SPFreshVectorScan", plan)
	}

	noLimit := explainBrief("explain format = 'brief' select id from t_spfresh_l2 order by vec_l2_distance(vec, '[0,0,0]')")
	require.NotContains(t, noLimit, "SPFreshVectorScan", noLimit)

	partitionedTopN := explainBrief(`explain format = 'brief'
		select * from (
			select id, row_number() over (partition by id order by vec_l2_distance(vec, '[0,0,0]')) as rn
			from t_spfresh_l2
			) t where rn <= 1`)
	require.NotContains(t, partitionedTopN, "SPFreshVectorScan", partitionedTopN)
	require.Contains(t, partitionedTopN, "Window", partitionedTopN)
	require.Contains(t, partitionedTopN, "TableFullScan", partitionedTopN)

	columnDefs := make([]string, 0, 257)
	columnNames := make([]string, 0, 257)
	for i := range 257 {
		name := fmt.Sprintf("c%d", i)
		columnDefs = append(columnDefs, name+" int")
		columnNames = append(columnNames, name)
	}
	tk.MustExec("create table t_spfresh_wide(id int primary key, " + strings.Join(columnDefs, ", ") + ", vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_wide ((vec_l2_distance(vec))) using spfresh")
	wideProjection := explainBrief("explain format = 'brief' select " + strings.Join(columnNames, ", ") + " from t_spfresh_wide order by vec_l2_distance(vec, '[0,0,0]') limit 1")
	require.Contains(t, wideProjection, "SPFreshVectorScan", wideProjection)
}

func TestSPFreshVectorScanInnerProduct(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlanRows := func() []string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		plan, _ := plannercore.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		require.NoError(t, err)
		return getPlanRows(normalizedPlan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_ip")
	tk.MustExec("create table t_spfresh_ip(vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_ip ((vec_negative_inner_product(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_ip values ('[1,0,0]'), ('[0,1,0]'), ('[2,0,0]')")

	tk.MustExec("explain select * from t_spfresh_ip order by vec_negative_inner_product(vec, '[1,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	joined := strings.Join(rows, "\n")
	require.True(t, strings.Contains(joined, "SPFreshVectorScan"), joined)
	require.True(t, strings.Contains(joined, "spfresh:INNER_PRODUCT(vec..[?], limit:?)"), joined)

	tk.MustExec("explain select * from t_spfresh_ip order by vec_cosine_distance(vec, '[1,0,0]') limit 1")
	rows = getNormalizedPlanRows()
	require.False(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

func TestSPFreshVectorScanExplicitVectorIndexWhenRemoteBackendEnabled(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlanRows := func() []string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		plan, _ := plannercore.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		require.NoError(t, err)
		return getPlanRows(normalizedPlan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_default")
	tk.MustExec("create table t_spfresh_default(vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_default ((vec_cosine_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_default values ('[1,1,1]'), ('[2,2,2]'), ('[3,3,3]')")

	tk.MustExec("explain select * from t_spfresh_default order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

func TestSPFreshVectorScanExplicitVectorIndexInlineWhenRemoteBackendEnabled(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlanRows := func() []string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		plan, _ := plannercore.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		require.NoError(t, err)
		return getPlanRows(normalizedPlan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_default_inline")
	tk.MustExec("create table t_spfresh_default_inline(vec vector(3), vector index idx_vec((vec_cosine_distance(vec))) using spfresh)")
	tk.MustExec("insert into t_spfresh_default_inline values ('[1,1,1]'), ('[2,2,2]'), ('[3,3,3]')")

	tk.MustExec("explain select * from t_spfresh_default_inline order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	require.True(t, strings.Contains(strings.Join(rows, "\n"), "SPFreshVectorScan"), strings.Join(rows, "\n"))
}

func TestSPFreshVectorScanExplainNormalizedDistanceProjection(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	getNormalizedPlanRows := func() []string {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		plan, _ := plannercore.NormalizePlan(p)
		normalizedPlan, err := plancodec.DecodeNormalizedPlan(plan)
		require.NoError(t, err)
		return getPlanRows(normalizedPlan)
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_dist")
	tk.MustExec("create table t_spfresh_dist(id int, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_dist ((vec_cosine_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_dist values (1, '[1,1,1]'), (2, '[2,2,2]'), (3, '[3,3,3]')")

	tk.MustExec("explain select id, vec_cosine_distance(vec, '[0,0,0]') from t_spfresh_dist order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	rows := getNormalizedPlanRows()
	joined := strings.Join(rows, "\n")
	require.True(t, strings.Contains(joined, "SPFreshVectorScan"), joined)
	require.True(t, strings.Contains(joined, "spfresh:COSINE(vec..[?], limit:?)"), joined)
	require.True(t, strings.Contains(joined, "->Column#"), joined)
}

func TestSPFreshVectorScanDistanceProjectionRequiresSameQueryVector(t *testing.T) {
	spfreshBootstrapURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshBootstrapURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	explainBrief := func(sql string) string {
		rows := tk.MustQuery("explain format = 'brief' " + sql).Rows()
		lines := make([]string, 0, len(rows))
		for _, row := range rows {
			parts := make([]string, 0, len(row))
			for _, col := range row {
				parts = append(parts, col.(string))
			}
			lines = append(lines, strings.Join(parts, " "))
		}
		return strings.Join(lines, "\n")
	}

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_spfresh_dist_vec")
	tk.MustExec("create table t_spfresh_dist_vec(id int, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_dist_vec ((vec_cosine_distance(vec))) using spfresh")

	matching := explainBrief("select id, vec_cosine_distance(vec, '[0,0,0]') from t_spfresh_dist_vec order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	require.Contains(t, matching, "SPFreshVectorScan", matching)
	require.Contains(t, matching, "->Column#", matching)
	require.NotContains(t, matching, "vec_cosine_distance", matching)

	mismatched := explainBrief("select id, vec_cosine_distance(vec, '[1,1,1]') from t_spfresh_dist_vec order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	require.Contains(t, mismatched, "SPFreshVectorScan", mismatched)
	require.Contains(t, mismatched, "vec_cosine_distance", mismatched)
}

func TestSPFreshVectorScanPreparedPlanCache(t *testing.T) {
	spfreshURL := mockSPFreshBootstrapEndpoint(t)
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVAPIServiceAddrs = []string{spfreshURL}
	})

	store, _ := testkit.CreateMockStoreAndDomain(t)
	defer ingesttestutil.InjectMockBackendCtx(t, store)()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_prepared_plan_cache = 1")
	tk.MustExec("drop table if exists t_spfresh_plan_cache")
	tk.MustExec("create table t_spfresh_plan_cache(id int, vec vector(3))")
	tk.MustExec("create vector index idx_vec on t_spfresh_plan_cache ((vec_cosine_distance(vec))) using spfresh")
	tk.MustExec("insert into t_spfresh_plan_cache values (1, '[1,1,1]')")

	tk.MustExec(`prepare st from "select id from t_spfresh_plan_cache order by vec_cosine_distance(vec, '[0,0,0]') limit 1"`)
	tk.MustQuery("execute st").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute st").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}
