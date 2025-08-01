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

package vectorsearch

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/types"
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
}

func TestTiFlashANNIndex(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3),
			a int,
			b int,
			c vector(3),
			d vector
		)
	`)
	tk.MustExec("alter table t1 set tiflash replica 1;")
	tk.MustExec("alter table t1 add vector index ((vec_cosine_distance(vec))) USING HNSW;")
	tk.MustExec(`
		insert into t1 values
			('[1,1,1]', 1, 1, '[1,1,1]', '[1,1,1]'),
			('[2,2,2]', 2, 2, '[2,2,2]', '[2,2,2]'),
			('[3,3,3]', 3, 3, '[3,3,3]', '[3,3,3]')
	`)
	for range 4 {
		tk.MustExec("insert into t1(vec, a, b, c, d) select vec, a, b, c, d from t1")
	}
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	handle := dom.StatsHandle()
	err := statstestutil.HandleNextDDLEventWithTxn(handle)
	require.NoError(t, err)
	tk.MustExec("analyze table t1")

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
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

	getNormalizedPlan := func() ([]string, string) {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
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
			vec vector(3)
		)
	`)
	tk.MustExec("alter table t set tiflash replica 1;")
	tk.MustExec("alter table t add vector index ((vec_cosine_distance(vec))) using hnsw;")
	tk.MustExec(`
		insert into t values
			('[1,1,1]'),
			('[2,2,2]'),
			('[3,3,3]')
	`)

	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t")

	tk.MustExec("analyze table t")

	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash, tikv'")

	tk.MustExec("explain select * from t order by vec_cosine_distance(vec, '[0,0,0]') limit 1")
	p1, d1 := getNormalizedPlan()
	require.Equal(t, []string{
		" TopN                  root ?",
		" └─TableReader         root ",
		"   └─TopN              cop  ?",
		"     └─Projection      cop  test.t.vec, vec_cosine_distance(test.t.vec, ?)",
		"       └─TableFullScan cop  table:t, range:[?,?], keep order:false",
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

	// test for TiFlashReplica's Available
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tbl.Meta().TiFlashReplica.Available = false
	tk.MustExec("explain select * from t order by vec_cosine_distance(vec, '[1,2,3]') limit 3")
	p2, _ := getNormalizedPlan()
	require.Equal(t, []string{
		" TopN                  root ?",
		" └─TableReader         root ",
		"   └─TopN              cop  ?",
		"     └─Projection      cop  test.t.vec, vec_cosine_distance(test.t.vec, ?)",
		"       └─TableFullScan cop  table:t, range:[?,?], keep order:false",
	}, p2)
	tbl.Meta().TiFlashReplica.Available = true
	tk.MustExec("explain select * from t order by vec_cosine_distance(vec, '[1,2,3]') limit 3")
	_, d4 := getNormalizedPlan()
	require.Equal(t, d1, d4)
}

func TestANNInexWithSimpleCBO(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3),
			a int,
			b int,
			c vector(3),
			d vector
		)
	`)
	tk.MustExec("alter table t1 set tiflash replica 1;")
	tk.MustExec("alter table t1 add vector index ((vec_cosine_distance(vec))) USING HNSW;")
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	tk.MustUseIndex("select * from t1 order by vec_cosine_distance(vec, '[1,1,1]') limit 1", "vector_index")
}

func TestANNIndexWithNonIntClusteredPk(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, 1*time.Second, mockstore.WithMockTiFlash(2))

	tk := testkit.NewTestKit(t, store)

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`
		create table t1 (
			vec vector(3),
			a int,
			b int,
			c vector(3),
			d vector,
			primary key (a, b)
		)
	`)
	tk.MustExec("alter table t1 set tiflash replica 1;")
	tk.MustExec("alter table t1 add vector index ((vec_cosine_distance(vec))) USING HNSW;")
	tk.MustExec("insert into t1 values ('[1,1,1]', 1, 1, '[1,1,1]', '[1,1,1]')")
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	sctx := tk.Session()
	stmts, err := session.Parse(sctx, "select * from t1 use index(vector_index) order by vec_cosine_distance(vec, '[1,1,1]') limit 1")
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	stmt := stmts[0]
	ret := &core.PreprocessorReturn{}
	nodeW := resolve.NewNodeW(stmt)
	err = core.Preprocess(context.Background(), sctx, nodeW, core.WithPreprocessorReturn(ret))
	require.NoError(t, err)
	var finalPlanTree base.Plan
	finalPlanTree, _, err = planner.Optimize(context.Background(), sctx, nodeW, ret.InfoSchema)
	require.NoError(t, err)
	physicalTree, ok := finalPlanTree.(base.PhysicalPlan)
	require.True(t, ok)
	// Find the PhysicalTableReader node.
	tableReader := physicalTree
	for ; len(tableReader.Children()) > 0; tableReader = tableReader.Children()[0] {
	}
	castedTableReader, ok := tableReader.(*core.PhysicalTableReader)
	require.True(t, ok)
	tableScan, err := castedTableReader.GetTableScan()
	require.NoError(t, err)
	// Check that it has the extra vector index information.
	require.Len(t, tableScan.UsedColumnarIndexes, 1)
	require.True(t, tableScan.UsedColumnarIndexes[0].QueryInfo.IndexType == tipb.ColumnarIndexType_TypeVector)
	require.Len(t, tableScan.Ranges, 1)
	// Check that it's full scan.
	require.Equal(t, "[-inf,+inf]", tableScan.Ranges[0].String())
	// Check that the -inf and +inf are the correct types.
	require.Equal(t, types.KindMinNotNull, tableScan.Ranges[0].LowVal[0].Kind())
	require.Equal(t, types.KindMaxValue, tableScan.Ranges[0].HighVal[0].Kind())
}

func prepareVectorSearchWithPK(t *testing.T) *testkit.TestKit {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 200*time.Millisecond, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists doc")

	// A non-partitioned table
	tk.MustExec(`
		create table t1 (
			id int primary key,
			vec vector(3),
			a int,
			b int,
			c vector(3),
			d vector,
			VECTOR INDEX idx_embedding ((VEC_COSINE_DISTANCE(vec)))
		)
	`)
	for i := range 2000 {
		tk.MustExec(fmt.Sprintf(`
		insert into t1 values
			(%d, '[1,1,1]', 1, 1, '[1,1,1]', '[1,1,1]'),
			(%d, '[2,2,2]', 2, 2, '[2,2,2]', '[2,2,2]'),
			(%d, '[3,3,3]', 3, 3, '[3,3,3]', '[3,3,3]');
		`, i, 2000+i, 2000*2+i))
	}
	tk.MustExec("analyze table t1")

	// Another table for join
	tk.MustExec("create table doc(id INT, doc LONGTEXT)")

	testkit.SetTiFlashReplica(t, dom, "test", "t1")

	return tk
}

func TestVectorSearchWithPKAuto(t *testing.T) {
	tk := prepareVectorSearchWithPK(t)
	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	suiteData := GetANNIndexSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
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
	}
}

func TestVectorSearchWithPKForceTiKV(t *testing.T) {
	tk := prepareVectorSearchWithPK(t)
	tk.MustExec("set @@tidb_isolation_read_engines = 'tikv'")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	suiteData := GetANNIndexSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
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
	}
}

func TestVectorSearchHeavyFunction(t *testing.T) {
	tk := prepareVectorSearchWithPK(t)
	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	suiteData := GetANNIndexSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
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
	}
}
