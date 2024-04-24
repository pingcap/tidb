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
	"encoding/json"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/stretchr/testify/require"
)

func getPlanRows(planStr string) []string {
	planStr = strings.Replace(planStr, "\t", " ", -1)
	return strings.Split(planStr, "\n")
}

func compareStringSlice(t *testing.T, ss1, ss2 []string) {
	require.Equal(t, len(ss1), len(ss2))
	for i, s := range ss1 {
		require.Equal(t, len(s), len(ss2[i]))
	}
}

func TestPreferRangeScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=0`) // affect this ut: tidb_opt_prefer_range_scan
	tk.MustExec("drop table if exists test;")
	tk.MustExec("create table test(`id` int(10) NOT NULL AUTO_INCREMENT,`name` varchar(50) NOT NULL DEFAULT 'tidb',`age` int(11) NOT NULL,`addr` varchar(50) DEFAULT 'The ocean of stars',PRIMARY KEY (`id`),KEY `idx_age` (`age`))")
	tk.MustExec("insert into test(age) values(5);")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("insert into test(name,age,addr) select name,age,addr from test;")
	tk.MustExec("analyze table test;")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	planNormalizedSuiteData := GetPlanNormalizedSuiteData()
	planNormalizedSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		if i == 0 {
			tk.MustExec("set session tidb_opt_prefer_range_scan=0")
		} else if i == 1 {
			tk.MustExec("set session tidb_opt_prefer_range_scan=1")
		}
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

func TestNormalizedPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='static';")
	tk.MustExec("drop table if exists t1,t2,t3,t4")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t2 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t3 (a int key,b int) partition by hash(a) partitions 2;")
	tk.MustExec("create table t4 (a int, b int, index(a)) partition by range(a) (partition p0 values less than (10),partition p1 values less than MAXVALUE);")
	tk.MustExec("set @@global.tidb_enable_foreign_key=1")
	tk.MustExec("set @@foreign_key_checks=1")
	tk.MustExec("create table t5 (id int key, id2 int, id3 int, unique index idx2(id2), index idx3(id3));")
	tk.MustExec("create table t6 (id int,     id2 int, id3 int, index idx_id(id), index idx_id2(id2), " +
		"foreign key fk_1 (id) references t5(id) ON UPDATE CASCADE ON DELETE CASCADE, " +
		"foreign key fk_2 (id2) references t5(id2) ON UPDATE CASCADE, " +
		"foreign key fk_3 (id3) references t5(id3) ON DELETE CASCADE);")
	tk.MustExec("insert into t5 values (1,1,1), (2,2,2)")
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
		// Test for GenHintsFromFlatPlan won't panic.
		core.GenHintsFromFlatPlan(flat)

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

func TestPlanDigest4InList(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int);")
	tk.MustExec("set global tidb_ignore_inlist_plan_digest=true;")
	tk.Session().GetSessionVars().PlanID.Store(0)
	queriesGroup1 := []string{
		"select * from t where a in (1, 2);",
		"select a in (1, 2) from t;",
	}
	queriesGroup2 := []string{
		"select * from t where a in (1, 2, 3);",
		"select a in (1, 2, 3) from t;",
	}
	for i := 0; i < len(queriesGroup1); i++ {
		query1 := queriesGroup1[i]
		query2 := queriesGroup2[i]
		t.Run(query1+" vs "+query2, func(t *testing.T) {
			tk.MustExec(query1)
			info1 := tk.Session().ShowProcess()
			require.NotNil(t, info1)
			p1, ok := info1.Plan.(base.Plan)
			require.True(t, ok)
			_, digest1 := core.NormalizePlan(p1)
			tk.MustExec(query2)
			info2 := tk.Session().ShowProcess()
			require.NotNil(t, info2)
			p2, ok := info2.Plan.(base.Plan)
			require.True(t, ok)
			_, digest2 := core.NormalizePlan(p2)
			require.Equal(t, digest1, digest2)
		})
	}
}

func TestIssue47634(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t3,t4")
	tk.MustExec("create table t3(a int, b int, c int);")
	tk.MustExec("create table t4(a int, b int, c int, primary key (a, b) clustered);")
	tk.MustExec("create table t5(a int, b int, c int, key idx_a_b (a, b));")
	tk.Session().GetSessionVars().PlanID.Store(0)
	queriesGroup1 := []string{
		"explain select /*+ inl_join(t4) */ * from t3 join t4 on t3.b = t4.b where t4.a = 1;",
		"explain select /*+ inl_join(t5) */ * from t3 join t5 on t3.b = t5.b where t5.a = 1;",
	}
	queriesGroup2 := []string{
		"explain select /*+ inl_join(t4) */ * from t3 join t4 on t3.b = t4.b where t4.a = 2;",
		"explain select /*+ inl_join(t5) */ * from t3 join t5 on t3.b = t5.b where t5.a = 2;",
	}
	for i := 0; i < len(queriesGroup1); i++ {
		query1 := queriesGroup1[i]
		query2 := queriesGroup2[i]
		t.Run(query1+" vs "+query2, func(t *testing.T) {
			tk.MustExec(query1)
			info1 := tk.Session().ShowProcess()
			require.NotNil(t, info1)
			p1, ok := info1.Plan.(base.Plan)
			require.True(t, ok)
			_, digest1 := core.NormalizePlan(p1)
			tk.MustExec(query2)
			info2 := tk.Session().ShowProcess()
			require.NotNil(t, info2)
			p2, ok := info2.Plan.(base.Plan)
			require.True(t, ok)
			_, digest2 := core.NormalizePlan(p2)
			require.Equal(t, digest1, digest2)
		})
	}
}

func TestNormalizedPlanForDiffStore(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int, c int, primary key(a))")
	tk.MustExec("insert into t1 values(1,1,1), (2,2,2), (3,3,3)")
	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	var input []string
	var output []struct {
		Digest string
		Plan   []string
	}
	planNormalizedSuiteData := GetPlanNormalizedSuiteData()
	planNormalizedSuiteData.LoadTestCases(t, &input, &output)
	lastDigest := ""
	for i, tt := range input {
		tk.Session().GetSessionVars().PlanID.Store(0)
		tk.MustExec(tt)
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		ep, ok := info.Plan.(*core.Explain)
		require.True(t, ok)
		normalized, digest := core.NormalizePlan(ep.TargetPlan)

		// test the new normalization code
		flat := core.FlattenPhysicalPlan(ep.TargetPlan, false)
		newNormalized, newPlanDigest := core.NormalizeFlatPlan(flat)
		require.Equal(t, digest, newPlanDigest)
		require.Equal(t, normalized, newNormalized)

		normalizedPlan, err := plancodec.DecodeNormalizedPlan(normalized)
		normalizedPlanRows := getPlanRows(normalizedPlan)
		require.NoError(t, err)
		testdata.OnRecord(func() {
			output[i].Digest = digest.String()
			output[i].Plan = normalizedPlanRows
		})
		compareStringSlice(t, normalizedPlanRows, output[i].Plan)
		require.NotEqual(t, digest.String(), lastDigest)
		lastDigest = digest.String()
	}
}

func TestJSONPlanInExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(id int, key(id))")
	tk.MustExec("create table t2(id int, key(id))")

	var input []string
	var output []struct {
		SQL      string
		JSONPlan []*core.ExplainInfoForEncode
	}
	planSuiteData := GetJSONPlanSuiteData()
	planSuiteData.LoadTestCases(t, &input, &output)

	for i, test := range input {
		resJSON := tk.MustQuery(test).Rows()
		var res []*core.ExplainInfoForEncode
		require.NoError(t, json.Unmarshal([]byte(resJSON[0][0].(string)), &res))
		for j, expect := range output[i].JSONPlan {
			require.Equal(t, expect.ID, res[j].ID)
			require.Equal(t, expect.EstRows, res[j].EstRows)
			require.Equal(t, expect.ActRows, res[j].ActRows)
			require.Equal(t, expect.TaskType, res[j].TaskType)
			require.Equal(t, expect.AccessObject, res[j].AccessObject)
			require.Equal(t, expect.OperatorInfo, res[j].OperatorInfo)
		}
	}
}
