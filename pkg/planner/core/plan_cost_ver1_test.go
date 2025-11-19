// Copyright 2022 PingCAP, Inc.
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

package core_test

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTrueCardCost(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int primary key, b int, key(b))`)

	checkPlanCost := func(sql string) {
		rs := tk.MustQuery(`explain analyze format=verbose ` + sql).Rows()
		planCost1 := rs[0][2].(string)

		rs = tk.MustQuery(`explain analyze format=true_card_cost ` + sql).Rows()
		planCost2 := rs[0][2].(string)

		// `true_card_cost` can work since the plan cost is changed
		require.NotEqual(t, planCost1, planCost2)
	}

	checkPlanCost(`select * from t`)
	checkPlanCost(`select * from t where a>10`)
	checkPlanCost(`select * from t where a>10 limit 10`)
	checkPlanCost(`select sum(a), b*2 from t use index(b) group by b order by sum(a) limit 10`)
}

func TestIssue36243(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`insert into mysql.expr_pushdown_blacklist values ('>','tikv','')`)
	tk.MustExec(`admin reload expr_pushdown_blacklist`)

	getCost := func() (selCost, readerCost float64) {
		res := tk.MustQuery(`explain format=verbose select * from t where a>0`).Rows()
		// TableScan -> TableReader -> Selection
		require.Equal(t, len(res), 3)
		require.Contains(t, res[0][0], "Selection")
		require.Contains(t, res[1][0], "TableReader")
		require.Contains(t, res[2][0], "Scan")
		var err error
		selCost, err = strconv.ParseFloat(res[0][2].(string), 64)
		require.NoError(t, err)
		readerCost, err = strconv.ParseFloat(res[1][2].(string), 64)
		require.NoError(t, err)
		return
	}

	tk.MustExec(`set @@tidb_cost_model_version=1`)
	// Selection has the same cost with TableReader, ignore Selection cost for compatibility in cost model ver1.
	selCost, readerCost := getCost()
	require.Equal(t, selCost, readerCost)

	tk.MustExec(`set @@tidb_cost_model_version=2`)
	selCost, readerCost = getCost()
	require.True(t, selCost > readerCost)
}

func TestScanOnSmallTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int)`)
	tk.MustExec("insert into t values (1), (2), (3), (4), (5)")
	tk.MustExec("analyze table t all columns")
	tk.MustExec(`set @@tidb_cost_model_version=2`)

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(ast.NewCIStr("test"))
	require.True(t, exists)
	tblInfos, err := is.SchemaTableInfos(context.Background(), db.Name)
	require.NoError(t, err)
	for _, tblInfo := range tblInfos {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	result := tk.MustQuery("explain select * from t")
	resStr := result.String()
	rs := result.Rows()
	useTiKVScan := false
	for _, r := range rs {
		op := r[0].(string)
		task := r[2].(string)
		if strings.Contains(op, "Scan") && strings.Contains(task, "tikv") {
			useTiKVScan = true
		}
	}
	require.True(t, useTiKVScan, "should use tikv scan, but got:\n%s", resStr)
}

func TestMaxEstimatedCost(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set @@tidb_cost_model_version=2`)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int auto_increment primary key, b int, filler varchar(255))`)
	tk.MustExec(`insert into t (b) values (1),(2),(3),(4)`)
	tk.MustExec(`insert into t (b) select t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5`)
	tk.MustExec(`analyze table t`)
	tk.MustContainErrMsg(`select /*+ SET_VAR(tidb_max_estimated_cost="1e12") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 2.9894856323188037e+12 > 1e+12")
	tk.MustContainErrMsg(`explain analyze format=verbose select /*+ SET_VAR(tidb_max_estimated_cost="1e12") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 2.9894856323188037e+12 > 1e+12")
	res := tk.MustQuery(`explain format=verbose select /*+ SET_VAR(tidb_max_estimated_cost="1e12") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`)
	require.Equal(t, "2989485632318.80", res.Rows()[0][2])
	tk.MustExec(`set @@tidb_cost_model_version=1`)
	tk.MustContainErrMsg(`select /*+ SET_VAR(tidb_max_estimated_cost="1e11") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 5.0864721888560364e+11 > 1e+11")
	tk.MustContainErrMsg(`explain analyze format=verbose select /*+ SET_VAR(tidb_max_estimated_cost="1e11") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 5.0864721888560364e+11 > 1e+11")
	res = tk.MustQuery(`explain format=verbose select /*+ SET_VAR(tidb_max_estimated_cost="1e11") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`)
	require.Equal(t, "508647218885.60", res.Rows()[0][2])
	tk.MustExec(`set @@tidb_cost_model_version=2`)
	tk.MustExec(`alter table t add index idx_b (b)`)
	tk.MustExec(`analyze table t`)
	// HashJoin cost:  v2                                                                688000108471
	// HashJoin cost:  v1                                                                508647218885
	tk.MustContainErrMsg(`select /*+ SET_VAR(tidb_max_estimated_cost="1e11") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 6.880001084711974e+11 > 1e+11")
	tk.MustContainErrMsg(`explain analyze format=verbose select /*+ SET_VAR(tidb_max_estimated_cost="1e11") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 6.880001084711974e+11 > 1e+11")
	res = tk.MustQuery(`explain format=verbose select /*+ SET_VAR(tidb_max_estimated_cost="1e11") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`)
	require.Equal(t, "688000108471.20", res.Rows()[0][2])
	tk.MustExec(`set @@tidb_cost_model_version=1`)
	tk.MustContainErrMsg(`select /*+ SET_VAR(tidb_max_estimated_cost="1e10") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 8.271535799387799e+10 > 1e+10")
	tk.MustContainErrMsg(`explain analyze select /*+ SET_VAR(tidb_max_estimated_cost="1e10") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 8.271535799387799e+10 > 1e+10")
	res = tk.MustQuery(`explain format=verbose select /*+ SET_VAR(tidb_max_estimated_cost="1e10") */ t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`)
	require.Equal(t, "82715357993.88", res.Rows()[0][2])

	tk.MustExec(`set tidb_max_estimated_cost=1e10`)
	tk.MustExec(`set @@tidb_cost_model_version=2`)
	tk.MustContainErrMsg(`select t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 6.880001084711974e+11 > 1e+10")
	tk.MustContainErrMsg(`explain analyze format=verbose select t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 6.880001084711974e+11 > 1e+10")
	res = tk.MustQuery(`explain format=verbose select t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`)
	require.Equal(t, "688000108471.20", res.Rows()[0][2])
	tk.MustExec(`set @@tidb_cost_model_version=1`)
	tk.MustContainErrMsg(`select t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 8.271535799387799e+10 > 1e+10")
	tk.MustContainErrMsg(`explain analyze select t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`, "optimizer cost exceeds tidb_max_estimated_cost: 8.271535799387799e+10 > 1e+10")
	res = tk.MustQuery(`explain format=verbose select t.b + t1.b + t2.b + t3.b + t4.b + t5.b from t, t t1, t t2, t t3, t t4, t t5 where t.a = 3 and t1.b = 3 and t2.a = 3`)
	require.Equal(t, "82715357993.88", res.Rows()[0][2])
}
