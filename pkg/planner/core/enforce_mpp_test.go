// Copyright 2019 PingCAP, Inc.
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
	"fmt"
	"strconv"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMppAggShouldAlignFinalMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (" +
		"  d date," +
		"  v int," +
		"  primary key(d, v)" +
		") partition by range columns (d) (" +
		"  partition p1 values less than ('2023-07-02')," +
		"  partition p2 values less than ('2023-07-03')" +
		");")
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustExec(`set tidb_partition_prune_mode='static';`)
	err := failpoint.Enable("github.com/pingcap/tidb/pkg/expression/aggregation/show-agg-mode", "return(true)")
	require.Nil(t, err)

	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\"")
	tk.MustQuery("explain format='brief' select 1 from (" +
		"  select /*+ read_from_storage(tiflash[t]) */ sum(1)" +
		"  from t where d BETWEEN '2023-07-01' and '2023-07-03' group by d" +
		") total;").Check(testkit.Rows("Projection 400.00 root  1->Column#4",
		"└─HashAgg 400.00 root  group by:test.t.d, funcs:count(complete,1)->Column#8",
		"  └─PartitionUnion 400.00 root  ",
		"    ├─Projection 200.00 root  test.t.d",
		"    │ └─HashAgg 200.00 root  group by:test.t.d, funcs:firstrow(partial2,test.t.d)->test.t.d, funcs:count(final,Column#12)->Column#9",
		"    │   └─TableReader 200.00 root  MppVersion: 2, data:ExchangeSender",
		"    │     └─ExchangeSender 200.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    │       └─HashAgg 200.00 mpp[tiflash]  group by:test.t.d, funcs:count(partial1,1)->Column#12",
		"    │         └─TableRangeScan 250.00 mpp[tiflash] table:t, partition:p1 range:[2023-07-01,2023-07-03], keep order:false, stats:pseudo",
		"    └─Projection 200.00 root  test.t.d",
		"      └─HashAgg 200.00 root  group by:test.t.d, funcs:firstrow(partial2,test.t.d)->test.t.d, funcs:count(final,Column#14)->Column#10",
		"        └─TableReader 200.00 root  MppVersion: 2, data:ExchangeSender",
		"          └─ExchangeSender 200.00 mpp[tiflash]  ExchangeType: PassThrough",
		"            └─HashAgg 200.00 mpp[tiflash]  group by:test.t.d, funcs:count(partial1,1)->Column#14",
		"              └─TableRangeScan 250.00 mpp[tiflash] table:t, partition:p2 range:[2023-07-01,2023-07-03], keep order:false, stats:pseudo"))

	err = failpoint.Disable("github.com/pingcap/tidb/pkg/expression/aggregation/show-agg-mode")
	require.Nil(t, err)
}
func TestRowSizeInMPP(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10), b varchar(20), c varchar(256))")
	tk.MustExec("insert into t values (space(10), space(20), space(256))")
	tk.MustExec("analyze table t")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(`set @@tidb_opt_tiflash_concurrency_factor=1`)
	tk.MustExec(`set @@tidb_allow_mpp=1`)
	var costs [3]float64
	for i, col := range []string{"a", "b", "c"} {
		rs := tk.MustQuery(fmt.Sprintf(`explain format='verbose' select /*+ read_from_storage(tiflash[t]) */ %v from t`, col)).Rows()
		cost, err := strconv.ParseFloat(rs[0][2].(string), 64)
		require.NoError(t, err)
		costs[i] = cost
	}
	require.True(t, costs[0] < costs[1] && costs[1] < costs[2]) // rowSize can affect the final cost
}
