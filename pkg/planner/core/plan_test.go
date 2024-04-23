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
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecodePlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table tp (a int ,b int,c int) partition by hash(b) partitions 5;")
	tk.MustExec("set tidb_enable_collect_execution_info=1;")
	tk.MustExec("set tidb_partition_prune_mode='static';")

	tk.Session().GetSessionVars().PlanID.Store(0)
	getPlanTree := func() (str1, str2 string) {
		info := tk.Session().ShowProcess()
		require.NotNil(t, info)
		p, ok := info.Plan.(base.Plan)
		require.True(t, ok)
		encodeStr := core.EncodePlan(p)
		planTree, err := plancodec.DecodePlan(encodeStr)
		require.NoError(t, err)

		// test the new encoding method
		flat := core.FlattenPhysicalPlan(p, true)
		newEncodeStr := core.EncodeFlatPlan(flat)
		newPlanTree, err := plancodec.DecodePlan(newEncodeStr)
		require.NoError(t, err)

		return planTree, newPlanTree
	}
	tk.MustExec("select max(a) from t1 where a>0;")
	planTree, newplanTree := getPlanTree()
	require.Contains(t, planTree, "time")
	require.Contains(t, planTree, "loops")
	require.Contains(t, newplanTree, "time")
	require.Contains(t, newplanTree, "loops")

	tk.MustExec("prepare stmt from \"select max(a) from t1 where a > ?\";")
	tk.MustExec("set @a = 1;")
	tk.MustExec("execute stmt using @a;")
	planTree, newplanTree = getPlanTree()
	require.Empty(t, planTree)
	require.Empty(t, newplanTree)

	tk.MustExec("insert into t1 values (1,1,1), (2,2,2);")
	planTree, newplanTree = getPlanTree()
	require.Contains(t, planTree, "Insert")
	require.Contains(t, planTree, "time")
	require.Contains(t, planTree, "loops")
	require.Contains(t, newplanTree, "Insert")
	require.Contains(t, newplanTree, "time")
	require.Contains(t, newplanTree, "loops")

	tk.MustExec("update t1 set b = 3 where c = 1;")
	planTree, newplanTree = getPlanTree()
	require.Contains(t, planTree, "Update")
	require.Contains(t, planTree, "time")
	require.Contains(t, planTree, "loops")
	require.Contains(t, newplanTree, "Update")
	require.Contains(t, newplanTree, "time")
	require.Contains(t, newplanTree, "loops")

	tk.MustExec("delete from t1 where b = 3;")
	planTree, newplanTree = getPlanTree()
	require.Contains(t, planTree, "Delete")
	require.Contains(t, planTree, "time")
	require.Contains(t, planTree, "loops")
	require.Contains(t, newplanTree, "Delete")
	require.Contains(t, newplanTree, "time")
	require.Contains(t, newplanTree, "loops")

	tk.MustExec("with cte(a) as (select 1) select * from cte")
	planTree, newplanTree = getPlanTree()
	require.Contains(t, planTree, "Projection_7")
	require.Contains(t, planTree, "1->Column#3")
	require.Contains(t, planTree, "time")
	require.Contains(t, planTree, "loops")
	require.Contains(t, newplanTree, "Projection_7")
	require.Contains(t, newplanTree, "1->Column#3")
	require.Contains(t, newplanTree, "time")
	require.Contains(t, newplanTree, "loops")

	tk.MustExec("with cte(a) as (select 2) select * from cte")
	planTree, newplanTree = getPlanTree()
	require.Contains(t, planTree, "Projection_7")
	require.Contains(t, planTree, "2->Column#3")
	require.Contains(t, planTree, "time")
	require.Contains(t, planTree, "loops")
	require.Contains(t, newplanTree, "Projection_7")
	require.Contains(t, newplanTree, "2->Column#3")
	require.Contains(t, newplanTree, "time")
	require.Contains(t, newplanTree, "loops")

	tk.MustExec("select * from tp")
	planTree, newplanTree = getPlanTree()
	require.Contains(t, planTree, "PartitionUnion")
	require.Contains(t, newplanTree, "PartitionUnion")

	tk.MustExec("select row_number() over (partition by c) from t1;")
	planTree, newplanTree = getPlanTree()
	require.Contains(t, planTree, "Shuffle")
	require.Contains(t, planTree, "ShuffleReceiver")
	require.Contains(t, newplanTree, "Shuffle")
	require.Contains(t, newplanTree, "ShuffleReceiver")
}

func TestNormalizedDigest(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1,t2,t3,t4, bmsql_order_line, bmsql_district,bmsql_stock")
	tk.MustExec("create table t1 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t2 (a int key,b int,c int, index (b));")
	tk.MustExec("create table t3 (a int, b int, index(a)) partition by range(a) (partition p0 values less than (10),partition p1 values less than MAXVALUE);")
	tk.MustExec("create table t4 (a int key,b int) partition by hash(a) partitions 2;")
	tk.MustExec(`CREATE TABLE  bmsql_order_line  (
	   ol_w_id  int(11) NOT NULL,
	   ol_d_id  int(11) NOT NULL,
	   ol_o_id  int(11) NOT NULL,
	   ol_number  int(11) NOT NULL,
	   ol_i_id  int(11) NOT NULL,
	   ol_delivery_d  timestamp NULL DEFAULT NULL,
	   ol_amount  decimal(6,2) DEFAULT NULL,
	   ol_supply_w_id  int(11) DEFAULT NULL,
	   ol_quantity  int(11) DEFAULT NULL,
	   ol_dist_info  char(24) DEFAULT NULL,
	  PRIMARY KEY ( ol_w_id , ol_d_id , ol_o_id , ol_number ) NONCLUSTERED
	);`)
	tk.MustExec(`CREATE TABLE  bmsql_district  (
	   d_w_id  int(11) NOT NULL,
	   d_id  int(11) NOT NULL,
	   d_ytd  decimal(12,2) DEFAULT NULL,
	   d_tax  decimal(4,4) DEFAULT NULL,
	   d_next_o_id  int(11) DEFAULT NULL,
	   d_name  varchar(10) DEFAULT NULL,
	   d_street_1  varchar(20) DEFAULT NULL,
	   d_street_2  varchar(20) DEFAULT NULL,
	   d_city  varchar(20) DEFAULT NULL,
	   d_state  char(2) DEFAULT NULL,
	   d_zip  char(9) DEFAULT NULL,
	  PRIMARY KEY ( d_w_id , d_id ) NONCLUSTERED
	);`)
	tk.MustExec(`CREATE TABLE  bmsql_stock  (
	   s_w_id  int(11) NOT NULL,
	   s_i_id  int(11) NOT NULL,
	   s_quantity  int(11) DEFAULT NULL,
	   s_ytd  int(11) DEFAULT NULL,
	   s_order_cnt  int(11) DEFAULT NULL,
	   s_remote_cnt  int(11) DEFAULT NULL,
	   s_data  varchar(50) DEFAULT NULL,
	   s_dist_01  char(24) DEFAULT NULL,
	   s_dist_02  char(24) DEFAULT NULL,
	   s_dist_03  char(24) DEFAULT NULL,
	   s_dist_04  char(24) DEFAULT NULL,
	   s_dist_05  char(24) DEFAULT NULL,
	   s_dist_06  char(24) DEFAULT NULL,
	   s_dist_07  char(24) DEFAULT NULL,
	   s_dist_08  char(24) DEFAULT NULL,
	   s_dist_09  char(24) DEFAULT NULL,
	   s_dist_10  char(24) DEFAULT NULL,
	  PRIMARY KEY ( s_w_id , s_i_id ) NONCLUSTERED
	);`)

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/planner/mockRandomPlanID", "return(true)")
	require.NoError(t, err)
	defer func() {
		err = failpoint.Disable("github.com/pingcap/tidb/pkg/planner/mockRandomPlanID")
		require.NoError(t, err)
	}()

	normalizedDigestCases := []struct {
		sql1   string
		sql2   string
		isSame bool
	}{
		{
			sql1:   "select * from t1;",
			sql2:   "select * from t2;",
			isSame: false,
		},
		{ // test for tableReader and tableScan.
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a<2",
			isSame: true,
		},
		{
			sql1:   "select * from t1 where a<1",
			sql2:   "select * from t1 where a=2",
			isSame: false,
		},
		{ // test for point get.
			sql1:   "select * from t1 where a=3",
			sql2:   "select * from t1 where a=2",
			isSame: true,
		},
		{ // test for indexLookUp.
			sql1:   "select * from t1 use index(b) where b=3",
			sql2:   "select * from t1 use index(b) where b=1",
			isSame: true,
		},
		{ // test for indexReader.
			sql1:   "select a+1,b+2 from t1 use index(b) where b=3",
			sql2:   "select a+2,b+3 from t1 use index(b) where b=2",
			isSame: true,
		},
		{ // test for merge join.
			sql1:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>2;",
			isSame: true,
		},
		{ // test for indexLookUpJoin.
			sql1:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for hashJoin.
			sql1:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: true,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
		{ // test for diff join.
			sql1:   "SELECT /*+ TIDB_INLJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>1;",
			sql2:   "SELECT /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.a = t2.a and t1.c>3;",
			isSame: false,
		},
		{ // test for apply.
			sql1:   "select * from t1 where t1.b > 0 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >1)",
			sql2:   "select * from t1 where t1.b > 1 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >0)",
			isSame: true,
		},
		{ // test for apply.
			sql1:   "select * from t1 where t1.b > 0 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null and t2.c >1)",
			sql2:   "select * from t1 where t1.b > 1 and  t1.a in (select sum(t2.b) from t2 where t2.a=t1.a and t2.b is not null)",
			isSame: false,
		},
		{ // test for topN.
			sql1:   "SELECT * from t1 where a!=1 order by c limit 1",
			sql2:   "SELECT * from t1 where a!=2 order by c limit 2",
			isSame: true,
		},
		{ // test for union
			sql1:   "select count(1) as num,a from t1 where a=1 group by a union select count(1) as num,a from t1 where a=3 group by a;",
			sql2:   "select count(1) as num,a from t1 where a=2 group by a union select count(1) as num,a from t1 where a=4 group by a;",
			isSame: true,
		},
		{ // test for tablescan partition
			sql1:   "select * from t3 where a=5",
			sql2:   "select * from t3 where a=15",
			isSame: true,
		},
		{ // test for point get partition
			sql1:   "select * from t4 where a=4",
			sql2:   "select * from t4 where a=30",
			isSame: true,
		},
		{
			sql1: `SELECT  COUNT(*) AS low_stock
					FROM
					(
						SELECT  *
						FROM bmsql_stock
						WHERE s_w_id = 1
						AND s_quantity < 2
						AND s_i_id IN ( SELECT /*+ TIDB_INLJ(bmsql_order_line) */ ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 20 AND ol_o_id < d_next_o_id WHERE d_w_id = 1 AND d_id = 2 )
					) AS L;`,
			sql2: `SELECT  COUNT(*) AS low_stock
					FROM
					(
						SELECT  *
						FROM bmsql_stock
						WHERE s_w_id = 5
						AND s_quantity < 6
						AND s_i_id IN ( SELECT /*+ TIDB_INLJ(bmsql_order_line) */ ol_i_id FROM bmsql_district JOIN bmsql_order_line ON ol_w_id = d_w_id AND ol_d_id = d_id AND ol_o_id >= d_next_o_id - 70 AND ol_o_id < d_next_o_id WHERE d_w_id = 5 AND d_id = 6 )
					) AS L;`,
			isSame: true,
		},
	}
	for _, testCase := range normalizedDigestCases {
		testNormalizeDigest(tk, t, testCase.sql1, testCase.sql2, testCase.isSame)
	}
}

func testNormalizeDigest(tk *testkit.TestKit, t *testing.T, sql1, sql2 string, isSame bool) {
	tk.MustQuery(sql1)
	info := tk.Session().ShowProcess()
	require.NotNil(t, info)
	physicalPlan, ok := info.Plan.(base.PhysicalPlan)
	require.True(t, ok)
	normalized1, digest1 := core.NormalizePlan(physicalPlan)

	// test the new normalization code
	flat := core.FlattenPhysicalPlan(physicalPlan, false)
	newNormalized, newPlanDigest := core.NormalizeFlatPlan(flat)
	require.Equal(t, digest1, newPlanDigest)
	require.Equal(t, normalized1, newNormalized)

	tk.MustQuery(sql2)
	info = tk.Session().ShowProcess()
	require.NotNil(t, info)
	physicalPlan, ok = info.Plan.(base.PhysicalPlan)
	require.True(t, ok)
	normalized2, digest2 := core.NormalizePlan(physicalPlan)

	// test the new normalization code
	flat = core.FlattenPhysicalPlan(physicalPlan, false)
	newNormalized, newPlanDigest = core.NormalizeFlatPlan(flat)
	require.Equal(t, digest2, newPlanDigest)
	require.Equal(t, normalized2, newNormalized)

	comment := fmt.Sprintf("sql1: %v, sql2: %v\n%v !=\n%v\n", sql1, sql2, normalized1, normalized2)
	if isSame {
		require.Equal(t, normalized1, normalized2, comment)
		require.Equal(t, digest1.String(), digest2.String(), comment)
	} else {
		require.NotEqual(t, normalized1, normalized2, comment)
		require.NotEqual(t, digest1.String(), digest2.String(), comment)
	}
}

func TestExplainFormatHintRecoverableForTiFlashReplica(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	// Create virtual `tiflash` replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := tk.MustQuery("explain select * from t").Rows()
	require.Equal(t, rows[len(rows)-1][2], "mpp[tiflash]")

	rows = tk.MustQuery("explain format='hint' select * from t").Rows()
	require.Equal(t, rows[0][0], "read_from_storage(@`sel_1` tiflash[`test`.`t`])")

	hints := tk.MustQuery("explain format='hint' select * from t;").Rows()[0][0]
	rows = tk.MustQuery(fmt.Sprintf("explain select /*+ %s */ * from t", hints)).Rows()
	require.Equal(t, rows[len(rows)-1][2], "mpp[tiflash]")
}

func BenchmarkDecodePlan(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) key,b int);")
	tk.MustExec("set @@tidb_slow_log_threshold=200000")

	// generate SQL
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024*4))
	for i := 0; i < 50000; i++ {
		if i > 0 {
			buf.WriteString(" union ")
		}
		buf.WriteString(fmt.Sprintf("select count(1) as num,a from t where a='%v' group by a", i))
	}
	query := buf.String()
	tk.Session().GetSessionVars().PlanID.Store(0)
	tk.MustExec(query)
	info := tk.Session().ShowProcess()
	require.NotNil(b, info)
	p, ok := info.Plan.(base.PhysicalPlan)
	require.True(b, ok)
	// TODO: optimize the encode plan performance when encode plan with runtimeStats
	tk.Session().GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	encodedPlanStr := core.EncodePlan(p)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := plancodec.DecodePlan(encodedPlanStr)
		require.NoError(b, err)
	}
}

func BenchmarkEncodePlan(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists th")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustExec("create table th (i int, a int,b int, c int, index (a)) partition by hash (a) partitions 8192;")
	tk.MustExec("set @@tidb_slow_log_threshold=200000")

	query := "select count(*) from th t1 join th t2 join th t3 join th t4 join th t5 join th t6 where t1.i=t2.a and t1.i=t3.i and t3.i=t4.i and t4.i=t5.i and t5.i=t6.i"
	tk.Session().GetSessionVars().PlanID.Store(0)
	tk.MustExec(query)
	info := tk.Session().ShowProcess()
	require.NotNil(b, info)
	p, ok := info.Plan.(base.PhysicalPlan)
	require.True(b, ok)
	tk.Session().GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		core.EncodePlan(p)
	}
}

func BenchmarkEncodeFlatPlan(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists th")
	tk.MustExec("set @@session.tidb_enable_table_partition = 1")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustExec("create table th (i int, a int,b int, c int, index (a)) partition by hash (a) partitions 8192;")
	tk.MustExec("set @@tidb_slow_log_threshold=200000")

	query := "select count(*) from th t1 join th t2 join th t3 join th t4 join th t5 join th t6 where t1.i=t2.a and t1.i=t3.i and t3.i=t4.i and t4.i=t5.i and t5.i=t6.i"
	tk.Session().GetSessionVars().PlanID.Store(0)
	tk.MustExec(query)
	info := tk.Session().ShowProcess()
	require.NotNil(b, info)
	p, ok := info.Plan.(base.PhysicalPlan)
	require.True(b, ok)
	tk.Session().GetSessionVars().StmtCtx.RuntimeStatsColl = nil
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		flat := core.FlattenPhysicalPlan(p, false)
		core.EncodeFlatPlan(flat)
	}
}

func TestCopPaging(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set session tidb_enable_paging = 1")
	tk.MustExec("create table t(id int, c1 int, c2 int, primary key (id), key i(c1))")
	defer tk.MustExec("drop table t")
	for i := 0; i < 1024; i++ {
		tk.MustExec("insert into t values(?, ?, ?)", i, i, i)
	}
	tk.MustExec("analyze table t")

	// limit 960 should go paging
	for i := 0; i < 10; i++ {
		tk.MustQuery("explain format='brief' select * from t force index(i) where id <= 1024 and c1 >= 0 and c1 <= 1024 and c2 in (2, 4, 6, 8) order by c1 limit 960").Check(testkit.Rows(
			"Limit 4.00 root  offset:0, count:960",
			"└─IndexLookUp 4.00 root  ",
			"  ├─Selection(Build) 1024.00 cop[tikv]  le(test.t.id, 1024)",
			"  │ └─IndexRangeScan 1024.00 cop[tikv] table:t, index:i(c1) range:[0,1024], keep order:true",
			"  └─Selection(Probe) 4.00 cop[tikv]  in(test.t.c2, 2, 4, 6, 8)",
			"    └─TableRowIDScan 1024.00 cop[tikv] table:t keep order:false"))
	}

	// selection between limit and indexlookup, limit 960 should also go paging
	for i := 0; i < 10; i++ {
		tk.MustQuery("explain format='brief' select * from t force index(i) where mod(id, 2) > 0 and id <= 1024 and c1 >= 0 and c1 <= 1024 and c2 in (2, 4, 6, 8) order by c1 limit 960").Check(testkit.Rows(
			"Limit 3.20 root  offset:0, count:960",
			"└─IndexLookUp 3.20 root  ",
			"  ├─Selection(Build) 819.20 cop[tikv]  gt(mod(test.t.id, 2), 0), le(test.t.id, 1024)",
			"  │ └─IndexRangeScan 1024.00 cop[tikv] table:t, index:i(c1) range:[0,1024], keep order:true",
			"  └─Selection(Probe) 3.20 cop[tikv]  in(test.t.c2, 2, 4, 6, 8)",
			"    └─TableRowIDScan 819.20 cop[tikv] table:t keep order:false"))
	}

	// limit 961 exceeds the threshold, it should not go paging
	for i := 0; i < 10; i++ {
		tk.MustQuery("explain format='brief' select * from t force index(i) where id <= 1024 and c1 >= 0 and c1 <= 1024 and c2 in (2, 4, 6, 8) order by c1 limit 961").Check(testkit.Rows(
			"Limit 4.00 root  offset:0, count:961",
			"└─IndexLookUp 4.00 root  ",
			"  ├─Selection(Build) 1024.00 cop[tikv]  le(test.t.id, 1024)",
			"  │ └─IndexRangeScan 1024.00 cop[tikv] table:t, index:i(c1) range:[0,1024], keep order:true",
			"  └─Selection(Probe) 4.00 cop[tikv]  in(test.t.c2, 2, 4, 6, 8)",
			"    └─TableRowIDScan 1024.00 cop[tikv] table:t keep order:false"))
	}

	// selection between limit and indexlookup, limit 961 should not go paging too
	for i := 0; i < 10; i++ {
		tk.MustQuery("explain format='brief' select * from t force index(i) where mod(id, 2) > 0 and id <= 1024 and c1 >= 0 and c1 <= 1024 and c2 in (2, 4, 6, 8) order by c1 limit 961").Check(testkit.Rows(
			"Limit 3.20 root  offset:0, count:961",
			"└─IndexLookUp 3.20 root  ",
			"  ├─Selection(Build) 819.20 cop[tikv]  gt(mod(test.t.id, 2), 0), le(test.t.id, 1024)",
			"  │ └─IndexRangeScan 1024.00 cop[tikv] table:t, index:i(c1) range:[0,1024], keep order:true",
			"  └─Selection(Probe) 3.20 cop[tikv]  in(test.t.c2, 2, 4, 6, 8)",
			"    └─TableRowIDScan 819.20 cop[tikv] table:t keep order:false"))
	}
}

func TestBuildFinalModeAggregation(t *testing.T) {
	aggSchemaBuilder := func(sctx base.PlanContext, aggFuncs []*aggregation.AggFuncDesc) *expression.Schema {
		schema := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncs))...)
		for _, agg := range aggFuncs {
			newCol := &expression.Column{
				UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  agg.RetTp,
			}
			schema.Append(newCol)
		}
		return schema
	}
	isFinalAggMode := func(mode aggregation.AggFunctionMode) bool {
		return mode == aggregation.FinalMode || mode == aggregation.CompleteMode
	}
	checkResult := func(sctx base.PlanContext, aggFuncs []*aggregation.AggFuncDesc, groubyItems []expression.Expression) {
		for partialIsCop := 0; partialIsCop < 2; partialIsCop++ {
			for isMPPTask := 0; isMPPTask < 2; isMPPTask++ {
				partial, final, _ := core.BuildFinalModeAggregation(sctx, &core.AggInfo{
					AggFuncs:     aggFuncs,
					GroupByItems: groubyItems,
					Schema:       aggSchemaBuilder(sctx, aggFuncs),
				}, partialIsCop == 0, isMPPTask == 0)
				if partial != nil {
					for _, aggFunc := range partial.AggFuncs {
						if partialIsCop == 0 {
							require.True(t, !isFinalAggMode(aggFunc.Mode))
						} else {
							require.True(t, isFinalAggMode(aggFunc.Mode))
						}
					}
				}
				if final != nil {
					for _, aggFunc := range final.AggFuncs {
						require.True(t, isFinalAggMode(aggFunc.Mode))
					}
				}
			}
		}
	}

	ctx := core.MockContext()
	defer func() {
		domain.GetDomain(ctx).StatsHandle().Close()
	}()
	aggCol := &expression.Column{
		Index:   0,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	gbyCol := &expression.Column{
		Index:   1,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}
	orderCol := &expression.Column{
		Index:   2,
		RetType: types.NewFieldType(mysql.TypeLonglong),
	}

	emptyGroupByItems := make([]expression.Expression, 0, 1)
	groupByItems := make([]expression.Expression, 0, 1)
	groupByItems = append(groupByItems, gbyCol)

	orderByItems := make([]*util.ByItems, 0, 1)
	orderByItems = append(orderByItems, &util.ByItems{
		Expr: orderCol,
		Desc: true,
	})

	aggFuncs := make([]*aggregation.AggFuncDesc, 0, 5)
	desc, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncMax, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncFirstRow, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncSum, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{aggCol}, false)
	require.NoError(t, err)
	aggFuncs = append(aggFuncs, desc)

	aggFuncsWithDistinct := make([]*aggregation.AggFuncDesc, 0, 2)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncAvg, []expression.Expression{aggCol}, true)
	require.NoError(t, err)
	aggFuncsWithDistinct = append(aggFuncsWithDistinct, desc)
	desc, err = aggregation.NewAggFuncDesc(ctx, ast.AggFuncCount, []expression.Expression{aggCol}, true)
	require.NoError(t, err)
	aggFuncsWithDistinct = append(aggFuncsWithDistinct, desc)

	groupConcatAggFuncs := make([]*aggregation.AggFuncDesc, 0, 4)
	groupConcatWithoutDistinctWithoutOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, false)
	require.NoError(t, err)
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithoutDistinctWithoutOrderBy)
	groupConcatWithoutDistinctWithOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, false)
	require.NoError(t, err)
	groupConcatWithoutDistinctWithOrderBy.OrderByItems = orderByItems
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithoutDistinctWithOrderBy)
	groupConcatWithDistinctWithoutOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, true)
	require.NoError(t, err)
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithDistinctWithoutOrderBy)
	groupConcatWithDistinctWithOrderBy, err := aggregation.NewAggFuncDesc(ctx, ast.AggFuncGroupConcat, []expression.Expression{aggCol, aggCol}, true)
	require.NoError(t, err)
	groupConcatWithDistinctWithOrderBy.OrderByItems = orderByItems
	groupConcatAggFuncs = append(groupConcatAggFuncs, groupConcatWithDistinctWithOrderBy)

	// case 1 agg without distinct
	checkResult(ctx, aggFuncs, emptyGroupByItems)
	checkResult(ctx, aggFuncs, groupByItems)

	// case 2 agg with distinct
	checkResult(ctx, aggFuncsWithDistinct, emptyGroupByItems)
	checkResult(ctx, aggFuncsWithDistinct, groupByItems)

	// case 3 mixed with distinct and without distinct
	mixedAggFuncs := make([]*aggregation.AggFuncDesc, 0, 10)
	mixedAggFuncs = append(mixedAggFuncs, aggFuncs...)
	mixedAggFuncs = append(mixedAggFuncs, aggFuncsWithDistinct...)
	checkResult(ctx, mixedAggFuncs, emptyGroupByItems)
	checkResult(ctx, mixedAggFuncs, groupByItems)

	// case 4 group concat
	for _, groupConcatAggFunc := range groupConcatAggFuncs {
		checkResult(ctx, []*aggregation.AggFuncDesc{groupConcatAggFunc}, emptyGroupByItems)
		checkResult(ctx, []*aggregation.AggFuncDesc{groupConcatAggFunc}, groupByItems)
	}
	checkResult(ctx, groupConcatAggFuncs, emptyGroupByItems)
	checkResult(ctx, groupConcatAggFuncs, groupByItems)

	// case 5 mixed group concat and other agg funcs
	for _, groupConcatAggFunc := range groupConcatAggFuncs {
		funcs := make([]*aggregation.AggFuncDesc, 0, 10)
		funcs = append(funcs, groupConcatAggFunc)
		funcs = append(funcs, aggFuncs...)
		checkResult(ctx, funcs, emptyGroupByItems)
		checkResult(ctx, funcs, groupByItems)
		funcs = append(funcs, aggFuncsWithDistinct...)
		checkResult(ctx, funcs, emptyGroupByItems)
		checkResult(ctx, funcs, groupByItems)
	}
	mixedAggFuncs = append(mixedAggFuncs, groupConcatAggFuncs...)
	checkResult(ctx, mixedAggFuncs, emptyGroupByItems)
	checkResult(ctx, mixedAggFuncs, groupByItems)
}

func TestCloneFineGrainedShuffleStreamCount(t *testing.T) {
	window := &core.PhysicalWindow{}
	newPlan, err := window.Clone()
	require.NoError(t, err)
	newWindow, ok := newPlan.(*core.PhysicalWindow)
	require.Equal(t, ok, true)
	require.Equal(t, window.TiFlashFineGrainedShuffleStreamCount, newWindow.TiFlashFineGrainedShuffleStreamCount)

	window.TiFlashFineGrainedShuffleStreamCount = 8
	newPlan, err = window.Clone()
	require.NoError(t, err)
	newWindow, ok = newPlan.(*core.PhysicalWindow)
	require.Equal(t, ok, true)
	require.Equal(t, window.TiFlashFineGrainedShuffleStreamCount, newWindow.TiFlashFineGrainedShuffleStreamCount)

	sort := &core.PhysicalSort{}
	newPlan, err = sort.Clone()
	require.NoError(t, err)
	newSort, ok := newPlan.(*core.PhysicalSort)
	require.Equal(t, ok, true)
	require.Equal(t, sort.TiFlashFineGrainedShuffleStreamCount, newSort.TiFlashFineGrainedShuffleStreamCount)

	sort.TiFlashFineGrainedShuffleStreamCount = 8
	newPlan, err = sort.Clone()
	require.NoError(t, err)
	newSort, ok = newPlan.(*core.PhysicalSort)
	require.Equal(t, ok, true)
	require.Equal(t, sort.TiFlashFineGrainedShuffleStreamCount, newSort.TiFlashFineGrainedShuffleStreamCount)
}

func TestImportIntoBuildPlan(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int);")
	tk.MustExec("create table t2 (a int, b int);")
	require.ErrorIs(t, tk.ExecToErr("IMPORT INTO t1 FROM select a from t2;"),
		plannererrors.ErrWrongValueCountOnRow)
	require.ErrorIs(t, tk.ExecToErr("IMPORT INTO t1(a) FROM select * from t2;"),
		plannererrors.ErrWrongValueCountOnRow)

	time.Sleep(100 * time.Millisecond)
	now := tk.MustQuery("select now(6)").Rows()[0][0].(string)
	time.Sleep(100 * time.Millisecond)
	tk.MustExec("create table t3 (a int, b int);")
	// set tidb_snapshot will fail without this
	tk.MustExec(`replace into mysql.tidb(variable_name, variable_value) values ('tikv_gc_safe_point', '20240131-00:00:00.000 +0800')`)
	tk.MustExec("set tidb_snapshot = '" + now + "'")
	require.ErrorContains(t, tk.ExecToErr("IMPORT INTO t1 FROM select * from t2"),
		"can not execute write statement when 'tidb_snapshot' is set")
	require.ErrorIs(t, tk.ExecToErr("IMPORT INTO t3 FROM select * from t2"),
		infoschema.ErrTableNotExists)
}
