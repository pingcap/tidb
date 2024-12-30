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

package index

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNullConditionForPrefixIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  id char(1) DEFAULT NULL,
  c1 varchar(255) DEFAULT NULL,
  c2 text DEFAULT NULL,
  KEY idx1 (c1),
  KEY idx2 (c1,c2(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t2(a int, b varchar(10), index idx(b(5)))")
	tk.MustExec("create table t3(a int, b varchar(10), c int, primary key (a, b(5)) clustered)")
	tk.MustExec("set tidb_opt_prefix_index_single_scan = 1")
	tk.MustExec("insert into t1 values ('a', '0xfff', '111111'), ('b', '0xfff', '22    '), ('c', '0xfff', ''), ('d', '0xfff', null)")
	tk.MustExec("insert into t2 values (1, 'aaaaaa'), (2, 'bb    '), (3, ''), (4, null)")
	tk.MustExec("insert into t3 values (1, 'aaaaaa', 2), (1, 'bb    ', 3), (1, '', 4)")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
	}

	// test plan cache
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec("prepare stmt from 'select count(1) from t1 where c1 = ? and c2 is not null'")
	tk.MustExec("set @a = '0xfff'")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows(
		"StreamAgg_17 1.00 root  funcs:count(Column#7)->Column#5",
		"└─IndexReader_18 1.00 root  index:StreamAgg_9",
		"  └─StreamAgg_9 1.00 cop[tikv]  funcs:count(1)->Column#7",
		"    └─IndexRangeScan_16 99.90 cop[tikv] table:t1, index:idx2(c1, c2) range:[\"0xfff\" -inf,\"0xfff\" +inf], keep order:false, stats:pseudo"))
}

func TestInvisibleIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 ( a INT, KEY( a ) INVISIBLE );")
	tk.MustExec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);")
	tk.MustQuery(`EXPLAIN SELECT a FROM t1;`).Check(
		testkit.Rows(
			`TableReader_5 10000.00 root  data:TableFullScan_4`,
			`└─TableFullScan_4 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo`))
	tk.MustExec("set session tidb_opt_use_invisible_indexes=on;")
	tk.MustQuery(`EXPLAIN SELECT a FROM t1;`).Check(
		testkit.Rows(
			`IndexReader_7 10000.00 root  index:IndexFullScan_6`,
			`└─IndexFullScan_6 10000.00 cop[tikv] table:t1, index:a(a) keep order:false, stats:pseudo`))
}

func TestRangeDerivation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_fix_control = "54337:ON"`)
	tk.MustExec("create table t1 (a1 int, b1 int, c1 int, primary key pkx (a1,b1));")
	tk.MustExec("create table t1char (a1 char(5), b1 char(5), c1 int, primary key pkx (a1,b1));")
	tk.MustExec("create table t(a int, b int, c int, primary key(a,b));")
	tk.MustExec("create table tuk (a int, b int, c int, unique key (a, b, c));")
	tk.MustExec("set @@session.tidb_regard_null_as_point=false;")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	indexRangeSuiteData := GetIndexRangeSuiteData()
	indexRangeSuiteData.LoadTestCases(t, &input, &output)
	indexRangeSuiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

func TestRowFunctionMatchTheIndexRangeScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_fix_control = "54337:ON"`)
	tk.MustExec(`CREATE TABLE t1 (k1 int , k2 int, k3 int, index pk1(k1, k2))`)
	tk.MustExec(`create table t2 (k1 int, k2 int)`)
	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + tt).Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestRangeIntersection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_fix_control = "54337:ON"`)
	tk.MustExec("create table t1 (a1 int, b1 int, c1 int, key pkx (a1,b1));")

	tk.MustExec("create table t_inlist_test(a1 int,b1 int,c1 varbinary(767) DEFAULT NULL, KEY twoColIndex (a1,b1));")
	tk.MustExec("insert into t1 values (1,1,1);")
	tk.MustExec("insert into t1 values (null,1,1);")
	tk.MustExec("insert into t1 values (1,null,1);")
	tk.MustExec("insert into t1 values (1,1,null);")
	tk.MustExec("insert into t1 values (1,10,1);")
	tk.MustExec("insert into t1 values (10,20,1);")
	tk.MustExec("insert into t1 select a1+1,b1,c1+1 from t1;")
	tk.MustExec("insert into t1 select a1,b1+1,c1+1 from t1;")
	tk.MustExec("insert into t1 select a1-1,b1+1,c1+1 from t1;")
	tk.MustExec("insert into t1 select a1+2,b1+2,c1+2 from t1;")
	tk.MustExec("insert into t1 select a1+2,b1-2,c1+2 from t1;")
	tk.MustExec("insert into t1 select a1+2,b1-1,c1+2 from t1;")
	tk.MustExec("insert into t1 select null,b1,c1+1 from t1;")
	tk.MustExec("insert into t1 select a1,null,c1+1 from t1;")

	tk.MustExec("create table t11 (a1 int, b1 int, c1 int);")
	tk.MustExec("insert into t11 select * from t1;")

	tk.MustExec("CREATE TABLE `tablename` (`primary_key` varbinary(1024) NOT NULL,`secondary_key` varbinary(1024) NOT NULL,`timestamp` bigint(20) NOT NULL,`value` mediumblob DEFAULT NULL,PRIMARY KEY PKK (`primary_key`,`secondary_key`,`timestamp`));")

	tk.MustExec("create table t(a int, b int, c int, key PKK(a,b,c));")
	tk.MustExec("create table tt(a int, b int, c int, primary key PKK(a,b,c));")
	tk.MustExec("insert into t select * from t1;")
	tk.MustExec("insert into tt select * from t1 where a1 is not null and b1 is not null and c1 is not null;")
	tk.MustExec("CREATE TABLE tnull (a INT, KEY PK(a));")
	tk.MustExec("create table tkey_string(id1 CHAR(16) not null, id2 VARCHAR(16) not null, id3 BINARY(16) not null, id4 VARBINARY(16) not null, id5 BLOB not null, id6 TEXT not null, id7 ENUM('x-small', 'small', 'medium', 'large', 'x-large') not null, id8 SET ('a', 'b', 'c', 'd') not null, name varchar(16), primary key(id1, id2, id3, id4, id7, id8)) PARTITION BY KEY(id7) partitions 4;")
	tk.MustExec("INSERT INTO tkey_string VALUES('huaian','huaian','huaian','huaian','huaian','huaian','x-small','a','linpin');")
	tk.MustExec("INSERT INTO tkey_string VALUES('nanjing','nanjing','nanjing','nanjing','nanjing','nanjing','small','b','linpin');")
	tk.MustExec("INSERT INTO tkey_string VALUES('zhenjiang','zhenjiang','zhenjiang','zhenjiang','zhenjiang','zhenjiang','medium','c','linpin');")
	tk.MustExec("INSERT INTO tkey_string VALUES('suzhou','suzhou','suzhou','suzhou','suzhou','suzhou','large','d','linpin');")
	tk.MustExec("INSERT INTO tkey_string VALUES('wuxi','wuxi','wuxi','wuxi','wuxi','wuxi','x-large','a','linpin');")

	var input []string
	var output []struct {
		SQL    string
		Plan   []string
		Result []string
	}
	indexRangeSuiteData := GetIndexRangeSuiteData()
	indexRangeSuiteData.LoadTestCases(t, &input, &output)
	indexRangeSuiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Sort().Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(sql).Sort().Check(testkit.Rows(output[i].Result...))
	}
}

func TestOrderedIndexWithIsNull(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 (a int key, b int, c int, index (b, c));")
	tk.MustQuery("explain select a from t1 where b is null order by c").Check(testkit.Rows(
		"Projection_6 10.00 root  test.t1.a",
		"└─IndexReader_12 10.00 root  index:IndexRangeScan_11",
		"  └─IndexRangeScan_11 10.00 cop[tikv] table:t1, index:b(b, c) range:[NULL,NULL], keep order:true, stats:pseudo",
	))
	// https://github.com/pingcap/tidb/issues/56116
	tk.MustExec("create table t2(id bigint(20) DEFAULT NULL, UNIQUE KEY index_on_id (id))")
	tk.MustExec("insert into t2 values (), (), ()")
	tk.MustExec("analyze table t2")
	tk.MustQuery("explain select count(*) from t2 where id is null;").Check(testkit.Rows(
		"StreamAgg_17 1.00 root  funcs:count(Column#5)->Column#3",
		"└─IndexReader_18 1.00 root  index:StreamAgg_9",
		"  └─StreamAgg_9 1.00 cop[tikv]  funcs:count(1)->Column#5",
		"    └─IndexRangeScan_16 3.00 cop[tikv] table:t2, index:index_on_id(id) range:[NULL,NULL], keep order:false"))
}

const tiflashReplicaLease = 600 * time.Millisecond

func TestVectorIndex(t *testing.T) {
	store := testkit.CreateMockStoreWithSchemaLease(t, tiflashReplicaLease, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckVectorIndexProcess", `return(1)`)

	tk.MustExec("create table t (a int, b vector, c vector(3), d vector(4));")
	tk.MustExec("alter table t set tiflash replica 1;")
	tk.MustExec("alter table t add vector index vecIdx1((vec_cosine_distance(d))) USING HNSW;")
	dom := domain.GetDomain(tk.Session())
	testkit.SetTiFlashReplica(t, dom, "test", "t")
	tk.MustUseIndex("select * from t use index(vecIdx1) order by vec_cosine_distance(d, '[1,1,1,1]') limit 1", "vecIdx1")
	tk.MustUseIndex("select * from t use index(vecIdx1) order by vec_cosine_distance('[1,1,1,1]', d) limit 1", "vecIdx1")
	tk.MustExecToErr("select * from t use index(vecIdx1) order by vec_l2_distance(d, '[1,1,1,1]') limit 1")
	tk.MustExecToErr("select * from t use index(vecIdx1) where a = 5 order by vec_cosine_distance(d, '[1,1,1,1]') limit 1")
}

func TestAnalyzeVectorIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomainWithSchemaLease(t, 200*time.Millisecond, mockstore.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	tiflash := infosync.NewMockTiFlash()
	infosync.SetMockTiFlash(tiflash)
	defer func() {
		tiflash.Lock()
		tiflash.StatusServer.Close()
		tiflash.Unlock()
	}()
	tk.MustExec(`create table t(a int, b vector(2), c vector(3), j json, index(a))`)
	tk.MustExec("insert into t values(1, '[1, 0]', '[1, 0, 0]', '{\"a\": 1}')")
	tk.MustExec("alter table t set tiflash replica 2 location labels 'a','b';")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	err = domain.GetDomain(tk.Session()).DDLExecutor().UpdateTableReplicaInfo(tk.Session(), tblInfo.ID, true)
	require.NoError(t, err)
	testkit.SetTiFlashReplica(t, dom, "test", "t")

	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckVectorIndexProcess", `return(1)`)
	tk.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING HNSW")
	tk.MustExec("alter table t add vector index idx2((VEC_COSINE_DISTANCE(c))) USING HNSW")

	tk.MustUseIndex("select * from t use index(idx) order by vec_cosine_distance(b, '[1, 0]') limit 1", "idx")
	tk.MustUseIndex("select * from t order by vec_cosine_distance(b, '[1, 0]') limit 1", "idx")
	tk.MustNoIndexUsed("select * from t ignore index(idx) order by vec_cosine_distance(b, '[1, 0]') limit 1")

	tk.MustExec("set tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Warning 1105 No predicate column has been collected yet for table test.t, so only indexes and the columns composing the indexes will be analyzed",
		"Warning 1105 analyzing vector index is not supported, skip idx",
		"Warning 1105 analyzing vector index is not supported, skip idx2"))
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/1) as the sample-rate=1\"",
		"Warning 1105 No predicate column has been collected yet for table test.t, so only indexes and the columns composing the indexes will be analyzed",
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes",
		"Warning 1105 analyzing vector index is not supported, skip idx",
		"Warning 1105 analyzing vector index is not supported, skip idx2"))

	statsHandle := dom.StatsHandle()
	statsTbl := statsHandle.GetTableStats(tblInfo)
	require.True(t, statsTbl.LastAnalyzeVersion > 0)
	// int col
	col := statsTbl.GetCol(1)
	require.NotNil(t, col)
	// It has stats.
	require.True(t, (col.Histogram.Len()+col.TopN.Num()) > 0)
	// vec col
	col = statsTbl.GetCol(2)
	require.NotNil(t, col)
	// It doesn't have stats.
	require.False(t, (col.Histogram.Len()+col.TopN.Num()) > 0)

	tk.MustExec("set tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1105 analyzing vector index is not supported, skip idx",
		"Warning 1105 analyzing vector index is not supported, skip idx2"))
	tk.MustExec("analyze table t index idx")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1105 analyzing vector index is not supported, skip idx"))
	tk.MustExec("analyze table t index a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows())
	tk.MustExec("analyze table t index a, idx, idx2")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1105 analyzing vector index is not supported, skip idx",
		"Warning 1105 analyzing vector index is not supported, skip idx2"))
}
