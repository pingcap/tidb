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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
)

func TestNullConditionForPrefixIndex(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`CREATE TABLE t1 (
  id char(1) DEFAULT NULL,
  c1 varchar(255) DEFAULT NULL,
  c2 text DEFAULT NULL,
  KEY idx1 (c1),
  KEY idx2 (c1,c2(5))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)

		testKit.MustExec("create table t2(a int, b varchar(10), index idx(b(5)))")
		testKit.MustExec("create table t3(a int, b varchar(10), c int, primary key (a, b(5)) clustered)")
		testKit.MustExec("set tidb_opt_prefix_index_single_scan = 1")
		testKit.MustExec("insert into t1 values ('a', '0xfff', '111111'), ('b', '0xfff', '22    '), ('c', '0xfff', ''), ('d', '0xfff', null)")
		testKit.MustExec("insert into t2 values (1, 'aaaaaa'), (2, 'bb    '), (3, ''), (4, null)")
		testKit.MustExec("insert into t3 values (1, 'aaaaaa', 2), (1, 'bb    ', 3), (1, '', 4)")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format='brief' " + tt).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Sort().Rows())
			})
			testKit.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
		}

		// test plan cache
		testKit.MustExec(`set tidb_enable_prepared_plan_cache=1`)
		testKit.MustExec("set @@tidb_enable_collect_execution_info=0")
		testKit.MustExec("prepare stmt from 'select count(1) from t1 where c1 = ? and c2 is not null'")
		testKit.MustExec("set @a = '0xfff'")
		testKit.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
		testKit.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))

		testKit.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
		testKit.MustQuery("execute stmt using @a").Check(testkit.Rows("3"))
		tkProcess := testKit.Session().ShowProcess()
		ps := []*sessmgr.ProcessInfo{tkProcess}
		testKit.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		testKit.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Check(testkit.Rows(
			"StreamAgg_19 1.00 root  funcs:count(Column#8)->Column#6",
			"└─IndexReader_20 1.00 root  index:StreamAgg_11",
			"  └─StreamAgg_11 1.00 cop[tikv]  funcs:count(1)->Column#8",
			"    └─IndexRangeScan_18 99.90 cop[tikv] table:t1, index:idx2(c1, c2) range:[\"0xfff\" -inf,\"0xfff\" +inf], keep order:false, stats:pseudo"))
	})
}

func TestInvisibleIndex(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE t1 ( a INT, KEY( a ) INVISIBLE );")
		testKit.MustExec("INSERT INTO t1 VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);")
		testKit.MustQuery(`EXPLAIN SELECT a FROM t1;`).Check(
			testkit.Rows(
				`TableReader_6 10000.00 root  data:TableFullScan_5`,
				`└─TableFullScan_5 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo`))
		testKit.MustExec("set session tidb_opt_use_invisible_indexes=on;")
		testKit.MustQuery(`EXPLAIN SELECT a FROM t1;`).Check(
			testkit.Rows(
				`IndexReader_8 10000.00 root  index:IndexFullScan_7`,
				`└─IndexFullScan_7 10000.00 cop[tikv] table:t1, index:a(a) keep order:false, stats:pseudo`))
	})
}

func TestRangeDerivation(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`set @@tidb_opt_fix_control = "54337:ON"`)
		testKit.MustExec("create table t1 (a1 int, b1 int, c1 int, primary key pkx (a1,b1));")
		testKit.MustExec("create table t1char (a1 char(5), b1 char(5), c1 int, primary key pkx (a1,b1));")
		testKit.MustExec("create table t(a int, b int, c int, primary key(a,b));")
		testKit.MustExec("create table tuk (a int, b int, c int, unique key (a, b, c));")
		testKit.MustExec("set @@session.tidb_regard_null_as_point=false;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		indexRangeSuiteData := GetIndexRangeSuiteData()
		indexRangeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := testKit.MustQuery("explain format = 'brief' " + sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestRowFunctionMatchTheIndexRangeScan(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`set @@tidb_opt_fix_control = "54337:ON"`)
		testKit.MustExec(`CREATE TABLE t1 (k1 int , k2 int, k3 int, index pk1(k1, k2))`)
		testKit.MustExec(`create table t2 (k1 int, k2 int)`)
		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery("explain format='brief' " + tt).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Sort().Rows())
			})
			testKit.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Result...))
		}
	})
}

func TestRangeIntersection(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`set @@tidb_opt_fix_control = "54337:ON"`)
		testKit.MustExec("create table t1 (a1 int, b1 int, c1 int, key pkx (a1,b1));")

		testKit.MustExec("create table t_inlist_test(a1 int,b1 int,c1 varbinary(767) DEFAULT NULL, KEY twoColIndex (a1,b1));")
		testKit.MustExec("insert into t1 values (1,1,1);")
		testKit.MustExec("insert into t1 values (null,1,1);")
		testKit.MustExec("insert into t1 values (1,null,1);")
		testKit.MustExec("insert into t1 values (1,1,null);")
		testKit.MustExec("insert into t1 values (1,10,1);")
		testKit.MustExec("insert into t1 values (10,20,1);")
		testKit.MustExec("insert into t1 select a1+1,b1,c1+1 from t1;")
		testKit.MustExec("insert into t1 select a1,b1+1,c1+1 from t1;")
		testKit.MustExec("insert into t1 select a1-1,b1+1,c1+1 from t1;")
		testKit.MustExec("insert into t1 select a1+2,b1+2,c1+2 from t1;")
		testKit.MustExec("insert into t1 select a1+2,b1-2,c1+2 from t1;")
		testKit.MustExec("insert into t1 select a1+2,b1-1,c1+2 from t1;")
		testKit.MustExec("insert into t1 select null,b1,c1+1 from t1;")
		testKit.MustExec("insert into t1 select a1,null,c1+1 from t1;")

		testKit.MustExec("create table t11 (a1 int, b1 int, c1 int);")
		testKit.MustExec("insert into t11 select * from t1;")

		testKit.MustExec("CREATE TABLE `tablename` (`primary_key` varbinary(1024) NOT NULL,`secondary_key` varbinary(1024) NOT NULL,`timestamp` bigint(20) NOT NULL,`value` mediumblob DEFAULT NULL,PRIMARY KEY PKK (`primary_key`,`secondary_key`,`timestamp`));")

		testKit.MustExec("create table t(a int, b int, c int, key PKK(a,b,c));")
		testKit.MustExec("create table tt(a int, b int, c int, primary key PKK(a,b,c));")
		testKit.MustExec("insert into t select * from t1;")
		testKit.MustExec("insert into tt select * from t1 where a1 is not null and b1 is not null and c1 is not null;")
		testKit.MustExec("CREATE TABLE tnull (a INT, KEY PK(a));")
		testKit.MustExec("create table tkey_string(id1 CHAR(16) not null, id2 VARCHAR(16) not null, id3 BINARY(16) not null, id4 VARBINARY(16) not null, id5 BLOB not null, id6 TEXT not null, id7 ENUM('x-small', 'small', 'medium', 'large', 'x-large') not null, id8 SET ('a', 'b', 'c', 'd') not null, name varchar(16), primary key(id1, id2, id3, id4, id7, id8)) PARTITION BY KEY(id7) partitions 4;")
		testKit.MustExec("INSERT INTO tkey_string VALUES('huaian','huaian','huaian','huaian','huaian','huaian','x-small','a','linpin');")
		testKit.MustExec("INSERT INTO tkey_string VALUES('nanjing','nanjing','nanjing','nanjing','nanjing','nanjing','small','b','linpin');")
		testKit.MustExec("INSERT INTO tkey_string VALUES('zhenjiang','zhenjiang','zhenjiang','zhenjiang','zhenjiang','zhenjiang','medium','c','linpin');")
		testKit.MustExec("INSERT INTO tkey_string VALUES('suzhou','suzhou','suzhou','suzhou','suzhou','suzhou','large','d','linpin');")
		testKit.MustExec("INSERT INTO tkey_string VALUES('wuxi','wuxi','wuxi','wuxi','wuxi','wuxi','x-large','a','linpin');")
		testKit.MustExec("create table t_issue_60556(a int, b int, ac char(3), bc char(3), key ab(a,b), key acbc(ac,bc));")
		testKit.MustExec("insert into t_issue_60556 values (100, 500, '100', '500');")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		indexRangeSuiteData := GetIndexRangeSuiteData()
		indexRangeSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			plan := testKit.MustQuery("explain format = 'brief' " + sql)
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
				output[i].Result = testdata.ConvertRowsToStrings(testKit.MustQuery(sql).Sort().Rows())
			})
			plan.Check(testkit.Rows(output[i].Plan...))
			testKit.MustQuery(sql).Sort().Check(testkit.Rows(output[i].Result...))
		}
	})
}

func TestOrderedIndexWithIsNull(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE t1 (a int key, b int, c int, index (b, c));")
		testKit.MustQuery("explain select a from t1 where b is null order by c").Check(testkit.Rows(
			"Projection_6 10.00 root  test.t1.a",
			"└─IndexReader_14 10.00 root  index:IndexRangeScan_13",
			"  └─IndexRangeScan_13 10.00 cop[tikv] table:t1, index:b(b, c) range:[NULL,NULL], keep order:true, stats:pseudo",
		))
		// https://github.com/pingcap/tidb/issues/56116
		testKit.MustExec("create table t2(id bigint(20) DEFAULT NULL, UNIQUE KEY index_on_id (id))")
		testKit.MustExec("insert into t2 values (), (), ()")
		testKit.MustExec("analyze table t2")
		testKit.MustQuery("explain select count(*) from t2 where id is null;").Check(testkit.Rows(
			"StreamAgg_19 1.00 root  funcs:count(Column#6)->Column#4",
			"└─IndexReader_20 1.00 root  index:StreamAgg_11",
			"  └─StreamAgg_11 1.00 cop[tikv]  funcs:count(1)->Column#6",
			"    └─IndexRangeScan_18 3.00 cop[tikv] table:t2, index:index_on_id(id) range:[NULL,NULL], keep order:false"))
	})
}

func TestVectorIndex(t *testing.T) {
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

		testKit.MustExec("create table t (a int, b vector, c vector(3), d vector(4));")
		testKit.MustExec("alter table t set tiflash replica 1;")
		testKit.MustExec("alter table t add vector index vecIdx1((vec_cosine_distance(d))) USING HNSW;")
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		testKit.MustUseIndex("select * from t use index(vecIdx1) order by vec_cosine_distance(d, '[1,1,1,1]') limit 1", "vecIdx1")
		testKit.MustUseIndex("select * from t use index(vecIdx1) order by vec_cosine_distance('[1,1,1,1]', d) limit 1", "vecIdx1")
		testKit.MustExecToErr("select * from t use index(vecIdx1) order by vec_l2_distance(d, '[1,1,1,1]') limit 1")
		testKit.MustExecToErr("select * from t use index(vecIdx1) where a = 5 order by vec_cosine_distance(d, '[1,1,1,1]') limit 1")
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

		testKit.MustExec("create table t (a int, b bigint, c tinyint, d smallint unsigned, columnar index idx_a (a) using inverted, columnar index idx_b (b) using inverted);")
		testKit.MustExec("alter table t add columnar index idx_c (c) USING inverted;")
		testKit.MustExec("alter table t add columnar index idx_d (d) USING inverted;")
		testKit.MustExec("insert into t values (1, 1, 1, 1), (2, 2, 2, 2), (3, 3, 3, 3), (4, 4, 4, 4);")
		testkit.SetTiFlashReplica(t, dom, "test", "t")

		testKit.MustUseIndex("select * from t force index(idx_a) where a > 0", "idx_a")
		testKit.MustUseIndex("select * from t force index(idx_b) where b < 0", "idx_b")
		testKit.MustUseIndex("select * from t force index(idx_c) where c = 0", "idx_c")
		testKit.MustUseIndex("select * from t force index(idx_d) where d != 0", "idx_d")
		testKit.MustNoIndexUsed("select * from t ignore index(idx_a) where a = 1")
		testKit.MustNoIndexUsed("select * from t ignore index(idx_b) where b = 2")
		testKit.MustNoIndexUsed("select * from t ignore index(idx_c) where c = 3")
		testKit.MustNoIndexUsed("select * from t ignore index(idx_d) where d < 1")
	})
}

func TestAnalyzeColumnarIndex(t *testing.T) {
	testkit.RunTestUnderCascadesAndDomainWithSchemaLease(t, 200*time.Millisecond, []mockstore.MockTiKVStoreOption{mockstore.WithMockTiFlash(2)}, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t;")

		tiflash := infosync.NewMockTiFlash()
		infosync.SetMockTiFlash(tiflash)
		defer func() {
			tiflash.Lock()
			tiflash.StatusServer.Close()
			tiflash.Unlock()
		}()
		testKit.MustExec(`create table t(a int, b vector(2), c datetime, j json, index(a))`)
		testKit.MustExec("insert into t values(1, '[1, 0]', '2022-01-01 12:00:00', '{\"a\": 1}')")
		testKit.MustExec("alter table t set tiflash replica 2 location labels 'a','b';")
		tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tblInfo := tbl.Meta()
		err = domain.GetDomain(testKit.Session()).DDLExecutor().UpdateTableReplicaInfo(testKit.Session(), tblInfo.ID, true)
		require.NoError(t, err)
		testkit.SetTiFlashReplica(t, dom, "test", "t")

		testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/MockCheckColumnarIndexProcess", `return(1)`)
		testKit.MustExec("alter table t add vector index idx((VEC_COSINE_DISTANCE(b))) USING HNSW")
		testKit.MustExec("alter table t add columnar index idx2(c) USING INVERTED")

		testKit.MustUseIndex("select * from t use index(idx) order by vec_cosine_distance(b, '[1, 0]') limit 1", "idx")
		testKit.MustUseIndex("select * from t order by vec_cosine_distance(b, '[1, 0]') limit 1", "idx")
		testKit.MustNoIndexUsed("select * from t ignore index(idx) order by vec_cosine_distance(b, '[1, 0]') limit 1")

		testKit.MustExec("set tidb_analyze_version=2")
		testKit.MustExec("analyze table t")
		testKit.MustQuery("show warnings").Sort().Check(testkit.Rows(
			"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
			"Warning 1105 analyzing columnar index is not supported, skip idx",
			"Warning 1105 analyzing columnar index is not supported, skip idx2"))
		testKit.MustExec("analyze table t index idx")
		testKit.MustQuery("show warnings").Sort().Check(testkit.Rows(
			"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/1) as the sample-rate=1\"",
			"Warning 1105 The version 2 would collect all statistics not only the selected indexes",
			"Warning 1105 analyzing columnar index is not supported, skip idx",
			"Warning 1105 analyzing columnar index is not supported, skip idx2"))

		statsHandle := dom.StatsHandle()
		statsTbl := statsHandle.GetPhysicalTableStats(tblInfo.ID, tblInfo)
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

		testKit.MustExec("set tidb_analyze_version=1")
		testKit.MustExec("analyze table t")
		testKit.MustQuery("show warnings").Sort().Check(testkit.Rows(
			"Warning 1105 analyzing columnar index is not supported, skip idx",
			"Warning 1105 analyzing columnar index is not supported, skip idx2"))
		testKit.MustExec("analyze table t index idx")
		testKit.MustQuery("show warnings").Sort().Check(testkit.Rows(
			"Warning 1105 analyzing columnar index is not supported, skip idx"))
		testKit.MustExec("analyze table t index a")
		testKit.MustQuery("show warnings").Sort().Check(testkit.Rows())
		testKit.MustExec("analyze table t index a, idx, idx2")
		testKit.MustQuery("show warnings").Sort().Check(testkit.Rows(
			"Warning 1105 analyzing columnar index is not supported, skip idx",
			"Warning 1105 analyzing columnar index is not supported, skip idx2"))
	})
}

func TestPartialIndexWithPlanCache(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, index idx1(a) where a is not null, index idx2(b) where b > 10)")

		tk.MustExec("prepare stmt from 'select * from t where a = ?'")
		tk.MustExec("set @a = 123")

		// IS NOT NULL pre condition can use plan cache.
		tk.MustExec("execute stmt using @a")
		tk.MustExec("execute stmt using @a")
		tkProcess := tk.Session().ShowProcess()
		ps := []*sessmgr.ProcessInfo{tkProcess}
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckContain("idx1")
		tk.MustExec("execute stmt using @a")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		// Normal pre condition can not use plan cache.
		tk.MustExec("prepare stmt from 'select * from t where b = ?'")
		tk.MustExec("set @a = 20")
		tk.MustExec("execute stmt using @a")
		tk.MustExec("execute stmt using @a")
		tkProcess = tk.Session().ShowProcess()
		ps[0] = tkProcess
		tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
		tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckContain("idx2")
		tk.MustExec("execute stmt using @a")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	})
}

func TestPartialIndexWithIndexPrune(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t(a int, b int, index idx1(a) where a is not null, index idx2(b) where b > 10)")
		tk.MustQuery("explain select * from t use index(idx1) where a > 1").CheckContain("idx1")

		// Set the prune behavior to prune all non interesting ones.
		tk.MustExec("set @@tidb_opt_index_prune_threshold=0")

		// The failpoint will check whether all partial indexes are pruned.
		fpName := "github.com/pingcap/tidb/pkg/planner/core/rule/InjectCheckForIndexPrune"
		require.NoError(t, failpoint.EnableCall(fpName, func(paths []*util.AccessPath) {
			for _, path := range paths {
				if path != nil && path.Index != nil && path.Index.ConditionExprString != "" {
					require.True(t, false, "Partial index should be pruned")
				}
			}
		}))
		tk.MustQuery("select * from t")

		// idx1 is pruned because a is not referenced as interesting one.
		// idx2 is kept though its constraint is not matched.
		require.NoError(t, failpoint.EnableCall(fpName, func(paths []*util.AccessPath) {
			idx2Found := false
			for _, path := range paths {
				if path != nil && path.Index != nil && path.Index.Name.L == "idx1" {
					require.True(t, false, "Partial index idx1 should be pruned")
				}
				if path != nil && path.Index != nil && path.Index.Name.L == "idx2" {
					idx2Found = true
				}
			}
			require.True(t, idx2Found, "Partial index idx2 should not be pruned")
		}))
		tk.MustQuery("explain select * from t order by b").CheckNotContain("idx2")

		// idx2 is pruned because b is not referenced as interesting one.
		// idx1 is kept though its constraint is not matched.
		require.NoError(t, failpoint.EnableCall(fpName, func(paths []*util.AccessPath) {
			idx1Found := false
			for _, path := range paths {
				if path != nil && path.Index != nil && path.Index.Name.L == "idx2" {
					require.True(t, false, "Partial index idx2 should be pruned")
				}
				if path != nil && path.Index != nil && path.Index.Name.L == "idx1" {
					idx1Found = true
				}
			}
			require.True(t, idx1Found, "Partial index idx1 should not be pruned")
		}))
		tk.MustQuery("explain select * from t where a is null").CheckNotContain("idx1")
		require.NoError(t, failpoint.Disable(fpName))
	})
}
