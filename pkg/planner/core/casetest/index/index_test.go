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
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestIndexJoinUniqueCompositeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t1(a int not null, c int not null)")
	tk.MustExec("create table t2(a int not null, b int not null, c int not null, primary key(a,b))")
	tk.MustExec("insert into t1 values(1,1)")
	tk.MustExec("insert into t2 values(1,1,1),(1,2,1)")
	tk.MustExec("analyze table t1,t2")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIndexMerge(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, unique index(a), unique index(b), primary key(c))")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

// for issue #14822 and #38258
func TestIndexJoinTableRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, primary key (a), key idx_t1_b (b))")
	tk.MustExec("create table t2(a int, b int, primary key (a), key idx_t1_b (b))")
	tk.MustExec("create table t3(a int, b int, c int)")
	tk.MustExec("create table t4(a int, b int, c int, primary key (a, b) clustered)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIndexJoinInnerIndexNDV(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null, c int not null)")
	tk.MustExec("create table t2(a int not null, b int not null, c int not null, index idx1(a,b), index idx2(c))")
	tk.MustExec("insert into t1 values(1,1,1),(1,1,1),(1,1,1)")
	tk.MustExec("insert into t2 values(1,1,1),(1,1,2),(1,1,3)")
	tk.MustExec("analyze table t1, t2")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestIndexMergeSerial(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, unique key(a), unique key(b))")
	tk.MustExec("insert into t value (1, 5), (2, 4), (3, 3), (4, 2), (5, 1)")
	tk.MustExec("insert into t value (6, 0), (7, -1), (8, -2), (9, -3), (10, -4)")
	tk.MustExec("analyze table t")

	var input []string
	var output []struct {
		SQL      string
		Plan     []string
		Warnings []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warnings = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warnings...))
	}
}

func TestIndexJoinOnClusteredIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
	tk.MustExec(`insert into t values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)`)
	tk.MustExec("analyze table t")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain  format = 'brief'" + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestIndexMergeWithCorrelatedColumns(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(c1 int, c2 int, c3 int, primary key(c1), key(c2));")
	tk.MustExec("insert into t1 values(1, 1, 1);")
	tk.MustExec("insert into t1 values(2, 2, 2);")
	tk.MustExec("create table t2(c1 int, c2 int, c3 int);")
	tk.MustExec("insert into t2 values(1, 1, 1);")
	tk.MustExec("insert into t2 values(2, 2, 2);")

	tk.MustExec("drop table if exists tt1, tt2;")
	tk.MustExec("create table tt1  (c_int int, c_str varchar(40), c_datetime datetime, c_decimal decimal(12, 6), primary key(c_int), key(c_int), key(c_str), unique key(c_decimal), key(c_datetime));")
	tk.MustExec("create table tt2  like tt1 ;")
	tk.MustExec(`insert into tt1 (c_int, c_str, c_datetime, c_decimal) values (6, 'sharp payne', '2020-06-07 10:40:39', 6.117000) ,
			    (7, 'objective kare', '2020-02-05 18:47:26', 1.053000) ,
			    (8, 'thirsty pasteur', '2020-01-02 13:06:56', 2.506000) ,
			    (9, 'blissful wilbur', '2020-06-04 11:34:04', 9.144000) ,
			    (10, 'reverent mclean', '2020-02-12 07:36:26', 7.751000) ;`)
	tk.MustExec(`insert into tt2 (c_int, c_str, c_datetime, c_decimal) values (6, 'beautiful joliot', '2020-01-16 01:44:37', 5.627000) ,
			    (7, 'hopeful blackburn', '2020-05-23 21:44:20', 7.890000) ,
			    (8, 'ecstatic davinci', '2020-02-01 12:27:17', 5.648000) ,
			    (9, 'hopeful lewin', '2020-05-05 05:58:25', 7.288000) ,
			    (10, 'sharp jennings', '2020-01-28 04:35:03', 9.758000) ;`)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format=brief " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery("explain format=brief " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Res...))
	}
}

func TestIndexJoinRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c varchar(10), d varchar(10), index idx_a_b_c_d(a, b, c(2), d(2)))")
	tk.MustExec("create table t2(e int, f int, g varchar(10), h varchar(10))")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		setStmt := strings.HasPrefix(tt, "set")
		testdata.OnRecord(func() {
			output[i].SQL = tt
			if !setStmt {
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())
			}
		})
		if setStmt {
			tk.MustExec(tt)
		} else {
			tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	}
}

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

func TestHeuristicIndexSelection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, d int, e int, f int, g int, primary key (a), unique key c_d_e (c, d, e), unique key f (f), unique key f_g (f, g), key g (g))")
	tk.MustExec("create table t2(a int, b int, c int, d int, unique index idx_a (a), unique index idx_b_c (b, c), unique index idx_b_c_a_d (b, c, a, d))")
	tk.MustExec("create table t3(a bigint, b varchar(255), c bigint, primary key(a, b) clustered)")
	tk.MustExec("create table t4(a bigint, b varchar(255), c bigint, primary key(a, b) nonclustered)")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	var input []string
	var output []struct {
		SQL      string
		Plan     []string
		Warnings []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'verbose' " + tt).Rows())
			output[i].Warnings = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'verbose' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warnings...))
	}
}

func TestLimitIndexLookUpKeepOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, c int, d int, index idx(a,b,c));")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
	}
}

func TestAccessPathOnClusterIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b varchar(20), c decimal(40,10), d int, primary key(a,b), key(c))")
	tk.MustExec(`insert into t1 values (1,"111",1.1,11), (2,"222",2.2,12), (3,"333",3.3,13)`)
	tk.MustExec("analyze table t1")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	integrationSuiteData := GetIntegrationSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + tt).Rows())
			output[i].Res = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Sort().Rows())
		})
		tk.MustQuery("explain format='brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery(tt).Sort().Check(testkit.Rows(output[i].Res...))
	}
}
