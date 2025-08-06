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

package mpp

import (
	"context"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestMPPJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists d1_t")
		testKit.MustExec("create table d1_t(d1_k int, value int)")
		testKit.MustExec("insert into d1_t values(1,2),(2,3)")
		testKit.MustExec("analyze table d1_t all columns")
		testKit.MustExec("drop table if exists d2_t")
		testKit.MustExec("create table d2_t(d2_k decimal(10,2), value int)")
		testKit.MustExec("insert into d2_t values(10.11,2),(10.12,3)")
		testKit.MustExec("analyze table d2_t all columns")
		testKit.MustExec("drop table if exists d3_t")
		testKit.MustExec("create table d3_t(d3_k date, value int)")
		testKit.MustExec("insert into d3_t values(date'2010-01-01',2),(date'2010-01-02',3)")
		testKit.MustExec("analyze table d3_t all columns")
		testKit.MustExec("drop table if exists fact_t")
		testKit.MustExec("create table fact_t(d1_k int, d2_k decimal(10,2), d3_k date, col1 int, col2 int, col3 int)")
		testKit.MustExec("insert into fact_t values(1,10.11,date'2010-01-01',1,2,3),(1,10.11,date'2010-01-02',1,2,3),(1,10.12,date'2010-01-01',1,2,3),(1,10.12,date'2010-01-02',1,2,3)")
		testKit.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
		testKit.MustExec("analyze table fact_t all columns")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "fact_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d1_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d2_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d3_t")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 1")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPLeftSemiJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		// test table
		testKit.MustExec("use test")
		testKit.MustExec("create table test.t(a int not null, b int null);")
		testKit.MustExec("set tidb_allow_mpp=1; set tidb_enforce_mpp=1;")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
			})
			if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
				testKit.MustExec(tt)
				continue
			}
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
				output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
			require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
		}
	})
}

func TestMPPOuterJoinBuildSideForBroadcastJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists a")
		testKit.MustExec("create table a(id int, value int)")
		testKit.MustExec("insert into a values(1,2),(2,3)")
		testKit.MustExec("analyze table a all columns")
		testKit.MustExec("drop table if exists b")
		testKit.MustExec("create table b(id int, value int)")
		testKit.MustExec("insert into b values(1,2),(2,3),(3,4)")
		testKit.MustExec("analyze table b all columns")
		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "a")
		testkit.SetTiFlashReplica(t, dom, "test", "b")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 0")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 10000")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 10000")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPOuterJoinBuildSideForShuffleJoinWithFixedBuildSide(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists a")
		testKit.MustExec("create table a(id int, value int, index idx(id, value))")
		testKit.MustExec("insert into a values(1,2),(2,3)")
		testKit.MustExec("analyze table a")
		testKit.MustExec("drop table if exists b")
		testKit.MustExec("create table b(id int, value int, index idx(id, value))")
		testKit.MustExec("insert into b values(1,2),(2,3),(3,4)")
		testKit.MustExec("analyze table b")
		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "a")
		testkit.SetTiFlashReplica(t, dom, "test", "b")
		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPOuterJoinBuildSideForShuffleJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists a")
		testKit.MustExec("create table a(id int, value int)")
		testKit.MustExec("insert into a values(1,2),(2,3)")
		testKit.MustExec("analyze table a all columns")
		testKit.MustExec("drop table if exists b")
		testKit.MustExec("create table b(id int, value int)")
		testKit.MustExec("insert into b values(1,2),(2,3),(3,4)")
		testKit.MustExec("analyze table b all columns")
		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "a")
		testkit.SetTiFlashReplica(t, dom, "test", "b")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 0")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPShuffledJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists d1_t")
		testKit.MustExec("create table d1_t(d1_k int, value int)")
		testKit.MustExec("insert into d1_t values(1,2),(2,3)")
		testKit.MustExec("insert into d1_t values(1,2),(2,3)")
		testKit.MustExec("analyze table d1_t all columns")
		testKit.MustExec("drop table if exists d2_t")
		testKit.MustExec("create table d2_t(d2_k decimal(10,2), value int)")
		testKit.MustExec("insert into d2_t values(10.11,2),(10.12,3)")
		testKit.MustExec("insert into d2_t values(10.11,2),(10.12,3)")
		testKit.MustExec("analyze table d2_t all columns")
		testKit.MustExec("drop table if exists d3_t")
		testKit.MustExec("create table d3_t(d3_k date, value int)")
		testKit.MustExec("insert into d3_t values(date'2010-01-01',2),(date'2010-01-02',3)")
		testKit.MustExec("insert into d3_t values(date'2010-01-01',2),(date'2010-01-02',3)")
		testKit.MustExec("analyze table d3_t all columns")
		testKit.MustExec("drop table if exists fact_t")
		testKit.MustExec("create table fact_t(d1_k int, d2_k decimal(10,2), d3_k date, col1 int, col2 int, col3 int)")
		testKit.MustExec("insert into fact_t values(1,10.11,date'2010-01-01',1,2,3),(1,10.11,date'2010-01-02',1,2,3),(1,10.12,date'2010-01-01',1,2,3),(1,10.12,date'2010-01-02',1,2,3)")
		testKit.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
		testKit.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
		testKit.MustExec("insert into fact_t values(2,10.11,date'2010-01-01',1,2,3),(2,10.11,date'2010-01-02',1,2,3),(2,10.12,date'2010-01-01',1,2,3),(2,10.12,date'2010-01-02',1,2,3)")
		testKit.MustExec("analyze table fact_t all columns")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "fact_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d1_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d2_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d3_t")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPJoinWithCanNotFoundColumnInSchemaColumnsError(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t1")
		testKit.MustExec("create table t1(id int, v1 decimal(20,2), v2 decimal(20,2))")
		testKit.MustExec("create table t2(id int, v1 decimal(10,2), v2 decimal(10,2))")
		testKit.MustExec("create table t3(id int, v1 decimal(10,2), v2 decimal(10,2))")
		testKit.MustExec("insert into t1 values(1,1,1),(2,2,2)")
		testKit.MustExec("insert into t2 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8)")
		testKit.MustExec("insert into t3 values(1,1,1)")
		testKit.MustExec("analyze table t1 all columns")
		testKit.MustExec("analyze table t2 all columns")
		testKit.MustExec("analyze table t3 all columns")

		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testkit.SetTiFlashReplica(t, dom, "test", "t2")
		testkit.SetTiFlashReplica(t, dom, "test", "t3")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_enforce_mpp = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		testKit.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 0")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPWithHashExchangeUnderNewCollation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists table_1")
		testKit.MustExec("create table table_1(id int not null, value char(10), index idx(id, value)) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;")
		testKit.MustExec("insert into table_1 values(1,'1'),(2,'2')")
		testKit.MustExec("drop table if exists table_2")
		testKit.MustExec("create table table_2(id int not null, value char(10), index idx(id, value)) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;")
		testKit.MustExec("insert into table_2 values(1,'1'),(2,'2')")
		testKit.MustExec("analyze table table_1")
		testKit.MustExec("analyze table table_2")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "table_1")
		testkit.SetTiFlashReplica(t, dom, "test", "table_2")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		testKit.MustExec("set @@session.tidb_hash_exchange_with_new_collation = 1")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPWithBroadcastExchangeUnderNewCollation(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists table_1")
		testKit.MustExec("create table table_1(id int not null, value char(10), index idx(id, value))")
		testKit.MustExec("insert into table_1 values(1,'1'),(2,'2')")
		testKit.MustExec("analyze table table_1")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "table_1")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 1")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMPPAvgRewrite(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists table_1")
		testKit.MustExec("create table table_1(id int not null, value decimal(10,2), index idx(id, value))")
		testKit.MustExec("insert into table_1 values(1,1),(2,2)")
		testKit.MustExec("analyze table table_1")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "table_1")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 1")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMppUnionAll(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("drop table if exists t1")
		testKit.MustExec("create table t (a int not null, b int, c varchar(20))")
		testKit.MustExec("create table t1 (a int, b int not null, c double)")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		testkit.SetTiFlashReplica(t, dom, "test", "t1")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMppJoinDecimal(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("drop table if exists tt")
		testKit.MustExec("create table t (c1 decimal(8, 5), c2 decimal(9, 5), c3 decimal(9, 4) NOT NULL, c4 decimal(8, 4) NOT NULL, c5 decimal(40, 20))")
		testKit.MustExec("create table tt (pk int(11) NOT NULL AUTO_INCREMENT primary key,col_varchar_64 varchar(64),col_char_64_not_null char(64) NOT null, col_decimal_30_10_key decimal(30,10), col_tinyint tinyint, col_varchar_key varchar(1), key col_decimal_30_10_key (col_decimal_30_10_key), key col_varchar_key(col_varchar_key));")
		testKit.MustExec("analyze table t")
		testKit.MustExec("analyze table tt")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		testkit.SetTiFlashReplica(t, dom, "test", "tt")

		testKit.MustExec("set @@tidb_allow_mpp=1;")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMppJoinExchangeColumnPrune(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("drop table if exists tt")
		testKit.MustExec("create table t (c1 int, c2 int, c3 int NOT NULL, c4 int NOT NULL, c5 int)")
		testKit.MustExec("create table tt (b1 int)")
		testKit.MustExec("analyze table t")
		testKit.MustExec("analyze table tt")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		testkit.SetTiFlashReplica(t, dom, "test", "tt")

		testKit.MustExec("set @@tidb_allow_mpp=1;")
		testKit.MustExec("set @@tidb_enforce_mpp=1;")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMppFineGrainedJoinAndAgg(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("drop table if exists tt")
		testKit.MustExec("create table t (c1 int, c2 int, c3 int NOT NULL, c4 int NOT NULL, c5 int)")
		testKit.MustExec("create table tt (b1 int)")
		testKit.MustExec("analyze table t")
		testKit.MustExec("analyze table tt")

		instances := []string{
			"tiflash,127.0.0.1:3933,127.0.0.1:7777,,",
			"tikv,127.0.0.1:11080,127.0.0.1:10080,,",
		}
		fpName := "github.com/pingcap/tidb/pkg/infoschema/mockStoreServerInfo"
		fpExpr := `return("` + strings.Join(instances, ";") + `")`
		require.NoError(t, failpoint.Enable(fpName, fpExpr))
		defer func() { require.NoError(t, failpoint.Disable(fpName)) }()
		fpName2 := "github.com/pingcap/tidb/pkg/planner/core/mockTiFlashStreamCountUsingMinLogicalCores"
		require.NoError(t, failpoint.Enable(fpName2, `return("8")`))
		defer func() { require.NoError(t, failpoint.Disable(fpName2)) }()

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")
		testkit.SetTiFlashReplica(t, dom, "test", "tt")

		testKit.MustExec("set @@tidb_allow_mpp=1;")
		testKit.MustExec("set @@tidb_enforce_mpp=1;")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMppAggTopNWithJoin(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")

		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, value decimal(6,3))")
		testKit.MustExec("analyze table t")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec(" set @@tidb_allow_mpp=1;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestRejectSortForMPP(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, value decimal(6,3), name char(128))")
		testKit.MustExec("analyze table t")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownSelectionForMPP(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, value decimal(6,3), name char(128))")
		testKit.MustExec("analyze table t")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
		testKit.MustExec("set @@tidb_isolation_read_engines='tiflash,tidb';")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownProjectionForMPP(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, value decimal(6,3), name char(128))")
		testKit.MustExec("analyze table t")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownAggForMPP(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, value decimal(6,3))")
		testKit.MustExec("analyze table t")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec(" set @@tidb_allow_mpp=1; set @@tidb_broadcast_join_threshold_count = 1; set @@tidb_broadcast_join_threshold_size=1;")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestMppVersion(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a bigint, b bigint)")
		testKit.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
		testKit.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			setStmt := strings.HasPrefix(tt, "set")
			testdata.OnRecord(func() {
				output[i].SQL = tt
				if !setStmt {
					output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
					output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
				}
			})
			if setStmt {
				testKit.MustExec(tt)
			} else {
				testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
				require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
			}
		}
	})
}

func TestIssue52828(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("DROP TABLE IF EXISTS b")
		testKit.MustExec("CREATE TABLE b (col_int int(11) DEFAULT NULL, col_datetime_not_null datetime NOT NULL, col_int_not_null int(11) NOT NULL, col_decimal_not_null decimal(10,0) NOT NULL)")
		testKit.MustExec("INSERT INTO b VALUES (0,'2001-09-16 00:00:00',1,1622212608)")
		testKit.MustExec("DROP TABLE IF EXISTS c")
		testKit.MustExec("CREATE TABLE c (pk int(11) NOT NULL, col_decimal_not_null decimal(10,0) NOT NULL)")
		testKit.MustExec("INSERT INTO C VALUES (30,-636485632); INSERT INTO c VALUES (50,-1094713344)")
		testKit.MustExec("DROP TABLE IF EXISTS dd")
		testKit.MustExec("CREATE TABLE dd (col_varchar_10 varchar(10) DEFAULT NULL, col_varchar_10_not_null varchar(10) NOT NULL, pk int(11), col_int_not_null int(11) NOT NULL)")
		testKit.MustExec("INSERT INTO dd VALUES ('','t',1,-1823473664), ('for','p',2,1150025728), ('p','',3,2014511104), ('y','this',4,0), ('y','w',5,-510132224)")
		testKit.MustExec("analyze table b")
		testKit.MustExec("analyze table c")
		testKit.MustExec("analyze table dd")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "b")
		testkit.SetTiFlashReplica(t, dom, "test", "c")
		testkit.SetTiFlashReplica(t, dom, "test", "dd")

		testKit.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
		testKit.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@tidb_broadcast_join_threshold_size = 0")
		testKit.MustExec("set @@tidb_broadcast_join_threshold_count = 0")
		testKit.MustQuery("explain SELECT MAX( OUTR . col_int ) AS X FROM C AS OUTR2 INNER JOIN B AS OUTR ON ( OUTR2 . col_decimal_not_null = OUTR . col_decimal_not_null AND OUTR2 . pk = OUTR . col_int_not_null ) " +
			"WHERE OUTR . col_decimal_not_null IN ( SELECT INNR . col_int_not_null + 1 AS Y FROM DD AS INNR WHERE INNR . pk > INNR . pk OR INNR . col_varchar_10_not_null >= INNR . col_varchar_10 ) GROUP BY OUTR . col_datetime_not_null")
	})
}

func TestMPPJoinWithRemoveUselessExchange(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec(`CREATE TABLE t1 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
		testKit.MustExec(`CREATE TABLE t2 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
		testKit.MustExec(`CREATE TABLE t3 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
		testKit.MustExec(`CREATE TABLE t4 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testkit.SetTiFlashReplica(t, dom, "test", "t2")
		testkit.SetTiFlashReplica(t, dom, "test", "t3")
		testkit.SetTiFlashReplica(t, dom, "test", "t4")
		var input []string
		var output []struct {
			SQL  string
			Plan []string
			Warn []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			setStmt := strings.HasPrefix(tt, "set")
			testdata.OnRecord(func() {
				output[i].SQL = tt
				if !setStmt {
					output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
					output[i].Warn = testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings())
				}
			})
			if setStmt {
				testKit.MustExec(tt)
			} else {
				testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
				require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(testKit.Session().GetSessionVars().StmtCtx.GetWarnings()))
			}
		}
	})
}

func TestMPPJoinWithoutUselessExchange(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t1")
		testKit.MustExec("create table t1(id int, v1 decimal(20,2), v2 decimal(20,2))")
		testKit.MustExec("create table t2(id int, v1 decimal(10,2), v2 decimal(10,2))")
		testKit.MustExec("create table t3(id int, v1 decimal(10,2), v2 decimal(10,2))")
		testKit.MustExec("insert into t1 values(1,1,1),(2,2,2)")
		testKit.MustExec("insert into t2 values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8)")
		testKit.MustExec("insert into t3 values(1,1,1)")
		testKit.MustExec("analyze table t1 all columns")
		testKit.MustExec("analyze table t2 all columns")
		testKit.MustExec("analyze table t3 all columns")

		testKit.MustExec("create table d1_t(d1_k int, value int)")
		testKit.MustExec("create table d2_t(d2_k decimal(10,2), value int)")
		testKit.MustExec("create table d3_t(d3_k date, value int)")
		testKit.MustExec("create table fact_t(d1_k int, d2_k decimal(10,2), d3_k date, col1 int, col2 int, col3 int)")

		testkit.SetTiFlashReplica(t, dom, "test", "t1")
		testkit.SetTiFlashReplica(t, dom, "test", "t2")
		testkit.SetTiFlashReplica(t, dom, "test", "t3")
		testkit.SetTiFlashReplica(t, dom, "test", "fact_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d1_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d2_t")
		testkit.SetTiFlashReplica(t, dom, "test", "d3_t")

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_enforce_mpp = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		testKit.MustExec("set @@session.tidb_opt_mpp_outer_join_fixed_build_side = 0")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			res := testKit.MustQuery(tt)
			res.Check(testkit.Rows(output[i].Plan...))
		}
	})
}
