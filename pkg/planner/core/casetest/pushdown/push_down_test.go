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

package pushdown

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestPushDownToTiFlashWithKeepOrder(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int primary key, b varchar(20))")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 0")
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
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownToTiFlashWithKeepOrderInFastMode(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int primary key, b varchar(20))")
		testKit.MustExec("set @@session.tiflash_fastscan=ON")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 0")
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
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownProjectionForTiFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (id int, value decimal(6,3), name char(128))")
		testKit.MustExec("analyze table t")
		testKit.MustExec("set session tidb_allow_mpp=OFF")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "t")

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
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestPushDownProjectionForTiFlashCoprocessor(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t (a int, b real, i int, id int, value decimal(6,3), name char(128), d decimal(6,3), s char(128), t datetime, c bigint as ((a+1)) virtual, e real as ((b+a)))")
		testKit.MustExec("analyze table t")
		testKit.MustExec("set session tidb_opt_projection_push_down=1")

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
		}
		integrationSuiteData := GetIntegrationSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestSelPushDownTiFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists t")
		testKit.MustExec("create table t(a int primary key, b varchar(20))")
		// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
		testKit.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

		// Create virtual tiflash replica info.
		is := dom.InfoSchema()
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
			Count:     1,
			Available: true,
		}

		testKit.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		testKit.MustExec("set @@session.tidb_allow_mpp = 0")

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
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}

func TestJoinNotSupportedByTiFlash(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, testKit *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("drop table if exists table_1")
		testKit.MustExec("create table table_1(id int not null, bit_col bit(2) not null, datetime_col datetime not null, index idx(id, bit_col, datetime_col))")
		testKit.MustExec("insert into table_1 values(1,b'1','2020-01-01 00:00:00'),(2,b'0','2020-01-01 00:00:00')")
		testKit.MustExec("analyze table table_1")

		testKit.MustExec("insert into mysql.expr_pushdown_blacklist values('dayofmonth', 'tiflash', '');")
		testKit.MustExec("admin reload expr_pushdown_blacklist;")

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
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}

		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_size = 1")
		testKit.MustExec("set @@session.tidb_broadcast_join_threshold_count = 1")
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, tt := range input {
			testdata.OnRecord(func() {
				output[i].SQL = tt
				output[i].Plan = testdata.ConvertRowsToStrings(testKit.MustQuery(tt).Rows())
			})
			testKit.MustQuery(tt).Check(testkit.Rows(output[i].Plan...))
		}
	})
}
