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

package enforcempp

import (
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/collate"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/stretchr/testify/require"
)

func TestEnforceMPP(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create index idx on t(a)")
	tk.MustExec("CREATE TABLE `s` (\n  `a` int(11) DEFAULT NULL,\n  `b` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  `d` int(11) DEFAULT NULL,\n  UNIQUE KEY `a` (`a`),\n  KEY `ii` (`a`,`b`)\n)")
	tk.MustExec("create table t3(id int, sala char(10), name char(100), primary key(id, sala)) partition by list columns (sala)(partition p1 values in('a'));")

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")
	// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
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
		if tblInfo.Name.L == "s" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
		if tblInfo.Name.L == "t3" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
	filterWarnings := func(originalWarnings []contextutil.SQLWarn) []contextutil.SQLWarn {
		warnings := make([]contextutil.SQLWarn, 0, 4)
		for _, warning := range originalWarnings {
			// filter out warning about skyline pruning
			if !strings.Contains(warning.Err.Error(), "remain after pruning paths for") {
				warnings = append(warnings, warning)
			}
		}
		return warnings
	}
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = testdata.ConvertSQLWarnToStrings(filterWarnings(tk.Session().GetSessionVars().StmtCtx.GetWarnings()))
		})
		require.Eventually(t,
			func() bool {
				res := tk.MustQuery(tt)
				return res.Equal(testkit.Rows(output[i].Plan...))
			}, 1*time.Second, 100*time.Millisecond)
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(filterWarnings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())))
	}
}

// general cases.
func TestEnforceMPPWarning1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int as (a+1), c enum('xx', 'yy'), d bit(1))")
	tk.MustExec("create index idx on t(a)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		if strings.HasPrefix(tt, "cmd: create-replica") {
			// Create virtual tiflash replica info.
			dom := domain.GetDomain(tk.Session())
			is := dom.InfoSchema()
			db, exists := is.SchemaByName(model.NewCIStr("test"))
			require.True(t, exists)
			for _, tbl := range is.SchemaTables(db.Name) {
				tblInfo := tbl.Meta()
				if tblInfo.Name.L == "t" {
					tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
						Count:     1,
						Available: false,
					}
				}
			}
			continue
		}
		if strings.HasPrefix(tt, "cmd: enable-replica") {
			// Create virtual tiflash replica info.
			dom := domain.GetDomain(tk.Session())
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

// partition table.
func TestEnforceMPPWarning2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b char(20)) PARTITION BY HASH(a)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
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

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
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

// new collation.
func TestEnforceMPPWarning3(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b char(20))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
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

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
			tk.MustExec(tt)
			continue
		}
		if strings.HasPrefix(tt, "cmd: enable-new-collation") {
			collate.SetNewCollationEnabledForTest(true)
			continue
		}
		if strings.HasPrefix(tt, "cmd: disable-new-collation") {
			collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
}

// Test enforce mpp warning for joins
func TestEnforceMPPWarning4(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(a int primary key)")
	tk.MustExec("drop table if exists s")
	tk.MustExec("CREATE TABLE s(a int primary key)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" || tblInfo.Name.L == "s" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
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

// Test agg push down for MPP mode
func TestMPP2PhaseAggPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists c")
	tk.MustExec("drop table if exists o")
	tk.MustExec("create table c(c_id bigint)")
	tk.MustExec("create table o(o_id bigint, c_id bigint not null)")

	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")
	tk.MustExec("insert into t values (1, 1);")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "c" || tblInfo.Name.L == "o" || tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
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

// Test skewed group distinct aggregate rewrite for MPP mode
func TestMPPSkewedGroupDistinctRewrite(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b bigint not null, c bigint, d date, e varchar(20))")
	// since allow-mpp is adjusted to false, there will be no physical plan if TiFlash cop is banned.
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
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

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
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

// Test 3 stage aggregation for single count distinct
func TestMPPSingleDistinct3Stage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b bigint not null, c bigint, d date, e varchar(20) collate utf8mb4_general_ci)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
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

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
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

// todo: some post optimization after resolveIndices will inject another projection below agg, which change the column name used in higher operator,
//
//	since it doesn't change the schema out (index ref is still the right), so by now it's fine. SEE case: EXPLAIN select count(distinct a), count(distinct b), sum(c) from t.
func TestMPPMultiDistinct3Stage(t *testing.T) {
	store := testkit.CreateMockStore(t, coretestsdk.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, d int);")
	tk.MustExec("alter table t set tiflash replica 1")
	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)
	tk.MustExec("set @@session.tidb_opt_enable_three_stage_multi_distinct_agg=1")
	defer tk.MustExec("set @@session.tidb_opt_enable_three_stage_multi_distinct_agg=0")
	tk.MustExec("set @@session.tidb_isolation_read_engines=\"tiflash\";")
	tk.MustExec("set @@session.tidb_enforce_mpp=1")
	tk.MustExec("set @@session.tidb_allow_mpp=ON;")
	// todo: current mock regionCache won't scale the regions among tiFlash nodes. The under layer still collect data from only one of the nodes.
	tk.MustExec("split table t BETWEEN (0) AND (5000) REGIONS 5;")
	tk.MustExec("insert into t values(1000, 1000, 1000, 1)")
	tk.MustExec("insert into t values(1000, 1000, 1000, 1)")
	tk.MustExec("insert into t values(2000, 2000, 2000, 1)")
	tk.MustExec("insert into t values(2000, 2000, 2000, 1)")
	tk.MustExec("insert into t values(3000, 3000, 3000, 1)")
	tk.MustExec("insert into t values(3000, 3000, 3000, 1)")
	tk.MustExec("insert into t values(4000, 4000, 4000, 1)")
	tk.MustExec("insert into t values(4000, 4000, 4000, 1)")
	tk.MustExec("insert into t values(5000, 5000, 5000, 1)")
	tk.MustExec("insert into t values(5000, 5000, 5000, 1)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
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

// Test null-aware semi join push down for MPP mode
func TestMPPNullAwareSemiJoinPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t, coretestsdk.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("create table s(a int, b int, c int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("alter table s set tiflash replica 1")

	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tb = external.GetTableByName(t, tk, "test", "s")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
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

func TestMPPSharedCTEScan(t *testing.T) {
	store := testkit.CreateMockStore(t, coretestsdk.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("create table s(a int, b int, c int)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("alter table s set tiflash replica 1")

	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tb = external.GetTableByName(t, tk, "test", "s")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}

	tk.MustExec("set @@tidb_enforce_mpp='on'")
	tk.MustExec("set @@tidb_opt_enable_mpp_shared_cte_execution='on'")

	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
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

func TestRollupMPP(t *testing.T) {
	store := testkit.CreateMockStore(t, coretestsdk.WithMockTiFlash(2))
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("create table s(a int, b int, c int)")
	tk.MustExec("CREATE TABLE `sales` (`year` int(11) DEFAULT NULL, `country` varchar(20) DEFAULT NULL,  `product` varchar(32) DEFAULT NULL,  `profit` int(11) DEFAULT NULL)")
	tk.MustExec("alter table t set tiflash replica 1")
	tk.MustExec("alter table s set tiflash replica 1")
	tk.MustExec("alter table sales set tiflash replica 1")

	tb := external.GetTableByName(t, tk, "test", "t")
	err := domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tb = external.GetTableByName(t, tk, "test", "s")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	tb = external.GetTableByName(t, tk, "test", "sales")
	err = domain.GetDomain(tk.Session()).DDL().UpdateTableReplicaInfo(tk.Session(), tb.Meta().ID, true)
	require.NoError(t, err)

	// error test
	err = tk.ExecToErr("explain format = 'brief' SELECT country, product, SUM(profit) AS profit FROM sales GROUP BY country, country, product with rollup order by grouping(year);")
	require.Equal(t, err.Error(), "[planner:3602]Argument #0 of GROUPING function is not in GROUP BY")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}

	tk.MustExec("set @@tidb_enforce_mpp='on'")
	tk.Session().GetSessionVars().TiFlashFineGrainedShuffleStreamCount = -1

	enforceMPPSuiteData := GetEnforceMPPSuiteData()
	enforceMPPSuiteData.LoadTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
		})
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
