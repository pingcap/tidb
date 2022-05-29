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
	"strings"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
)

func TestSetVariables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// test value limit of tidb_opt_tiflash_concurrency_factor
	tk.MustExec("set @@tidb_opt_tiflash_concurrency_factor = 0")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_opt_tiflash_concurrency_factor value: '0'"))
	tk.MustQuery(`select @@tidb_opt_tiflash_concurrency_factor`).Check(testkit.Rows("1"))

	// test set tidb_enforce_mpp when tidb_allow_mpp=false;
	err := tk.ExecToErr("set @@tidb_allow_mpp = 0; set @@tidb_enforce_mpp = 1;")
	require.EqualError(t, err, `[variable:1231]Variable 'tidb_enforce_mpp' can't be set to the value of '1' but tidb_allow_mpp is 0, please activate tidb_allow_mpp at first.'`)
	err = tk.ExecToErr("set @@tidb_allow_mpp = 1; set @@tidb_enforce_mpp = 1;")
	require.NoError(t, err)
	err = tk.ExecToErr("set @@tidb_allow_mpp = 0;")
	require.NoError(t, err)
}

func TestRowSizeInMPP(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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

func TestEnforceMPP(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create index idx on t(a)")

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

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := plannercore.GetEnforceMPPSuiteData()
	enforceMPPSuiteData.GetTestCases(t, &input, &output)
	filterWarnings := func(originalWarnings []stmtctx.SQLWarn) []stmtctx.SQLWarn {
		warnings := make([]stmtctx.SQLWarn, 0, 4)
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
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		require.Equal(t, output[i].Warn, testdata.ConvertSQLWarnToStrings(filterWarnings(tk.Session().GetSessionVars().StmtCtx.GetWarnings())))
	}
}

// general cases.
func TestEnforceMPPWarning1(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int as (a+1), c enum('xx', 'yy'), d bit(1))")
	tk.MustExec("create index idx on t(a)")

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := plannercore.GetEnforceMPPSuiteData()
	enforceMPPSuiteData.GetTestCases(t, &input, &output)
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
			for _, tblInfo := range db.Tables {
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
			for _, tblInfo := range db.Tables {
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b char(20)) PARTITION BY HASH(a)")

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

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := plannercore.GetEnforceMPPSuiteData()
	enforceMPPSuiteData.GetTestCases(t, &input, &output)
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b char(20))")

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

	var input []string
	var output []struct {
		SQL  string
		Plan []string
		Warn []string
	}
	enforceMPPSuiteData := plannercore.GetEnforceMPPSuiteData()
	enforceMPPSuiteData.GetTestCases(t, &input, &output)
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(a int primary key)")
	tk.MustExec("drop table if exists s")
	tk.MustExec("CREATE TABLE s(a int primary key)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
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
	enforceMPPSuiteData := plannercore.GetEnforceMPPSuiteData()
	enforceMPPSuiteData.GetTestCases(t, &input, &output)
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("drop table if exists c")
	tk.MustExec("drop table if exists o")
	tk.MustExec("create table c(c_id bigint)")
	tk.MustExec("create table o(o_id bigint, c_id bigint not null)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "c" || tblInfo.Name.L == "o" {
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
	enforceMPPSuiteData := plannercore.GetEnforceMPPSuiteData()
	enforceMPPSuiteData.GetTestCases(t, &input, &output)
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
