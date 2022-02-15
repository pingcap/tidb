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
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = SerialSuites(&testEnforceMPPSuite{})

type testEnforceMPPSuite struct {
	testData testutil.TestData
	store    kv.Storage
	dom      *domain.Domain
}

func (s *testEnforceMPPSuite) SetUpSuite(c *C) {
	var err error
	s.testData, err = testutil.LoadTestSuiteData("testdata", "enforce_mpp_suite")
	c.Assert(err, IsNil)
}

func (s *testEnforceMPPSuite) TearDownSuite(c *C) {
	c.Assert(s.testData.GenerateOutputIfNeeded(), IsNil)
}

func (s *testEnforceMPPSuite) SetUpTest(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
}

func (s *testEnforceMPPSuite) TearDownTest(c *C) {
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testEnforceMPPSuite) TestSetVariables(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test value limit of tidb_opt_tiflash_concurrency_factor
	tk.MustExec("set @@tidb_opt_tiflash_concurrency_factor = 0")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_opt_tiflash_concurrency_factor value: '0'"))
	tk.MustQuery(`select @@tidb_opt_tiflash_concurrency_factor`).Check(testkit.Rows("1"))

	// test set tidb_enforce_mpp when tidb_allow_mpp=false;
	err := tk.ExecToErr("set @@tidb_allow_mpp = 0; set @@tidb_enforce_mpp = 1;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, `[variable:1231]Variable 'tidb_enforce_mpp' can't be set to the value of '1' but tidb_allow_mpp is 0, please activate tidb_allow_mpp at first.'`)

	err = tk.ExecToErr("set @@tidb_allow_mpp = 1; set @@tidb_enforce_mpp = 1;")
	c.Assert(err, IsNil)

	err = tk.ExecToErr("set @@tidb_allow_mpp = 0;")
	c.Assert(err, IsNil)
}

func (s *testEnforceMPPSuite) TestEnforceMPP(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create index idx on t(a)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
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
	s.testData.GetTestCases(c, &input, &output)
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
		s.testData.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(filterWarnings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()))
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(filterWarnings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())), DeepEquals, output[i].Warn)
	}
}

// general cases.
func (s *testEnforceMPPSuite) TestEnforceMPPWarning1(c *C) {
	tk := testkit.NewTestKit(c, s.store)

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
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		if strings.HasPrefix(tt, "cmd: create-replica") {
			// Create virtual tiflash replica info.
			dom := domain.GetDomain(tk.Se)
			is := dom.InfoSchema()
			db, exists := is.SchemaByName(model.NewCIStr("test"))
			c.Assert(exists, IsTrue)
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
			dom := domain.GetDomain(tk.Se)
			is := dom.InfoSchema()
			db, exists := is.SchemaByName(model.NewCIStr("test"))
			c.Assert(exists, IsTrue)
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
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

// partition table.
func (s *testEnforceMPPSuite) TestEnforceMPPWarning2(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b char(20)) PARTITION BY HASH(a)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
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
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") {
			tk.MustExec(tt)
			continue
		}
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}

// new collation.
func (s *testEnforceMPPSuite) TestEnforceMPPWarning3(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test query
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b char(20))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
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
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
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
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
	collate.SetNewCollationEnabledForTest(true)
}

// Test enforce mpp warning for joins
func (s *testEnforceMPPSuite) TestEnforceMPPWarning4(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	// test table
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(a int primary key)")
	tk.MustExec("drop table if exists s")
	tk.MustExec("CREATE TABLE s(a int primary key)")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	c.Assert(exists, IsTrue)
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
	s.testData.GetTestCases(c, &input, &output)
	for i, tt := range input {
		s.testData.OnRecord(func() {
			output[i].SQL = tt
		})
		if strings.HasPrefix(tt, "set") || strings.HasPrefix(tt, "UPDATE") {
			tk.MustExec(tt)
			continue
		}
		s.testData.OnRecord(func() {
			output[i].SQL = tt
			output[i].Plan = s.testData.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Warn = s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings())
		})
		res := tk.MustQuery(tt)
		res.Check(testkit.Rows(output[i].Plan...))
		c.Assert(s.testData.ConvertSQLWarnToStrings(tk.Se.GetSessionVars().StmtCtx.GetWarnings()), DeepEquals, output[i].Warn)
	}
}
