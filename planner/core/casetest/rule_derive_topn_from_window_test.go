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

package casetest

import (
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner/core/internal"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func setTiFlashReplica(t *testing.T, dom *domain.Domain, dbName, tableName string) {
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr(dbName))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == tableName {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
}

// Rule should bot be applied for TiKV.
func TestPushDerivedTopnNegative(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_opt_derive_topn=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, primary key(b,a))")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(a int, b int, c int, primary key(b,a) nonclustered)")
	tk.MustExec("drop table if exists ti")
	tk.MustExec("create table ti(a int, b int, c int unique)")
	tk.MustExec("drop table if exists td")
	tk.MustExec("create table td(a int, b int as (a+1) stored, primary key(b,a));")
	var input Input
	var output []struct {
		SQL  string
		Plan []string
	}
	suiteData := GetDerivedTopNSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

// TiFlash cases. TopN pushed down to storage only when no partition by.
func TestPushDerivedTopnFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())

	tk.MustExec("set tidb_opt_derive_topn=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(b,a))")
	internal.SetTiFlashReplica(t, dom, "test", "t")
	tk.MustExec("set tidb_enforce_mpp=1")
	tk.MustExec("set @@session.tidb_allow_mpp=ON;")
	var input Input
	var output []struct {
		SQL  string
		Plan []string
	}
	suiteData := GetDerivedTopNSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}

// Rule should be applied for TiKV.
func TestPushDerivedTopnPositive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_opt_derive_topn=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(b,a))")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(a int, b int, c int, primary key(b,a) nonclustered)")
	tk.MustExec("drop table if exists ti")
	tk.MustExec("create table ti(a int, b int, c int unique)")
	tk.MustExec("drop table if exists customer")
	tk.MustExec("create table customer(primary_key VARBINARY(1024), secondary_key VARBINARY(1024), c_timestamp BIGINT, value MEDIUMBLOB, PRIMARY KEY (primary_key, secondary_key, c_timestamp) clustered);")
	tk.MustExec("drop table if exists td")
	tk.MustExec("create table td(a int, b int as (a+1) stored, primary key(b,a));")
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("insert into t values(2,1)")
	tk.MustExec("insert into t values(3,2)")
	tk.MustExec("insert into t values(4,2)")
	tk.MustExec("insert into t values(5,2)")
	tk.MustExec("insert into tt select *,55 from t")
	tk.MustExec("insert into ti select *,a from t")
	tk.MustExec("insert into td(a) select a from t")
	var input Input
	var output []struct {
		SQL  string
		Plan []string
		Res  []string
	}
	suiteData := GetDerivedTopNSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		res := tk.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			output[i].Res = testdata.ConvertRowsToStrings(res.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
		res.Check(testkit.Rows(output[i].Res...))
	}
}

// Negative test when tidb_opt_derive_topn is off
func TestPushDerivedTopnFlagOff(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_opt_derive_topn=0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(b,a))")
	var input Input
	var output []struct {
		SQL  string
		Plan []string
	}
	suiteData := GetDerivedTopNSuiteData()
	suiteData.LoadTestCases(t, &input, &output)
	for i, sql := range input {
		plan := tk.MustQuery("explain format = 'brief' " + sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}
