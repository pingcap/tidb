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
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func SetTiFlashReplica(t *testing.T, dom *domain.Domain, dbName, tableName string) {
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

type Input []string
type Output []struct {
	SQL  string
	Plan []string
	Warn []string
}

func testWithData(t *testing.T, tk *testkit.TestKit, input Input, output Output) {
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
		// c.Assert(len(output[i].Warn), Equals, 0)
	}
}

// Test WindowFuncDesc.CanPushDownToTiFlash
func TestWindowFunctionDescCanPushDown(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())

	tk.MustExec("use test")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table employee (empid int, deptid int, salary decimal(10,2))")
	SetTiFlashReplica(t, dom, "test", "employee")

	var input Input
	var output Output
	suiteData := plannercore.GetWindowPushDownSuiteData()
	suiteData.GetTestCases(t, &input, &output)
	testWithData(t, tk, input, output)
}


func TestWindowPushDownPlans(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())

	tk.MustExec("use test")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table employee (empid int, deptid int, salary decimal(10,2))")
	SetTiFlashReplica(t, dom, "test", "employee")

	var input Input
	var output Output
	suiteData := plannercore.GetWindowPushDownSuiteData()
	suiteData.GetTestCases(t, &input, &output)
	testWithData(t, tk, input, output)
}

func TestWindowPlanWithOtherOperators(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	dom := domain.GetDomain(tk.Session())

	tk.MustExec("use test")
	tk.MustExec("drop table if exists employee")
	tk.MustExec("create table employee (empid int, deptid int, salary decimal(10,2))")
	SetTiFlashReplica(t, dom, "test", "employee")

	var input Input
	var output Output
	suiteData := plannercore.GetWindowPushDownSuiteData()
	suiteData.GetTestCases(t, &input, &output)
	testWithData(t, tk, input, output)
}
