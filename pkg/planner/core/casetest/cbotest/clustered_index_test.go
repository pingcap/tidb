// Copyright 2026 PingCAP, Inc.
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

package cbotest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestClusteredIndexPlansWithStats(t *testing.T) {
	// TestKit disables periodic stats updates so a background refresh cannot
	// publish a partial snapshot while the fixtures are being loaded.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_outer_join_reorder = true")

	for _, schema := range []struct {
		name       string
		primaryKey string
	}{
		{name: "with_cluster_index", primaryKey: "clustered"},
		{name: "wout_cluster_index", primaryKey: "nonclustered"},
	} {
		tk.MustExec("create database " + schema.name)
		tk.MustExec("use " + schema.name)
		tk.MustExec(fmt.Sprintf(`create table tbl_0 (
			col_0 decimal not null,
			col_1 blob(207),
			col_2 text,
			col_3 datetime default '1986-07-01',
			col_4 bigint unsigned default 1504335725690712365,
			primary key idx_0 (col_3, col_2(1), col_1(6)) %s,
			key idx_1 (col_3),
			unique key idx_2 (col_3),
			unique key idx_3 (col_0),
			key idx_4 (col_1(1), col_2(1)),
			key idx_5 (col_2(1))
		)`, schema.primaryKey))
		tk.MustExec(fmt.Sprintf(`create table tbl_2 (
			col_10 datetime default '1976-05-11',
			col_11 datetime,
			col_12 float,
			col_13 double(56,29) default 18.0118,
			col_14 char not null,
			primary key idx_8 (col_14, col_13, col_10) %s,
			key idx_9 (col_11)
		)`, schema.primaryKey))
		for _, table := range []string{"tbl_0", "tbl_2"} {
			require.NoError(t, testkit.LoadTableStats(schema.name+"_"+table+".json", dom))
			dom.StatsHandle().Cache().WaitForAsyncUpdates()
		}
	}

	var input []string
	var output []struct {
		SQL  string
		Plan []string
	}
	suite := testDataMap["clustered_index"]
	suite.LoadTestCases(t, &input, &output)
	require.Len(t, output, len(input))
	for i, sql := range input {
		plan := tk.MustQuery(sql)
		testdata.OnRecord(func() {
			output[i].SQL = sql
			output[i].Plan = testdata.ConvertRowsToStrings(plan.Rows())
		})
		plan.Check(testkit.Rows(output[i].Plan...))
	}
}
