// Copyright 2024 PingCAP, Inc.
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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestOuter2Inner(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(a1 int, b1 int, c1 int)")
	tk.MustExec("create table t2(a2 int, b2 int, c2 int)")
	tk.MustExec("create table t3(a3 int, b3 int, c3 int)")
	tk.MustExec("create table t4(a4 int, b4 int, c4 int)")
	tk.MustExec("create table ti(i int)")
	tk.MustExec("CREATE TABLE lineitem (L_PARTKEY INTEGER ,L_QUANTITY DECIMAL(15,2),L_EXTENDEDPRICE  DECIMAL(15,2))")
	tk.MustExec("CREATE TABLE part(P_PARTKEY INTEGER,P_BRAND CHAR(10),P_CONTAINER CHAR(10))")
	tk.MustExec("CREATE TABLE d (pk int, col_blob blob, col_blob_key blob, col_varchar_key varchar(1) , col_date date, col_int_key int)")
	tk.MustExec("CREATE TABLE dd (pk int, col_blob blob, col_blob_key blob, col_date date, col_int_key int)")
	tk.MustExec("create table t0 (a0 int, b0 char, c0 char(2))")
	tk.MustExec("create table t11 (a1 int, b1 char, c1 char)")

	var input Input
	var output []struct {
		SQL  string
		Plan []string
	}
	suiteData := GetOuter2InnerSuiteData()
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
