// Copyright 2025 PingCAP, Inc.
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

package tpch

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func TestQ1(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createLineItem(tk)
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ3(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createCustomer(tk)
	createOrders(tk)
	createLineItem(tk)
	testkit.SetTiFlashReplica(t, dom, "test", "customer")
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ4(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database olap")
	tk.MustExec("use test")
	createOrders(tk)
	createLineItem(tk)
	testkit.LoadTableStats("lineitem_stats.json", dom)
	testkit.LoadTableStats("orders_stats.json", dom)
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData := GetTPCHSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}

// check the cost trace's cost and verbose's cost. they should be the same.
// it is from https://github.com/pingcap/tidb/issues/61155
func checkCost(t *testing.T, tk *testkit.TestKit, q4 string) {
	costTraceFormat := `explain format='cost_trace' `
	verboseFormat := `explain format='verbose' `
	costTraceRows := tk.MustQuery(costTraceFormat + q4)
	verboseRows := tk.MustQuery(verboseFormat + q4)
	require.Equal(t, len(costTraceRows.Rows()), len(verboseRows.Rows()))
	for i := 0; i < len(costTraceRows.Rows()); i++ {
		// check id / estRows / estCost. they should be the same one
		require.Equal(t, costTraceRows.Rows()[i][:3], verboseRows.Rows()[i][:3])
	}
}

func TestQ9(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createLineItem(tk)
	createNation(tk)
	createOrders(tk)
	createPart(tk)
	createPartsupp(tk)
	createSupplier(tk)
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
	testkit.SetTiFlashReplica(t, dom, "test", "nation")
	testkit.SetTiFlashReplica(t, dom, "test", "part")
	testkit.SetTiFlashReplica(t, dom, "test", "partsupp")
	testkit.SetTiFlashReplica(t, dom, "test", "supplier")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ13(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createCustomer(tk)
	createOrders(tk)
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	testkit.SetTiFlashReplica(t, dom, "test", "customer")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ18(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	createCustomer(tk)
	createOrders(tk)
	createLineItem(tk)
	testkit.SetTiFlashReplica(t, dom, "test", "customer")
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	integrationSuiteData := GetTPCHSuiteData()
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData.LoadTestCases(t, &input, &output)
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestQ21(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	createSupplier(tk)
	createLineItem(tk)
	createOrders(tk)
	createNation(tk)
	testkit.SetTiFlashReplica(t, dom, "test", "supplier")
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	testkit.SetTiFlashReplica(t, dom, "test", "nation")
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData := GetTPCHSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}

func TestQ22(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	createCustomer(tk)
	createOrders(tk)
	testkit.SetTiFlashReplica(t, dom, "test", "customer")
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	tk.MustExec("set @@tidb_opt_enable_non_eval_scalar_subquery=true")
	var (
		input  []string
		output []struct {
			SQL    string
			Result []string
		}
	)
	integrationSuiteData := GetTPCHSuiteData()
	integrationSuiteData.LoadTestCases(t, &input, &output)
	costTraceFormat := `explain format='cost_trace' `
	for i := range input {
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
		})
		testdata.OnRecord(func() {
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(costTraceFormat + input[i]).Rows())
		})
		tk.MustQuery(costTraceFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		checkCost(t, tk, input[i])
	}
}
