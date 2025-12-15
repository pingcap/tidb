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

package ch

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestQ2(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("create database tpcc")
		tk.MustExec("use tpcc")
		createItem(t, tk, dom)
		createNation(t, tk, dom)
		createRegion(t, tk, dom)
		createStock(t, tk, dom)
		createSupplier(t, tk, dom)
		testkit.LoadTableStats("tpcc.item.json", dom)
		testkit.LoadTableStats("tpcc.nation.json", dom)
		testkit.LoadTableStats("tpcc.region.json", dom)
		testkit.LoadTableStats("tpcc.stock.json", dom)
		testkit.LoadTableStats("tpcc.supplier.json", dom)
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		integrationSuiteData := GetCHSuiteData()
		var (
			input  []string
			output []struct {
				SQL    string
				Result []string
			}
		)
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		briefFormat := `explain format='brief' `
		for i := range input {
			testdata.OnRecord(func() {
				output[i].SQL = input[i]
			})
			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(briefFormat + input[i]).Rows())
			})
			tk.MustQuery(briefFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		}
	})
}

func TestQ5(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("create database tpcc")
		tk.MustExec("use tpcc")
		createCustomer(t, tk, dom)
		createNation(t, tk, dom)
		createOrders(t, tk, dom)
		createOrderLine(t, tk, dom)
		createSupplier(t, tk, dom)
		createRegion(t, tk, dom)
		createStock(t, tk, dom)
		testkit.LoadTableStats("tpcc.customer.json", dom)
		testkit.LoadTableStats("tpcc.nation.json", dom)
		testkit.LoadTableStats("tpcc.order_line.json", dom)
		testkit.LoadTableStats("tpcc.orders.json", dom)
		testkit.LoadTableStats("tpcc.region.json", dom)
		testkit.LoadTableStats("tpcc.stock.json", dom)
		testkit.LoadTableStats("tpcc.supplier.json", dom)
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		integrationSuiteData := GetCHSuiteData()
		var (
			input  []string
			output []struct {
				SQL    string
				Result []string
			}
		)
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		briefFormat := `explain format='brief' `
		for i := range input {
			testdata.OnRecord(func() {
				output[i].SQL = input[i]
			})
			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(briefFormat + input[i]).Rows())
			})
			tk.MustQuery(briefFormat + input[i]).Check(testkit.Rows(output[i].Result...))
		}
	})
}
