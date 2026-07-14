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

package tpcds

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/pingcap/tidb/pkg/util/benchdaily"
)

func TestTPCDSQ64(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("create database if not exists tpcds;")
		tk.MustExec("use tpcds;")
		createCatalogReturns(t, tk, dom)
		createCatalogSales(t, tk, dom)
		createCustomerAddress(t, tk, dom)
		createCustomerDemographics(t, tk, dom)
		createCustomer(t, tk, dom)
		createDateDim(t, tk, dom)
		createHouseholdDemographics(t, tk, dom)
		createIncomeBand(t, tk, dom)
		createItem(t, tk, dom)
		createPromotion(t, tk, dom)
		createStoreReturns(t, tk, dom)
		createStoreSales(t, tk, dom)
		createStore(t, tk, dom)
		tk.MustExec("set @@tidb_enforce_mpp=ON")
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
		tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
		var (
			input  []string
			output []struct {
				SQL    string
				Result []string
			}
		)
		integrationSuiteData := GetTPCDSSuiteData()
		integrationSuiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i := range input {
			testdata.OnRecord(func() {
				output[i].SQL = input[i]
			})
			testdata.OnRecord(func() {
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
			})
			tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
		}
	})
}

func BenchmarkTPCDSQ64(b *testing.B) {
	store, dom := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("create database if not exists tpcds;")
	tk.MustExec("use tpcds;")
	createCatalogReturns(b, tk, dom)
	createCatalogSales(b, tk, dom)
	createCustomerAddress(b, tk, dom)
	createCustomerDemographics(b, tk, dom)
	createCustomer(b, tk, dom)
	createDateDim(b, tk, dom)
	createHouseholdDemographics(b, tk, dom)
	createIncomeBand(b, tk, dom)
	createItem(b, tk, dom)
	createPromotion(b, tk, dom)
	createStoreReturns(b, tk, dom)
	createStoreSales(b, tk, dom)
	createStore(b, tk, dom)
	testkit.LoadTableStats("tpcds50.catalog_returns.json", dom)
	testkit.LoadTableStats("tpcds50.catalog_sales.json", dom)
	testkit.LoadTableStats("tpcds50.customer.json", dom)
	testkit.LoadTableStats("tpcds50.customer_address.json", dom)
	testkit.LoadTableStats("tpcds50.customer_demographics.json", dom)
	testkit.LoadTableStats("tpcds50.date_dim.json", dom)
	testkit.LoadTableStats("tpcds50.household_demographics.json", dom)
	testkit.LoadTableStats("tpcds50.income_band.json", dom)
	testkit.LoadTableStats("tpcds50.item.json", dom)
	testkit.LoadTableStats("tpcds50.promotion.json", dom)
	testkit.LoadTableStats("tpcds50.store.json", dom)
	testkit.LoadTableStats("tpcds50.store_returns.json", dom)
	testkit.LoadTableStats("tpcds50.store_sales.json", dom)
	testkit.LoadTableStats("tpcds_suite_in.json", dom)
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_size = 0")
	tk.MustExec("set @@session.tidb_broadcast_join_threshold_count = 0")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tk.MustQuery(`explain format='brief' with cs_ui as
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales
      ,catalog_returns
  where cs_item_sk = cr_item_sk
    and cs_order_number = cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
cross_sales as
 (select i_product_name product_name
     ,i_item_sk item_sk
     ,s_store_name store_name
     ,s_zip store_zip
     ,ad1.ca_street_number b_street_number
     ,ad1.ca_street_name b_street_name
     ,ad1.ca_city b_city
     ,ad1.ca_zip b_zip
     ,ad2.ca_street_number c_street_number
     ,ad2.ca_street_name c_street_name
     ,ad2.ca_city c_city
     ,ad2.ca_zip c_zip
     ,d1.d_year as syear
     ,d2.d_year as fsyear
     ,d3.d_year s2year
     ,count(*) cnt
     ,sum(ss_wholesale_cost) s1
     ,sum(ss_list_price) s2
     ,sum(ss_coupon_amt) s3
  FROM   store_sales
        ,store_returns
        ,cs_ui
        ,date_dim d1
        ,date_dim d2
        ,date_dim d3
        ,store
        ,customer
        ,customer_demographics cd1
        ,customer_demographics cd2
        ,promotion
        ,household_demographics hd1
        ,household_demographics hd2
        ,customer_address ad1
        ,customer_address ad2
        ,income_band ib1
        ,income_band ib2
        ,item
  WHERE  ss_store_sk = s_store_sk AND
         ss_sold_date_sk = d1.d_date_sk AND
         ss_customer_sk = c_customer_sk AND
         ss_cdemo_sk= cd1.cd_demo_sk AND
         ss_hdemo_sk = hd1.hd_demo_sk AND
         ss_addr_sk = ad1.ca_address_sk and
         ss_item_sk = i_item_sk and
         ss_item_sk = sr_item_sk and
         ss_ticket_number = sr_ticket_number and
         ss_item_sk = cs_ui.cs_item_sk and
         c_current_cdemo_sk = cd2.cd_demo_sk AND
         c_current_hdemo_sk = hd2.hd_demo_sk AND
         c_current_addr_sk = ad2.ca_address_sk and
         c_first_sales_date_sk = d2.d_date_sk and
         c_first_shipto_date_sk = d3.d_date_sk and
         ss_promo_sk = p_promo_sk and
         hd1.hd_income_band_sk = ib1.ib_income_band_sk and
         hd2.hd_income_band_sk = ib2.ib_income_band_sk and
         cd1.cd_marital_status <> cd2.cd_marital_status and
         i_color in ('maroon','burnished','dim','steel','navajo','chocolate') and
         i_current_price between 35 and 35 + 10 and
         i_current_price between 35 + 1 and 35 + 15
group by i_product_name
       ,i_item_sk
       ,s_store_name
       ,s_zip
       ,ad1.ca_street_number
       ,ad1.ca_street_name
       ,ad1.ca_city
       ,ad1.ca_zip
       ,ad2.ca_street_number
       ,ad2.ca_street_name
       ,ad2.ca_city
       ,ad2.ca_zip
       ,d1.d_year
       ,d2.d_year
       ,d3.d_year
)
select cs1.product_name
     ,cs1.store_name
     ,cs1.store_zip
     ,cs1.b_street_number
     ,cs1.b_street_name
     ,cs1.b_city
     ,cs1.b_zip
     ,cs1.c_street_number
     ,cs1.c_street_name
     ,cs1.c_city
     ,cs1.c_zip
     ,cs1.syear
     ,cs1.cnt
     ,cs1.s1 as s11
     ,cs1.s2 as s21
     ,cs1.s3 as s31
     ,cs2.s1 as s12
     ,cs2.s2 as s22
     ,cs2.s3 as s32
     ,cs2.syear
     ,cs2.cnt
from cross_sales cs1,cross_sales cs2
where cs1.item_sk=cs2.item_sk and
     cs1.syear = 2000 and
     cs2.syear = 2000 + 1 and
     cs2.cnt <= cs1.cnt and
     cs1.store_name = cs2.store_name and
     cs1.store_zip = cs2.store_zip
order by cs1.product_name
       ,cs1.store_name
       ,cs2.cnt
       ,cs1.s1
       ,cs2.s1;`)
	}
}

func TestBenchDaily(t *testing.T) {
	benchdaily.Run(
		BenchmarkTPCDSQ64,
	)
}
