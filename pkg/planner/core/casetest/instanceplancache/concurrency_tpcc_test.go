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

package instanceplancache

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestInstancePlanCacheConcurrencyTPCC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database normal`)
	tk.MustExec(`create database prepared`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	for _, db := range []string{"normal", "prepared"} {
		tk.MustExec("use " + db)
		tk.MustExec(tpccCustomer)
		tk.MustExec(tpccWarehouse)
		tk.MustExec(tpccDistrict)
		tk.MustExec(tpccOrders)
		tk.MustExec(tpccNewOrder)
		tk.MustExec(tpccStock)
	}

	// prepare initial data
	tk.MustExec("use normal")
	nWarehouse := 10
	stockPerWarehouse := 100
	districtPerWarehouse := 10
	customerPerDistrict := 30
	orderPerdistrict := 30
	for w := 1; w <= nWarehouse; w++ {
		tk.MustExec(`INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ` +
			fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v, %d, %d)",
				w, rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rInt(), rInt()))

		for s := 1; s <= stockPerWarehouse; s++ {
			tk.MustExec(`insert into stock (s_i_id, s_w_id, s_quantity,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
				s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES ` +
				fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v",
					s, w, rInt(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rInt(), rInt(), rInt(), rStr()))
		}

		for d := 1; d <= districtPerWarehouse; d++ {
			tk.MustExec(`insert into district (d_id, d_w_id, d_name, d_street_1, d_street_2,
				d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES ` +
				fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v, %v, %d, %d, %d",
					d, w, rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rInt(), rInt(), rInt()))
		}

		districts := nWarehouse * districtPerWarehouse
		for i := 0; i < districts; i++ {
			warehouse := (i/districtPerWarehouse)%nWarehouse + 1
			district := i%districtPerWarehouse + 1
			for i := 0; i < customerPerDistrict; i++ {
				tk.MustExec(`insert into customer (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2,
					c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment,
					c_payment_cnt, c_delivery_cnt, c_data) VALUES ` +
					fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v",
						i, district, warehouse, rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(),
						rStr(), rInt(), rInt(), rInt(), rInt(), rInt(), rInt(), rStr()))
			}

			for o := 0; o < orderPerdistrict; o++ {
				tk.MustExec(`insert into orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ` +
					fmt.Sprintf("%v, %v, %v, %v, %v, %v, %v, %v",
						o, district, warehouse, rInt(), rStr(), rInt(), rInt(), rInt()))
			}
		}
	}
}

func rInt() int {
	return rand.Intn(10)
}

func rStr() string {
	return fmt.Sprintf("%v", rand.Intn(10))
}
