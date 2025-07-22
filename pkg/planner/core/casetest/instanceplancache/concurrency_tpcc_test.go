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
	prepareTPCC(tk)

	genNewOrder := func() []*testStmt {
		stmts := make([]*testStmt, 0, 5)
		stmts = append(stmts, &testStmt{normalStmt: "begin"})

		// SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?
		wID := rInt()
		cwID := rInt()
		cdID := rInt()
		cID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = %d AND c_w_id = w_id AND c_d_id = %d AND c_id = %d", wID, cwID, cID),
			prepStmt:   "prepare st from 'SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = ? AND c_w_id = w_id AND c_d_id = ? AND c_id = ?'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @c_w_id = %v, @c_d_id = %v, @c_id = %v", wID, cwID, cdID, cID),
			execStmt:   "execute st using @w_id, @c_w_id, @c_d_id, @c_id",
		})

		// SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE
		dID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT d_next_o_id, d_tax FROM district WHERE d_id = %d AND d_w_id = %d FOR UPDATE", dID, wID),
			prepStmt:   "prepare st from 'SELECT d_next_o_id, d_tax FROM district WHERE d_id = ? AND d_w_id = ? FOR UPDATE'",
			setStmt:    fmt.Sprintf("set @d_id = %v, @d_w_id = %v", dID, wID),
			execStmt:   "execute st using @d_id, @d_w_id",
		})

		// UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?
		nextOID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("UPDATE district SET d_next_o_id = %d + 1 WHERE d_id = %d AND d_w_id = %d", nextOID, dID, wID),
			prepStmt:   "prepare st from 'UPDATE district SET d_next_o_id = ? + 1 WHERE d_id = ? AND d_w_id = ?'",
			setStmt:    fmt.Sprintf("set @next_oid = %v, @d_id = %v, @d_w_id = %v", nextOID, dID, wID),
			execStmt:   "execute st using @next_oid, @d_id, @d_w_id",
		})

		// insert orders and new_order, update stock
		stmts = append(stmts, &testStmt{normalStmt: "commit"})
		return stmts
	}

	genPayment := func() []*testStmt {
		stmts := make([]*testStmt, 0, 5)
		stmts = append(stmts, &testStmt{normalStmt: "begin"})

		// UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?
		ytd := rInt()
		wID := rInt()
		dID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("UPDATE district SET d_ytd = d_ytd + %d WHERE d_w_id = %d AND d_id = %d", ytd, wID, dID),
			prepStmt:   "prepare st from 'UPDATE district SET d_ytd = d_ytd + ? WHERE d_w_id = ? AND d_id = ?'",
			setStmt:    fmt.Sprintf("set @ytd = %v, @w_id = %v, @d_id = %v", ytd, wID, dID),
			execStmt:   "execute st using @ytd, @w_id, @d_id",
		})

		// SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = %d AND d_id = %d", wID, dID),
			prepStmt:   "prepare st from 'SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @d_id = %v", wID, dID),
			execStmt:   "execute st using @w_id, @d_id",
		})

		// UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("UPDATE warehouse SET w_ytd = w_ytd + %d WHERE w_id = %d", ytd, wID),
			prepStmt:   "prepare st from 'UPDATE warehouse SET w_ytd = w_ytd + ? WHERE w_id = ?'",
			setStmt:    fmt.Sprintf("set @ytd = %v, @w_id = %v", ytd, wID),
			execStmt:   "execute st using @ytd, @w_id",
		})

		// SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = %d", wID),
			prepStmt:   "prepare st from 'SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?'",
			setStmt:    fmt.Sprintf("set @w_id = %v", wID),
			execStmt:   "execute st using @w_id",
		})

		// SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first
		cwID := rInt()
		cdID := rInt()
		cLast := rStr()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT c_id FROM customer WHERE c_w_id = %d AND c_d_id = %d AND c_last = %v ORDER BY c_first", cwID, cdID, cLast),
			prepStmt:   "prepare st from 'SELECT c_id FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first'",
			setStmt:    fmt.Sprintf("set @c_w_id = %v, @c_d_id = %v, @c_last = %v", cwID, cdID, cLast),
			execStmt:   "execute st using @c_w_id, @c_d_id, @c_last",
		})

		// SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE
		cID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d FOR UPDATE", cwID, cdID, cID),
			prepStmt:   "prepare st from 'SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ? FOR UPDATE'",
			setStmt:    fmt.Sprintf("set @c_w_id = %v, @c_d_id = %v, @c_id = %v", cwID, cdID, cID),
			execStmt:   "execute st using @c_w_id, @c_d_id, @c_id",
		})

		// UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		balance := rInt()
		payment := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("UPDATE customer SET c_balance = c_balance - %d, c_ytd_payment = c_ytd_payment + %d, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d", balance, payment, cwID, cdID, cID),
			prepStmt:   "prepare st from 'UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?'",
			setStmt:    fmt.Sprintf("set @balance = %v, @payment = %v, @c_w_id = %v, @c_d_id = %v, @c_id = %v", balance, payment, cwID, cdID, cID),
			execStmt:   "execute st using @balance, @payment, @c_w_id, @c_d_id, @c_id",
		})

		// SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT c_data FROM customer WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d", cwID, cdID, cID),
			prepStmt:   "prepare st from 'SELECT c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?'",
			setStmt:    fmt.Sprintf("set @c_w_id = %v, @c_d_id = %v, @c_id = %v", cwID, cdID, cID),
			execStmt:   "execute st using @c_w_id, @c_d_id, @c_id",
		})

		// UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		data := rStr()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("UPDATE customer SET c_balance = c_balance - %d, c_ytd_payment = c_ytd_payment + %d, c_payment_cnt = c_payment_cnt + 1, c_data = %v WHERE c_w_id = %d AND c_d_id = %d AND c_id = %d", balance, payment, data, cwID, cdID, cID),
			prepStmt:   "prepare st from 'UPDATE customer SET c_balance = c_balance - ?, c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?'",
			setStmt:    fmt.Sprintf("set @balance = %v, @payment = %v, @data = %v, @c_w_id = %v, @c_d_id = %v, @c_id = %v", balance, payment, data, cwID, cdID, cID),
			execStmt:   "execute st using @balance, @payment, @data, @c_w_id, @c_d_id, @c_id",
		})

		stmts = append(stmts, &testStmt{normalStmt: "commit"})
		return stmts
	}

	genOrderStatus := func() []*testStmt {
		stmts := make([]*testStmt, 0, 5)
		stmts = append(stmts, &testStmt{normalStmt: "begin"})

		// SELECT count(c_id) INTO :namecnt FROM customer WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id
		wID := rInt()
		dID := rInt()
		cLast := rStr()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT count(c_id) INTO :namecnt FROM customer WHERE c_last=%v AND c_d_id=%v AND c_w_id=%v", cLast, dID, wID),
			prepStmt:   "prepare st from 'SELECT count(c_id) INTO :namecnt FROM customer WHERE c_last=? AND c_d_id=? AND c_w_id=?'",
			setStmt:    fmt.Sprintf("set @c_last = %v, @d_id = %v, @w_id = %v", cLast, dID, wID),
			execStmt:   "execute st using @c_last, @d_id, @w_id",
		})

		// SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = %v AND c_d_id = %v AND c_last = %v ORDER BY c_first", wID, dID, cLast),
			prepStmt:   "prepare st from 'SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @d_id = %v, @c_last = %v", wID, dID, cLast),
			execStmt:   "execute st using @w_id, @d_id, @c_last",
		})

		// SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		cID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = %v AND c_d_id = %v AND c_id = %v", wID, dID, cID),
			prepStmt:   "prepare st from 'SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @d_id = %v, @c_id = %v", wID, dID, cID),
			execStmt:   "execute st using @w_id, @d_id, @c_id",
		})

		// SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = %v AND o_d_id = %v AND o_c_id = %v ORDER BY o_id DESC LIMIT 1", wID, dID, cID),
			prepStmt:   "prepare st from 'SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @d_id = %v, @c_id = %v", wID, dID, cID),
			execStmt:   "execute st using @w_id, @d_id, @c_id",
		})

		stmts = append(stmts, &testStmt{normalStmt: "commit"})
		return stmts
	}

	genDelivery := func() []*testStmt {
		stmts := make([]*testStmt, 0, 5)
		stmts = append(stmts, &testStmt{normalStmt: "begin"})

		// SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE
		wID := rInt()
		dID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT no_o_id FROM new_order WHERE no_w_id = %v AND no_d_id = %v ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE", wID, dID),
			prepStmt:   "prepare st from 'SELECT no_o_id FROM new_order WHERE no_w_id = ? AND no_d_id = ? ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @d_id = %v", wID, dID),
			execStmt:   "execute st using @w_id, @d_id",
		})

		// DELETE FROM new_order WHERE (no_w_id, no_d_id, no_o_id) IN ((?,?,?),(?,?,?))
		oID1 := rInt()
		oID2 := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("DELETE FROM new_order WHERE (no_w_id, no_d_id, no_o_id) IN ((%v,%v,%v),(%v,%v,%v))", wID, dID, oID1, wID, dID, oID2),
			prepStmt:   "prepare st from 'DELETE FROM new_order WHERE (no_w_id, no_d_id, no_o_id) IN ((?,?,?),(?,?,?))'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @d_id = %v, @o_id1 = %v, @o_id2 = %v", wID, dID, oID1, oID2),
			execStmt:   "execute st using @w_id, @d_id, @o_id1, @w_id, @d_id, @o_id2",
		})

		// UPDATE orders SET o_carrier_id = ? WHERE (o_w_id, o_d_id, o_id) IN ((?,?,?),(?,?,?))
		carrierID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("UPDATE orders SET o_carrier_id = %v WHERE (o_w_id, o_d_id, o_id) IN ((%v,%v,%v),(%v,%v,%v))", carrierID, wID, dID, oID1, wID, dID, oID2),
			prepStmt:   "prepare st from 'UPDATE orders SET o_carrier_id = ? WHERE (o_w_id, o_d_id, o_id) IN ((?,?,?),(?,?,?))'",
			setStmt:    fmt.Sprintf("set @carrier_id = %v, @w_id = %v, @d_id = %v, @o_id1 = %v, @w_id = %v, @d_id = %v, @o_id2 = %v", carrierID, wID, dID, oID1, wID, dID, oID2),
			execStmt:   "execute st using @carrier_id, @w_id, @d_id, @o_id1, @w_id, @d_id, @o_id2",
		})

		// SELECT o_d_id, o_c_id FROM orders WHERE (o_w_id, o_d_id, o_id) IN ((?,?,?),(?,?,?))
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("SELECT o_d_id, o_c_id FROM orders WHERE (o_w_id, o_d_id, o_id) IN ((%v,%v,%v),(%v,%v,%v))", wID, dID, oID1, wID, dID, oID2),
			prepStmt:   "prepare st from 'SELECT o_d_id, o_c_id FROM orders WHERE (o_w_id, o_d_id, o_id) IN ((?,?,?),(?,?,?))'",
			setStmt:    fmt.Sprintf("set @w_id = %v, @d_id = %v, @o_id1 = %v, @w_id = %v, @d_id = %v, @o_id2 = %v", wID, dID, oID1, wID, dID, oID2),
			execStmt:   "execute st using @w_id, @d_id, @o_id1, @w_id, @d_id, @o_id2",
		})

		// UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?
		balance := rInt()
		cID := rInt()
		stmts = append(stmts, &testStmt{
			normalStmt: fmt.Sprintf("UPDATE customer SET c_balance = c_balance + %v, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = %v AND c_d_id = %v AND c_id = %v", balance, wID, dID, cID),
			prepStmt:   "prepare st from 'UPDATE customer SET c_balance = c_balance + ?, c_delivery_cnt = c_delivery_cnt + 1 WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?'",
			setStmt:    fmt.Sprintf("set @balance = %v, @w_id = %v, @d_id = %v, @c_id = %v", balance, wID, dID, cID),
			execStmt:   "execute st using @balance, @w_id, @d_id, @c_id",
		})

		stmts = append(stmts, &testStmt{normalStmt: "commit"})
		return stmts
	}

	nTxn := 300
	stmts := make([]*testStmt, 0, nTxn*5)
	for i := 0; i < nTxn; i++ {
		switch rand.Intn(4) {
		case 0:
			stmts = append(stmts, genNewOrder()...)
		case 1:
			stmts = append(stmts, genPayment()...)
		case 2:
			stmts = append(stmts, genOrderStatus()...)
		case 3:
			stmts = append(stmts, genDelivery()...)
		}
	}

	nConcurrency := 10
	TKs := make([]*testkit.TestKit, nConcurrency)
	for i := range TKs {
		TKs[i] = testkit.NewTestKit(t, store)
	}

	testWithWorkers(TKs, stmts)
}

func prepareTPCC(tk *testkit.TestKit) {
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
	stockPerWarehouse := 50
	districtPerWarehouse := 10
	customerPerDistrict := 15
	orderPerDistrict := 15
	newOrderPerDistrict := 8
	for w := 1; w <= nWarehouse; w++ {
		tk.MustExec(`INSERT INTO warehouse (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) VALUES ` +
			fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v, %v, %d)",
				w, rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), "0.1", rInt()))

		for s := 1; s <= stockPerWarehouse; s++ {
			tk.MustExec(`insert into stock (s_i_id, s_w_id, s_quantity,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
				s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) VALUES ` +
				fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
					s, w, rInt(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rInt(), rInt(), rInt(), rStr()))
		}

		for d := 1; d <= districtPerWarehouse; d++ {
			tk.MustExec(`insert into district (d_id, d_w_id, d_name, d_street_1, d_street_2,
				d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) VALUES ` +
				fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v, %v, %v, %d, %d)",
					d, w, rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), "0.1", rInt(), rInt()))
		}
	}

	districts := nWarehouse * districtPerWarehouse
	for i := 0; i < districts; i++ {
		warehouse := (i/districtPerWarehouse)%nWarehouse + 1
		district := i%districtPerWarehouse + 1
		for c := 1; c <= customerPerDistrict; c++ {
			tk.MustExec(`insert into customer (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2,
					c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment,
					c_payment_cnt, c_delivery_cnt, c_data) VALUES ` +
				fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v, %v)",
					c, district, warehouse, rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), rStr(), "'2010-01-01'",
					rStr(), rInt(), "0.1", rInt(), rInt(), rInt(), rInt(), rStr()))
		}

		for o := 1; o <= orderPerDistrict; o++ {
			tk.MustExec(`insert into orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) VALUES ` +
				fmt.Sprintf("(%v, %v, %v, %v, %v, %v, %v, %v)",
					o, district, warehouse, rInt(), "'2010-01-01'", rInt(), rInt(), rInt()))
		}

		for no := 1; no <= newOrderPerDistrict; no++ {
			tk.MustExec(`insert into new_order (no_o_id, no_d_id, no_w_id) VALUES ` +
				fmt.Sprintf("(%v, %v, %v)", 2100+no, district, warehouse))
		}
	}

	tk.MustExec(`insert into prepared.customer select * from normal.customer`)
	tk.MustExec(`insert into prepared.warehouse select * from normal.warehouse`)
	tk.MustExec(`insert into prepared.district select * from normal.district`)
	tk.MustExec(`insert into prepared.orders select * from normal.orders`)
	tk.MustExec(`insert into prepared.new_order select * from normal.new_order`)
	tk.MustExec(`insert into prepared.stock select * from normal.stock`)
}

func rInt() int {
	return rand.Intn(10)
}

func rStr() string {
	return fmt.Sprintf("%v", rand.Intn(10))
}
