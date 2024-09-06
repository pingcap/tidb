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

var (
	tpccWarehouse = `CREATE TABLE IF NOT EXISTS warehouse (
		w_id INT NOT NULL,
		w_name VARCHAR(10),
		w_street_1 VARCHAR(20),
		w_street_2 VARCHAR(20),
		w_city VARCHAR(20),
		w_state CHAR(2),
		w_zip CHAR(9),
		w_tax DECIMAL(4, 4),
		w_ytd DECIMAL(12, 2),
		PRIMARY KEY (w_id) /*T![clustered_index] CLUSTERED */)`
	tpccDistrict = `CREATE TABLE IF NOT EXISTS district (
		d_id INT NOT NULL,
		d_w_id INT NOT NULL,
		d_name VARCHAR(10),
		d_street_1 VARCHAR(20),
		d_street_2 VARCHAR(20),
		d_city VARCHAR(20),
		d_state CHAR(2),
		d_zip CHAR(9),
		d_tax DECIMAL(4, 4),
		d_ytd DECIMAL(12, 2),
		d_next_o_id INT,
		PRIMARY KEY (d_w_id, d_id) /*T![clustered_index] CLUSTERED */)`
	tpccCustomer = `CREATE TABLE IF NOT EXISTS customer (
		c_id INT NOT NULL,
		c_d_id INT NOT NULL,
		c_w_id INT NOT NULL,
		c_first VARCHAR(16),
		c_middle CHAR(2),
		c_last VARCHAR(16),
		c_street_1 VARCHAR(20),
		c_street_2 VARCHAR(20),
		c_city VARCHAR(20),
		c_state CHAR(2),
		c_zip CHAR(9),
		c_phone CHAR(16),
		c_since DATETIME,
		c_credit CHAR(2),
		c_credit_lim DECIMAL(12, 2),
		c_discount DECIMAL(4,4),
		c_balance DECIMAL(12,2),
		c_ytd_payment DECIMAL(12,2),
		c_payment_cnt INT,
		c_delivery_cnt INT,
		c_data VARCHAR(500),
		PRIMARY KEY(c_w_id, c_d_id, c_id) /*T![clustered_index] CLUSTERED */,
		INDEX idx_customer (c_w_id, c_d_id, c_last, c_first))`
)

func TestInstancePlanCacheDMLTPCC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk1.MustExec(`create database tpcc1`)
	tk1.MustExec(`use tpcc1`)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`create database tpcc2`)
	tk2.MustExec(`use tpcc2`)

	tk1.MustExec(tpccWarehouse)
	tk2.MustExec(tpccWarehouse)
	for i := 0; i < 100; i++ {
		sql := `insert into warehouse values (%d, 'name', 'street1', 'street2', 'city', 'st', 'zip', 0.1, 0.1)`
		tk1.MustExec(fmt.Sprintf(sql, i))
		tk2.MustExec(fmt.Sprintf(sql, i))
	}
	tk1.MustExec(tpccDistrict)
	tk2.MustExec(tpccDistrict)
	for i := 0; i < 100; i++ {
		sql := `insert into district values (%d, %d, 'name', 'street1', 'street2', 'city', 'st', 'zip', 0.1, 0.1, 0)`
		tk1.MustExec(fmt.Sprintf(sql, i, i))
		tk2.MustExec(fmt.Sprintf(sql, i, i))
	}
	tk1.MustExec(tpccCustomer)
	tk2.MustExec(tpccCustomer)
	for i := 0; i < 100; i++ {
		sql := `insert into customer values (%d, %d, %d, 'first', 'm', 'last', 'street1', 'street2',
                             'city', 'st', 'zip', 'phone', '2021-01-01', 'CR',
                             0.1, 0.1, 0.1, 0.1, 1, 1, 'data')`
		tk1.MustExec(fmt.Sprintf(sql, i, i, i))
		tk2.MustExec(fmt.Sprintf(sql, i, i, i))
	}
	check := func() {
		tk1.MustQuery("select * from warehouse").Sort().Check(
			tk2.MustQuery("select * from warehouse").Sort().Rows())
		tk1.MustQuery("select * from district").Sort().Check(
			tk2.MustQuery("select * from district").Sort().Rows())
		tk1.MustQuery("select * from customer").Sort().Check(
			tk2.MustQuery("select * from customer").Sort().Rows())
	}
	check()

	paymentUpdateDistrict := func() (prep, set, exec, update string) {
		dID := rand.Intn(100)
		wID := rand.Intn(100)
		amount := rand.Float64()
		prep = `prepare stmt from 'update district set d_ytd = d_ytd + ? where d_w_id = ? and d_id = ?'`
		set = fmt.Sprintf("set @amount = %f, @wID = %d, @dID = %d", amount, wID, dID)
		exec = `execute stmt using @amount, @wID, @dID`
		update = fmt.Sprintf("update district set d_ytd = d_ytd + %f where d_w_id = %d and d_id = %d", amount, wID, dID)
		return
	}

	paymentUpdateWarehouse := func() (prep, set, exec, update string) {
		wID := rand.Intn(100)
		amount := rand.Float64()
		prep = `prepare stmt from 'update warehouse set w_ytd = w_ytd + ? where w_id = ?'`
		set = fmt.Sprintf("set @amount = %f, @wID = %d", amount, wID)
		exec = `execute stmt using @amount, @wID`
		update = fmt.Sprintf("update warehouse set w_ytd = w_ytd + %f where w_id = %d", amount, wID)
		return
	}

	paymentUpdateCustomerWithData := func() (prep, set, exec, update string) {
		cID := rand.Intn(100)
		dID := rand.Intn(100)
		wID := rand.Intn(100)
		amount := rand.Float64()
		data := "data"
		prep = `prepare stmt from 'update customer set c_balance = c_balance - ?,
					c_ytd_payment = c_ytd_payment + ?, c_payment_cnt = c_payment_cnt + 1,
					c_data = ? where c_w_id = ? and c_d_id = ? and c_id = ?'`
		set = fmt.Sprintf("set @amount = %f, @cID = %d, @dID = %d, @wID = %d, @data = '%s'",
			amount, cID, dID, wID, data)
		exec = `execute stmt using @amount, @amount, @data, @wID, @dID, @cID`
		update = fmt.Sprintf(`update customer set c_balance = c_balance - %f,
				c_ytd_payment = c_ytd_payment + %f, c_payment_cnt = c_payment_cnt + 1,
                    c_data = '%s' where c_w_id = %d and c_d_id = %d and c_id = %d`,
			amount, amount, data, wID, dID, cID)
		return
	}

	newOrderUpdateDistrict := func() (prep, set, exec, update string) {
		dID := rand.Intn(100)
		wID := rand.Intn(100)
		prep = `prepare stmt from 'update district set d_next_o_id = d_next_o_id + 1 where d_id = ? and d_w_id = ?'`
		set = fmt.Sprintf("set @dID = %d, @wID = %d", dID, wID)
		exec = `execute stmt using @dID, @wID`
		update = fmt.Sprintf("update district set d_next_o_id = d_next_o_id + 1 "+
			"where d_id = %d and d_w_id = %d", dID, wID)
		return
	}

	deliveryUpdateCustomer := func() (prep, set, exec, update string) {
		cID := rand.Intn(100)
		dID := rand.Intn(100)
		wID := rand.Intn(100)
		amount := rand.Float64()
		prep = `prepare stmt from 'update customer set c_balance = c_balance + ?,
					c_delivery_cnt = c_delivery_cnt + 1 where c_w_id = ? and c_d_id = ? and c_id = ?'`
		set = fmt.Sprintf("set @amount = %f, @cID = %d, @dID = %d, @wID = %d", amount, cID, dID, wID)
		exec = `execute stmt using @amount, @wID, @dID, @cID`
		update = fmt.Sprintf("update customer set c_balance = c_balance + %f, "+
			"c_delivery_cnt = c_delivery_cnt + 1 where c_w_id = %d and c_d_id = %d and c_id = %d",
			amount, wID, dID, cID)
		return
	}

	for i := 0; i < 100; i++ {
		var prep, set, exec, update string
		switch rand.Intn(5) {
		case 0:
			prep, set, exec, update = paymentUpdateDistrict()
		case 1:
			prep, set, exec, update = paymentUpdateWarehouse()
		case 2:
			prep, set, exec, update = paymentUpdateCustomerWithData()
		case 3:
			prep, set, exec, update = newOrderUpdateDistrict()
		case 4:
			prep, set, exec, update = deliveryUpdateCustomer()
		}
		tk1.MustExec(prep)
		tk1.MustExec(set)
		tk1.MustExec(exec)
		tk2.MustExec(update)
		check()
	}
}

func TestInstancePlanCacheDMLBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)

	tk.MustExec(`create table t1 (a int, b int, c int, key(a), key(b, c))`)
	tk.MustExec(`create table t2 (a int, b int, c int, key(a), key(b, c))`)

	randInsert := func() (prep, set, exec, insert string) {
		a := rand.Intn(100)
		b := rand.Intn(100)
		c := rand.Intn(100)
		prep = `prepare stmt from 'insert into t1 values (?, ?, ?)'`
		set = fmt.Sprintf("set @a = %d, @b = %d, @c = %d", a, b, c)
		exec = `execute stmt using @a, @b, @c`
		insert = fmt.Sprintf("insert into t2 values (%d, %d, %d)", a, b, c)
		return
	}
	randDelete := func() (prep, set, exec, delete string) {
		a := rand.Intn(100)
		b := rand.Intn(100)
		c := rand.Intn(100)
		prep = `prepare stmt from 'delete from t1 where a < ? and b < ? or c < ?'`
		set = fmt.Sprintf("set @a = %d, @b = %d, @c = %d", a, b, c)
		exec = `execute stmt using @a, @b, @c`
		delete = fmt.Sprintf("delete from t2 where a < %d and b < %d or c < %d", a, b, c)
		return
	}
	randUpdate := func() (prep, set, exec, update string) {
		a := rand.Intn(100)
		b := rand.Intn(100)
		c := rand.Intn(100)
		prep = `prepare stmt from 'update t1 set a = ? where b = ? or c = ?'`
		set = fmt.Sprintf("set @a = %d, @b = %d, @c = %d", a, b, c)
		exec = `execute stmt using @a, @b, @c`
		update = fmt.Sprintf("update t2 set a = %d where b = %d or c = %d", a, b, c)
		return
	}

	checkResult := func() {
		tk.MustQuery("select * from t1").Sort().Check(
			tk.MustQuery("select * from t2").Sort().Rows())
	}

	for i := 0; i < 50; i++ {
		prep, set, exec, insert := randInsert()
		tk.MustExec(prep)
		tk.MustExec(set)
		tk.MustExec(exec)
		tk.MustExec(insert)
		checkResult()
	}

	for i := 0; i < 100; i++ {
		var prep, set, exec, dml string
		switch rand.Intn(3) {
		case 0:
			prep, set, exec, dml = randInsert()
		case 1:
			prep, set, exec, dml = randDelete()
		case 2:
			prep, set, exec, dml = randUpdate()
		}
		tk.MustExec(prep)
		tk.MustExec(set)
		tk.MustExec(exec)
		tk.MustExec(dml)
		checkResult()
	}
}
