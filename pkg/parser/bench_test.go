// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"testing"
)

func BenchmarkSysbenchSelect(b *testing.B) {
	parser := New()
	sql := "SELECT pad FROM sbtest1 WHERE id=1;"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := parser.Parse(sql, "", "")
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
}

func BenchmarkParseComplex(b *testing.B) {
	var table = []string{
		`SELECT DISTINCT ca.l9_convergence_code AS atb2, cu.cust_sub_type AS account_type, cst.description AS account_type_desc, ss.prim_resource_val AS msisdn, ca.ban AS ban_key, To_char(mo.memo_date, 'YYYYMMDD') AS memo_date, cu.l9_identification AS thai_id, ss.subscriber_no AS subs_key, ss.dealer_code AS shop_code, cd.description AS shop_name, mot.short_desc, Regexp_substr(mo.attr1value, '[^ ;]+', 1, 3) staff_id, mo.operator_id AS user_id, mo.memo_system_text, co2.soc_name AS first_socname, co3.soc_name AS previous_socname, co.soc_name AS current_socname, Regexp_substr(mo.attr1value, '[^ ; ]+', 1, 1) NAME, co.soc_description AS current_pp_desc, co3.soc_description AS prev_pp_desc, co.soc_cd AS soc_cd, ( SELECT Sum(br.amount) FROM bl1_rc_rates BR, customer CU, subscriber SS WHERE br.service_receiver_id = ss.subscriber_no AND br.receiver_customer = ss.customer_id AND br.effective_date <= br.expiration_date AND (( ss. sub_status <> 'C' AND ss. sub_status <> 'T' AND br.expiration_date IS NULL) OR ( ss. sub_status = 'C' AND br.expiration_date LIKE ss.effective_date)) AND br.pp_ind = 'Y' AND br.cycle_code = cu.bill_cycle) AS pp_rate, cu.bill_cycle AS cycle_code, To_char(Nvl(ss.l9_tmv_act_date, ss.init_act_date),'YYYYMMDD') AS activated_date, To_char(cd.effective_date, 'YYYYMMDD') AS shop_effective_date, cd.expiration_date AS shop_expired_date, ca.l9_company_code AS company_code FROM service_details S, product CO, csm_pay_channel CPC, account CA, subscriber SS, customer CU, customer_sub_type CST, csm_dealer CD, service_details S2, product CO2, service_details S3, product CO3, memo MO , memo_type MOT, logical_date LO, charge_details CHD WHERE ss.subscriber_no = chd.agreement_no AND cpc.pym_channel_no = chd.target_pcn AND chd.chg_split_type = 'DR' AND chd.expiration_date IS NULL AND s.soc = co.soc_cd AND co.soc_type = 'P' AND s.agreement_no = ss.subscriber_no AND ss.prim_resource_tp = 'C' AND cpc.payment_category = 'POST' AND ca.ban = cpc.ban AND ( ca.l9_company_code = 'RF' OR ca.l9_company_code = 'RM' OR ca.l9_company_code = 'TM') AND ss.customer_id = cu.customer_id AND cu.cust_sub_type = cst.cust_sub_type AND cu.customer_type = cst.customer_type AND ss.dealer_code = cd.dealer AND s2.effective_date= ( SELECT Max(sa1.effective_date) FROM service_details SA1, product o1 WHERE sa1.agreement_no = ss.subscriber_no AND co.soc_cd = sa1.soc AND co.soc_type = 'P' ) AND s2.agreement_no = s.agreement_no AND s2.soc = co2.soc_cd AND co2.soc_type = 'P' AND s2.effective_date = ( SELECT Min(sa1.effective_date) FROM service_details SA1, product o1 WHERE sa1.agreement_no = ss.subscriber_no AND co2.soc_cd = sa1.soc AND co.soc_type = 'P' ) AND s3.agreement_no = s.agreement_no AND s3.soc = co3.soc_cd AND co3.soc_type = 'P' AND s3.effective_date = ( SELECT Max(sa1.effective_date) FROM service_details SA1, a product o1 WHERE sa1.agreement_no = ss.subscriber_no AND sa1.effective_date < ( SELECT Max(sa1.effective_date) FROM service_details SA1, product o1 WHERE sa1.agreement_no = ss.subscriber_no AND co3.soc_cd = sa1.soc AND co3.soc_type = 'P' ) AND co3.soc_cd = sa1.soc AND o1.soc_type = 'P' ) AND mo.entity_id = ss.subscriber_no AND mo.entity_type_id = 6 AND mo.memo_type_id = mot.memo_type_id AND Trunc(mo.sys_creation_date) = ( SELECT Trunc(lo.logical_date - 1) FROM lo) trunc(lo.logical_date - 1) AND lo.expiration_date IS NULL AND lo.logical_date_type = 'B' AND lo.expiration_date IS NULL AND ( mot.short_desc = 'BCN' OR mot.short_desc = 'BCNM' )`}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range table {
			_, _, err := parser.Parse(v, "", "")
			if err != nil {
				b.Failed()
			}
		}
	}
	b.ReportAllocs()
}

func BenchmarkParseSimple(b *testing.B) {
	var table = []string{
		"insert into t values (1), (2), (3)",
		"insert into t values (4), (5), (6), (7)",
		"select c from t where c > 2",
	}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range table {
			_, _, err := parser.Parse(v, "", "")
			if err != nil {
				b.Failed()
			}
		}
	}
	b.ReportAllocs()
}
