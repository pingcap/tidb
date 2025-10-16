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
	"fmt"
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

// BenchmarkWindowFunctions tests parsing of window function queries
func BenchmarkWindowFunctions(b *testing.B) {
	var queries = []string{
		`SELECT
			id, name, salary,
			ROW_NUMBER() OVER (ORDER BY salary DESC) as rn,
			RANK() OVER (ORDER BY salary DESC) as rnk,
			DENSE_RANK() OVER (ORDER BY salary DESC) as dense_rnk,
			LAG(salary, 1) OVER (ORDER BY salary) as prev_salary,
			LEAD(salary, 1) OVER (ORDER BY salary) as next_salary,
			SUM(salary) OVER (PARTITION BY department ORDER BY salary ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_sum,
			AVG(salary) OVER (PARTITION BY department ORDER BY hire_date RANGE BETWEEN INTERVAL '1' YEAR PRECEDING AND CURRENT ROW) as yearly_avg
		FROM employees`,
		`SELECT
			department,
			employee_id,
			salary,
			NTILE(4) OVER (PARTITION BY department ORDER BY salary) as quartile,
			PERCENT_RANK() OVER (PARTITION BY department ORDER BY salary) as pct_rank,
			CUME_DIST() OVER (PARTITION BY department ORDER BY salary) as cum_dist,
			FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary ROWS UNBOUNDED PRECEDING) as min_sal,
			LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY salary ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as max_sal
		FROM employees
		WHERE hire_date > '2020-01-01'`,
	}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range queries {
			_, _, err := parser.Parse(sql, "", "")
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
}

// BenchmarkCTERecursive tests parsing of Common Table Expressions and recursive queries
func BenchmarkCTERecursive(b *testing.B) {
	var queries = []string{
		`WITH RECURSIVE employee_hierarchy AS (
			SELECT id, name, manager_id, 1 as level
			FROM employees
			WHERE manager_id IS NULL
			UNION ALL
			SELECT e.id, e.name, e.manager_id, eh.level + 1
			FROM employees e
			INNER JOIN employee_hierarchy eh ON e.manager_id = eh.id
			WHERE eh.level < 10
		)
		SELECT * FROM employee_hierarchy ORDER BY level, name`,
		`WITH
		sales_summary AS (
			SELECT
				region,
				SUM(amount) as total_sales,
				COUNT(*) as transaction_count
			FROM sales
			WHERE sale_date >= '2023-01-01'
			GROUP BY region
		),
		top_regions AS (
			SELECT region, total_sales
			FROM sales_summary
			WHERE total_sales > (SELECT AVG(total_sales) FROM sales_summary)
		),
		regional_details AS (
			SELECT
				s.region,
				s.customer_id,
				s.amount,
				ss.total_sales,
				ss.transaction_count
			FROM sales s
			JOIN sales_summary ss ON s.region = ss.region
			JOIN top_regions tr ON s.region = tr.region
		)
		SELECT rd.region, COUNT(DISTINCT rd.customer_id) as unique_customers,
			AVG(rd.amount) as avg_transaction,
			MAX(rd.total_sales) as region_total
		FROM regional_details rd
		GROUP BY rd.region
		HAVING COUNT(DISTINCT rd.customer_id) > 100`,
	}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range queries {
			_, _, err := parser.Parse(sql, "", "")
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
}

// BenchmarkLargeJoins tests parsing of queries with many table joins
func BenchmarkLargeJoins(b *testing.B) {
	sql := `SELECT
		u.user_id, u.username, u.email,
		p.profile_id, p.first_name, p.last_name,
		a.address_id, a.street, a.city, a.country,
		o.order_id, o.order_date, o.total_amount,
		oi.item_id, oi.quantity, oi.unit_price,
		prod.product_id, prod.product_name, prod.category,
		cat.category_id, cat.category_name,
		inv.inventory_id, inv.stock_quantity,
		sup.supplier_id, sup.supplier_name, sup.contact_email,
		ship.shipment_id, ship.tracking_number, ship.ship_date,
		pay.payment_id, pay.payment_method, pay.payment_status,
		disc.discount_id, disc.discount_code, disc.discount_amount,
		rev.review_id, rev.rating, rev.review_text,
		ret.return_id, ret.return_date, ret.return_reason
	FROM users u
	INNER JOIN profiles p ON u.user_id = p.user_id
	LEFT JOIN addresses a ON u.user_id = a.user_id
	INNER JOIN orders o ON u.user_id = o.user_id
	INNER JOIN order_items oi ON o.order_id = oi.order_id
	INNER JOIN products prod ON oi.product_id = prod.product_id
	INNER JOIN categories cat ON prod.category_id = cat.category_id
	LEFT JOIN inventory inv ON prod.product_id = inv.product_id
	LEFT JOIN suppliers sup ON prod.supplier_id = sup.supplier_id
	LEFT JOIN shipments ship ON o.order_id = ship.order_id
	LEFT JOIN payments pay ON o.order_id = pay.order_id
	LEFT JOIN discounts disc ON o.discount_id = disc.discount_id
	LEFT JOIN reviews rev ON prod.product_id = rev.product_id AND u.user_id = rev.user_id
	LEFT JOIN returns ret ON oi.item_id = ret.item_id
	WHERE o.order_date >= '2023-01-01'
		AND o.order_date < '2024-01-01'
		AND u.active = 1
		AND prod.active = 1
	ORDER BY o.order_date DESC, u.username`

	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := parser.Parse(sql, "", "")
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
}

// BenchmarkBatchInsertLarge tests parsing of large batch INSERT statements
func BenchmarkBatchInsertLarge(b *testing.B) {
	// Generate a large INSERT statement with 1000 rows
	sql := "INSERT INTO user_events (user_id, event_type, event_data, created_at) VALUES "
	values := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = "(12345, 'click', '{\"page\": \"home\", \"element\": \"button\"}', '2023-10-15 10:30:00')"
	}
	sql += values[0]
	for i := 1; i < len(values); i++ {
		sql += ", " + values[i]
	}

	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := parser.Parse(sql, "", "")
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
}

// BenchmarkComplexSubqueries tests parsing of deeply nested subqueries
func BenchmarkComplexSubqueries(b *testing.B) {
	var queries = []string{
		`SELECT
			customer_id,
			customer_name,
			(SELECT COUNT(*)
			 FROM orders o1
			 WHERE o1.customer_id = c.customer_id
			   AND o1.order_date > (
				   SELECT AVG(order_date)
				   FROM orders o2
				   WHERE o2.customer_id = c.customer_id
				     AND EXISTS (
					     SELECT 1
					     FROM order_items oi
					     WHERE oi.order_id = o2.order_id
					       AND oi.product_id IN (
						       SELECT p.product_id
						       FROM products p
						       WHERE p.category_id = (
							       SELECT category_id
							       FROM categories
							       WHERE category_name = 'Electronics'
						       )
					       )
				     )
			   )
			) as recent_orders_count,
			(SELECT SUM(total_amount)
			 FROM orders o3
			 WHERE o3.customer_id = c.customer_id
			   AND o3.order_id NOT IN (
				   SELECT DISTINCT r.order_id
				   FROM returns r
				   WHERE r.customer_id = c.customer_id
			   )
			) as net_revenue
		FROM customers c
		WHERE EXISTS (
			SELECT 1
			FROM orders o
			WHERE o.customer_id = c.customer_id
			  AND o.order_date >= (
				  SELECT DATE_SUB(MAX(order_date), INTERVAL 1 YEAR)
				  FROM orders
			  )
		)`,
		`SELECT
			product_id,
			product_name,
			(SELECT category_name FROM categories WHERE category_id = p.category_id) as category,
			CASE
				WHEN price > (SELECT AVG(price) FROM products WHERE category_id = p.category_id)
				THEN 'Above Average'
				WHEN price < (SELECT AVG(price) * 0.8 FROM products WHERE category_id = p.category_id)
				THEN 'Below Average'
				ELSE 'Average'
			END as price_tier
		FROM products p
		WHERE p.product_id IN (
			SELECT oi.product_id
			FROM order_items oi
			WHERE oi.order_id IN (
				SELECT o.order_id
				FROM orders o
				WHERE o.customer_id IN (
					SELECT c.customer_id
					FROM customers c
					WHERE c.customer_segment = (
						SELECT segment_name
						FROM customer_segments
						WHERE segment_id = 1
					)
				)
			)
			GROUP BY oi.product_id
			HAVING SUM(oi.quantity) > (
				SELECT AVG(total_qty)
				FROM (
					SELECT SUM(quantity) as total_qty
					FROM order_items
					GROUP BY product_id
				) as qty_stats
			)
		)`,
	}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range queries {
			_, _, err := parser.Parse(sql, "", "")
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
}

// BenchmarkLargeInClause tests parsing of queries with large IN clauses
func BenchmarkLargeInClause(b *testing.B) {
	// Generate a query with IN clause containing 1000 elements
	sql := "SELECT * FROM products WHERE product_id IN ("
	values := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = fmt.Sprintf("%d", i+1)
	}
	sql += values[0]
	for i := 1; i < len(values); i++ {
		sql += ", " + values[i]
	}
	sql += ") AND category_id IN ("
	catValues := make([]string, 50)
	for i := 0; i < 50; i++ {
		catValues[i] = fmt.Sprintf("%d", i+1)
	}
	sql += catValues[0]
	for i := 1; i < len(catValues); i++ {
		sql += ", " + catValues[i]
	}
	sql += ") ORDER BY product_name LIMIT 100"

	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := parser.Parse(sql, "", "")
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
}

// BenchmarkComplexGroupBy tests parsing of complex GROUP BY queries with aggregations
func BenchmarkComplexGroupBy(b *testing.B) {
	var queries = []string{
		`SELECT
			YEAR(order_date) as order_year,
			MONTH(order_date) as order_month,
			customer_segment,
			product_category,
			region,
			COUNT(DISTINCT customer_id) as unique_customers,
			COUNT(DISTINCT order_id) as total_orders,
			SUM(total_amount) as total_revenue,
			AVG(total_amount) as avg_order_value,
			MAX(total_amount) as max_order_value,
			MIN(total_amount) as min_order_value,
			STDDEV(total_amount) as stddev_order_value,
			SUM(CASE WHEN payment_method = 'credit_card' THEN total_amount ELSE 0 END) as credit_card_revenue,
			SUM(CASE WHEN payment_method = 'paypal' THEN total_amount ELSE 0 END) as paypal_revenue,
			SUM(CASE WHEN payment_method = 'bank_transfer' THEN total_amount ELSE 0 END) as bank_transfer_revenue,
			COUNT(CASE WHEN order_status = 'completed' THEN 1 END) as completed_orders,
			COUNT(CASE WHEN order_status = 'cancelled' THEN 1 END) as cancelled_orders,
			SUM(CASE WHEN discount_amount > 0 THEN 1 ELSE 0 END) as discounted_orders
		FROM orders o
		JOIN customers c ON o.customer_id = c.customer_id
		JOIN order_items oi ON o.order_id = oi.order_id
		JOIN products p ON oi.product_id = p.product_id
		WHERE order_date >= '2022-01-01'
		  AND order_date < '2024-01-01'
		GROUP BY
			YEAR(order_date),
			MONTH(order_date),
			customer_segment,
			product_category,
			region
		WITH ROLLUP
		HAVING total_revenue > 1000
		ORDER BY order_year DESC, order_month DESC, total_revenue DESC`,
		`SELECT
			supplier_id,
			supplier_name,
			COUNT(*) as product_count,
			AVG(price) as avg_product_price,
			SUM(CASE WHEN stock_quantity > 0 THEN 1 ELSE 0 END) as in_stock_products,
			SUM(CASE WHEN stock_quantity = 0 THEN 1 ELSE 0 END) as out_of_stock_products,
			GROUP_CONCAT(DISTINCT category_name ORDER BY category_name SEPARATOR ', ') as categories,
			MAX(last_updated) as last_product_update,
			SUM(price * stock_quantity) as total_inventory_value
		FROM products p
		JOIN suppliers s ON p.supplier_id = s.supplier_id
		JOIN categories c ON p.category_id = c.category_id
		WHERE p.active = 1
		GROUP BY supplier_id, supplier_name
		HAVING product_count >= 5
		   AND avg_product_price > (
			   SELECT AVG(price)
			   FROM products
			   WHERE active = 1
		   )
		ORDER BY total_inventory_value DESC, product_count DESC
		LIMIT 50`,
	}
	parser := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sql := range queries {
			_, _, err := parser.Parse(sql, "", "")
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.ReportAllocs()
}
