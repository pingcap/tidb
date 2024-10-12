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

package indexadvisor_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/testkit"
	s "github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

func check(ctx context.Context, t *testing.T, tk *testkit.TestKit,
	expected, SQLs string) {
	if ctx == nil {
		ctx = context.Background()
	}
	var sqls []string
	if SQLs != "" {
		sqls = strings.Split(SQLs, ";")
	}
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), sqls, nil)
	if expected == "err" {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)

	if expected == "" {
		require.Len(t, r, 0)
		return
	}

	indexes := make([]string, 0, len(r))
	for _, result := range r {
		indexes = append(indexes, fmt.Sprintf("%v.%v.%v", result.Database, result.Table, strings.Join(result.IndexColumns, "_")))
	}
	sort.Strings(indexes)
	require.Equal(t, expected, strings.Join(indexes, ","))
}

func TestIndexAdvisorInvalidQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	check(nil, t, tk, "err", "xxx")
	check(nil, t, tk, "err", "xxx;select a from t where a=1")
}

func TestIndexAdvisorFrequency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)
	tk.MustExec(`recommend index set max_num_index=1`)

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where a=1", Frequency: 2})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where b=1", Frequency: 1})
	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "test.t.a", "")

	querySet = s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where a=1", Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where b=1", Frequency: 2})
	ctx = context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "test.t.b", "")

	querySet = s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where a=1", Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where b=1", Frequency: 2})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where c=1", Frequency: 100})
	ctx = context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "test.t.c", "")
}

func TestIndexAdvisorBasic1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	check(nil, t, tk, "test.t.a", "select * from t where a=1")
	check(nil, t, tk, "test.t.a,test.t.b", "select * from t where a=1; select * from t where b=1")
	check(nil, t, tk, "test.t.a,test.t.b", "select a from t where a=1; select b from t where b=1")
}

func TestIndexAdvisorBasic2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	sqls := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		tk.MustExec(fmt.Sprintf(`create table t%d (a int, b int, c int)`, i))
		sql := fmt.Sprintf("select * from t%d", i) // useless SQLs
		sqls = append(sqls, sql)
	}
	sqls = append(sqls, "select * from t0 where a=1") // only 1 single useful SQL
	check(nil, t, tk, "test.t0.a", strings.Join(sqls, ";"))
}

func TestIndexAdvisorCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	check(nil, t, tk, "test.t.a_b",
		"with cte as (select * from t where a=1) select * from cte where b=1")
	check(nil, t, tk, "test.t.a_b_c,test.t.c",
		"with cte as (select * from t where a=1) select * from cte where b=1; select * from t where c=1")
}

func TestIndexAdvisorFixControl43817(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int)`)
	tk.MustExec(`create table t2 (a int, b int, c int)`)

	check(nil, t, tk, "err", "select * from t1 where a=(select max(a) from t2)")
	check(nil, t, tk, "err",
		"select * from t1 where a=(select max(a) from t2); select * from t1 where b=1")
	check(nil, t, tk, "err",
		"select * from t1 where a=(select max(a) from t2);select a from t1 where a=1")

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test",
		Text: "select * from t1 where a=(select max(a) from t2)", Frequency: 1})
	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "err", "") // empty query set after filtering invalid queries
	querySet.Add(indexadvisor.Query{SchemaName: "test",
		Text: "select * from t1 where a=(select max(a) from t2); select * from t1 where b=1", Frequency: 1})
	check(ctx, t, tk, "err", "") // empty query set after filtering invalid queries
	querySet.Add(indexadvisor.Query{SchemaName: "test",
		Text: "select a from t1 where a=1", Frequency: 1})
	check(ctx, t, tk, "test.t1.a", "") // invalid queries would be ignored
}

func TestIndexAdvisorView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)
	tk.MustExec("create DEFINER=`root`@`127.0.0.1` view v as select * from t where a=1")

	check(nil, t, tk, "test.t.b", "select * from v where b=1")
	check(nil, t, tk, "test.t.b,test.t.c",
		"select * from v where b=1; select * from t where c=1")
}

func TestIndexAdvisorMassive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`recommend index set max_num_index=3`)

	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf(`create table t%d(c0 int,c1 int,c2 int,c3 int,c4 int,c5 int,c6 int,c7 int)`, i)
		tk.MustExec(sql)
	}
	sqls := make([]string, 0, 10)
	for i := 0; i < 10; i++ {
		sql := fmt.Sprintf("select * from t%d where c%d=1 and c%d=1 and c%d=1",
			rand.Intn(10), rand.Intn(8), rand.Intn(8), rand.Intn(8))
		sqls = append(sqls, sql)
	}
	r, err := indexadvisor.AdviseIndexes(context.Background(), tk.Session(), sqls, nil)
	require.NoError(t, err) // no error and can get some recommendations
	require.Len(t, r, 3)
}

func TestIndexAdvisorIncorrectCurrentDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	tk.MustExec(`use mysql`)
	check(nil, t, tk, "test.t.a", "select * from test.t where a=1")
}

func TestIndexAdvisorPrefix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	check(nil, t, tk, "test.t.a_b",
		"select * from t where a=1;select * from t where a=1 and b=1")
	check(nil, t, tk, "test.t.a_b_c", // a_b_c can cover a_b
		"select * from t where a=1;select * from t where a=1 and b=1; select * from t where a=1 and b=1 and c=1")
}

func TestIndexAdvisorCoveringIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int)`)

	check(nil, t, tk, "test.t.a_b", "select b from t where a=1")
	check(nil, t, tk, "test.t.a_b_c", "select b, c from t where a=1")
	check(nil, t, tk, "test.t.a_d_b", "select b from t where a=1 and d=1")
}

func TestIndexAdvisorExistingIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, index ab (a, b))`)

	check(nil, t, tk, "", "select * from t where a=1") // covered by existing a_b
	check(nil, t, tk, "", "select * from t where a=1; select * from t where a=1 and b=1")
	check(nil, t, tk, "test.t.c",
		"select * from t where a=1; select * from t where a=1 and b=1; select * from t where c=1")
}

func TestIndexAdvisorTPCC(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	// All PKs are removed otherwise can't recommend any index.
	tk.MustExec(`CREATE TABLE IF NOT EXISTS customer (
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
	c_since TIMESTAMP,
	c_credit CHAR(2),
	c_credit_lim DECIMAL(12, 2),
	c_discount DECIMAL(4,4),
	c_balance DECIMAL(12,2),
	c_ytd_payment DECIMAL(12,2),
	c_payment_cnt INT,
	c_delivery_cnt INT,
	c_data VARCHAR(500))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS warehouse (
	w_id INT NOT NULL,
	w_name VARCHAR(10),
	w_street_1 VARCHAR(20),
	w_street_2 VARCHAR(20),
	w_city VARCHAR(20),
	w_state CHAR(2),
	w_zip CHAR(9),
	w_tax DECIMAL(4, 4),
	w_ytd DECIMAL(12, 2))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS stock (
	s_i_id INT NOT NULL,
	s_w_id INT NOT NULL,
	s_quantity INT,
	s_dist_01 CHAR(24),
	s_dist_02 CHAR(24),
	s_dist_03 CHAR(24),
	s_dist_04 CHAR(24),
	s_dist_05 CHAR(24),
	s_dist_06 CHAR(24),
	s_dist_07 CHAR(24),
	s_dist_08 CHAR(24),
	s_dist_09 CHAR(24),
	s_dist_10 CHAR(24),
	s_ytd INT,
	s_order_cnt INT,
	s_remote_cnt INT,
	s_data VARCHAR(50))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS orders (
	o_id INT NOT NULL,
	o_d_id INT NOT NULL,
	o_w_id INT NOT NULL,
	o_c_id INT,
	o_entry_d DATETIME,
	o_carrier_id INT,
	o_ol_cnt INT,
	o_all_local INT)`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS new_order (
	no_o_id INT NOT NULL,
	no_d_id INT NOT NULL,
	no_w_id INT NOT NULL)`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS district (
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
	d_next_o_id INT)`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS item (
	i_id INT NOT NULL,
	i_im_id INT,
	i_name VARCHAR(24),
	i_price DECIMAL(5, 2),
	i_data VARCHAR(50))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS order_line (
		ol_o_id INT NOT NULL,
		ol_d_id INT NOT NULL,
		ol_w_id INT NOT NULL,
		ol_number INT NOT NULL,
		ol_i_id INT NOT NULL,
		ol_supply_w_id INT,
		ol_delivery_d TIMESTAMP,
		ol_quantity INT,
		ol_amount DECIMAL(6, 2),
		ol_dist_info CHAR(24))`)

	q1 := `SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE w_id = 1 AND c_w_id = w_id AND c_d_id = 6 AND c_id = 1309`
	q2 := `SELECT s_i_id, s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE (s_w_id, s_i_id) IN ((1, 54388), (1, 40944), (1, 66045)) FOR UPDATE`
	q3 := `SELECT o_id, o_carrier_id, o_entry_d FROM orders WHERE o_w_id = 4 AND o_d_id = 6 AND o_c_id = 914 ORDER BY o_id DESC LIMIT 1`
	q4 := `SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_id = 1106 FOR UPDATE`
	q5 := `SELECT count(c_id) namecnt FROM customer WHERE c_w_id = 4 AND c_d_id = 6 AND c_last = 'EINGOUGHTPRI'`
	q6 := `SELECT c_id FROM customer WHERE c_w_id = 2 AND c_d_id = 1 AND c_last = "PRESCALLYCALLY" ORDER BY c_first`
	q7 := `SELECT no_o_id FROM new_order WHERE no_w_id = 1 AND no_d_id = 1 ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE`
	q8 := `SELECT d_next_o_id, d_tax FROM district WHERE d_id = 1 AND d_w_id = 3 FOR UPDATE`
	q9 := `SELECT i_price, i_name, i_data, i_id FROM item WHERE i_id IN (81071, 93873, 97661, 2909, 24471, 8669, 40429, 31485, 31064, 20916, 16893, 8283)`
	q10 := `SELECT s_i_id, s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM stock WHERE (s_w_id, s_i_id) IN ((1, 33259),(1, 98411)) FOR UPDATE`
	q11 := `SELECT c_balance, c_first, c_middle, c_id FROM customer WHERE c_w_id = 4 AND c_d_id = 4 AND c_last = 'EINGOUGHTPRI' ORDER BY c_first`
	q12 := `SELECT d_next_o_id FROM district WHERE d_w_id = 4 AND d_id = 5`
	q13 := `SELECT /*+ TIDB_INLJ(order_line,stock) */ COUNT(DISTINCT (s_i_id)) stock_count FROM order_line, stock  WHERE ol_w_id = 4 AND ol_d_id = 5 AND ol_o_id < 3005 AND ol_o_id >= 3005 - 20 AND s_w_id = 4 AND s_i_id = ol_i_id AND s_quantity < 14`
	q14 := `SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = 4 AND ol_d_id = 6 AND ol_o_id = 93`
	q15 := `SELECT c_data FROM customer WHERE c_w_id = 3 AND c_d_id = 9 AND c_id = 640`

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q1, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q2, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q3, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q4, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q5, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q6, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q7, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q8, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q9, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q10, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q11, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q12, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q13, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q14, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q15, Frequency: 1})

	tk.MustExec(`recommend index set max_num_index=3`)
	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), nil, nil)
	require.NoError(t, err)
	require.True(t, len(r) > 0)
}

func TestIndexAdvisorWeb3Bench(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`CREATE TABLE blocks (
                          timestamp bigint(20) DEFAULT NULL,
                          number bigint(20) DEFAULT NULL,
                          hash varchar(66) DEFAULT NULL,
                          parent_hash varchar(66) DEFAULT NULL,
                          nonce varchar(42) DEFAULT NULL,
                          sha3_uncles varchar(66) DEFAULT NULL,
                          logs_bloom text DEFAULT NULL,
                          transactions_root varchar(66) DEFAULT NULL,
                          state_root varchar(66) DEFAULT NULL,
                          receipts_root varchar(66) DEFAULT NULL,
                          miner varchar(42) DEFAULT NULL,
                          difficulty decimal(38,0) DEFAULT NULL,
                          total_difficulty decimal(38,0) DEFAULT NULL,
                          size bigint(20) DEFAULT NULL,
                          extra_data text DEFAULT NULL,
                          gas_limit bigint(20) DEFAULT NULL,
                          gas_used bigint(20) DEFAULT NULL,
                          transaction_count bigint(20) DEFAULT NULL,
                          base_fee_per_gas bigint(20) DEFAULT NULL)`)
	tk.MustExec(`CREATE TABLE transactions (
                                hash varchar(66) DEFAULT NULL,
                                nonce bigint(20) DEFAULT NULL,
                                block_hash varchar(66) DEFAULT NULL,
                                block_number bigint(20) DEFAULT NULL,
                                transaction_index bigint(20) DEFAULT NULL,
                                from_address varchar(42) DEFAULT NULL,
                                to_address varchar(42) DEFAULT NULL,
                                value decimal(38,0) DEFAULT NULL,
                                gas bigint(20) DEFAULT NULL,
                                gas_price bigint(20) DEFAULT NULL,
                                input text DEFAULT NULL,
                                block_timestamp bigint(20) DEFAULT NULL,
                                max_fee_per_gas bigint(20) DEFAULT NULL,
                                max_priority_fee_per_gas bigint(20) DEFAULT NULL,
                                transaction_type bigint(20) DEFAULT NULL)`)
	tk.MustExec(`CREATE TABLE token_transfers (
                                   token_address varchar(42) DEFAULT NULL,
                                   from_address varchar(42) DEFAULT NULL,
                                   to_address varchar(42) DEFAULT NULL,
                                   value varchar(78) DEFAULT NULL COMMENT 'Postgresql use numeric(78), while the max_value of Decimal is decimal(65), thus use string here',
                                   transaction_hash varchar(66) DEFAULT NULL,
                                   log_index bigint(20) DEFAULT NULL,
                                   block_number bigint(20) DEFAULT NULL)`)
	tk.MustExec(`CREATE TABLE receipts (
                            transaction_hash varchar(66) DEFAULT NULL,
                            transaction_index bigint(20) DEFAULT NULL,
                            block_hash varchar(66) DEFAULT NULL,
                            block_number bigint(20) DEFAULT NULL,
                            cumulative_gas_used bigint(20) DEFAULT NULL,
                            gas_used bigint(20) DEFAULT NULL,
                            contract_address varchar(42) DEFAULT NULL,
                            root varchar(66) DEFAULT NULL,
                            status bigint(20) DEFAULT NULL,
                            effective_gas_price bigint(20) DEFAULT NULL)`)
	tk.MustExec(`CREATE TABLE logs (
                        log_index bigint(20) DEFAULT NULL,
                        transaction_hash varchar(66) DEFAULT NULL,
                        transaction_index bigint(20) DEFAULT NULL,
                        block_hash varchar(66) DEFAULT NULL,
                        block_number bigint(20) DEFAULT NULL,
                        address varchar(42) DEFAULT NULL,
                        data text DEFAULT NULL,
                        topics text DEFAULT NULL)`)
	tk.MustExec(`CREATE TABLE contracts (
                             address char(42) DEFAULT NULL,
                             bytecode text DEFAULT NULL,
                             function_sighashes text DEFAULT NULL,
                             is_erc20 tinyint(1) DEFAULT NULL,
                             is_erc721 tinyint(1) DEFAULT NULL,
                             block_number bigint(20) DEFAULT NULL)`)
	tk.MustExec(`CREATE TABLE tokens (
                          address char(42) DEFAULT NULL,
                          symbol text DEFAULT NULL,
                          name text DEFAULT NULL,
                          decimals bigint(20) DEFAULT NULL,
                          total_supply decimal(38,0) DEFAULT NULL,
                          block_number bigint(20) DEFAULT NULL)`)
	tk.MustExec(`CREATE TABLE traces (
                          block_number bigint(20) DEFAULT NULL,
                          transaction_hash varchar(66) DEFAULT NULL,
                          transaction_index bigint(20) DEFAULT NULL,
                          from_address varchar(42) DEFAULT NULL,
                          to_address varchar(42) DEFAULT NULL,
                          value decimal(38,0) DEFAULT NULL,
                          input text DEFAULT NULL,
                          output text DEFAULT NULL,
                          trace_type varchar(16) DEFAULT NULL,
                          call_type varchar(16) DEFAULT NULL,
                          reward_type varchar(16) DEFAULT NULL,
                          gas bigint(20) DEFAULT NULL,
                          gas_used bigint(20) DEFAULT NULL,
                          subtraces bigint(20) DEFAULT NULL,
                          trace_address text DEFAULT NULL,
                          error text DEFAULT NULL,
                          status bigint(20) DEFAULT NULL,
                          trace_id text DEFAULT NULL)`)

	q1 := `Select to_address, from_address from transactions where hash = '0x1f415defb2729863fd8088727900d99b7df6f03d5e22e2105fc984cac3d0fb1c'`
	q2 := `Select * from transactions where to_address in ('0x70f0f4f40fed33420c1e4ceefa1eb482e044ba24',
                                                '0x34662f274a42a17876926bc7b0ba541535e40e5f',
                                               '0x7259c2a51a9b1f7e373dcd00898d26a44ffc2e7c')`
	q3 := `Select *  from transactions
where hash in ('0x1f415defb2729863fd8088727900d99b7df6f03d5e22e2105fc984cac3d0fb1c',
               '0xbeff7a4cf341d10c6293a2ecfb255f39c21836bf8956c6877d0f2486794fd5b8',
               '0x5dee984c63cc26037a81d0f2861565c4e0c21a87ebf165b331faec347d7a76a1',
              '0xc7da1e3391e4b7769fffe8e6afc284175a6cbe5fd9b333d9c0585944a36118dd') and to_address <> from_address`
	q4 := `SELECT * FROM token_transfers WHERE from_address = '0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98' ORDER BY block_number DESC LIMIT 5`
	q5 := `Select count(*) from token_transfers where token_address = '0x7a93f0d9f302c0818022f8dca6ee1eb0f1b50308'`
	q6 := `SELECT * FROM transactions
WHERE from_address = '0x31d118c5f75502b96ca21d3d0d3fb8d7b19fed24' OR to_address = '0x6364989a903f45798c7a292778285a83d0928608'
ORDER BY block_timestamp DESC LIMIT 10`
	q7 := `SELECT count(DISTINCT from_address) FROM transactions`
	q8 := `SELECT
    count(*) as count
FROM (SELECT *
    FROM token_transfers t
    WHERE from_address = '0xfbb1b73c4f0bda4f67dca266ce6ef42f520fbb98'
    UNION ALL
    SELECT t2.*
    FROM token_transfers t2
    INNER JOIN token_transfers t ON t2.from_address = t.to_address
    AND t.value < t2.value
    LIMIT 100) as temp`
	q9 := `SELECT COUNT(DISTINCT block_receipts) as count
FROM (SELECT block_number AS block_receipts
    FROM receipts
    WHERE NOT EXISTS (
    SELECT block_number
    FROM transactions
    WHERE block_number = receipts.block_number)) as temp`

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q1, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q2, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q3, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q4, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q5, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q6, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q7, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q8, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: q9, Frequency: 1})

	tk.MustExec(`recommend index set max_num_index=3`)
	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), nil, nil)
	require.NoError(t, err)
	require.True(t, len(r) > 0)
}

func TestIndexAdvisorRunFor(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int)`)
	tk.MustExec(`create table t2 (a int, b int, c int)`)

	r := tk.MustQuery(`recommend index run for "select * from t1 where a=1"`)
	require.True(t, len(r.Rows()) == 1)
	r = tk.MustQuery(`recommend index run for "select * from t1 where a=1;select * from t2 where b=1"`)
	require.True(t, len(r.Rows()) == 2)
	tk.MustQueryToErr(`recommend index run for ";"`)
	tk.MustQueryToErr(`recommend index run for "xxx"`)
	tk.MustQueryToErr(`recommend index run for ";;;"`)
	tk.MustQueryToErr(`recommend index run for ";;xx;"`)
	r = tk.MustQuery(`recommend index run for ";;select * from t1 where a=1;; ;;  ;"`)
	require.True(t, len(r.Rows()) == 1)
}

func TestIndexAdvisorStorage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d varchar(32))`)
	q := `select index_columns, index_details->'$.Reason' from mysql.index_advisor_results`

	tk.MustQuery(`recommend index run for "select a from t where a=1"`)
	tk.MustQuery(q).Sort().Check(testkit.Rows(
		"a \"Column [a] appear in Equal or Range Predicate clause(s) in query: select `a` from `test` . `t` where `a` = ?\""))

	tk.MustQuery(`recommend index run for "select b from t where b=1"`)
	tk.MustQuery(q).Sort().Check(testkit.Rows(
		"a \"Column [a] appear in Equal or Range Predicate clause(s) in query: select `a` from `test` . `t` where `a` = ?\"",
		"b \"Column [b] appear in Equal or Range Predicate clause(s) in query: select `b` from `test` . `t` where `b` = ?\""))

	tk.MustQuery(`recommend index run for "select d from t where d='x'"`)
	tk.MustQuery(q).Sort().Check(testkit.Rows(
		"a \"Column [a] appear in Equal or Range Predicate clause(s) in query: select `a` from `test` . `t` where `a` = ?\"",
		"b \"Column [b] appear in Equal or Range Predicate clause(s) in query: select `b` from `test` . `t` where `b` = ?\"",
		"d \"Column [d] appear in Equal or Range Predicate clause(s) in query: select `d` from `test` . `t` where `d` = ?\""))

	tk.MustQuery(`recommend index run for "select c, b from t where c=1 and b=1"`)
	tk.MustQuery(q).Sort().Check(testkit.Rows(
		"a \"Column [a] appear in Equal or Range Predicate clause(s) in query: select `a` from `test` . `t` where `a` = ?\"",
		"b \"Column [b] appear in Equal or Range Predicate clause(s) in query: select `b` from `test` . `t` where `b` = ?\"",
		"b,c \"Column [b c] appear in Equal or Range Predicate clause(s) in query: select `c` , `b` from `test` . `t` where `c` = ? and `b` = ?\"",
		"d \"Column [d] appear in Equal or Range Predicate clause(s) in query: select `d` from `test` . `t` where `d` = ?\""))
}
