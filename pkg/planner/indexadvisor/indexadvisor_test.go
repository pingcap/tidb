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

func option(SQLs string) *indexadvisor.Option {
	if SQLs == "" {
		return &indexadvisor.Option{MaxNumIndexes: 3, MaxIndexWidth: 3}
	}
	return &indexadvisor.Option{MaxNumIndexes: 3, MaxIndexWidth: 3, SpecifiedSQLs: strings.Split(SQLs, ";")}
}

func check(ctx context.Context, t *testing.T, tk *testkit.TestKit,
	expected string, opt *indexadvisor.Option) {
	if ctx == nil {
		ctx = context.Background()
	}
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), opt)
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

	check(nil, t, tk, "err", option("xxx"))
	check(nil, t, tk, "err", option("xxx;select a from t where a=1"))
}

func TestIndexAdvisorFrequency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)
	opt := &indexadvisor.Option{
		MaxNumIndexes: 1,
		MaxIndexWidth: 3,
	}

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where a=1", Frequency: 2})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where b=1", Frequency: 1})
	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "test.t.a", opt)

	querySet = s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where a=1", Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where b=1", Frequency: 2})
	ctx = context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "test.t.b", opt)

	querySet = s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where a=1", Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where b=1", Frequency: 2})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: "select * from t where c=1", Frequency: 100})
	ctx = context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	check(ctx, t, tk, "test.t.c", opt)
}

func TestIndexAdvisorBasic1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	check(nil, t, tk, "test.t.a", option("select * from t where a=1"))
	check(nil, t, tk, "test.t.a,test.t.b",
		option("select * from t where a=1; select * from t where b=1"))
	check(nil, t, tk, "test.t.a,test.t.b",
		option("select a from t where a=1; select b from t where b=1"))
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
	check(nil, t, tk, "test.t0.a", option(strings.Join(sqls, ";")))
}

func TestIndexAdvisorCTE(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	check(nil, t, tk, "test.t.a_b",
		option("with cte as (select * from t where a=1) select * from cte where b=1"))
	check(nil, t, tk, "test.t.a_b_c,test.t.c",
		option("with cte as (select * from t where a=1) select * from cte where b=1; select * from t where c=1"))
}

func TestIndexAdvisorFixControl43817(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, c int)`)
	tk.MustExec(`create table t2 (a int, b int, c int)`)

	check(nil, t, tk, "err", option("select * from t1 where a=(select max(a) from t2)"))
	check(nil, t, tk, "err",
		option("select * from t1 where a=(select max(a) from t2); select * from t1 where b=1"))
}

func TestIndexAdvisorView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)
	tk.MustExec("create DEFINER=`root`@`127.0.0.1` view v as select * from t where a=1")

	check(nil, t, tk, "test.t.b", option("select * from v where b=1"))
	check(nil, t, tk, "test.t.b,test.t.c",
		option("select * from v where b=1; select * from t where c=1"))
}

func TestIndexAdvisorMassive(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

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
	r, err := indexadvisor.AdviseIndexes(context.Background(), tk.Session(), option(strings.Join(sqls, ";")))
	require.NoError(t, err) // no error and can get some recommendations
	require.Len(t, r, 3)
}

func TestIndexAdvisorIncorrectCurrentDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	tk.MustExec(`use mysql`)
	check(nil, t, tk, "test.t.a", option("select * from test.t where a=1"))
}

func TestIndexAdvisorPrefix(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int)`)

	check(nil, t, tk, "test.t.a_b",
		option("select * from t where a=1;select * from t where a=1 and b=1"))
	check(nil, t, tk, "test.t.a_b_c", // a_b_c can cover a_b
		option("select * from t where a=1;select * from t where a=1 and b=1; select * from t where a=1 and b=1 and c=1"))
}

func TestIndexAdvisorCoveringIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int)`)

	check(nil, t, tk, "test.t.a_b", option("select b from t where a=1"))
	check(nil, t, tk, "test.t.a_b_c", option("select b, c from t where a=1"))
	check(nil, t, tk, "test.t.a_d_b", option("select b from t where a=1 and d=1"))
}

func TestIndexAdvisorExistingIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, index ab (a, b))`)

	check(nil, t, tk, "", option("select * from t where a=1")) // covered by existing a_b
	check(nil, t, tk, "", option("select * from t where a=1; select * from t where a=1 and b=1"))
	check(nil, t, tk, "test.t.c",
		option("select * from t where a=1; select * from t where a=1 and b=1; select * from t where c=1"))
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

	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), &indexadvisor.Option{
		MaxNumIndexes: 3,
		MaxIndexWidth: 3,
		SpecifiedSQLs: nil,
	})
	require.NoError(t, err)
	require.True(t, len(r) > 0)
}
