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
	tk.MustExec(`
CREATE TABLE lineitem (
    L_ORDERKEY bigint NOT NULL,
    L_PARTKEY bigint NOT NULL,
    L_SUPPKEY bigint NOT NULL,
    L_LINENUMBER bigint NOT NULL,
    L_QUANTITY decimal(15,2) NOT NULL,
    L_EXTENDEDPRICE decimal(15,2) NOT NULL,
    L_DISCOUNT decimal(15,2) NOT NULL,
    L_TAX decimal(15,2) NOT NULL,
    L_RETURNFLAG char(1) NOT NULL,
    L_LINESTATUS char(1) NOT NULL,
    L_SHIPDATE date NOT NULL,
    L_COMMITDATE date NOT NULL,
    L_RECEIPTDATE date NOT NULL,
    L_SHIPINSTRUCT char(25) NOT NULL,
    L_SHIPMODE char(10) NOT NULL,
    L_COMMENT varchar(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
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
	tk.MustExec(`CREATE TABLE customer (
    C_CUSTKEY bigint NOT NULL,
    C_NAME varchar(25) NOT NULL,
    C_ADDRESS varchar(40) NOT NULL,
    C_NATIONKEY bigint NOT NULL,
    C_PHONE char(15) NOT NULL,
    C_ACCTBAL decimal(15,2) NOT NULL,
    C_MKTSEGMENT char(10) NOT NULL,
    C_COMMENT varchar(117) NOT NULL,
    PRIMARY KEY (C_CUSTKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`
CREATE TABLE orders (
    O_ORDERKEY bigint NOT NULL,
    O_CUSTKEY bigint NOT NULL,
    O_ORDERSTATUS char(1) NOT NULL,
    O_TOTALPRICE decimal(15,2) NOT NULL,
    O_ORDERDATE date NOT NULL,
    O_ORDERPRIORITY char(15) NOT NULL,
    O_CLERK char(15) NOT NULL,
    O_SHIPPRIORITY bigint NOT NULL,
    O_COMMENT varchar(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`
CREATE TABLE lineitem (
    L_ORDERKEY bigint NOT NULL,
    L_PARTKEY bigint NOT NULL,
    L_SUPPKEY bigint NOT NULL,
    L_LINENUMBER bigint NOT NULL,
    L_QUANTITY decimal(15,2) NOT NULL,
    L_EXTENDEDPRICE decimal(15,2) NOT NULL,
    L_DISCOUNT decimal(15,2) NOT NULL,
    L_TAX decimal(15,2) NOT NULL,
    L_RETURNFLAG char(1) NOT NULL,
    L_LINESTATUS char(1) NOT NULL,
    L_SHIPDATE date NOT NULL,
    L_COMMITDATE date NOT NULL,
    L_RECEIPTDATE date NOT NULL,
    L_SHIPINSTRUCT char(25) NOT NULL,
    L_SHIPMODE char(10) NOT NULL,
    L_COMMENT varchar(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
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
	tk.MustExec("use olap")
	tk.MustExec(`
CREATE TABLE orders (
    O_ORDERKEY bigint NOT NULL,
    O_CUSTKEY bigint NOT NULL,
    O_ORDERSTATUS char(1) NOT NULL,
    O_TOTALPRICE decimal(15,2) NOT NULL,
    O_ORDERDATE date NOT NULL,
    O_ORDERPRIORITY char(15) NOT NULL,
    O_CLERK char(15) NOT NULL,
    O_SHIPPRIORITY bigint NOT NULL,
    O_COMMENT varchar(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`
CREATE TABLE lineitem (
    L_ORDERKEY bigint NOT NULL,
    L_PARTKEY bigint NOT NULL,
    L_SUPPKEY bigint NOT NULL,
    L_LINENUMBER bigint NOT NULL,
    L_QUANTITY decimal(15,2) NOT NULL,
    L_EXTENDEDPRICE decimal(15,2) NOT NULL,
    L_DISCOUNT decimal(15,2) NOT NULL,
    L_TAX decimal(15,2) NOT NULL,
    L_RETURNFLAG char(1) NOT NULL,
    L_LINESTATUS char(1) NOT NULL,
    L_SHIPDATE date NOT NULL,
    L_COMMITDATE date NOT NULL,
    L_RECEIPTDATE date NOT NULL,
    L_SHIPINSTRUCT char(25) NOT NULL,
    L_SHIPMODE char(10) NOT NULL,
    L_COMMENT varchar(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
	testkit.LoadTableStats("lineitem_stats.json", dom)
	testkit.LoadTableStats("orders_stats.json", dom)
	testkit.SetTiFlashReplica(t, dom, "olap", "orders")
	testkit.SetTiFlashReplica(t, dom, "olap", "lineitem")
	briefFormat := `explain format='brief' `
	q4 := `select
        o_orderpriority,
        count(*) as order_count
from
        orders
where
        o_orderdate >= '1995-01-01'
        and o_orderdate < date_add('1995-01-01', interval '3' month)
        and exists (
                select    *
                from
                        lineitem
                where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate  )
group by  o_orderpriority
order by       o_orderpriority`
	tk.MustQuery(briefFormat + q4).Check(testkit.Rows(
		"Sort 1.00 root  olap.orders.o_orderpriority",
		"└─Projection 1.00 root  olap.orders.o_orderpriority, Column#26",
		"  └─HashAgg 1.00 root  group by:olap.orders.o_orderpriority, funcs:count(1)->Column#26, funcs:firstrow(olap.orders.o_orderpriority)->olap.orders.o_orderpriority",
		"    └─IndexJoin 45161741.07 root  semi join, inner:TableReader, left side:TableReader, outer key:olap.orders.o_orderkey, inner key:olap.lineitem.l_orderkey, equal cond:eq(olap.orders.o_orderkey, olap.lineitem.l_orderkey)",
		"      ├─TableReader(Build) 56452176.33 root  MppVersion: 3, data:ExchangeSender",
		"      │ └─ExchangeSender 56452176.33 mpp[tiflash]  ExchangeType: PassThrough",
		"      │   └─TableFullScan 56452176.33 mpp[tiflash] table:orders pushed down filter:ge(olap.orders.o_orderdate, 1995-01-01 00:00:00.000000), lt(olap.orders.o_orderdate, 1995-04-01 00:00:00.000000), keep order:false",
		"      └─TableReader(Probe) 45161741.07 root  data:Selection",
		"        └─Selection 45161741.07 cop[tikv]  lt(olap.lineitem.l_commitdate, olap.lineitem.l_receiptdate)",
		"          └─TableRangeScan 56452176.33 cop[tikv] table:lineitem range: decided by [eq(olap.lineitem.l_orderkey, olap.orders.o_orderkey)], keep order:false",
	))
	checkCost(t, tk, q4)
	// https://github.com/pingcap/tidb/issues/60991
	tk.MustExec(`set @@session.tidb_enforce_mpp=1;`)
	tk.MustQuery(briefFormat + q4).Check(testkit.Rows("Sort 1.00 root  olap.orders.o_orderpriority",
		"└─TableReader 1.00 root  MppVersion: 3, data:ExchangeSender",
		"  └─ExchangeSender 1.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    └─Projection 1.00 mpp[tiflash]  olap.orders.o_orderpriority, Column#26",
		"      └─Projection 1.00 mpp[tiflash]  Column#26, olap.orders.o_orderpriority",
		"        └─HashAgg 1.00 mpp[tiflash]  group by:olap.orders.o_orderpriority, funcs:sum(Column#31)->Column#26, funcs:firstrow(olap.orders.o_orderpriority)->olap.orders.o_orderpriority",
		"          └─ExchangeReceiver 1.00 mpp[tiflash]  ",
		"            └─ExchangeSender 1.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: olap.orders.o_orderpriority, collate: utf8mb4_bin]",
		"              └─HashAgg 1.00 mpp[tiflash]  group by:olap.orders.o_orderpriority, funcs:count(1)->Column#31",
		"                └─Projection 45161741.07 mpp[tiflash]  olap.orders.o_orderpriority, olap.orders.o_orderkey",
		"                  └─HashJoin 45161741.07 mpp[tiflash]  semi join, left side:ExchangeReceiver, equal:[eq(olap.orders.o_orderkey, olap.lineitem.l_orderkey)]",
		"                    ├─ExchangeReceiver(Build) 56452176.33 mpp[tiflash]  ",
		"                    │ └─ExchangeSender 56452176.33 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: olap.orders.o_orderkey, collate: binary]",
		"                    │   └─TableFullScan 56452176.33 mpp[tiflash] table:orders pushed down filter:ge(olap.orders.o_orderdate, 1995-01-01 00:00:00.000000), lt(olap.orders.o_orderdate, 1995-04-01 00:00:00.000000), keep order:false",
		"                    └─ExchangeReceiver(Probe) 4799991767.20 mpp[tiflash]  ",
		"                      └─ExchangeSender 4799991767.20 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: olap.lineitem.l_orderkey, collate: binary]",
		"                        └─Projection 4799991767.20 mpp[tiflash]  olap.lineitem.l_orderkey",
		"                          └─Selection 4799991767.20 mpp[tiflash]  lt(olap.lineitem.l_commitdate, olap.lineitem.l_receiptdate)",
		"                            └─TableFullScan 5999989709.00 mpp[tiflash] table:lineitem pushed down filter:empty, keep order:false"))
	checkCost(t, tk, q4)
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
	tk.MustExec(`
CREATE TABLE lineitem (
    L_ORDERKEY bigint NOT NULL,
    L_PARTKEY bigint NOT NULL,
    L_SUPPKEY bigint NOT NULL,
    L_LINENUMBER bigint NOT NULL,
    L_QUANTITY decimal(15,2) NOT NULL,
    L_EXTENDEDPRICE decimal(15,2) NOT NULL,
    L_DISCOUNT decimal(15,2) NOT NULL,
    L_TAX decimal(15,2) NOT NULL,
    L_RETURNFLAG char(1) NOT NULL,
    L_LINESTATUS char(1) NOT NULL,
    L_SHIPDATE date NOT NULL,
    L_COMMITDATE date NOT NULL,
    L_RECEIPTDATE date NOT NULL,
    L_SHIPINSTRUCT char(25) NOT NULL,
    L_SHIPMODE char(10) NOT NULL,
    L_COMMENT varchar(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
	tk.MustExec(`CREATE TABLE nation (
  N_NATIONKEY bigint NOT NULL,
  N_NAME char(25) NOT NULL,
  N_REGIONKEY bigint NOT NULL,
  N_COMMENT varchar(152) DEFAULT NULL,
  PRIMARY KEY (N_NATIONKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`
CREATE TABLE orders (
    O_ORDERKEY bigint NOT NULL,
    O_CUSTKEY bigint NOT NULL,
    O_ORDERSTATUS char(1) NOT NULL,
    O_TOTALPRICE decimal(15,2) NOT NULL,
    O_ORDERDATE date NOT NULL,
    O_ORDERPRIORITY char(15) NOT NULL,
    O_CLERK char(15) NOT NULL,
    O_SHIPPRIORITY bigint NOT NULL,
    O_COMMENT varchar(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`CREATE TABLE part (
  P_PARTKEY bigint NOT NULL,
  P_NAME varchar(55) NOT NULL,
  P_MFGR char(25) NOT NULL,
  P_BRAND char(10) NOT NULL,
  P_TYPE varchar(25) NOT NULL,
  P_SIZE bigint NOT NULL,
  P_CONTAINER char(10) NOT NULL,
  P_RETAILPRICE decimal(15,2) NOT NULL,
  P_COMMENT varchar(23) NOT NULL,
  PRIMARY KEY (P_PARTKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`CREATE TABLE partsupp (
  PS_PARTKEY bigint NOT NULL,
  PS_SUPPKEY bigint NOT NULL,
  PS_AVAILQTY bigint NOT NULL,
  PS_SUPPLYCOST decimal(15,2) NOT NULL,
  PS_COMMENT varchar(199) NOT NULL,
  PRIMARY KEY (PS_PARTKEY,PS_SUPPKEY) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`CREATE TABLE supplier (
  S_SUPPKEY bigint NOT NULL,
  S_NAME char(25) NOT NULL,
  S_ADDRESS varchar(40) NOT NULL,
  S_NATIONKEY bigint NOT NULL,
  S_PHONE char(15) NOT NULL,
  S_ACCTBAL decimal(15,2) NOT NULL,
  S_COMMENT varchar(101) NOT NULL,
  PRIMARY KEY (S_SUPPKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
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
	tk.MustExec(`CREATE TABLE customer (
    C_CUSTKEY bigint NOT NULL,
    C_NAME varchar(25) NOT NULL,
    C_ADDRESS varchar(40) NOT NULL,
    C_NATIONKEY bigint NOT NULL,
    C_PHONE char(15) NOT NULL,
    C_ACCTBAL decimal(15,2) NOT NULL,
    C_MKTSEGMENT char(10) NOT NULL,
    C_COMMENT varchar(117) NOT NULL,
    PRIMARY KEY (C_CUSTKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`
CREATE TABLE orders (
    O_ORDERKEY bigint NOT NULL,
    O_CUSTKEY bigint NOT NULL,
    O_ORDERSTATUS char(1) NOT NULL,
    O_TOTALPRICE decimal(15,2) NOT NULL,
    O_ORDERDATE date NOT NULL,
    O_ORDERPRIORITY char(15) NOT NULL,
    O_CLERK char(15) NOT NULL,
    O_SHIPPRIORITY bigint NOT NULL,
    O_COMMENT varchar(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
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
	tk.MustExec(`CREATE TABLE customer (
    C_CUSTKEY bigint NOT NULL,
    C_NAME varchar(25) NOT NULL,
    C_ADDRESS varchar(40) NOT NULL,
    C_NATIONKEY bigint NOT NULL,
    C_PHONE char(15) NOT NULL,
    C_ACCTBAL decimal(15,2) NOT NULL,
    C_MKTSEGMENT char(10) NOT NULL,
    C_COMMENT varchar(117) NOT NULL,
    PRIMARY KEY (C_CUSTKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`
CREATE TABLE orders (
    O_ORDERKEY bigint NOT NULL,
    O_CUSTKEY bigint NOT NULL,
    O_ORDERSTATUS char(1) NOT NULL,
    O_TOTALPRICE decimal(15,2) NOT NULL,
    O_ORDERDATE date NOT NULL,
    O_ORDERPRIORITY char(15) NOT NULL,
    O_CLERK char(15) NOT NULL,
    O_SHIPPRIORITY bigint NOT NULL,
    O_COMMENT varchar(79) NOT NULL,
    PRIMARY KEY (O_ORDERKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`
CREATE TABLE lineitem (
    L_ORDERKEY bigint NOT NULL,
    L_PARTKEY bigint NOT NULL,
    L_SUPPKEY bigint NOT NULL,
    L_LINENUMBER bigint NOT NULL,
    L_QUANTITY decimal(15,2) NOT NULL,
    L_EXTENDEDPRICE decimal(15,2) NOT NULL,
    L_DISCOUNT decimal(15,2) NOT NULL,
    L_TAX decimal(15,2) NOT NULL,
    L_RETURNFLAG char(1) NOT NULL,
    L_LINESTATUS char(1) NOT NULL,
    L_SHIPDATE date NOT NULL,
    L_COMMITDATE date NOT NULL,
    L_RECEIPTDATE date NOT NULL,
    L_SHIPINSTRUCT char(25) NOT NULL,
    L_SHIPMODE char(10) NOT NULL,
    L_COMMENT varchar(44) NOT NULL,
    PRIMARY KEY (L_ORDERKEY, L_LINENUMBER) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
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
