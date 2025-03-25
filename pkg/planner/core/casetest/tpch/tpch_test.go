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
)

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
	for i := 0; i < len(input); i++ {
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
	for i := 0; i < len(input); i++ {
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
	tk.MustQuery(`explain format="brief" select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 314
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100;`).Check(testkit.Rows(
		"Projection 100.00 root  test.customer.c_name, test.customer.c_custkey, test.orders.o_orderkey, test.orders.o_orderdate, test.orders.o_totalprice, Column#52",
		"└─TopN 100.00 root  test.orders.o_totalprice:desc, test.orders.o_orderdate, offset:0, count:100",
		"  └─TableReader 100.00 root  MppVersion: 3, data:ExchangeSender",
		"    └─ExchangeSender 100.00 mpp[tiflash]  ExchangeType: PassThrough",
		"      └─TopN 100.00 mpp[tiflash]  test.orders.o_totalprice:desc, test.orders.o_orderdate, offset:0, count:100",
		"        └─Projection 8000.00 mpp[tiflash]  Column#52, test.customer.c_custkey, test.customer.c_name, test.orders.o_orderkey, test.orders.o_totalprice, test.orders.o_orderdate",
		"          └─HashAgg 8000.00 mpp[tiflash]  group by:test.customer.c_custkey, test.customer.c_name, test.orders.o_orderdate, test.orders.o_orderkey, test.orders.o_totalprice, funcs:sum(Column#85)->Column#52, funcs:firstrow(test.customer.c_custkey)->test.customer.c_custkey, funcs:firstrow(test.customer.c_name)->test.customer.c_name, funcs:firstrow(test.orders.o_orderkey)->test.orders.o_orderkey, funcs:firstrow(test.orders.o_totalprice)->test.orders.o_totalprice, funcs:firstrow(test.orders.o_orderdate)->test.orders.o_orderdate",
		"            └─ExchangeReceiver 8000.00 mpp[tiflash]  ",
		"              └─ExchangeSender 8000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.customer.c_name, collate: utf8mb4_bin], [name: test.customer.c_custkey, collate: binary], [name: test.orders.o_orderkey, collate: binary], [name: test.orders.o_orderdate, collate: binary], [name: test.orders.o_totalprice, collate: binary]",
		"                └─HashAgg 8000.00 mpp[tiflash]  group by:test.customer.c_custkey, test.customer.c_name, test.orders.o_orderdate, test.orders.o_orderkey, test.orders.o_totalprice, funcs:sum(test.lineitem.l_quantity)->Column#85",
		"                  └─Projection 12500.00 mpp[tiflash]  test.customer.c_custkey, test.customer.c_name, test.orders.o_orderkey, test.orders.o_totalprice, test.orders.o_orderdate, test.lineitem.l_quantity",
		"                    └─HashJoin 12500.00 mpp[tiflash]  inner join, equal:[eq(test.orders.o_orderkey, test.lineitem.l_orderkey)]",
		"                      ├─ExchangeReceiver(Build) 6400.00 mpp[tiflash]  ",
		"                      │ └─ExchangeSender 6400.00 mpp[tiflash]  ExchangeType: Broadcast, Compression: FAST",
		"                      │   └─Selection 6400.00 mpp[tiflash]  gt(Column#50, 314)",
		"                      │     └─Projection 8000.00 mpp[tiflash]  Column#50, test.lineitem.l_orderkey",
		"                      │       └─HashAgg 8000.00 mpp[tiflash]  group by:test.lineitem.l_orderkey, funcs:sum(Column#57)->Column#50, funcs:firstrow(test.lineitem.l_orderkey)->test.lineitem.l_orderkey",
		"                      │         └─ExchangeReceiver 8000.00 mpp[tiflash]  ",
		"                      │           └─ExchangeSender 8000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_orderkey, collate: binary]",
		"                      │             └─HashAgg 8000.00 mpp[tiflash]  group by:test.lineitem.l_orderkey, funcs:sum(test.lineitem.l_quantity)->Column#57",
		"                      │               └─TableFullScan 10000.00 mpp[tiflash] table:lineitem keep order:false, stats:pseudo",
		"                      └─Projection(Probe) 15625.00 mpp[tiflash]  test.customer.c_custkey, test.customer.c_name, test.orders.o_orderkey, test.orders.o_totalprice, test.orders.o_orderdate, test.lineitem.l_quantity",
		"                        └─HashJoin 15625.00 mpp[tiflash]  inner join, equal:[eq(test.orders.o_orderkey, test.lineitem.l_orderkey)]",
		"                          ├─ExchangeReceiver(Build) 10000.00 mpp[tiflash]  ",
		"                          │ └─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: Broadcast, Compression: FAST",
		"                          │   └─TableFullScan 10000.00 mpp[tiflash] table:lineitem keep order:false, stats:pseudo",
		"                          └─Projection(Probe) 12500.00 mpp[tiflash]  test.customer.c_custkey, test.customer.c_name, test.orders.o_orderkey, test.orders.o_totalprice, test.orders.o_orderdate",
		"                            └─HashJoin 12500.00 mpp[tiflash]  inner join, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"                              ├─ExchangeReceiver(Build) 10000.00 mpp[tiflash]  ",
		"                              │ └─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: Broadcast, Compression: FAST",
		"                              │   └─TableFullScan 10000.00 mpp[tiflash] table:customer keep order:false, stats:pseudo",
		"                              └─TableFullScan(Probe) 10000.00 mpp[tiflash] table:orders keep order:false, stats:pseudo",
	))
}
