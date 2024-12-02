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
	"github.com/stretchr/testify/require"
)

func TestTPCHQ3(t *testing.T) {
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
	require.NoError(t, loadTableStats("test.customer.json", dom))
	require.NoError(t, loadTableStats("test.lineitem.json", dom))
	require.NoError(t, loadTableStats("test.orders.json", dom))
	tk.MustQuery(`explain select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
from customer, orders, lineitem
where c_mktsegment = 'AUTOMOBILE' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-13' and l_shipdate > '1995-03-13'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate
limit 10;`).Check(testkit.Rows(
		"Projection_14 10.00 root  test.lineitem.l_orderkey, Column#34, test.orders.o_orderdate, test.orders.o_shippriority",
		"└─TopN_18 10.00 root  Column#34:desc, test.orders.o_orderdate, offset:0, count:10",
		"  └─TableReader_144 10.00 root  MppVersion: 3, data:ExchangeSender_143",
		"    └─ExchangeSender_143 10.00 mpp[tiflash]  ExchangeType: PassThrough",
		"      └─TopN_142 10.00 mpp[tiflash]  Column#34:desc, test.orders.o_orderdate, offset:0, count:10",
		"        └─Projection_138 39998176.29 mpp[tiflash]  Column#34, test.orders.o_orderdate, test.orders.o_shippriority, test.lineitem.l_orderkey",
		"          └─HashAgg_139 39998176.29 mpp[tiflash]  group by:test.lineitem.l_orderkey, test.orders.o_orderdate, test.orders.o_shippriority, funcs:sum(Column#43)->Column#34, funcs:firstrow(test.orders.o_orderdate)->test.orders.o_orderdate, funcs:firstrow(test.orders.o_shippriority)->test.orders.o_shippriority, funcs:firstrow(test.lineitem.l_orderkey)->test.lineitem.l_orderkey",
		"            └─ExchangeReceiver_141 39998176.29 mpp[tiflash]  ",
		"              └─ExchangeSender_140 39998176.29 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_orderkey, collate: binary], [name: test.orders.o_orderdate, collate: binary], [name: test.orders.o_shippriority, collate: binary]",
		"                └─HashAgg_136 39998176.29 mpp[tiflash]  group by:Column#48, Column#49, Column#50, funcs:sum(Column#47)->Column#43",
		"                  └─Projection_145 92822759.11 mpp[tiflash]  mul(test.lineitem.l_extendedprice, minus(1, test.lineitem.l_discount))->Column#47, test.lineitem.l_orderkey->Column#48, test.orders.o_orderdate->Column#49, test.orders.o_shippriority->Column#50",
		"                    └─Projection_125 92822759.11 mpp[tiflash]  test.orders.o_orderdate, test.orders.o_shippriority, test.lineitem.l_orderkey, test.lineitem.l_extendedprice, test.lineitem.l_discount",
		"                      └─HashJoin_124 92822759.11 mpp[tiflash]  inner join, equal:[eq(test.orders.o_orderkey, test.lineitem.l_orderkey)]",
		"                        ├─ExchangeReceiver_53(Build) 22867441.30 mpp[tiflash]  ",
		"                        │ └─ExchangeSender_52 22867441.30 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.orders.o_orderkey, collate: binary]",
		"                        │   └─Projection_51 22867441.30 mpp[tiflash]  test.orders.o_orderkey, test.orders.o_orderdate, test.orders.o_shippriority",
		"                        │     └─HashJoin_44 22867441.30 mpp[tiflash]  inner join, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"                        │       ├─ExchangeReceiver_48(Build) 1501762.80 mpp[tiflash]  ",
		"                        │       │ └─ExchangeSender_47 1501762.80 mpp[tiflash]  ExchangeType: Broadcast, Compression: FAST",
		"                        │       │   └─TableFullScan_45 1501762.80 mpp[tiflash] table:customer pushed down filter:eq(test.customer.c_mktsegment, \"AUTOMOBILE\"), keep order:false",
		"                        │       └─TableFullScan_49(Probe) 36377904.50 mpp[tiflash] table:orders pushed down filter:lt(test.orders.o_orderdate, 1995-03-13 00:00:00.000000), keep order:false",
		"                        └─ExchangeReceiver_57(Probe) 162359270.28 mpp[tiflash]  ",
		"                          └─ExchangeSender_56 162359270.28 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_orderkey, collate: binary]",
		"                            └─TableFullScan_54 162359270.28 mpp[tiflash] table:lineitem pushed down filter:gt(test.lineitem.l_shipdate, 1995-03-13 00:00:00.000000), keep order:false"))
}
