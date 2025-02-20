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
		"  └─TableReader_142 10.00 root  MppVersion: 3, data:ExchangeSender_141",
		"    └─ExchangeSender_141 10.00 mpp[tiflash]  ExchangeType: PassThrough",
		"      └─TopN_140 10.00 mpp[tiflash]  Column#34:desc, test.orders.o_orderdate, offset:0, count:10",
		"        └─Projection_135 39998176.29 mpp[tiflash]  Column#34, test.orders.o_orderdate, test.orders.o_shippriority, test.lineitem.l_orderkey",
		"          └─HashAgg_133 39998176.29 mpp[tiflash]  group by:Column#48, Column#49, Column#50, funcs:sum(Column#47)->Column#34, funcs:firstrow(Column#48)->test.orders.o_orderdate, funcs:firstrow(Column#49)->test.orders.o_shippriority, funcs:firstrow(Column#50)->test.lineitem.l_orderkey",
		"            └─Projection_143 92822759.11 mpp[tiflash]  mul(test.lineitem.l_extendedprice, minus(1, test.lineitem.l_discount))->Column#47, test.orders.o_orderdate->Column#48, test.orders.o_shippriority->Column#49, test.lineitem.l_orderkey->Column#50",
		"              └─Projection_118 92822759.11 mpp[tiflash]  test.orders.o_orderdate, test.orders.o_shippriority, test.lineitem.l_orderkey, test.lineitem.l_extendedprice, test.lineitem.l_discount",
		"                └─HashJoin_117 92822759.11 mpp[tiflash]  inner join, equal:[eq(test.orders.o_orderkey, test.lineitem.l_orderkey)]",
		"                  ├─ExchangeReceiver_53(Build) 22867441.30 mpp[tiflash]  ",
		"                  │ └─ExchangeSender_52 22867441.30 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.orders.o_orderkey, collate: binary]",
		"                  │   └─Projection_51 22867441.30 mpp[tiflash]  test.orders.o_orderkey, test.orders.o_orderdate, test.orders.o_shippriority",
		"                  │     └─HashJoin_44 22867441.30 mpp[tiflash]  inner join, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"                  │       ├─ExchangeReceiver_48(Build) 1501762.80 mpp[tiflash]  ",
		"                  │       │ └─ExchangeSender_47 1501762.80 mpp[tiflash]  ExchangeType: Broadcast, Compression: FAST",
		"                  │       │   └─TableFullScan_45 1501762.80 mpp[tiflash] table:customer pushed down filter:eq(test.customer.c_mktsegment, \"AUTOMOBILE\"), keep order:false",
		"                  │       └─TableFullScan_49(Probe) 36377904.50 mpp[tiflash] table:orders pushed down filter:lt(test.orders.o_orderdate, 1995-03-13 00:00:00.000000), keep order:false",
		"                  └─ExchangeReceiver_57(Probe) 162359270.28 mpp[tiflash]  ",
		"                    └─ExchangeSender_56 162359270.28 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_orderkey, collate: binary]",
		"                      └─TableFullScan_54 162359270.28 mpp[tiflash] table:lineitem pushed down filter:gt(test.lineitem.l_shipdate, 1995-03-13 00:00:00.000000), keep order:false"))
}

func TestABC(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
	tk.MustExec(`CREATE TABLE t2 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
	tk.MustExec(`CREATE TABLE t3 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
	tk.MustExec(`CREATE TABLE t4 (
    v1 INT NOT NULL,
    v2 INT NOT NULL,
    PRIMARY KEY (v1)
);`)
	testkit.SetTiFlashReplica(t, dom, "test", "t1")
	testkit.SetTiFlashReplica(t, dom, "test", "t2")
	testkit.SetTiFlashReplica(t, dom, "test", "t3")
	testkit.SetTiFlashReplica(t, dom, "test", "t4")
	tk.MustQuery(`EXPLAIN SELECT /*+ SHUFFLE_JOIN(aaa, bbb) */ count(*)
FROM
(SELECT a.v1,
a.v2
FROM
(SELECT /*+ SHUFFLE_JOIN(t1, t2) */  t1.v1,
t1.v2
FROM t1
JOIN t2 ON t1.v1 = t2.v1) a
JOIN

(SELECT /*+ SHUFFLE_JOIN(t3, t4) */ t3.v1,
t3.v2
FROM t3
JOIN t4 ON t3.v1 = t4.v1) b ON a.v1 = b.v1
AND a.v2 = b.v2) aaa
JOIN

(SELECT a.v1,
a.v2
FROM
(SELECT /*+  SHUFFLE_JOIN(t1, t2) */ t1.v1,
t1.v2
FROM t1
JOIN t2 ON t1.v2 = t2.v2) a
JOIN

(SELECT /*+ SHUFFLE_JOIN(t3, t4) */ t3.v1,
t3.v2
FROM t3
JOIN t4 ON t3.v2 = t4.v2) b ON a.v1 = b.v1
AND a.v2 = b.v2) bbb ON aaa.v1 = bbb.v1
AND aaa.v2 = bbb.v2;`).Check(testkit.Rows("HashAgg_134 1.00 root  funcs:count(Column#18)->Column#17",
		"└─TableReader_136 1.00 root  MppVersion: 3, data:ExchangeSender_135",
		"  └─ExchangeSender_135 1.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    └─HashAgg_45 1.00 mpp[tiflash]  funcs:count(1)->Column#18",
		"      └─Projection_133 47683.72 mpp[tiflash]  test.t1.v1, test.t1.v1, test.t1.v2",
		"        └─HashJoin_132 47683.72 mpp[tiflash]  inner join, equal:[eq(test.t1.v1, test.t1.v1) eq(test.t1.v2, test.t1.v2)]",
		"          ├─ExchangeReceiver_76(Build) 19531.25 mpp[tiflash]  ",
		"          │ └─ExchangeSender_75 19531.25 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t1.v1, collate: binary], [name: test.t1.v2, collate: binary]",
		"          │   └─Projection_74 19531.25 mpp[tiflash]  test.t1.v1, test.t1.v2, test.t3.v1",
		"          │     └─HashJoin_53 19531.25 mpp[tiflash]  inner join, equal:[eq(test.t3.v1, test.t4.v1)]",
		"          │       ├─ExchangeReceiver_73(Build) 10000.00 mpp[tiflash]  ",
		"          │       │ └─ExchangeSender_72 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t4.v1, collate: binary]",
		"          │       │   └─TableFullScan_71 10000.00 mpp[tiflash] table:t4 keep order:false, stats:pseudo",
		"          │       └─ExchangeReceiver_70(Probe) 15625.00 mpp[tiflash]  ",
		"          │         └─ExchangeSender_69 15625.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t3.v1, collate: binary]",
		"          │           └─Projection_68 15625.00 mpp[tiflash]  test.t1.v1, test.t1.v2, test.t3.v1",
		"          │             └─HashJoin_54 15625.00 mpp[tiflash]  inner join, equal:[eq(test.t1.v1, test.t3.v1) eq(test.t1.v2, test.t3.v2)]",
		"          │               ├─ExchangeReceiver_67(Build) 10000.00 mpp[tiflash]  ",
		"          │               │ └─ExchangeSender_66 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t3.v1, collate: binary], [name: test.t3.v2, collate: binary]",
		"          │               │   └─TableFullScan_65 10000.00 mpp[tiflash] table:t3 keep order:false, stats:pseudo",
		"          │               └─ExchangeReceiver_64(Probe) 12500.00 mpp[tiflash]  ",
		"          │                 └─ExchangeSender_63 12500.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t1.v1, collate: binary], [name: test.t1.v2, collate: binary]",
		"          │                   └─HashJoin_55 12500.00 mpp[tiflash]  inner join, equal:[eq(test.t1.v1, test.t2.v1)]",
		"          │                     ├─ExchangeReceiver_58(Build) 10000.00 mpp[tiflash]  ",
		"          │                     │ └─ExchangeSender_57 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t1.v1, collate: binary]",
		"          │                     │   └─TableFullScan_56 10000.00 mpp[tiflash] table:t1 keep order:false, stats:pseudo",
		"          │                     └─ExchangeReceiver_61(Probe) 10000.00 mpp[tiflash]  ",
		"          │                       └─ExchangeSender_60 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t2.v1, collate: binary]",
		"          │                         └─TableFullScan_59 10000.00 mpp[tiflash] table:t2 keep order:false, stats:pseudo",
		"          └─ExchangeReceiver_100(Probe) 19531.25 mpp[tiflash]  ",
		"            └─ExchangeSender_99 19531.25 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t1.v1, collate: binary], [name: test.t1.v2, collate: binary]",
		"              └─Projection_98 19531.25 mpp[tiflash]  test.t1.v1, test.t1.v2, test.t3.v2",
		"                └─HashJoin_77 19531.25 mpp[tiflash]  inner join, equal:[eq(test.t3.v2, test.t4.v2)]",
		"                  ├─ExchangeReceiver_97(Build) 10000.00 mpp[tiflash]  ",
		"                  │ └─ExchangeSender_96 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t4.v2, collate: binary]",
		"                  │   └─TableFullScan_95 10000.00 mpp[tiflash] table:t4 keep order:false, stats:pseudo",
		"                  └─ExchangeReceiver_94(Probe) 15625.00 mpp[tiflash]  ",
		"                    └─ExchangeSender_93 15625.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t3.v2, collate: binary]",
		"                      └─Projection_92 15625.00 mpp[tiflash]  test.t1.v1, test.t1.v2, test.t3.v2",
		"                        └─HashJoin_78 15625.00 mpp[tiflash]  inner join, equal:[eq(test.t1.v1, test.t3.v1) eq(test.t1.v2, test.t3.v2)]",
		"                          ├─ExchangeReceiver_91(Build) 10000.00 mpp[tiflash]  ",
		"                          │ └─ExchangeSender_90 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t3.v1, collate: binary], [name: test.t3.v2, collate: binary]",
		"                          │   └─TableFullScan_89 10000.00 mpp[tiflash] table:t3 keep order:false, stats:pseudo",
		"                          └─ExchangeReceiver_88(Probe) 12500.00 mpp[tiflash]  ",
		"                            └─ExchangeSender_87 12500.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t1.v1, collate: binary], [name: test.t1.v2, collate: binary]",
		"                              └─HashJoin_79 12500.00 mpp[tiflash]  inner join, equal:[eq(test.t1.v2, test.t2.v2)]",
		"                                ├─ExchangeReceiver_82(Build) 10000.00 mpp[tiflash]  ",
		"                                │ └─ExchangeSender_81 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t1.v2, collate: binary]",
		"                                │   └─TableFullScan_80 10000.00 mpp[tiflash] table:t1 keep order:false, stats:pseudo",
		"                                └─ExchangeReceiver_85(Probe) 10000.00 mpp[tiflash]  ",
		"                                  └─ExchangeSender_84 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.t2.v2, collate: binary]",
		"                                    └─TableFullScan_83 10000.00 mpp[tiflash] table:t2 keep order:false, stats:pseudo",
	))
	tk.MustQuery("show warnings").Check(testkit.Rows())
}
