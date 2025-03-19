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
	tk.MustQuery(`explain format="brief" select /*+ HASH_JOIN(orders, lineitem, customer) */ l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority
from customer, orders, lineitem
where c_mktsegment = 'AUTOMOBILE' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-13' and l_shipdate > '1995-03-13'
group by l_orderkey, o_orderdate, o_shippriority
order by revenue desc, o_orderdate
limit 10;`).Check(testkit.Rows(
		// https://github.com/pingcap/tidb/issues/38610 is original case,
		// the exchanger under hashagg is eliminated
		"Projection 10.00 root  test.lineitem.l_orderkey, Column#34, test.orders.o_orderdate, test.orders.o_shippriority",
		"└─TopN 10.00 root  Column#34:desc, test.orders.o_orderdate, offset:0, count:10",
		"  └─TableReader 10.00 root  MppVersion: 3, data:ExchangeSender",
		"    └─ExchangeSender 10.00 mpp[tiflash]  ExchangeType: PassThrough",
		"      └─TopN 10.00 mpp[tiflash]  Column#34:desc, test.orders.o_orderdate, offset:0, count:10",
		"        └─Projection 15.62 mpp[tiflash]  Column#34, test.orders.o_orderdate, test.orders.o_shippriority, test.lineitem.l_orderkey",
		"          └─HashAgg 15.62 mpp[tiflash]  group by:test.lineitem.l_orderkey, test.orders.o_orderdate, test.orders.o_shippriority, funcs:sum(Column#43)->Column#34, funcs:firstrow(test.orders.o_orderdate)->test.orders.o_orderdate, funcs:firstrow(test.orders.o_shippriority)->test.orders.o_shippriority, funcs:firstrow(test.lineitem.l_orderkey)->test.lineitem.l_orderkey",
		"            └─ExchangeReceiver 15.62 mpp[tiflash]  ",
		"              └─ExchangeSender 15.62 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_orderkey, collate: binary], [name: test.orders.o_orderdate, collate: binary], [name: test.orders.o_shippriority, collate: binary]",
		"                └─HashAgg 15.62 mpp[tiflash]  group by:Column#48, Column#49, Column#50, funcs:sum(Column#47)->Column#43",
		"                  └─Projection 15.62 mpp[tiflash]  mul(test.lineitem.l_extendedprice, minus(1, test.lineitem.l_discount))->Column#47, test.lineitem.l_orderkey->Column#48, test.orders.o_orderdate->Column#49, test.orders.o_shippriority->Column#50",
		"                    └─Projection 15.62 mpp[tiflash]  test.orders.o_orderdate, test.orders.o_shippriority, test.lineitem.l_orderkey, test.lineitem.l_extendedprice, test.lineitem.l_discount",
		"                      └─HashJoin 15.62 mpp[tiflash]  inner join, equal:[eq(test.orders.o_orderkey, test.lineitem.l_orderkey)]",
		"                        ├─ExchangeReceiver(Build) 12.50 mpp[tiflash]  ",
		"                        │ └─ExchangeSender 12.50 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.orders.o_orderkey, collate: binary]",
		"                        │   └─Projection 12.50 mpp[tiflash]  test.orders.o_orderkey, test.orders.o_orderdate, test.orders.o_shippriority, test.orders.o_custkey",
		"                        │     └─HashJoin 12.50 mpp[tiflash]  inner join, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"                        │       ├─ExchangeReceiver(Build) 10.00 mpp[tiflash]  ",
		"                        │       │ └─ExchangeSender 10.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.customer.c_custkey, collate: binary]",
		`                        │       │   └─TableFullScan 10.00 mpp[tiflash] table:customer pushed down filter:eq(test.customer.c_mktsegment, "AUTOMOBILE"), keep order:false, stats:pseudo`,
		"                        │       └─ExchangeReceiver(Probe) 3323.33 mpp[tiflash]  ",
		"                        │         └─ExchangeSender 3323.33 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.orders.o_custkey, collate: binary]",
		"                        │           └─Selection 3323.33 mpp[tiflash]  lt(test.orders.o_orderdate, 1995-03-13 00:00:00.000000)",
		"                        │             └─TableFullScan 10000.00 mpp[tiflash] table:orders pushed down filter:empty, keep order:false, stats:pseudo",
		"                        └─ExchangeReceiver(Probe) 3333.33 mpp[tiflash]  ",
		"                          └─ExchangeSender 3333.33 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_orderkey, collate: binary]",
		"                            └─Selection 3333.33 mpp[tiflash]  gt(test.lineitem.l_shipdate, 1995-03-13 00:00:00.000000)",
		"                              └─TableFullScan 10000.00 mpp[tiflash] table:lineitem pushed down filter:empty, keep order:false, stats:pseudo",
	))
	// LEFT JOIN
	tk.MustQuery(`EXPLAIN FORMAT="brief"
SELECT /*+ HASH_JOIN(orders, lineitem, customer) */
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM
    customer AS c
    LEFT JOIN orders AS o ON c.c_custkey = o.o_custkey
    LEFT JOIN lineitem AS l ON l.l_orderkey = o.o_orderkey
WHERE
    c.c_mktsegment = 'AUTOMOBILE'
    AND o.o_orderdate < '1995-03-13'
    AND l.l_shipdate > '1995-03-13'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue DESC,
    o_orderdate
LIMIT 10;
`).Check(testkit.Rows("Projection 10.00 root  test.lineitem.l_orderkey, Column#34, test.orders.o_orderdate, test.orders.o_shippriority",
		"└─TopN 10.00 root  Column#34:desc, test.orders.o_orderdate, offset:0, count:10",
		"  └─HashAgg 15.62 root  group by:Column#48, Column#49, Column#50, funcs:sum(Column#47)->Column#34, funcs:firstrow(Column#48)->test.orders.o_orderdate, funcs:firstrow(Column#49)->test.orders.o_shippriority, funcs:firstrow(Column#50)->test.lineitem.l_orderkey",
		"    └─Projection 15.62 root  mul(test.lineitem.l_extendedprice, minus(1, test.lineitem.l_discount))->Column#47, test.orders.o_orderdate->Column#48, test.orders.o_shippriority->Column#49, test.lineitem.l_orderkey->Column#50",
		"      └─IndexJoin 15.62 root  inner join, inner:TableReader, outer key:test.orders.o_orderkey, inner key:test.lineitem.l_orderkey, equal cond:eq(test.orders.o_orderkey, test.lineitem.l_orderkey)",
		"        ├─TableReader(Build) 12.50 root  MppVersion: 3, data:ExchangeSender",
		"        │ └─ExchangeSender 12.50 mpp[tiflash]  ExchangeType: PassThrough",
		"        │   └─Projection 12.50 mpp[tiflash]  test.orders.o_orderkey, test.orders.o_orderdate, test.orders.o_shippriority, test.orders.o_custkey",
		"        │     └─HashJoin 12.50 mpp[tiflash]  inner join, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"        │       ├─ExchangeReceiver(Build) 10.00 mpp[tiflash]  ",
		"        │       │ └─ExchangeSender 10.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.customer.c_custkey, collate: binary]",
		`        │       │   └─TableFullScan 10.00 mpp[tiflash] table:c pushed down filter:eq(test.customer.c_mktsegment, "AUTOMOBILE"), keep order:false, stats:pseudo`,
		"        │       └─ExchangeReceiver(Probe) 3323.33 mpp[tiflash]  ",
		"        │         └─ExchangeSender 3323.33 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.orders.o_custkey, collate: binary]",
		"        │           └─Selection 3323.33 mpp[tiflash]  lt(test.orders.o_orderdate, 1995-03-13 00:00:00.000000)",
		"        │             └─TableFullScan 10000.00 mpp[tiflash] table:o pushed down filter:empty, keep order:false, stats:pseudo",
		"        └─TableReader(Probe) 4.17 root  data:Selection",
		"          └─Selection 4.17 cop[tikv]  gt(test.lineitem.l_shipdate, 1995-03-13 00:00:00.000000)",
		"            └─TableRangeScan 12.50 cop[tikv] table:l range: decided by [eq(test.lineitem.l_orderkey, test.orders.o_orderkey)], keep order:false, stats:pseudo"))
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
	tk.MustQuery(`explain format="brief" select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%pending%deposits%'
		group by
			c_custkey
	) c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;`).Check(testkit.Rows(
		"Sort 8000.00 root  Column#19:desc, Column#18:desc",
		"└─TableReader 8000.00 root  MppVersion: 3, data:ExchangeSender",
		"  └─ExchangeSender 8000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    └─Projection 8000.00 mpp[tiflash]  Column#18, Column#19",
		"      └─Projection 8000.00 mpp[tiflash]  Column#19, Column#18",
		"        └─HashAgg 8000.00 mpp[tiflash]  group by:Column#18, funcs:count(1)->Column#19, funcs:firstrow(Column#18)->Column#18",
		"          └─ExchangeReceiver 8000.00 mpp[tiflash]  ",
		"            └─ExchangeSender 8000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#18, collate: binary]",
		"              └─Projection 8000.00 mpp[tiflash]  Column#18",
		"                └─HashAgg 8000.00 mpp[tiflash]  group by:test.customer.c_custkey, funcs:count(test.orders.o_orderkey)->Column#18",
		"                  └─Projection 10000.00 mpp[tiflash]  test.customer.c_custkey, test.orders.o_orderkey",
		"                    └─HashJoin 10000.00 mpp[tiflash]  left outer join, left side:ExchangeReceiver, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"                      ├─ExchangeReceiver(Build) 8000.00 mpp[tiflash]  ",
		"                      │ └─ExchangeSender 8000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.orders.o_custkey, collate: binary]",
		`                      │   └─Selection 8000.00 mpp[tiflash]  not(like(test.orders.o_comment, "%pending%deposits%", 92))`,
		"                      │     └─TableFullScan 10000.00 mpp[tiflash] table:orders pushed down filter:empty, keep order:false, stats:pseudo",
		"                      └─ExchangeReceiver(Probe) 10000.00 mpp[tiflash]  ",
		"                        └─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.customer.c_custkey, collate: binary]",
		"                          └─TableFullScan 10000.00 mpp[tiflash] table:customer keep order:false, stats:pseudo"))
}

func TestQ9(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(` CREATE TABLE part (
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
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`
CREATE TABLE supplier (
    S_SUPPKEY bigint NOT NULL,
    S_NAME char(25) NOT NULL,
    S_ADDRESS varchar(40) NOT NULL,
    S_NATIONKEY bigint NOT NULL,
    S_PHONE char(15) NOT NULL,
    S_ACCTBAL decimal(15,2) NOT NULL,
    S_COMMENT varchar(101) NOT NULL,
    PRIMARY KEY (S_SUPPKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
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
	tk.MustExec(`
CREATE TABLE partsupp (
    PS_PARTKEY bigint NOT NULL,
    PS_SUPPKEY bigint NOT NULL,
    PS_AVAILQTY bigint NOT NULL,
    PS_SUPPLYCOST decimal(15,2) NOT NULL,
    PS_COMMENT varchar(199) NOT NULL,
    PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
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
CREATE TABLE nation (
    N_NATIONKEY bigint NOT NULL,
    N_NAME char(25) NOT NULL,
    N_REGIONKEY bigint NOT NULL,
    N_COMMENT varchar(152) DEFAULT NULL,
    PRIMARY KEY (N_NATIONKEY) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
`)
	testkit.SetTiFlashReplica(t, dom, "test", "part")
	testkit.SetTiFlashReplica(t, dom, "test", "supplier")
	testkit.SetTiFlashReplica(t, dom, "test", "lineitem")
	testkit.SetTiFlashReplica(t, dom, "test", "partsupp")
	testkit.SetTiFlashReplica(t, dom, "test", "orders")
	testkit.SetTiFlashReplica(t, dom, "test", "nation")
	require.NoError(t, loadTableStats("test.part.json", dom))
	require.NoError(t, loadTableStats("test.supplier.json", dom))
	require.NoError(t, loadTableStats("test.lineitem.json", dom))
	require.NoError(t, loadTableStats("test.partsupp.json", dom))
	require.NoError(t, loadTableStats("test.orders.json", dom))
	require.NoError(t, loadTableStats("test.nation.json", dom))
	tk.MustQuery(`explain select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%dim%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;`).Check(testkit.Rows(
		"Sort_26 2406.00 root  test.nation.n_name, Column#51:desc",
		"└─Projection_28 2406.00 root  test.nation.n_name, Column#51, Column#53",
		"  └─HashAgg_32 2406.00 root  group by:Column#57, Column#58, funcs:sum(Column#56)->Column#53, funcs:firstrow(Column#57)->test.nation.n_name, funcs:firstrow(Column#58)->Column#51",
		"    └─Projection_185 247789900.85 root  minus(mul(test.lineitem.l_extendedprice, minus(1, test.lineitem.l_discount)), mul(test.partsupp.ps_supplycost, test.lineitem.l_quantity))->Column#56, test.nation.n_name->Column#57, extract(YEAR, test.orders.o_orderdate)->Column#58",
		"      └─Projection_36 247789900.85 root  test.lineitem.l_quantity, test.lineitem.l_extendedprice, test.lineitem.l_discount, test.partsupp.ps_supplycost, test.orders.o_orderdate, test.nation.n_name",
		"        └─HashJoin_50 247789900.85 root  inner join, equal:[eq(test.lineitem.l_orderkey, test.orders.o_orderkey)]",
		"          ├─TableReader_176(Build) 75000000.00 root  MppVersion: 3, data:ExchangeSender_175",
		"          │ └─ExchangeSender_175 75000000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"          │   └─TableFullScan_174 75000000.00 mpp[tiflash] table:orders keep order:false",
		"          └─HashJoin_105(Probe) 244182819.96 root  inner join, equal:[eq(test.lineitem.l_suppkey, test.partsupp.ps_suppkey) eq(test.lineitem.l_partkey, test.partsupp.ps_partkey)]",
		"            ├─TableReader_171(Build) 40000000.00 root  MppVersion: 3, data:ExchangeSender_170",
		"            │ └─ExchangeSender_170 40000000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"            │   └─TableFullScan_169 40000000.00 mpp[tiflash] table:partsupp keep order:false",
		"            └─TableReader_123(Probe) 241491729.94 root  MppVersion: 3, data:ExchangeSender_122",
		"              └─ExchangeSender_122 241491729.94 mpp[tiflash]  ExchangeType: PassThrough",
		"                └─Projection_121 241491729.94 mpp[tiflash]  test.nation.n_name, test.lineitem.l_orderkey, test.lineitem.l_partkey, test.lineitem.l_suppkey, test.lineitem.l_quantity, test.lineitem.l_extendedprice, test.lineitem.l_discount",
		"                  └─HashJoin_107 241491729.94 mpp[tiflash]  inner join, equal:[eq(test.lineitem.l_partkey, test.part.p_partkey)]",
		"                    ├─ExchangeReceiver_72(Build) 8000000.00 mpp[tiflash]  ",
		"                    │ └─ExchangeSender_71 8000000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.part.p_partkey, collate: binary]",
		`                    │   └─Selection_70 8000000.00 mpp[tiflash]  like(test.part.p_name, "%dim%", 92)`,
		"                    │     └─TableFullScan_69 10000000.00 mpp[tiflash] table:part pushed down filter:empty, keep order:false",
		"                    └─ExchangeReceiver_68(Probe) 300825282.01 mpp[tiflash]  ",
		"                      └─ExchangeSender_67 300825282.01 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_partkey, collate: binary]",
		"                        └─Projection_66 300825282.01 mpp[tiflash]  test.nation.n_name, test.lineitem.l_orderkey, test.lineitem.l_partkey, test.lineitem.l_suppkey, test.lineitem.l_quantity, test.lineitem.l_extendedprice, test.lineitem.l_discount",
		"                          └─HashJoin_54 300825282.01 mpp[tiflash]  inner join, equal:[eq(test.supplier.s_suppkey, test.lineitem.l_suppkey)]",
		"                            ├─ExchangeReceiver_62(Build) 499986.00 mpp[tiflash]  ",
		"                            │ └─ExchangeSender_61 499986.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.supplier.s_suppkey, collate: binary]",
		"                            │   └─Projection_60 499986.00 mpp[tiflash]  test.nation.n_name, test.supplier.s_suppkey",
		"                            │     └─HashJoin_55 499986.00 mpp[tiflash]  inner join, equal:[eq(test.nation.n_nationkey, test.supplier.s_nationkey)]",
		"                            │       ├─ExchangeReceiver_58(Build) 25.00 mpp[tiflash]  ",
		"                            │       │ └─ExchangeSender_57 25.00 mpp[tiflash]  ExchangeType: Broadcast, Compression: FAST",
		"                            │       │   └─TableFullScan_56 25.00 mpp[tiflash] table:nation keep order:false",
		"                            │       └─TableFullScan_59(Probe) 500000.00 mpp[tiflash] table:supplier keep order:false",
		"                            └─ExchangeReceiver_65(Probe) 300005811.00 mpp[tiflash]  ",
		"                              └─ExchangeSender_64 300005811.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.lineitem.l_suppkey, collate: binary]",
		"                                └─TableFullScan_63 300005811.00 mpp[tiflash] table:lineitem keep order:false",
	))
}
