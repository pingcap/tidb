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
