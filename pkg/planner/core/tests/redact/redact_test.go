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

package redact

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
)

func TestRedactExplain(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t2(id int, a int, b int, primary key(id, a)) partition by hash(id + a) partitions 10;")
	tk.MustExec("create table t1(id int primary key, a int, b int) partition by hash(id) partitions 10;")
	tk.MustExec("create table t(a int primary key, b int);")
	tk.MustExec(`create table tlist (a int) partition by list (a) (
    partition p0 values in (0, 1, 2),
    partition p1 values in (3, 4, 5),
    partition p2 values in (6, 7, 8),
    partition p3 values in (9, 10, 11))`)
	tk.MustExec("create table employee (empid int, deptid int, salary decimal(10,2))")
	tk.MustExec("CREATE TABLE person (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,name VARCHAR(255) NOT NULL,address_info JSON,city_no INT AS (JSON_EXTRACT(address_info, '$.city_no')) VIRTUAL,KEY(city_no));")

	testkit.SetTiFlashReplica(t, dom, "test", "employee")
	// ---------------------------------------------------------------------------
	// tidb_redact_log=MARKER
	// ---------------------------------------------------------------------------
	tk.MustExec("set session tidb_redact_log=MARKER")
	// in multi-value
	tk.MustQuery("explain format='brief' select 1 from t left join tlist on tlist.a=t.a where t.a in (12, 13)").
		Check(testkit.Rows(
			"Projection 2.50 root  ‹1›->Column#5",
			"└─HashJoin 2.50 root  left outer join, left side:Batch_Point_Get, equal:[eq(test.t.a, test.tlist.a)]",
			"  ├─Batch_Point_Get(Build) 2.00 root table:t handle:[12 13], keep order:false, desc:false",
			"  └─TableReader(Probe) 20.00 root partition:dual data:Selection",
			"    └─Selection 20.00 cop[tikv]  in(test.tlist.a, ‹12›, ‹13›), not(isnull(test.tlist.a))",
			"      └─TableFullScan 10000.00 cop[tikv] table:tlist keep order:false, stats:pseudo"))
	// TableRangeScan + Limit
	tk.MustQuery("explain format='brief' select * from t where a > 1 limit 10 offset 10;").
		Check(testkit.Rows(
			"Limit 10.00 root  offset:‹10›, count:‹10›",
			"└─TableReader 20.00 root  data:Limit",
			"  └─Limit 20.00 cop[tikv]  offset:‹0›, count:‹20›",
			"    └─TableRangeScan 20.00 cop[tikv] table:t range:(‹1›,+inf], keep order:false, stats:pseudo"))
	tk.MustQuery("explain format='brief' select * from t where a < 1;").
		Check(testkit.Rows(
			"TableReader 3333.33 root  data:TableRangeScan",
			"└─TableRangeScan 3333.33 cop[tikv] table:t range:[-inf,‹1›), keep order:false, stats:pseudo"))
	// PointGet + order by
	tk.MustQuery("explain format='brief' select b+1 as vt from t where a = 1 order by vt;").
		Check(testkit.Rows(
			"Sort 1.00 root  Column#3",
			"└─Projection 1.00 root  plus(test.t.b, ‹1›)->Column#3",
			"  └─Point_Get 1.00 root table:t handle:‹1›"))
	// expression partition key
	tk.MustQuery("explain format='brief' select *, row_number() over (partition by deptid+1) FROM employee").Check(testkit.Rows(
		"TableReader 10000.00 root  MppVersion: 3, data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Projection 10000.00 mpp[tiflash]  test.employee.empid, test.employee.deptid, test.employee.salary, Column#7, stream_count: 8",
		"    └─Window 10000.00 mpp[tiflash]  row_number()->Column#7 over(partition by Column#6 rows between current row and current row), stream_count: 8",
		"      └─Sort 10000.00 mpp[tiflash]  Column#6, stream_count: 8",
		"        └─ExchangeReceiver 10000.00 mpp[tiflash]  stream_count: 8",
		"          └─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#6, collate: binary], stream_count: 8",
		"            └─Projection 10000.00 mpp[tiflash]  test.employee.empid, test.employee.deptid, test.employee.salary, plus(test.employee.deptid, ‹1›)->Column#6", // <- here
		"              └─TableFullScan 10000.00 mpp[tiflash] table:employee keep order:false, stats:pseudo"))
	tk.MustQuery("explain format = 'brief' select * from tlist where a in (2)").Check(testkit.Rows(
		"TableReader 10.00 root partition:p0 data:Selection",
		"└─Selection 10.00 cop[tikv]  eq(test.tlist.a, ‹2›)",
		"  └─TableFullScan 10000.00 cop[tikv] table:tlist keep order:false, stats:pseudo"))
	// CTE
	tk.MustQuery("explain format='brief' with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000) select * from cte, t limit 100 offset 100;").Check(
		testkit.Rows(
			"Limit 100.00 root  offset:‹100›, count:‹100›",
			"└─HashJoin 200.00 root  CARTESIAN inner join",
			"  ├─CTEFullScan(Build) 2.00 root CTE:cte data:CTE_0",
			"  └─TableReader(Probe) 100.00 root  data:TableFullScan",
			"    └─TableFullScan 100.00 cop[tikv] table:t keep order:false, stats:pseudo",
			"CTE_0 2.00 root  Recursive CTE",
			"├─Projection(Seed Part) 1.00 root  ‹1›->Column#2",
			"│ └─TableDual 1.00 root  rows:1",
			"└─Projection(Recursive Part) 0.80 root  cast(plus(Column#3, ‹1›), bigint(1) BINARY)->Column#5",
			"  └─Selection 0.80 root  lt(Column#3, ‹1000›)",
			"    └─CTETable 1.00 root  Scan on CTE_0"))
	// virtual generated column
	tk.MustQuery("EXPLAIN format = 'brief' SELECT name FROM person where city_no=1").Check(testkit.Rows(
		"Projection 10.00 root  test.person.name",
		"└─Projection 10.00 root  test.person.name, test.person.city_no",
		"  └─IndexLookUp 10.00 root  ",
		"    ├─IndexRangeScan(Build) 10.00 cop[tikv] table:person, index:city_no(city_no) range:[‹1›,‹1›], keep order:false, stats:pseudo",
		"    └─TableRowIDScan(Probe) 10.00 cop[tikv] table:person keep order:false, stats:pseudo"))
	// group by
	tk.MustQuery(" explain format='brief' select 1 from test.t group by 1").Check(testkit.Rows(
		"Projection 1.00 root  ‹1›->Column#3",
		"└─HashAgg 1.00 root  group by:Column#7, funcs:firstrow(Column#8)->Column#6",
		"  └─TableReader 1.00 root  data:HashAgg",
		"    └─HashAgg 1.00 cop[tikv]  group by:‹1›, funcs:firstrow(‹1›)->Column#8",
		"      └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	// ---------------------------------------------------------------------------
	// tidb_redact_log=ON
	// ---------------------------------------------------------------------------
	tk.MustExec("set session tidb_redact_log=ON")
	// in multi-value
	tk.MustQuery("explain format='brief' select 1 from t left join tlist on tlist.a=t.a where t.a in (12, 13)").
		Check(testkit.Rows(
			"Projection 2.50 root  ?->Column#5",
			"└─HashJoin 2.50 root  left outer join, left side:Batch_Point_Get, equal:[eq(test.t.a, test.tlist.a)]",
			"  ├─Batch_Point_Get(Build) 2.00 root table:t handle:[12 13], keep order:false, desc:false",
			"  └─TableReader(Probe) 20.00 root partition:dual data:Selection",
			"    └─Selection 20.00 cop[tikv]  in(test.tlist.a, ?, ?), not(isnull(test.tlist.a))",
			"      └─TableFullScan 10000.00 cop[tikv] table:tlist keep order:false, stats:pseudo"))
	// TableRangeScan + Limit
	tk.MustQuery("explain format='brief' select * from t where a > 1 limit 10 offset 10;").
		Check(testkit.Rows(
			"Limit 10.00 root  offset:?, count:?",
			"└─TableReader 20.00 root  data:Limit",
			"  └─Limit 20.00 cop[tikv]  offset:?, count:?",
			"    └─TableRangeScan 20.00 cop[tikv] table:t range:(?,+inf], keep order:false, stats:pseudo"))
	tk.MustQuery("explain format='brief' select * from t where a < 1;").
		Check(testkit.Rows(
			"TableReader 3333.33 root  data:TableRangeScan",
			"└─TableRangeScan 3333.33 cop[tikv] table:t range:[-inf,?), keep order:false, stats:pseudo"))
	// PointGet + order by
	tk.MustQuery("explain format='brief' select b+1 as vt from t where a = 1 order by vt;").
		Check(testkit.Rows(
			"Sort 1.00 root  Column#3",
			"└─Projection 1.00 root  plus(test.t.b, ?)->Column#3",
			"  └─Point_Get 1.00 root table:t handle:?"))
	// expression partition key
	tk.MustQuery("explain format='brief' select *, row_number() over (partition by deptid+1) FROM employee").Check(testkit.Rows(
		"TableReader 10000.00 root  MppVersion: 3, data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Projection 10000.00 mpp[tiflash]  test.employee.empid, test.employee.deptid, test.employee.salary, Column#7, stream_count: 8",
		"    └─Window 10000.00 mpp[tiflash]  row_number()->Column#7 over(partition by Column#6 rows between current row and current row), stream_count: 8",
		"      └─Sort 10000.00 mpp[tiflash]  Column#6, stream_count: 8",
		"        └─ExchangeReceiver 10000.00 mpp[tiflash]  stream_count: 8",
		"          └─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: Column#6, collate: binary], stream_count: 8",
		"            └─Projection 10000.00 mpp[tiflash]  test.employee.empid, test.employee.deptid, test.employee.salary, plus(test.employee.deptid, ?)->Column#6", // <- here
		"              └─TableFullScan 10000.00 mpp[tiflash] table:employee keep order:false, stats:pseudo"))
	tk.MustQuery("explain format = 'brief' select * from tlist where a in (2)").Check(testkit.Rows(
		"TableReader 10.00 root partition:p0 data:Selection",
		"└─Selection 10.00 cop[tikv]  eq(test.tlist.a, ?)",
		"  └─TableFullScan 10000.00 cop[tikv] table:tlist keep order:false, stats:pseudo"))
	// CTE
	tk.MustQuery("explain format='brief' with recursive cte(a) as (select 1 union select a + 1 from cte where a < 1000) select * from cte, t limit 100 offset 100;").Check(
		testkit.Rows("Limit 100.00 root  offset:?, count:?",
			"└─HashJoin 200.00 root  CARTESIAN inner join",
			"  ├─CTEFullScan(Build) 2.00 root CTE:cte data:CTE_0",
			"  └─TableReader(Probe) 100.00 root  data:TableFullScan",
			"    └─TableFullScan 100.00 cop[tikv] table:t keep order:false, stats:pseudo",
			"CTE_0 2.00 root  Recursive CTE",
			"├─Projection(Seed Part) 1.00 root  ?->Column#2",
			"│ └─TableDual 1.00 root  rows:1",
			"└─Projection(Recursive Part) 0.80 root  cast(plus(Column#3, ?), bigint(1) BINARY)->Column#5",
			"  └─Selection 0.80 root  lt(Column#3, ?)",
			"    └─CTETable 1.00 root  Scan on CTE_0"))
	// virtual generated column
	tk.MustQuery("EXPLAIN format = 'brief' SELECT name FROM person where city_no=1").Check(testkit.Rows(
		"Projection 10.00 root  test.person.name",
		"└─Projection 10.00 root  test.person.name, test.person.city_no",
		"  └─IndexLookUp 10.00 root  ",
		"    ├─IndexRangeScan(Build) 10.00 cop[tikv] table:person, index:city_no(city_no) range:[?,?], keep order:false, stats:pseudo",
		"    └─TableRowIDScan(Probe) 10.00 cop[tikv] table:person keep order:false, stats:pseudo"))
	// group by
	tk.MustQuery(" explain format='brief' select 1 from test.t group by 1").Check(testkit.Rows(
		"Projection 1.00 root  ?->Column#3",
		"└─HashAgg 1.00 root  group by:Column#7, funcs:firstrow(Column#8)->Column#6",
		"  └─TableReader 1.00 root  data:HashAgg",
		"    └─HashAgg 1.00 cop[tikv]  group by:?, funcs:firstrow(?)->Column#8",
		"      └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
}

func TestRedactForRangeInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int, b int, c int, index idx(a, b))")
	tk.MustExec("set session tidb_redact_log=ON")
	tk.MustQuery("explain format='brief' select /*+ inl_join(t2) */ * from t1 join t2 on t1.a = t2.a where t2.b in (10, 20, 30)").Check(
		testkit.Rows(
			"IndexJoin 37.46 root  inner join, inner:IndexLookUp, outer key:test.t1.a, inner key:test.t2.a, equal cond:eq(test.t1.a, test.t2.a)",
			"├─TableReader(Build) 9990.00 root  data:Selection",
			"│ └─Selection 9990.00 cop[tikv]  not(isnull(test.t1.a))",
			"│   └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
			"└─IndexLookUp(Probe) 37.46 root  ",
			"  ├─Selection(Build) 37.46 cop[tikv]  not(isnull(test.t2.a))",
			"  │ └─IndexRangeScan 37.50 cop[tikv] table:t2, index:idx(a, b) range: decided by [eq(test.t2.a, test.t1.a) in(test.t2.b, ?, ?, ?)], keep order:false, stats:pseudo",
			"  └─TableRowIDScan(Probe) 37.46 cop[tikv] table:t2 keep order:false, stats:pseudo",
		))
}

func TestJoinNotSupportedByTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_1")
	tk.MustExec("create table table_1(id int not null, bit_col bit(2) not null, datetime_col datetime not null, index idx(id, bit_col, datetime_col))")
	tk.MustExec("insert into table_1 values(1,b'1','2020-01-01 00:00:00'),(2,b'0','2020-01-01 00:00:00')")
	tk.MustExec("analyze table table_1")

	tk.MustExec("insert into mysql.expr_pushdown_blacklist values('dayofmonth', 'tiflash', '');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	tk.MustExec("set session tidb_redact_log=ON")
	tk.MustQuery("explain format = 'brief' select * from table_1 a left join table_1 b on a.id = b.id and dayofmonth(a.datetime_col) > 100").Check(testkit.Rows(
		"MergeJoin 2.00 root  left outer join, left side:IndexReader, left key:test.table_1.id, right key:test.table_1.id, left cond:gt(dayofmonth(test.table_1.datetime_col), ?)",
		"├─IndexReader(Build) 2.00 root  index:IndexFullScan",
		"│ └─IndexFullScan 2.00 cop[tikv] table:b, index:idx(id, bit_col, datetime_col) keep order:true",
		"└─IndexReader(Probe) 2.00 root  index:IndexFullScan",
		"  └─IndexFullScan 2.00 cop[tikv] table:a, index:idx(id, bit_col, datetime_col) keep order:true"))
	tk.MustExec("set session tidb_redact_log=MARKER")
	tk.MustQuery("explain format = 'brief' select * from table_1 a left join table_1 b on a.id = b.id and dayofmonth(a.datetime_col) > 100").Check(testkit.Rows(
		"MergeJoin 2.00 root  left outer join, left side:IndexReader, left key:test.table_1.id, right key:test.table_1.id, left cond:gt(dayofmonth(test.table_1.datetime_col), ‹100›)",
		"├─IndexReader(Build) 2.00 root  index:IndexFullScan",
		"│ └─IndexFullScan 2.00 cop[tikv] table:b, index:idx(id, bit_col, datetime_col) keep order:true",
		"└─IndexReader(Probe) 2.00 root  index:IndexFullScan",
		"  └─IndexFullScan 2.00 cop[tikv] table:a, index:idx(id, bit_col, datetime_col) keep order:true"))
}

func TestRedactTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create table test.first_range(p int not null, o int not null, v int not null, o_datetime datetime not null, o_time time not null);")
	tk.MustExec("insert into test.first_range (p, o, v, o_datetime, o_time) values (0, 0, 0, '2023-9-20 11:17:10', '11:17:10');")
	tk.MustExec("create table test.first_range_d64(p int not null, o decimal(17,1) not null, v int not null);")
	tk.MustExec("insert into test.first_range_d64 (p, o, v) values (0, 0.1, 0), (1, 1.0, 1), (1, 2.1, 2), (1, 4.1, 4), (1, 8.1, 8), (2, 0.0, 0), (2, 3.1, 3), (2, 10.0, 10), (2, 13.1, 13), (2, 15.1, 15), (3, 1.1, 1), (3, 2.9, 3), (3, 5.1, 5), (3, 9.1, 9), (3, 15.0, 15), (3, 20.1, 20), (3, 31.1, 31);")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	// Create virtual tiflash replica info.
	testkit.SetTiFlashReplica(t, dom, "test", "first_range")
	testkit.SetTiFlashReplica(t, dom, "test", "first_range_d64")

	tk.MustExec(`set @@tidb_max_tiflash_threads=20`)
	tk.MustExec("set session tidb_redact_log=ON")
	tk.MustQuery("explain format='brief' select *, first_value(v) over (partition by p order by o range between 3 preceding and 0 following) as a from test.first_range;").Check(testkit.Rows(
		"TableReader 10000.00 root  MppVersion: 3, data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Window 10000.00 mpp[tiflash]  first_value(test.first_range.v)->Column#8 over(partition by test.first_range.p order by test.first_range.o range between ? preceding and ? following), stream_count: 20",
		"    └─Sort 10000.00 mpp[tiflash]  test.first_range.p, test.first_range.o, stream_count: 20",
		"      └─ExchangeReceiver 10000.00 mpp[tiflash]  stream_count: 20",
		"        └─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.first_range.p, collate: binary], stream_count: 20",
		"          └─TableFullScan 10000.00 mpp[tiflash] table:first_range keep order:false, stats:pseudo"))
	tk.MustExec("set session tidb_redact_log=MARKER")
	tk.MustQuery("explain format='brief' select *, first_value(v) over (partition by p order by o range between 3 preceding and 0 following) as a from test.first_range;").Check(testkit.Rows(
		"TableReader 10000.00 root  MppVersion: 3, data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Window 10000.00 mpp[tiflash]  first_value(test.first_range.v)->Column#8 over(partition by test.first_range.p order by test.first_range.o range between ‹3› preceding and ‹0› following), stream_count: 20",
		"    └─Sort 10000.00 mpp[tiflash]  test.first_range.p, test.first_range.o, stream_count: 20",
		"      └─ExchangeReceiver 10000.00 mpp[tiflash]  stream_count: 20",
		"        └─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.first_range.p, collate: binary], stream_count: 20",
		"          └─TableFullScan 10000.00 mpp[tiflash] table:first_range keep order:false, stats:pseudo"))
}
