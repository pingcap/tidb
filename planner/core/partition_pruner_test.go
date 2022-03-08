// Copyright 2019 PingCAP, Inc.
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

package core_test

import (
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

<<<<<<< HEAD
var _ = Suite(&testPartitionPruneSuit{})
=======
func TestHashPartitionPruner(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("drop table if exists t1, t2;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table t2(id int, a int, b int, primary key(id, a)) partition by hash(id + a) partitions 10;")
	tk.MustExec("create table t1(id int primary key, a int, b int) partition by hash(id) partitions 10;")
	tk.MustExec("create table t3(id int, a int, b int, primary key(id, a)) partition by hash(id) partitions 10;")
	tk.MustExec("create table t4(d datetime, a int, b int, primary key(d, a)) partition by hash(year(d)) partitions 10;")
	tk.MustExec("create table t5(d date, a int, b int, primary key(d, a)) partition by hash(month(d)) partitions 10;")
	tk.MustExec("create table t6(a int, b int) partition by hash(a) partitions 3;")
	tk.MustExec("create table t7(a int, b int) partition by hash(a + b) partitions 10;")
	tk.MustExec("create table t8(a int, b int) partition by hash(a) partitions 6;")
	tk.MustExec("create table t9(a bit(1) default null, b int(11) default null) partition by hash(a) partitions 3;") //issue #22619
	tk.MustExec("create table t10(a bigint unsigned) partition BY hash (a);")

	var input []string
	var output []struct {
		SQL    string
		Result []string
	}
	partitionPrunerData := plannercore.GetPartitionPrunerData()
	partitionPrunerData.GetTestCases(t, &input, &output)
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
		})
		tk.MustQuery(tt).Check(testkit.Rows(output[i].Result...))
	}
}

func TestRangeColumnPartitionPruningForIn(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_range_col_in")
	tk.MustExec("create database test_range_col_in")
	tk.MustExec("use test_range_col_in")
	tk.MustExec(`set @@session.tidb_enable_list_partition = 1`)
	tk.MustExec("set @@session.tidb_partition_prune_mode='static'")

	// case in issue-26739
	tk.MustExec(`CREATE TABLE t1 (
		id bigint(20)  NOT NULL AUTO_INCREMENT,
		dt date,
		PRIMARY KEY (id,dt))
		PARTITION BY RANGE COLUMNS(dt) (
		PARTITION p20201125 VALUES LESS THAN ("20201126"),
		PARTITION p20201126 VALUES LESS THAN ("20201127"),
		PARTITION p20201127 VALUES LESS THAN ("20201128"),
		PARTITION p20201128 VALUES LESS THAN ("20201129"),
		PARTITION p20201129 VALUES LESS THAN ("20201130"))`)
	tk.MustQuery(`explain format='brief' select /*+ HASH_AGG() */ count(1) from t1 where dt in ('2020-11-27','2020-11-28')`).Check(
		testkit.Rows("HashAgg 1.00 root  funcs:count(Column#5)->Column#4",
			"└─PartitionUnion 2.00 root  ",
			"  ├─HashAgg 1.00 root  funcs:count(Column#7)->Column#5",
			"  │ └─IndexReader 1.00 root  index:HashAgg",
			"  │   └─HashAgg 1.00 cop[tikv]  funcs:count(1)->Column#7",
			"  │     └─Selection 20.00 cop[tikv]  in(test_range_col_in.t1.dt, 2020-11-27 00:00:00.000000, 2020-11-28 00:00:00.000000)",
			"  │       └─IndexFullScan 10000.00 cop[tikv] table:t1, partition:p20201127, index:PRIMARY(id, dt) keep order:false, stats:pseudo",
			"  └─HashAgg 1.00 root  funcs:count(Column#10)->Column#5",
			"    └─IndexReader 1.00 root  index:HashAgg",
			"      └─HashAgg 1.00 cop[tikv]  funcs:count(1)->Column#10",
			"        └─Selection 20.00 cop[tikv]  in(test_range_col_in.t1.dt, 2020-11-27 00:00:00.000000, 2020-11-28 00:00:00.000000)",
			"          └─IndexFullScan 10000.00 cop[tikv] table:t1, partition:p20201128, index:PRIMARY(id, dt) keep order:false, stats:pseudo"))

	tk.MustExec(`insert into t1 values (1, "2020-11-25")`)
	tk.MustExec(`insert into t1 values (2, "2020-11-26")`)
	tk.MustExec(`insert into t1 values (3, "2020-11-27")`)
	tk.MustExec(`insert into t1 values (4, "2020-11-28")`)
	tk.MustQuery(`select id from t1 where dt in ('2020-11-27','2020-11-28') order by id`).Check(testkit.Rows("3", "4"))
	tk.MustQuery(`select id from t1 where dt in (20201127,'2020-11-28') order by id`).Check(testkit.Rows("3", "4"))
	tk.MustQuery(`select id from t1 where dt in (20201127,20201128) order by id`).Check(testkit.Rows("3", "4"))
	tk.MustQuery(`select id from t1 where dt in (20201127,20201128,null) order by id`).Check(testkit.Rows("3", "4"))
	tk.MustQuery(`select id from t1 where dt in ('2020-11-26','2020-11-25','2020-11-28') order by id`).Check(testkit.Rows("1", "2", "4"))
	tk.MustQuery(`select id from t1 where dt in ('2020-11-26','wrong','2020-11-28') order by id`).Check(testkit.Rows("2", "4"))

	// int
	tk.MustExec(`create table t2 (a int) partition by range columns(a) (
		partition p0 values less than (0),
		partition p1 values less than (10),
		partition p2 values less than (20))`)
	tk.MustExec(`insert into t2 values (-1), (1), (11), (null)`)
	tk.MustQuery(`select a from t2 where a in (-1, 1) order by a`).Check(testkit.Rows("-1", "1"))
	tk.MustQuery(`select a from t2 where a in (1, 11, null) order by a`).Check(testkit.Rows("1", "11"))
	tk.MustQuery(`explain format='brief' select a from t2 where a in (-1, 1)`).Check(testkit.Rows("PartitionUnion 40.00 root  ",
		"├─TableReader 20.00 root  data:Selection",
		"│ └─Selection 20.00 cop[tikv]  in(test_range_col_in.t2.a, -1, 1)",
		"│   └─TableFullScan 10000.00 cop[tikv] table:t2, partition:p0 keep order:false, stats:pseudo",
		"└─TableReader 20.00 root  data:Selection",
		"  └─Selection 20.00 cop[tikv]  in(test_range_col_in.t2.a, -1, 1)",
		"    └─TableFullScan 10000.00 cop[tikv] table:t2, partition:p1 keep order:false, stats:pseudo"))

	tk.MustExec(`create table t3 (a varchar(10)) partition by range columns(a) (
		partition p0 values less than ("aaa"),
		partition p1 values less than ("bbb"),
		partition p2 values less than ("ccc"))`)
	tk.MustQuery(`explain format='brief' select a from t3 where a in ('aaa', 'aab')`).Check(testkit.Rows(
		`TableReader 20.00 root  data:Selection`,
		`└─Selection 20.00 cop[tikv]  in(test_range_col_in.t3.a, "aaa", "aab")`,
		`  └─TableFullScan 10000.00 cop[tikv] table:t3, partition:p1 keep order:false, stats:pseudo`))
	tk.MustQuery(`explain format='brief' select a from t3 where a in ('aaa', 'bu')`).Check(testkit.Rows(
		`PartitionUnion 40.00 root  `,
		`├─TableReader 20.00 root  data:Selection`,
		`│ └─Selection 20.00 cop[tikv]  in(test_range_col_in.t3.a, "aaa", "bu")`,
		`│   └─TableFullScan 10000.00 cop[tikv] table:t3, partition:p1 keep order:false, stats:pseudo`,
		`└─TableReader 20.00 root  data:Selection`,
		`  └─Selection 20.00 cop[tikv]  in(test_range_col_in.t3.a, "aaa", "bu")`,
		`    └─TableFullScan 10000.00 cop[tikv] table:t3, partition:p2 keep order:false, stats:pseudo`))
}

func TestRangeColumnPartitionPruningForInString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_range_col_in_string")
	tk.MustExec("create database test_range_col_in_string")

	tk.MustExec("use test_range_col_in_string")
	tk.MustExec("set names utf8mb4 collate utf8mb4_bin")
	tk.MustExec("set @@session.tidb_partition_prune_mode='static'")

	type testStruct struct {
		sql        string
		partitions string
		rows       []string
	}

	extractPartitions := func(res *testkit.Result) string {
		planStrings := testdata.ConvertRowsToStrings(res.Rows())
		partitions := []string{}
		for _, s := range planStrings {
			parts := getFieldValue("partition:", s)
			if parts != "" {
				partitions = append(partitions, strings.Split(parts, ",")...)
			}
		}
		return strings.Join(partitions, ",")
	}
	checkColumnStringPruningTests := func(tests []testStruct) {
		modes := []string{"dynamic", "static"}
		for _, mode := range modes {
			tk.MustExec(`set @@tidb_partition_prune_mode = '` + mode + `'`)
			for _, test := range tests {
				explainResult := tk.MustQuery("explain format = 'brief' " + test.sql)
				partitions := strings.ToLower(extractPartitions(explainResult))
				require.Equal(t, test.partitions, partitions, "Mode: %s sql: %s", mode, test.sql)
				tk.MustQuery(test.sql).Sort().Check(testkit.Rows(test.rows...))
			}
		}
	}
	tk.MustExec("create table t (a varchar(255) charset utf8mb4 collate utf8mb4_bin) partition by range columns(a)" +
		`( partition pNull values less than (""),` +
		`partition pAAAA values less than ("AAAA"),` +
		`partition pCCC values less than ("CCC"),` +
		`partition pShrimpsandwich values less than ("Räksmörgås"),` +
		`partition paaa values less than ("aaa"),` +
		`partition pSushi values less than ("🍣🍣🍣"),` +
		`partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values (NULL), ("a"), ("Räkmacka"), ("🍣 is life"), ("🍺 after work?"), ("🍺🍺🍺🍺🍺 for oktoberfest"),("AA"),("aa"),("AAA"),("aaa")`)
	tests := []testStruct{
		// Lower case partition names due to issue#32719
		{sql: `select * from t where a IS NULL`, partitions: "pnull", rows: []string{"<nil>"}},
		{sql: `select * from t where a = 'AA'`, partitions: "paaaa", rows: []string{"AA"}},
		{sql: `select * from t where a = 'AA' collate utf8mb4_general_ci`, partitions: "paaaa", rows: []string{"AA"}}, // Notice that the it not uses _bin collation for partition => 'aa' not found! #32749
		{sql: `select * from t where a = 'aa'`, partitions: "paaa", rows: []string{"aa"}},
		{sql: `select * from t where a = 'aa' collate utf8mb4_general_ci`, partitions: "paaaa", rows: []string{"AA"}}, // Notice that the it not uses _bin collation for partition => 'aa' not found! #32749
		{sql: `select * from t where a = 'AAA'`, partitions: "paaaa", rows: []string{"AAA"}},
		{sql: `select * from t where a = 'AB'`, partitions: "pccc", rows: []string{}},
		{sql: `select * from t where a = 'aB'`, partitions: "paaa", rows: []string{}},
		{sql: `select * from t where a = '🍣'`, partitions: "psushi", rows: []string{}},
		{sql: `select * from t where a in ('🍣 is life', "Räkmacka", "🍺🍺🍺🍺  after work?")`, partitions: "pshrimpsandwich,psushi,pmax", rows: []string{"Räkmacka", "🍣 is life"}},
		{sql: `select * from t where a in ('AAA', 'aa')`, partitions: "paaaa,paaa", rows: []string{"AAA", "aa"}},
		{sql: `select * from t where a in ('AAA' collate utf8mb4_general_ci, 'aa')`, partitions: "paaaa,paaa", rows: []string{"AA", "AAA", "aa"}}, // aaa missing due to #32749
		{sql: `select * from t where a in ('AAA', 'aa' collate utf8mb4_general_ci)`, partitions: "paaaa", rows: []string{"AA", "AAA"}},            // aa, aaa missing due to #32749
	}
	checkColumnStringPruningTests(tests)
	tk.MustExec(`set names utf8mb4 collate utf8mb4_general_ci`)
	checkColumnStringPruningTests(tests)
	tk.MustExec(`set names utf8mb4 collate utf8mb4_unicode_ci`)
	checkColumnStringPruningTests(tests)
	tk.MustExec("drop table t")
	tk.MustExec("create table t (a varchar(255) charset utf8mb4 collate utf8mb4_general_ci) partition by range columns(a)" +
		`( partition pNull values less than (""),` +
		`partition paaa values less than ("aaa"),` +
		`partition pAAAA values less than ("AAAA"),` +
		`partition pCCC values less than ("CCC"),` +
		`partition pShrimpsandwich values less than ("Räksmörgås"),` +
		`partition pSushi values less than ("🍣🍣🍣"),` +
		`partition pMax values less than (MAXVALUE))`)
	tk.MustExec(`insert into t values (NULL), ("a"), ("Räkmacka"), ("🍣 is life"), ("🍺 after work?"), ("🍺🍺🍺🍺🍺 for oktoberfest"),("AA"),("aa"),("AAA"),("aaa")`)

	tests = []testStruct{
		// Lower case partition names due to issue#32719
		{sql: `select * from t where a IS NULL`, partitions: "pnull", rows: []string{"<nil>"}},
		{sql: `select * from t where a = 'AA'`, partitions: "paaa", rows: []string{"AA", "aa"}},
		{sql: `select * from t where a = 'AA' collate utf8mb4_bin`, partitions: "paaa", rows: []string{"AA"}},
		{sql: `select * from t where a = 'AAA'`, partitions: "paaaa", rows: []string{"AAA", "aaa"}},
		{sql: `select * from t where a = 'AAA' collate utf8mb4_bin`, partitions: "paaa", rows: []string{}}, // Notice that the it uses _bin collation for partition => not found! #32749
		{sql: `select * from t where a = 'AB'`, partitions: "pccc", rows: []string{}},
		{sql: `select * from t where a = 'aB'`, partitions: "pccc", rows: []string{}},
		{sql: `select * from t where a = '🍣'`, partitions: "psushi", rows: []string{}},
		{sql: `select * from t where a in ('🍣 is life', "Räkmacka", "🍺🍺🍺🍺  after work?")`, partitions: "pshrimpsandwich,psushi,pmax", rows: []string{"Räkmacka", "🍣 is life"}},
		{sql: `select * from t where a in ('AA', 'aaa')`, partitions: "paaa,paaaa", rows: []string{"AA", "AAA", "aa", "aaa"}},
		{sql: `select * from t where a in ('AAA' collate utf8mb4_bin, 'aa')`, partitions: "paaa", rows: []string{"aa"}},          // AAA missing due to #32749, why is AA missing?
		{sql: `select * from t where a in ('AAA', 'aa' collate utf8mb4_bin)`, partitions: "paaaa,psushi", rows: []string{"AAA"}}, // aa, aaa missing due to #32749 also all missing paaa
	}

	tk.MustExec(`set names utf8mb4 collate utf8mb4_bin`)
	checkColumnStringPruningTests(tests)
	tk.MustExec(`set names utf8mb4 collate utf8mb4_general_ci`)
	checkColumnStringPruningTests(tests)
	tk.MustExec(`set names utf8mb4 collate utf8mb4_unicode_ci`)
	checkColumnStringPruningTests(tests)
}

func TestListPartitionPruner(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_partition;")
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec(`set @@session.tidb_regard_null_as_point=false`)
	tk.MustExec("create table t1 (id int, a int, b int                 ) partition by list (    a    ) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t2 (a int, id int, b int) partition by list (a*3 + b - 2*a - b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t3 (b int, id int, a int) partition by list columns (a) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t4 (id int, a int, b int, primary key (a)) partition by list (    a    ) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10));")
	tk.MustExec("create table t5 (a int, id int, b int, unique key (a,b)) partition by list (a*3 + b - 2*a - b) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("create table t6 (b int, id int, a int, unique key (a,b)) partition by list columns (a) (partition p0 values in (1,2,3,4,5), partition p1 values in (6,7,8,9,10,null));")
	tk.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t3 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t4 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10)")
	tk.MustExec("insert into t5 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec("insert into t6 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk.MustExec(`create table t7 (a int unsigned) partition by list (a)(partition p0 values in (0),partition p1 values in (1),partition pnull values in (null),partition p2 values in (2));`)
	tk.MustExec("insert into t7 values (null),(0),(1),(2);")

	// tk2 use to compare the result with normal table.
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("drop database if exists test_partition_2;")
	tk2.MustExec("create database test_partition_2")
	tk2.MustExec("use test_partition_2")
	tk2.MustExec("create table t1 (id int, a int, b int)")
	tk2.MustExec("create table t2 (a int, id int, b int)")
	tk2.MustExec("create table t3 (b int, id int, a int)")
	tk2.MustExec("create table t4 (id int, a int, b int, primary key (a));")
	tk2.MustExec("create table t5 (a int, id int, b int, unique key (a,b));")
	tk2.MustExec("create table t6 (b int, id int, a int, unique key (a,b));")
	tk2.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t3 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t4 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10)")
	tk2.MustExec("insert into t5 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec("insert into t6 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")
	tk2.MustExec(`create table t7 (a int unsigned);`)
	tk2.MustExec("insert into t7 values (null),(0),(1),(2);")

	var input []string
	var output []struct {
		SQL    string
		Result []string
		Plan   []string
	}
	partitionPrunerData := plannercore.GetPartitionPrunerData()
	partitionPrunerData.GetTestCases(t, &input, &output)
	valid := false
	for i, tt := range input {
		testdata.OnRecord(func() {
			output[i].SQL = tt
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt).Rows())
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + tt).Rows())
		})
		tk.MustQuery("explain format = 'brief' " + tt).Check(testkit.Rows(output[i].Plan...))
		result := tk.MustQuery(tt)
		result.Check(testkit.Rows(output[i].Result...))
		// If the query doesn't specified the partition, compare the result with normal table
		if !strings.Contains(tt, "partition(") {
			result.Check(tk2.MustQuery(tt).Rows())
			valid = true
		}
		require.True(t, valid)
	}
}

func TestListColumnsPartitionPruner(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("drop database if exists test_partition;")
	tk.MustExec("create database test_partition")
	tk.MustExec("use test_partition")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk.MustExec("create table t1 (id int, a int, b int) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
	tk.MustExec("create table t2 (id int, a int, b int) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
	tk.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	// tk1 use to test partition table with index.
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("drop database if exists test_partition_1;")
	tk1.MustExec(`set @@session.tidb_regard_null_as_point=false`)
	tk1.MustExec("create database test_partition_1")
	tk1.MustExec("use test_partition_1")
	tk1.MustExec("set @@session.tidb_enable_list_partition = ON")
	tk1.MustExec("create table t1 (id int, a int, b int, unique key (a,b,id)) partition by list columns (b,a) (partition p0 values in ((1,1),(2,2),(3,3),(4,4),(5,5)), partition p1 values in ((6,6),(7,7),(8,8),(9,9),(10,10),(null,10)));")
	tk1.MustExec("create table t2 (id int, a int, b int, unique key (a,b,id)) partition by list columns (id,a,b) (partition p0 values in ((1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5)), partition p1 values in ((6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)));")
	tk1.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk1.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	// tk2 use to compare the result with normal table.
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("drop database if exists test_partition_2;")
	tk2.MustExec(`set @@session.tidb_regard_null_as_point=false`)
	tk2.MustExec("create database test_partition_2")
	tk2.MustExec("use test_partition_2")
	tk2.MustExec("create table t1 (id int, a int, b int)")
	tk2.MustExec("create table t2 (id int, a int, b int)")
	tk2.MustExec("insert into t1 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,10,null)")
	tk2.MustExec("insert into t2 (id,a,b) values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(null,null,null)")

	var input []struct {
		SQL    string
		Pruner string
	}
	var output []struct {
		SQL       string
		Result    []string
		Plan      []string
		IndexPlan []string
	}
	partitionPrunerData := plannercore.GetPartitionPrunerData()
	partitionPrunerData.GetTestCases(t, &input, &output)
	valid := false
	for i, tt := range input {
		// Test for table without index.
		plan := tk.MustQuery("explain format = 'brief' " + tt.SQL)
		planTree := testdata.ConvertRowsToStrings(plan.Rows())
		// Test for table with index.
		indexPlan := tk1.MustQuery("explain format = 'brief' " + tt.SQL)
		indexPlanTree := testdata.ConvertRowsToStrings(indexPlan.Rows())
		testdata.OnRecord(func() {
			output[i].SQL = tt.SQL
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(tt.SQL).Rows())
			// Test for table without index.
			output[i].Plan = planTree
			// Test for table with index.
			output[i].IndexPlan = indexPlanTree
		})
		// compare the plan.
		plan.Check(testkit.Rows(output[i].Plan...))
		indexPlan.Check(testkit.Rows(output[i].IndexPlan...))

		// compare the pruner information.
		checkPrunePartitionInfo(t, tt.SQL, tt.Pruner, planTree)
		checkPrunePartitionInfo(t, tt.SQL, tt.Pruner, indexPlanTree)

		// compare the result.
		result := tk.MustQuery(tt.SQL)
		idxResult := tk1.MustQuery(tt.SQL)
		result.Check(idxResult.Rows())
		result.Check(testkit.Rows(output[i].Result...))

		// If the query doesn't specified the partition, compare the result with normal table
		if !strings.Contains(tt.SQL, "partition(") {
			result.Check(tk2.MustQuery(tt.SQL).Rows())
			valid = true
		}
	}
	require.True(t, valid)
}

func checkPrunePartitionInfo(c *testing.T, query string, infos1 string, plan []string) {
	infos2 := getPartitionInfoFromPlan(plan)
	comment := fmt.Sprintf("the query is: %v, the plan is:\n%v", query, strings.Join(plan, "\n"))
	require.Equal(c, infos1, infos2, comment)
}

type testTablePartitionInfo struct {
	Table      string
	Partitions string
}

// getPartitionInfoFromPlan uses to extract table partition information from the plan tree string. Here is an example, the plan is like below:
//          "Projection_7 80.00 root  test_partition.t1.id, test_partition.t1.a, test_partition.t1.b, test_partition.t2.id, test_partition.t2.a, test_partition.t2.b",
//          "└─HashJoin_9 80.00 root  CARTESIAN inner join",
//          "  ├─TableReader_12(Build) 8.00 root partition:p1 data:Selection_11",
//          "  │ └─Selection_11 8.00 cop[tikv]  1, eq(test_partition.t2.b, 6), in(test_partition.t2.a, 6, 7, 8)",
//          "  │   └─TableFullScan_10 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
//          "  └─TableReader_15(Probe) 10.00 root partition:p0 data:Selection_14",
//          "    └─Selection_14 10.00 cop[tikv]  1, eq(test_partition.t1.a, 5)",
//          "      └─TableFullScan_13 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"
//
// The return table partition info is: t1: p0; t2: p1
func getPartitionInfoFromPlan(plan []string) string {
	infos := make([]testTablePartitionInfo, 0, 2)
	info := testTablePartitionInfo{}
	for _, row := range plan {
		partitions := getFieldValue("partition:", row)
		if partitions != "" {
			info.Partitions = partitions
			continue
		}
		tbl := getFieldValue("table:", row)
		if tbl != "" {
			info.Table = tbl
			infos = append(infos, info)
		}
	}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Table != infos[j].Table {
			return infos[i].Table < infos[j].Table
		}
		return infos[i].Partitions < infos[j].Partitions
	})
	buf := bytes.NewBuffer(nil)
	for i, info := range infos {
		if i > 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(fmt.Sprintf("%v: %v", info.Table, info.Partitions))
	}
	return buf.String()
}
>>>>>>> 7bf5e4e23... planner: Columns in string pruning (#32626) (#32721)

type testPartitionPruneSuit struct {
	store    kv.Storage
	dom      *domain.Domain
	ctx      sessionctx.Context
	testData testutil.TestData
}

func (s *testPartitionPruneSuit) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_partition")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testPartitionPruneSuit) SetUpSuite(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testPartitionPruneSuit) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testPartitionPruneSuit) TestIssue23622(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t2 (a int, b int) partition by range (a) (partition p0 values less than (0), partition p1 values less than (5));")
	tk.MustExec("insert into t2(a) values (-1), (1);")
	tk.MustQuery("select * from t2 where a > 10 or b is NULL order by a;").Check(testkit.Rows("-1 <nil>", "1 <nil>"))
}
