// Copyright 2026 PingCAP, Inc.
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

//! Go `pkg/executor/partition_table_test.go` (`TestBatchGetandPointGetwithHashPartition`, `TestBatchGetforRangeandListPartitionTable`, `TestDML`, `TestDirectReadingWithAgg`, `TestIdexMerge`, `TestOrderByAndLimit`, `TestParallelApply`, `TestParallelApplyWithLimitOnRangeColumnsPartition`, `TestPartitionTableWithDifferentJoin`, `TestPointGetwithRangeAndListPartitionTable`, `TestSplitRegion`, `TestSubqueries`, `TestUnion`, `TestUnsignedPartitionColumn`, `TestView`), on the mock store through the session, as Go's testkit
//! runs them.

#![allow(unused_mut, unused_variables, unused_imports)]

use crate::support::testkit::*;

/// Go `TestBatchGetandPointGetwithHashPartition`.
#[test]
fn test_batch_getand_point_getwith_hash_partition() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_batchget_pointget");
    tk.must_exec("use test_batchget_pointget");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    // hash partition table
    tk.must_exec("create table thash(a int, unique key(a)) partition by hash(a) partitions 4;");
    // regular partition table
    tk.must_exec("create table tregular(a int, unique key(a));");
    let mut vals = Vec::<String>::with_capacity(100);
    // insert data into range partition table and hash partition table
    for i in 0..100 {
        vals.push(format!("({})", i + 1));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular values ",
        vals.join(",")
    ));
    // test PointGet
    for _ in 0..100 {
        // explain select a from t where a = {x}; // x >= 1 and x <= 100 Check if PointGet is used
        // select a from t where a={x}; // the result is {x}
        let x = rand_intn(100) + 1;
        let mut query_hash = format!("select a from thash where a={}", go_v(&x));
        let mut query_regular = format!("select a from tregular where a={}", go_v(&x));
        tk.must_have_plan(&query_hash, "Point_Get");
        {
            let expected = tk.must_query(&query_regular);
            tk.must_query(&query_hash).check_result(&expected);
        }
    }
    // test empty PointGet
    let mut query_hash = "select a from thash where a=200".to_owned();
    tk.must_have_plan(&query_hash, "Point_Get");
    tk.must_query(&query_hash).check_empty();
    // test BatchGet
    for _ in 0..100 {
        // explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
        // select a from t where where a in ({x1}, {x2}, ... {x10});
        let mut points = Vec::<String>::with_capacity(10);
        for _ in 0..10 {
            let x = rand_intn(100) + 1;
            points.push(format!("{}", x));
        }
        let mut query_hash = format!("select a from thash where a in ({})", points.join(","));
        let mut query_regular = format!("select a from tregular where a in ({})", points.join(","));
        tk.must_have_plan(&query_hash, "Point_Get");
        {
            let expected = tk.must_query(&query_regular).sort();
            tk.must_query(&query_hash).sort().check_result(&expected);
        }
    }
}

/// Go `TestBatchGetforRangeandListPartitionTable`.
#[test]
fn test_batch_getfor_rangeand_list_partition_table() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_pointget");
    tk.must_exec("use test_pointget");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    // list partition table
    tk.must_exec(r#"create table tlist(a int, b int, unique index idx_a(a), index idx_b(b)) partition by list(a)(partition p0 values in (1, 2, 3, 4), partition p1 values in (5, 6, 7, 8), partition p2 values in (9, 10, 11, 12));"#);
    // range partition table
    tk.must_exec(r#"create table trange(a int, unique key(a)) partition by range(a) (partition p0 values less than (30), partition p1 values less than (60), partition p2 values less than (90), partition p3 values less than (120));"#);
    // hash partition table
    tk.must_exec(
        "create table thash(a int unsigned, unique key(a)) partition by hash(a) partitions 4;",
    );
    // insert data into list partition table
    tk.must_exec("insert into tlist values(1,1), (2,2), (3, 3), (4, 4), (5,5), (6, 6), (7,7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12);");
    // regular partition table
    tk.must_exec("create table tregular1(a int, unique key(a));");
    tk.must_exec("create table tregular2(a int, unique key(a));");
    let mut vals = Vec::<String>::with_capacity(100);
    // insert data into range partition table and hash partition table
    for i in 0..100 {
        vals.push(format!("({})", i + 1));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular1 values ",
        vals.join(",")
    ));
    tk.must_exec("insert into tregular2 values (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)");
    // test BatchGet
    for _ in 0..100 {
        // explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
        // select a from t where where a in ({x1}, {x2}, ... {x10});
        let mut points = Vec::<String>::with_capacity(10);
        for _ in 0..10 {
            let x = rand_intn(100) + 1;
            points.push(format!("{}", x));
        }
        let mut query_regular1 =
            format!("select a from tregular1 where a in ({})", points.join(","));
        let mut query_hash = format!("select a from thash where a in ({})", points.join(","));
        tk.must_have_plan(&query_hash, "Batch_Point_Get");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_hash).sort().check_result(&expected);
        }
        let mut query_range = format!("select a from trange where a in ({})", points.join(","));
        tk.must_have_plan(&query_range, "Batch_Point_Get");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_range).sort().check_result(&expected);
        }
        points = Vec::<String>::with_capacity(10);
        for _ in 0..10 {
            let x = rand_intn(12) + 1;
            points.push(format!("{}", x));
        }
        let mut query_regular2 =
            format!("select a from tregular2 where a in ({})", points.join(","));
        let mut query_list = format!("select a from tlist where a in ({})", points.join(","));
        tk.must_have_plan(&query_list, "Batch_Point_Get");
        {
            let expected = tk.must_query(&query_regular2).sort();
            tk.must_query(&query_list).sort().check_result(&expected);
        }
    }
    // test different data type
    // unsigned flag
    // partition table and reguar table pair
    tk.must_exec(r#"create table trange3(a int unsigned, unique key(a)) partition by range(a) (partition p0 values less than (30), partition p1 values less than (60), partition p2 values less than (90), partition p3 values less than (120));"#);
    tk.must_exec("create table tregular3(a int unsigned, unique key(a));");
    vals = Vec::<String>::with_capacity(100);
    // insert data into range partition table and hash partition table
    for i in 0..100 {
        vals.push(format!("({})", i + 1));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange3 values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular3 values ",
        vals.join(",")
    ));
    // test BatchGet
    // explain select a from t where a in ({x1}, {x2}, ... {x10}); // BatchGet is used
    // select a from t where where a in ({x1}, {x2}, ... {x10});
    let mut points = Vec::<String>::with_capacity(10);
    for _ in 0..10 {
        let x = rand_intn(100) + 1;
        points.push(format!("{}", x));
    }
    let mut query_regular = format!("select a from tregular3 where a in ({})", points.join(","));
    let mut query_range = format!("select a from trange3 where a in ({})", points.join(","));
    tk.must_have_plan(&query_range, "Batch_Point_Get");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_range).sort().check_result(&expected);
    }
}

/// Go `TestDML`.
#[test]
fn test_dml() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_DML");
    tk.must_exec("use test_DML");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec(r#"create table tinner (a int, b int)"#);
    tk.must_exec(r#"create table thash (a int, b int) partition by hash(a) partitions 4"#);
    tk.must_exec(r#"create table trange (a int, b int) partition by range(a) (partition p0 values less than(10000), partition p1 values less than(20000), partition p2 values less than(30000), partition p3 values less than(40000), partition p4 values less than MAXVALUE)"#);
    let mut vals = Vec::<String>::with_capacity(50);
    for _ in 0..50 {
        vals.push(format!("({}, {})", rand_intn(40000), rand_intn(40000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into tinner values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into thash values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into trange values "#,
        vals.join(", ")
    ));
    // delete, insert, replace, update
    for _ in 0..200 {
        let mut pattern = String::new();
        match rand_intn(4) {
            0 => {
                let col = ["a", "b"][(rand_intn(2)) as usize];
                let l = rand_intn(40000);
                let r = l + rand_intn(5000);
                pattern = format!(
                    r#"delete from %v where {}>{} and {}<{}"#,
                    go_v(&col),
                    go_v(&l),
                    go_v(&col),
                    go_v(&r)
                );
            }
            1 => {
                let (mut a, mut b) = (rand_intn(40000), rand_intn(40000));
                pattern = format!(r#"insert into %v values ({}, {})"#, go_v(&a), go_v(&b));
            }
            2 => {
                let (mut a, mut b) = (rand_intn(40000), rand_intn(40000));
                pattern = format!(
                    r#"replace into %v(a, b) values ({}, {})"#,
                    go_v(&a),
                    go_v(&b)
                );
            }
            3 => {
                let col = ["a", "b"][(rand_intn(2)) as usize];
                let l = rand_intn(40000);
                let r = l + rand_intn(5000);
                let x = rand_intn(1000) - 500;
                pattern = format!(
                    r#"update %v set {}={}+{} where {}>{} and {}<{}"#,
                    go_v(&col),
                    go_v(&col),
                    go_v(&x),
                    go_v(&col),
                    go_v(&l),
                    go_v(&col),
                    go_v(&r)
                );
            }
            _ => {}
        }
        for tbl in ["tinner", "thash", "trange"] {
            tk.must_exec(&go_sprintf(&pattern, &[&tbl]));
        }
        // check
        let mut r = tk.must_query(r#"select * from tinner"#).sort();
        tk.must_query(r#"select * from thash"#)
            .sort()
            .check_result(&r);
        tk.must_query(r#"select * from trange"#)
            .sort()
            .check_result(&r);
    }
}

/// Go `TestDirectReadingWithAgg`.
#[test]
fn test_direct_reading_with_agg() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_dr_agg");
    tk.must_exec("use test_dr_agg");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    // list partition table
    tk.must_exec(r#"create table tlist(a int, b int, index idx_a(a), index idx_b(b)) partition by list(a)(partition p0 values in (1, 2, 3, 4), partition p1 values in (5, 6, 7, 8), partition p2 values in (9, 10, 11, 12));"#);
    // range partition table
    tk.must_exec(r#"create table trange(a int, b int, index idx_a(a), index idx_b(b)) partition by range(a) (partition p0 values less than(300), partition p1 values less than (500), partition p2 values less than(1100));"#);
    // hash partition table
    tk.must_exec(r#"create table thash(a int, b int) partition by hash(a) partitions 4;"#);
    // regular table
    tk.must_exec("create table tregular1(a int, b int, index idx_a(a))");
    tk.must_exec("create table tregular2(a int, b int, index idx_a(a))");
    // generate some random data to be inserted
    let mut vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(1100), rand_intn(2000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular1 values ",
        vals.join(",")
    ));
    vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(12) + 1, rand_intn(20)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into tlist values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular2 values ",
        vals.join(",")
    ));
    // test range partition
    for _ in 0..200 {
        // select /*+ stream_agg() */ a from t where a > ? group by a;
        // select /*+ hash_agg() */ a from t where a > ? group by a;
        // select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
        // select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
        let x = rand_intn(1099);
        let mut query_partition1 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a > {} group by a;",
            go_v(&x)
        );
        let mut query_regular1 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > {} group by a;",
            go_v(&x)
        );
        tk.must_have_plan(&query_partition1, "StreamAgg");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_partition1)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition2 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a > {} group by a;",
            go_v(&x)
        );
        let mut query_regular2 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > {} group by a;",
            go_v(&x)
        );
        tk.must_have_plan(&query_partition2, "HashAgg");
        {
            let expected = tk.must_query(&query_regular2).sort();
            tk.must_query(&query_partition2)
                .sort()
                .check_result(&expected);
        }
        let y = rand_intn(1099);
        let z = rand_intn(1099);
        let mut query_partition3 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from trange where a in({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        let mut query_regular3 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        tk.must_have_plan(&query_partition3, "StreamAgg");
        {
            let expected = tk.must_query(&query_regular3).sort();
            tk.must_query(&query_partition3)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition4 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from trange where a in ({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        let mut query_regular4 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in ({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        tk.must_have_plan(&query_partition4, "HashAgg");
        {
            let expected = tk.must_query(&query_regular4).sort();
            tk.must_query(&query_partition4)
                .sort()
                .check_result(&expected);
        }
    }
    // test hash partition
    for _ in 0..200 {
        // select /*+ stream_agg() */ a from t where a > ? group by a;
        // select /*+ hash_agg() */ a from t where a > ? group by a;
        // select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
        // select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
        let x = rand_intn(1099);
        let mut query_partition1 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a > {} group by a;",
            go_v(&x)
        );
        let mut query_regular1 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a > {} group by a;",
            go_v(&x)
        );
        tk.must_have_plan(&query_partition1, "StreamAgg");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_partition1)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition2 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a > {} group by a;",
            go_v(&x)
        );
        let mut query_regular2 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a > {} group by a;",
            go_v(&x)
        );
        tk.must_have_plan(&query_partition2, "HashAgg");
        {
            let expected = tk.must_query(&query_regular2).sort();
            tk.must_query(&query_partition2)
                .sort()
                .check_result(&expected);
        }
        let y = rand_intn(1099);
        let z = rand_intn(1099);
        let mut query_partition3 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from thash where a in({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        let mut query_regular3 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular1 where a in({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        tk.must_have_plan(&query_partition3, "StreamAgg");
        {
            let expected = tk.must_query(&query_regular3).sort();
            tk.must_query(&query_partition3)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition4 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from thash where a in ({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        let mut query_regular4 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular1 where a in ({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        tk.must_have_plan(&query_partition4, "HashAgg");
        {
            let expected = tk.must_query(&query_regular4).sort();
            tk.must_query(&query_partition4)
                .sort()
                .check_result(&expected);
        }
    }
    // test list partition
    for _ in 0..200 {
        // select /*+ stream_agg() */ a from t where a > ? group by a;
        // select /*+ hash_agg() */ a from t where a > ? group by a;
        // select /*+ stream_agg() */ a from t where a in(?, ?, ?) group by a;
        // select /*+ hash_agg() */ a from t where a in (?, ?, ?) group by a;
        let x = rand_intn(12) + 1;
        let mut query_partition1 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a > {} group by a;",
            go_v(&x)
        );
        let mut query_regular1 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a > {} group by a;",
            go_v(&x)
        );
        tk.must_have_plan(&query_partition1, "StreamAgg");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_partition1)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition2 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a > {} group by a;",
            go_v(&x)
        );
        let mut query_regular2 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a > {} group by a;",
            go_v(&x)
        );
        tk.must_have_plan(&query_partition2, "HashAgg");
        {
            let expected = tk.must_query(&query_regular2).sort();
            tk.must_query(&query_partition2)
                .sort()
                .check_result(&expected);
        }
        let y = rand_intn(12) + 1;
        let z = rand_intn(12) + 1;
        let mut query_partition3 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tlist where a in({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        let mut query_regular3 = format!(
            "select /*+ stream_agg() */ count(*), sum(b), max(b), a from tregular2 where a in({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        tk.must_have_plan(&query_partition3, "StreamAgg");
        {
            let expected = tk.must_query(&query_regular3).sort();
            tk.must_query(&query_partition3)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition4 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tlist where a in ({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        let mut query_regular4 = format!(
            "select /*+ hash_agg() */ count(*), sum(b), max(b), a from tregular2 where a in ({}, {}, {}) group by a;",
            go_v(&x),
            go_v(&y),
            go_v(&z)
        );
        tk.must_have_plan(&query_partition4, "HashAgg");
        {
            let expected = tk.must_query(&query_regular4).sort();
            tk.must_query(&query_partition4)
                .sort()
                .check_result(&expected);
        }
    }
}

/// Go `TestIdexMerge`.
#[test]
fn test_idex_merge() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_idx_merge");
    tk.must_exec("use test_idx_merge");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    // list partition table
    tk.must_exec(r#"create table tlist(a int, b int, primary key(a) clustered, index idx_b(b)) partition by list(a)(partition p0 values in (1, 2, 3, 4), partition p1 values in (5, 6, 7, 8), partition p2 values in (9, 10, 11, 12));"#);
    // range partition table
    tk.must_exec(r#"create table trange(a int, b int, primary key(a) clustered, index idx_b(b)) partition by range(a) (partition p0 values less than(300), partition p1 values less than (500), partition p2 values less than(1100));"#);
    // hash partition table
    tk.must_exec(r#"create table thash(a int, b int, primary key(a) clustered, index idx_b(b)) partition by hash(a) partitions 4;"#);
    // regular table
    tk.must_exec("create table tregular1(a int, b int, primary key(a) clustered)");
    tk.must_exec("create table tregular2(a int, b int, primary key(a) clustered)");
    // generate some random data to be inserted
    let mut vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(1100), rand_intn(2000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert ignore into trange values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert ignore into thash values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert ignore into tregular1 values ",
        vals.join(",")
    ));
    vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(12) + 1, rand_intn(20)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert ignore into tlist values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert ignore into tregular2 values ",
        vals.join(",")
    ));
    // test range partition
    for _ in 0..100 {
        let x1 = rand_intn(1099);
        let x2 = rand_intn(1099);
        let mut query_partition1 = format!(
            "select /*+ use_index_merge(trange) */ * from trange where a > {} or b < {};",
            go_v(&x1),
            go_v(&x2)
        );
        let mut query_regular1 = format!(
            "select /*+ use_index_merge(tregular1) */ * from tregular1 where a > {} or b < {};",
            go_v(&x1),
            go_v(&x2)
        );
        tk.must_have_plan(&query_partition1, "IndexMerge");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_partition1)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition2 = format!(
            "select /*+ use_index_merge(trange) */ * from trange where a > {} or b > {};",
            go_v(&x1),
            go_v(&x2)
        );
        let mut query_regular2 = format!(
            "select /*+ use_index_merge(tregular1) */ * from tregular1 where a > {} or b > {};",
            go_v(&x1),
            go_v(&x2)
        );
        tk.must_have_plan(&query_partition2, "IndexMerge");
        {
            let expected = tk.must_query(&query_regular2).sort();
            tk.must_query(&query_partition2)
                .sort()
                .check_result(&expected);
        }
    }
    // test hash partition
    for _ in 0..100 {
        let x1 = rand_intn(1099);
        let x2 = rand_intn(1099);
        let mut query_partition1 = format!(
            "select /*+ use_index_merge(thash) */ * from thash where a > {} or b < {};",
            go_v(&x1),
            go_v(&x2)
        );
        let mut query_regular1 = format!(
            "select /*+ use_index_merge(tregualr1) */ * from tregular1 where a > {} or b < {};",
            go_v(&x1),
            go_v(&x2)
        );
        tk.must_have_plan(&query_partition1, "IndexMerge");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_partition1)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition2 = format!(
            "select /*+ use_index_merge(thash) */ * from thash where a > {} or b > {};",
            go_v(&x1),
            go_v(&x2)
        );
        let mut query_regular2 = format!(
            "select /*+ use_index_merge(tregular1) */ * from tregular1 where a > {} or b > {};",
            go_v(&x1),
            go_v(&x2)
        );
        tk.must_have_plan(&query_partition2, "IndexMerge");
        {
            let expected = tk.must_query(&query_regular2).sort();
            tk.must_query(&query_partition2)
                .sort()
                .check_result(&expected);
        }
    }
    // test list partition
    for _ in 0..100 {
        let x1 = rand_intn(12) + 1;
        let x2 = rand_intn(12) + 1;
        let mut query_partition1 = format!(
            "select /*+ use_index_merge(tlist) */ * from tlist where a > {} or b < {};",
            go_v(&x1),
            go_v(&x2)
        );
        let mut query_regular1 = format!(
            "select /*+ use_index_merge(tregular2) */ * from tregular2 where a > {} or b < {};",
            go_v(&x1),
            go_v(&x2)
        );
        tk.must_have_plan(&query_partition1, "IndexMerge");
        {
            let expected = tk.must_query(&query_regular1).sort();
            tk.must_query(&query_partition1)
                .sort()
                .check_result(&expected);
        }
        let mut query_partition2 = format!(
            "select /*+ use_index_merge(tlist) */ * from tlist where a > {} or b > {};",
            go_v(&x1),
            go_v(&x2)
        );
        let mut query_regular2 = format!(
            "select /*+ use_index_merge(tregular2) */ * from tregular2 where a > {} or b > {};",
            go_v(&x1),
            go_v(&x2)
        );
        tk.must_have_plan(&query_partition2, "IndexMerge");
        {
            let expected = tk.must_query(&query_regular2).sort();
            tk.must_query(&query_partition2)
                .sort()
                .check_result(&expected);
        }
    }
}

/// Go `TestOrderByAndLimit`.
#[test]
fn test_order_by_and_limit() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_orderby_limit");
    tk.must_exec("use test_orderby_limit");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    // range partition table
    tk.must_exec(r#"create table trange(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b)) partition by range(a) (partition p0 values less than(300), partition p1 values less than (500), partition p2 values less than(1100));"#);
    // hash partition table
    tk.must_exec("create table thash(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b)) partition by hash(a) partitions 4;");
    // regular table
    tk.must_exec(
        "create table tregular(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b))",
    );
    // range partition table with int pk
    tk.must_exec(r#"create table trange_intpk(a int primary key, b int) partition by range(a) (partition p0 values less than(300), partition p1 values less than (500), partition p2 values less than(1100));"#);
    // hash partition table with int pk
    tk.must_exec(
        "create table thash_intpk(a int primary key, b int) partition by hash(a) partitions 4;",
    );
    // regular table with int pk
    tk.must_exec("create table tregular_intpk(a int primary key, b int)");
    // range partition table with clustered index
    tk.must_exec(r#"create table trange_clustered(a int, b int, primary key(a, b) clustered) partition by range(a) (partition p0 values less than(300), partition p1 values less than (500), partition p2 values less than(1100));"#);
    // hash partition table with clustered index
    tk.must_exec("create table thash_clustered(a int, b int, primary key(a, b) clustered) partition by hash(a) partitions 4;");
    // regular table with clustered index
    tk.must_exec("create table tregular_clustered(a int, b int, primary key(a, b) clustered)");
    let mut list_vals = Vec::<i64>::with_capacity(1000);
    for i in 0..1000 {
        list_vals.push(i.clone());
    }
    rand::seq::SliceRandom::shuffle(list_vals.as_mut_slice(), &mut rand::rng());
    let mut list_vals1 = String::new();
    let mut list_vals2 = String::new();
    let mut list_vals3 = String::new();
    for i in 0..=300 {
        list_vals1 += &list_vals[i].to_string();
        if i != 300 {
            list_vals1 += ",";
        }
    }
    for i in 301..=600 {
        list_vals2 += &list_vals[i].to_string();
        if i != 600 {
            list_vals2 += ",";
        }
    }
    for i in 601..=999 {
        list_vals3 += &list_vals[i].to_string();
        if i != 999 {
            list_vals3 += ",";
        }
    }
    tk.must_exec(&format!(r#"create table tlist_intpk(a int primary key, b int) partition by list(a)(partition p1 values in ({}), partition p2 values in ({}), partition p3 values in ({}) )"#, go_v(&list_vals1), go_v(&list_vals2), go_v(&list_vals3)));
    tk.must_exec(&format!(r#"create table tlist(a int, b int, index idx_a(a), index idx_b(b), index idx_ab(a, b)) partition by list(a)(partition p1 values in ({}), partition p2 values in ({}), partition p3 values in ({}) )"#, go_v(&list_vals1), go_v(&list_vals2), go_v(&list_vals3)));
    tk.must_exec(&format!(r#"create table tlist_clustered(a int, b int, primary key(a, b)) partition by list(a)(partition p1 values in ({}), partition p2 values in ({}), partition p3 values in ({}) )"#, go_v(&list_vals1), go_v(&list_vals2), go_v(&list_vals3)));
    // generate some random data to be inserted
    let mut vals = Vec::<String>::with_capacity(1000);
    for _ in 0..1000 {
        vals.push(format!("({}, {})", rand_intn(550), rand_intn(1000)));
    }
    let mut dedup_vals_a = Vec::<String>::with_capacity(1000);
    let mut dedup_map_a = std::collections::HashSet::new();
    for _ in 0..1000 {
        let val_a = rand_intn(550);
        if dedup_map_a.contains(&val_a) {
            continue;
        }
        dedup_vals_a.push(format!("({}, {})", val_a, rand_intn(1000)));
        dedup_map_a.insert(val_a.clone());
    }
    let mut dedup_vals_ab = Vec::<String>::with_capacity(1000);
    let mut dedup_map_ab = std::collections::HashSet::new();
    for _ in 0..1000 {
        let mut val = format!("({}, {})", rand_intn(550), rand_intn(1000));
        if dedup_map_ab.contains(&val) {
            continue;
        }
        dedup_vals_ab.push(val.clone());
        dedup_map_ab.insert(val.clone());
    }
    let mut val_inserted = vals.join(",");
    let mut val_dedup_a_inserted = dedup_vals_a.join(",");
    let mut val_dedup_ab_inserted = dedup_vals_ab.join(",");
    tk.must_exec(&format!("{}{}", "insert into trange values ", val_inserted));
    tk.must_exec(&format!("{}{}", "insert into thash values ", val_inserted));
    tk.must_exec(&format!("{}{}", "insert into tlist values", val_inserted));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular values ", val_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange_intpk values ", val_dedup_a_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash_intpk values ", val_dedup_a_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tlist_intpk values ", val_dedup_a_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular_intpk values ", val_dedup_a_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange_clustered values ", val_dedup_ab_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash_clustered values ", val_dedup_ab_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tlist_clustered values ", val_dedup_ab_inserted
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular_clustered values ", val_dedup_ab_inserted
    ));
    tk.must_exec("analyze table trange");
    tk.must_exec("analyze table trange_intpk");
    tk.must_exec("analyze table trange_clustered");
    tk.must_exec("analyze table thash");
    tk.must_exec("analyze table thash_intpk");
    tk.must_exec("analyze table thash_clustered");
    tk.must_exec("analyze table tregular");
    tk.must_exec("analyze table tregular_intpk");
    tk.must_exec("analyze table tregular_clustered");
    tk.must_exec("analyze table tlist");
    tk.must_exec("analyze table tlist_intpk");
    tk.must_exec("analyze table tlist_clustered");
    // Create virtual tiflash replica info.
    set_tiflash_replica(&tk, "test_orderby_limit", "trange");
    set_tiflash_replica(&tk, "test_orderby_limit", "thash");
    set_tiflash_replica(&tk, "test_orderby_limit", "tlist");
    set_tiflash_replica(&tk, "test_orderby_limit", "tregular");
    set_tiflash_replica(&tk, "test_orderby_limit", "trange_intpk");
    set_tiflash_replica(&tk, "test_orderby_limit", "thash_intpk");
    set_tiflash_replica(&tk, "test_orderby_limit", "tlist_intpk");
    set_tiflash_replica(&tk, "test_orderby_limit", "tregular_intpk");
    set_tiflash_replica(&tk, "test_orderby_limit", "trange_clustered");
    set_tiflash_replica(&tk, "test_orderby_limit", "thash_clustered");
    set_tiflash_replica(&tk, "test_orderby_limit", "tlist_clustered");
    set_tiflash_replica(&tk, "test_orderby_limit", "tregular_clustered");
    tk.must_exec("set @@session.tidb_isolation_read_engines=\"tikv\"");
    // test indexLookUp
    for _ in 0..50 {
        // explain select * from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
        // select * from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
        let x = rand_intn(549);
        let y = rand_intn(500) + 1;
        let mut query_partition = format!(
            "select * from trange use index(idx_a) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select * from tregular use index(idx_a) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_partition, "IndexLookUp");
        {
            let expected = tk.must_query(&query_regular);
            tk.must_query(&query_partition).check_result(&expected);
        }
    }
    // test indexLookUp with order property pushed down.
    for i in 0..50 {
        if i % 2 == 0 {
            tk.must_exec("set tidb_partition_prune_mode = `static-only`");
        } else {
            tk.must_exec("set tidb_partition_prune_mode = `dynamic-only`");
        }
        // explain select * from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
        // select * from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
        let x = rand_intn(549);
        let y = rand_intn(1000) + 1;
        // Since we only use order by a not order by a, b, the result is not stable when we read both a and b.
        // We cut the max element so that the result can be stable.
        let mut max_ele = tk.must_query(&format!("select ifnull(max(a), 1100) from (select * from tregular use index(idx_a) where a > {} order by a limit {}) t", go_v(&x), go_v(&y))).rows()[0][0].clone();
        let mut query_range_partition_with_limit_hint = format!(
            "select /*+ LIMIT_TO_COP() */ * from trange use index(idx_a) where a > {} and a < {} order by a limit {}",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut query_hash_partition_with_limit_hint = format!(
            "select /*+ LIMIT_TO_COP() */ * from thash use index(idx_a) where a > {} and a < {} order by a limit {}",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut query_list_partition_with_limit_hint = format!(
            "select /*+ LIMIT_TO_COP() */ * from tlist use index(idx_a) where a > {} and a < {} order by a limit {}",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select * from tregular use index(idx_a) where a > {} and a < {} order by a limit {};",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut regular_result = tk.must_query(&query_regular).sort();
        if (regular_result.len() as i64) > 0 {
            tk.must_have_plan(&query_range_partition_with_limit_hint, "Limit");
            tk.must_have_plan(&query_range_partition_with_limit_hint, "IndexLookUp");
            tk.must_have_plan(&query_hash_partition_with_limit_hint, "Limit");
            tk.must_have_plan(&query_hash_partition_with_limit_hint, "IndexLookUp");
            tk.must_have_plan(&query_list_partition_with_limit_hint, "Limit");
            tk.must_have_plan(&query_list_partition_with_limit_hint, "IndexLookUp");
        }
        if i % 2 != 0 {
            tk.must_not_have_plan(&query_range_partition_with_limit_hint, "TopN");
            tk.must_not_have_plan(&query_hash_partition_with_limit_hint, "TopN");
            tk.must_not_have_plan(&query_list_partition_with_limit_hint, "TopN");
        }
        tk.must_query(&query_range_partition_with_limit_hint)
            .sort()
            .check_result(&regular_result);
        tk.must_query(&query_hash_partition_with_limit_hint)
            .sort()
            .check_result(&regular_result);
        tk.must_query(&query_list_partition_with_limit_hint)
            .sort()
            .check_result(&regular_result);
    }
    // test indexLookUp with order property pushed down.
    for i in 0..50 {
        if i % 2 == 0 {
            tk.must_exec("set tidb_partition_prune_mode = `static-only`");
        } else {
            tk.must_exec("set tidb_partition_prune_mode = `dynamic-only`");
        }
        // explain select * from t where b > {y}  use index(idx_b) order by b limit {x}; // check if IndexLookUp is used
        // select * from t where b > {y} use index(idx_b) order by b limit {x}; // it can return the correct result
        let x = rand_intn(549);
        let y = rand_intn(500) + 1;
        let mut max_ele = tk.must_query(&format!("select ifnull(max(b), 2000) from (select * from tregular use index(idx_b) where b > {} order by b limit {}) t", go_v(&x), go_v(&y))).rows()[0][0].clone();
        let mut query_range_partition_with_limit_hint = format!(
            "select /*+ LIMIT_TO_COP() */ * from trange use index(idx_b) where b > {} and b < {} order by b limit {}",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut query_hash_partition_with_limit_hint = format!(
            "select /*+ LIMIT_TO_COP() */ * from thash use index(idx_b) where b > {} and b < {} order by b limit {}",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut query_list_partition_with_limit_hint = format!(
            "select /*+ LIMIT_TO_COP() */ * from tlist use index(idx_b) where b > {} and b < {} order by b limit {}",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select * from tregular use index(idx_b) where b > {} and b < {} order by b limit {};",
            go_v(&x),
            go_v(&max_ele),
            go_v(&y)
        );
        let mut regular_result = tk.must_query(&query_regular).sort();
        if (regular_result.len() as i64) > 0 {
            tk.must_have_plan(&query_range_partition_with_limit_hint, "Limit");
            tk.must_have_plan(&query_range_partition_with_limit_hint, "IndexLookUp");
            tk.must_have_plan(&query_hash_partition_with_limit_hint, "Limit");
            tk.must_have_plan(&query_hash_partition_with_limit_hint, "IndexLookUp");
            tk.must_have_plan(&query_list_partition_with_limit_hint, "Limit");
            tk.must_have_plan(&query_list_partition_with_limit_hint, "IndexLookUp");
        }
        if i % 2 != 0 {
            tk.must_not_have_plan(&query_range_partition_with_limit_hint, "TopN");
            tk.must_not_have_plan(&query_hash_partition_with_limit_hint, "TopN");
            tk.must_not_have_plan(&query_list_partition_with_limit_hint, "TopN");
        }
        tk.must_query(&query_range_partition_with_limit_hint)
            .sort()
            .check_result(&regular_result);
        tk.must_query(&query_hash_partition_with_limit_hint)
            .sort()
            .check_result(&regular_result);
        tk.must_query(&query_list_partition_with_limit_hint)
            .sort()
            .check_result(&regular_result);
    }
    tk.must_exec("set tidb_partition_prune_mode = default");
    // test tableReader
    for _ in 0..50 {
        // explain select * from t where a > {y}  ignore index(idx_a) order by a limit {x}; // check if IndexLookUp is used
        // select * from t where a > {y} ignore index(idx_a) order by a limit {x}; // it can return the correct result
        let x = rand_intn(549);
        let y = rand_intn(500) + 1;
        let mut query_partition = format!(
            "select * from trange ignore index(idx_a, idx_ab) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select * from tregular ignore index(idx_a, idx_ab) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_partition, "TableReader");
        {
            let expected = tk.must_query(&query_regular);
            tk.must_query(&query_partition).check_result(&expected);
        }
    }
    // test tableReader with order property pushed down.
    for _ in 0..50 {
        // explain select * from t where a > {y}  ignore index(idx_a) order by a limit {x}; // check if IndexLookUp is used
        // select * from t where a > {y} ignore index(idx_a) order by a limit {x}; // it can return the correct result
        let x = rand_intn(549);
        let y = rand_intn(500) + 1;
        let mut query_range_partition = format!(
            "select /*+ LIMIT_TO_COP() */ * from trange ignore index(idx_a, idx_ab) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_hash_partition = format!(
            "select /*+ LIMIT_TO_COP() */ * from thash ignore index(idx_a, idx_ab) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_list_partition = format!(
            "select /*+ LIMIT_TO_COP() */ * from tlist ignore index(idx_a, idx_ab) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select * from tregular ignore index(idx_a) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_range_partition, "TableReader");
        tk.must_have_plan(&query_hash_partition, "TableReader");
        tk.must_have_plan(&query_list_partition, "TableReader");
        tk.must_not_have_plan(&query_range_partition, "Limit");
        tk.must_not_have_plan(&query_hash_partition, "Limit");
        tk.must_not_have_plan(&query_list_partition, "Limit");
        let mut regular_result = tk.must_query(&query_regular);
        tk.must_query(&query_range_partition)
            .check_result(&regular_result);
        tk.must_query(&query_hash_partition)
            .check_result(&regular_result);
        tk.must_query(&query_list_partition)
            .check_result(&regular_result);
        // test int pk
        // To be simplified, we only read column a.
        query_range_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from trange_intpk use index(primary) where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        query_hash_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from thash_intpk use index(primary) where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        query_list_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from tlist_intpk use index(primary) where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        query_regular = format!(
            "select a from tregular_intpk where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_range_partition, "TableReader");
        tk.must_have_plan(&query_hash_partition, "TableReader");
        tk.must_have_plan(&query_list_partition, "TableReader");
        tk.must_have_plan(&query_range_partition, "Limit");
        tk.must_not_have_plan(&query_range_partition, "TopN");
        tk.must_have_plan(&query_hash_partition, "Limit");
        tk.must_not_have_plan(&query_hash_partition, "TopN");
        tk.must_have_plan(&query_list_partition, "Limit");
        tk.must_not_have_plan(&query_list_partition, "TopN");
        regular_result = tk.must_query(&query_regular);
        tk.must_query(&query_range_partition)
            .check_result(&regular_result);
        tk.must_query(&query_hash_partition)
            .check_result(&regular_result);
        tk.must_query(&query_list_partition)
            .check_result(&regular_result);
        // test clustered index
        query_range_partition = format!(
            "select /*+ LIMIT_TO_COP() */ * from trange_clustered use index(primary) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        query_hash_partition = format!(
            "select /*+ LIMIT_TO_COP() */ * from thash_clustered use index(primary) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        query_list_partition = format!(
            "select /*+ LIMIT_TO_COP() */ * from tlist_clustered use index(primary) where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        query_regular = format!(
            "select * from tregular_clustered where a > {} order by a, b limit {};",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_range_partition, "TableReader");
        tk.must_have_plan(&query_hash_partition, "TableReader");
        tk.must_have_plan(&query_list_partition, "TableReader");
        tk.must_have_plan(&query_range_partition, "Limit");
        tk.must_have_plan(&query_hash_partition, "Limit");
        tk.must_have_plan(&query_list_partition, "Limit");
        tk.must_not_have_plan(&query_range_partition, "TopN");
        tk.must_not_have_plan(&query_hash_partition, "TopN");
        tk.must_not_have_plan(&query_list_partition, "TopN");
        regular_result = tk.must_query(&query_regular);
        tk.must_query(&query_range_partition)
            .check_result(&regular_result);
        tk.must_query(&query_hash_partition)
            .check_result(&regular_result);
        tk.must_query(&query_list_partition)
            .check_result(&regular_result);
        tk.must_exec(" set @@tidb_allow_mpp=1;");
        tk.must_exec("set @@session.tidb_isolation_read_engines=\"tiflash,tikv\"");
        let mut query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[trange_intpk]) */ * from trange_intpk where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        // but order is not pushed
        tk.must_not_have_plan(&query_partition_with_ti_flash, "Limit");
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[trange_intpk]) */ /*+ LIMIT_TO_COP() */ * from trange_intpk where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        // but order is not pushed
        tk.must_not_have_plan(&query_partition_with_ti_flash, "Limit");
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[trange_clustered]) */ * from trange_clustered where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[trange_clustered]) */ /*+ LIMIT_TO_COP() */ * from trange_clustered where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        // but order is not pushed
        tk.must_not_have_plan(&query_partition_with_ti_flash, "Limit");
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[thash_intpk]) */ * from thash_intpk where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[thash_intpk]) */ /*+ LIMIT_TO_COP() */ * from thash_intpk where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        // but order is not pushed
        tk.must_not_have_plan(&query_partition_with_ti_flash, "Limit");
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[thash_clustered]) */ * from thash_clustered where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[thash_clustered]) */ /*+ LIMIT_TO_COP() */ * from thash_clustered where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        // but order is not pushed
        tk.must_not_have_plan(&query_partition_with_ti_flash, "Limit");
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[tlist_intpk]) */ * from tlist_intpk where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[tlist_intpk]) */ /*+ LIMIT_TO_COP() */ * from tlist_intpk where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        // but order is not pushed
        tk.must_not_have_plan(&query_partition_with_ti_flash, "Limit");
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[tlist_clustered]) */ * from tlist_clustered where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        query_partition_with_ti_flash = format!(
            "select /*+ read_from_storage(tiflash[tlist_clustered]) */ /*+ LIMIT_TO_COP() */ * from tlist_clustered where a > {} order by a limit {}",
            go_v(&x),
            go_v(&y)
        );
        // check if tiflash is used
        assert!(tk.has_tiflash_plan(&query_partition_with_ti_flash));
        // but order is not pushed
        tk.must_not_have_plan(&query_partition_with_ti_flash, "Limit");
        tk.must_exec(" set @@tidb_allow_mpp=0;");
        tk.must_exec("set @@session.tidb_isolation_read_engines=\"tikv\"");
    }
    // test indexReader
    for _ in 0..50 {
        // explain select a from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
        // select a from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
        let x = rand_intn(549);
        let y = rand_intn(500) + 1;
        let mut query_partition = format!(
            "select a from trange use index(idx_a) where a > {} order by a limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select a from tregular use index(idx_a) where a > {} order by a limit {};",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_partition, "IndexReader");
        {
            let expected = tk.must_query(&query_regular);
            tk.must_query(&query_partition).check_result(&expected);
        }
    }
    // test indexReader with order property pushed down.
    for _ in 0..50 {
        // explain select a from t where a > {y}  use index(idx_a) order by a limit {x}; // check if IndexLookUp is used
        // select a from t where a > {y} use index(idx_a) order by a limit {x}; // it can return the correct result
        let x = rand_intn(549);
        let y = rand_intn(500) + 1;
        let mut query_range_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from trange use index(idx_a) where a > {} order by a limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_hash_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from thash use index(idx_a) where a > {} order by a limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select a from tregular use index(idx_a) where a > {} order by a limit {};",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_range_partition, "IndexReader");
        tk.must_have_plan(&query_hash_partition, "IndexReader");
        tk.must_have_plan(&query_range_partition, "Limit");
        tk.must_have_plan(&query_hash_partition, "Limit");
        tk.must_not_have_plan(&query_range_partition, "TopN");
        tk.must_not_have_plan(&query_hash_partition, "TopN");
        let mut regular_result = tk.must_query(&query_regular);
        tk.must_query(&query_range_partition)
            .check_result(&regular_result);
        tk.must_query(&query_hash_partition)
            .check_result(&regular_result);
    }
    // test indexReader use idx_ab(a, b) with a = {x} order by b limit {y}
    for _ in 0..50 {
        let x = rand_intn(549);
        let y = rand_intn(500) + 1;
        let mut query_range_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from trange use index(idx_ab) where a = {} order by b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_hash_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from thash use index(idx_ab) where a = {} order by b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_list_partition = format!(
            "select /*+ LIMIT_TO_COP() */ a from tlist use index(idx_ab) where a = {} order by b limit {};",
            go_v(&x),
            go_v(&y)
        );
        let mut query_regular = format!(
            "select a from tregular use index(idx_ab) where a = {} order by b limit {};",
            go_v(&x),
            go_v(&y)
        );
        tk.must_have_plan(&query_range_partition, "IndexReader");
        tk.must_have_plan(&query_hash_partition, "IndexReader");
        tk.must_have_plan(&query_list_partition, "IndexReader");
        tk.must_have_plan(&query_range_partition, "Limit");
        tk.must_have_plan(&query_hash_partition, "Limit");
        tk.must_have_plan(&query_list_partition, "Limit");
        tk.must_not_have_plan(&query_range_partition, "TopN");
        tk.must_not_have_plan(&query_hash_partition, "TopN");
        tk.must_not_have_plan(&query_list_partition, "TopN");
        let mut regular_result = tk.must_query(&query_regular);
        tk.must_query(&query_range_partition)
            .check_result(&regular_result);
        tk.must_query(&query_hash_partition)
            .check_result(&regular_result);
        tk.must_query(&query_list_partition)
            .check_result(&regular_result);
    }
    // test indexMerge
    for _ in 0..50 {
        // explain select /*+ use_index_merge(t) */ * from t where a > 2 or b < 5 order by a, b limit {x}; // check if IndexMerge is used
        // select /*+ use_index_merge(t) */ * from t where a > 2 or b < 5 order by a, b limit {x};  // can return the correct value
        let y = rand_intn(500) + 1;
        let mut query_hash_partition = format!(
            "select /*+ use_index_merge(thash) */ * from thash where a > 2 or b < 5 order by a, b limit {};",
            go_v(&y)
        );
        let mut query_regular = format!(
            "select * from tregular where a > 2 or b < 5 order by a, b limit {};",
            go_v(&y)
        );
        tk.must_have_plan(&query_hash_partition, "IndexMerge");
        {
            let expected = tk.must_query(&query_regular);
            tk.must_query(&query_hash_partition).check_result(&expected);
        }
    }
    // test sql killed when memory exceed `tidb_mem_quota_query`
    let mut origin_mem_quota = tk
        .must_query("show variables like 'tidb_mem_quota_query'")
        .rows()[0][1]
        .clone();
    let mut origin_oom_action = tk
        .must_query("show variables like 'tidb_mem_oom_action'")
        .rows()[0][1]
        .clone();
    tk.must_exec("set session tidb_mem_quota_query=128");
    tk.must_exec("set global tidb_mem_oom_action=CANCEL");
    let mut err = tk.exec_to_err("select /*+ LIMIT_TO_COP() */ a from trange use index(idx_a) where a > 1 order by a limit 2000");
    assert!(err.is_some());
    assert_eq!(
        err.as_ref().map(|e| e.code),
        Some(tidb_errno::errcode::ErrMemoryExceedForQuery),
        "{err:?}"
    );
    tk.must_exec(&format!(
        "set session tidb_mem_quota_query={}",
        go_v(&origin_mem_quota)
    ));
    tk.must_exec(&format!(
        "set global tidb_mem_oom_action={}",
        go_v(&origin_oom_action)
    ));
}

/// Go `TestParallelApply`.
#[test]
fn test_parallel_apply() {
    let _fp1 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune",
        r#"return(true)"#,
    );
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_parallel_apply");
    tk.must_exec("use test_parallel_apply");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec("set tidb_enable_parallel_apply=true");
    tk.must_exec(r#"create table touter (a int, b int)"#);
    tk.must_exec(r#"create table tinner (a int, b int, key(a))"#);
    tk.must_exec(r#"create table thash (a int, b int, key(a)) partition by hash(a) partitions 4"#);
    tk.must_exec(r#"create table trange (a int, b int, key(a)) partition by range(a) (partition p0 values less than(10000), partition p1 values less than(20000), partition p2 values less than(30000), partition p3 values less than(40000))"#);
    let mut vouter = Vec::<String>::with_capacity(100);
    for _ in 0..100 {
        vouter.push(format!("({}, {})", rand_intn(40000), rand_intn(40000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into touter values ",
        vouter.join(", ")
    ));
    let mut vals = Vec::<String>::with_capacity(2000);
    for _ in 0..100 {
        vals.push(format!("({}, {})", rand_intn(40000), rand_intn(40000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into tinner values ",
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash values ",
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange values ",
        vals.join(", ")
    ));
    // parallel apply + hash partition + IndexReader as its inner child
    tk.must_query(r#"explain format='brief' select * from touter where touter.a > (select sum(thash.a) from thash use index(a) where thash.a>touter.b)"#).check([r#"Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b"#, r#"└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#9)"#, r#"  ├─TableReader(Build) 10000.00 root  data:TableFullScan"#, r#"  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo"#, r#"  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#10)->Column#9"#, r#"    └─IndexReader 10000.00 root partition:all index:HashAgg"#, r#"      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.thash.a)->Column#10"#, r#"        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.thash.a, test_parallel_apply.touter.b)"#, r#"          └─IndexRangeScan 100000000.00 cop[tikv] table:thash, index:a(a) range: decided by [gt(test_parallel_apply.thash.a, test_parallel_apply.touter.b)], keep order:false, stats:pseudo"#]);
    {
        let expected = tk.must_query(r#"select * from touter where touter.a > (select sum(tinner.a) from tinner use index(a) where tinner.a>touter.b)"#).sort();
        tk.must_query(r#"select * from touter where touter.a > (select sum(thash.a) from thash use index(a) where thash.a>touter.b)"#).sort().check_result(&expected);
    }
    // parallel apply + hash partition + TableReader as its inner child
    tk.must_query(r#"explain format='brief' select * from touter where touter.a > (select sum(thash.b) from thash ignore index(a) where thash.a>touter.b)"#).check([r#"Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b"#, r#"└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#9)"#, r#"  ├─TableReader(Build) 10000.00 root  data:TableFullScan"#, r#"  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo"#, r#"  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#10)->Column#9"#, r#"    └─TableReader 10000.00 root partition:all data:HashAgg"#, r#"      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.thash.b)->Column#10"#, r#"        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.thash.a, test_parallel_apply.touter.b)"#, r#"          └─TableFullScan 100000000.00 cop[tikv] table:thash keep order:false, stats:pseudo"#]);
    {
        let expected = tk.must_query(r#"select * from touter where touter.a > (select sum(tinner.b) from tinner ignore index(a) where tinner.a>touter.b)"#).sort();
        tk.must_query(r#"select * from touter where touter.a > (select sum(thash.b) from thash ignore index(a) where thash.a>touter.b)"#).sort().check_result(&expected);
    }
    // parallel apply + hash partition + IndexLookUp as its inner child
    tk.must_query(r#"explain format='brief' select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)"#).check([r#"Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b"#, r#"└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#9)"#, r#"  ├─TableReader(Build) 10000.00 root  data:TableFullScan"#, r#"  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo"#, r#"  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#11)->Column#9"#, r#"    └─IndexLookUp 10000.00 root  "#, r#"      ├─Selection(Build) 80000000.00 cop[tikv]  gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)"#, r#"      │ └─IndexRangeScan 100000000.00 cop[tikv] table:tinner, index:a(a) range: decided by [gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)], keep order:false, stats:pseudo"#, r#"      └─HashAgg(Probe) 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.tinner.b)->Column#11"#, r#"        └─TableRowIDScan 80000000.00 cop[tikv] table:tinner keep order:false, stats:pseudo"#]);
    {
        let expected = tk.must_query(r#"select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)"#).sort();
        tk.must_query(r#"select * from touter where touter.a > (select sum(thash.b) from thash use index(a) where thash.a>touter.b)"#).sort().check_result(&expected);
    }
    // parallel apply + range partition + IndexReader as its inner child
    tk.must_query(r#"explain format='brief' select * from touter where touter.a > (select sum(trange.a) from trange use index(a) where trange.a>touter.b)"#).check([r#"Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b"#, r#"└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#9)"#, r#"  ├─TableReader(Build) 10000.00 root  data:TableFullScan"#, r#"  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo"#, r#"  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#10)->Column#9"#, r#"    └─IndexReader 10000.00 root partition:all index:HashAgg"#, r#"      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.trange.a)->Column#10"#, r#"        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.trange.a, test_parallel_apply.touter.b)"#, r#"          └─IndexRangeScan 100000000.00 cop[tikv] table:trange, index:a(a) range: decided by [gt(test_parallel_apply.trange.a, test_parallel_apply.touter.b)], keep order:false, stats:pseudo"#]);
    {
        let expected = tk.must_query(r#"select * from touter where touter.a > (select sum(tinner.a) from tinner use index(a) where tinner.a>touter.b)"#).sort();
        tk.must_query(r#"select * from touter where touter.a > (select sum(trange.a) from trange use index(a) where trange.a>touter.b)"#).sort().check_result(&expected);
    }
    // parallel apply + range partition + TableReader as its inner child
    tk.must_query(r#"explain format='brief' select * from touter where touter.a > (select sum(trange.b) from trange ignore index(a) where trange.a>touter.b)"#).check([r#"Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b"#, r#"└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#9)"#, r#"  ├─TableReader(Build) 10000.00 root  data:TableFullScan"#, r#"  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo"#, r#"  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#10)->Column#9"#, r#"    └─TableReader 10000.00 root partition:all data:HashAgg"#, r#"      └─HashAgg 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.trange.b)->Column#10"#, r#"        └─Selection 80000000.00 cop[tikv]  gt(test_parallel_apply.trange.a, test_parallel_apply.touter.b)"#, r#"          └─TableFullScan 100000000.00 cop[tikv] table:trange keep order:false, stats:pseudo"#]);
    {
        let expected = tk.must_query(r#"select * from touter where touter.a > (select sum(tinner.b) from tinner ignore index(a) where tinner.a>touter.b)"#).sort();
        tk.must_query(r#"select * from touter where touter.a > (select sum(trange.b) from trange ignore index(a) where trange.a>touter.b)"#).sort().check_result(&expected);
    }
    // parallel apply + range partition + IndexLookUp as its inner child
    tk.must_query(r#"explain format='brief' select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)"#).check([r#"Projection 10000.00 root  test_parallel_apply.touter.a, test_parallel_apply.touter.b"#, r#"└─Apply 10000.00 root  CARTESIAN inner join, other cond:gt(cast(test_parallel_apply.touter.a, decimal(10,0) BINARY), Column#9)"#, r#"  ├─TableReader(Build) 10000.00 root  data:TableFullScan"#, r#"  │ └─TableFullScan 10000.00 cop[tikv] table:touter keep order:false, stats:pseudo"#, r#"  └─HashAgg(Probe) 10000.00 root  funcs:sum(Column#11)->Column#9"#, r#"    └─IndexLookUp 10000.00 root  "#, r#"      ├─Selection(Build) 80000000.00 cop[tikv]  gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)"#, r#"      │ └─IndexRangeScan 100000000.00 cop[tikv] table:tinner, index:a(a) range: decided by [gt(test_parallel_apply.tinner.a, test_parallel_apply.touter.b)], keep order:false, stats:pseudo"#, r#"      └─HashAgg(Probe) 10000.00 cop[tikv]  funcs:sum(test_parallel_apply.tinner.b)->Column#11"#, r#"        └─TableRowIDScan 80000000.00 cop[tikv] table:tinner keep order:false, stats:pseudo"#]);
    {
        let expected = tk.must_query(r#"select * from touter where touter.a > (select sum(tinner.b) from tinner use index(a) where tinner.a>touter.b)"#).sort();
        tk.must_query(r#"select * from touter where touter.a > (select sum(trange.b) from trange use index(a) where trange.a>touter.b)"#).sort().check_result(&expected);
    }
    // random queries
    let mut ops = ["!=", ">", "<", ">=", "<="];
    let mut agg_funcs = ["sum", "count", "max", "min"];
    let mut tbls = ["tinner", "thash", "trange"];
    for _ in 0..50 {
        let mut r = TkResult::nil();
        let op = ops[(rand_intn((ops.len() as i64))) as usize];
        let agg = agg_funcs[(rand_intn((agg_funcs.len() as i64))) as usize];
        let x = rand_intn(10000);
        for tbl in tbls.clone() {
            let mut q = format!(
                r#"select * from touter where touter.a > (select {}({}.b) from {} where {}.a{}touter.b-{})"#,
                go_v(&agg),
                go_v(&tbl),
                go_v(&tbl),
                go_v(&tbl),
                go_v(&op),
                go_v(&x)
            );
            if r.is_nil() {
                r = tk.must_query(&q).sort();
            } else {
                tk.must_query(&q).sort().check_result(&r);
            }
        }
    }
}

/// Go `TestParallelApplyWithLimitOnRangeColumnsPartition`.
#[test]
fn test_parallel_apply_with_limit_on_range_columns_partition() {
    let _fp1 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune",
        r#"return(true)"#,
    );
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec("set @@tidb_enable_parallel_apply = 1");
    tk.must_exec("set @@tidb_executor_concurrency = 2");
    tk.must_exec(r#"create table t_outer (join_id varchar(32) not null, part_col int not null, payload varchar(32), key idx_outer_join(join_id) )"#);
    tk.must_exec(r#"create table t_part (part_col int not null, join_id varchar(32) not null, filter_col varchar(32) not null, metric decimal(35,15), seq_id bigint not null, row_key varchar(32) not null, primary key (part_col, seq_id, row_key, filter_col, join_id) nonclustered, key idx_join_filter_part(join_id, filter_col, part_col) ) partition by range columns (part_col) (partition p0 values less than (100))"#);
    tk.must_exec("insert into t_outer values ('key1', 10, 'payload1')");
    tk.must_exec("insert into t_part values (10, 'key1', 'flag1', 0.001, 1, 'row1')");
    let mut sql = r#"select payload from t_outer o where ifnull((select s.metric from t_part s use index(idx_join_filter_part) where s.join_id = o.join_id and s.part_col = o.part_col and s.filter_col = 'flag1' limit 1), 0) < 0.01"#.to_owned();
    tk.must_have_plan(&sql, "IndexLookUp");
    tk.must_query(&sql).check(["payload1"]);
}

/// Go `TestPartitionTableWithDifferentJoin`.
#[test]
fn test_partition_table_with_different_join() {
    let _fp1 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune",
        r#"return(true)"#,
    );
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_partition_joins");
    tk.must_exec("use test_partition_joins");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    // hash and range partition
    tk.must_exec("create table thash(a int, b int, key(a)) partition by hash(a) partitions 4");
    tk.must_exec("create table tregular1(a int, b int, key(a))");
    tk.must_exec(r#"create table trange(a int, b int, key(a)) partition by range(a) (partition p0 values less than (200), partition p1 values less than (400), partition p2 values less than (600), partition p3 values less than (800), partition p4 values less than (1001))"#);
    tk.must_exec("create table tregular2(a int, b int, key(a))");
    let mut vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(1000), rand_intn(1000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular1 values ",
        vals.join(",")
    ));
    vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(1000), rand_intn(1000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular2 values ",
        vals.join(",")
    ));
    // random params
    let x1 = rand_intn(1000);
    let x2 = rand_intn(1000);
    let x3 = rand_intn(1000);
    let x4 = rand_intn(1000);
    // group 1
    // hash_join range partition and hash partition
    let mut query_hash = format!(
        "select /*+ hash_join(trange, thash) */ * from trange, thash where trange.b=thash.b and thash.a = {} and trange.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    let mut query_regular = format!(
        "select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.b=tregular1.b and tregular1.a = {} and tregular2.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "HashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a > {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a > {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "HashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and trange.b = thash.b and thash.a > {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.b = tregular2.b and tregular1.a > {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "HashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ hash_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a = {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a = {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "HashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    // group 2
    // hash_join range partition and regular table
    query_hash = format!(
        "select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a >= {} and tregular1.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a >= {} and tregular1.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "HashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a in ({}, {}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3)
    );
    query_regular = format!(
        "select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a in ({}, {}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3)
    );
    tk.must_have_plan(&query_hash, "HashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ hash_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and tregular1.a >= {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ hash_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular1.a >= {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "HashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    // group 3
    // merge_join range partition and hash partition
    query_hash = format!(
        "select /*+ merge_join(trange, thash) */ * from trange, thash where trange.b=thash.b and thash.a = {} and trange.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.b=tregular1.b and tregular1.a = {} and tregular2.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "MergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a > {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a > {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "MergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and trange.b = thash.b and thash.a > {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.b = tregular2.b and tregular1.a > {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "MergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ merge_join(trange, thash) */ * from trange, thash where trange.a=thash.a and thash.a = {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a=tregular1.a and tregular1.a = {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "MergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    // group 4
    // merge_join range partition and regular table
    query_hash = format!(
        "select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a >= {} and tregular1.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a >= {} and tregular1.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "MergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and trange.a in ({}, {}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3)
    );
    query_regular = format!(
        "select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular2.a in ({}, {}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3)
    );
    tk.must_have_plan(&query_hash, "MergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ merge_join(trange, tregular1) */ * from trange, tregular1 where trange.a = tregular1.a and tregular1.a >= {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ merge_join(tregular2, tregular1) */ * from tregular2, tregular1 where tregular2.a = tregular1.a and tregular1.a >= {};",
        go_v(&x1)
    );
    tk.must_have_plan(&query_hash, "MergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    // new table instances
    tk.must_exec(
        "create table thash2(a int, b int, index idx(a)) partition by hash(a) partitions 4",
    );
    tk.must_exec("create table tregular3(a int, b int, index idx(a))");
    tk.must_exec(r#"create table trange2(a int, b int, index idx(a)) partition by range(a) (partition p0 values less than (200), partition p1 values less than (400), partition p2 values less than (600), partition p3 values less than (800), partition p4 values less than (1001))"#);
    tk.must_exec("create table tregular4(a int, b int, index idx(a))");
    vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(1000), rand_intn(1000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into thash2 values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular3 values ",
        vals.join(",")
    ));
    vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!("({}, {})", rand_intn(1000), rand_intn(1000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange2 values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into tregular4 values ",
        vals.join(",")
    ));
    // group 5
    // index_merge_join range partition and range partition
    // Currently don't support index merge join on two partition tables. Only test warning.
    query_hash = format!(
        "select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > {};",
        go_v(&x1)
    );
    // queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v;", x1)
    // tk.MustHavePlan(queryHash, "IndexMergeJoin")
    // tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
    tk.must_query(&query_hash);
    tk.must_query("show warnings").check_with_sep(
        "|",
        ["Warning|1815|The INDEX MERGE JOIN hint is deprecated for usage, try other hints."],
    );
    query_hash = format!(
        "select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > {} and trange2.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    // queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.a > %v;", x1, x2)
    // tk.MustHavePlan(queryHash, "IndexMergeJoin")
    // tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
    tk.must_query(&query_hash);
    tk.must_query("show warnings").check_with_sep(
        "|",
        ["Warning|1815|The INDEX MERGE JOIN hint is deprecated for usage, try other hints."],
    );
    query_hash = format!(
        "select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > {} and trange.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    // queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular2.b > %v;", x1, x2)
    // tk.MustHavePlan(queryHash, "IndexMergeJoin")
    // tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
    tk.must_query(&query_hash);
    tk.must_query("show warnings").check_with_sep(
        "|",
        ["Warning|1815|The INDEX MERGE JOIN hint is deprecated for usage, try other hints."],
    );
    query_hash = format!(
        "select /*+ inl_merge_join(trange, trange2) */ * from trange, trange2 where trange.a=trange2.a and trange.a > {} and trange2.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    // queryRegular = fmt.Sprintf("select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > %v and tregular4.b > %v;", x1, x2)
    // tk.MustHavePlan(queryHash, "IndexMergeJoin")
    // tk.MustQuery(queryHash).Sort().Check(tk.MustQuery(queryRegular).Sort().Rows())
    tk.must_query(&query_hash);
    tk.must_query("show warnings").check_with_sep(
        "|",
        ["Warning|1815|The INDEX MERGE JOIN hint is deprecated for usage, try other hints."],
    );
    // group 6
    // index_merge_join range partition and regualr table
    query_hash = format!(
        "select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > {};",
        go_v(&x1)
    );
    query_regular = format!(
        "select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > {};",
        go_v(&x1)
    );
    tk.must_not_have_plan(&query_hash, "IndexMergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > {} and tregular4.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > {} and tregular4.a > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_not_have_plan(&query_hash, "IndexMergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > {} and trange.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > {} and tregular2.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_not_have_plan(&query_hash, "IndexMergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ inl_merge_join(trange, tregular4) */ * from trange, tregular4 where trange.a=tregular4.a and trange.a > {} and tregular4.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ inl_merge_join(tregular2, tregular4) */ * from tregular2, tregular4 where tregular2.a=tregular4.a and tregular2.a > {} and tregular4.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_not_have_plan(&query_hash, "IndexMergeJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    // group 7
    // index_hash_join hash partition and hash partition
    query_hash = format!(
        "select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a in ({}, {});",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in ({}, {});",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "IndexHashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a in ({}, {}) and thash2.a in ({}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3),
        go_v(&x4)
    );
    query_regular = format!(
        "select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in ({}, {}) and tregular3.a in ({}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3),
        go_v(&x4)
    );
    tk.must_have_plan(&query_hash, "IndexHashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ inl_hash_join(thash, thash2) */ * from thash, thash2 where thash.a = thash2.a and thash.a > {} and thash2.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a > {} and tregular3.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "IndexHashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    // group 8
    // index_hash_join hash partition and hash partition
    query_hash = format!(
        "select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a in ({}, {});",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in ({}, {});",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "IndexHashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a in ({}, {}) and tregular3.a in ({}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3),
        go_v(&x4)
    );
    query_regular = format!(
        "select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a in ({}, {}) and tregular3.a in ({}, {});",
        go_v(&x1),
        go_v(&x2),
        go_v(&x3),
        go_v(&x4)
    );
    tk.must_have_plan(&query_hash, "IndexHashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
    query_hash = format!(
        "select /*+ inl_hash_join(thash, tregular3) */ * from thash, tregular3 where thash.a = tregular3.a and thash.a > {} and tregular3.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    query_regular = format!(
        "select /*+ inl_hash_join(tregular1, tregular3) */ * from tregular1, tregular3 where tregular1.a = tregular3.a and tregular1.a > {} and tregular3.b > {};",
        go_v(&x1),
        go_v(&x2)
    );
    tk.must_have_plan(&query_hash, "IndexHashJoin");
    {
        let expected = tk.must_query(&query_regular).sort();
        tk.must_query(&query_hash).sort().check_result(&expected);
    }
}

/// Go `TestPointGetwithRangeAndListPartitionTable`.
#[test]
fn test_point_getwith_range_and_list_partition_table() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    // list partition table
    tk.must_exec(r#"create table tlist(a int, b int, unique index idx_a(a), index idx_b(b)) partition by list(a)(partition p0 values in (NULL, 1, 2, 3, 4), partition p1 values in (5, 6, 7, 8), partition p2 values in (9, 10, 11, 12));"#);
    // range partition table
    tk.must_exec(r#"create table trange1(a int, unique key(a)) partition by range(a) (partition p0 values less than (30), partition p1 values less than (60), partition p2 values less than (90), partition p3 values less than (120));"#);
    // range partition table + unsigned int
    tk.must_exec(r#"create table trange2(a int unsigned, unique key(a)) partition by range(a) (partition p0 values less than (30), partition p1 values less than (60), partition p2 values less than (90), partition p3 values less than (120));"#);
    // insert data into list partition table
    tk.must_exec("insert into tlist values(1,1), (2,2), (3, 3), (4, 4), (5,5), (6, 6), (7,7), (8, 8), (9, 9), (10, 10), (11, 11), (12, 12), (NULL, NULL);");
    let mut vals = Vec::<String>::with_capacity(100);
    // insert data into range partition table and hash partition table
    for i in 0..100 {
        vals.push(format!("({})", i + 1));
    }
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange1 values ",
        vals.join(",")
    ));
    tk.must_exec(&format!(
        "{}{}",
        "insert into trange2 values ",
        vals.join(",")
    ));
    // test PointGet
    for _ in 0..100 {
        // explain select a from t where a = {x}; // x >= 1 and x <= 100 Check if PointGet is used
        // select a from t where a={x}; // the result is {x}
        let x = rand_intn(100) + 1;
        let mut query_range1 = format!("select a from trange1 where a={}", go_v(&x));
        tk.must_have_plan(&query_range1, "Point_Get");
        tk.must_query(&query_range1)
            .check([format!("{}", go_v(&x)).as_str()]);
        let mut query_range2 = format!("select a from trange1 where a={}", go_v(&x));
        tk.must_have_plan(&query_range2, "Point_Get");
        tk.must_query(&query_range2)
            .check([format!("{}", go_v(&x)).as_str()]);
        let y = rand_intn(12) + 1;
        let mut query_list = format!("select a from tlist where a={}", go_v(&y));
        tk.must_have_plan(&query_list, "Point_Get");
        tk.must_query(&query_list)
            .check([format!("{}", go_v(&y)).as_str()]);
    }
    // test table dual
    let mut query_range1 = "select a from trange1 where a=200".to_owned();
    tk.must_query(&format!("{}{}", "EXPLAIN FORMAT='brief' ", query_range1))
        .check(["Point_Get 1.00 root table:trange1, partition:dual, index:a(a) "]);
    tk.must_query(&query_range1).check_empty();
    let mut query_range2 = "select a from trange2 where a=200".to_owned();
    tk.must_query(&format!("{}{}", "EXPLAIN FORMAT='brief' ", query_range2))
        .check(["Point_Get 1.00 root table:trange2, partition:dual, index:a(a) "]);
    tk.must_query(&query_range2).check_empty();
    let mut query_list = "select a from tlist where a=200".to_owned();
    tk.must_query(&format!("{}{}", "EXPLAIN FORMAT='brief' ", query_list))
        .check(["Point_Get 1.00 root table:tlist, partition:dual, index:idx_a(a) "]);
    tk.must_query(&query_list).check_empty();
    // test PointGet for one partition
    let mut query_one_partition = "select a from t where a = -1".to_owned();
    tk.must_exec("create table t(a int primary key, b int) PARTITION BY RANGE (a) (partition p0 values less than(1))");
    tk.must_exec("insert into t values (-1, 1), (-2, 1)");
    tk.must_exec("analyze table t");
    tk.must_have_plan(&query_one_partition, "Point_Get");
    tk.must_query(&query_one_partition)
        .check([format!("{}", -1).as_str()]);
    tk.must_exec("drop table t");
    tk.must_exec("create table t(a int primary key, b int) PARTITION BY list (a) (partition p0 values in (-1, -2))");
    tk.must_exec("insert into t values (-1, 1), (-2, 1)");
    tk.must_exec("analyze table t");
    tk.must_have_plan(&query_one_partition, "Point_Get");
    tk.must_query(&query_one_partition)
        .check([format!("{}", -1).as_str()]);
}

/// Go `TestSplitRegion`.
#[test]
fn test_split_region() {
    let _fp1 = enable_failpoint(
        "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune",
        r#"return(true)"#,
    );
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_split_region");
    tk.must_exec("use test_split_region");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec(r#"create table tnormal (a int, b int)"#);
    tk.must_exec(
        r#"create table thash (a int, b int, index(a)) partition by hash(a) partitions 4"#,
    );
    tk.must_exec(r#"create table trange (a int, b int, index(a)) partition by range(a) (partition p0 values less than (10000), partition p1 values less than (20000), partition p2 values less than (30000), partition p3 values less than (40000))"#);
    let mut vals = Vec::<String>::with_capacity(1000);
    for _ in 0..1000 {
        vals.push(format!("({}, {})", rand_intn(40000), rand_intn(40000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into tnormal values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into thash values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into trange values "#,
        vals.join(", ")
    ));
    tk.must_exec(r#"SPLIT TABLE thash INDEX a BETWEEN (1) AND (25000) REGIONS 10"#);
    tk.must_exec(r#"SPLIT TABLE trange INDEX a BETWEEN (1) AND (25000) REGIONS 10"#);
    let mut result = tk
        .must_query(r#"select * from tnormal where a>=1 and a<=15000"#)
        .sort();
    tk.must_partition(r#"select * from trange where a>=1 and a<=15000"#, "p0,p1")
        .sort()
        .check_result(&result);
    tk.must_partition(r#"select * from thash where a>=1 and a<=15000"#, "all")
        .sort()
        .check_result(&result);
    result = tk
        .must_query(r#"select * from tnormal where a in (1, 10001, 20001)"#)
        .sort();
    tk.must_partition(
        r#"select * from trange where a in (1, 10001, 20001)"#,
        "p0,p1,p2",
    )
    .sort()
    .check_result(&result);
    tk.must_partition(r#"select * from thash where a in (1, 10001, 20001)"#, "p1")
        .sort()
        .check_result(&result);
}

/// Go `TestSubqueries`.
#[test]
fn test_subqueries() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_subquery");
    tk.must_exec("use test_subquery");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec(r#"create table touter (a int, b int, index(a))"#);
    tk.must_exec(r#"create table tinner (a int, b int, c int, index(a))"#);
    tk.must_exec(
        r#"create table thash (a int, b int, c int, index(a)) partition by hash(a) partitions 4"#,
    );
    tk.must_exec(r#"create table trange (a int, b int, c int, index(a)) partition by range(a) (partition p0 values less than(10000), partition p1 values less than(20000), partition p2 values less than(30000), partition p3 values less than(40000))"#);
    let mut outer_vals = Vec::<String>::with_capacity(100);
    for _ in 0..100 {
        outer_vals.push(format!(r#"({}, {})"#, rand_intn(40000), rand_intn(40000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into touter values "#,
        outer_vals.join(", ")
    ));
    let mut vals = Vec::<String>::with_capacity(2000);
    for _ in 0..2000 {
        vals.push(format!(
            r#"({}, {}, {})"#,
            rand_intn(40000),
            rand_intn(40000),
            rand_intn(40000)
        ));
    }
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into tinner values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into thash values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into trange values "#,
        vals.join(", ")
    ));
    // in
    for _ in 0..50 {
        for op in ["in", "not in"] {
            let x = rand_intn(40000);
            let mut r = TkResult::nil();
            for t in ["tinner", "thash", "trange"] {
                let mut q = format!(
                    r#"select * from touter where touter.a {} (select {}.b from {} where {}.a > touter.b and {}.c > {})"#,
                    go_v(&op),
                    go_v(&t),
                    go_v(&t),
                    go_v(&t),
                    go_v(&t),
                    go_v(&x)
                );
                if r.is_nil() {
                    r = tk.must_query(&q).sort();
                } else {
                    tk.must_query(&q).sort().check_result(&r);
                }
            }
        }
    }
    // exist
    for _ in 0..50 {
        for op in ["exists", "not exists"] {
            let x = rand_intn(40000);
            let mut r = TkResult::nil();
            for t in ["tinner", "thash", "trange"] {
                let mut q = format!(
                    r#"select * from touter where {} (select {}.b from {} where {}.a > touter.b and {}.c > {})"#,
                    go_v(&op),
                    go_v(&t),
                    go_v(&t),
                    go_v(&t),
                    go_v(&t),
                    go_v(&x)
                );
                if r.is_nil() {
                    r = tk.must_query(&q).sort();
                } else {
                    tk.must_query(&q).sort().check_result(&r);
                }
            }
        }
    }
}

/// Go `TestUnion`.
#[test]
fn test_union() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_union");
    tk.must_exec("use test_union");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec(r#"create table t(a int, b int, key(a))"#);
    tk.must_exec(r#"create table thash (a int, b int, key(a)) partition by hash(a) partitions 4"#);
    tk.must_exec(r#"create table trange (a int, b int, key(a)) partition by range(a) (partition p0 values less than (10000), partition p1 values less than (20000), partition p2 values less than (30000), partition p3 values less than (40000))"#);
    let mut vals = Vec::<String>::with_capacity(1000);
    for _ in 0..1000 {
        vals.push(format!("({}, {})", rand_intn(40000), rand_intn(40000)));
    }
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into t values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into thash values "#,
        vals.join(", ")
    ));
    tk.must_exec(&format!(
        "{}{}",
        r#"insert into trange values "#,
        vals.join(", ")
    ));
    let rand_range = |tk: &mut TestKit| {
        let (mut l, mut r) = (rand_intn(40000), rand_intn(40000));
        if l > r {
            (l, r) = (r, l);
        }
        return (l, r);
    };
    for _ in 0..100 {
        let (mut a1l, mut a1r) = rand_range(&mut tk);
        let (mut a2l, mut a2r) = rand_range(&mut tk);
        let (mut b1l, mut b1r) = rand_range(&mut tk);
        let (mut b2l, mut b2r) = rand_range(&mut tk);
        for utype in ["union all", "union distinct"] {
            let mut pattern = format!(
                r#"select * from %v where a>={} and a<={} and b>={} and b<={} {} select * from %v where a>={} and a<={} and b>={} and b<={}"#,
                go_v(&a1l),
                go_v(&a1r),
                go_v(&b1l),
                go_v(&b1r),
                go_v(&utype),
                go_v(&a2l),
                go_v(&a2r),
                go_v(&b2l),
                go_v(&b2r)
            );
            let mut r = tk.must_query(&go_sprintf(&pattern, &[&"t", &"t"])).sort();
            tk.must_query(&go_sprintf(&pattern, &[&"thash", &"thash"]))
                .sort()
                .check_result(&r);
            tk.must_query(&go_sprintf(&pattern, &[&"trange", &"trange"]))
                .sort()
                .check_result(&r);
            tk.must_query(&go_sprintf(&pattern, &[&"trange", &"thash"]))
                .sort()
                .check_result(&r);
        }
    }
}

/// Go `TestUnsignedPartitionColumn`.
#[test]
fn test_unsigned_partition_column() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_unsigned_partition");
    tk.must_exec("use test_unsigned_partition");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec(r#"create table thash_pk (a int unsigned, b int, primary key(a)) partition by hash (a) partitions 3"#);
    tk.must_exec(r#"create table trange_pk (a int unsigned, b int, primary key(a)) partition by range (a) (partition p1 values less than (100000), partition p2 values less than (200000), partition p3 values less than (300000), partition p4 values less than (400000))"#);
    tk.must_exec(r#"create table tnormal_pk (a int unsigned, b int, primary key(a))"#);
    tk.must_exec(r#"create table thash_uniq (a int unsigned, b int, unique key(a)) partition by hash (a) partitions 3"#);
    tk.must_exec(r#"create table trange_uniq (a int unsigned, b int, unique key(a)) partition by range (a) (partition p1 values less than (100000), partition p2 values less than (200000), partition p3 values less than (300000), partition p4 values less than (400000))"#);
    tk.must_exec(r#"create table tnormal_uniq (a int unsigned, b int, unique key(a))"#);
    let mut val_col_a = std::collections::HashSet::new();
    let mut vals = Vec::<String>::with_capacity(1000);
    while (vals.len() as i64) < 1000 {
        let a = rand_intn(400000);
        if val_col_a.contains(&a) {
            continue;
        }
        val_col_a.insert(a.clone());
        vals.push(format!("({}, {})", a, rand_intn(400000)));
    }
    let mut val_str = vals.join(", ");
    for tbl in [
        "thash_pk",
        "trange_pk",
        "tnormal_pk",
        "thash_uniq",
        "trange_uniq",
        "tnormal_uniq",
    ] {
        tk.must_exec(&format!(
            "insert into {} values {}",
            go_v(&tbl),
            go_v(&val_str)
        ));
    }
    for _ in 0..100 {
        let mut scan_cond = format!(
            "a {} {}",
            [">", "<"][(rand_intn(2)) as usize],
            rand_intn(400000)
        );
        let mut point_cond = format!("a = {}", rand_intn(400000));
        let mut batch_cond = format!(
            "a in ({}, {}, {})",
            rand_intn(400000),
            rand_intn(400000),
            rand_intn(400000)
        );
        let mut r_scan = TkResult::nil();
        let mut r_point = TkResult::nil();
        let mut r_batch = TkResult::nil();
        for (tid, tbl) in ["tnormal_pk", "trange_pk", "thash_pk"]
            .into_iter()
            .enumerate()
        {
            // unsigned + TableReader
            let mut scan_sql = format!(
                "select * from {} use index(primary) where {}",
                go_v(&tbl),
                go_v(&scan_cond)
            );
            tk.must_have_plan(&scan_sql, "TableReader");
            let mut r = tk.must_query(&scan_sql).sort();
            if tid == 0 {
                r_scan = r;
            } else {
                r.check_result(&r_scan);
            }
            // unsigned + PointGet on PK
            let mut point_sql = format!(
                "select * from {} use index(primary) where {}",
                go_v(&tbl),
                go_v(&point_cond)
            );
            tk.must_point_get(&point_sql);
            r = tk.must_query(&point_sql).sort();
            if tid == 0 {
                r_point = r;
            } else {
                r.check_result(&r_point);
            }
            // unsigned + BatchGet on PK
            let mut batch_sql = format!("select * from {} where {}", go_v(&tbl), go_v(&batch_cond));
            tk.must_have_plan(&batch_sql, "Batch_Point_Get");
            r = tk.must_query(&batch_sql).sort();
            if tid == 0 {
                r_batch = r;
            } else {
                r.check_result(&r_batch);
            }
        }
        let mut lookup_cond = format!(
            "a {} {}",
            [">", "<"][(rand_intn(2)) as usize],
            rand_intn(400000)
        );
        let mut r_lookup = TkResult::nil();
        for (tid, tbl) in ["tnormal_uniq", "trange_uniq", "thash_uniq"]
            .into_iter()
            .enumerate()
        {
            // unsigned + IndexReader
            let mut scan_sql = format!(
                "select a from {} use index(a) where {}",
                go_v(&tbl),
                go_v(&scan_cond)
            );
            tk.must_have_plan(&scan_sql, "IndexReader");
            let mut r = tk.must_query(&scan_sql).sort();
            if tid == 0 {
                r_scan = r;
            } else {
                r.check_result(&r_scan);
            }
            // unsigned + IndexLookUp
            let mut lookup_sql = format!(
                "select * from {} use index(a) where {}",
                go_v(&tbl),
                go_v(&lookup_cond)
            );
            tk.must_index_lookup(&lookup_sql);
            r = tk.must_query(&lookup_sql).sort();
            if tid == 0 {
                r_lookup = r;
            } else {
                r.check_result(&r_lookup);
            }
            // unsigned + PointGet on UniqueIndex
            let mut point_sql = format!(
                "select * from {} use index(a) where {}",
                go_v(&tbl),
                go_v(&point_cond)
            );
            tk.must_point_get(&point_sql);
            r = tk.must_query(&point_sql).sort();
            if tid == 0 {
                r_point = r;
            } else {
                r.check_result(&r_point);
            }
            // unsigned + BatchGet on UniqueIndex
            let mut batch_sql = format!("select * from {} where {}", go_v(&tbl), go_v(&batch_cond));
            tk.must_have_plan(&batch_sql, "Batch_Point_Get");
            r = tk.must_query(&batch_sql).sort();
            if tid == 0 {
                r_batch = r;
            } else {
                r.check_result(&r_batch);
            }
        }
    }
}

/// Go `TestView`.
#[test]
fn test_view() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create database test_view");
    tk.must_exec("use test_view");
    tk.must_exec("set @@tidb_partition_prune_mode = 'dynamic'");
    tk.must_exec(r#"create table thash (a int, b int, key(a)) partition by hash(a) partitions 4"#);
    tk.must_exec(r#"create table trange (a varchar(10), b varchar(10), key(a)) partition by range columns(a) (partition p0 values less than ('300'), partition p1 values less than ('600'), partition p2 values less than ('900'), partition p3 values less than ('9999'))"#);
    tk.must_exec(r#"create table t1 (a int, b int, key(a))"#);
    tk.must_exec(r#"create table t2 (a varchar(10), b varchar(10), key(a))"#);
    // insert the same data into thash and t1
    let mut vals = Vec::<String>::with_capacity(3000);
    for _ in 0..3000 {
        vals.push(format!(r#"({}, {})"#, rand_intn(10000), rand_intn(10000)));
    }
    tk.must_exec(&format!(r#"insert into thash values {}"#, vals.join(", ")));
    tk.must_exec(&format!(r#"insert into t1 values {}"#, vals.join(", ")));
    // insert the same data into trange and t2
    vals.clear();
    for _ in 0..2000 {
        vals.push(format!(r#"("{}", "{}")"#, rand_intn(1000), rand_intn(1000)));
    }
    tk.must_exec(&format!(r#"insert into trange values {}"#, vals.join(", ")));
    tk.must_exec(&format!(r#"insert into t2 values {}"#, vals.join(", ")));
    // test views on a single table
    tk.must_exec(
        r#"create definer='root'@'localhost' view vhash as select a*2 as a, a+b as b from thash"#,
    );
    tk.must_exec(
        r#"create definer='root'@'localhost' view v1 as select a*2 as a, a+b as b from t1"#,
    );
    tk.must_exec(r#"create definer='root'@'localhost' view vrange as select concat(a, b) as a, a+b as b from trange"#);
    tk.must_exec(r#"create definer='root'@'localhost' view v2 as select concat(a, b) as a, a+b as b from t2"#);
    for _ in 0..100 {
        let xhash = rand_intn(10000);
        {
            let expected = tk
                .must_query(&format!(r#"select * from v1 where a>={}"#, go_v(&xhash)))
                .sort();
            tk.must_query(&format!(r#"select * from vhash where a>={}"#, go_v(&xhash)))
                .sort()
                .check_result(&expected);
        }
        {
            let expected = tk
                .must_query(&format!(r#"select * from v1 where b>={}"#, go_v(&xhash)))
                .sort();
            tk.must_query(&format!(r#"select * from vhash where b>={}"#, go_v(&xhash)))
                .sort()
                .check_result(&expected);
        }
        {
            let expected = tk
                .must_query(&format!(
                    r#"select * from v1 where a>={} and b>={}"#,
                    go_v(&xhash),
                    go_v(&xhash)
                ))
                .sort();
            tk.must_query(&format!(
                r#"select * from vhash where a>={} and b>={}"#,
                go_v(&xhash),
                go_v(&xhash)
            ))
            .sort()
            .check_result(&expected);
        }
        let mut xrange = format!(r#""{}""#, rand_intn(1000));
        {
            let expected = tk
                .must_query(&format!(r#"select * from v2 where a>={}"#, go_v(&xrange)))
                .sort();
            tk.must_query(&format!(
                r#"select * from vrange where a>={}"#,
                go_v(&xrange)
            ))
            .sort()
            .check_result(&expected);
        }
        {
            let expected = tk
                .must_query(&format!(r#"select * from v2 where b>={}"#, go_v(&xrange)))
                .sort();
            tk.must_query(&format!(
                r#"select * from vrange where b>={}"#,
                go_v(&xrange)
            ))
            .sort()
            .check_result(&expected);
        }
        {
            let expected = tk
                .must_query(&format!(
                    r#"select * from v2 where a>={} and b<={}"#,
                    go_v(&xrange),
                    go_v(&xrange)
                ))
                .sort();
            tk.must_query(&format!(
                r#"select * from vrange where a>={} and b<={}"#,
                go_v(&xrange),
                go_v(&xrange)
            ))
            .sort()
            .check_result(&expected);
        }
    }
    // test views on both tables
    tk.must_exec(r#"create definer='root'@'localhost' view vboth as select thash.a+trange.a as a, thash.b+trange.b as b from thash, trange where thash.a=trange.a"#);
    tk.must_exec(r#"create definer='root'@'localhost' view vt as select t1.a+t2.a as a, t1.b+t2.b as b from t1, t2 where t1.a=t2.a"#);
    for _ in 0..100 {
        let x = rand_intn(10000);
        {
            let expected = tk
                .must_query(&format!(r#"select * from vt where a>={}"#, go_v(&x)))
                .sort();
            tk.must_query(&format!(r#"select * from vboth where a>={}"#, go_v(&x)))
                .sort()
                .check_result(&expected);
        }
        {
            let expected = tk
                .must_query(&format!(r#"select * from vt where b>={}"#, go_v(&x)))
                .sort();
            tk.must_query(&format!(r#"select * from vboth where b>={}"#, go_v(&x)))
                .sort()
                .check_result(&expected);
        }
        {
            let expected = tk
                .must_query(&format!(
                    r#"select * from vt where a>={} and b>={}"#,
                    go_v(&x),
                    go_v(&x)
                ))
                .sort();
            tk.must_query(&format!(
                r#"select * from vboth where a>={} and b>={}"#,
                go_v(&x),
                go_v(&x)
            ))
            .sort()
            .check_result(&expected);
        }
    }
}
