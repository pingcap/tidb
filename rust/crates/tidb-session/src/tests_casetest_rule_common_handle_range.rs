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

//! Go master `pkg/planner/core/casetest/rule/rule_common_handle_range_test.go`
//! (f994e8da277, 70cff7d9f66): a non-unique secondary index on a clustered
//! table ends with the full common handle, so predicates on the primary key
//! columns become index scan ranges.

use crate::testkit::TestKit;

/// Go `explainHas`.
fn explain_has(tk: &mut TestKit, sql: &str, text: &str) -> bool {
    tk.must_query(sql).rows().iter().any(|row| row.iter().any(|cell| cell.contains(text)))
}

fn plan_tree(sql: &str) -> String {
    format!("explain format = 'plan_tree' {sql}")
}

/// Go `TestCommonHandleIndexRanges`, under both planners
/// (`RunTestUnderCascades`).
#[test]
fn test_common_handle_index_ranges() {
    for cascades in ["off", "on"] {
        let mut tk = TestKit::new();
        tk.must_exec(&format!("set tidb_enable_cascades_planner = {cascades}"));
        tk.must_exec(
            "CREATE TABLE t_chr (tenant int, seq bigint, a int, b int, c int,
			PRIMARY KEY(tenant, seq) CLUSTERED, KEY ia(a), KEY ia_overlap(a, tenant), UNIQUE KEY ub(b))",
        );
        // Several tenants share the same a values so range narrowing is
        // observable.
        tk.must_exec("insert into t_chr values (1, 100, 10, 1, 0), (1, 200, 10, 2, 0), (2, 100, 10, 3, 0), (2, 200, 20, 4, 0), (3, 100, 10, 5, 1)");
        let mut check = |tk: &mut TestKit, case: &str, sql: &str, range: &str, present: bool, rows: &[&str]| {
            assert_eq!(explain_has(tk, &plan_tree(sql), range), present, "{cascades} {case}: {range} in {sql}");
            tk.must_query(sql).check(rows);
        };

        // Case 1: equality on all index columns plus the leading PK column
        // becomes a multi-column point range on the secondary index.
        check(&mut tk, "case 1", "select * from t_chr use index(ia) where a = 10 and tenant = 2", "range:[10 2,10 2]", true, &["2 100 10 3 0"]);
        // Case 2: the full PK suffix participates.
        check(&mut tk, "case 2", "select * from t_chr use index(ia) where a = 10 and tenant = 1 and seq > 100", "range:(10 1 100,10 1 +inf]", true, &["1 200 10 2 0"]);
        // Case 3: a non-point range on the leading PK column.
        assert!(explain_has(&mut tk, &plan_tree("select * from t_chr use index(ia) where a = 10 and tenant > 1"), "range:(10 1,10 +inf]"), "{cascades} case 3");
        tk.must_query("select * from t_chr use index(ia) where a = 10 and tenant > 1 order by tenant, seq").check(["2 100 10 3 0", "3 100 10 5 1"]);
        // Case 4: an IN list on the index column combines with the PK
        // suffix.
        assert!(explain_has(&mut tk, &plan_tree("select * from t_chr use index(ia) where a in (10, 20) and tenant = 2"), "range:[10 2,10 2], [20 2,20 2]"), "{cascades} case 4");
        tk.must_query("select * from t_chr use index(ia) where a in (10, 20) and tenant = 2 order by tenant, seq").check(["2 100 10 3 0", "2 200 20 4 0"]);
        // Case 5: a predicate on a later PK column without the earlier one
        // stays a filter.
        assert!(explain_has(&mut tk, &plan_tree("select * from t_chr use index(ia) where a = 10 and seq = 100"), "range:[10,10]"), "{cascades} case 5");
        tk.must_query("select * from t_chr use index(ia) where a = 10 and seq = 100 order by tenant, seq").check(["1 100 10 1 0", "2 100 10 3 0", "3 100 10 5 1"]);
        // Case 6: ia_overlap(a, tenant) already contains a PK column, so the
        // PK suffix must not be appended.
        let sql = "select * from t_chr use index(ia_overlap) where a = 10 and tenant = 1 and seq > 100";
        assert!(explain_has(&mut tk, &plan_tree(sql), "range:[10 1,10 1]"), "{cascades} case 6");
        assert!(!explain_has(&mut tk, &plan_tree(sql), "range:[10 1 100"), "{cascades} case 6");
        tk.must_query(sql).check(["1 200 10 2 0"]);
        // Case 7: a unique secondary index keeps the handle in its value.
        assert!(!explain_has(&mut tk, &plan_tree("select * from t_chr use index(ub) where b > 0 and tenant = 2"), "range:(0 2"), "{cascades} case 7");
        tk.must_query("select * from t_chr use index(ub) where b > 0 and tenant = 2 order by tenant, seq").check(["2 100 10 3 0", "2 200 20 4 0"]);
        // Case 8: the appended PK columns are readable from the index.
        let sql = "select tenant, seq, a from t_chr use index(ia) where a = 10 and tenant = 2";
        assert!(explain_has(&mut tk, &plan_tree(sql), "IndexReader"), "{cascades} case 8");
        assert!(explain_has(&mut tk, &plan_tree(sql), "range:[10 2,10 2]"), "{cascades} case 8");
        tk.must_query(sql).check(["2 100 10"]);

        // Case 9: a case-insensitive string PK column honors collation
        // equality.
        tk.must_exec("CREATE TABLE t_chr_ci (tenant varchar(32) COLLATE utf8mb4_general_ci, seq int, a int, PRIMARY KEY(tenant, seq) CLUSTERED, KEY ia(a))");
        tk.must_exec("insert into t_chr_ci values ('abc', 1, 10), ('ABC', 2, 10), ('xyz', 1, 10), ('abc', 3, 20)");
        tk.must_query("select * from t_chr_ci use index(ia) where a = 10 and tenant = 'AbC' order by seq").check(["abc 1 10", "ABC 2 10"]);
        tk.must_query("select * from t_chr_ci use index(ia) where a = 10 and tenant = 'abc' and seq >= 2").check(["ABC 2 10"]);

        // Case 10: a prefixed clustered PK re-checks the full predicate.
        tk.must_exec("CREATE TABLE t_chr_prefix (p1 varchar(64), p2 int, c int, PRIMARY KEY(p1(2), p2) CLUSTERED, KEY ic(c))");
        tk.must_exec("insert into t_chr_prefix values ('abz', 1, 0), ('abc', 2, 0), ('axy', 3, 0), ('abc', 4, 1)");
        tk.must_query("select * from t_chr_prefix use index(ic) where c = 0 and p1 = 'abc'").check(["abc 2 0"]);
        tk.must_query("select * from t_chr_prefix use index(ic) where c = 0 and p1 = 'abz' and p2 = 1").check(["abz 1 0"]);

        // Case 11: a single-column string common handle.
        tk.must_exec("CREATE TABLE t_chr_single (pk varchar(32), a int, b int, PRIMARY KEY(pk) CLUSTERED, KEY ia(a))");
        tk.must_exec("insert into t_chr_single values ('u1', 10, 1), ('u2', 10, 2), ('u3', 10, 3), ('u4', 20, 4)");
        check(&mut tk, "case 11", "select * from t_chr_single use index(ia) where a = 10 and pk = 'u2'", r#"range:[10 "u2",10 "u2"]"#, true, &["u2 10 2"]);
        assert!(explain_has(&mut tk, &plan_tree("select * from t_chr_single use index(ia) where a = 10 and pk > 'u1'"), r#"range:(10 "u1",10 +inf]"#), "{cascades} case 11");
        tk.must_query("select * from t_chr_single use index(ia) where a = 10 and pk > 'u1' order by pk").check(["u2 10 2", "u3 10 3"]);
        assert!(explain_has(&mut tk, &plan_tree("select pk, a from t_chr_single use index(ia) where a = 10 and pk = 'u2'"), "IndexReader"), "{cascades} case 11");

        // Case 12: a single-column decimal common handle.
        tk.must_exec("CREATE TABLE t_chr_dec (pk decimal(10,2), a int, PRIMARY KEY(pk) CLUSTERED, KEY ia(a))");
        tk.must_exec("insert into t_chr_dec values (1.50, 10), (2.50, 10), (3.50, 20)");
        check(&mut tk, "case 12", "select * from t_chr_dec use index(ia) where a = 10 and pk = 2.50", "range:[10 2.50,10 2.50]", true, &["2.50 10"]);
    }
}

/// Go `TestCommonHandleIndexRangesWithTupleCompare` (70cff7d9f66): a tuple
/// comparison spanning the declared index column and the whole appended
/// common handle.
#[test]
fn test_common_handle_index_ranges_with_tuple_compare() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "CREATE TABLE t_chr_tuple (a bigint not null, b bigint not null, c bigint not null, PRIMARY KEY(b, c) CLUSTERED, KEY ia(a))",
    );
    tk.must_exec("insert into t_chr_tuple values (1, 2, 3), (1, 2, 4), (1, 3, 1), (2, 1, 1)");
    // The index key is (a, b, c), so the tuple comparison is fully covered by
    // ranges.
    assert!(explain_has(
        &mut tk,
        &plan_tree("select * from t_chr_tuple where (a, b, c) > (1, 2, 3)"),
        "range:(1 2 3,1 2 +inf], (1 2,1 +inf], (1,+inf]",
    ));
    tk.must_query("select * from t_chr_tuple where (a, b, c) > (1, 2, 3) order by a, b, c").check(["1 2 4", "1 3 1", "2 1 1"]);

    // A three-column handle reaches one column further into the appended
    // tail.
    tk.must_exec(
        "CREATE TABLE t_chr_tuple3 (a bigint not null, b bigint not null, c bigint not null, d bigint not null, PRIMARY KEY(b, c, d) CLUSTERED, KEY ia(a))",
    );
    tk.must_exec("insert into t_chr_tuple3 values (1, 2, 3, 4), (1, 2, 3, 5), (1, 2, 4, 1)");
    assert!(explain_has(
        &mut tk,
        &plan_tree("select * from t_chr_tuple3 use index(ia) where (a, b, c, d) > (1, 2, 3, 4)"),
        "range:(1 2 3 4,1 2 3 +inf], (1 2 3,1 2 +inf], (1 2,1 +inf], (1,+inf]",
    ));
    tk.must_query("select * from t_chr_tuple3 where (a, b, c, d) > (1, 2, 3, 4) order by a, b, c, d").check(["1 2 3 5", "1 2 4 1"]);
}
