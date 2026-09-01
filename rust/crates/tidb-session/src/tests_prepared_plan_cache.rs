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

//! The PREPARED plan cache's observable contract: what
//! `@@last_plan_from_cache` reports across `EXECUTE`s. See
//! [`crate::prepared_plan_cache`] for what is and is not modelled.

use tidb_datatype::Datum;

use crate::tests_support::row_text;
use crate::Session;

#[test]
fn unchanged_session_reuses_the_prepared_plan_cache_environment() {
    let mut session = Session::new();
    let first = session.prepared_plan_cache_environment().unwrap();
    let second = session.prepared_plan_cache_environment().unwrap();
    assert!(std::sync::Arc::ptr_eq(&first, &second));

    session.run("SELECT 1").unwrap();
    let after_ordinary_statement = session.prepared_plan_cache_environment().unwrap();
    assert!(std::sync::Arc::ptr_eq(&second, &after_ordinary_statement));

    session.run("SET time_zone = '+00:00'").unwrap();
    let changed = session.prepared_plan_cache_environment().unwrap();
    assert!(!std::sync::Arc::ptr_eq(&second, &changed));
    assert!(std::sync::Arc::ptr_eq(
        &changed,
        &session.prepared_plan_cache_environment().unwrap()
    ));

    session.run("BEGIN").unwrap();
    let transaction = session.prepared_plan_cache_environment().unwrap();
    assert!(!std::sync::Arc::ptr_eq(&changed, &transaction));

    session
        .run("SET sql_select_limit = 100")
        .expect("set a plan-cache-incompatible limit");
    assert!(session.prepared_plan_cache_environment().is_none());
    assert!(session.prepared_plan_cache_environment().is_none());
}

#[test]
fn cached_dml_binding_refuses_a_pinned_historical_read() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id int primary key, v int)")
        .unwrap();
    let prepared = session
        .prepare_ast("UPDATE t SET v = ? WHERE id = ?")
        .unwrap();
    let plan = prepared.dml_plan().expect("prepared point UPDATE plan");

    assert!(session
        .bind_cached_prepared_dml(&plan, &[Datum::Int(20), Datum::Int(1)])
        .is_some());
    session.run("SET tidb_read_staleness = -1").unwrap();
    assert!(session
        .bind_cached_prepared_dml(&plan, &[Datum::Int(20), Datum::Int(1)])
        .is_none());
}

fn cache_flag(session: &mut Session) -> String {
    row_text(session.run("SELECT @@last_plan_from_cache"))[0][0].clone()
}

fn cache_and_binding_flags(session: &mut Session) -> [String; 2] {
    let row =
        row_text(session.run("SELECT @@last_plan_from_cache, @@last_plan_from_binding")).remove(0);
    [row[0].clone(), row[1].clone()]
}

/// The second `EXECUTE` of a cacheable statement is a HIT.
///
/// Go decides cacheability at PREPARE (`IsASTCacheable`) and serves the
/// second execute from the cache (`GetPlanFromPlanCache`); the statement is
/// the corpus's own (`planner/core/rule_result_reorder`).
#[test]
fn the_second_execute_of_a_cacheable_statement_reports_a_hit() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a int primary key, b int, c int, d int, key(b))")
        .unwrap();
    session
        .run("PREPARE s1 FROM 'select * from t where a > ? limit 10'")
        .unwrap();
    session.run("SET @a = 10").unwrap();

    session.run("EXECUTE s1 USING @a").unwrap();
    assert_eq!(cache_flag(&mut session), "0", "the first execute plans");
    session.run("EXECUTE s1 USING @a").unwrap();
    assert_eq!(cache_flag(&mut session), "1", "the second is served");
    // A different parameter VALUE still hits: the value is bound at
    // execution, not baked into the plan.
    session.run("SET @a = 20").unwrap();
    session.run("EXECUTE s1 USING @a").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
}

#[test]
fn prepared_set_operation_reuses_the_general_physical_plan_cache() {
    let mut session = Session::new();
    session.run("CREATE TABLE union_cache (a int)").unwrap();
    session
        .run("INSERT INTO union_cache VALUES (1), (2), (3), (9)")
        .unwrap();
    session
        .run(
            "PREPARE union_stmt FROM 'SELECT a FROM union_cache WHERE a > ? AND a < 9 UNION ALL SELECT a FROM union_cache WHERE a = 9'",
        )
        .unwrap();
    session.run("SET @a = 1").unwrap();

    assert_eq!(
        row_text(session.run("EXECUTE union_stmt USING @a")),
        [["2"], ["3"], ["9"]]
    );
    assert_eq!(cache_flag(&mut session), "0");
    assert_eq!(
        row_text(session.run("EXECUTE union_stmt USING @a")),
        [["2"], ["3"], ["9"]]
    );
    assert_eq!(cache_flag(&mut session), "1");

    session.run("SET @a = 2").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE union_stmt USING @a")),
        [["3"], ["9"]]
    );
    assert_eq!(cache_flag(&mut session), "1");
}

/// Go `pkg/executor/prepared_test.go::TestPlanCacheWithDifferentVariableTypes`:
/// a parameterized `TableDual` executes normally but is never cached, while a
/// parameterized join retains and rebuilds one general physical tree.
#[test]
fn table_dual_is_uncached_and_a_parameterized_join_is_cached() {
    let mut session = Session::new();
    session.run("PREPARE dual_stmt FROM 'SELECT ?, ?'").unwrap();
    session.run("SET @a = 1, @b = 2").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE dual_stmt USING @a, @b")),
        [["1", "2"]]
    );
    assert_eq!(cache_flag(&mut session), "0");
    session.run("SET @a = 10, @b = 'cba'").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE dual_stmt USING @a, @b")),
        [["10", "cba"]]
    );
    assert_eq!(cache_flag(&mut session), "0");

    session
        .run("CREATE TABLE t1(a varchar(20), b int, c float, key(b, a))")
        .unwrap();
    session
        .run("CREATE TABLE t2(a varchar(20), b int, c float, key(b, a))")
        .unwrap();
    session
        .run("INSERT INTO t1 VALUES('1',1,1.1),('2',2,222),('3',3,333)")
        .unwrap();
    session
        .run("INSERT INTO t2 VALUES('3',3,3.3),('2',2,222),('3',3,333)")
        .unwrap();
    session
        .run(
            "PREPARE joined FROM 'SELECT t1.c, t2.c FROM t1 JOIN t2 \
             ON t1.b = t2.b AND t1.a = t2.a WHERE t1.b = ?'",
        )
        .unwrap();
    session.run("SET @b = 1").unwrap();
    assert!(row_text(session.run("EXECUTE joined USING @b")).is_empty());
    assert_eq!(cache_flag(&mut session), "0");
    session.run("SET @b = 2").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE joined USING @b")),
        [["222", "222"]]
    );
    assert_eq!(cache_flag(&mut session), "1");
}

/// Go `pkg/executor/prepared_test.go::TestParameterPushDown`: marker values
/// inside scalar and aggregate expressions are read from the retained
/// constants on every recursive rebuild.
#[test]
fn expression_and_aggregate_parameters_rebuild_on_cache_hits() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE push (a int, b int, c int, key(a))")
        .unwrap();
    session
        .run(
            "INSERT INTO push VALUES (1,1,1),(2,2,2),(3,3,3),\
             (4,4,4),(5,5,5),(6,6,6)",
        )
        .unwrap();
    session
        .run("PREPARE filtered FROM 'SELECT a FROM push USE INDEX(a) WHERE a+0>? ORDER BY a'")
        .unwrap();
    session.run("SET @x = 1").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE filtered USING @x")),
        [["2"], ["3"], ["4"], ["5"], ["6"]]
    );
    assert_eq!(cache_flag(&mut session), "0");
    session.run("SET @x = 5").unwrap();
    assert_eq!(row_text(session.run("EXECUTE filtered USING @x")), [["6"]]);
    assert_eq!(cache_flag(&mut session), "1");

    session
        .run(
            "PREPARE pair_stmt FROM 'SELECT a,b,c FROM push USE INDEX(a) \
             WHERE a+0>? AND b>? ORDER BY a'",
        )
        .unwrap();
    session.run("SET @x = 1, @y = 1").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE pair_stmt USING @x, @y")),
        [
            ["2", "2", "2"],
            ["3", "3", "3"],
            ["4", "4", "4"],
            ["5", "5", "5"],
            ["6", "6", "6"]
        ]
    );
    assert_eq!(cache_flag(&mut session), "0");
    session.run("SET @x = 5, @y = 5").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE pair_stmt USING @x, @y")),
        [["6", "6", "6"]]
    );
    assert_eq!(cache_flag(&mut session), "1");

    session
        .run(
            "PREPARE aggregated FROM 'SELECT b, SUM(c+?) FROM push \
             GROUP BY b ORDER BY b'",
        )
        .unwrap();
    session.run("SET @x = 1").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE aggregated USING @x")),
        [
            ["1", "2"],
            ["2", "3"],
            ["3", "4"],
            ["4", "5"],
            ["5", "6"],
            ["6", "7"]
        ]
    );
    assert_eq!(cache_flag(&mut session), "0");
    session.run("SET @x = 5").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE aggregated USING @x")),
        [
            ["1", "6"],
            ["2", "7"],
            ["3", "8"],
            ["4", "9"],
            ["5", "10"],
            ["6", "11"]
        ]
    );
    assert_eq!(cache_flag(&mut session), "1");
}

/// Go places execute-time LIMIT values in the cache key. Different bounds
/// therefore enumerate separate plans instead of rebuilding one hit.
#[test]
fn parameterized_limit_values_are_distinct_cache_entries() {
    let mut session = Session::new();
    session.run("CREATE TABLE limited (a int)").unwrap();
    session
        .run("INSERT INTO limited VALUES (1),(2),(3),(4),(5),(6)")
        .unwrap();
    session
        .run("PREPARE top_stmt FROM 'SELECT a FROM limited ORDER BY a LIMIT ?'")
        .unwrap();
    session.run("SET @n = 1").unwrap();
    assert_eq!(row_text(session.run("EXECUTE top_stmt USING @n")), [["1"]]);
    assert_eq!(cache_flag(&mut session), "0");
    session.run("SET @n = 5").unwrap();
    assert_eq!(
        row_text(session.run("EXECUTE top_stmt USING @n")),
        [["1"], ["2"], ["3"], ["4"], ["5"]]
    );
    assert_eq!(cache_flag(&mut session), "0");
    session.run("EXECUTE top_stmt USING @n").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
}

/// Go `checkTypesCompatibility4PC` compares the complete parameter field
/// type. Decimal precision/scale are asymmetric, while string collation is
/// exact.
#[test]
fn parameter_type_compatibility_matches_go() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE typed (d DECIMAL(20,6), s VARCHAR(20))")
        .unwrap();
    session
        .run("INSERT INTO typed VALUES (1.2, 'a'), (123.45, 'b'), (1234.567, 'c')")
        .unwrap();
    session
        .run("PREPARE decimal_stmt FROM 'SELECT d FROM typed WHERE d >= ? ORDER BY d'")
        .unwrap();

    session.run("SET @p = 123.45").unwrap();
    session.run("EXECUTE decimal_stmt USING @p").unwrap();
    assert_eq!(cache_flag(&mut session), "0");
    session.run("SET @p = 1.2").unwrap();
    session.run("EXECUTE decimal_stmt USING @p").unwrap();
    assert_eq!(
        cache_flag(&mut session),
        "1",
        "a plan built for wider precision and scale accepts a narrower decimal"
    );
    session.run("SET @p = 1234.567").unwrap();
    session.run("EXECUTE decimal_stmt USING @p").unwrap();
    assert_eq!(
        cache_flag(&mut session),
        "0",
        "a wider decimal must enumerate another physical entry"
    );

    session
        .run("PREPARE string_stmt FROM 'SELECT s FROM typed WHERE s = ?'")
        .unwrap();
    session
        .run("SET collation_connection = 'utf8mb4_bin'")
        .unwrap();
    session.run("SET @p = 'a'").unwrap();
    session.run("EXECUTE string_stmt USING @p").unwrap();
    assert_eq!(cache_flag(&mut session), "0");
    session
        .run("SET collation_connection = 'utf8mb4_general_ci'")
        .unwrap();
    session.run("SET @p = 'a'").unwrap();
    session.run("EXECUTE string_stmt USING @p").unwrap();
    assert_eq!(
        cache_flag(&mut session),
        "0",
        "a different string collation is an incompatible parameter type"
    );
}

/// Go caches the physical UPDATE root itself, then rebuilds its `SelectPlan`
/// for new parameter values and hands both misses and hits to the ordinary
/// DML executor.
#[test]
fn prepared_update_rebuilds_and_executes_its_cached_physical_source() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE cached_update (id int primary key, v int)")
        .unwrap();
    session
        .run("INSERT INTO cached_update VALUES (1, 10), (2, 20)")
        .unwrap();
    session
        .run("PREPARE update_cached FROM 'UPDATE cached_update SET v = ? WHERE id = ?'")
        .unwrap();

    session.run("SET @v = 11, @id = 1").unwrap();
    session.run("EXECUTE update_cached USING @v, @id").unwrap();
    assert_eq!(cache_flag(&mut session), "0");

    session.run("SET @v = 22, @id = 2").unwrap();
    session.run("EXECUTE update_cached USING @v, @id").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
    assert_eq!(
        row_text(session.run("SELECT id, v FROM cached_update ORDER BY id")),
        vec![vec!["1", "11"], vec!["2", "22"]]
    );
}

#[test]
fn prepared_insert_select_rebuilds_and_executes_its_cached_select_plan() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE cached_insert_source (id int primary key)")
        .unwrap();
    session
        .run("CREATE TABLE cached_insert_target (id int primary key, v int)")
        .unwrap();
    session
        .run("INSERT INTO cached_insert_source VALUES (1), (2)")
        .unwrap();
    session
        .run(
            "PREPARE insert_cached FROM 'INSERT INTO cached_insert_target \
             SELECT id, ? FROM cached_insert_source WHERE id = ?'",
        )
        .unwrap();

    session.run("SET @v = 11, @id = 1").unwrap();
    session.run("EXECUTE insert_cached USING @v, @id").unwrap();
    assert_eq!(cache_flag(&mut session), "0");

    session.run("SET @v = 22, @id = 2").unwrap();
    session.run("EXECUTE insert_cached USING @v, @id").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
    assert_eq!(
        row_text(session.run("SELECT id, v FROM cached_insert_target ORDER BY id")),
        vec![vec!["1", "11"], vec!["2", "22"]]
    );
}

#[test]
fn prepared_delete_rebuilds_and_executes_its_cached_physical_source() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE cached_delete (id int primary key, v int)")
        .unwrap();
    session
        .run("INSERT INTO cached_delete VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    session
        .run("PREPARE delete_cached FROM 'DELETE FROM cached_delete WHERE id = ?'")
        .unwrap();

    session.run("SET @id = 1").unwrap();
    session.run("EXECUTE delete_cached USING @id").unwrap();
    assert_eq!(cache_flag(&mut session), "0");

    session.run("SET @id = 2").unwrap();
    session.run("EXECUTE delete_cached USING @id").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
    assert_eq!(
        row_text(session.run("SELECT id, v FROM cached_delete ORDER BY id")),
        vec![vec!["3", "30"]]
    );
}

#[test]
fn a_matched_binding_is_part_of_the_prepared_plan_cache_key() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a int, b int, key kb(b))")
        .unwrap();
    session
        .run(
            "CREATE SESSION BINDING FOR SELECT * FROM t WHERE a = 1 \
             USING SELECT * FROM t USE INDEX(kb) WHERE a = 1",
        )
        .unwrap();
    session
        .run("PREPARE s FROM 'SELECT * FROM t WHERE a = ?'")
        .unwrap();
    session.run("SET @a = 1").unwrap();

    session.run("EXECUTE s USING @a").unwrap();
    assert_eq!(cache_and_binding_flags(&mut session), ["0", "1"]);
    session.run("EXECUTE s USING @a").unwrap();
    assert_eq!(cache_and_binding_flags(&mut session), ["1", "1"]);
}

#[test]
fn a_prepared_binding_is_published_when_the_plan_cache_is_disabled() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a int, b int, key kb(b))")
        .unwrap();
    session
        .run(
            "CREATE SESSION BINDING FOR SELECT * FROM t WHERE a = 1 \
             USING SELECT * FROM t USE INDEX(kb) WHERE a = 1",
        )
        .unwrap();
    session
        .run("SET tidb_enable_prepared_plan_cache = OFF")
        .unwrap();
    session
        .run("PREPARE s FROM 'SELECT * FROM t WHERE a = ?'")
        .unwrap();
    session.run("SET @a = 1").unwrap();

    session.run("EXECUTE s USING @a").unwrap();
    assert_eq!(cache_and_binding_flags(&mut session), ["0", "1"]);
}

#[test]
fn disabling_the_cache_disables_retained_range_execution() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id int primary key, v int)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    session
        .run("SET tidb_enable_prepared_plan_cache = OFF")
        .unwrap();
    session
        .run("PREPARE s FROM 'SELECT v FROM t WHERE id BETWEEN ? AND ? ORDER BY v'")
        .unwrap();
    session.run("SET @lo = 1, @hi = 2").unwrap();

    for _ in 0..2 {
        assert_eq!(row_text(session.run("EXECUTE s USING @lo, @hi"))[0], ["10"]);
        assert_eq!(cache_flag(&mut session), "0");
    }
}

#[test]
fn disabling_the_cache_disables_retained_point_execution() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id int primary key, v int)")
        .unwrap();
    session.run("INSERT INTO t VALUES (1, 10)").unwrap();
    session
        .run("SET tidb_enable_prepared_plan_cache = OFF")
        .unwrap();
    session
        .run("PREPARE s FROM 'SELECT v FROM t WHERE id = ?'")
        .unwrap();
    let prepared = session.prepare_ast("SELECT v FROM t WHERE id = ?").unwrap();
    let point = prepared.point_get_plan().expect("retained point plan");
    assert!(!session.can_reuse_prepared_point_get(&point));
    session.run("SET @id = 1").unwrap();

    for _ in 0..2 {
        assert_eq!(row_text(session.run("EXECUTE s USING @id"))[0], ["10"]);
        assert_eq!(cache_flag(&mut session), "0");
    }
}

#[test]
fn point_plan_is_published_before_executor_construction() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE point_publish (id bigint primary key, v bigint)")
        .unwrap();
    let prepared = session
        .prepare_ast("SELECT v FROM point_publish WHERE id = ?")
        .unwrap();
    let plan = prepared.point_get_plan().expect("point query is cacheable");

    let first = session
        .bind_cached_prepared_point_get(&plan, &[Datum::Int(1)])
        .expect("the first point plan is generated");
    assert!(!first.cache_hit());
    // Do not construct or drain an executor. Go publishes in
    // `generateNewPlan`, before `ExecStmt.buildExecutor`, so a second bind is
    // already a hit even if execution of the first plan never started.
    drop(first);
    let second = session
        .bind_cached_prepared_point_get(&plan, &[Datum::Int(1)])
        .expect("the published point plan is reusable");
    assert!(second.cache_hit());
}

/// DDL between two executes is a MISS: the schema version is in Go's cache
/// key (`NewPlanCacheKey`), which is what keeps a cached plan from reading a
/// dropped column.
#[test]
fn ddl_between_executes_invalidates() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session
        .run("PREPARE s FROM 'select * from t where a > ?'")
        .unwrap();
    session.run("SET @a = 1").unwrap();
    session.run("EXECUTE s USING @a").unwrap();
    session.run("CREATE TABLE other (x int)").unwrap();
    session.run("EXECUTE s USING @a").unwrap();
    assert_eq!(
        cache_flag(&mut session),
        "0",
        "any DDL moves the schema version"
    );
    session.run("EXECUTE s USING @a").unwrap();
    assert_eq!(
        cache_flag(&mut session),
        "1",
        "and the next pair hits again"
    );
}

/// `ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST` is a MISS: Go stamps
/// `ExprPushDownBlackListReloadTimeStamp` on every reload and mixes it into
/// the plan-cache key (`plan_cache_utils.go:443`), because a cached plan may
/// push an expression the new blacklist refuses. The corpus reads this
/// directly (`planner/core/plan_cache`'s `TestPlanCacheExprBlacklistCompatibility`).
#[test]
fn a_blacklist_reload_invalidates() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session
        .run("PREPARE st FROM 'select * from t where mod(a, 2)=1'")
        .unwrap();
    session.run("EXECUTE st").unwrap();
    session.run("EXECUTE st").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
    session
        .run("INSERT INTO mysql.expr_pushdown_blacklist(name) VALUES ('mod')")
        .unwrap();
    session.run("ADMIN RELOAD EXPR_PUSHDOWN_BLACKLIST").unwrap();
    session.run("EXECUTE st").unwrap();
    assert_eq!(cache_flag(&mut session), "0", "the reload moved the key");
    session.run("EXECUTE st").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
}

/// A statement with a USER VARIABLE is never cached: the variable's value is
/// not in the key (`cacheableChecker`'s `*ast.VariableExpr` arm).
#[test]
fn a_user_variable_statement_never_hits() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session
        .run("PREPARE s FROM 'select * from t where a > ? and a < @ub'")
        .unwrap();
    session.run("SET @a = 1").unwrap();
    session.run("SET @ub = 100").unwrap();
    for _ in 0..2 {
        session.run("EXECUTE s USING @a").unwrap();
        assert_eq!(cache_flag(&mut session), "0");
    }
}

/// Go's `IsASTCacheable` reads this switch while visiting `ast.Limit`.
/// With it disabled, a parameterized LIMIT executes normally but never
/// enters the prepared plan cache.
#[test]
fn a_parameterized_limit_never_hits_when_its_cache_switch_is_off() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session.run("INSERT INTO t VALUES (1), (2)").unwrap();
    session
        .run("SET tidb_enable_plan_cache_for_param_limit = OFF")
        .unwrap();
    session
        .run("PREPARE s FROM 'select a from t order by a limit ?'")
        .unwrap();
    session.run("SET @n = 1").unwrap();

    for _ in 0..2 {
        assert_eq!(row_text(session.run("EXECUTE s USING @n"))[0], ["1"]);
        assert_eq!(cache_flag(&mut session), "0");
    }
}

#[test]
fn ignore_plan_cache_hint_never_hits() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session
        .run("PREPARE s FROM 'select /*+ ignore_plan_cache() */ * from t where a > ?'")
        .unwrap();
    session.run("SET @a = 0").unwrap();

    for _ in 0..2 {
        session.run("EXECUTE s USING @a").unwrap();
        assert_eq!(cache_flag(&mut session), "0");
    }
}

/// Go's `hint_only` strategy admits only statements carrying
/// `USE_PLAN_CACHE`; it is evaluated at EXECUTE time rather than frozen when
/// PREPARE parses the statement.
#[test]
fn hint_only_strategy_requires_use_plan_cache() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session
        .run("SET tidb_plan_cache_strategy = 'hint_only'")
        .unwrap();
    session
        .run("PREPARE plain FROM 'select * from t where a > ?'")
        .unwrap();
    session
        .run("PREPARE hinted FROM 'select /*+ use_plan_cache() */ * from t where a > ?'")
        .unwrap();
    session.run("SET @a = 0").unwrap();

    for _ in 0..2 {
        session.run("EXECUTE plain USING @a").unwrap();
        assert_eq!(cache_flag(&mut session), "0");
    }
    session.run("EXECUTE hinted USING @a").unwrap();
    assert_eq!(cache_flag(&mut session), "0");
    session.run("EXECUTE hinted USING @a").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
}

#[test]
fn plan_cache_max_plan_size_is_checked_after_physical_planning() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a int)").unwrap();
    session
        .run("SET tidb_plan_cache_max_plan_size = 1")
        .unwrap();
    session
        .run("PREPARE s FROM 'select * from t where a > ?'")
        .unwrap();
    session.run("SET @a = 0").unwrap();

    for _ in 0..2 {
        session.run("EXECUTE s USING @a").unwrap();
        assert_eq!(cache_flag(&mut session), "0");
    }
}

/// A plan containing an APPLY is never cached: Go's
/// `isPhysicalPlanCacheable` refuses `PhysicalApply` on the BUILT plan, after
/// the AST checker said yes -- a per-outer-row executor cannot be reused
/// across parameter sets. The statement is the corpus's own
/// (`executor/parallel_apply`), whose recording expects 0 after two executes.
#[test]
fn a_correlated_apply_plan_never_hits() {
    let mut session = Session::new();
    session.run("CREATE TABLE t1 (a int, b int)").unwrap();
    session.run("CREATE TABLE t2 (a int, b int)").unwrap();
    session.run("INSERT INTO t1 VALUES (1, 2), (2, 1)").unwrap();
    session
        .run("INSERT INTO t2 VALUES (0, 1), (2, -1)")
        .unwrap();
    session
        .run(
            "PREPARE stmt FROM 'select * from t1 where t1.b >= \
             (select sum(t2.b) from t2 where t2.a > t1.a and t2.a > ?)'",
        )
        .unwrap();
    session.run("SET @a = 1").unwrap();
    for _ in 0..2 {
        session.run("EXECUTE stmt USING @a").unwrap();
        assert_eq!(cache_flag(&mut session), "0");
    }
}

/// The fulltext LIKE fallback and the plan cache, together: a LITERAL
/// `AGAINST` is cacheable, because the baked pattern constants are stable
/// across executions (the corpus's own test 37,
/// `planner/core/fulltext_search`).
#[test]
fn a_literal_fts_search_is_cacheable() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_opt_enable_alternative_logical_plans=ON")
        .unwrap();
    session
        .run("CREATE TABLE articles (id int primary key, title varchar(200), body text)")
        .unwrap();
    session
        .run("INSERT INTO articles VALUES (1, 'MySQL Tutorial', 'basic')")
        .unwrap();
    session
        .run("PREPARE st FROM 'select id, title from articles where match(title) against(''MySQL'')'")
        .unwrap();
    session.run("EXECUTE st").unwrap();
    session.run("EXECUTE st").unwrap();
    assert_eq!(cache_flag(&mut session), "1");
}
