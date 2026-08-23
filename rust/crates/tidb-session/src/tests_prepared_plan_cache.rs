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

use crate::tests_support::row_text;
use crate::Session;

fn cache_flag(session: &mut Session) -> String {
    row_text(session.run("SELECT @@last_plan_from_cache"))[0][0].clone()
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
