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

//! 6d's seam tests, for [`super::set_opr`] and [`super::cte`].
//!
//! WRITTEN, not transcreated. Go's coverage for `buildSetOpr`/`buildCte` is
//! `pkg/planner/core/casetest` and `tests/integrationtest`, both of which need
//! a live session, a `testkit` cluster and a golden-plan file; none of that is
//! reachable from this crate. The ONE exception is
//! [`test_union_join_field_type_matches_gos_own_cases`], whose expectations are
//! read off `pkg/types/field_type_test.go`'s `TestAggFieldType` plus
//! `unionJoinFieldType`'s own arithmetic — that one is TRANSCREATED reasoning
//! over a WRITTEN driver.
//!
//! What each group proves is named on the group.

use tidb_ast::{QueryStmt, Stmt};
use tidb_datatype::{
    FieldType, FieldTypeCode, FieldTypeFlags, SessionTimeZone, UNSPECIFIED_LENGTH,
};
use tidb_expr::ZonedNoColumns;

use super::catalog::{SourceColumn, SourceTable, TableSource};
use super::set_opr::union_join_field_type;
use super::PlanBuilder;
use crate::expression_rewriter::ColumnIdAllocator;
use crate::logical::LogicalPlan;
use crate::plan_base::{PlanError, PlanIdAllocator};

// ***** the catalogue *****

struct TestCatalog {
    current_database: String,
    tables: Vec<SourceTable>,
}

impl TableSource for TestCatalog {
    fn current_database(&self) -> &str {
        &self.current_database
    }

    fn find_table(&self, db_name: &str, table_name: &str) -> Option<&SourceTable> {
        self.tables.iter().find(|table| {
            table.db_name.eq_ignore_ascii_case(db_name)
                && table.table_name.eq_ignore_ascii_case(table_name)
        })
    }

    fn database_exists(&self, db_name: &str) -> bool {
        self.tables
            .iter()
            .any(|table| table.db_name.eq_ignore_ascii_case(db_name))
    }
}

fn column(offset: usize, name: &str, ret_type: FieldType) -> SourceColumn {
    SourceColumn {
        id: offset as i64 + 1,
        name: name.to_owned(),
        is_primary_key: false,
        offset,
        ret_type,
        is_public: true,
        is_hidden: false,
        is_virtual_generated: false,
    }
}

fn bigint() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(20);
    ft.set_decimal(0);
    ft
}

fn varchar(flen: i64) -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::Varchar);
    ft.set_flen(flen);
    ft.set_decimal(UNSPECIFIED_LENGTH);
    ft.set_charset_name("utf8mb4");
    ft.set_collation_name("utf8mb4_bin");
    ft
}

/// `CREATE TABLE test.t (a BIGINT, b BIGINT)` and
/// `CREATE TABLE test.s (a BIGINT, v VARCHAR(10))`.
fn catalog() -> TestCatalog {
    TestCatalog {
        current_database: "test".to_owned(),
        tables: vec![
            SourceTable {
                table_id: 100,
                table_name: "t".to_owned(),
                db_name: "test".to_owned(),
                physical_table_id: 100,
                columns: vec![column(0, "a", bigint()), column(1, "b", bigint())],
                ..SourceTable::default()
            },
            SourceTable {
                table_id: 101,
                table_name: "s".to_owned(),
                db_name: "test".to_owned(),
                physical_table_id: 101,
                columns: vec![column(0, "a", bigint()), column(1, "v", varchar(10))],
                ..SourceTable::default()
            },
        ],
    }
}

struct Harness {
    catalog: TestCatalog,
    ctx: ZonedNoColumns,
    plan_ids: PlanIdAllocator,
    column_ids: ColumnIdAllocator,
}

impl Harness {
    fn new() -> Self {
        Self {
            catalog: catalog(),
            ctx: ZonedNoColumns(SessionTimeZone::utc()),
            plan_ids: PlanIdAllocator::default(),
            column_ids: ColumnIdAllocator::new(),
        }
    }

    fn builder(&self) -> PlanBuilder<'_, TestCatalog, ZonedNoColumns> {
        PlanBuilder::new(
            &self.catalog,
            &self.ctx,
            &self.plan_ids,
            &self.column_ids,
            SessionTimeZone::utc(),
        )
    }
}

fn parse_query(sql: &str) -> QueryStmt {
    match tidb_parser::parse(sql).expect("the seam's SQL parses") {
        Stmt::Query(query) => query.into_inner(),
        other => panic!("expected a query, got {other:?}"),
    }
}

fn build_in(
    builder: &mut PlanBuilder<'_, TestCatalog, ZonedNoColumns>,
    sql: &str,
) -> Result<LogicalPlan, PlanError> {
    let query = parse_query(sql);
    builder.build_query_stmt(&query, false)
}

fn build(sql: &str) -> LogicalPlan {
    let harness = Harness::new();
    let mut builder = harness.builder();
    build_in(&mut builder, sql)
        .unwrap_or_else(|error| panic!("{sql} should build: {}", error.message()))
}

fn build_err(sql: &str) -> String {
    let harness = Harness::new();
    let mut builder = harness.builder();
    match build_in(&mut builder, sql) {
        Ok(_) => panic!("{sql} should have been refused"),
        Err(error) => error.message().to_owned(),
    }
}

/// The first operator of the given kind on the way down from the root.
fn find<'a>(plan: &'a LogicalPlan, tp: &str) -> Option<&'a LogicalPlan> {
    if plan.tp() == tp {
        return Some(plan);
    }
    plan.children().iter().find_map(|child| find(child, tp))
}

fn operator_names(plan: &LogicalPlan) -> Vec<String> {
    let mut names = vec![plan.tp().to_owned()];
    for child in plan.children() {
        names.extend(operator_names(child));
    }
    names
}

// ***** UNION vs UNION ALL: the de-duplication SHAPE *****

#[test]
fn test_union_all_is_a_bare_union_all_with_one_projection_per_branch() {
    // `buildUnionAll` (`:2373`) wraps every branch in the projection
    // `buildProjection4Union` builds, and nothing de-duplicates.
    let plan = build("SELECT a FROM t UNION ALL SELECT b FROM t");
    let LogicalPlan::UnionAll(union) = &plan else {
        panic!("expected a Union at the root, got {}", plan.tp());
    };
    assert_eq!(union.base.children().len(), 2);
    assert!(union
        .base
        .children()
        .iter()
        .all(|child| child.tp() == "Projection"));
    assert!(!operator_names(&plan).iter().any(|tp| tp == "Aggregation"));
}

#[test]
fn test_union_distinct_puts_an_aggregation_over_the_union_all() {
    // `buildUnion` (`:2317`): the DISTINCT prefix is unioned and then handed
    // to `buildDistinct`, which is a `LogicalAggregation` of `firstrow`s.
    let plan = build("SELECT a FROM t UNION SELECT b FROM t");
    let LogicalPlan::Aggregation(agg) = &plan else {
        panic!("expected an Aggregation at the root, got {}", plan.tp());
    };
    assert!(agg.agg_funcs.iter().all(|func| func.name() == "firstrow"));
    assert_eq!(agg.group_by_items.len(), 1);
    assert_eq!(agg.base.children()[0].tp(), "Union");
}

#[test]
fn test_a_distinct_union_overrides_every_all_union_to_its_left() {
    // `divideUnionSelectPlans` (`:2355`), quoting MySQL: "Mixed UNION types
    // are treated such that a DISTINCT union overrides any ALL union to its
    // left." All THREE branches therefore land in the de-duplicated half.
    let plan = build("SELECT a FROM t UNION ALL SELECT b FROM t UNION SELECT a FROM s");
    let LogicalPlan::Aggregation(agg) = &plan else {
        panic!("expected an Aggregation at the root, got {}", plan.tp());
    };
    let LogicalPlan::UnionAll(union) = &agg.base.children()[0] else {
        panic!("expected a Union under the Aggregation");
    };
    assert_eq!(union.base.children().len(), 3);
}

#[test]
fn test_an_all_union_to_the_right_stays_a_separate_union_all() {
    // The mirror case: the DISTINCT half covers branches 1-2, and branch 3
    // joins a SECOND union whose FIRST child is the de-duplicated half —
    // Go's "Can't change the statements order in order to get the correct
    // column info".
    let plan = build("SELECT a FROM t UNION SELECT b FROM t UNION ALL SELECT a FROM s");
    let LogicalPlan::UnionAll(outer) = &plan else {
        panic!("expected a Union at the root, got {}", plan.tp());
    };
    assert_eq!(outer.base.children().len(), 2);
    // Each child is `buildProjection4Union`'s projection; the FIRST one's own
    // child is the de-duplicating aggregation.
    let first = &outer.base.children()[0];
    assert_eq!(first.tp(), "Projection");
    assert_eq!(first.children()[0].tp(), "Aggregation");
}

// ***** INTERSECT / EXCEPT: operator shape and precedence *****

#[test]
fn test_intersect_builds_a_semi_join_over_a_de_duplicated_left_side() {
    // `buildSemiJoinForSetOperator` (`:2201`).
    let plan = build("SELECT a FROM t INTERSECT SELECT b FROM t");
    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join at the root, got {}", plan.tp());
    };
    assert_eq!(join.join_type, crate::find_best_task::LogicalJoinType::Semi);
    assert_eq!(join.base.children()[0].tp(), "Aggregation");
    // The two branches carry the same column type, so the `<=>` becomes a
    // JOIN KEY rather than a general condition.
    assert_eq!(join.equal_conditions.len(), 1);
    assert!(join.other_conditions.is_empty());
    assert_eq!(join.equal_conditions[0].func_name.original(), "nulleq");
}

#[test]
fn test_except_builds_an_anti_semi_join() {
    let plan = build("SELECT a FROM t EXCEPT SELECT b FROM t");
    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join at the root, got {}", plan.tp());
    };
    assert_eq!(
        join.join_type,
        crate::find_best_task::LogicalJoinType::AntiSemi
    );
}

#[test]
fn test_intersect_binds_tighter_than_union() {
    // `buildSetOpr`'s grouping loop (`:2123`): `a UNION b INTERSECT c` is
    // `a UNION (b INTERSECT c)`, so the UNION is the root and the semi join
    // lives under its SECOND branch only.
    let plan = build("SELECT a FROM t UNION SELECT b FROM t INTERSECT SELECT a FROM s");
    let LogicalPlan::Aggregation(agg) = &plan else {
        panic!("expected an Aggregation at the root, got {}", plan.tp());
    };
    let LogicalPlan::UnionAll(union) = &agg.base.children()[0] else {
        panic!("expected a Union under the Aggregation");
    };
    assert_eq!(union.base.children().len(), 2);
    assert!(find(&union.base.children()[0], "Join").is_none());
    assert!(find(&union.base.children()[1], "Join").is_some());
}

#[test]
fn test_except_and_union_share_a_precedence_and_fold_left_to_right() {
    // `buildExcept` (`:2286`): an EXCEPT closes the union run to its left, so
    // `a UNION b EXCEPT c` is `(a UNION b) EXCEPT c` — the anti semi join is
    // the ROOT and the union is its left input.
    let plan = build("SELECT a FROM t UNION SELECT b FROM t EXCEPT SELECT a FROM s");
    let LogicalPlan::Join(join) = &plan else {
        panic!("expected a Join at the root, got {}", plan.tp());
    };
    assert_eq!(
        join.join_type,
        crate::find_best_task::LogicalJoinType::AntiSemi
    );
    assert!(find(&join.base.children()[0], "Union").is_some());
}

#[test]
fn test_intersect_all_and_except_all_are_refused_with_gos_own_message() {
    assert_eq!(
        build_err("SELECT a FROM t INTERSECT ALL SELECT b FROM t"),
        "TiDB do not support intersect all"
    );
    assert_eq!(
        build_err("SELECT a FROM t EXCEPT ALL SELECT b FROM t"),
        "TiDB do not support except all"
    );
}

#[test]
fn test_a_branch_with_a_different_arity_is_refused() {
    // `ErrWrongNumberOfColumnsInSelect`, raised from
    // `divideUnionSelectPlans`/`buildExcept`/`buildIntersect` alike.
    assert!(build_err("SELECT a FROM t UNION SELECT a, b FROM t")
        .contains("different number of columns"));
    assert!(build_err("SELECT a FROM t INTERSECT SELECT a, b FROM t")
        .contains("different number of columns"));
}

// ***** result column type unification across branches *****

#[test]
fn test_a_union_joins_its_branches_types_and_casts_the_odd_branch() {
    // `buildProjection4Union` (`:2053`): the union's column type is
    // `unionJoinFieldType` folded across the branches, and a branch whose own
    // type differs gets a CAST in its projection.
    let plan = build("SELECT a FROM t UNION ALL SELECT v FROM s");
    let LogicalPlan::UnionAll(union) = &plan else {
        panic!("expected a Union at the root, got {}", plan.tp());
    };
    let schema = union.base.base.schema().expect("the union has a schema");
    assert_eq!(schema.columns.len(), 1);
    let joined = schema.columns[0]
        .ret_type
        .as_ref()
        .expect("a union column has a type");
    // BIGINT joined with VARCHAR is a string type, per `AggFieldType`'s table.
    assert_eq!(joined.eval_type(), tidb_datatype::EvalType::String);

    // The BIGINT branch is cast; the VARCHAR branch may or may not be,
    // depending on the joined flen/charset, so only the first is asserted.
    let LogicalPlan::Projection(first) = &union.base.children()[0] else {
        panic!("expected a Projection per branch");
    };
    assert!(
        matches!(&first.exprs[0], tidb_expr::expression::Expression::ScalarFunction(sf)
            if sf.func_name.original().starts_with("cast")),
        "the BIGINT branch is cast to the joined type, got {:?}",
        first.exprs[0]
    );
}

#[test]
fn test_a_union_column_takes_the_first_branchs_name_and_loses_its_table() {
    // Go builds `&types.FieldName{ColName: u.Children()[0].OutputNames()[i].ColName}`
    // — nothing else. So `t.a UNION s.v` is called `a`, unqualified.
    let plan = build("SELECT a FROM t UNION ALL SELECT v FROM s");
    let names = plan.output_names();
    assert_eq!(names.len(), 1);
    assert_eq!(names[0].names.column.lower, "a");
    assert!(names[0].names.table.original.is_empty());
    assert!(names[0].names.database.original.is_empty());
}

#[test]
fn test_union_join_field_type_matches_gos_own_cases() {
    // TRANSCREATED from `unionJoinFieldType`'s own body (`:2001`) and the
    // `AggFieldType` table it calls.

    // "We ignore the pure NULL type": the other side is returned WHOLE.
    let null = FieldType::new(FieldTypeCode::Null);
    let joined = union_join_field_type(&null, &bigint());
    assert_eq!(joined.code(), FieldTypeCode::LongLong);
    let joined = union_join_field_type(&bigint(), &null);
    assert_eq!(joined.code(), FieldTypeCode::LongLong);

    // "Non-decimal results will be unsigned when a,b both unsigned."
    let mut unsigned = bigint();
    unsigned.add_flags(FieldTypeFlags::UNSIGNED);
    let both = union_join_field_type(&unsigned, &unsigned);
    assert!(both.flags() & FieldTypeFlags::UNSIGNED != 0);
    let mixed = union_join_field_type(&unsigned, &bigint());
    assert_eq!(
        mixed.flags() & FieldTypeFlags::UNSIGNED,
        0,
        "one signed branch makes the union signed"
    );

    // The `MaxIntWidth` promotion: a non-INT result that had an INT branch is
    // widened to 20 so the integer's digits still fit.
    let mut narrow = FieldType::new(FieldTypeCode::Varchar);
    narrow.set_flen(3);
    narrow.set_decimal(UNSPECIFIED_LENGTH);
    let widened = union_join_field_type(&bigint(), &narrow);
    assert_ne!(widened.eval_type(), tidb_datatype::EvalType::Int);
    assert_eq!(widened.flen(), 20);

    // An UNSPECIFIED flen on either side leaves the result unspecified.
    let mut unbounded = FieldType::new(FieldTypeCode::Varchar);
    unbounded.set_flen(UNSPECIFIED_LENGTH);
    unbounded.set_decimal(UNSPECIFIED_LENGTH);
    let joined = union_join_field_type(&unbounded, &varchar(5));
    assert_eq!(joined.flen(), UNSPECIFIED_LENGTH);
}

// ***** the statement tail *****

#[test]
fn test_a_set_operations_order_by_and_limit_sit_above_the_whole_operation() {
    let plan = build("SELECT a FROM t UNION ALL SELECT b FROM t ORDER BY a LIMIT 3");
    assert_eq!(plan.tp(), "Limit");
    assert_eq!(plan.children()[0].tp(), "Sort");
    assert_eq!(plan.children()[0].children()[0].tp(), "Union");
}

#[test]
fn test_a_set_operation_is_usable_as_a_derived_table() {
    // `buildResultSetNode`'s `*ast.SetOprStmt` arm (`:579`), which 6b left
    // refused by name.
    let plan = build("SELECT x.a FROM (SELECT a FROM t UNION ALL SELECT b FROM t) x");
    assert!(find(&plan, "Union").is_some());
}

// ***** CTE reference resolution *****

#[test]
fn test_a_cte_reference_becomes_a_logical_cte_over_a_shared_class() {
    // `tryBuildCTE` (`:4739`) plus `buildWith` (`:7994`).
    let plan = build("WITH c AS (SELECT a FROM t) SELECT a FROM c");
    let cte = find(&plan, "CTE").expect("a LogicalCTE is built for the reference");
    let LogicalPlan::CTE(cte) = cte else {
        unreachable!()
    };
    assert_eq!(cte.cte_name, "c");
    let class = cte.cte.as_ref().expect("the reference points at a class");
    assert!(class.borrow().seed_part_logical_plan.is_some());
    assert!(class.borrow().recursive_part_logical_plan.is_none());
    // Not inlined: the consumer count is unavailable here, which is Go's
    // "cannot determine" arm; see `cte`'s ConsumerCount narrowing.
    assert!(find(&plan, "DataSource").is_none());
}

#[test]
fn test_a_cte_shadows_a_real_table_of_the_same_name() {
    // Go looks the CTE up FIRST, in `buildDataSource`'s `dbName.L == ""` arm.
    let plan = build("WITH t AS (SELECT a FROM s) SELECT a FROM t");
    assert!(find(&plan, "CTE").is_some());
}

#[test]
fn test_two_ctes_with_the_same_name_are_refused() {
    // `buildWith`'s `ErrNonUniqTable` (`:7998`).
    assert_eq!(
        build_err("WITH c AS (SELECT a FROM t), c AS (SELECT b FROM t) SELECT a FROM c"),
        "Not unique table/alias"
    );
}

#[test]
fn test_a_ctes_column_list_renames_its_output_and_a_wrong_length_is_refused() {
    // `adjustCTEPlanOutputName` (`:7916`).
    let plan = build("WITH c (x) AS (SELECT a FROM t) SELECT x FROM c");
    assert_eq!(plan.output_names()[0].names.column.lower, "x");
    assert!(
        build_err("WITH c (x, y) AS (SELECT a FROM t) SELECT x FROM c")
            .contains("different column counts")
    );
}

#[test]
fn test_a_later_cte_may_reference_an_earlier_one() {
    let plan = build("WITH c1 AS (SELECT a FROM t), c2 AS (SELECT a FROM c1) SELECT a FROM c2");
    assert!(find(&plan, "CTE").is_some());
}

#[test]
fn test_a_non_recursive_cte_cannot_see_itself() {
    // `tryBuildCTE`'s "Can't see this CTE, try outer definition" — the name
    // falls through to the real table `t`, and there is no table `c`.
    assert!(build_err("WITH c AS (SELECT a FROM c) SELECT a FROM c").contains("doesn't exist"));
    // Inside the CTE's own body, `t` is the real TABLE — the seed plan the
    // class holds is a DataSource, not a second CTE reference.
    let plan = build("WITH t AS (SELECT a FROM t) SELECT a FROM t");
    let LogicalPlan::CTE(cte) = find(&plan, "CTE").expect("the outer reference") else {
        unreachable!()
    };
    let class = cte.cte.as_ref().expect("a class");
    let class = class.borrow();
    let seed = class.seed_part_logical_plan.as_ref().expect("a seed");
    assert!(find(seed, "DataSource").is_some());
    assert!(find(seed, "CTE").is_none());
}

// ***** recursive CTE *****

#[test]
fn test_a_recursive_cte_builds_a_seed_a_recursive_part_and_a_cte_table() {
    // `buildRecursiveCTE` (`:7750`) end to end: the seed is the terms before
    // the self-reference, the recursive part is what follows, and the
    // reference inside it is a `LogicalCTETable` over the same storage.
    let plan = build(
        "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c",
    );
    let LogicalPlan::CTE(cte) = find(&plan, "CTE").expect("a LogicalCTE") else {
        unreachable!()
    };
    let class = cte.cte.as_ref().expect("a class");
    let class = class.borrow();
    assert!(class.seed_part_logical_plan.is_some());
    let recursive = class
        .recursive_part_logical_plan
        .as_ref()
        .expect("a recursive part");
    // `UNION ALL` between the seed and the recursive part, so NOT distinct.
    assert!(!class.is_distinct);
    let cte_table = find(recursive, "CTETable").expect("the self-reference");
    let LogicalPlan::CTETable(cte_table) = cte_table else {
        unreachable!()
    };
    assert_eq!(cte_table.id_for_storage, class.id_for_storage);
    assert_eq!(cte_table.name, "c");
}

#[test]
fn test_union_between_seed_and_recursive_part_records_is_distinct() {
    let plan = build(
        "WITH RECURSIVE c (n) AS (SELECT 1 UNION SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c",
    );
    let LogicalPlan::CTE(cte) = find(&plan, "CTE").expect("a LogicalCTE") else {
        unreachable!()
    };
    assert!(cte.cte.as_ref().expect("a class").borrow().is_distinct);
}

#[test]
fn test_the_recursive_parts_columns_are_nullable_and_freshly_identified() {
    // `getResultCTESchema` (`:8049`): "The recursive part/CTE's schema is
    // nullable, and the UID should be unique."
    let plan = build("WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c");
    let LogicalPlan::CTE(cte) = find(&plan, "CTE").expect("a LogicalCTE") else {
        unreachable!()
    };
    let schema = cte.base.base.schema().expect("the reference has a schema");
    for column in &schema.columns {
        let ret_type = column.ret_type.as_ref().expect("a type");
        assert_eq!(
            ret_type.flags() & FieldTypeFlags::NOT_NULL,
            0,
            "a recursive CTE's column is nullable"
        );
    }
    let class = cte.cte.as_ref().expect("a class");
    let seed_schema = class
        .borrow()
        .seed_part_logical_plan
        .as_ref()
        .and_then(|seed| seed.schema().cloned())
        .expect("a seed schema");
    for (reference, seed) in schema.columns.iter().zip(&seed_schema.columns) {
        assert_ne!(
            reference.unique_id, seed.unique_id,
            "a reference re-allocates every unique ID"
        );
    }
}

#[test]
fn test_a_recursive_cte_whose_first_term_is_recursive_is_refused() {
    assert!(build_err(
        "WITH RECURSIVE c (n) AS (SELECT n FROM c UNION ALL SELECT 1) SELECT n FROM c"
    )
    .contains("non-recursive query blocks"));
}

#[test]
fn test_a_recursive_cte_without_a_union_is_refused() {
    // `buildRecursiveCTE`'s `default` arm refines
    // `ErrCTERecursiveRequiresNonRecursiveFirst` into
    // `ErrCTERecursiveRequiresUnion`.
    assert!(
        build_err("WITH RECURSIVE c (n) AS (SELECT n FROM c) SELECT n FROM c")
            .contains("neither aggregation nor window functions")
    );
}

#[test]
fn test_only_union_may_join_the_seed_and_the_recursive_part() {
    let message = build_err(
        "WITH RECURSIVE c (n) AS (SELECT 1 EXCEPT SELECT n + 1 FROM c WHERE n < 5) SELECT n FROM c",
    );
    assert!(
        message.contains("between seed part and recursive part") || message.contains("except all"),
        "unexpected refusal: {message}"
    );
}

#[test]
fn test_order_by_over_a_recursive_union_is_refused() {
    assert!(build_err(
        "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5 ORDER BY n) SELECT n FROM c"
    )
    .contains("recursive"));
}

#[test]
fn test_a_recursive_declaration_with_no_self_reference_is_an_ordinary_cte() {
    // "In this case, even if SQL specifies 'WITH RECURSIVE', the CTE is
    // non-recursive."
    let plan = build(
        "WITH RECURSIVE c (n) AS (SELECT a FROM t UNION ALL SELECT b FROM t) SELECT n FROM c",
    );
    let LogicalPlan::CTE(cte) = find(&plan, "CTE").expect("a LogicalCTE") else {
        unreachable!()
    };
    assert!(cte
        .cte
        .as_ref()
        .expect("a class")
        .borrow()
        .recursive_part_logical_plan
        .is_none());
}

#[test]
fn test_a_recursive_ctes_limit_becomes_the_classs_limit_bounds() {
    // `buildRecursiveCTE`'s step 4: "Limit clause is for the whole CTE instead
    // of only for the seed part", and `tryBuildCTE` reads it back as
    // `LimitBeg`/`LimitEnd`.
    let plan = build(
        "WITH RECURSIVE c (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM c WHERE n < 5 LIMIT 2, 3) SELECT n FROM c",
    );
    let LogicalPlan::CTE(cte) = find(&plan, "CTE").expect("a LogicalCTE") else {
        unreachable!()
    };
    let class = cte.cte.as_ref().expect("a class");
    let class = class.borrow();
    assert!(class.has_limit);
    assert_eq!((class.limit_beg, class.limit_end), (2, 5));
}

// ***** the sequence, which only MPP shared execution asks for *****

#[test]
fn test_no_sequence_is_built_unless_mpp_shared_cte_execution_is_on() {
    // `tryToBuildSequence` (`:4624`) returns the plan untouched by default.
    let plan = build("WITH c AS (SELECT a FROM t) SELECT a FROM c");
    assert!(!operator_names(&plan).iter().any(|tp| tp == "Sequence"));

    let harness = Harness::new();
    let mut builder = harness.builder();
    builder.enable_mpp_shared_cte_execution = true;
    let plan = build_in(&mut builder, "WITH c AS (SELECT a FROM t) SELECT a FROM c")
        .expect("the sequence form builds");
    assert_eq!(plan.tp(), "Sequence");
    // The CTE producers come FIRST and the main query LAST; see
    // `logical::sequence`'s header.
    let children = plan.children();
    assert_eq!(children.len(), 2);
    assert_eq!(children[0].tp(), "CTE");
}
