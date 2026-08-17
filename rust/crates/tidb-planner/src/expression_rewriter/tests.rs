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

//! WRITTEN tests, not transcreated: Go's own coverage for
//! `expression_rewriter.go` is end-to-end SQL through `tests/integrationtest`
//! and `planner/core/casetest`, neither of which is reachable from a crate
//! with no parser and no optimizer driver. These check the PLAN SHAPE each
//! subquery form produces, which is the property those SQL tests are really
//! asserting.

use tidb_datatype::{FieldName, FieldNameMetadata, FieldType, FieldTypeCode, QualifiedColumnName};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::NoColumns;

use super::*;
use crate::logical::data_source::DataSource;
use crate::logical::selection::LogicalSelection;

// ***** fixtures *****

fn long_long() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn col(unique_id: i64) -> Column {
    Column::new(unique_id, long_long())
}

fn name(table: &str, column: &str) -> FieldName {
    FieldName::new(FieldNameMetadata {
        original_table: tidb_datatype::IdentifierMetadata::new(table),
        original_column: tidb_datatype::IdentifierMetadata::new(column),
        database: tidb_datatype::IdentifierMetadata::new("test"),
        table: tidb_datatype::IdentifierMetadata::new(table),
        column: tidb_datatype::IdentifierMetadata::new(column),
    })
}

fn qualified(table: &str, column: &str) -> QualifiedColumnName {
    QualifiedColumnName::new("", table, column)
}

struct Fixture {
    plan_ids: PlanIdAllocator,
    column_ids: ColumnIdAllocator,
    ctx: NoColumns,
}

impl Fixture {
    fn new() -> Self {
        Self {
            plan_ids: PlanIdAllocator::new(),
            column_ids: ColumnIdAllocator::new(),
            ctx: NoColumns,
        }
    }

    fn rewriter(&self, flags: RewriterSessionFlags) -> ExpressionRewriter<'_, NoColumns> {
        ExpressionRewriter::new(RewriterEnv {
            ctx: &self.ctx,
            plan_ids: &self.plan_ids,
            column_ids: &self.column_ids,
            select_offset: 0,
            flags,
            hints: RewriterHints::default(),
        })
    }

    /// A leaf that produces `columns`, standing in for a `DataSource`.
    fn source(&self, columns: Vec<Column>, names: Vec<FieldName>) -> LogicalPlan {
        let base = BaseLogicalPlan::new(&self.plan_ids, DataSource::TYPE, 0);
        let mut plan = LogicalPlan::DataSource(DataSource::new(base, 1, "t"));
        set_own_schema(&mut plan, Schema::new(columns), names);
        plan
    }

    /// `t(a)`, the outer side of every test below.
    fn outer(&self) -> LogicalPlan {
        self.source(vec![col(1)], vec![name("t", "a")])
    }

    /// `s(b)`, the inner side.
    fn inner(&self) -> LogicalPlan {
        self.source(vec![col(2)], vec![name("s", "b")])
    }
}

fn join_type_of(plan: &LogicalPlan) -> LogicalJoinType {
    match plan {
        LogicalPlan::Apply(apply) => apply.join.join_type,
        LogicalPlan::Join(join) => join.join_type,
        other => panic!("not a join-shaped plan: {}", other.tp()),
    }
}

fn correlated(unique_id: i64) -> Expression {
    Expression::CorrelatedColumn(CorrelatedColumn {
        column: col(unique_id),
        data: None,
    })
}

// ***** EXISTS *****

#[test]
fn exists_builds_a_semi_apply_with_no_condition() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        disable_subquery_preprocessing: true,
        ..RewriterSessionFlags::default()
    });
    let outcome = er
        .handle_exist_subquery(f.outer(), f.inner(), false, 0)
        .expect("exists rewrite");
    let ScalarSubqueryOutcome::Applied(plan) = outcome else {
        panic!("expected an apply");
    };
    assert_eq!(join_type_of(&plan), LogicalJoinType::Semi);
    let LogicalPlan::Apply(apply) = &plan else {
        unreachable!()
    };
    // A semi join's schema is the outer one: no aux column, nothing appended.
    assert_eq!(plan.schema().expect("schema").len(), 1);
    assert!(apply.join.equal_conditions.is_empty());
    assert!(apply.join.other_conditions.is_empty());
    // Nothing is pushed on the ctx stack when no value is needed.
    assert_eq!(er.ctx_stack_len(), 0);
}

#[test]
fn not_exists_builds_an_anti_semi_apply() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        disable_subquery_preprocessing: true,
        ..RewriterSessionFlags::default()
    });
    let outcome = er
        .handle_exist_subquery(f.outer(), f.inner(), true, 0)
        .expect("not exists rewrite");
    let ScalarSubqueryOutcome::Applied(plan) = outcome else {
        panic!("expected an apply");
    };
    assert_eq!(join_type_of(&plan), LogicalJoinType::AntiSemi);
}

#[test]
fn exists_as_scalar_builds_a_left_outer_semi_apply_with_an_aux_column() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        disable_subquery_preprocessing: true,
        ..RewriterSessionFlags::default()
    });
    er.as_scalar = true;
    let outcome = er
        .handle_exist_subquery(f.outer(), f.inner(), false, 0)
        .expect("exists rewrite");
    let ScalarSubqueryOutcome::Applied(plan) = outcome else {
        panic!("expected an apply");
    };
    assert_eq!(join_type_of(&plan), LogicalJoinType::LeftOuterSemi);
    // The aux column carrying the match answer is appended to the outer schema.
    let schema = plan.schema().expect("schema");
    assert_eq!(schema.len(), 2);
    // And it is what the parent expression reads.
    assert_eq!(er.ctx_stack_len(), 1);
    let Expression::Column(pushed) = &er.ctx_stack[0] else {
        panic!("expected a column on the stack");
    };
    assert_eq!(pushed.unique_id, schema.columns[1].unique_id);
}

#[test]
fn not_exists_as_scalar_builds_an_anti_left_outer_semi_apply() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        disable_subquery_preprocessing: true,
        ..RewriterSessionFlags::default()
    });
    er.as_scalar = true;
    let outcome = er
        .handle_exist_subquery(f.outer(), f.inner(), true, 0)
        .expect("not exists rewrite");
    let ScalarSubqueryOutcome::Applied(plan) = outcome else {
        panic!("expected an apply");
    };
    assert_eq!(join_type_of(&plan), LogicalJoinType::AntiLeftOuterSemi);
}

#[test]
fn an_uncorrelated_exists_is_evaluated_separately() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let outcome = er
        .handle_exist_subquery(f.outer(), f.inner(), false, 0)
        .expect("exists rewrite");
    // Go optimizes and runs it here; this port reports it instead.
    assert!(matches!(
        outcome,
        ScalarSubqueryOutcome::EvaluateSeparately { .. }
    ));
}

#[test]
fn a_correlated_exists_must_build_an_apply() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let inner = correlated_inner(&f);
    let outcome = er
        .handle_exist_subquery(f.outer(), inner, false, 0)
        .expect("exists rewrite");
    assert!(matches!(outcome, ScalarSubqueryOutcome::Applied(_)));
}

/// `select ... from s where s.b = t.a`, whose filter reads the OUTER column.
fn correlated_inner(f: &Fixture) -> LogicalPlan {
    let base = BaseLogicalPlan::new(&f.plan_ids, LogicalSelection::TYPE, 0);
    let cond = tidb_expr::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new("eq"),
        FieldType::new(FieldTypeCode::Tiny),
        vec![Expression::Column(col(2)), correlated(1)],
    );
    let mut plan = LogicalPlan::Selection(LogicalSelection::new(
        base,
        vec![Expression::ScalarFunction(cond)],
    ));
    plan.set_children(vec![f.inner()]);
    plan
}

#[test]
fn no_decorrelate_adds_a_limit_one_under_exists() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        disable_subquery_preprocessing: true,
        ..RewriterSessionFlags::default()
    });
    let outcome = er
        .handle_exist_subquery(
            f.outer(),
            correlated_inner(&f),
            false,
            HINT_FLAG_NO_DECORRELATE,
        )
        .expect("exists rewrite");
    let ScalarSubqueryOutcome::Applied(plan) = outcome else {
        panic!("expected an apply");
    };
    let LogicalPlan::Apply(apply) = &plan else {
        unreachable!()
    };
    assert!(apply.no_decorrelate);
    // Early exit: the inner side stops at the first row.
    assert!(has_limit(&plan.children()[1]));
}

#[test]
fn no_decorrelate_without_correlated_columns_warns_and_is_dropped() {
    let mut warnings = Vec::new();
    let no_decorrelate = is_no_decorrelate(
        ClauseCode::Where,
        RewriterSessionFlags::default(),
        &[],
        HINT_FLAG_NO_DECORRELATE,
        SubQueryCtx::Exists,
        &mut warnings,
    );
    assert!(!no_decorrelate);
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("inapplicable"));
}

#[test]
fn no_decorrelate_in_select_applies_only_in_the_field_list() {
    let flags = RewriterSessionFlags {
        enable_no_decorrelate_in_select: true,
        ..RewriterSessionFlags::default()
    };
    let cor = vec![CorrelatedColumn {
        column: col(1),
        data: None,
    }];
    let mut warnings = Vec::new();
    assert!(is_no_decorrelate(
        ClauseCode::FieldList,
        flags,
        &cor,
        0,
        SubQueryCtx::Scalar,
        &mut warnings
    ));
    assert!(!is_no_decorrelate(
        ClauseCode::Where,
        flags,
        &cor,
        0,
        SubQueryCtx::Scalar,
        &mut warnings
    ));
    // Only scalar and EXISTS qualify; an IN subquery does not.
    assert!(!is_no_decorrelate(
        ClauseCode::FieldList,
        flags,
        &cor,
        0,
        SubQueryCtx::In,
        &mut warnings
    ));
    assert!(warnings.is_empty());
}

// ***** popExistsSubPlan *****

#[test]
fn exists_strips_a_projection_and_a_sort() {
    let f = Fixture::new();
    let base = BaseLogicalPlan::new(&f.plan_ids, LogicalProjection::TYPE, 0);
    let mut proj = LogicalPlan::Projection(LogicalProjection::new(base, Vec::new()));
    proj.set_children(vec![f.inner()]);
    let stripped = pop_exists_sub_plan(proj, &f.plan_ids);
    assert!(matches!(stripped, LogicalPlan::DataSource(_)));
}

#[test]
fn exists_collapses_a_group_free_aggregation_to_a_one_row_dual() {
    let f = Fixture::new();
    let base = BaseLogicalPlan::new(&f.plan_ids, LogicalAggregation::TYPE, 0);
    let mut agg = LogicalPlan::Aggregation(LogicalAggregation::new(base, Vec::new(), Vec::new()));
    agg.set_children(vec![f.inner()]);
    let stripped = pop_exists_sub_plan(agg, &f.plan_ids);
    let LogicalPlan::TableDual(dual) = stripped else {
        panic!("expected a dual");
    };
    // `exists (select count(*) from t)` is always true.
    assert_eq!(dual.row_count, 1);
}

#[test]
fn exists_keeps_a_grouped_aggregations_child_only() {
    let f = Fixture::new();
    let base = BaseLogicalPlan::new(&f.plan_ids, LogicalAggregation::TYPE, 0);
    let mut agg = LogicalPlan::Aggregation(LogicalAggregation::new(
        base,
        Vec::new(),
        vec![Expression::Column(col(2))],
    ));
    agg.set_children(vec![f.inner()]);
    let stripped = pop_exists_sub_plan(agg, &f.plan_ids);
    assert!(matches!(stripped, LogicalPlan::DataSource(_)));
}

// ***** IN *****

#[test]
fn in_subquery_builds_a_semi_apply_when_the_join_rewrite_is_off() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let lexpr = Expression::Column(col(1));
    let plan = er
        .handle_in_subquery(f.outer(), &lexpr, f.inner(), false, false, 0, true, true)
        .expect("in rewrite");
    assert_eq!(join_type_of(&plan), LogicalJoinType::Semi);
    let LogicalPlan::Apply(apply) = &plan else {
        unreachable!()
    };
    // `a = b` spans the two children, so it is an EQUAL condition.
    assert_eq!(apply.join.equal_conditions.len(), 1);
}

#[test]
fn not_in_subquery_builds_an_anti_semi_apply_and_marks_the_in_operand() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        allow_in_subq_to_join_and_agg: true,
        ..RewriterSessionFlags::default()
    });
    let lexpr = Expression::Column(col(1));
    let plan = er
        .handle_in_subquery(f.outer(), &lexpr, f.inner(), true, false, 0, true, true)
        .expect("not in rewrite");
    // `NOT IN` can never take the inner-join rewrite.
    assert_eq!(join_type_of(&plan), LogicalJoinType::AntiSemi);
    let LogicalPlan::Apply(apply) = &plan else {
        unreachable!()
    };
    // Marked `InOperand`, so the `=` stays null-aware; that also routes it to
    // `other` rather than `equal` (Go `isEQCondFromIn`).
    let conds = &apply.join.other_conditions;
    assert_eq!(conds.len(), 1);
    let Expression::ScalarFunction(eq) = &conds[0] else {
        panic!("expected an eq");
    };
    let Expression::Column(rhs) = &eq.args[1] else {
        panic!("expected a column on the right");
    };
    assert!(rhs.in_operand);
}

#[test]
fn in_subquery_becomes_an_inner_join_over_a_distinct_when_allowed() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        allow_in_subq_to_join_and_agg: true,
        ..RewriterSessionFlags::default()
    });
    let lexpr = Expression::Column(col(1));
    let plan = er
        .handle_in_subquery(f.outer(), &lexpr, f.inner(), false, false, 0, true, true)
        .expect("in rewrite");
    assert_eq!(join_type_of(&plan), LogicalJoinType::Inner);
    assert!(matches!(plan, LogicalPlan::Join(_)));
    // The inner side is de-duplicated first, or the join would multiply rows.
    let LogicalPlan::Aggregation(agg) = &plan.children()[1] else {
        panic!("expected a distinct aggregation on the right");
    };
    assert_eq!(agg.group_by_items.len(), 1);
    assert_eq!(agg.agg_funcs.len(), 1);
    assert_eq!(agg.agg_funcs[0].name(), "firstrow");
}

#[test]
fn incompatible_collations_forbid_the_inner_join_rewrite() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        allow_in_subq_to_join_and_agg: true,
        ..RewriterSessionFlags::default()
    });
    let lexpr = Expression::Column(col(1));
    let plan = er
        .handle_in_subquery(f.outer(), &lexpr, f.inner(), false, false, 0, false, true)
        .expect("in rewrite");
    assert_eq!(join_type_of(&plan), LogicalJoinType::Semi);
}

#[test]
fn a_correlated_in_subquery_forbids_the_inner_join_rewrite() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        allow_in_subq_to_join_and_agg: true,
        ..RewriterSessionFlags::default()
    });
    let lexpr = Expression::Column(col(1));
    let plan = er
        .handle_in_subquery(
            f.outer(),
            &lexpr,
            correlated_inner(&f),
            false,
            false,
            0,
            true,
            true,
        )
        .expect("in rewrite");
    assert_eq!(join_type_of(&plan), LogicalJoinType::Semi);
}

#[test]
fn in_subquery_arity_mismatch_is_refused() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let lexpr = Expression::Column(col(1));
    let wide = f.source(vec![col(2), col(3)], vec![name("s", "b"), name("s", "c")]);
    let err = er
        .handle_in_subquery(f.outer(), &lexpr, wide, false, false, 0, true, true)
        .expect_err("arity mismatch");
    assert_eq!(err, RewriteError::OperandColumns(1));
}

// ***** scalar subqueries *****

#[test]
fn a_correlated_scalar_subquery_becomes_a_left_outer_apply_over_max_one_row() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let outcome = er
        .handle_scalar_subquery(f.outer(), correlated_inner(&f), 0)
        .expect("scalar rewrite");
    let ScalarSubqueryOutcome::Applied(plan) = outcome else {
        panic!("expected an apply");
    };
    // LEFT OUTER: an outer row with no inner match reads NULL, not nothing.
    assert_eq!(join_type_of(&plan), LogicalJoinType::LeftOuter);
    assert!(matches!(plan.children()[1], LogicalPlan::MaxOneRow(_)));
    // The apply's schema is outer ++ inner, and the pushed expression is the
    // inner column.
    let schema = plan.schema().expect("schema");
    assert_eq!(schema.len(), 2);
    assert_eq!(er.ctx_stack_len(), 1);
    let Expression::Column(pushed) = &er.ctx_stack[0] else {
        panic!("expected a column");
    };
    assert_eq!(pushed.unique_id, schema.columns[1].unique_id);
}

#[test]
fn a_left_outer_apply_clears_the_inner_not_null_flags() {
    let f = Fixture::new();
    let er = f.rewriter(RewriterSessionFlags::default());
    let mut not_null = long_long();
    not_null.set_flags(FieldTypeFlags::NOT_NULL);
    let inner = f.source(vec![Column::new(2, not_null)], vec![name("s", "b")]);
    let plan = er
        .build_apply_with_join_type(f.outer(), inner, LogicalJoinType::LeftOuter, false)
        .expect("apply");
    let schema = plan.schema().expect("schema");
    assert!(!schema.columns[1]
        .ret_type
        .as_ref()
        .expect("type")
        .has_flag(FieldTypeFlags::NOT_NULL));
}

#[test]
fn a_row_scalar_subquery_is_read_back_as_a_row_function() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags {
        disable_subquery_preprocessing: true,
        ..RewriterSessionFlags::default()
    });
    let wide = f.source(vec![col(2), col(3)], vec![name("s", "b"), name("s", "c")]);
    let outcome = er
        .handle_scalar_subquery(f.outer(), wide, 0)
        .expect("scalar rewrite");
    assert!(matches!(outcome, ScalarSubqueryOutcome::Applied(_)));
    let Expression::ScalarFunction(row) = &er.ctx_stack[0] else {
        panic!("expected a row function");
    };
    assert_eq!(row.func_name.lowercase(), "row");
    assert_eq!(row.args.len(), 2);
}

// ***** quantified comparisons *****

#[test]
fn eq_any_is_rewritten_like_in() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let lexpr = Expression::Column(col(1));
    er.ctx_stack_append(lexpr.clone(), name("t", "a"));
    let plan = er
        .handle_compare_subquery(f.outer(), &lexpr, f.inner(), CompareOp::Eq, false, 0)
        .expect("= any");
    // `= any` sets asScalar, so the semi join carries an aux column.
    assert_eq!(join_type_of(&plan), LogicalJoinType::LeftOuterSemi);
}

#[test]
fn ne_all_is_rewritten_like_not_in() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let lexpr = Expression::Column(col(1));
    er.ctx_stack_append(lexpr.clone(), name("t", "a"));
    let plan = er
        .handle_compare_subquery(f.outer(), &lexpr, f.inner(), CompareOp::Ne, true, 0)
        .expect("!= all");
    assert_eq!(join_type_of(&plan), LogicalJoinType::AntiLeftOuterSemi);
}

#[test]
fn lt_any_compares_against_max_and_gt_any_against_min() {
    for (op, all, expected) in [
        (CompareOp::Lt, false, "max"),
        (CompareOp::Lt, true, "min"),
        (CompareOp::Gt, false, "min"),
        (CompareOp::Gt, true, "max"),
    ] {
        let f = Fixture::new();
        let mut er = f.rewriter(RewriterSessionFlags::default());
        let lexpr = Expression::Column(col(1));
        er.ctx_stack_append(lexpr.clone(), name("t", "a"));
        let plan = er
            .handle_compare_subquery(f.outer(), &lexpr, f.inner(), op, all, 0)
            .expect("quantified compare");
        // No value needed, so the condition rides a semi apply's ON clause.
        assert_eq!(join_type_of(&plan), LogicalJoinType::Semi);
        let LogicalPlan::Aggregation(agg) = &plan.children()[1] else {
            panic!("expected the quantifier aggregation on the right");
        };
        assert_eq!(agg.agg_funcs[0].name(), expected);
        // Plus `sum(inner is null)` and `count(1)`, which carry the
        // three-valued answer.
        assert_eq!(agg.agg_funcs.len(), 3);
        assert_eq!(agg.agg_funcs[1].name(), "sum");
        assert_eq!(agg.agg_funcs[2].name(), "count");
    }
}

#[test]
fn ne_any_and_eq_all_count_distinct_inner_values() {
    for (op, all) in [(CompareOp::Ne, false), (CompareOp::Eq, true)] {
        let f = Fixture::new();
        let mut er = f.rewriter(RewriterSessionFlags::default());
        let lexpr = Expression::Column(col(1));
        er.ctx_stack_append(lexpr.clone(), name("t", "a"));
        let plan = er
            .handle_compare_subquery(f.outer(), &lexpr, f.inner(), op, all, 0)
            .expect("quantified compare");
        let LogicalPlan::Aggregation(agg) = &plan.children()[1] else {
            panic!("expected the quantifier aggregation on the right");
        };
        assert_eq!(agg.agg_funcs[0].name(), "max");
        assert_eq!(agg.agg_funcs[1].name(), "count");
        assert!(agg.agg_funcs[1].has_distinct);
    }
}

#[test]
fn null_eq_quantifiers_are_refused() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let lexpr = Expression::Column(col(1));
    er.ctx_stack_append(lexpr.clone(), name("t", "a"));
    let err = er
        .handle_compare_subquery(f.outer(), &lexpr, f.inner(), CompareOp::NullEq, true, 0)
        .expect_err("<=> all");
    assert_eq!(err, RewriteError::NullEqQuantifierUnsupported);
}

#[test]
fn an_ordered_quantifier_refuses_a_row_on_the_left() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let lexpr = er
        .new_function(
            "row",
            long_long(),
            vec![Expression::Column(col(1)), Expression::Column(col(4))],
        )
        .expect("row");
    er.ctx_stack_append(lexpr.clone(), FieldName::default());
    let err = er
        .handle_compare_subquery(f.outer(), &lexpr, f.inner(), CompareOp::Lt, false, 0)
        .expect_err("row < any");
    assert_eq!(err, RewriteError::OperandColumns(1));
}

#[test]
fn a_scalar_quantifier_projects_the_condition_as_an_extra_column() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    er.as_scalar = true;
    let lexpr = Expression::Column(col(1));
    er.ctx_stack_append(lexpr.clone(), name("t", "a"));
    let plan = er
        .handle_compare_subquery(f.outer(), &lexpr, f.inner(), CompareOp::Lt, false, 0)
        .expect("< any as a value");
    // A value is needed, so the apply is INNER and a projection appends it.
    let LogicalPlan::Projection(proj) = &plan else {
        panic!("expected a projection, got {}", plan.tp());
    };
    assert_eq!(proj.exprs.len(), 2);
    assert_eq!(join_type_of(&plan.children()[0]), LogicalJoinType::Inner);
    assert_eq!(plan.schema().expect("schema").len(), 2);
}

// ***** constructBinaryOpFunction *****

#[test]
fn row_equality_becomes_a_conjunction() {
    let f = Fixture::new();
    let er = f.rewriter(RewriterSessionFlags::default());
    let l = er
        .new_function(
            "row",
            long_long(),
            vec![Expression::Column(col(1)), Expression::Column(col(2))],
        )
        .expect("row");
    let r = er
        .new_function(
            "row",
            long_long(),
            vec![Expression::Column(col(3)), Expression::Column(col(4))],
        )
        .expect("row");
    let cond = er
        .construct_binary_op_function(&l, &r, "eq")
        .expect("row eq");
    let items = split_cnf_items(&cond);
    assert_eq!(items.len(), 2);
    for item in &items {
        let Expression::ScalarFunction(func) = item else {
            panic!("expected a function");
        };
        assert_eq!(func.func_name.lowercase(), "eq");
    }
}

#[test]
fn row_ordering_becomes_a_lexicographic_disjunction() {
    let f = Fixture::new();
    let er = f.rewriter(RewriterSessionFlags::default());
    let l = er
        .new_function(
            "row",
            long_long(),
            vec![Expression::Column(col(1)), Expression::Column(col(2))],
        )
        .expect("row");
    let r = er
        .new_function(
            "row",
            long_long(),
            vec![Expression::Column(col(3)), Expression::Column(col(4))],
        )
        .expect("row");
    let cond = er
        .construct_binary_op_function(&l, &r, "ge")
        .expect("row ge");
    // `(a0,a1) >= (b0,b1)` is `a0 > b0 or (a0 = b0 and a1 >= b1)`: the PREFIX
    // comparison is strict even though the operator is not.
    let Expression::ScalarFunction(or) = &cond else {
        panic!("expected an or");
    };
    assert_eq!(or.func_name.lowercase(), "or");
    let Expression::ScalarFunction(first) = &or.args[0] else {
        panic!("expected a function");
    };
    assert_eq!(first.func_name.lowercase(), "gt");
}

#[test]
fn mismatched_row_widths_are_refused() {
    let f = Fixture::new();
    let er = f.rewriter(RewriterSessionFlags::default());
    let l = er
        .new_function(
            "row",
            long_long(),
            vec![Expression::Column(col(1)), Expression::Column(col(2))],
        )
        .expect("row");
    let r = Expression::Column(col(3));
    let err = er
        .construct_binary_op_function(&l, &r, "eq")
        .expect_err("width mismatch");
    assert_eq!(err, RewriteError::OperandColumns(2));
}

// ***** correlated column resolution *****

#[test]
fn a_name_the_inner_block_resolves_is_a_plain_column() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    er.schema = Some(Schema::new(vec![col(2)]));
    er.names = vec![name("s", "b")];
    er.plan_ctx.outer_schemas = vec![Schema::new(vec![col(1)])];
    er.plan_ctx.outer_names = vec![vec![name("t", "a")]];
    er.to_column(&f.inner(), &qualified("s", "b"))
        .expect("resolve");
    let Expression::Column(column) = &er.ctx_stack[0] else {
        panic!("expected a plain column");
    };
    assert_eq!(column.unique_id, 2);
}

#[test]
fn a_name_only_an_outer_block_resolves_becomes_a_correlated_column() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    er.schema = Some(Schema::new(vec![col(2)]));
    er.names = vec![name("s", "b")];
    er.plan_ctx.outer_schemas = vec![Schema::new(vec![col(1)])];
    er.plan_ctx.outer_names = vec![vec![name("t", "a")]];
    er.to_column(&f.inner(), &qualified("t", "a"))
        .expect("resolve");
    let Expression::CorrelatedColumn(cor) = &er.ctx_stack[0] else {
        panic!("expected a correlated column");
    };
    assert_eq!(cor.column.unique_id, 1);
}

#[test]
fn the_innermost_enclosing_block_wins() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    er.schema = Some(Schema::new(vec![col(9)]));
    er.names = vec![name("u", "z")];
    // Two enclosing blocks both name `t.a`; Go walks outerSchemas backwards,
    // so the innermost one (last) resolves it.
    er.plan_ctx.outer_schemas = vec![Schema::new(vec![col(1)]), Schema::new(vec![col(5)])];
    er.plan_ctx.outer_names = vec![vec![name("t", "a")], vec![name("t", "a")]];
    er.to_column(&f.inner(), &qualified("t", "a"))
        .expect("resolve");
    let Expression::CorrelatedColumn(cor) = &er.ctx_stack[0] else {
        panic!("expected a correlated column");
    };
    assert_eq!(cor.column.unique_id, 5);
}

#[test]
fn an_unresolvable_name_names_its_clause() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    er.plan_ctx.cur_clause = ClauseCode::Where;
    er.schema = Some(Schema::new(vec![col(2)]));
    er.names = vec![name("s", "b")];
    let err = er
        .to_column(&f.inner(), &qualified("t", "a"))
        .expect_err("unresolvable");
    assert_eq!(
        err,
        RewriteError::UnknownColumn("a".to_owned(), "where clause")
    );
}

#[test]
fn a_hidden_column_is_not_referenceable() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let mut hidden = col(2);
    hidden.is_hidden = true;
    er.schema = Some(Schema::new(vec![hidden]));
    er.names = vec![name("s", "b")];
    let err = er
        .to_column(&f.inner(), &qualified("s", "b"))
        .expect_err("hidden");
    assert!(matches!(err, RewriteError::UnknownColumn(_, _)));
}

// ***** correlated-column extraction *****

#[test]
fn correlated_columns_are_collected_from_the_whole_subtree() {
    let f = Fixture::new();
    let inner = correlated_inner(&f);
    let cor = extract_correlated_cols_4_logical_plan(&inner);
    assert_eq!(cor.len(), 1);
    assert_eq!(cor[0].column.unique_id, 1);
}

#[test]
fn only_the_columns_the_outer_schema_supplies_are_resolved() {
    let f = Fixture::new();
    let inner = correlated_inner(&f);
    // The outer plan produces column 1, so the reference is correlated HERE.
    let resolved = extract_cor_columns_by_schema_4_logical_plan(&inner, &Schema::new(vec![col(1)]));
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].column.index, 0);
    // A schema that does not produce it means the reference reaches further out.
    let unresolved =
        extract_cor_columns_by_schema_4_logical_plan(&inner, &Schema::new(vec![col(7)]));
    assert!(unresolved.is_empty());
}

#[test]
fn duplicate_references_to_one_outer_column_resolve_once() {
    let cor = vec![
        CorrelatedColumn {
            column: col(1),
            data: None,
        },
        CorrelatedColumn {
            column: col(1),
            data: None,
        },
    ];
    let resolved = extract_cor_columns_by_schema(&cor, &Schema::new(vec![col(1)]));
    assert_eq!(resolved.len(), 1);
}

// ***** decorrelation of the ON clause *****

#[test]
fn a_semi_joins_on_clause_is_decorrelated_against_the_outer_schema() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    let cond = er
        .new_function(
            "eq",
            FieldType::new(FieldTypeCode::Tiny),
            vec![Expression::Column(col(2)), correlated(1)],
        )
        .expect("eq");
    let plan = er
        .build_semi_join(f.outer(), f.inner(), &[cond], false, false, false)
        .expect("semi join");
    let LogicalPlan::Join(join) = &plan else {
        unreachable!()
    };
    // The outer side supplies column 1, so the reference becomes a plain
    // column and the predicate becomes an EQUAL join condition.
    assert_eq!(join.equal_conditions.len(), 1);
    assert!(join.other_conditions.is_empty());
}

#[test]
fn a_reference_the_outer_schema_does_not_supply_stays_correlated() {
    let cond = Expression::ScalarFunction(tidb_expr::scalar_function::ScalarFunction::new(
        tidb_ast::CiString::new("eq"),
        FieldType::new(FieldTypeCode::Tiny),
        vec![Expression::Column(col(2)), correlated(99)],
    ));
    let decorrelated = decorrelate_expr(&cond, &Schema::new(vec![col(1)]));
    let Expression::ScalarFunction(func) = &decorrelated else {
        panic!("expected a function");
    };
    assert!(matches!(func.args[1], Expression::CorrelatedColumn(_)));
}

// ***** misc predicates *****

#[test]
fn a_cte_consumer_anywhere_forces_the_apply_path() {
    let f = Fixture::new();
    assert!(!has_cte_consumer_in_sub_plan(&f.inner()));
}

#[test]
fn expr_not_null_reads_the_flag_and_the_constant_value() {
    assert!(!expr_not_null(&Expression::Column(col(1))));
    let mut not_null = long_long();
    not_null.set_flags(FieldTypeFlags::NOT_NULL);
    assert!(expr_not_null(&Expression::Column(Column::new(1, not_null))));
    assert!(expr_not_null(&one()));
    assert!(!expr_not_null(&null()));
}

#[test]
fn redundant_remap_applies_only_to_where_and_having_outside_dml() {
    let mut redundant = name("t", "a");
    redundant.redundant = true;
    assert!(should_remap_redundant_base_column(
        false,
        ClauseCode::Where,
        &redundant
    ));
    assert!(should_remap_redundant_base_column(
        false,
        ClauseCode::Having,
        &redundant
    ));
    assert!(!should_remap_redundant_base_column(
        false,
        ClauseCode::FieldList,
        &redundant
    ));
    assert!(!should_remap_redundant_base_column(
        true,
        ClauseCode::Where,
        &redundant
    ));
    assert!(!should_remap_redundant_base_column(
        false,
        ClauseCode::Where,
        &name("t", "a")
    ));
}

#[test]
fn the_ctx_stack_pops_names_with_expressions() {
    let f = Fixture::new();
    let mut er = f.rewriter(RewriterSessionFlags::default());
    er.ctx_stack_append(Expression::Column(col(1)), name("t", "a"));
    er.ctx_stack_append(Expression::Column(col(2)), name("s", "b"));
    assert_eq!(er.ctx_stack_len(), 2);
    er.ctx_stack_pop(1);
    assert_eq!(er.ctx_stack_len(), 1);
    assert_eq!(er.ctx_name_stk.len(), 1);
}
