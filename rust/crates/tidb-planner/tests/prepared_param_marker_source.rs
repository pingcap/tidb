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

//! Typed parameter-marker admission tests for configured prepared point reads.

use tidb_ast::{Expr, QueryStmt, SelectField, Stmt};
use tidb_planner::{
    condition_binding::{bind_residual, ConditionBindingError},
    configured_catalog::ConfiguredCatalog,
    join_condition::JoinSchema,
    read_only_scan::{
        lower_prepared_point_read, ConfiguredColumn, ConfiguredPreparedPointReadTemplate,
        ConfiguredTable, PreparedBindError, PreparedPlanError, ReadOnlyScanError,
        UnsupportedReadOnlyPredicate,
    },
    residual_condition::{classify_residual, ResidualPredicate, ResidualUnsupported},
};

fn catalog() -> ConfiguredCatalog {
    ConfiguredCatalog::new([ConfiguredTable::new(
        "test",
        "accounts",
        42,
        [
            ConfiguredColumn::clustered_primary_key("id", 7),
            ConfiguredColumn::stored_not_null("balance", 9),
        ],
    )])
    .expect("test catalog is valid")
}

fn select(sql: &str) -> tidb_ast::SelectStmt {
    let Stmt::Query(query) = tidb_parser::parse(sql).expect("SQL must parse") else {
        panic!("expected query statement");
    };
    let QueryStmt::Select(select) = *query else {
        panic!("expected SELECT");
    };
    *select
}

fn template(sql: &str) -> ConfiguredPreparedPointReadTemplate {
    lower_prepared_point_read(&select(sql), &catalog()).expect("prepared point read must lower")
}

#[test]
fn parser_numbers_parameter_markers_left_to_right_and_per_statement() {
    let statement = select("SELECT ?, ? FROM accounts WHERE id = ?");
    let [SelectField::Expr {
        expr: Expr::ParamMarker { position: 0 },
        ..
    }, SelectField::Expr {
        expr: Expr::ParamMarker { position: 1 },
        ..
    }] = statement.fields.as_slice()
    else {
        panic!("projection markers must retain source order");
    };
    let Some(Expr::Binary(_, _, right)) = statement.where_clause.as_ref() else {
        panic!("WHERE must retain the third marker");
    };
    assert!(matches!(right.as_ref(), Expr::ParamMarker { position: 2 }));

    let statements = tidb_parser::parse_multi("SELECT ?; SELECT ?").expect("statements parse");
    for statement in statements {
        let Stmt::Query(query) = statement else {
            panic!("expected query statement");
        };
        let QueryStmt::Select(select) = *query else {
            panic!("expected SELECT");
        };
        assert!(matches!(
            select.fields.as_slice(),
            [SelectField::Expr {
                expr: Expr::ParamMarker { position: 0 },
                ..
            }]
        ));
    }
}

#[test]
fn prepared_primary_key_template_binds_signed_bigint_without_sql_interpolation() {
    let plan = template("SELECT a.balance AS amount FROM test.accounts AS a WHERE a.id = ?")
        .bind(&[-7])
        .expect("typed parameter must lower through the ordinary scan authority");

    assert_eq!(plan.table_id(), 42);
    assert_eq!(plan.projected_columns()[0].output_name(), "amount");
    assert_eq!(plan.handle_ranges().len(), 1);
    assert_eq!(plan.handle_ranges()[0].start(), -7);
    assert_eq!(plan.handle_ranges()[0].end(), -7);
    assert!(plan.selection().is_none());

    let reversed = template("SELECT balance FROM accounts WHERE ? = id")
        .bind(&[9])
        .expect("reversed equality remains a primary-key point read");
    assert_eq!(reversed.handle_ranges()[0].start(), 9);
    assert_eq!(reversed.handle_ranges()[0].end(), 9);
}

#[test]
fn marker_admission_is_exact_and_text_query_lowering_remains_fail_closed() {
    let parsed = select("SELECT balance FROM accounts WHERE id = ?");
    assert_eq!(
        tidb_planner::read_only_scan::ReadOnlyScanPlan::lower(
            "SELECT balance FROM accounts WHERE id = ?",
            catalog().tables().first().expect("catalog has one table"),
        ),
        Err(ReadOnlyScanError::UnsupportedPredicate(
            UnsupportedReadOnlyPredicate::Operand
        ))
    );

    assert_eq!(
        lower_prepared_point_read(&parsed, &catalog())
            .expect("template lowers")
            .bind(&[]),
        Err(PreparedBindError::ParameterCount(0))
    );
    assert_eq!(
        template("SELECT balance FROM accounts WHERE id = ?").bind(&[1, 2]),
        Err(PreparedBindError::ParameterCount(2))
    );
    // A non-primary-key comparison is rejected: the prepared read filters only
    // on the clustered handle, alone or mixed with a valid handle comparison.
    assert_eq!(
        lower_prepared_point_read(
            &select("SELECT balance FROM accounts WHERE balance = ?"),
            &catalog(),
        ),
        Err(PreparedPlanError::PrimaryKeyComparison)
    );
    assert_eq!(
        lower_prepared_point_read(
            &select("SELECT balance FROM accounts WHERE id = ? AND balance = ?"),
            &catalog(),
        ),
        Err(PreparedPlanError::PrimaryKeyComparison)
    );
}

#[test]
fn prepared_range_read_binds_one_marker_per_handle_bound() {
    use tidb_planner::signed_bigint_ranger::SignedBigIntRange;

    // `id >= ? AND id <= ?` binds two handles into one closed range.
    let plan = template("SELECT balance FROM accounts WHERE id >= ? AND id <= ?")
        .bind(&[5, 10])
        .expect("range binds through the scan authority");
    assert_eq!(
        plan.handle_ranges(),
        [SignedBigIntRange::new(5, 10).unwrap()]
    );
    assert!(plan.selection().is_none());

    // `id BETWEEN ? AND ?` desugars to the same two-marker range.
    let between = template("SELECT balance FROM accounts WHERE id BETWEEN ? AND ?");
    assert_eq!(between.parameter_count(), 2);
    assert_eq!(
        between.bind(&[5, 10]).unwrap().handle_ranges(),
        [SignedBigIntRange::new(5, 10).unwrap()]
    );

    // A single strict inequality is now a valid half-open range read.
    let half_open = template("SELECT balance FROM accounts WHERE id > ?");
    assert_eq!(half_open.parameter_count(), 1);
    assert_eq!(
        half_open.bind(&[5]).unwrap().handle_ranges(),
        [SignedBigIntRange::new(6, i64::MAX).unwrap()]
    );

    // The point read stays a special case: one equality, one parameter.
    let point = template("SELECT balance FROM accounts WHERE id = ?");
    assert_eq!(point.parameter_count(), 1);
    let range = point.bind(&[7]).unwrap();
    assert_eq!(range.handle_ranges()[0].start(), 7);
    assert_eq!(range.handle_ranges()[0].end(), 7);
}

#[test]
fn unbound_markers_are_rejected_by_generic_residual_planning() {
    let marker = Expr::ParamMarker { position: 3 };
    assert_eq!(
        bind_residual(&marker, &JoinSchema::new([], [])),
        Err(ConditionBindingError::UnboundParameterMarker { position: 3 })
    );
    assert_eq!(
        classify_residual(&marker),
        ResidualPredicate::Unsupported(ResidualUnsupported::AstVariant {
            category: "param_marker"
        })
    );
}
