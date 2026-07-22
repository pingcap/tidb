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

//! Admission and binding tests for the configured prepared write shapes.
//!
//! The obligated originals are the repeated positional VALUES insert in
//! `pkg/executor/test/seqtest/prepared_test.go:328:TestPreparedInsert` and the
//! repeated point arithmetic update in `:405:TestPreparedUpdate`. Both use
//! `int PRIMARY KEY` tables and assert persisted rows rather than affected
//! rows, so this file owns exactly the statement admission and marker binding
//! those tests exercise; persistence belongs to the dependent live proof and
//! affected-row accounting belongs to `tidb-exec`.

use tidb_ast::{DmlStmt, Expr, Stmt};
use tidb_planner::{
    configured_catalog::ConfiguredCatalog,
    prepared_dml::{
        lower_prepared_write, ConfiguredAssignment, ConfiguredPreparedWrite,
        ConfiguredPreparedWriteTemplate, PreparedBindValue, PreparedWriteBindError,
        PreparedWritePlanError, UnsupportedPreparedWrite, MAX_PREPARED_INSERT_ROWS,
    },
    read_only_scan::{ConfiguredColumn, ConfiguredTable},
};

/// Wraps signed integers as the planner's bind currency (these cases are int-only).
fn ints(values: &[i64]) -> Vec<PreparedBindValue> {
    values.iter().copied().map(PreparedBindValue::Int).collect()
}

/// The expected `(column index, value)` pairs for an int-only bound row.
fn int_pairs(pairs: &[(usize, i64)]) -> Vec<(usize, PreparedBindValue)> {
    pairs
        .iter()
        .map(|(index, value)| (*index, PreparedBindValue::Int(*value)))
        .collect()
}

const TABLE_ID: i64 = 114;
const ID_COLUMN: i64 = 1;
const BALANCE_COLUMN: i64 = 2;

fn catalog() -> ConfiguredCatalog {
    ConfiguredCatalog::new([ConfiguredTable::new(
        "campaign28",
        "accounts",
        TABLE_ID,
        [
            ConfiguredColumn::clustered_primary_key("id", ID_COLUMN),
            ConfiguredColumn::stored_not_null("balance", BALANCE_COLUMN),
        ],
    )])
    .expect("configured catalog must validate")
}

fn parse(sql: &str) -> Stmt {
    tidb_parser::parse(sql).expect("SQL must parse")
}

fn template(sql: &str) -> ConfiguredPreparedWriteTemplate {
    lower_prepared_write(&parse(sql), &catalog()).expect("prepared write must lower")
}

fn rejection(sql: &str) -> PreparedWritePlanError {
    lower_prepared_write(&parse(sql), &catalog()).expect_err("prepared write must be rejected")
}

fn unsupported(sql: &str) -> UnsupportedPreparedWrite {
    match rejection(sql) {
        PreparedWritePlanError::Unsupported(feature) => feature,
        other => panic!("expected an unsupported feature, found {other:?}"),
    }
}

// -----------------------------------------------------------------------------
// INSERT admission and binding
// -----------------------------------------------------------------------------

#[test]
fn one_row_insert_binds_positional_markers_to_configured_columns() {
    let template = template("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)");
    assert_eq!(template.parameter_count(), 2);
    assert_eq!(template.table().table_id(), TABLE_ID);

    let ConfiguredPreparedWrite::InsertRows { table, rows } =
        template.bind(&ints(&[10, 100])).expect("bind must succeed")
    else {
        panic!("expected an INSERT command");
    };
    assert_eq!(table.table_id(), TABLE_ID);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values(), int_pairs(&[(0, 10), (1, 100)]).as_slice());
}

#[test]
fn two_row_insert_binds_every_marker_in_source_order() {
    let template = template("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?), (?, ?)");
    assert_eq!(template.parameter_count(), 4);

    let ConfiguredPreparedWrite::InsertRows { rows, .. } = template
        .bind(&ints(&[10, 100, 11, 110]))
        .expect("bind must succeed")
    else {
        panic!("expected an INSERT command");
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].values(), int_pairs(&[(0, 10), (1, 100)]).as_slice());
    assert_eq!(rows[1].values(), int_pairs(&[(0, 11), (1, 110)]).as_slice());
}

#[test]
fn insert_column_order_follows_the_written_list_not_the_catalog() {
    let template = template("INSERT INTO campaign28.accounts (balance, id) VALUES (?, ?)");
    let ConfiguredPreparedWrite::InsertRows { rows, .. } =
        template.bind(&ints(&[100, 10])).expect("bind must succeed")
    else {
        panic!("expected an INSERT command");
    };
    // Configured index 1 is `balance` and 0 is `id`: the first marker binds the
    // first written column, exactly as MySQL binds by column list.
    assert_eq!(rows[0].values(), int_pairs(&[(1, 100), (0, 10)]).as_slice());
}

#[test]
fn unqualified_and_case_insensitive_names_resolve_to_the_configured_table() {
    let template = template("insert into ACCOUNTS (ID, Balance) values (?, ?)");
    assert_eq!(template.table().table_id(), TABLE_ID);
    assert_eq!(template.parameter_count(), 2);
}

#[test]
fn insert_requires_every_configured_column_exactly_once() {
    assert_eq!(
        rejection("INSERT INTO campaign28.accounts (id) VALUES (?)"),
        PreparedWritePlanError::InsertColumnCoverage {
            configured: 2,
            named: 1,
        }
    );
    assert_eq!(
        rejection("INSERT INTO campaign28.accounts (id, id) VALUES (?, ?)"),
        PreparedWritePlanError::DuplicateInsertColumn("id".to_owned())
    );
    assert_eq!(
        unsupported("INSERT INTO campaign28.accounts VALUES (?, ?)"),
        UnsupportedPreparedWrite::MissingInsertColumns
    );
    assert_eq!(
        rejection("INSERT INTO campaign28.accounts (id, missing) VALUES (?, ?)"),
        PreparedWritePlanError::UnknownColumn("missing".to_owned())
    );
}

#[test]
fn every_inserted_value_must_be_its_own_left_to_right_marker() {
    assert_eq!(
        rejection("INSERT INTO campaign28.accounts (id, balance) VALUES (?, 100)"),
        PreparedWritePlanError::MarkerPosition {
            expected: 1,
            found: None,
        }
    );
    assert_eq!(
        rejection("INSERT INTO campaign28.accounts (id, balance) VALUES (10, ?)"),
        PreparedWritePlanError::MarkerPosition {
            expected: 0,
            found: None,
        }
    );
}

#[test]
fn insert_row_arity_must_match_the_column_list() {
    assert_eq!(
        rejection("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?), (?)"),
        PreparedWritePlanError::InsertRowArity {
            row: 1,
            values: 1,
            columns: 2,
        }
    );
}

#[test]
fn insert_rows_stay_inside_the_checked_process_limit() {
    let row_list = std::iter::repeat_n("(?, ?)", MAX_PREPARED_INSERT_ROWS)
        .collect::<Vec<_>>()
        .join(", ");
    let admitted = template(&format!(
        "INSERT INTO campaign28.accounts (id, balance) VALUES {row_list}"
    ));
    assert_eq!(
        admitted.parameter_count(),
        MAX_PREPARED_INSERT_ROWS * 2,
        "the limit itself must be admitted"
    );

    let over_limit = std::iter::repeat_n("(?, ?)", MAX_PREPARED_INSERT_ROWS + 1)
        .collect::<Vec<_>>()
        .join(", ");
    assert_eq!(
        rejection(&format!(
            "INSERT INTO campaign28.accounts (id, balance) VALUES {over_limit}"
        )),
        PreparedWritePlanError::InsertRowCount {
            rows: MAX_PREPARED_INSERT_ROWS + 1,
            limit: MAX_PREPARED_INSERT_ROWS,
        }
    );
}

#[test]
fn unsupported_insert_forms_are_rejected_before_a_handle_exists() {
    assert_eq!(
        unsupported("REPLACE INTO campaign28.accounts (id, balance) VALUES (?, ?)"),
        UnsupportedPreparedWrite::Replace
    );
    assert_eq!(
        unsupported("INSERT IGNORE INTO campaign28.accounts (id, balance) VALUES (?, ?)"),
        UnsupportedPreparedWrite::Ignore
    );
    assert_eq!(
        unsupported(
            "INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?) \
             ON DUPLICATE KEY UPDATE balance = ?"
        ),
        UnsupportedPreparedWrite::OnDuplicateKey
    );
    assert_eq!(
        unsupported("INSERT INTO campaign28.accounts SET id = ?, balance = ?"),
        UnsupportedPreparedWrite::SetSyntax
    );
    assert_eq!(
        unsupported(
            "INSERT INTO campaign28.accounts (id, balance) \
             SELECT id, balance FROM campaign28.accounts"
        ),
        UnsupportedPreparedWrite::InsertSelect
    );
    assert_eq!(
        unsupported("INSERT INTO campaign28.accounts PARTITION (p0) (id, balance) VALUES (?, ?)"),
        UnsupportedPreparedWrite::Partition
    );
}

// -----------------------------------------------------------------------------
// UPDATE admission and binding
// -----------------------------------------------------------------------------

#[test]
fn direct_point_update_binds_value_then_handle() {
    let template = template("UPDATE campaign28.accounts SET balance = ? WHERE id = ?");
    assert_eq!(template.parameter_count(), 2);

    let ConfiguredPreparedWrite::UpdatePoint {
        table,
        handle,
        column_index,
        assignment,
    } = template.bind(&ints(&[150, 10])).expect("bind must succeed")
    else {
        panic!("expected an UPDATE command");
    };
    assert_eq!(table.table_id(), TABLE_ID);
    assert_eq!(handle, 10);
    assert_eq!(column_index, 1);
    assert_eq!(assignment, ConfiguredAssignment::Set(150));
}

#[test]
fn arithmetic_point_update_preserves_the_add_shape() {
    let template = template("UPDATE campaign28.accounts SET balance = balance + ? WHERE id = ?");
    let ConfiguredPreparedWrite::UpdatePoint {
        handle, assignment, ..
    } = template.bind(&ints(&[7, 10])).expect("bind must succeed")
    else {
        panic!("expected an UPDATE command");
    };
    assert_eq!(handle, 10);
    assert_eq!(assignment, ConfiguredAssignment::Add(7));
}

#[test]
fn the_handle_equality_may_write_its_marker_on_either_side() {
    let template = template("UPDATE campaign28.accounts SET balance = ? WHERE ? = id");
    let ConfiguredPreparedWrite::UpdatePoint { handle, .. } =
        template.bind(&ints(&[150, 10])).expect("bind must succeed")
    else {
        panic!("expected an UPDATE command");
    };
    assert_eq!(handle, 10);
}

#[test]
fn update_cannot_move_a_row_or_assign_an_unrelated_column() {
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET id = ? WHERE id = ?"),
        PreparedWritePlanError::UpdateClusteredHandle("id".to_owned())
    );
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET balance = id + ? WHERE id = ?"),
        PreparedWritePlanError::UpdateAssignmentShape
    );
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET balance = balance - ? WHERE id = ?"),
        PreparedWritePlanError::UpdateAssignmentShape
    );
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET balance = 5 WHERE id = ?"),
        PreparedWritePlanError::UpdateAssignmentShape
    );
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET balance = ?, balance = ? WHERE id = ?"),
        PreparedWritePlanError::UpdateAssignmentCount(2)
    );
}

#[test]
fn update_requires_exactly_one_clustered_handle_equality() {
    assert_eq!(
        unsupported("UPDATE campaign28.accounts SET balance = ?"),
        UnsupportedPreparedWrite::MissingWhere
    );
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET balance = ? WHERE balance = ?"),
        PreparedWritePlanError::PointHandlePredicate
    );
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET balance = ? WHERE id > ?"),
        PreparedWritePlanError::PointHandlePredicate
    );
    assert_eq!(
        rejection("UPDATE campaign28.accounts SET balance = ? WHERE id = 10"),
        PreparedWritePlanError::MarkerPosition {
            expected: 1,
            found: None,
        }
    );
}

#[test]
fn unsupported_update_tails_and_targets_are_rejected() {
    assert_eq!(
        unsupported("UPDATE IGNORE campaign28.accounts SET balance = ? WHERE id = ?"),
        UnsupportedPreparedWrite::Ignore
    );
    assert_eq!(
        unsupported("UPDATE campaign28.accounts SET balance = ? WHERE id = ? ORDER BY id"),
        UnsupportedPreparedWrite::OrderBy
    );
    assert_eq!(
        unsupported("UPDATE campaign28.accounts SET balance = ? WHERE id = ? LIMIT 1"),
        UnsupportedPreparedWrite::Limit
    );
    assert_eq!(
        unsupported("UPDATE campaign28.accounts AS a SET balance = ? WHERE id = ?"),
        UnsupportedPreparedWrite::TableAlias
    );
    assert_eq!(
        unsupported(
            "UPDATE campaign28.accounts, campaign28.accounts AS b SET balance = ? WHERE id = ?"
        ),
        UnsupportedPreparedWrite::MultiTableUpdate
    );
}

// -----------------------------------------------------------------------------
// DELETE admission and binding
// -----------------------------------------------------------------------------

#[test]
fn point_delete_binds_the_clustered_handle() {
    let template = template("DELETE FROM campaign28.accounts WHERE id = ?");
    assert_eq!(template.parameter_count(), 1);

    let ConfiguredPreparedWrite::DeletePoint { table, handle } =
        template.bind(&ints(&[10])).expect("bind must succeed")
    else {
        panic!("expected a DELETE command");
    };
    assert_eq!(table.table_id(), TABLE_ID);
    assert_eq!(handle, 10);
}

#[test]
fn delete_handle_equality_may_write_its_marker_on_either_side() {
    let template = template("DELETE FROM campaign28.accounts WHERE ? = id");
    let ConfiguredPreparedWrite::DeletePoint { handle, .. } =
        template.bind(&ints(&[10])).expect("bind must succeed")
    else {
        panic!("expected a DELETE command");
    };
    assert_eq!(handle, 10);
}

#[test]
fn delete_requires_a_single_clustered_point_predicate() {
    assert_eq!(
        unsupported("DELETE FROM campaign28.accounts"),
        UnsupportedPreparedWrite::MissingWhere
    );
    // A non-primary-key equality is not a clustered point predicate.
    assert_eq!(
        rejection("DELETE FROM campaign28.accounts WHERE balance = ?"),
        PreparedWritePlanError::PointHandlePredicate
    );
    // A non-equality predicate is not a point predicate.
    assert_eq!(
        rejection("DELETE FROM campaign28.accounts WHERE id > ?"),
        PreparedWritePlanError::PointHandlePredicate
    );
}

#[test]
fn unsupported_delete_tails_and_targets_are_rejected() {
    assert_eq!(
        unsupported("DELETE IGNORE FROM campaign28.accounts WHERE id = ?"),
        UnsupportedPreparedWrite::Ignore
    );
    assert_eq!(
        unsupported("DELETE FROM campaign28.accounts WHERE id = ? ORDER BY id"),
        UnsupportedPreparedWrite::OrderBy
    );
    assert_eq!(
        unsupported("DELETE FROM campaign28.accounts WHERE id = ? LIMIT 1"),
        UnsupportedPreparedWrite::Limit
    );
    assert_eq!(
        unsupported(
            "DELETE campaign28.accounts FROM campaign28.accounts, campaign28.accounts AS b \
             WHERE id = ?"
        ),
        UnsupportedPreparedWrite::MultiTableDelete
    );
}

// -----------------------------------------------------------------------------
// Shared admission and binding boundaries
// -----------------------------------------------------------------------------

#[test]
fn only_admitted_dml_reaches_a_prepared_write_template() {
    assert_eq!(
        unsupported("SELECT id FROM campaign28.accounts WHERE id = ?"),
        UnsupportedPreparedWrite::NonDmlStatement
    );
    // TiDB attaches a `WITH` clause to UPDATE, DELETE, and SELECT only —
    // `ast.InsertStmt` has no `With` field — so UPDATE is the admissible way
    // to reach the CTE envelope at all.
    assert_eq!(
        unsupported(
            "WITH seed AS (SELECT 1) \
             UPDATE campaign28.accounts SET balance = ? WHERE id = ?"
        ),
        UnsupportedPreparedWrite::CommonTableExpression
    );
}

#[test]
fn an_unknown_table_never_becomes_a_prepared_write() {
    assert!(matches!(
        rejection("INSERT INTO campaign28.missing (id, balance) VALUES (?, ?)"),
        PreparedWritePlanError::Catalog(_)
    ));
    assert!(matches!(
        rejection("UPDATE other.accounts SET balance = ? WHERE id = ?"),
        PreparedWritePlanError::Catalog(_)
    ));
}

#[test]
fn binding_rejects_any_parameter_count_but_its_own() {
    let insert = template("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)");
    assert_eq!(
        insert.bind(&ints(&[10])),
        Err(PreparedWriteBindError::ParameterCount {
            expected: 2,
            found: 1,
        })
    );
    assert_eq!(
        insert.bind(&ints(&[10, 100, 11])),
        Err(PreparedWriteBindError::ParameterCount {
            expected: 2,
            found: 3,
        })
    );

    let update = template("UPDATE campaign28.accounts SET balance = ? WHERE id = ?");
    assert_eq!(
        update.bind(&ints(&[150])),
        Err(PreparedWriteBindError::ParameterCount {
            expected: 2,
            found: 1,
        })
    );
}

#[test]
fn a_marker_numbered_out_of_source_order_is_rejected() {
    // The parser numbers markers left-to-right, so no admitted statement text
    // can reach this branch. It is still the invariant every bound value
    // depends on: position N of the execute packet must feed marker N. Editing
    // the parsed AST directly is the only way to prove the guard is live
    // rather than decorative.
    let mut statement = parse("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)");
    let Stmt::Dml(dml) = &mut statement else {
        panic!("expected a DML statement");
    };
    let DmlStmt::Insert(insert) = dml.as_mut() else {
        panic!("expected an INSERT statement");
    };
    insert.rows[0][1] = Expr::ParamMarker { position: 7 };

    assert_eq!(
        lower_prepared_write(&statement, &catalog()).expect_err("a misnumbered marker is rejected"),
        PreparedWritePlanError::MarkerPosition {
            expected: 1,
            found: Some(7),
        }
    );
}

#[test]
fn binding_carries_the_full_signed_bigint_domain() {
    let insert = template("INSERT INTO campaign28.accounts (id, balance) VALUES (?, ?)");
    let ConfiguredPreparedWrite::InsertRows { rows, .. } = insert
        .bind(&ints(&[i64::MIN, i64::MAX]))
        .expect("signed extremes must bind")
    else {
        panic!("expected an INSERT command");
    };
    assert_eq!(
        rows[0].values(),
        int_pairs(&[(0, i64::MIN), (1, i64::MAX)]).as_slice()
    );
}
