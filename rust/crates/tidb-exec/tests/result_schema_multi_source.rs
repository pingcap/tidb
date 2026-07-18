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

#![allow(dead_code)]
#![allow(missing_docs)]

// Keep the source-derived assertions next to the leaf; the production module
// is re-exported by tidb-exec/src/lib.rs without moving this test's ownership.
#[path = "../src/result_field_resolver.rs"]
mod result_field_resolver;
#[path = "../src/result_metadata.rs"]
mod result_metadata;
#[path = "../src/result_schema.rs"]
mod result_schema;
#[path = "../src/result_schema_multi.rs"]
mod result_schema_multi;

use result_metadata::ResultFieldTypeMetadata;
use result_schema::{CatalogColumn, CatalogTableSchema};
use result_schema_multi::{resolve_catalog_relation_select_fields, CatalogRelationSchemaError};
use tidb_ast::Stmt;
use tidb_datatype::{Collation, FieldTypeCode};

fn column(name: &str, code: FieldTypeCode) -> CatalogColumn {
    CatalogColumn::new(
        name,
        ResultFieldTypeMetadata {
            code,
            flags: 0,
            flen: Some(32),
            decimal: None,
            collation: Collation::Utf8Mb4Bin,
        },
    )
}

fn schemas() -> Vec<CatalogTableSchema> {
    vec![
        CatalogTableSchema::new(
            "app",
            "users",
            vec![
                column("id", FieldTypeCode::LongLong),
                column("name", FieldTypeCode::VarString),
            ],
        ),
        CatalogTableSchema::new(
            "app",
            "orders",
            vec![
                column("id", FieldTypeCode::LongLong),
                column("user_id", FieldTypeCode::LongLong),
                column("total", FieldTypeCode::NewDecimal),
            ],
        ),
    ]
}

fn select(sql: &str) -> tidb_ast::SelectStmt {
    match tidb_parser::parse(sql).expect("parse source SQL") {
        Stmt::Query(query) => match *query {
            tidb_ast::QueryStmt::Select(select) => *select,
            tidb_ast::QueryStmt::SetOpr(_) => panic!("expected plain SELECT"),
        },
        other => panic!("expected query, got {other:?}"),
    }
}

#[test]
fn inner_left_and_comma_sources_preserve_relation_and_wildcard_order() {
    let schemas = schemas();
    let fields = resolve_catalog_relation_select_fields(
        &select(
            "SELECT u.*, app.orders.id AS order_id, o.total FROM app.users AS u LEFT JOIN app.orders AS o ON u.id = o.user_id",
        ),
        &schemas,
    )
    .expect("left-join result fields");
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[0].names.original_table.original, "users");
    assert_eq!(fields[0].names.table.original, "u");
    assert_eq!(fields[0].names.column.original, "id");
    assert_eq!(fields[1].names.column.original, "name");
    assert_eq!(fields[2].names.original_table.original, "orders");
    assert_eq!(fields[2].names.table.original, "o");
    assert_eq!(fields[2].names.column.original, "order_id");
    assert_eq!(fields[3].names.column.original, "total");

    let comma = resolve_catalog_relation_select_fields(
        &select("SELECT o.total, u.name FROM app.users u, app.orders o"),
        &schemas,
    )
    .expect("comma-join result fields");
    assert_eq!(
        comma
            .iter()
            .map(|field| field.names.column.original.as_str())
            .collect::<Vec<_>>(),
        ["total", "name"]
    );
    assert_eq!(comma[0].names.table.original, "o");
    assert_eq!(comma[1].names.table.original, "u");

    let self_join = resolve_catalog_relation_select_fields(
        &select("SELECT * FROM app.users AS u JOIN app.users AS v"),
        &schemas[..1],
    )
    .expect("self-join wildcard result fields");
    assert_eq!(self_join.len(), 4);
    assert_eq!(self_join[0].names.database.original, "app");
    assert_eq!(self_join[0].names.table.original, "u");
    assert_eq!(self_join[0].names.column.original, "id");
    assert_eq!(self_join[1].names.table.original, "u");
    assert_eq!(self_join[2].names.table.original, "v");
    assert_eq!(self_join[3].names.column.original, "name");
}

#[test]
fn qualified_and_unqualified_columns_preserve_aliases_and_reject_ambiguity() {
    let schemas = schemas();
    let fields = resolve_catalog_relation_select_fields(
        &select("SELECT u.id, app.orders.user_id AS uid FROM app.users AS u JOIN app.orders AS o ON u.id = o.user_id"),
        &schemas,
    )
    .expect("qualified columns");
    assert_eq!(fields[0].names.original_table.original, "users");
    assert_eq!(fields[0].names.table.original, "u");
    assert_eq!(fields[0].names.database.original, "app");
    assert_eq!(fields[1].names.original_table.original, "orders");
    assert_eq!(fields[1].names.table.original, "o");
    assert_eq!(fields[1].names.original_column.original, "user_id");
    assert_eq!(fields[1].names.column.original, "uid");

    let error = resolve_catalog_relation_select_fields(
        &select("SELECT id FROM app.users u JOIN app.orders o ON u.id = o.user_id"),
        &schemas,
    )
    .expect_err("same column in two relations is ambiguous");
    assert_eq!(
        error,
        CatalogRelationSchemaError::AmbiguousColumn {
            column: "id".to_owned(),
            qualifiers: vec!["u".to_owned(), "o".to_owned()],
        }
    );

    let error = resolve_catalog_relation_select_fields(
        &select("SELECT users.id FROM app.users AS u JOIN app.orders AS o ON u.id = o.user_id"),
        &schemas,
    )
    .expect_err("an alias hides the two-part original table qualifier");
    assert_eq!(
        error,
        CatalogRelationSchemaError::UnknownQualifier {
            qualifier: "users".to_owned()
        }
    );

    let users = vec![schemas[0].clone()];
    let error = resolve_catalog_relation_select_fields(
        &select("SELECT app.users.id FROM app.users AS u JOIN app.users AS v ON u.id = v.id"),
        &users,
    )
    .expect_err("original table path is ambiguous across aliases");
    assert_eq!(
        error,
        CatalogRelationSchemaError::AmbiguousQualifier {
            qualifier: "app.users".to_owned()
        }
    );
}

#[test]
fn missing_sources_columns_and_qualifiers_are_explicit() {
    let schemas = schemas();
    let error = resolve_catalog_relation_select_fields(
        &select("SELECT id FROM app.users u JOIN app.missing m ON u.id = m.id"),
        &schemas,
    )
    .expect_err("missing relation snapshot");
    assert_eq!(
        error,
        CatalogRelationSchemaError::MissingTable {
            table: "app.missing".to_owned()
        }
    );

    let error = resolve_catalog_relation_select_fields(
        &select("SELECT u.missing FROM app.users AS u JOIN app.orders AS o ON u.id = o.user_id"),
        &schemas,
    )
    .expect_err("missing qualified column");
    assert_eq!(
        error,
        CatalogRelationSchemaError::MissingColumn {
            qualifier: Some("u".to_owned()),
            column: "missing".to_owned(),
        }
    );

    let error = resolve_catalog_relation_select_fields(
        &select("SELECT x.id FROM app.users AS u JOIN app.orders AS o ON u.id = o.user_id"),
        &schemas,
    )
    .expect_err("unknown qualifier");
    assert_eq!(
        error,
        CatalogRelationSchemaError::UnknownQualifier {
            qualifier: "x".to_owned()
        }
    );
}

#[test]
fn derived_right_and_expression_shapes_are_not_guessed() {
    let schemas = schemas();
    let cases = [
        (
            "SELECT * FROM (SELECT id FROM app.users) AS u",
            CatalogRelationSchemaError::DerivedTable,
        ),
        (
            "SELECT * FROM app.users u RIGHT JOIN app.orders o ON u.id = o.user_id",
            CatalogRelationSchemaError::UnsupportedJoin {
                reason: "RIGHT OUTER JOIN is outside the bounded source leaf",
            },
        ),
    ];
    for (sql, expected) in cases {
        let error = resolve_catalog_relation_select_fields(&select(sql), &schemas)
            .expect_err("unsupported relation shape");
        assert_eq!(error, expected, "{sql}");
    }

    let error = resolve_catalog_relation_select_fields(
        &select("SELECT u.id + o.id FROM app.users u JOIN app.orders o ON u.id = o.user_id"),
        &schemas,
    )
    .expect_err("expression typing requires planner metadata");
    assert!(matches!(
        error,
        CatalogRelationSchemaError::UnsupportedExpression { .. }
    ));
}
