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

#![allow(missing_docs)]
#![allow(dead_code)]

// This leaf is intentionally tested before its crate-root wiring lands. The
// modules below mirror the existing private ownership paths and keep this
// source/test wave isolated from the shared `tidb-exec/src/lib.rs` seam.
#[path = "../src/result_field_resolver.rs"]
mod result_field_resolver;
#[path = "../src/result_metadata.rs"]
mod result_metadata;
#[path = "../src/result_schema.rs"]
mod result_schema;

use result_metadata::{ResultFieldTypeMetadata, UNSIGNED_FLAG};
use tidb_ast::Stmt;
use tidb_datatype::{Collation, FieldTypeCode};

use result_schema::{
    resolve_catalog_select_fields, CatalogColumn, CatalogSchemaError, CatalogTableSchema,
};

fn schema() -> CatalogTableSchema {
    CatalogTableSchema::new(
        "app",
        "users",
        vec![
            CatalogColumn::new(
                "id",
                ResultFieldTypeMetadata {
                    code: FieldTypeCode::LongLong,
                    flags: UNSIGNED_FLAG,
                    flen: Some(20),
                    decimal: Some(0),
                    collation: Collation::Binary,
                },
            ),
            CatalogColumn::new(
                "name",
                ResultFieldTypeMetadata {
                    code: FieldTypeCode::VarString,
                    flags: 0,
                    flen: Some(64),
                    decimal: None,
                    collation: Collation::Utf8Mb4Bin,
                },
            ),
        ],
    )
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
fn wildcard_and_qualified_columns_use_catalog_order_and_aliases() {
    let fields = resolve_catalog_select_fields(
        &select("SELECT u.name AS display_name, app.users.id FROM app.users AS u"),
        &schema(),
    )
    .expect("catalog projection");

    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0].names.original_table.original, "users");
    assert_eq!(fields[0].names.table.original, "u");
    assert_eq!(fields[0].names.original_column.original, "name");
    assert_eq!(fields[0].names.column.original, "display_name");
    assert_eq!(fields[0].field_type.code, FieldTypeCode::VarString);
    assert_eq!(fields[1].names.database.original, "app");
    assert_eq!(fields[1].names.column.original, "id");
    assert_eq!(fields[1].field_type.flags, UNSIGNED_FLAG);

    let wildcard = resolve_catalog_select_fields(&select("SELECT u.* FROM app.users u"), &schema())
        .expect("qualified wildcard");
    assert_eq!(wildcard.len(), 2);
    assert_eq!(wildcard[0].names.column.original, "id");
    assert_eq!(wildcard[1].names.column.original, "name");
}

#[test]
fn unqualified_column_matching_is_case_insensitive() {
    let fields = resolve_catalog_select_fields(&select("SELECT NAME FROM Users"), &schema())
        .expect("case-insensitive catalog lookup");
    assert_eq!(fields[0].names.original_column.original, "name");
    assert_eq!(fields[0].field_type.code, FieldTypeCode::VarString);
}

#[test]
fn schema_boundary_rejects_missing_columns_tables_joins_and_expressions() {
    let cases = [
        (
            "SELECT missing FROM users",
            CatalogSchemaError::MissingColumn {
                column: "missing".to_owned(),
            },
        ),
        (
            "SELECT id FROM other",
            CatalogSchemaError::MissingTable {
                table: "other".to_owned(),
            },
        ),
        (
            "SELECT id FROM users JOIN users AS other ON users.id = other.id",
            CatalogSchemaError::JoinRequiresRelationResolver,
        ),
    ];
    for (sql, expected) in cases {
        let error = resolve_catalog_select_fields(&select(sql), &schema()).expect_err(sql);
        assert_eq!(error, expected);
    }

    let error = resolve_catalog_select_fields(&select("SELECT 1 FROM users"), &schema())
        .expect_err("literal typing belongs to the expression resolver");
    assert!(matches!(
        error,
        CatalogSchemaError::UnsupportedExpression { .. }
    ));
}

#[test]
fn schema_boundary_rejects_unknown_qualifier_and_no_from() {
    let error = resolve_catalog_select_fields(&select("SELECT other.id FROM users"), &schema())
        .expect_err("unknown qualifier");
    assert_eq!(
        error,
        CatalogSchemaError::UnknownQualifier {
            qualifier: "other".to_owned()
        }
    );

    let error = resolve_catalog_select_fields(&select("SELECT id"), &schema())
        .expect_err("table-less path belongs to existing resolver");
    assert_eq!(error, CatalogSchemaError::FromRequired);
}

#[test]
fn wildcard_expands_declaration_order() {
    let fields =
        resolve_catalog_select_fields(&select("SELECT * FROM users"), &schema()).expect("wildcard");
    let names: Vec<_> = fields
        .iter()
        .map(|field| field.names.column.original.as_str())
        .collect();
    assert_eq!(names, ["id", "name"]);
    assert!(matches!(
        select("SELECT * FROM users").fields[0],
        tidb_ast::SelectField::Wildcard(_)
    ));
}
