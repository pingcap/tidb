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

//! Source-shaped tests for projection metadata over resolved JOIN schemas.
//!
//! Bare projections consume the visible coalesced schema, while qualified
//! columns and wildcards retain access to redundant USING-side full fields.

#![allow(dead_code)]
#![allow(missing_docs)]

#[path = "../src/result_field_resolver.rs"]
mod result_field_resolver;
#[path = "../src/result_metadata.rs"]
mod result_metadata;
#[path = "../src/result_schema_join_output.rs"]
mod result_schema_join_output;
#[path = "../src/result_schema_projection.rs"]
mod result_schema_projection;

use result_field_resolver::ResolvedResultField;
use result_metadata::{
    FieldNameMetadata, IdentifierMetadata, ResultFieldTypeMetadata, NOT_NULL_FLAG,
};
use result_schema_join_output::{derive_join_output_metadata, JoinOutputChild, JoinOutputField};
use result_schema_projection::{project_join_output_fields, JoinProjectionError};
use tidb_ast::{QueryStmt, Stmt};
use tidb_datatype::{Collation, FieldTypeCode};

fn field(table: &str, column: &str) -> JoinOutputField {
    JoinOutputField::new(
        ResolvedResultField {
            names: FieldNameMetadata {
                original_table: IdentifierMetadata::new(match table {
                    "l" => "left_table",
                    "r" => "right_table",
                    _ => table,
                }),
                original_column: IdentifierMetadata::new(column),
                database: IdentifierMetadata::new("app"),
                table: IdentifierMetadata::new(table),
                column: IdentifierMetadata::new(column),
            },
            field_type: ResultFieldTypeMetadata {
                code: FieldTypeCode::LongLong,
                flags: 0,
                flen: Some(20),
                decimal: Some(0),
                collation: Collation::Binary,
            },
        },
        false,
    )
}

fn select(sql: &str) -> tidb_ast::SelectStmt {
    let Stmt::Query(query) = tidb_parser::parse(sql).expect("parse source SQL") else {
        panic!("expected query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected plain SELECT")
    };
    *select
}

fn join_metadata(
    sql: &str,
    left: Vec<JoinOutputField>,
    right: Vec<JoinOutputField>,
) -> result_schema_join_output::JoinOutputMetadata {
    let from = select(sql).from.expect("join source");
    derive_join_output_metadata(
        &from,
        JoinOutputChild::Fields(left),
        JoinOutputChild::Fields(right),
    )
    .expect("join output metadata")
}

#[test]
fn direct_columns_and_wildcards_preserve_join_order_and_apply_aliases() {
    let sql = "SELECT r.payload AS right_payload, l.id, * FROM app.left_table AS l JOIN app.right_table AS r ON l.id = r.id";
    let statement = select(sql);
    let metadata = join_metadata(
        sql,
        vec![field("l", "id"), field("l", "payload")],
        vec![field("r", "id"), field("r", "payload")],
    );
    let projected =
        project_join_output_fields(&statement.fields, &metadata).expect("bounded join projection");

    assert_eq!(
        projected
            .iter()
            .map(|field| field.names.column.original.as_str())
            .collect::<Vec<_>>(),
        ["right_payload", "id", "id", "payload", "id", "payload"]
    );
    assert_eq!(projected[0].names.table.original, "r");
    assert_eq!(projected[0].names.original_column.original, "payload");
    assert_eq!(projected[2].names.table.original, "l");
    assert_eq!(projected[4].names.table.original, "r");
}

#[test]
fn qualified_wildcards_share_the_row_owner_width_contract() {
    let sql = "SELECT l.*, r.* FROM app.left_table AS l JOIN app.right_table AS r ON l.id = r.id";
    let statement = select(sql);
    let metadata = join_metadata(
        sql,
        vec![field("l", "id"), field("l", "payload")],
        vec![field("r", "id"), field("r", "payload")],
    );
    let projected = project_join_output_fields(&statement.fields, &metadata)
        .expect("qualified wildcard projection");
    assert_eq!(projected.len(), 4);
    assert_eq!(projected[0].names.table.original, "l");
    assert_eq!(projected[1].names.table.original, "l");
    assert_eq!(projected[2].names.table.original, "r");
    assert_eq!(projected[3].names.table.original, "r");
}

#[test]
fn left_projection_keeps_null_extended_right_type_metadata() {
    let mut right_id = field("r", "id");
    right_id.field.field_type.flags = NOT_NULL_FLAG;
    let sql = "SELECT r.id, l.id AS left_id FROM app.left_table AS l LEFT JOIN app.right_table AS r ON l.id = r.id";
    let statement = select(sql);
    let metadata = join_metadata(sql, vec![field("l", "id")], vec![right_id]);
    let projected = project_join_output_fields(&statement.fields, &metadata)
        .expect("left join direct projection");
    assert_eq!(projected.len(), 2);
    assert_eq!(projected[0].names.column.original, "id");
    assert_eq!(projected[0].field_type.flags & NOT_NULL_FLAG, 0);
    assert_eq!(projected[1].names.column.original, "left_id");
    assert_eq!(projected[1].field_type.flags, 0);
}

#[test]
fn using_projection_keeps_bare_star_coalesced_and_qualified_right_visible() {
    let sql = "SELECT id AS merged, r.right_only, * FROM app.left_table AS l JOIN app.right_table AS r USING (id)";
    let statement = select(sql);
    let metadata = join_metadata(
        sql,
        vec![field("l", "id"), field("l", "left_only")],
        vec![field("r", "id"), field("r", "right_only")],
    );
    let projected =
        project_join_output_fields(&statement.fields, &metadata).expect("using projection");
    assert_eq!(
        projected
            .iter()
            .map(|field| field.names.column.original.as_str())
            .collect::<Vec<_>>(),
        ["merged", "right_only", "id", "left_only", "right_only"]
    );
    assert_eq!(projected[0].names.table.original, "l");

    let right = project_join_output_fields(
        &select("SELECT r.id FROM app.left_table AS l JOIN app.right_table AS r USING (id)").fields,
        &metadata,
    )
    .expect("qualified right USING column resolves through FullSchema");
    assert_eq!(right.len(), 1);
    assert_eq!(right[0].names.table.original, "r");

    let qualified = project_join_output_fields(
        &select("SELECT l.*, r.* FROM app.left_table AS l JOIN app.right_table AS r USING (id)")
            .fields,
        &metadata,
    )
    .expect("qualified wildcards retain each physical side");
    assert_eq!(qualified.len(), 4);
    assert_eq!(qualified[0].names.table.original, "l");
    assert_eq!(qualified[2].names.table.original, "r");
}

#[test]
fn projection_boundary_rejects_ambiguous_and_untyped_expressions() {
    let sql = "SELECT id FROM app.left_table AS l JOIN app.right_table AS r ON l.id = r.id";
    let metadata = join_metadata(sql, vec![field("l", "id")], vec![field("r", "id")]);
    let error = project_join_output_fields(&select(sql).fields, &metadata)
        .expect_err("unqualified join column is ambiguous");
    assert_eq!(
        error,
        JoinProjectionError::AmbiguousColumn {
            column: "id".to_owned(),
        }
    );

    let unknown = project_join_output_fields(
        &select("SELECT nope.id FROM app.left_table AS l JOIN app.right_table AS r ON l.id = r.id")
            .fields,
        &metadata,
    )
    .expect_err("unknown relation qualifier must not become a missing column");
    assert_eq!(
        unknown,
        JoinProjectionError::UnknownQualifier {
            qualifier: "nope".to_owned(),
        }
    );

    let expression_sql =
        "SELECT l.id + 1 FROM app.left_table AS l JOIN app.right_table AS r ON l.id = r.id";
    let error = project_join_output_fields(&select(expression_sql).fields, &metadata)
        .expect_err("expression typing belongs to planner/executor");
    assert!(matches!(
        error,
        JoinProjectionError::UnsupportedExpression { .. }
    ));
}
