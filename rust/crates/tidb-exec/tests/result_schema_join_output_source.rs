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

//! Source-shaped tests for planner-owned JOIN output metadata.
//!
//! These tests preserve visible versus full schema order, USING coalescing,
//! redundant-column mapping, and outer-join nullability without claiming row
//! execution.

#![allow(dead_code)]
#![allow(missing_docs)]

// Keep this source-derived contract isolated until the planner/executor root
// has a real relation-schema owner. The production module is intentionally not
// wired into tidb-exec's public surface in this leaf.
#[path = "../src/result_field_resolver.rs"]
mod result_field_resolver;
#[path = "../src/result_metadata.rs"]
mod result_metadata;
#[path = "../src/result_schema_join_output.rs"]
mod result_schema_join_output;

use result_field_resolver::ResolvedResultField;
use result_metadata::{
    FieldNameMetadata, IdentifierMetadata, ResultFieldTypeMetadata, NOT_NULL_FLAG,
};
use result_schema_join_output::{
    derive_join_output_metadata, JoinOutputChild, JoinOutputField, JoinOutputOrigin,
    JoinOutputSchemaError, JoinOutputUnsupported,
};
use tidb_ast::{Join, QueryStmt, Stmt};
use tidb_datatype::{Collation, FieldTypeCode};

fn field(table: &str, column: &str) -> JoinOutputField {
    JoinOutputField::new(
        ResolvedResultField {
            names: FieldNameMetadata {
                original_table: IdentifierMetadata::new(table),
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

fn join(sql: &str) -> Join {
    let Stmt::Query(query) = tidb_parser::parse(sql).expect("parse join SQL") else {
        panic!("expected query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("expected plain SELECT")
    };
    let select = *select;
    select.from.expect("join source")
}

fn child(fields: Vec<JoinOutputField>) -> JoinOutputChild {
    JoinOutputChild::Fields(fields)
}

#[test]
fn inner_and_cross_output_keep_declared_child_order_and_nullability() {
    let output = derive_join_output_metadata(
        &join("SELECT * FROM app.left_table l JOIN app.right_table r ON l.id = r.id"),
        child(vec![field("l", "id"), field("l", "payload")]),
        child(vec![field("r", "id"), field("r", "payload")]),
    )
    .expect("inner join metadata");

    assert_eq!(
        output
            .fields
            .iter()
            .map(|field| field.field.names.column.original.as_str())
            .collect::<Vec<_>>(),
        ["id", "payload", "id", "payload"]
    );
    assert!(output.fields.iter().all(|field| !field.nullable));
    assert_eq!(
        output
            .full_fields
            .iter()
            .map(|field| field.field.names.table.original.as_str())
            .collect::<Vec<_>>(),
        ["l", "l", "r", "r"]
    );
    assert_eq!(output.full_to_output_indices, [0, 1, 2, 3]);
    assert_eq!(
        output.unsupported,
        [JoinOutputUnsupported::OnExpressionEvaluation]
    );

    let cross = derive_join_output_metadata(
        &join("SELECT * FROM app.left_table l CROSS JOIN app.right_table r"),
        child(vec![field("l", "id")]),
        child(vec![field("r", "id")]),
    )
    .expect("cross join metadata");
    assert!(cross.unsupported.is_empty());
    assert_eq!(cross.fields.len(), 2);
}

#[test]
fn left_join_marks_inner_side_nullable_but_keeps_execution_gaps_explicit() {
    let mut right_id = field("r", "id");
    right_id.field.field_type.flags = NOT_NULL_FLAG;
    let output = derive_join_output_metadata(
        &join("SELECT * FROM app.left_table l LEFT JOIN app.right_table r ON l.id = r.id"),
        child(vec![field("l", "id"), field("l", "payload")]),
        child(vec![right_id, field("r", "payload")]),
    )
    .expect("left join metadata");

    assert_eq!(
        output
            .fields
            .iter()
            .map(|field| field.nullable)
            .collect::<Vec<_>>(),
        [false, false, true, true]
    );
    assert_eq!(
        output.unsupported,
        [
            JoinOutputUnsupported::OnExpressionEvaluation,
            JoinOutputUnsupported::RowNullExtension,
        ]
    );
    assert_eq!(output.fields[2].field.field_type.flags & NOT_NULL_FLAG, 0);
    assert_eq!(
        output
            .full_fields
            .iter()
            .map(|field| field.nullable)
            .collect::<Vec<_>>(),
        [false, false, true, true]
    );
}

#[test]
fn using_coalesces_common_fields_in_left_declaration_order() {
    let output = derive_join_output_metadata(
        &join("SELECT * FROM app.left_table l JOIN app.right_table r USING (id, z)"),
        child(vec![
            field("l", "z"),
            field("l", "id"),
            field("l", "left_only"),
        ]),
        child(vec![
            field("r", "id"),
            field("r", "z"),
            field("r", "right_only"),
        ]),
    )
    .expect("using metadata");

    assert_eq!(
        output
            .fields
            .iter()
            .map(|field| field.field.names.column.original.as_str())
            .collect::<Vec<_>>(),
        ["z", "id", "left_only", "right_only"]
    );
    assert!(matches!(
        output.fields[0].origin,
        JoinOutputOrigin::UsingCoalesced { .. }
    ));
    assert!(matches!(
        output.fields[1].origin,
        JoinOutputOrigin::UsingCoalesced { .. }
    ));
    assert_eq!(
        output
            .full_fields
            .iter()
            .map(|field| field.field.names.column.original.as_str())
            .collect::<Vec<_>>(),
        ["z", "id", "left_only", "id", "z", "right_only"]
    );
    // FullSchema keeps all source fields, while the redundant right USING
    // keys resolve to the canonical visible left field.
    assert_eq!(output.full_to_output_indices, [0, 1, 2, 1, 0, 3]);
    assert_eq!(
        output.unsupported,
        [JoinOutputUnsupported::UsingPredicateEvaluation]
    );
}

#[test]
fn right_join_keeps_visible_order_but_mirrors_full_schema() {
    let mut left_id = field("l", "id");
    left_id.field.field_type.flags = NOT_NULL_FLAG;
    let output = derive_join_output_metadata(
        &join("SELECT * FROM app.left_table l RIGHT JOIN app.right_table r ON l.id = r.id"),
        child(vec![left_id, field("l", "left_only")]),
        child(vec![field("r", "id"), field("r", "right_only")]),
    )
    .expect("right join metadata");

    // Ordinary RIGHT output stays syntactic left+right.
    assert_eq!(
        output
            .fields
            .iter()
            .map(|field| field.field.names.table.original.as_str())
            .collect::<Vec<_>>(),
        ["l", "l", "r", "r"]
    );
    assert_eq!(
        output
            .fields
            .iter()
            .map(|field| field.nullable)
            .collect::<Vec<_>>(),
        [true, true, false, false]
    );
    assert_eq!(output.fields[0].field.field_type.flags & NOT_NULL_FLAG, 0);

    // FullSchema mirrors to outer(right)+inner(left), with its inner suffix
    // null-extended and mapped back to the executable left+right row.
    assert_eq!(
        output
            .full_fields
            .iter()
            .map(|field| field.field.names.table.original.as_str())
            .collect::<Vec<_>>(),
        ["r", "r", "l", "l"]
    );
    assert_eq!(output.full_to_output_indices, [2, 3, 0, 1]);
    assert_eq!(
        output.unsupported,
        [
            JoinOutputUnsupported::OnExpressionEvaluation,
            JoinOutputUnsupported::RowNullExtension,
        ]
    );
}

#[test]
fn right_using_and_natural_follow_outer_child_declaration_order() {
    let left = vec![field("l", "z"), field("l", "id"), field("l", "left_only")];
    let right = vec![field("r", "id"), field("r", "z"), field("r", "right_only")];
    for sql in [
        "SELECT * FROM app.left_table l RIGHT JOIN app.right_table r USING (z, id)",
        "SELECT * FROM app.left_table l NATURAL RIGHT JOIN app.right_table r",
    ] {
        let output =
            derive_join_output_metadata(&join(sql), child(left.clone()), child(right.clone()))
                .expect("right coalescing metadata");
        assert_eq!(
            output
                .fields
                .iter()
                .map(|field| field.field.names.column.original.as_str())
                .collect::<Vec<_>>(),
            ["id", "z", "right_only", "left_only"],
            "SQL: {sql}"
        );
        assert_eq!(
            output
                .full_fields
                .iter()
                .map(|field| field.field.names.table.original.as_str())
                .collect::<Vec<_>>(),
            ["r", "r", "r", "l", "l", "l"]
        );
        assert_eq!(output.full_to_output_indices, [0, 1, 2, 1, 0, 3]);
        assert!(output.full_fields[3..].iter().all(|field| field.nullable));
    }

    let zero_common = derive_join_output_metadata(
        &join("SELECT * FROM app.left_table l NATURAL JOIN app.right_table r"),
        child(vec![field("l", "a")]),
        child(vec![field("r", "b")]),
    )
    .expect("zero-common NATURAL is cross metadata");
    assert_eq!(zero_common.fields.len(), 2);
    assert!(zero_common.unsupported.is_empty());
}

#[test]
fn using_errors_are_explicit() {
    let left = child(vec![field("l", "id")]);
    let right = child(vec![field("r", "id")]);

    let join_with_using = join("SELECT * FROM app.left_table l JOIN app.right_table r USING (id)");
    let error = derive_join_output_metadata(
        &join_with_using,
        child(vec![field("l", "id"), field("l", "id")]),
        right.clone(),
    )
    .expect_err("ambiguous left using column");
    assert_eq!(
        error,
        JoinOutputSchemaError::AmbiguousUsingColumn {
            side: "left",
            column: "id".to_owned(),
        }
    );

    let error = derive_join_output_metadata(
        &join_with_using,
        left.clone(),
        child(vec![field("r", "missing")]),
    )
    .expect_err("missing right using column");
    assert_eq!(
        error,
        JoinOutputSchemaError::MissingUsingColumn {
            side: "right",
            column: "id".to_owned(),
        }
    );

    let error = derive_join_output_metadata(&join_with_using, JoinOutputChild::Derived, right)
        .expect_err("derived output needs planner schema");
    assert_eq!(error, JoinOutputSchemaError::DerivedRelation);
}
