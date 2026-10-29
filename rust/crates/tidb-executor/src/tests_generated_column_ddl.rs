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

#[allow(unused_imports)]
use std::cell::{Cell, RefCell};
#[allow(unused_imports)]
use tidb_datatype::{Datum, FieldType};
#[allow(unused_imports)]
use tidb_expr::expression::Expression;
#[allow(unused_imports)]
use tidb_expr::rewriter::ColumnResolver;
#[allow(unused_imports)]
use tidb_hack::GoCaseCompare;
#[allow(unused_imports)]
use tidb_ddl_build::generated_column::*;
#[allow(unused_imports)]
use tidb_table::generated_column::*;
use tidb_datatype::FieldTypeCode;

fn materialize<S: GeneratedColumnSlot>(
    columns: &[S],
    row: &mut [Datum],
    only_virtual: bool,
) -> Result<(), crate::DriverError> {
    let ctx = crate::StmtContext::for_query();
    materialize_with(columns, row, only_virtual, &ctx, |column, value| {
        tidb_table::column::cast_table_value(
            value,
            column.column_type(),
            column.column_name(),
            &ctx,
            true,
        )
        .map_err(crate::DriverError::from)
    })
}

struct Slot {
    name: String,
    generation: Option<GeneratedColumn>,
    field_type: FieldType,
}

impl GeneratedColumnSlot for Slot {
    fn generation(&self) -> Option<&GeneratedColumn> {
        self.generation.as_ref()
    }
    fn column_type(&self) -> &FieldType {
        &self.field_type
    }
    fn column_name(&self) -> &str {
        &self.name
    }
}

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::Long)
}

/// The generated-column expression of `create table t (x int as (<text>))`,
/// i.e. the same AST node the DDL path reads.
fn parse_generated_expr(text: &str) -> tidb_ast::Expr {
    let sql = format!("create table t (x int as ({text}))");
    let stmt = tidb_parser::parse(&sql).unwrap();
    let tidb_ast::Stmt::Ddl(ddl) = &stmt else {
        unreachable!("a CREATE TABLE parses as DDL")
    };
    let tidb_ast::DdlStmt::CreateTable(create) = &**ddl else {
        unreachable!("a CREATE TABLE parses as CreateTable")
    };
    for option in &create.columns[0].options {
        if let tidb_ast::ColumnOption::Generated { expression, .. } = option {
            return expression.clone();
        }
    }
    unreachable!("the column carries a generated option")
}

/// Builds `[a, b AS (a+1) <kind>, c AS (b+1) <kind>]`.
fn chain(stored: bool) -> Vec<Slot> {
    let names = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let types = vec![int_type(), int_type(), int_type()];
    let build = |text: &str| {
        let resolver =
            TableColumnResolver::new(&names, &types, tidb_datatype::SessionTimeZone::utc());
        let expr = parse_generated_expr(text);
        let built = tidb_expr::rewriter::rewrite_expr_resolved(&expr, &resolver).unwrap();
        GeneratedColumn {
            expr_text: text.to_owned(),
            stored,
            expr: built,
            dependencies: resolver.dependency_names(),
            source: expr,
            build_zone: tidb_datatype::SessionTimeZone::utc(),
            build_like_default_escape: b'\\',
            zone_sensitive: resolver.zone_was_read(),
            like_default_escape_sensitive: resolver.like_default_escape_was_read(),
        }
    };
    vec![
        Slot {
            name: "a".to_owned(),
            generation: None,
            field_type: int_type(),
        },
        Slot {
            name: "b".to_owned(),
            generation: Some(build("a + 1")),
            field_type: int_type(),
        },
        Slot {
            name: "c".to_owned(),
            generation: Some(build("b + 1")),
            field_type: int_type(),
        },
    ]
}

#[test]
fn a_chain_of_generated_columns_is_computed_left_to_right() {
    let columns = chain(true);
    let mut row = vec![Datum::Int(1), Datum::Null, Datum::Null];
    materialize(&columns, &mut row, false).unwrap();
    assert_eq!(row, vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]);
}

/// Pure dependency evaluation retains the same values on a second pass;
/// statement-owned warning effects must still be evaluated only once.
#[test]
fn materializing_twice_gives_the_same_row() {
    let columns = chain(true);
    let mut row = vec![Datum::Int(4), Datum::Null, Datum::Null];
    materialize(&columns, &mut row, false).unwrap();
    let once = row.clone();
    materialize(&columns, &mut row, false).unwrap();
    assert_eq!(row, once);
}

/// The staleness trap: a row whose dependency changed must not keep the
/// value that was computed from the OLD dependency.
#[test]
fn a_changed_dependency_recomputes_the_whole_chain() {
    let columns = chain(true);
    let mut row = vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)];
    row[0] = Datum::Int(10);
    materialize(&columns, &mut row, false).unwrap();
    assert_eq!(row, vec![Datum::Int(10), Datum::Int(11), Datum::Int(12)]);
}

/// The read path restores only what the bytes did not hold.
#[test]
fn only_virtual_leaves_a_stored_column_as_decoded() {
    let columns = chain(true);
    let mut row = vec![Datum::Int(1), Datum::Int(99), Datum::Int(98)];
    materialize(&columns, &mut row, true).unwrap();
    assert_eq!(row, vec![Datum::Int(1), Datum::Int(99), Datum::Int(98)]);
}

#[test]
fn only_virtual_recomputes_a_virtual_column() {
    let columns = chain(false);
    let mut row = vec![Datum::Int(1), Datum::Null, Datum::Null];
    materialize(&columns, &mut row, true).unwrap();
    assert_eq!(row, vec![Datum::Int(1), Datum::Int(2), Datum::Int(3)]);
}

#[test]
fn an_unknown_dependency_is_reported_by_name() {
    let names = vec!["a".to_owned()];
    let types = vec![int_type()];
    let resolver =
        TableColumnResolver::new(&names, &types, tidb_datatype::SessionTimeZone::utc());
    let expr = parse_generated_expr("zz + 1");
    assert!(tidb_expr::rewriter::rewrite_expr_resolved(&expr, &resolver).is_err());
    assert_eq!(resolver.missing_name().as_deref(), Some("zz"));
}

#[test]
fn dependencies_are_the_offsets_the_expression_reads() {
    let names = vec!["a".to_owned(), "b".to_owned(), "c".to_owned()];
    let types = vec![int_type(), int_type(), int_type()];
    let resolver =
        TableColumnResolver::new(&names, &types, tidb_datatype::SessionTimeZone::utc());
    let expr = parse_generated_expr("c + a");
    tidb_expr::rewriter::rewrite_expr_resolved(&expr, &resolver).unwrap();
    assert_eq!(resolver.dependencies(), vec![2, 0]);
}
