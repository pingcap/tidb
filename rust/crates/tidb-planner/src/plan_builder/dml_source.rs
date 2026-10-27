// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The read of an UPDATE or DELETE as the one SELECT this builder plans.
//!
//! Go's `buildUpdate` / `buildDelete` build the read from the statement's own
//! clauses (`buildResultSetNode(TableRefs)`, `buildSelection(Where)`,
//! `buildSort(Order)`, `buildLimit`), with the statement's `WITH` and table
//! hints pushed for it (`pushTableHints(stmt.TableHints, 0)`). This crate's
//! builder takes that input as a `SELECT` over the complete rows (`*`);
//! [`dml_source_select`] is the one place that statement is assembled.

use tidb_ast::{
    Expr, Hint, Join, JoinNode, JoinType, Limit, OrderItem, SelectField, SelectFieldList,
    SelectStmt, TableRef, WithClause,
};

/// The `FROM` of a single-table write: the table alone.
#[must_use]
pub fn single_table_join(table_ref: &TableRef) -> Join {
    Join {
        left: JoinNode::Table(table_ref.clone()),
        right: None,
        tp: JoinType::Cross,
        straight: false,
        on: None,
        using: Vec::new(),
        natural: false,
        explicit_parens: false,
    }
}

/// The read of a write: `from` with the `WHERE`, `ORDER BY` and `LIMIT`
/// above it, carrying the statement's `WITH` clause and hints.
#[must_use]
pub fn dml_source_select(
    with: Option<&WithClause>,
    hints: &[Hint],
    from: Join,
    where_clause: Option<&Expr>,
    order_by: &[OrderItem],
    limit: Option<&Limit>,
) -> SelectStmt {
    let mut fields = SelectFieldList::default();
    fields.push(SelectField::Wildcard(Vec::new()));
    SelectStmt {
        kind: Default::default(),
        is_in_braces: false,
        with_before_braces: false,
        with: with.cloned(),
        hints: hints.to_vec(),
        priority: Default::default(),
        sql_small_result: false,
        sql_big_result: false,
        sql_buffer_result: false,
        sql_no_cache: false,
        straight_join: false,
        calc_found_rows: false,
        distinct: false,
        all: false,
        fields,
        values: Vec::new(),
        from: Some(from),
        where_clause: where_clause.cloned(),
        group_by: Vec::new(),
        rollup: false,
        having: None,
        windows: Vec::new(),
        order_by: order_by.to_vec(),
        limit: limit.cloned(),
        lock: None,
        into_outfile: None,
        query_block_offset: 0,
        as_view_schema: false,
    }
}
