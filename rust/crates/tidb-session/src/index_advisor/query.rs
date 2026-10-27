// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/planner/indexadvisor/utils.go`: what the advisor reads out of the
//! workload's statements.
//!
//! Go walks typed nodes (`TableName`, `ColumnNameExpr`, `OrderByClause`, ...);
//! the Rust AST keeps a clause's items as plain vectors, so the clause-level
//! checks run per item, in the same traversal order, which yields the same
//! answer.

use std::any::Any;
use std::collections::HashSet;

use tidb_ast::{
    BinaryOp, Expr, GroupByItem, OrderItem, SelectField, SelectStmt, Stmt, TableRef, Visitable,
    Visitor, WithClause,
};
use tidb_datatype::FieldType;
use tidb_hack::GoToLower;
use tidb_util::stringutil::go_to_lower;

use super::keyset::KeySet;
use super::model::{Column, Query};
use super::{advisor_error, Optimizer, Result};

type ColumnSet = KeySet<Column>;
type QuerySet = KeySet<Query>;

/// Go `ParseOneSQL`.
pub(crate) fn parse_one_sql(text: &str) -> Result<Stmt> {
    tidb_parser::parse(text).map_err(|error| parse_failure(&error, text))
}

fn parse_failure(error: &tidb_parser::ParseError, sql: &str) -> tidb_executor::DriverError {
    match error.terror {
        Some(proto) => {
            tidb_executor::DriverError::from((proto).generate_with_stack(error.message.clone()))
        }
        None => tidb_executor::DriverError::from(tidb_util::misc::syntax_error(
            error.compatibility_message(sql),
        )),
    }
}

/// The text Go's `err` prints for a parse failure: the terror's `[class:code]`
/// prefix and its message.
fn parse_failure_text(error: &tidb_parser::ParseError, sql: &str) -> String {
    parse_failure(error, sql).to_mysql_error().error_string()
}

struct FnVisitor<E, L> {
    enter: E,
    leave: L,
}

impl<E, L> Visitor for FnVisitor<E, L>
where
    E: FnMut(&mut dyn Any) -> bool,
    L: FnMut(&mut dyn Any) -> bool,
{
    fn enter(&mut self, node: &mut dyn Any) -> bool {
        (self.enter)(node)
    }

    fn leave(&mut self, node: &mut dyn Any) -> bool {
        (self.leave)(node)
    }
}

/// Go `visitNode`: `enter` returns whether to skip the children, `leave`
/// whether to go on.
fn visit_node(
    statement: &mut Stmt,
    enter: impl FnMut(&mut dyn Any) -> bool,
    leave: impl FnMut(&mut dyn Any) -> bool,
) {
    statement.accept(&mut FnVisitor { enter, leave });
}

/// Go `NormalizeDigest`: the normalized text of a statement.
pub(crate) fn normalize_digest(text: &str) -> String {
    tidb_parser::normalize_digest(text).0
}

/// Go `CollectTableNamesFromQuery`: every referenced table as
/// `schema.table`.
pub(crate) fn collect_table_names_from_query(
    default_schema: &str,
    query: &str,
) -> Result<Vec<String>> {
    let mut statement = parse_one_sql(query)?;
    let mut cte_names: HashSet<String> = HashSet::new();
    let mut table_names = Vec::new();
    visit_node(
        &mut statement,
        |node| {
            if let Some(with) = node.downcast_mut::<WithClause>() {
                for cte in &with.ctes {
                    cte_names.insert(format!("{default_schema}.{}", cte.name));
                }
            } else if let Some(table) = node.downcast_mut::<TableRef>() {
                let name = match table.name.as_slice() {
                    [name] => format!("{default_schema}.{name}"),
                    [schema, name] if schema.is_empty() => format!("{default_schema}.{name}"),
                    [schema, name] => format!("{}.{name}", go_to_lower(schema)),
                    _ => String::new(),
                };
                if !cte_names.contains(&name) {
                    table_names.push(name);
                }
            }
            false
        },
        |_| true,
    );
    Ok(table_names)
}

fn single_table(query: &Query) -> Result<Option<(String, String)>> {
    let names = collect_table_names_from_query(&query.schema_name, &query.text)?;
    if names.len() != 1 {
        // Unsupported yet.
        return Ok(None);
    }
    let (schema, table) = names[0].split_once('.').unwrap_or((names[0].as_str(), ""));
    Ok(Some((schema.to_owned(), table.to_owned())))
}

fn column_name(expression: &Expr) -> Option<&String> {
    match expression {
        Expr::Column(path) => path.last(),
        _ => None,
    }
}

/// Go `CollectSelectColumnsFromQuery`: the columns the select list reads.
/// `None` when the statement does not read exactly one table.
pub(crate) fn collect_select_columns_from_query(query: &Query) -> Result<Option<ColumnSet>> {
    let Some((schema, table)) = single_table(query)? else {
        return Ok(None);
    };
    let mut statement = parse_one_sql(&query.text)?;
    let under_select_field = std::cell::Cell::new(false);
    let mut columns = ColumnSet::new();
    visit_node(
        &mut statement,
        |node| {
            if node.is::<SelectField>() {
                under_select_field.set(true);
            } else if let Some(expression) = node.downcast_ref::<Expr>() {
                if under_select_field.get() {
                    if let Some(name) = column_name(expression) {
                        columns.add(Column {
                            schema_name: schema.clone(),
                            table_name: table.clone(),
                            column_name: name.clone(),
                        });
                    }
                }
            }
            false
        },
        |node| {
            if node.is::<SelectField>() {
                under_select_field.set(false);
            }
            true
        },
    );
    Ok(Some(columns))
}

/// Go `CollectOrderByColumnsFromQuery`: the order-by columns, or none if any
/// item is not a plain column.
pub(crate) fn collect_order_by_columns_from_query(query: &Query) -> Result<Vec<Column>> {
    let Some((schema, table)) = single_table(query)? else {
        return Ok(Vec::new());
    };
    let mut statement = parse_one_sql(&query.text)?;
    let mut columns: Vec<Column> = Vec::new();
    let mut exit = false;
    visit_node(
        &mut statement,
        |node| {
            if exit {
                return true;
            }
            if let Some(item) = node.downcast_ref::<OrderItem>() {
                match column_name(&item.expr) {
                    Some(name) => columns.push(Column {
                        schema_name: schema.clone(),
                        table_name: table.clone(),
                        column_name: name.clone(),
                    }),
                    None => {
                        columns.clear();
                        exit = true;
                        return true;
                    }
                }
            }
            false
        },
        |_| true,
    );
    Ok(columns)
}

/// Go `CollectDNFColumnsFromQuery`: for `where c1=1 or c2=2 or c3=3`, the
/// columns c1, c2 and c3. `None` when the statement does not read exactly
/// one table.
pub(crate) fn collect_dnf_columns_from_query(query: &Query) -> Result<Option<ColumnSet>> {
    let Some((schema, table)) = single_table(query)? else {
        return Ok(None);
    };
    let mut statement = parse_one_sql(&query.text)?;
    let mut dnf_columns = ColumnSet::new();
    visit_node(
        &mut statement,
        |node| {
            if dnf_columns.size() > 0 {
                // Already collected.
                return true;
            }
            if let Some(select) = node.downcast_ref::<SelectStmt>() {
                for conjunct in flatten_cnf(select.where_clause.as_ref()) {
                    let disjuncts = flatten_dnf(conjunct);
                    if disjuncts.len() <= 1 {
                        continue;
                    }
                    // c1=1 or c2=2 or c3=3
                    let columns: Option<Vec<&String>> = disjuncts
                        .iter()
                        .map(|disjunct| flatten_col_eq_const(disjunct))
                        .collect();
                    let Some(columns) = columns else {
                        continue;
                    };
                    for name in columns {
                        dnf_columns.add(Column {
                            schema_name: schema.clone(),
                            table_name: table.clone(),
                            column_name: name.clone(),
                        });
                    }
                }
            }
            false
        },
        |_| true,
    );
    Ok(Some(dnf_columns))
}

fn is_constant(expression: &Expr) -> bool {
    matches!(
        expression,
        Expr::Int(_)
            | Expr::Decimal(_)
            | Expr::Float(_)
            | Expr::Hex(_)
            | Expr::Bit(_)
            | Expr::String(_)
            | Expr::RawString(_)
            | Expr::CharsetString { .. }
            | Expr::CharsetBinary { .. }
            | Expr::Null
            | Expr::Bool(_)
            | Expr::ParamMarker { .. }
    )
}

/// Go `flattenColEQConst`: the column of `col = const` or `const = col`.
fn flatten_col_eq_const(expression: &Expr) -> Option<&String> {
    match expression {
        Expr::Paren(inner) => flatten_col_eq_const(inner),
        Expr::Binary(BinaryOp::Eq, left, right) => {
            if is_constant(right) {
                column_name(left)
            } else if is_constant(left) {
                column_name(right)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Go `flattenCNF`: the conjuncts of an expression. An absent `where` is a
/// single absent conjunct, which no later step finds a disjunction in.
fn flatten_cnf(expression: Option<&Expr>) -> Vec<&Expr> {
    let mut conjuncts = Vec::new();
    if let Some(expression) = expression {
        flatten_cnf_into(expression, &mut conjuncts);
    }
    conjuncts
}

fn flatten_cnf_into<'a>(expression: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expression {
        Expr::Paren(inner) => flatten_cnf_into(inner, out),
        Expr::Binary(BinaryOp::LogicAnd, left, right) => {
            flatten_cnf_into(left, out);
            flatten_cnf_into(right, out);
        }
        other => out.push(other),
    }
}

/// Go `flattenDNF`.
fn flatten_dnf(expression: &Expr) -> Vec<&Expr> {
    let mut disjuncts = Vec::new();
    flatten_dnf_into(expression, &mut disjuncts);
    disjuncts
}

fn flatten_dnf_into<'a>(expression: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expression {
        Expr::Paren(inner) => flatten_dnf_into(inner, out),
        Expr::Binary(BinaryOp::LogicOr, left, right) => {
            flatten_dnf_into(left, out);
            flatten_dnf_into(right, out);
        }
        other => out.push(other),
    }
}

/// Go `RestoreSchemaName`.
pub(crate) fn restore_schema_name(
    default_schema: &str,
    queries: &QuerySet,
    ignore_error: bool,
) -> Result<QuerySet> {
    let mut restored = QuerySet::new();
    for mut query in queries.to_list() {
        if query.schema_name.is_empty() {
            query.schema_name = default_schema.to_owned();
        }
        let statement = match tidb_parser::parse(&query.text) {
            Ok(statement) => statement,
            Err(error) => {
                if ignore_error {
                    continue;
                }
                return Err(advisor_error(format!(
                    "invalid query: {}, err: {}",
                    query.text,
                    parse_failure_text(&error, &query.text)
                )));
            }
        };
        query.text = tidb_parser::util_parser::restore_with_default_db(
            &statement,
            &query.schema_name,
            &query.text,
        );
        restored.add(query);
    }
    Ok(restored)
}

/// Go `FilterInvalidQueries`: drops the queries the optimizer cannot cost.
pub(crate) fn filter_invalid_queries(
    optimizer: &mut dyn Optimizer,
    queries: &QuerySet,
    ignore_error: bool,
) -> Result<QuerySet> {
    let mut valid = QuerySet::new();
    for query in queries.to_list() {
        match optimizer.query_plan_cost(&query.text, &[]) {
            Ok(_) => valid.add(query),
            Err(error) => {
                if ignore_error {
                    continue;
                }
                return Err(error);
            }
        }
    }
    Ok(valid)
}

/// Go `FilterSQLAccessingSystemTables`.
pub(crate) fn filter_sql_accessing_system_tables(
    queries: &QuerySet,
    ignore_error: bool,
) -> Result<QuerySet> {
    let mut kept = QuerySet::new();
    for query in queries.to_list() {
        let names = match collect_table_names_from_query(&query.schema_name, &query.text) {
            Ok(names) => names,
            Err(error) => {
                if ignore_error {
                    continue;
                }
                return Err(error);
            }
        };
        if names.is_empty() {
            // `select @@some_var` or `select some_func()`.
            continue;
        }
        let accesses_system_table = names.iter().any(|name| {
            let schema = name.split('.').next().unwrap_or("").go_to_lower();
            tidb_metadef::db::is_mem_db(&schema) || tidb_metadef::db::is_system_db(&schema)
        });
        if !accesses_system_table {
            kept.add(query);
        }
    }
    Ok(kept)
}

/// Go `CollectIndexableColumnsForQuerySet`: every column in any range filter,
/// order-by or group-by.
pub(crate) fn collect_indexable_columns_for_query_set(
    optimizer: &mut dyn Optimizer,
    queries: &QuerySet,
) -> Result<ColumnSet> {
    let mut indexable = ColumnSet::new();
    for query in queries.to_list() {
        for column in collect_indexable_columns_from_query(&query, optimizer)?.to_list() {
            indexable.add(column);
        }
    }
    Ok(indexable)
}

/// Go `CollectIndexableColumnsFromQuery`.
pub(crate) fn collect_indexable_columns_from_query(
    query: &Query,
    optimizer: &mut dyn Optimizer,
) -> Result<ColumnSet> {
    let table_names = collect_table_names_from_query(&query.schema_name, &query.text)?;
    let mut possible_schemas: Vec<String> = vec![query.schema_name.clone()];
    for name in &table_names {
        let schema = name.split('.').next().unwrap_or("").go_to_lower();
        if !possible_schemas.contains(&schema) {
            possible_schemas.push(schema);
        }
    }

    let mut statement = parse_one_sql(&query.text)?;
    // The columns each referenced expression names: schema (if written) and
    // lower-cased column name.
    let mut referenced: Vec<(Option<String>, String)> = Vec::new();
    let mut collect = |expression: &Expr| {
        if let Expr::Column(path) = expression {
            if let Some(name) = path.last() {
                let schema = if path.len() >= 3 {
                    Some(go_to_lower(&path[path.len() - 3]))
                } else {
                    None
                };
                referenced.push((schema, go_to_lower(name)));
            }
        }
    };
    visit_node(
        &mut statement,
        |node| {
            if let Some(item) = node.downcast_ref::<GroupByItem>() {
                // group by {col}
                collect(&item.expr);
                return true;
            }
            if let Some(item) = node.downcast_ref::<OrderItem>() {
                // order by {col}
                collect(&item.expr);
                return true;
            }
            if let Some(expression) = node.downcast_ref::<Expr>() {
                match expression {
                    // {col} between ? and ?
                    Expr::Between { expr, .. }
                    // {col} in (?, ?, ...)
                    | Expr::In { expr, .. }
                    | Expr::InSubquery { expr, .. } => collect(expr),
                    // range predicates like `{col} > ?`
                    Expr::Binary(
                        BinaryOp::Eq | BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge,
                        left,
                        right,
                    ) => {
                        collect(left);
                        collect(right);
                    }
                    _ => {}
                }
            }
            false
        },
        |_| true,
    );

    let mut columns = ColumnSet::new();
    for (schema, name) in referenced {
        let schema_names: Vec<String> = match schema {
            Some(schema) => vec![schema],
            None => possible_schemas.clone(),
        };
        let mut possible_columns = Vec::new();
        for schema_name in &schema_names {
            // Go logs and skips a schema whose columns it cannot read.
            if let Ok(found) = optimizer.possible_columns(schema_name, &name) {
                possible_columns.extend(found);
            }
        }
        for column in possible_columns {
            let Ok(column_type) = optimizer.column_type(&column) else {
                continue;
            };
            if !is_indexable_column_type(&column_type) {
                continue;
            }
            columns.add(column);
        }
    }
    Ok(columns)
}

/// Go `isIndexableColumnType`.
fn is_indexable_column_type(field_type: &FieldType) -> bool {
    use tidb_datatype::FieldTypeCode as Code;
    match field_type.code() {
        Code::Tiny
        | Code::Short
        | Code::Int24
        | Code::Long
        | Code::LongLong
        | Code::Year
        | Code::Float
        | Code::Double
        | Code::NewDecimal
        | Code::Duration
        | Code::Date
        | Code::Datetime
        | Code::Timestamp => true,
        Code::Varchar | Code::String | Code::VarString => field_type.flen() <= 512,
        _ => false,
    }
}
