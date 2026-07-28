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

//! `CREATE VIEW` / `DROP VIEW` execution.
//!
//! Go stores a view as a `model.TableInfo` carrying a `model.ViewInfo`, whose
//! `SelectStmt` field holds the definition *canonicalized*: the planner
//! resolves the body once, then restores it with every select field
//! explicitly aliased and every table reference schema-qualified. That
//! canonical text is what `SHOW CREATE VIEW` and
//! `information_schema.views.VIEW_DEFINITION` print, and what a read of the
//! view re-plans. This module reproduces that canonicalization directly from
//! the parsed statement.
//!
//! A body may be a set operation or contain derived tables; both are
//! canonicalized term by term, exactly as Go does (captured: a `UNION` body
//! restores each term's fields aliased and each term's tables qualified, and
//! a derived table's own subquery is canonicalized inside its parentheses).
//!
//! NOT MODELLED (documented): privileges (the definer is recorded but never
//! checked), enforcing `WITH CHECK OPTION` on write-through (the mode is
//! stored and reported, and writes through a view are refused outright, which
//! is the only place the check would apply), and `ALGORITHM = MERGE`'s effect
//! on planning -- reading a view always materializes its body here.

use crate::driver::{Catalog, DriverError, ViewDef};
use crate::SchemaErrorKind;
use tidb_ast::{
    CreateViewStmt, Expr, JoinNode, QueryStmt, SelectField, SelectFieldList, SelectStmt,
    SetOprTermBody, Stmt,
};
use tidb_datatype::FieldType;

/// Creates (or replaces) a view.
///
/// The body is resolved now, as Go does: a `CREATE VIEW` over a missing table
/// or column fails at creation rather than at the first read.
pub fn run_create_view_in(
    create: &CreateViewStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    // `CREATE VIEW` over an existing name is Go's ErrTableExists whether the
    // name belongs to a table or to another view; `OR REPLACE` overwrites.
    if catalog.contains_in(&database, &name) && !create.or_replace {
        return Err(DriverError::Schema(SchemaErrorKind::TableExists(format!(
            "{database}.{name}"
        ))));
    }

    // `CREATE OR REPLACE VIEW v AS SELECT ... FROM v` is Go's `ErrTableNotExists`,
    // not a self-referencing view: the name being defined is hidden from its
    // own body. Resolving against a catalog without it reproduces that, and
    // makes a directly recursive view impossible to build in the first place.
    let mut hidden;
    let resolving = if create.or_replace {
        hidden = catalog.clone();
        hidden.drop_table_in(&database, &name);
        &hidden
    } else {
        &*catalog
    };
    let select_sql = canonical_view_query(&create.query, resolving, &database)?;
    // Running the canonical body both validates it and settles the output
    // column types the view reports to DESCRIBE and SHOW CREATE VIEW.
    let (body_columns, _) =
        crate::driver::run_select_meta_in(&select_sql, resolving, &database, ctx)?;
    let columns = match create.columns.len() {
        0 => body_columns,
        n if n == body_columns.len() => create
            .columns
            .iter()
            .zip(&body_columns)
            .map(|(name, (_, ft))| (name.clone(), ft.clone()))
            .collect(),
        // Go `ErrViewWrongList` (1353).
        _ => return Err(DriverError::ViewWrongList),
    };

    let view = ViewDef {
        name: name.clone(),
        columns,
        select_sql,
        // This tier authenticates no user, so the definer is the empty
        // identity -- which is exactly what a mock-store TiDB records too.
        definer_user: create.definer.user.clone(),
        definer_host: create.definer.host.clone(),
        algorithm: create.algorithm.sql().to_owned(),
        security: create.security.sql().to_owned(),
        // Go records a check option on every view, `CASCADED` by default:
        // captured, `information_schema.views.CHECK_OPTION` reads CASCADED
        // for a view written without one at all.
        check_option: create.check_option.sql().to_owned(),
    };
    catalog.register_view_in(&database, &name, view);
    Ok(())
}

/// Drops views, refusing to touch a base table of the same name.
pub fn run_drop_view_in(
    if_exists: bool,
    names: &[Vec<String>],
    catalog: &mut Catalog,
    current_db: &str,
) -> Result<(), DriverError> {
    let mut missing: Option<String> = None;
    for path in names {
        let (database, name) = crate::driver::split_table_path_pub(path, current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        match catalog.table_in(&database, &name) {
            // Go raises ErrWrongObject immediately, even under IF EXISTS: the
            // name exists, it is simply the other kind of object.
            Some(entry) if !entry.is_view() => {
                return Err(DriverError::Schema(SchemaErrorKind::NotView(format!(
                    "{database}.{name}"
                ))))
            }
            Some(_) => {
                catalog.drop_table_in(&database, &name);
            }
            None => {
                if missing.is_none() {
                    missing = Some(format!("{database}.{name}"));
                }
            }
        }
    }
    match missing {
        Some(name) if !if_exists => Err(DriverError::Schema(SchemaErrorKind::BadTable(name))),
        _ => Ok(()),
    }
}

/// One `FROM` table of a view body, after qualification.
struct BodyTable {
    /// The path a column reference to this table is written with: the alias
    /// when one was written, otherwise `db`.`table`.
    qualifier: Vec<String>,
    columns: Vec<String>,
}

/// Canonicalizes a view body and restores it to the text Go stores.
///
/// A set operation canonicalizes term by term; the statement-level
/// `ORDER BY`/`LIMIT` and a nested term's parentheses restore unchanged,
/// which is what Go prints (captured).
fn canonical_view_query(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Result<String, DriverError> {
    let canonical = canonical_query(query, catalog, current_db)?;
    Ok(Stmt::Query(tidb_ast::NodeBox::new(canonical)).restore())
}

/// The canonical form of a view body, as an AST.
fn canonical_query(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Result<QueryStmt, DriverError> {
    Ok(match query {
        QueryStmt::Select(select) => {
            QueryStmt::Select(Box::new(canonical_select(select, catalog, current_db)?))
        }
        QueryStmt::SetOpr(set_opr) => {
            let mut set_opr = set_opr.clone();
            for term in &mut set_opr.terms {
                term.body = match &term.body {
                    SetOprTermBody::Select(select) => SetOprTermBody::Select(Box::new(
                        canonical_select(select, catalog, current_db)?,
                    )),
                    SetOprTermBody::Nested(nested) => {
                        let QueryStmt::SetOpr(nested) = canonical_query(
                            &QueryStmt::SetOpr(nested.clone()),
                            catalog,
                            current_db,
                        )?
                        else {
                            unreachable!("a set operation canonicalizes to a set operation");
                        };
                        SetOprTermBody::Nested(nested)
                    }
                };
            }
            QueryStmt::SetOpr(set_opr)
        }
    })
}

/// The output column names of a canonicalized body: every field is aliased by
/// then, and a set operation takes the names of its first term (Go's
/// `buildSetOpr`, and what the capture shows a `UNION` view report).
fn query_output_names(query: &QueryStmt) -> Vec<String> {
    match query {
        QueryStmt::Select(select) => select
            .fields
            .fields()
            .iter()
            .map(|field| match field {
                SelectField::Expr {
                    alias: Some(alias), ..
                } => alias.clone(),
                _ => String::new(),
            })
            .collect(),
        QueryStmt::SetOpr(set_opr) => match set_opr.terms.first().map(|term| &term.body) {
            Some(SetOprTermBody::Select(select)) => {
                query_output_names(&QueryStmt::Select(select.clone()))
            }
            Some(SetOprTermBody::Nested(nested)) => {
                query_output_names(&QueryStmt::SetOpr(nested.clone()))
            }
            None => Vec::new(),
        },
    }
}

/// Canonicalizes one `SELECT` of a view body the way Go stores it:
/// schema-qualified table references, `*` expanded to explicit columns, and
/// every field aliased.
fn canonical_select(
    select: &SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Result<SelectStmt, DriverError> {
    let mut select = select.clone();
    let mut tables = Vec::new();
    if let Some(join) = &mut select.from {
        qualify_join_node(&mut join.left, catalog, current_db, &mut tables)?;
        if let Some(right) = &mut join.right {
            qualify_join_node(right, catalog, current_db, &mut tables)?;
        }
    }

    let mut fields = Vec::new();
    for (index, field) in select.fields.fields().iter().enumerate() {
        match field {
            SelectField::Wildcard(path) => {
                let qualifier = path.last();
                let mut matched = false;
                for table in &tables {
                    if let Some(qualifier) = qualifier {
                        let visible = table
                            .qualifier
                            .last()
                            .expect("a qualifier always has a last segment");
                        if !visible.eq_ignore_ascii_case(qualifier) {
                            continue;
                        }
                    }
                    matched = true;
                    for column in &table.columns {
                        let mut path = table.qualifier.clone();
                        path.push(column.clone());
                        fields.push(SelectField::Expr {
                            expr: Expr::Column(path),
                            alias: Some(column.clone()),
                        });
                    }
                }
                if !matched {
                    return Err(DriverError::Unsupported(
                        "a qualified wildcard names no table in the view's FROM",
                    ));
                }
            }
            SelectField::Expr { expr, alias } => {
                let alias = alias
                    .clone()
                    .unwrap_or_else(|| default_field_name(expr, select_text(&select, index)));
                fields.push(SelectField::Expr {
                    expr: expr.clone(),
                    alias: Some(alias),
                });
            }
        }
    }
    select.fields = SelectFieldList::from(fields);
    Ok(select)
}

/// The name a field takes when no `AS` was written: a column reference keeps
/// its column name, anything else keeps the text it was WRITTEN with, which
/// is Go's `SelectField.Text` -- `count(*)` names the column `count(*)` even
/// though the expression restores as `COUNT(1)`.
fn default_field_name(expr: &Expr, written: Option<&str>) -> String {
    match expr {
        Expr::Column(path) => path.last().cloned().unwrap_or_default(),
        other => written.map_or_else(|| other.restore(), str::to_owned),
    }
}

/// The source text field `index` was written with, when the parser kept it.
fn select_text(select: &SelectStmt, index: usize) -> Option<&str> {
    select
        .fields
        .text(index)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
}

/// Schema-qualifies every table reference of a `FROM` tree, collecting each
/// table's columns for wildcard expansion.
fn qualify_join_node(
    node: &mut JoinNode,
    catalog: &Catalog,
    current_db: &str,
    tables: &mut Vec<BodyTable>,
) -> Result<(), DriverError> {
    match node {
        JoinNode::Table(table_ref) => {
            let (database, name) =
                crate::driver::split_table_path_pub(&table_ref.name, current_db)?;
            let (database, name) = (database.to_owned(), name.to_owned());
            let entry = catalog.table_in(&database, &name).ok_or_else(|| {
                DriverError::Schema(SchemaErrorKind::UnknownTable(format!("{database}.{name}")))
            })?;
            let columns = entry
                .column_list()
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            let qualifier = match &table_ref.alias {
                Some(alias) => vec![alias.clone()],
                None => vec![database.clone(), name.clone()],
            };
            table_ref.name = vec![database, name];
            tables.push(BodyTable { qualifier, columns });
            Ok(())
        }
        JoinNode::Join(join) => {
            qualify_join_node(&mut join.left, catalog, current_db, tables)?;
            if let Some(right) = &mut join.right {
                qualify_join_node(right, catalog, current_db, tables)?;
            }
            Ok(())
        }
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            if *lateral || !column_names.is_empty() {
                return Err(DriverError::Unsupported(
                    "a LATERAL derived table is not supported yet",
                ));
            }
            // Captured from Go: an alias-less derived table in a view body is
            // ErrDerivedMustHaveAlias, the same error a plain SELECT reports.
            let alias = alias.clone().filter(|alias| !alias.is_empty());
            let Some(alias) = alias else {
                return Err(DriverError::DerivedMustHaveAlias);
            };
            // The subquery is canonicalized in place, so the stored body
            // carries the qualified, fully aliased inner SELECT too.
            let canonical = canonical_query(subquery, catalog, current_db)?;
            let columns = query_output_names(&canonical);
            **subquery = canonical;
            tables.push(BodyTable {
                qualifier: vec![alias],
                columns,
            });
            Ok(())
        }
    }
}

/// The columns a view reports today: its stored names, paired with the types
/// its body resolves to *now*.
///
/// Go re-plans a view's body for every metadata answer (`tryFillViewColumnType`
/// for `DESCRIBE`/`SHOW COLUMNS`, and the same fill for
/// `information_schema.columns`), so an `ALTER TABLE base MODIFY COLUMN`
/// underneath a view shows through on the next read -- captured: a view over
/// `bigint`/`varchar(10)` reports `varchar(32)`/`varchar(64)` once the base
/// columns are altered, with no touch to the view itself.
///
/// A body that no longer resolves is the caller's error to shape: Go reports
/// the body's own failure from `DESCRIBE` and omits the view from
/// `information_schema.columns` entirely (both captured).
pub fn view_column_list(
    view: &ViewDef,
    database: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Vec<(String, FieldType)>, DriverError> {
    let (body_columns, _) =
        crate::driver::run_select_meta_in(&view.select_sql, catalog, database, ctx)?;
    if body_columns.len() != view.columns.len() {
        return Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(format!(
            "{database}.{}",
            view.name
        ))));
    }
    Ok(view
        .columns
        .iter()
        .zip(&body_columns)
        .map(|((name, _), (_, field_type))| (name.clone(), field_type.clone()))
        .collect())
}
