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

//! The names an `ON DUPLICATE KEY UPDATE` assignment may read, as Go's
//! `buildInsert` builds them (`pkg/planner/core/planbuilder.go`).
//!
//! An assignment's value is rewritten over `Insert.Names4OnDuplicate`: the
//! target table's columns (the row already stored), then -- for `INSERT ...
//! SELECT` -- the source columns the SELECT did not output but the
//! assignments name, which `buildSelectPlanOfInsert` appends to the SELECT's
//! fields, then the SELECT's outputs placed at the target column each one
//! fills (the row the insert would have written). `FindFieldName` matches a
//! name against all of them at once, so `a = b` over `t1(a, b)` filled from
//! `t2(a, b)` is ambiguous, while `VALUES(col)` reads the target's columns
//! only.

use super::{Catalog, DriverError};

/// Go `types.FieldName`, lowercased as `FindFieldName` compares it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct FieldName {
    db: String,
    tbl: String,
    col: String,
}

impl FieldName {
    fn new(db: &str, tbl: &str, col: &str) -> Self {
        Self {
            db: lower(db),
            tbl: lower(tbl),
            col: lower(col),
        }
    }
}

fn lower(value: &str) -> String {
    tidb_util::stringutil::go_to_lower(value)
}

/// What a column reference in an assignment value reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OnDuplicateBinding {
    /// The stored row's column at this offset.
    Target(usize),
    /// The SELECT's appended field at this position past its own outputs.
    Extra(usize),
    /// The would-be-inserted row's column at this offset.
    NewRow(usize),
}

/// Go `Insert.Names4OnDuplicate` with the positions each part reads.
#[derive(Clone, Debug, Default)]
pub(crate) struct OnDuplicateScope {
    names: Vec<FieldName>,
    bindings: Vec<OnDuplicateBinding>,
    target_len: usize,
    /// How many of the SELECT's output columns fill the insert; the rest are
    /// the appended fields. `None` without a source or an extension.
    actual_col_len: Option<usize>,
}

impl OnDuplicateScope {
    /// Builds the scope for `insert` writing `column_list` of
    /// `target_db`.`target_table`, its SELECT outputs filling
    /// `target_offsets` in order.
    pub(crate) fn build(
        insert: &tidb_ast::InsertStmt,
        catalog: &Catalog,
        current_db: &str,
        column_list: &[(String, tidb_datatype::FieldType)],
        target_db: &str,
        target_table: &str,
        target_offsets: &[usize],
    ) -> Self {
        let mut scope = Self::default();
        for (offset, (column, _)) in column_list.iter().enumerate() {
            scope.names.push(FieldName::new(target_db, target_table, column));
            scope.bindings.push(OnDuplicateBinding::Target(offset));
        }
        scope.target_len = scope.names.len();
        let Some(source) = insert.source.as_deref() else {
            return scope;
        };
        let extended = extended_source(insert, catalog, current_db);
        let (query, actual_col_len) = match &extended {
            Some((query, len)) => (query, Some(*len)),
            None => (source, None),
        };
        scope.actual_col_len = actual_col_len;
        let output = query_output_names(query, catalog, current_db);
        let filled = actual_col_len.unwrap_or(output.len());
        for (position, name) in output.iter().enumerate().skip(filled) {
            scope.names.push(name.clone());
            scope.bindings.push(OnDuplicateBinding::Extra(position - filled));
        }
        for (name, &offset) in output.iter().zip(target_offsets).take(filled) {
            scope.names.push(name.clone());
            scope.bindings.push(OnDuplicateBinding::NewRow(offset));
        }
        scope
    }

    /// How many leading SELECT output columns fill the insert row, when the
    /// SELECT was extended with appended fields.
    pub(crate) const fn actual_col_len(&self) -> Option<usize> {
        self.actual_col_len
    }

    /// Go `FindFieldName` over the whole scope.
    pub(crate) fn resolve(&self, path: &[String]) -> Result<Option<OnDuplicateBinding>, DriverError> {
        find_field_name(&self.names, path)
            .map(|found| found.map(|index| self.bindings[index]))
            .map_err(|()| DriverError::AmbiguousColumnInClause {
                column: path.join("."),
                clause: "field list".to_owned(),
            })
    }

    /// `VALUES(col)`: Go's rewriter reads `insertPlan.TableColNames` only.
    pub(crate) fn resolve_values(&self, path: &[String]) -> Result<usize, DriverError> {
        let unknown = || DriverError::UnknownColumnInClause {
            column: path.join("."),
            clause: "field list".to_owned(),
        };
        match find_field_name(&self.names[..self.target_len], path) {
            Ok(Some(index)) => match self.bindings[index] {
                OnDuplicateBinding::Target(offset) => Ok(offset),
                _ => Err(unknown()),
            },
            Ok(None) => Err(unknown()),
            Err(()) => Err(DriverError::AmbiguousColumnInClause {
                column: path.join("."),
                clause: "field list".to_owned(),
            }),
        }
    }

    /// Go `ResolveOnDuplicate`'s rewrite of every assignment value: a name
    /// the scope does not hold is 1054 and one it holds twice 1052, at plan
    /// time, whether or not any row conflicts.
    pub(crate) fn validate(&self, assignments: &[tidb_ast::Assignment]) -> Result<(), DriverError> {
        for assignment in assignments {
            // The assigned column first, against the target's columns.
            self.resolve_values(&assignment.col)?;
            let mut references = References::default();
            collect_references(&assignment.value, &mut references);
            for path in &references.columns {
                if self.resolve(path)?.is_none() {
                    return Err(DriverError::UnknownColumnInClause {
                        column: path.join("."),
                        clause: "field list".to_owned(),
                    });
                }
            }
            for path in &references.values {
                self.resolve_values(path)?;
            }
        }
        Ok(())
    }
}

/// Go `FindFieldName` (`pkg/expression/simple_rewriter.go`): an empty
/// qualifier matches any; two matches are ambiguous.
fn find_field_name(names: &[FieldName], path: &[String]) -> Result<Option<usize>, ()> {
    let (db, tbl, col) = match path {
        [col] => (String::new(), String::new(), lower(col)),
        [tbl, col] => (String::new(), lower(tbl), lower(col)),
        [db, tbl, col] => (lower(db), lower(tbl), lower(col)),
        _ => return Ok(None),
    };
    let mut found = None;
    for (index, name) in names.iter().enumerate() {
        if name.col != col
            || !(db.is_empty() || db == name.db)
            || !(tbl.is_empty() || tbl == name.tbl)
        {
            continue;
        }
        if found.is_some() {
            return Err(());
        }
        found = Some(index);
    }
    Ok(found)
}

/// The column references of an assignment value, outside subqueries: plain
/// references and `VALUES(col)` arguments.
#[derive(Default)]
struct References {
    columns: Vec<Vec<String>>,
    values: Vec<Vec<String>>,
}

fn collect_references(expr: &tidb_ast::Expr, references: &mut References) {
    use tidb_ast::Visitable;
    struct Collector<'a> {
        references: &'a mut References,
    }
    impl tidb_ast::Visitor for Collector<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<tidb_ast::QueryStmt>() {
                return true;
            }
            let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() else {
                return false;
            };
            match expr {
                tidb_ast::Expr::Subquery(_) => true,
                tidb_ast::Expr::Func { name, args, .. } if name.eq_ignore_ascii_case("values") => {
                    if let Some(tidb_ast::Expr::Column(path)) = args.first() {
                        self.references.values.push(path.clone());
                    }
                    true
                }
                tidb_ast::Expr::Column(path) => {
                    self.references.columns.push(path.clone());
                    true
                }
                _ => false,
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut expr = expr.clone();
    expr.accept(&mut Collector { references });
}

/// Go `buildSelectPlanOfInsert`'s extension: an `INSERT ... SELECT` (a plain
/// SELECT, no aggregation, no wildcard) whose ON DUPLICATE assignments name a
/// column that is neither the target's nor a SELECT output gets that column
/// appended as a SELECT field, so the assignment can read it (`insert into a
/// select x from b on duplicate key update a.x = b.y`). Returns the extended
/// query and the original field count, or `None` when nothing is appended.
/// Go appends in map order; this keeps first-reference order.
pub(crate) fn extended_source(
    insert: &tidb_ast::InsertStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Option<(tidb_ast::QueryStmt, usize)> {
    if insert.on_duplicate.is_empty() {
        return None;
    }
    let tidb_ast::QueryStmt::Select(select) = insert.source.as_deref()? else {
        return None;
    };
    if detect_select_agg(select)
        || select
            .fields
            .fields()
            .iter()
            .any(|field| matches!(field, tidb_ast::SelectField::Wildcard(_)))
    {
        return None;
    }
    let (target_db, target_table) = match insert.table.as_slice() {
        [table] => (current_db.to_owned(), table.clone()),
        [db, table] => (db.clone(), table.clone()),
        _ => return None,
    };
    let target_columns = catalog
        .get_in(&target_db, &target_table)
        .map(|entry| entry.column_list())
        .unwrap_or_default();
    let target_names = target_columns
        .iter()
        .map(|(column, _)| FieldName::new(&target_db, &target_table, column))
        .collect::<Vec<_>>();
    let mut references = References::default();
    for assignment in &insert.on_duplicate {
        collect_references(&assignment.value, &mut references);
    }
    let mut seen: Vec<Vec<String>> = Vec::new();
    let mut appended = Vec::new();
    for path in references.values.iter().chain(&references.columns) {
        if seen.contains(path) {
            continue;
        }
        seen.push(path.clone());
        let in_target = matches!(find_field_name(&target_names, path), Ok(Some(_)) | Err(()));
        if in_target || matches_select_field(select, path) {
            continue;
        }
        appended.push(path.clone());
    }
    if appended.is_empty() {
        return None;
    }
    let actual_col_len = select.fields.fields().len();
    let mut select = select.as_ref().clone();
    for path in appended {
        select.fields.push(tidb_ast::SelectField::Expr {
            expr: tidb_ast::Expr::Column(path),
            alias: None,
        });
    }
    Some((tidb_ast::QueryStmt::Select(Box::new(select)), actual_col_len))
}

/// Go's loose field match in `buildSelectPlanOfInsert`: a column field whose
/// qualifiers do not conflict with the reference's.
fn matches_select_field(select: &tidb_ast::SelectStmt, path: &[String]) -> bool {
    let split = |path: &[String]| -> (String, String, String) {
        match path {
            [col] => (String::new(), String::new(), lower(col)),
            [tbl, col] => (String::new(), lower(tbl), lower(col)),
            [db, tbl, col, ..] => (lower(db), lower(tbl), lower(col)),
            [] => Default::default(),
        }
    };
    let (db, tbl, col) = split(path);
    select.fields.fields().iter().any(|field| {
        let tidb_ast::SelectField::Expr {
            expr: tidb_ast::Expr::Column(field_path),
            ..
        } = field
        else {
            return false;
        };
        let (field_db, field_tbl, field_col) = split(field_path);
        (db.is_empty() || field_db.is_empty() || db == field_db)
            && (tbl.is_empty() || field_tbl.is_empty() || tbl == field_tbl)
            && col == field_col
    })
}

/// Go `detectSelectAgg`: GROUP BY, or an aggregate in a field, HAVING or
/// ORDER BY.
fn detect_select_agg(select: &tidb_ast::SelectStmt) -> bool {
    if !select.group_by.is_empty() {
        return true;
    }
    select.fields.fields().iter().any(|field| match field {
        tidb_ast::SelectField::Expr { expr, .. } => has_agg(expr),
        tidb_ast::SelectField::Wildcard(_) => false,
    }) || select.having.as_ref().is_some_and(has_agg)
        || select.order_by.iter().any(|item| has_agg(&item.expr))
}

/// Go `ast.HasAggFlag`: an aggregate outside any subquery.
fn has_agg(expr: &tidb_ast::Expr) -> bool {
    use tidb_ast::Visitable;
    struct Finder(bool);
    impl tidb_ast::Visitor for Finder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if node.is::<tidb_ast::QueryStmt>() {
                return true;
            }
            match node.downcast_mut::<tidb_ast::Expr>() {
                Some(tidb_ast::Expr::Subquery(_)) => true,
                Some(tidb_ast::Expr::Aggregate { .. } | tidb_ast::Expr::GroupConcat { .. }) => {
                    self.0 = true;
                    true
                }
                _ => false,
            }
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            !self.0
        }
    }
    let mut finder = Finder(false);
    let mut expr = expr.clone();
    expr.accept(&mut finder);
    finder.0
}

/// One FROM source and the columns it exposes.
struct SourceTable {
    db: String,
    name: String,
    columns: Vec<String>,
}

fn from_tables(
    join: Option<&tidb_ast::Join>,
    catalog: &Catalog,
    current_db: &str,
    out: &mut Vec<SourceTable>,
) {
    let Some(join) = join else {
        return;
    };
    from_node(&join.left, catalog, current_db, out);
    if let Some(right) = &join.right {
        from_node(right, catalog, current_db, out);
    }
}

fn from_node(
    node: &tidb_ast::JoinNode,
    catalog: &Catalog,
    current_db: &str,
    out: &mut Vec<SourceTable>,
) {
    match node {
        tidb_ast::JoinNode::Table(table) => {
            let (db, name) = match table.name.as_slice() {
                [name] => (current_db.to_owned(), name.clone()),
                [db, name] => (db.clone(), name.clone()),
                _ => return,
            };
            let columns = catalog
                .get_in(&db, &name)
                .map(|entry| {
                    entry
                        .column_list()
                        .into_iter()
                        .map(|(column, _)| column)
                        .collect()
                })
                .unwrap_or_default();
            out.push(SourceTable {
                db,
                name: table.alias.clone().unwrap_or(name),
                columns,
            });
        }
        tidb_ast::JoinNode::Derived {
            subquery, alias, ..
        } => {
            let columns = query_output_names(subquery, catalog, current_db)
                .into_iter()
                .map(|name| name.col)
                .collect();
            out.push(SourceTable {
                db: String::new(),
                name: alias.clone().unwrap_or_default(),
                columns,
            });
        }
        tidb_ast::JoinNode::Join(join) => from_tables(Some(join), catalog, current_db, out),
    }
}

/// The output names of a query, as Go's projection names them: a column
/// field keeps its table and database, an alias renames its column, and a
/// set operation answers with its first SELECT's names.
fn query_output_names(
    query: &tidb_ast::QueryStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Vec<FieldName> {
    match query {
        tidb_ast::QueryStmt::Select(select) => select_output_names(select, catalog, current_db),
        tidb_ast::QueryStmt::SetOpr(set_opr) => set_opr
            .terms
            .first()
            .map(|term| match &term.body {
                tidb_ast::SetOprTermBody::Select(select) => {
                    select_output_names(select, catalog, current_db)
                }
                tidb_ast::SetOprTermBody::Nested(nested) => query_output_names(
                    &tidb_ast::QueryStmt::SetOpr(nested.clone()),
                    catalog,
                    current_db,
                ),
            })
            .unwrap_or_default(),
    }
}

fn select_output_names(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
) -> Vec<FieldName> {
    let mut tables = Vec::new();
    from_tables(select.from.as_ref(), catalog, current_db, &mut tables);
    let mut names = Vec::new();
    for field in select.fields.fields() {
        match field {
            tidb_ast::SelectField::Wildcard(scope) => {
                let scope_tbl = scope.last().map(|tbl| lower(tbl));
                for table in &tables {
                    if scope_tbl.as_ref().is_some_and(|tbl| *tbl != lower(&table.name)) {
                        continue;
                    }
                    for column in &table.columns {
                        names.push(FieldName::new(&table.db, &table.name, column));
                    }
                }
            }
            tidb_ast::SelectField::Expr { expr, alias } => {
                let name = match expr {
                    tidb_ast::Expr::Column(path) => {
                        let (db, tbl, col) = match path.as_slice() {
                            [col] => (None, None, col),
                            [tbl, col] => (None, Some(tbl), col),
                            [db, tbl, col, ..] => (Some(db), Some(tbl), col),
                            [] => continue,
                        };
                        let table = tables.iter().find(|table| {
                            db.is_none_or(|db| lower(db) == lower(&table.db))
                                && tbl.is_none_or(|tbl| lower(tbl) == lower(&table.name))
                                && table.columns.iter().any(|column| lower(column) == lower(col))
                        });
                        let (db, tbl) = match table {
                            Some(table) => (table.db.clone(), table.name.clone()),
                            None => (
                                db.cloned().unwrap_or_default(),
                                tbl.cloned().unwrap_or_default(),
                            ),
                        };
                        FieldName::new(&db, &tbl, alias.as_deref().unwrap_or(col))
                    }
                    other => FieldName::new(
                        "",
                        "",
                        &alias.clone().unwrap_or_else(|| other.restore()),
                    ),
                };
                names.push(name);
            }
        }
    }
    names
}
