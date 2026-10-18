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

/// Resolves and publishes a view while retaining session-local name shadows.
pub fn run_create_view_in(
    create: &CreateViewStmt,
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let (database, name, view) = resolve_view_definition(create, catalog, current_db, ctx)?;
    // Go `CreateView` builds the view as a table of its column names, which
    // `createTableWithInfoJob` checks like any other table.
    crate::ddl::check_table_info_valid_extra(
        &name,
        view.columns.iter().map(|(column, _)| column.as_str()),
        0,
        catalog.table_column_count_limit(),
    )?;
    crate::ddl::bdr::submit(
        catalog,
        ctx,
        &database,
        &name,
        None,
        &[crate::ddl::bdr::SubmittedJob::new(
            tidb_model::ActionType::ACTION_CREATE_VIEW,
        )],
    )?;
    catalog.register_persistent_view_in(&database, &name, view)?;
    Ok(())
}

/// Go `checkForUserVariables` (`planbuilder.go:5291`), run before the view
/// body is built: any `ast.VariableExpr` -- a user or system variable read,
/// or an inline `@v := ...` assignment -- refuses the view.
fn view_body_has_variable(create: &CreateViewStmt) -> bool {
    struct Finder {
        found: bool,
    }
    impl tidb_ast::Visitor for Finder {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_mut::<tidb_ast::Expr>() {
                if matches!(
                    expr,
                    tidb_ast::Expr::UserVar(_)
                        | tidb_ast::Expr::SysVar { .. }
                        | tidb_ast::Expr::Assign { .. }
                ) {
                    self.found = true;
                    return true;
                }
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    use tidb_ast::Visitable;
    let mut query = (*create.query).clone();
    let mut finder = Finder { found: false };
    query.accept(&mut finder);
    finder.found
}

/// Every written table path in a `CREATE VIEW` body, including the ones
/// inside subqueries and CTEs -- Go reaches them all because `buildDataSource`
/// runs once per `TableName` node however deep it is.
///
/// A CTE's own name is in here too and resolves to nothing, which is
/// harmless: the caller only asks whether a path names a LOCAL temporary
/// table.
fn view_body_table_paths(create: &CreateViewStmt) -> Vec<Vec<String>> {
    struct Collector {
        paths: Vec<Vec<String>>,
    }
    impl tidb_ast::Visitor for Collector {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                self.paths.push(table_ref.name.clone());
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    use tidb_ast::Visitable;
    let mut query = (*create.query).clone();
    let mut collector = Collector { paths: Vec::new() };
    query.accept(&mut collector);
    collector.paths
}

/// Resolves a `CREATE VIEW` into the [`ViewDef`] it would register, without
/// registering it: the shared first half of [`run_create_view_in`], split out
/// so the cluster DDL route can validate and settle a view's columns against
/// this node's catalog before publishing the definition to the cluster's
/// meta — Go's shape, where `executeCreateView` preprocesses the body in the
/// executor and hands DDL a finished `TableInfo`.
///
/// Returns `(database, name, view)`.
pub fn resolve_view_definition(
    create: &CreateViewStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(String, String, ViewDef), DriverError> {
    if view_body_has_variable(create) {
        return Err(DriverError::ViewSelectVariable);
    }
    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    // Go `buildDataSource` (`pkg/planner/core/logical_plan_builder.go:4963`):
    // a view whose body names a LOCAL temporary table is refused with 1352.
    // The view definition is shared -- any session may read it -- while the
    // table it would read belongs to one connection and dies with it, so the
    // view would be unresolvable for everyone including its own creator after
    // reconnecting. A GLOBAL temporary table is NOT refused here: its schema
    // is a real, shared infoschema object, and every session simply reads its
    // own (empty) rows through it.
    for path in view_body_table_paths(create) {
        let Ok((referenced_db, referenced_name)) =
            crate::driver::split_table_path_pub(&path, current_db)
        else {
            continue;
        };
        if matches!(catalog.table_in(referenced_db, referenced_name),
            Some(crate::TableEntry::Kv(table))
                if table.temp_table_type() == tidb_model::TempTableType::LOCAL)
        {
            return Err(DriverError::ViewSelectTemporaryTable(
                referenced_name.to_owned(),
            ));
        }
    }

    // `CREATE OR REPLACE VIEW v AS SELECT ... FROM v` is Go's `ErrTableNotExists`,
    // not a self-referencing view: the name being defined is hidden from its
    // own body. Resolving against a catalog without it reproduces that, and
    // makes a directly recursive view impossible to build in the first place.
    // Go only hides a VIEW (`renamingViewName`); a base table of that name is
    // read like any other and the DDL then refuses it (1347).
    let replacing_view = matches!(
        catalog.table_in(&database, &name),
        Some(crate::TableEntry::View(_))
    );
    let mut renaming;
    let resolving = if create.or_replace && replacing_view {
        renaming = catalog.clone();
        renaming.set_renaming_view(&database, &name);
        &renaming
    } else {
        catalog
    };
    let select_sql = canonical_view_query(&create.query, resolving, current_db)?;
    // Planning the canonical body both validates it and settles the output
    // column types the view reports to DESCRIBE and SHOW CREATE VIEW; Go's
    // `PlanBuilder` builds the body's plan and reads its schema, and never
    // runs it (a view over a large table must not scan it at CREATE time).
    // Go buildProjection delays the new GROUP BY checker during CREATE VIEW.
    // The stored body is checked again in the querying session's context.
    let definition_ctx = if ctx.new_only_full_group_by_check() {
        ctx.clone().with_only_full_group_by(false)
    } else {
        ctx.clone()
    }
    .with_create_view(true);
    let body_columns =
        crate::driver::plan_select_meta_in(&select_sql, resolving, current_db, &definition_ctx)?;
    let columns = match create.columns.len() {
        // Go `adjustOverlongViewColname`: an unnamed output column whose name
        // is longer than 64 bytes is named `name_exp_<offset>`.
        0 => body_columns
            .into_iter()
            .enumerate()
            .map(|(offset, (name, field_type))| {
                if name.to_lowercase().len() > crate::ddl::MAX_IDENTIFIER_LENGTH {
                    (format!("name_exp_{}", offset + 1), field_type)
                } else {
                    (name, field_type)
                }
            })
            .collect(),
        n if n == body_columns.len() => create
            .columns
            .iter()
            .zip(&body_columns)
            .map(|(name, (_, ft))| (name.clone(), ft.clone()))
            .collect(),
        // Go `ErrViewWrongList` (1353).
        _ => return Err(DriverError::ViewWrongList),
    };

    // Go plans the body first, then CreateTableWithInfo checks the durable
    // target. A local table is neither a duplicate nor a replaceable view.
    check_view_target(
        &database,
        &name,
        create.or_replace,
        catalog
            .persistent_table_in(&database, &name)
            .map(crate::TableEntry::is_view),
    )?;

    let (definer_user, definer_host) = if create.definer.current_user {
        ctx.authenticated_identity().map_or_else(
            || (String::new(), String::new()),
            |(user, host)| (user.to_owned(), host.to_owned()),
        )
    } else {
        (create.definer.user.clone(), create.definer.host.clone())
    };
    let (character_set_client, collation_connection) = ctx.connection_charset_info();
    let view = ViewDef {
        name: name.clone(),
        columns,
        select_sql,
        definer_user,
        definer_host,
        character_set_client: character_set_client.to_owned(),
        collation_connection: collation_connection.to_owned(),
        algorithm: create.algorithm.sql().to_owned(),
        security: create.security.sql().to_owned(),
        // Go records a check option on every view, `CASCADED` by default:
        // captured, `information_schema.views.CHECK_OPTION` reads CASCADED
        // for a view written without one at all.
        check_option: create.check_option.sql().to_owned(),
        update_ts: 0,
        id: 0,
    };
    Ok((database, name, view))
}

/// Go CreateTableWithInfo's shared view-name admission, including its worker
/// recheck against the publication snapshot. `existing` describes the durable
/// object only; true means a view and false means another object kind.
pub fn check_view_target(
    database: &str,
    name: &str,
    or_replace: bool,
    existing: Option<bool>,
) -> Result<(), DriverError> {
    match existing {
        Some(_) if !or_replace => Err(DriverError::Schema(SchemaErrorKind::TableExists(format!(
            "{database}.{name}"
        )))),
        Some(false) => Err(DriverError::Schema(SchemaErrorKind::WrongObject {
            name: format!("{database}.{name}"),
            expected: "VIEW",
        })),
        _ => Ok(()),
    }
}

/// Drops views, refusing to touch a base table of the same name.
///
/// Returns the names that were not there. `IF EXISTS` does not discard them:
/// Go files one `Note 1051` per missing name, the same `ErrBadTable` it would
/// have raised (`pkg/ddl/executor.go`). Without `IF EXISTS` the whole list
/// becomes the error's text -- captured from `gorun`, `drop view nvA, nvB` is
/// `Unknown table 'test.nvA,test.nvB'` -- and the return is empty.
pub fn run_drop_view_in(
    if_exists: bool,
    names: &[Vec<String>],
    catalog: &mut Catalog,
    current_db: &str,
    // The statement's context: its `tidb_cdc_write_source` exempts the jobs
    // from BDR admission, and its job sink records them.
    ctx: &crate::StmtContext,
) -> Result<Vec<String>, DriverError> {
    use crate::ddl::{check_drop_object, drop_objects, DropObjectKind, DropObjectsError};
    let names = names
        .iter()
        .map(|path| {
            crate::driver::split_table_path_pub(path, current_db)
                .map(|(schema, name)| (schema.to_owned(), name.to_owned()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    drop_objects(&names, if_exists, |schema, name| {
        let Some(entry) = catalog.table_in(schema, name) else {
            return Ok(false);
        };
        let actual = if entry.is_view() {
            DropObjectKind::View
        } else {
            DropObjectKind::Other
        };
        check_drop_object(schema, name, DropObjectKind::View, actual, false)?;
        // Go `dropTableObject` submits one ActionDropView per view.
        crate::ddl::bdr::submit(
            catalog,
            ctx,
            schema,
            name,
            None,
            &[crate::ddl::bdr::SubmittedJob::new(
                tidb_model::ActionType::ACTION_DROP_VIEW,
            )],
        )?;
        Ok(catalog.drop_table_in(schema, name))
    })
    .map_err(|error| match error {
        DropObjectsError::Target(error) => error,
        DropObjectsError::Missing(names) => {
            DriverError::Schema(SchemaErrorKind::BadTable(names.join(",")))
        }
    })
}

/// One column a `FROM` clause exposes to an unqualified `*`, with the path a
/// reference to it is written with.
#[derive(Clone)]
struct OutColumn {
    qualifier: Vec<String>,
    name: String,
}

/// Go `coalesceCommonColumns`: the columns a join exposes to `*`. A
/// `USING (..)`/`NATURAL` join shows each common column once, ahead of the
/// rest -- taken from the left operand, from the right for a RIGHT join,
/// which then lists its own columns before the left's. Any other join is the
/// two operands side by side.
fn combine_join_output(
    join: &tidb_ast::Join,
    left: Vec<OutColumn>,
    right: Vec<OutColumn>,
) -> Vec<OutColumn> {
    let names: Vec<String> = if !join.using.is_empty() {
        join.using.clone()
    } else if join.natural {
        let mut common = Vec::new();
        for column in &left {
            if right.iter().any(|other| other.name.eq_ignore_ascii_case(&column.name))
                && !common
                    .iter()
                    .any(|seen: &String| seen.eq_ignore_ascii_case(&column.name))
            {
                common.push(column.name.clone());
            }
        }
        common
    } else {
        Vec::new()
    };
    if names.is_empty() {
        return left.into_iter().chain(right).collect();
    }
    let is_common =
        |column: &OutColumn| names.iter().any(|name| name.eq_ignore_ascii_case(&column.name));
    let source = if join.tp == tidb_ast::JoinType::Right { &right } else { &left };
    let mut output: Vec<OutColumn> = Vec::new();
    for name in &names {
        match source.iter().find(|column| column.name.eq_ignore_ascii_case(name)) {
            Some(column) => output.push(column.clone()),
            // A name the planner will reject; keep the operands whole.
            None => return left.into_iter().chain(right).collect(),
        }
    }
    let left_rest = left.iter().filter(|column| !is_common(column)).cloned();
    let right_rest = right.iter().filter(|column| !is_common(column)).cloned();
    if join.tp == tidb_ast::JoinType::Right {
        output.extend(right_rest.chain(left_rest));
    } else {
        output.extend(left_rest.chain(right_rest));
    }
    output
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
    // Go `checkCreateViewWithSelect` sets the lock of the body's SELECT, or of
    // each term of a set operation, to `SelectLockNone` before the text is
    // stored. A SelectStmt restores a non-nil lock info as a space followed by
    // nothing, so the stored text keeps a trailing space where the clause was.
    let mut query = query.clone();
    let mut locked_terms = Vec::new();
    match &mut query {
        QueryStmt::Select(select) => locked_terms.push(select.lock.take().is_some()),
        QueryStmt::SetOpr(set_opr) => {
            for term in &mut set_opr.terms {
                locked_terms.push(match &mut term.body {
                    SetOprTermBody::Select(select) => select.lock.take().is_some(),
                    SetOprTermBody::Nested(_) => false,
                });
            }
        }
    }
    let canonical = canonical_query_with(&query, catalog, current_db, &mut Vec::new(), true)?;
    let mut text = Stmt::Query(tidb_ast::NodeBox::new(canonical.clone())).restore();
    match &canonical {
        QueryStmt::Select(_) => {
            if locked_terms[0] {
                text.push(' ');
            }
        }
        QueryStmt::SetOpr(set_opr) => {
            let mut cursor = 0;
            for (term, locked) in set_opr.terms.iter().zip(&locked_terms) {
                let SetOprTermBody::Select(select) = &term.body else {
                    continue;
                };
                let term_text = Stmt::Query(tidb_ast::NodeBox::new(QueryStmt::Select(
                    select.clone(),
                )))
                .restore();
                let Some(found) = text[cursor..].find(&term_text) else {
                    continue;
                };
                cursor += found + term_text.len();
                if *locked {
                    text.insert(cursor, ' ');
                    cursor += 1;
                }
            }
        }
    }
    Ok(text)
}

/// One CTE visible to a view body. Go `tryBuildCTE` resolves an unqualified
/// table name to the innermost CTE of that name before any stored table, and
/// the CTE's output names carry its name and no schema (captured: `SELECT
/// `cte`.`a` AS `a` FROM `cte``).
#[derive(Clone)]
struct BodyCte {
    name: String,
    columns: Vec<String>,
}

/// The canonical form of a view body, as an AST. `ctes` is the stack of CTEs
/// visible here, restored on return.
fn canonical_query(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctes: &mut Vec<BodyCte>,
) -> Result<QueryStmt, DriverError> {
    canonical_query_with(query, catalog, current_db, ctes, false)
}

/// [`canonical_query`]; `as_view_schema` marks the query whose first SELECT
/// is Go's `findStmtAsViewSchema`, the one that names the view's columns.
fn canonical_query_with(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctes: &mut Vec<BodyCte>,
    as_view_schema: bool,
) -> Result<QueryStmt, DriverError> {
    Ok(match query {
        QueryStmt::Select(select) => QueryStmt::Select(Box::new(canonical_select(
            select,
            catalog,
            current_db,
            ctes,
            as_view_schema,
        )?)),
        QueryStmt::SetOpr(set_opr) => {
            let mut set_opr = set_opr.clone();
            let depth = ctes.len();
            let terms =
                canonical_with(set_opr.with.as_mut(), catalog, current_db, ctes).and_then(|()| {
                    canonical_set_opr_terms(&mut set_opr, catalog, current_db, ctes, as_view_schema)
                });
            ctes.truncate(depth);
            terms?;
            QueryStmt::SetOpr(set_opr)
        }
    })
}

fn canonical_set_opr_terms(
    set_opr: &mut tidb_ast::SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctes: &mut Vec<BodyCte>,
    as_view_schema: bool,
) -> Result<(), DriverError> {
    for (index, term) in set_opr.terms.iter_mut().enumerate() {
        let as_view_schema = as_view_schema && index == 0;
        term.body =
            match &term.body {
                SetOprTermBody::Select(select) => SetOprTermBody::Select(Box::new(
                    canonical_select(select, catalog, current_db, ctes, as_view_schema)?,
                )),
                SetOprTermBody::Nested(nested) => {
                    let QueryStmt::SetOpr(nested) = canonical_query_with(
                        &QueryStmt::SetOpr(nested.clone()),
                        catalog,
                        current_db,
                        ctes,
                        as_view_schema,
                    )?
                    else {
                        unreachable!("a set operation canonicalizes to a set operation");
                    };
                    SetOprTermBody::Nested(nested)
                }
            };
    }
    Ok(())
}

/// Canonicalizes a `WITH` clause's definitions in order, leaving each CTE on
/// `ctes` for the definitions after it and the query it prefixes. A
/// recursive definition sees itself, with its declared columns or its seed
/// term's output names, as Go's recursive part reads `cte.seedLP`'s names.
fn canonical_with(
    with: Option<&mut tidb_ast::WithClause>,
    catalog: &Catalog,
    current_db: &str,
    ctes: &mut Vec<BodyCte>,
) -> Result<(), DriverError> {
    let Some(with) = with else {
        return Ok(());
    };
    for cte in &mut with.ctes {
        let canonical = if with.recursive {
            let columns = if cte.columns.is_empty() {
                query_output_names(&canonical_seed(&cte.query, catalog, current_db, ctes)?)
            } else {
                cte.columns.clone()
            };
            ctes.push(BodyCte {
                name: cte.name.clone(),
                columns,
            });
            let canonical = canonical_query(&cte.query, catalog, current_db, ctes);
            ctes.pop();
            canonical?
        } else {
            canonical_query(&cte.query, catalog, current_db, ctes)?
        };
        let output = query_output_names(&canonical);
        let columns = if cte.columns.is_empty() {
            output
        } else if cte.columns.len() == output.len() {
            cte.columns.clone()
        } else {
            // Go `ErrViewWrongList` (1353) for a CTE's column list too.
            return Err(DriverError::ViewWrongList);
        };
        *cte.query = canonical;
        ctes.push(BodyCte {
            name: cte.name.clone(),
            columns,
        });
    }
    Ok(())
}

/// A recursive CTE's seed: its first set-operation term, canonicalized.
fn canonical_seed(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctes: &mut Vec<BodyCte>,
) -> Result<QueryStmt, DriverError> {
    match query {
        QueryStmt::SetOpr(set_opr) => match set_opr.terms.first().map(|term| &term.body) {
            Some(SetOprTermBody::Select(select)) => Ok(QueryStmt::Select(Box::new(
                canonical_select(select, catalog, current_db, ctes, false)?,
            ))),
            Some(SetOprTermBody::Nested(nested)) => canonical_seed(
                &QueryStmt::SetOpr(nested.clone()),
                catalog,
                current_db,
                ctes,
            ),
            None => canonical_query(query, catalog, current_db, ctes),
        },
        QueryStmt::Select(_) => canonical_query(query, catalog, current_db, ctes),
    }
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
    ctes: &mut Vec<BodyCte>,
    as_view_schema: bool,
) -> Result<SelectStmt, DriverError> {
    let mut select = select.clone();
    let depth = ctes.len();
    let result = canonical_select_in_scope(&mut select, catalog, current_db, ctes, as_view_schema);
    ctes.truncate(depth);
    result.map(|()| select)
}

fn canonical_select_in_scope(
    select: &mut SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctes: &mut Vec<BodyCte>,
    as_view_schema: bool,
) -> Result<(), DriverError> {
    canonical_with(select.with.as_mut(), catalog, current_db, ctes)?;
    let mut tables = Vec::new();
    // What an unqualified `*` expands to, in Go's order (see
    // [`combine_join_output`]).
    let mut star_columns = Vec::new();
    if let Some(join) = &mut select.from {
        let left = qualify_join_node(&mut join.left, catalog, current_db, ctes, &mut tables)?;
        let right = match &mut join.right {
            Some(right) => Some(qualify_join_node(right, catalog, current_db, ctes, &mut tables)?),
            None => None,
        };
        canonical_expr_subqueries(join.on.as_mut(), catalog, current_db, ctes)?;
        star_columns = match right {
            Some(right) => combine_join_output(join, left, right),
            None => left,
        };
    }

    let mut fields = Vec::new();
    // Go `addAliasName`'s anonymous fields: neither a column reference nor
    // aliased by the user.
    let mut anonymous = Vec::new();
    for (index, field) in select.fields.fields().iter().enumerate() {
        match field {
            SelectField::Wildcard(path) if path.is_empty() => {
                for column in &star_columns {
                    let mut path = column.qualifier.clone();
                    path.push(column.name.clone());
                    fields.push(SelectField::Expr {
                        expr: Expr::Column(path),
                        alias: Some(column.name.clone()),
                    });
                    anonymous.push(false);
                }
            }
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
                        anonymous.push(false);
                    }
                }
                if !matched {
                    return Err(DriverError::unsupported(
                        "a qualified wildcard names no table in the view's FROM",
                    ));
                }
            }
            SelectField::Expr { expr, alias } => {
                anonymous.push(alias.is_none() && !matches!(expr, Expr::Column(_)));
                let alias = alias.clone().unwrap_or_else(|| {
                    if let Expr::Column(path) = expr {
                        return path.last().cloned().unwrap_or_default();
                    }
                    // Go names the column as the body's plan does
                    // (`buildProjectionFieldNameFromExpressions`): a string
                    // literal by its content, anything else by its text.
                    tidb_planner::plan_builder::field_name::expression_field_display_name(
                        &select.fields,
                        index,
                        expr,
                    )
                });
                fields.push(SelectField::Expr {
                    expr: expr.clone(),
                    alias: Some(alias),
                });
            }
        }
    }
    if as_view_schema {
        rename_duplicated_anonymous_fields(&mut fields, &anonymous);
    }
    select.fields = SelectFieldList::from(fields);
    // Go builds every subquery of the body with the same AST expansion.
    for field in select.fields.fields_mut() {
        if let SelectField::Expr { expr, .. } = field {
            canonical_expr_subqueries(Some(expr), catalog, current_db, ctes)?;
        }
    }
    canonical_expr_subqueries(select.where_clause.as_mut(), catalog, current_db, ctes)?;
    canonical_expr_subqueries(select.having.as_mut(), catalog, current_db, ctes)?;
    Ok(())
}

/// Go `addAliasName` for the view's schema SELECT (`AsViewSchema`, issue
/// 29326): an anonymous field whose name an earlier field already took is
/// renamed `Name_exp_<name>`, then `Name_exp_<n>_<name>`.
fn rename_duplicated_anonymous_fields(fields: &mut [SelectField], anonymous: &[bool]) {
    let name_of = |field: &SelectField| match field {
        SelectField::Expr {
            alias: Some(alias), ..
        } => alias.clone(),
        _ => String::new(),
    };
    let mut dedup: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for (field, anonymous) in fields.iter().zip(anonymous) {
        if !anonymous {
            dedup.insert(name_of(field).to_lowercase(), 0);
        }
    }
    for (field, anonymous) in fields.iter_mut().zip(anonymous) {
        if !anonymous {
            continue;
        }
        let old = name_of(field);
        let key = old.to_lowercase();
        match dedup.get(&key).copied() {
            Some(dup) => {
                let renamed = if dup == 0 {
                    format!("Name_exp_{old}")
                } else {
                    format!("Name_exp_{dup}_{old}")
                };
                if let SelectField::Expr { alias, .. } = field {
                    *alias = Some(renamed);
                }
                dedup.insert(key, dup + 1);
            }
            None => {
                dedup.insert(key, 0);
            }
        }
    }
}

/// Canonicalizes the subqueries inside one expression of a view body, with
/// the CTEs visible at that expression.
fn canonical_expr_subqueries(
    expr: Option<&mut Expr>,
    catalog: &Catalog,
    current_db: &str,
    ctes: &[BodyCte],
) -> Result<(), DriverError> {
    struct Canonicalizer<'a> {
        catalog: &'a Catalog,
        current_db: &'a str,
        ctes: Vec<BodyCte>,
        error: Option<DriverError>,
    }
    impl tidb_ast::Visitor for Canonicalizer<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if self.error.is_some() {
                return true;
            }
            if let Some(query) = node.downcast_mut::<QueryStmt>() {
                match canonical_query(query, self.catalog, self.current_db, &mut self.ctes) {
                    Ok(canonical) => *query = canonical,
                    Err(error) => self.error = Some(error),
                }
                return true;
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            self.error.is_none()
        }
    }
    let Some(expr) = expr else {
        return Ok(());
    };
    let mut canonicalizer = Canonicalizer {
        catalog,
        current_db,
        ctes: ctes.to_vec(),
        error: None,
    };
    tidb_ast::Visitable::accept(expr, &mut canonicalizer);
    canonicalizer.error.map_or(Ok(()), Err)
}

/// Schema-qualifies every table reference of a `FROM` tree, collecting each
/// table's columns for wildcard expansion.
fn qualify_join_node(
    node: &mut JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctes: &mut Vec<BodyCte>,
    tables: &mut Vec<BodyTable>,
) -> Result<Vec<OutColumn>, DriverError> {
    let columns_of = |qualifier: &[String], columns: &[String]| -> Vec<OutColumn> {
        columns
            .iter()
            .map(|name| OutColumn {
                qualifier: qualifier.to_vec(),
                name: name.clone(),
            })
            .collect()
    };
    match node {
        JoinNode::Table(table_ref)
            if table_ref.name.len() == 1
                && ctes
                    .iter()
                    .any(|cte| cte.name.to_lowercase() == table_ref.name[0].to_lowercase()) =>
        {
            let cte = ctes
                .iter()
                .rev()
                .find(|cte| cte.name.to_lowercase() == table_ref.name[0].to_lowercase())
                .expect("matched above");
            let qualifier = vec![table_ref
                .alias
                .clone()
                .unwrap_or_else(|| table_ref.name[0].clone())];
            let out = columns_of(&qualifier, &cte.columns);
            tables.push(BodyTable {
                qualifier,
                columns: cte.columns.clone(),
            });
            Ok(out)
        }
        JoinNode::Table(table_ref) => {
            let (database, name) =
                crate::driver::split_table_path_pub(&table_ref.name, current_db)?;
            let (database, name) = (database.to_owned(), name.to_owned());
            let entry = catalog.table_in(&database, &name).ok_or_else(|| {
                DriverError::Schema(SchemaErrorKind::UnknownTable(format!("{database}.{name}")))
            })?;
            let columns: Vec<String> = entry
                .column_list()
                .into_iter()
                .map(|(name, _)| name)
                .collect();
            let qualifier = match &table_ref.alias {
                Some(alias) => vec![alias.clone()],
                None => vec![database.clone(), name.clone()],
            };
            table_ref.name = vec![database, name];
            let out = columns_of(&qualifier, &columns);
            tables.push(BodyTable { qualifier, columns });
            Ok(out)
        }
        JoinNode::Join(join) => {
            let left = qualify_join_node(&mut join.left, catalog, current_db, ctes, tables)?;
            let right = match &mut join.right {
                Some(right) => Some(qualify_join_node(right, catalog, current_db, ctes, tables)?),
                None => None,
            };
            canonical_expr_subqueries(join.on.as_mut(), catalog, current_db, ctes)?;
            Ok(match right {
                Some(right) => combine_join_output(join, left, right),
                None => left,
            })
        }
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            if *lateral || !column_names.is_empty() {
                return Err(DriverError::unsupported(
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
            let canonical = canonical_query(subquery, catalog, current_db, ctes)?;
            let columns = query_output_names(&canonical);
            **subquery = canonical;
            let qualifier = vec![alias];
            let out = columns_of(&qualifier, &columns);
            tables.push(BodyTable {
                qualifier,
                columns,
            });
            Ok(out)
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
    // Go builds the body's plan and reads its schema; the body never runs.
    let body_columns =
        crate::driver::plan_select_types_in(
            &view.select_sql,
            catalog,
            database,
            ctx,
            Some((database, &view.name)),
        )?;
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
        .map(|((name, _), field_type)| (name.clone(), field_type.clone()))
        .collect())
}
