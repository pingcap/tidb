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
//! `SelectStmt` is the body as the planner left it: `buildDDL` builds the body
//! with `canExpandAST` (`tidb_planner::plan_builder::create_view`), which
//! unfolds every `*`, aliases every field and resolves GROUP BY / HAVING /
//! ORDER BY in the AST, and `ddl.BuildViewInfo` restores that AST. That text
//! is what `SHOW CREATE VIEW` and `information_schema.views.VIEW_DEFINITION`
//! print, and what a read of the view parses and builds again
//! (`BuildDataSourceFromView`). This module is the executor and DDL half:
//! `preprocess`, the DDL checks, and `BuildViewInfo`.
//!
//! NOT MODELLED (documented): enforcing `WITH CHECK OPTION` on write-through
//! (the mode is stored and reported, and writes through a view are refused
//! outright, which is the only place the check would apply), and `ALGORITHM =
//! MERGE`'s effect on planning.

use crate::driver::{Catalog, DriverError, ViewDef};
use crate::SchemaErrorKind;
use tidb_ast::{CreateViewStmt, QueryStmt, SetOprTermBody, Stmt};
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
    let (database, name) = crate::driver::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    let mut create = create.clone();
    // Go `preprocess`: `checkCreateViewWithSelect` turns the lock of each
    // SELECT of the body into `SelectLockNone`, and `handleTableName` gives
    // every unqualified table name the current database.
    let locked_terms = check_create_view_with_select(&mut create.query)?;
    tidb_planner::preprocess::handle_table_name(&mut create, current_db)
        .map_err(|_| DriverError::Schema(SchemaErrorKind::NoDatabaseSelected))?;
    // Go `buildDDL` builds the body and reads its schema; the body never runs
    // (a view over a large table must not scan it at CREATE time).
    let body_types =
        crate::driver::build_create_view(&mut create, &database, catalog, current_db, ctx)?;
    let columns: Vec<(String, FieldType)> =
        create.columns.iter().cloned().zip(body_types).collect();

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
    let mut select_sql = build_view_info_select(&create.query, &locked_terms, character_set_client);
    // Go installs the view's `TableInfo` as read back from meta JSON: each
    // byte of the stored body and column names that is not UTF-8 becomes
    // U+FFFD.
    crate::kv_table::table_meta::go_json_text_round_trip(&mut select_sql);
    let mut columns: Vec<(String, tidb_datatype::FieldType)> = columns;
    for column in columns.iter_mut() {
        crate::kv_table::table_meta::go_json_text_round_trip(&mut column.0);
    }
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
        update_ts: current_tso(),
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

/// Go `preprocessor.checkCreateViewWithSelectGrammar` over
/// `checkCreateViewWithSelect`: each SELECT of the body -- the body itself, or
/// every term of a set operation, nested ones included -- may not carry an
/// `INTO` clause (1350), and its lock becomes `SelectLockNone`. Returns which
/// of them had a lock, in [`body_selects`] order, for the trailing space
/// `ddl.BuildViewInfo` restores in its place.
///
/// # Errors
///
/// `ErrViewSelectClause` for a SELECT with `INTO OUTFILE`.
pub fn check_create_view_with_select(query: &mut QueryStmt) -> Result<Vec<bool>, DriverError> {
    fn check(select: &mut tidb_ast::SelectStmt, locked: &mut Vec<bool>) -> Result<(), DriverError> {
        if select.into_outfile.is_some() {
            return Err(DriverError::DdlCoded {
                errno: tidb_error::tidb::errcode::ErrViewSelectClause,
                message: "View's SELECT contains a 'INFO' clause".to_owned(),
            });
        }
        locked.push(select.lock.take().is_some());
        Ok(())
    }
    fn check_set_opr(
        set_opr: &mut tidb_ast::SetOprStmt,
        locked: &mut Vec<bool>,
    ) -> Result<(), DriverError> {
        for term in &mut set_opr.terms {
            match &mut term.body {
                SetOprTermBody::Select(select) => check(select, locked)?,
                SetOprTermBody::Nested(nested) => check_set_opr(nested, locked)?,
            }
        }
        Ok(())
    }
    let mut locked = Vec::new();
    match query {
        QueryStmt::Select(select) => check(select, &mut locked)?,
        QueryStmt::SetOpr(set_opr) => check_set_opr(set_opr, &mut locked)?,
    }
    Ok(locked)
}

/// The SELECTs [`check_create_view_with_select`] visits, in its order, which
/// is also the order their text appears in the restored body.
fn body_selects(query: &QueryStmt) -> Vec<&tidb_ast::SelectStmt> {
    fn set_opr_selects<'a>(
        set_opr: &'a tidb_ast::SetOprStmt,
        out: &mut Vec<&'a tidb_ast::SelectStmt>,
    ) {
        for term in &set_opr.terms {
            match &term.body {
                SetOprTermBody::Select(select) => out.push(select),
                SetOprTermBody::Nested(nested) => set_opr_selects(nested, out),
            }
        }
    }
    let mut out = Vec::new();
    match query {
        QueryStmt::Select(select) => out.push(select.as_ref()),
        QueryStmt::SetOpr(set_opr) => set_opr_selects(set_opr, &mut out),
    }
    out
}

/// Go `ddl.BuildViewInfo`'s `SelectStmt`: the body restored with
/// `RestoreStringSingleQuotes | RestoreKeyWordUppercase |
/// RestoreNameBackQuotes`. `connection_charset` is the CREATE VIEW session's
/// `character_set_connection`: Go's parser typed each plain string literal of
/// the body with it, so the stored text writes it as the `_charset`
/// introducer (`_LATIN1'v'`).
///
/// A `SelectStmt` restores a lock info set to `SelectLockNone` as a space
/// followed by nothing, so each SELECT [`check_create_view_with_select`]
/// unlocked keeps a trailing space where its clause was.
fn build_view_info_select(
    query: &QueryStmt,
    locked_terms: &[bool],
    connection_charset: &str,
) -> String {
    let restore_context =
        tidb_ast::RestoreContext::default().with_string_charset(connection_charset);
    let mut text =
        Stmt::Query(tidb_ast::NodeBox::new(query.clone())).restore_with_context(&restore_context);
    if let QueryStmt::Select(_) = query {
        if locked_terms.first().copied().unwrap_or(false) {
            text.push(' ');
        }
        return text;
    }
    let mut cursor = 0;
    for (select, locked) in body_selects(query).into_iter().zip(locked_terms) {
        let term_text = Stmt::Query(tidb_ast::NodeBox::new(QueryStmt::Select(Box::new(
            select.clone(),
        ))))
        .restore_with_context(&restore_context);
        let Some(found) = text[cursor..].find(&term_text) else {
            continue;
        };
        cursor += found + term_text.len();
        if *locked {
            text.insert(cursor, ' ');
            cursor += 1;
        }
    }
    text
}

/// The columns a view reports today: its stored names, paired with the types
/// its body resolves to *now*.
///
/// Go fills a view's column types for every metadata answer
/// (`tryFillViewColumnType` for `DESCRIBE`/`SHOW COLUMNS`, and the same build
/// for `information_schema.columns`) by building the view as a data source,
/// `BuildDataSourceFromView`, so an `ALTER TABLE base MODIFY COLUMN`
/// underneath a view shows through on the next read. A column the build does
/// not name keeps its stored type.
///
/// Go builds it in a system session (`runWithSystemSession`), see
/// [`crate::StmtContext::for_system_session`].
///
/// A body that no longer resolves is that build's error -- `ErrViewInvalid`
/// for most -- which the caller shapes: `DESCRIBE` reports it and
/// `information_schema.columns` omits the view.
pub fn view_column_list(
    view: &ViewDef,
    database: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Vec<(String, FieldType)>, DriverError> {
    let names: Vec<String> = view.columns.iter().map(|(name, _)| name.clone()).collect();
    let system_session = ctx.for_system_session();
    let types =
        crate::driver::view_column_types(database, &view.name, &names, catalog, &system_session)?;
    Ok(view
        .columns
        .iter()
        .zip(types)
        .map(|((name, stored), field_type)| {
            (name.clone(), field_type.unwrap_or_else(|| stored.clone()))
        })
        .collect())
}

/// A TSO for "now" (physical milliseconds shifted over the logical bits), what
/// an in-process creation records as `TableInfo.UpdateTS` where no timestamp
/// oracle ran.
pub(crate) fn current_tso() -> u64 {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64);
    millis << 18
}
