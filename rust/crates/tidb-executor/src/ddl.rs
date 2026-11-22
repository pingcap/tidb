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

//! The executor's planner unit tests build their schema here
//! ([`run_create_table_on`], built only for tests and the `test-util`
//! feature): Go's `infoschema.MockInfoSchema` over tables built by the DDL
//! package's own `BuildTableInfoWithStmt` / `BuildTableInfoWithLike`
//! (`tidb_ddl_build::table_info_build`), checked by
//! `checkTableInfoValidWithStmt` and `checkTableInfoValidExtra` and loaded
//! back with `TableFromMeta` (`tidb_table::table_from_meta::cluster_table`).
//! The tables carry metadata only: no test runs rows through them (there is
//! no store behind the catalog), and every SQL-visible behavior runs on the
//! TestKit stack (`tidb-session` `unistore_cop`) through the DDL job, as
//! Go's testkit does.
//!
//! The submodules are the executor's own: `hypo_index` (the index
//! advisor's hypothetical indexes) and `indexes` (an index backfill
//! failure's rendering).

mod hypo_index;
mod indexes;
#[cfg(test)]
mod tests_modify_column_type;
#[cfg(test)]
mod tests_table_partition;

pub use hypo_index::set_advisor_hypo_index;
pub use indexes::{index_backfill_error};


use crate::driver::DriverError;
#[cfg(any(test, feature = "test-util"))]
use tidb_infoschema::SessionExtendedInfoSchema;
#[cfg(any(test, feature = "test-util"))]
use tidb_ast::{DdlStmt, Stmt};

/// Go `CREATE TABLE` through its job, in one process: the table built from
/// the statement (`BuildTableInfoWithStmt`), the ids the job draws
/// (`assignGIDsForJobs`, or a bootstrap table's reserved metadef id), and the
/// table loaded back from that metadata (`TableFromMeta`), registered in
/// `catalog`. Returns whether a table was created (`false` only for `IF NOT
/// EXISTS` over an existing name).
#[cfg(any(test, feature = "test-util"))]
pub fn create_table_in_catalog(
    create: &tidb_ast::CreateTableStmt,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
    reserved_table_id: Option<i64>,
) -> Result<bool, DriverError> {
    let (database, name) = tidb_infoschema::split_table_path_pub(&create.name, current_db)?;
    let (database, name) = (database.to_owned(), name.to_owned());
    let Some(charset) = catalog.database_charset(&database) else {
        return Err(DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                .gen_with_stack_by_args(&[(database).into()]),
        ));
    };
    if catalog.contains_in(&database, &name) {
        let exists = DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS
                .gen_with_stack_by_args(&[(format!("{database}.{name}")).into()]),
        );
        if create.if_not_exists {
            ctx.append_suppressed(&exists.clone().to_mysql_error());
            return Ok(false);
        }
        return Err(exists);
    }
    let refused = |error: tidb_ddl_build::table_info_build::DdlAdmissionError| {
        DriverError::Mysql(error.to_mysql_error())
    };
    let mb_ctx = new_metabuild_context_with_sctx(ctx);
    let mut info = match &create.like_table {
        // Go `CreateTable` with `ReferTable`: the preprocessor's checks on
        // the source, then `BuildTableInfoWithLike` over `referTbl.Meta()`.
        Some(source) => {
            let (source_database, source_name) =
                tidb_infoschema::split_table_path_pub(source, current_db)?;
            let not_exists = || {
                DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .gen_with_stack_by_args(&[
                            (&source_database).into(),
                            (&source_name).into(),
                        ]),
                )
            };
            let refer = match catalog.table_in(source_database, source_name) {
                Some(crate::TableEntry::Kv(table)) => table.meta(),
                // A view or sequence reaches `BuildTableInfoWithLike`'s
                // `ErrWrongObject`, which names the TARGET schema.
                Some(crate::TableEntry::View(_) | crate::TableEntry::Sequence(_)) => {
                    return Err(DriverError::from(
                        tidb_infoschema::infoschema_errors::ERR_WRONG_OBJECT
                            .gen_with_stack_by_args(&[
                                (&database).into(),
                                (&source_name).into(),
                                ("BASE TABLE").into(),
                            ]),
                    ));
                }
                None => return Err(not_exists()),
            };
            let refer = refer.read();
            tidb_ddl_build::check_create_table_like_source(&refer, create.temporary)
                .map_err(DriverError::Mysql)?;
            tidb_ddl_build::table_info_build::build_table_info_with_like(
                &database,
                &name,
                &refer,
                create.temporary,
                create.on_commit_delete,
            )
            .map_err(refused)?
        }
        None => tidb_ddl_build::table_info_build::build_table_info_with_stmt(
            create,
            &database,
            charset.charset.name(),
            charset.collation.name(),
            &mb_ctx,
            ctx.enable_check_constraint(),
        )
        .map_err(refused)?,
    };
    // Go `CreateTable`: `checkTableInfoValidWithStmt` and the region split
    // policies, then `createTableWithInfoJob`'s `checkTableInfoValidExtra`.
    tidb_ddl_build::table_info_build::check_table_info_valid_with_stmt(&mb_ctx, &info)
        .map_err(refused)?;
    tidb_ddl_build::table_info_build::set_create_table_split_policies(create, &mut info)
        .map_err(refused)?;
    tidb_ddl_build::table_info_build::check_table_info_valid_extra(
        &mut |warning| mb_ctx.append_warning(warning),
        None,
        &database,
        &mut info,
    )
    .map_err(refused)?;
    info.id = reserved_table_id.unwrap_or_else(fixture_global_id);
    if let Some(partition) = info.partition.as_ref() {
        let count = partition.read().definitions.len();
        for index in 0..count {
            let id = fixture_global_id();
            partition
                .read()
                .definitions
                .update(index, |definition| definition.id = id);
        }
    }
    info.state = tidb_model::SchemaState::PUBLIC;
    let db_id = catalog.database_id(&database).unwrap_or_default();
    let auto_ids = tidb_table::table_from_meta::LocalTableAutoIds::default();
    let table = tidb_table::table_from_meta::cluster_table(&info, &tidb_table::table_from_meta::AutoIdSource::In { db_id, ids: &auto_ids },
    )
    .map_err(DriverError::unsupported)?;
    catalog.register_kv_in(&database, &name, table)?;
    Ok(true)
}

/// The ids a table built with no store behind it takes: Go's tests over
/// `MockInfoSchema` number their tables themselves, a stored table's come
/// from meta's `GenGlobalID`. One process-wide sequence, so two fixtures'
/// tables never share an id.
#[cfg(any(test, feature = "test-util"))]
fn fixture_global_id() -> i64 {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
}

/// Parses and executes a `CREATE TABLE`, building a [`TableInfo`] and
/// registering a TiKV-byte-backed table in `catalog`. Returns whether a table
/// was created (`false` only for `IF NOT EXISTS` over an existing name).
#[cfg(any(test, feature = "test-util"))]
pub fn run_create_table_on(sql: &str, catalog: &mut SessionExtendedInfoSchema) -> Result<bool, DriverError> {
    // A stock session's context: UTC, the SHIPPED strict `sql_mode` and
    // `tidb_enable_clustered_index` ON (`StmtContext`'s derived `Default`
    // is non-strict and leaves clustering at Go's zero value, OFF).
    run_create_table_on_with(
        sql,
        catalog,
        tidb_executor_default_database(),
        &crate::StmtContext::for_query(),
    )
}

/// [`run_create_table_on`] in `current_db` under `ctx`: the session settings
/// (SQL mode, time zone, clock, `tidb_enable_clustered_index`) the statement
/// is built with.
#[cfg(any(test, feature = "test-util"))]
pub fn run_create_table_on_with(
    sql: &str,
    catalog: &mut SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    let stmt = tidb_parser::parse(sql)
        .map_err(|e| DriverError::from(tidb_util::misc::syntax_error(format!("{e:?}"))))?;
    let Stmt::Ddl(ddl) = &stmt else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    let DdlStmt::CreateTable(create) = &**ddl else {
        return Err(DriverError::unsupported("only CREATE TABLE is supported here"));
    };
    // Go's statement path: `Compile`'s preprocessor (`checkCreateTableGrammar`)
    // before the DDL executor.
    tidb_planner::preprocess::check_ddl_grammar(&stmt, ctx.strict()).map_err(DriverError::Mysql)?;
    create_table_in_catalog(create, catalog, current_db, ctx, None)
}

/// The default schema an unqualified `CREATE TABLE` lands in.
#[cfg(any(test, feature = "test-util"))]
fn tidb_executor_default_database() -> &'static str {
    tidb_infoschema::DEFAULT_DATABASE
}

#[cfg(any(test, feature = "test-util"))]
use tidb_ddl_build::metabuild::new_metabuild_context_with_sctx;
