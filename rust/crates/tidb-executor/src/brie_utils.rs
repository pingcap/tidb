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

//! Go `pkg/executor/brie_utils.go`: the DDL calls a BR restore makes, each
//! under the `SHOW CREATE` text of what it restores as the job's query.

use tidb_ddl::{ClusterDdl, CreateTableConfig, OnExist};
use tidb_model::{DBInfo, PolicyInfo, TableInfo};
use tidb_sessionctx::StmtContext;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

fn query_error(message: impl Into<String>) -> SqlQueryError {
    SqlQueryError::new(tidb_errno::errcode::ErrUnknown, *b"HY000", message.into())
}

/// Go `showRestoredCreateDatabase`.
fn show_restored_create_database(vars: &StmtContext, db: &DBInfo, br_comment: &str) -> String {
    let charset = tidb_table::tables::TableCharset {
        charset: tidb_charset::Charset::from_name(&db.charset)
            .unwrap_or(tidb_table::tables::TableCharset::default().charset),
        collation: tidb_charset::Collation::from_name(&db.collate)
            .unwrap_or(tidb_table::tables::TableCharset::default().collation),
    };
    let placement = db
        .placement_policy_ref
        .as_ref()
        .map(|p| p.read().name.original().to_owned());
    let rows = crate::show::create_database::rows(
        db.name.original().to_owned(),
        charset,
        placement.as_deref(),
        true,
        vars.sql_mode().ansi_quotes,
    );
    let text = match &rows[0][1] {
        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        _ => String::new(),
    };
    format!("{br_comment}{text}")
}

/// Go `BRIECreateDatabase`: `CreateSchemaWithInfo` with `OnExistError`, a
/// database without a charset getting the default one.
///
/// # Errors
/// The DDL error.
pub fn brie_create_database(
    ddl: &dyn ClusterDdl,
    vars: StmtContext,
    schema: &DBInfo,
    br_comment: &str,
) -> Result<(), SqlQueryError> {
    let query = show_restored_create_database(&vars, schema, br_comment);
    let mut schema = schema.clone_like_go();
    if schema.charset.is_empty() {
        // Go `mysql.DefaultCharset`.
        schema.charset = "utf8mb4".to_owned();
    }
    ddl.create_schema_with_info(schema, OnExist::Error, &vars.with_ddl_query(query))
        .map(|_| ())
}

/// Go `showRestoredCreateTable`: Go `ConstructResultOfShowCreateTable`
/// with no allocators, so a view or sequence shows its own `CREATE`.
fn show_restored_create_table(
    vars: &StmtContext,
    db_name: &str,
    table: &TableInfo,
    br_comment: &str,
) -> Result<String, SqlQueryError> {
    if table.is_view() {
        let view = tidb_infoschema::ViewDef::from_table_info(table).map_err(query_error)?;
        let text = crate::show::create::construct_result_of_show_create_view(
            &view,
            vars.sql_mode().ansi_quotes,
        );
        return Ok(format!("{br_comment}{text}"));
    }
    if table.is_sequence() {
        let sequence = tidb_table::sequence::SequenceDef {
            id: table.id,
            name: table.name.original().to_owned(),
            comment: table
                .sequence
                .as_ref()
                .map(|s| s.read().comment.clone())
                .unwrap_or_default(),
            allocator: tidb_table::sequence::SequenceAllocator::new(
                table
                    .sequence
                    .as_ref()
                    .map(|s| s.read().clone())
                    .unwrap_or_default(),
            ),
            update_ts: table.update_ts,
        };
        return Ok(format!(
            "{br_comment}{}",
            crate::ddl_sequence::show_create_sequence(&sequence)
        ));
    }
    let common = tidb_table::table_from_meta::table_from_meta(
        table,
        &tidb_table::table_from_meta::AutoIdSource::Unavailable,
    )
    .map_err(query_error)?;
    let text = crate::show::create::construct_result_of_show_create_table(
        db_name,
        table.name.original(),
        &common,
        vars,
        false,
    )
    .map_err(|e| query_error(e.to_string()))?;
    Ok(format!("{br_comment}{text}"))
}

/// Go `BRIECreateTable`: `CreateTableWithInfo` with `OnExistIgnore` and
/// the foreign key checks off.
///
/// # Errors
/// The DDL error.
pub fn brie_create_table(
    ddl: &dyn ClusterDdl,
    vars: StmtContext,
    db_name: &str,
    table: TableInfo,
    br_comment: &str,
    id_allocated: bool,
) -> Result<(), SqlQueryError> {
    let query = show_restored_create_table(&vars, db_name, &table, br_comment)?;
    let vars = vars.with_ddl_query(query).with_foreign_key_checks(false);
    let config = CreateTableConfig {
        on_exist: OnExist::Ignore,
        id_allocated,
    };
    ddl.create_table_with_info(db_name, table, &[], config, &vars)
        .map(|_| ())
}

/// Go `BRIECreateTables`: each database's tables in batches with
/// `OnExistIgnore` and the foreign key checks off.
///
/// # Errors
/// The DDL error; `ErrInvalidDDLJob` when the cluster cannot create tables
/// in batches.
pub fn brie_create_tables(
    ddl: &dyn ClusterDdl,
    vars: StmtContext,
    tables: &[(String, Vec<TableInfo>)],
    br_comment: &str,
    id_allocated: bool,
) -> Result<(), SqlQueryError> {
    let vars = vars.with_foreign_key_checks(false);
    for (db, tables_in_db) in tables {
        let mut queries = Vec::with_capacity(tables_in_db.len());
        for table in tables_in_db {
            queries.push(show_restored_create_table(&vars, db, table, br_comment)?);
        }
        if let Err(err) =
            split_batch_create_table(ddl, &vars, db, tables_in_db, &queries, id_allocated)
        {
            // An older cluster without `ActionCreateTables` answers
            // `ErrInvalidDDLJob`, and the caller creates one at a time.
            tidb_log::warn(
                "batch create table from tidb failure",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(err.message.clone()),
                )],
            );
            return Err(err);
        }
    }
    Ok(())
}

/// Go `mergeQuerys`.
fn merge_queries(queries: &[String]) -> String {
    queries.iter().map(|q| format!("{q};")).collect()
}

/// Go `splitBatchCreateTable`: halves a batch whose job exceeds the raft
/// entry limit.
fn split_batch_create_table(
    ddl: &dyn ClusterDdl,
    vars: &StmtContext,
    schema: &str,
    infos: &[TableInfo],
    queries: &[String],
    id_allocated: bool,
) -> Result<(), SqlQueryError> {
    let config = CreateTableConfig {
        on_exist: OnExist::Ignore,
        id_allocated,
    };
    let batch_vars = vars.clone().with_ddl_query(merge_queries(queries));
    let res = ddl.batch_create_table_with_info(
        schema,
        infos.iter().map(TableInfo::clone_like_go).collect(),
        config,
        &batch_vars,
    );
    match res {
        Err(err)
            if err.code == tidb_errno::errcode::ErrEntryTooLarge
                || err.code == tidb_errno::errcode::ErrTxnTooLarge =>
        {
            tidb_log::info(
                "entry too large, split batch create table",
                &[tidb_log::Field::new(
                    "num table",
                    tidb_log::Value::U64(infos.len() as u64),
                )],
            );
            if infos.len() == 1 {
                return Err(err);
            }
            let mid = infos.len() / 2;
            split_batch_create_table(
                ddl,
                vars,
                schema,
                &infos[..mid],
                &queries[..mid],
                id_allocated,
            )?;
            split_batch_create_table(
                ddl,
                vars,
                schema,
                &infos[mid..],
                &queries[mid..],
                id_allocated,
            )
        }
        res => res.map(|_| ()),
    }
}

/// Go `ConstructResultOfShowCreatePlacementPolicy`.
#[must_use]
pub fn construct_result_of_show_create_placement_policy(policy: &PolicyInfo) -> String {
    let settings = policy
        .placement_settings
        .as_ref()
        .map(|s| s.read().to_clause())
        .unwrap_or_default();
    let rows = crate::show::create_placement_policy::rows(policy.name.original(), &settings);
    match &rows[0][1] {
        tidb_datatype::Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        _ => String::new(),
    }
}

/// Go `tidbGlueSession.CreatePlacementPolicy`: duplicated policies are
/// ignored during restore.
///
/// # Errors
/// The DDL error.
pub fn brie_create_placement_policy(
    ddl: &dyn ClusterDdl,
    vars: StmtContext,
    policy: &PolicyInfo,
) -> Result<(), SqlQueryError> {
    let query = construct_result_of_show_create_placement_policy(policy);
    ddl.create_placement_policy_with_info(
        policy.clone_like_go(),
        OnExist::Ignore,
        &vars.with_ddl_query(query),
    )
    .map(|_| ())
}

/// Go `tidbGlueSession.AlterTableMode`.
///
/// # Errors
/// The DDL error.
pub fn brie_alter_table_mode(
    ddl: &dyn ClusterDdl,
    vars: StmtContext,
    schema_id: i64,
    table_id: i64,
    mode: tidb_model::TableMode,
) -> Result<(), SqlQueryError> {
    let query = format!("ALTER TABLE MODE SCHEMA_ID={schema_id} TABLE_ID={table_id} TO {mode}");
    ddl.alter_table_mode(mode, schema_id, table_id, &vars.with_ddl_query(query))
        .map(|_| ())
}

#[cfg(test)]
mod tests;
