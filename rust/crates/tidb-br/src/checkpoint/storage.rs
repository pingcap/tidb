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

//! Go `br/pkg/checkpoint/storage.go`: a checkpoint kept in TiDB tables. Each
//! flush is a row group of 512 KiB segments under one UUID; a group with a
//! missing segment is dropped when read back.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_sqlescape::SqlArg;

use super::{
    parse_checkpoint_checksum, parse_checkpoint_data, CheckpointStorage, ChecksumItem, GroupParser,
};
use crate::errors::{self, Result};
use crate::glue::{Domain, Session};

/// Go `LogRestoreCheckpointDatabaseName`.
pub const LOG_RESTORE_CHECKPOINT_DATABASE_NAME: &str = "__TiDB_BR_Temporary_Log_Restore_Checkpoint";
/// Go `SnapshotRestoreCheckpointDatabaseName`.
pub const SNAPSHOT_RESTORE_CHECKPOINT_DATABASE_NAME: &str =
    "__TiDB_BR_Temporary_Snapshot_Restore_Checkpoint";
/// Go `CustomSSTRestoreCheckpointDatabaseName`.
pub const CUSTOM_SST_RESTORE_CHECKPOINT_DATABASE_NAME: &str =
    "__TiDB_BR_Temporary_Custom_SST_Restore_Checkpoint";

/// Go `checkpointDataTableName`.
pub const CHECKPOINT_DATA_TABLE_NAME: &str = "cpt_data";
/// Go `checkpointChecksumTableName`.
pub const CHECKPOINT_CHECKSUM_TABLE_NAME: &str = "cpt_checksum";
/// Go `checkpointMetaTableName`.
pub const CHECKPOINT_META_TABLE_NAME: &str = "cpt_metadata";
/// Go `checkpointProgressTableName`.
pub const CHECKPOINT_PROGRESS_TABLE_NAME: &str = "cpt_progress";
/// Go `checkpointIngestTableName`.
pub const CHECKPOINT_INGEST_TABLE_NAME: &str = "cpt_ingest";

/// Go `createCheckpointTable`.
const CREATE_CHECKPOINT_TABLE: &str = "
		CREATE TABLE %n.%n (
			uuid binary(32) NOT NULL,
			segment_id BIGINT NOT NULL,
			data BLOB(524288) NOT NULL,
			update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(uuid, segment_id));";
/// Go `selectCheckpointSQLTemplate`.
const SELECT_CHECKPOINT_SQL_TEMPLATE: &str = "
		SELECT uuid, segment_id, data FROM %n.%n ORDER BY uuid, segment_id;";
/// Go `createCheckpointMetaTable`.
const CREATE_CHECKPOINT_META_TABLE: &str = "
		CREATE TABLE %n.%n (
			segment_id BIGINT NOT NULL,
			data BLOB(524288) NOT NULL,
			update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY(segment_id));";
/// Go `insertCheckpointMetaSQLTemplate`.
const INSERT_CHECKPOINT_META_SQL_TEMPLATE: &str = "
		REPLACE INTO %n.%n (segment_id, data) VALUES (%?, %?);";
/// Go `selectCheckpointMetaSQLTemplate`.
const SELECT_CHECKPOINT_META_SQL_TEMPLATE: &str = "SELECT segment_id, data FROM %n.%n;";

/// Go `insertCheckpointSQLTemplate` with the names formatted in.
fn insert_checkpoint_sql(db_name: &str, table_name: &str) -> String {
    format!(
        "
		REPLACE INTO {db_name}.{table_name}
			(uuid, segment_id, data) VALUES (%?, %?, %?);"
    )
}

/// Go `IsCheckpointDB`.
#[must_use]
pub fn is_checkpoint_db(dbname: &str) -> bool {
    dbname.starts_with(LOG_RESTORE_CHECKPOINT_DATABASE_NAME)
        || dbname.starts_with(SNAPSHOT_RESTORE_CHECKPOINT_DATABASE_NAME)
        || dbname.starts_with(CUSTOM_SST_RESTORE_CHECKPOINT_DATABASE_NAME)
}

/// Go `CheckpointIdMapBlockSize`.
pub const CHECKPOINT_ID_MAP_BLOCK_SIZE: usize = 524_288;

/// Go `chunkInsertCheckpointData`.
fn chunk_insert_checkpoint_data(
    data: &[u8],
    f: &mut dyn FnMut(u64, &[u8]) -> Result<()>,
) -> Result<()> {
    for (segment_id, chunk) in data.chunks(CHECKPOINT_ID_MAP_BLOCK_SIZE).enumerate() {
        f(segment_id as u64, chunk)?;
    }
    Ok(())
}

/// Go `tableCheckpointStorage`.
pub struct TableCheckpointStorage {
    se: Mutex<Option<Box<dyn Session>>>,
    checkpoint_db_name: String,
}

impl TableCheckpointStorage {
    /// Go `newTableCheckpointStorage`.
    #[must_use]
    pub fn new(se: Box<dyn Session>, checkpoint_db_name: &str) -> Self {
        Self {
            se: Mutex::new(Some(se)),
            checkpoint_db_name: checkpoint_db_name.to_owned(),
        }
    }

    /// Go `chunkInsertCheckpointSQLs` executed: one UUID for the flush,
    /// one `REPLACE` per segment.
    fn flush(&self, ctx: &Context, table: &str, data: &[u8]) -> Result<()> {
        let se = self.se.lock().unwrap();
        let se = se
            .as_deref()
            .ok_or_else(|| errors::errorf("the checkpoint storage is closed"))?;
        let sql = insert_checkpoint_sql(&self.checkpoint_db_name, table);
        let uuid = uuid::Uuid::new_v4();
        chunk_insert_checkpoint_data(data, &mut |segment_id, chunk| {
            se.execute_internal(
                ctx,
                &sql,
                &[
                    SqlArg::Bytes(Some(uuid.as_bytes())),
                    SqlArg::Unsigned(segment_id),
                    SqlArg::Bytes(Some(chunk)),
                ],
            )
        })
    }
}

impl CheckpointStorage for TableCheckpointStorage {
    fn flush_checkpoint_data(&self, ctx: &Context, data: &[u8]) -> Result<()> {
        self.flush(ctx, CHECKPOINT_DATA_TABLE_NAME, data)
    }

    fn flush_checkpoint_checksum(&self, ctx: &Context, data: &[u8]) -> Result<()> {
        self.flush(ctx, CHECKPOINT_CHECKSUM_TABLE_NAME, data)
    }

    /// Go `log.Fatal("unimplement!")`; the table runner never takes a lock.
    fn update_lock(&self, _: &Context) -> Result<()> {
        Err(errors::errorf("unimplement!"))
    }

    fn close(&self) {
        self.se.lock().unwrap().take();
    }
}

fn ident(s: &str) -> SqlArg<'_> {
    SqlArg::String(s.as_bytes())
}

/// Go `mergeSelectCheckpoint`: each UUID's segments joined, a UUID with a
/// missing segment skipped.
fn merge_select_checkpoint(
    ctx: &Context,
    se: &dyn Session,
    db_name: &str,
    table_name: &str,
) -> Result<Vec<Vec<u8>>> {
    let rows = se
        .exec_restricted_sql(
            ctx,
            SELECT_CHECKPOINT_SQL_TEMPLATE,
            &[ident(db_name), ident(table_name)],
        )
        .map_err(|e| {
            errors::annotate(
                e,
                format!("failed to get checkpoint data from table {db_name}.{table_name}"),
            )
        })?;
    let mut ret = Vec::with_capacity(rows.len());
    let mut row_data: Vec<u8> = Vec::new();
    let mut last_uuid: Option<Vec<u8>> = None;
    let mut last_uuid_invalid = false;
    let mut next_segment_id = 0_u64;
    for row in rows {
        let (uuid, segment_id, data) = (
            row[0].go_bytes().to_vec(),
            row[1].get_int64() as u64,
            row[2].go_bytes(),
        );
        if uuid.is_empty() {
            tidb_log::warn("get the empty uuid, but just skip it", &[]);
            continue;
        }
        if last_uuid.as_ref() != Some(&uuid) {
            if !last_uuid_invalid && !row_data.is_empty() {
                ret.push(std::mem::take(&mut row_data));
            }
            row_data.clear();
            last_uuid_invalid = false;
            next_segment_id = 0;
            last_uuid = Some(uuid);
        }
        if last_uuid_invalid {
            continue;
        }
        if next_segment_id != segment_id {
            last_uuid_invalid = true;
            continue;
        }
        row_data.extend_from_slice(data);
        next_segment_id += 1;
    }
    if !last_uuid_invalid && !row_data.is_empty() {
        ret.push(row_data);
    }
    Ok(ret)
}

/// Go `selectCheckpointData`.
///
/// # Errors
/// The read or `f`'s error.
pub fn select_checkpoint_data<K, V>(
    ctx: &Context,
    se: &dyn Session,
    db_name: &str,
    parse_group: &GroupParser<K, V>,
    f: &mut dyn FnMut(&K, V) -> Result<()>,
) -> Result<Duration> {
    let mut past = Duration::ZERO;
    for content in merge_select_checkpoint(ctx, se, db_name, CHECKPOINT_DATA_TABLE_NAME)? {
        parse_checkpoint_data(&content, &mut past, None, parse_group, f)?;
    }
    Ok(past)
}

/// Go `selectCheckpointChecksum`.
///
/// # Errors
/// The read error.
pub fn select_checkpoint_checksum(
    ctx: &Context,
    se: &dyn Session,
    db_name: &str,
) -> Result<(HashMap<i64, ChecksumItem>, Duration)> {
    let mut past = Duration::ZERO;
    let mut out = HashMap::new();
    for content in merge_select_checkpoint(ctx, se, db_name, CHECKPOINT_CHECKSUM_TABLE_NAME)? {
        parse_checkpoint_checksum(&content, &mut out, &mut past)?;
    }
    Ok((out, past))
}

/// Go `initCheckpointTable`.
///
/// # Errors
/// The DDL error.
pub fn init_checkpoint_table(
    ctx: &Context,
    se: &dyn Session,
    db_name: &str,
    table_names: &[&str],
) -> Result<()> {
    se.execute_internal(ctx, "CREATE DATABASE IF NOT EXISTS %n;", &[ident(db_name)])?;
    for table in table_names {
        se.execute_internal(
            ctx,
            CREATE_CHECKPOINT_TABLE,
            &[ident(db_name), ident(table)],
        )?;
    }
    Ok(())
}

/// Go `insertCheckpointMeta` of an already marshaled meta.
///
/// # Errors
/// The DDL or write error.
pub fn insert_checkpoint_meta(
    ctx: &Context,
    se: &dyn Session,
    db_name: &str,
    table_name: &str,
    data: &[u8],
) -> Result<()> {
    se.execute_internal(
        ctx,
        CREATE_CHECKPOINT_META_TABLE,
        &[ident(db_name), ident(table_name)],
    )?;
    chunk_insert_checkpoint_data(data, &mut |segment_id, chunk| {
        se.execute_internal(
            ctx,
            INSERT_CHECKPOINT_META_SQL_TEMPLATE,
            &[
                ident(db_name),
                ident(table_name),
                SqlArg::Unsigned(segment_id),
                SqlArg::Bytes(Some(chunk)),
            ],
        )
    })
}

/// Go `selectCheckpointMeta`, before the JSON decode: the joined segments.
///
/// # Errors
/// The read error, or missing segments.
pub fn select_checkpoint_meta(
    ctx: &Context,
    se: &dyn Session,
    db_name: &str,
    table_name: &str,
) -> Result<Vec<u8>> {
    let rows = se
        .exec_restricted_sql(
            ctx,
            SELECT_CHECKPOINT_META_SQL_TEMPLATE,
            &[ident(db_name), ident(table_name)],
        )
        .map_err(|e| {
            errors::annotate(
                e,
                format!("failed to get checkpoint metadata from table {db_name}.{table_name}"),
            )
        })?;
    if rows.is_empty() {
        return Err(errors::errorf(format!(
            "get the empty checkpoint meta, the checkpoint is incomplete from table {db_name}.{table_name}"
        )));
    }
    let mut data = Vec::with_capacity(rows.len() * CHECKPOINT_ID_MAP_BLOCK_SIZE);
    for (i, row) in rows.iter().enumerate() {
        let segment_id = row[0].get_int64() as u64;
        if i as u64 != segment_id {
            return Err(errors::errorf(format!(
                "the checkpoint metadata is incomplete from table {db_name}.{table_name} at segment {segment_id}"
            )));
        }
        data.extend_from_slice(row[1].go_bytes());
    }
    Ok(data)
}

/// Go `dropCheckpointTables`: the tables, then the database unless a user
/// table was created in it.
///
/// # Errors
/// The DDL error.
pub fn drop_checkpoint_tables(
    ctx: &Context,
    dom: &dyn Domain,
    se: &dyn Session,
    db_name: &str,
    table_names: &[&str],
) -> Result<()> {
    for table in table_names {
        se.execute_internal(
            ctx,
            "DROP TABLE IF EXISTS %n.%n;",
            &[ident(db_name), ident(table)],
        )?;
    }
    let tables = dom.info_schema()?.schema_table_infos(db_name);
    if let Some(t) = tables.first() {
        tidb_log::warn(
            "user tables in the checkpoint database, skip drop the database",
            &[
                Field::new("db", Value::Str(db_name.into())),
                Field::new("table", Value::Str(t.name.lowercase().into())),
            ],
        );
        return Ok(());
    }
    se.execute_internal(ctx, "DROP DATABASE %n;", &[ident(db_name)])
}

#[cfg(test)]
mod tests;
