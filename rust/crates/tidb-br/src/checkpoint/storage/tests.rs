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

//! The table checkpoint storage against an in-memory session that keeps
//! each table as `(key, segment) -> data` rows.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tidb_datatype::Datum;
use tidb_model::{DBInfo, PolicyInfo, TableInfo};

use super::*;
use crate::checkpoint::restore::{
    append_ranges_for_restore, new_checkpoint_file_item, new_checkpoint_range_key_item,
    parse_restore_group, value_marshaler_for_restore, RestoreValueType,
};
use crate::glue::CreateTableConfig;

type Rows = BTreeMap<(Vec<u8>, u64), Vec<u8>>;

/// The tables, by `db.table`, and the statements run.
#[derive(Default)]
pub(crate) struct MemDb {
    pub tables: BTreeMap<String, Rows>,
    pub log: Vec<String>,
}

#[derive(Clone, Default)]
pub(crate) struct MemSession(pub Arc<Mutex<MemDb>>);

fn arg_bytes<'a>(a: &'a SqlArg<'_>) -> &'a [u8] {
    match a {
        SqlArg::String(s) | SqlArg::Bytes(Some(s)) => s,
        _ => panic!("not a byte argument"),
    }
}

fn arg_str(a: &SqlArg<'_>) -> String {
    String::from_utf8(arg_bytes(a).to_vec()).unwrap()
}

fn arg_u64(a: &SqlArg<'_>) -> u64 {
    match a {
        SqlArg::Unsigned(v) => *v,
        SqlArg::Signed(v) => *v as u64,
        _ => panic!("not an integer argument"),
    }
}

impl Session for MemSession {
    fn execute(&self, _: &Context, sql: &str) -> Result<()> {
        self.0.lock().unwrap().log.push(sql.to_owned());
        Ok(())
    }

    fn execute_internal(&self, _: &Context, sql: &str, args: &[SqlArg<'_>]) -> Result<()> {
        let mut db = self.0.lock().unwrap();
        let sql_t = sql.trim();
        db.log.push(
            sql_t
                .split_whitespace()
                .take(3)
                .collect::<Vec<_>>()
                .join(" "),
        );
        if sql_t.starts_with("CREATE TABLE") {
            let name = format!("{}.{}", arg_str(&args[0]), arg_str(&args[1]));
            if db.tables.contains_key(&name) {
                return Err(errors::errorf(format!(
                    "[schema:1050]Table '{name}' already exists"
                )));
            }
            db.tables.insert(name, Rows::new());
        } else if let Some(rest) = sql_t.strip_prefix("REPLACE INTO ") {
            if args.len() == 3 {
                // Data: the table is formatted into the SQL.
                let name = rest.split_whitespace().next().unwrap().to_owned();
                let rows = db
                    .tables
                    .get_mut(&name)
                    .ok_or_else(|| errors::errorf(format!("no table {name}")))?;
                rows.insert(
                    (arg_bytes(&args[0]).to_vec(), arg_u64(&args[1])),
                    arg_bytes(&args[2]).to_vec(),
                );
            } else {
                let name = format!("{}.{}", arg_str(&args[0]), arg_str(&args[1]));
                let rows = db
                    .tables
                    .get_mut(&name)
                    .ok_or_else(|| errors::errorf(format!("no table {name}")))?;
                rows.insert(
                    (Vec::new(), arg_u64(&args[2])),
                    arg_bytes(&args[3]).to_vec(),
                );
            }
        } else if sql_t.starts_with("DROP TABLE IF EXISTS") {
            let name = format!("{}.{}", arg_str(&args[0]), arg_str(&args[1]));
            db.tables.remove(&name);
        }
        Ok(())
    }

    fn exec_restricted_sql(
        &self,
        _: &Context,
        sql: &str,
        args: &[SqlArg<'_>],
    ) -> Result<Vec<Vec<Datum>>> {
        let db = self.0.lock().unwrap();
        let name = format!("{}.{}", arg_str(&args[0]), arg_str(&args[1]));
        let rows = db
            .tables
            .get(&name)
            .ok_or_else(|| errors::errorf(format!("[schema:1146]Table '{name}' doesn't exist")))?;
        let with_uuid = sql.contains("uuid");
        Ok(rows
            .iter()
            .map(|((uuid, seg), data)| {
                let mut row = Vec::new();
                if with_uuid {
                    row.push(Datum::new_bytes_datum(uuid.clone()));
                }
                row.push(Datum::new_int(*seg as i64));
                row.push(Datum::new_bytes_datum(data.clone()));
                row
            })
            .collect())
    }

    fn create_database_on_exist_error(&self, _: &Context, _: &DBInfo) -> Result<()> {
        unimplemented!()
    }
    fn create_table(
        &self,
        _: &Context,
        _: &str,
        _: &TableInfo,
        _: CreateTableConfig,
    ) -> Result<()> {
        unimplemented!()
    }
    fn create_tables(
        &self,
        _: &Context,
        _: &[(String, Vec<TableInfo>)],
        _: CreateTableConfig,
    ) -> Option<Result<()>> {
        None
    }
    fn create_placement_policy(&self, _: &Context, _: &PolicyInfo) -> Result<()> {
        unimplemented!()
    }
    fn get_global_variable(&self, _: &str) -> Result<String> {
        unimplemented!()
    }
    fn get_global_sys_var(&self, _: &str) -> Result<String> {
        unimplemented!()
    }
    fn get_all_ddl_jobs(&self, _: &Context) -> Result<Vec<tidb_model::Job>> {
        unimplemented!()
    }
    fn alter_table_mode(
        &self,
        _: &Context,
        _: i64,
        _: i64,
        _: tidb_model::TableMode,
    ) -> Result<()> {
        unimplemented!()
    }
}

/// Segments of one UUID join back; a UUID missing a segment is dropped,
/// as Go `mergeSelectCheckpoint`.
#[test]
fn merge_select_checkpoint_drops_incomplete_groups() {
    let ctx = Context::background();
    let mem = MemSession::default();
    init_checkpoint_table(&ctx, &mem, "cp", &[CHECKPOINT_DATA_TABLE_NAME]).unwrap();
    {
        let mut db = mem.0.lock().unwrap();
        let rows = db.tables.get_mut("cp.cpt_data").unwrap();
        rows.insert((b"a".to_vec(), 0), b"he".to_vec());
        rows.insert((b"a".to_vec(), 1), b"llo".to_vec());
        rows.insert((b"b".to_vec(), 0), b"x".to_vec());
        rows.insert((b"b".to_vec(), 2), b"z".to_vec());
        rows.insert((b"c".to_vec(), 0), b"ok".to_vec());
    }
    let got = merge_select_checkpoint(&ctx, &mem, "cp", CHECKPOINT_DATA_TABLE_NAME).unwrap();
    assert_eq!(got, vec![b"hello".to_vec(), b"ok".to_vec()]);
}

/// A flush larger than one segment splits under one UUID and reads back
/// whole through `selectCheckpointData`; checksums read back too.
#[test]
fn runner_flushes_read_back() {
    let ctx = Context::background();
    let mem = MemSession::default();
    init_checkpoint_table(
        &ctx,
        &mem,
        "cp",
        &[CHECKPOINT_DATA_TABLE_NAME, CHECKPOINT_CHECKSUM_TABLE_NAME],
    )
    .unwrap();
    let storage = Arc::new(TableCheckpointStorage::new(Box::new(mem.clone()), "cp"));
    let tick = std::time::Duration::from_millis(10);
    let runner = crate::checkpoint::CheckpointRunner::start(
        &ctx,
        storage,
        None,
        Arc::new(value_marshaler_for_restore),
        [tick, tick, std::time::Duration::ZERO, tick],
    );
    let big = "k".repeat(CHECKPOINT_ID_MAP_BLOCK_SIZE);
    append_ranges_for_restore(&ctx, &runner, &new_checkpoint_range_key_item(42, &big)).unwrap();
    append_ranges_for_restore(&ctx, &runner, &new_checkpoint_file_item(43, "f.sst")).unwrap();
    runner.flush_checksum(&ctx, 42, 1, 2, 3).unwrap();
    runner.wait_for_finish(&ctx, true);
    assert!(
        mem.0.lock().unwrap().tables["cp.cpt_data"].len() >= 2,
        "the big group splits"
    );
    let mut got = Vec::new();
    select_checkpoint_data(
        &ctx,
        &mem,
        "cp",
        &parse_restore_group,
        &mut |k: &i64, v: RestoreValueType| {
            got.push((*k, v));
            Ok(())
        },
    )
    .unwrap();
    got.sort_by_key(|(k, _)| *k);
    assert_eq!(
        got,
        vec![
            (
                42,
                RestoreValueType {
                    range_key: big,
                    name: String::new()
                }
            ),
            (
                43,
                RestoreValueType {
                    range_key: String::new(),
                    name: "f.sst".into()
                }
            ),
        ]
    );
    let (sums, _) = select_checkpoint_checksum(&ctx, &mem, "cp").unwrap();
    assert_eq!(
        (
            sums[&42].crc64xor,
            sums[&42].total_kvs,
            sums[&42].total_bytes
        ),
        (1, 2, 3)
    );
}

/// The metadata segments must be numbered from 0 without gaps.
#[test]
fn checkpoint_meta_round_trip_and_gap() {
    let ctx = Context::background();
    let mem = MemSession::default();
    let data = vec![7_u8; CHECKPOINT_ID_MAP_BLOCK_SIZE + 3];
    insert_checkpoint_meta(&ctx, &mem, "cp", CHECKPOINT_META_TABLE_NAME, &data).unwrap();
    assert_eq!(
        select_checkpoint_meta(&ctx, &mem, "cp", CHECKPOINT_META_TABLE_NAME).unwrap(),
        data
    );
    mem.0
        .lock()
        .unwrap()
        .tables
        .get_mut("cp.cpt_metadata")
        .unwrap()
        .remove(&(Vec::new(), 0));
    let err = select_checkpoint_meta(&ctx, &mem, "cp", CHECKPOINT_META_TABLE_NAME).unwrap_err();
    assert!(
        err.to_string()
            .contains("incomplete from table cp.cpt_metadata at segment 1"),
        "{err}"
    );
    mem.0
        .lock()
        .unwrap()
        .tables
        .get_mut("cp.cpt_metadata")
        .unwrap()
        .clear();
    let err = select_checkpoint_meta(&ctx, &mem, "cp", CHECKPOINT_META_TABLE_NAME).unwrap_err();
    assert!(
        err.to_string().contains("get the empty checkpoint meta"),
        "{err}"
    );
}

/// Go `IsCheckpointDB`.
#[test]
fn is_checkpoint_db_prefixes() {
    assert!(is_checkpoint_db(
        "__TiDB_BR_Temporary_Snapshot_Restore_Checkpoint_1"
    ));
    assert!(is_checkpoint_db(
        "__TiDB_BR_Temporary_Log_Restore_Checkpoint"
    ));
    assert!(!is_checkpoint_db("__TiDB_BR_Temporary_mysql"));
}
