// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/duplicate.go`'s ErrorOnDup index scan.
use crate::{
    Error, Result,
    local::BackendConfig,
    region_job::{ImportClient, ingest_api_error},
};
use tidb_gocontext::Context;
use tidb_proto::{import_sstpb as sst, kvrpcpb};
/// Go `dupeDetector.RecordIndexConflictError`: identical index key/value
/// versions from repeated imports do not conflict; distinct values do.
#[derive(Default)]
pub struct IndexConflictDetector {
    key: Option<Vec<u8>>,
    value: Vec<u8>,
}
impl IndexConflictDetector {
    /// Go `RecordIndexConflictError` for the next ascending key/descending TS pair.
    pub fn next(&mut self, pair: sst::KvPair) -> Result<()> {
        if self.key.as_ref() == Some(&pair.key) {
            if self.value != pair.value {
                return Err(Error::Duplicate {
                    key: pair.key,
                    value: pair.value,
                });
            }
        } else {
            self.key = Some(pair.key);
            self.value = pair.value;
        }
        Ok(())
    }
}
/// Go `collectDuplicateRowsFromTiKV` / `NewRemoteDupKVStream` for one index
/// range, `[start, end)` encoded with the store's codec as Go's duplicate
/// tasks are (`EncodeRange`).
pub fn collect_remote_duplicate_rows(
    ctx: &Context,
    client: &dyn ImportClient,
    cfg: &BackendConfig,
    start: &[u8],
    end: &[u8],
    min_commit_ts: u64,
) -> Result<()> {
    let mut start = start.to_vec();
    while start.as_slice() < end {
        let mut last_error = None;
        let mut completed = None;
        for _ in 0..20 {
            if let Some(error) = ctx.err() {
                return Err(Error::Other(error.to_string()));
            }
            let region = client.get_region(&start)?;
            let leader = region.leader.as_ref().ok_or_else(|| {
                Error::Ingest(
                    tidb_ingestor_errdef::ERR_NO_LEADER
                        .gen_with_stack_by_args(&[(region.region.id as i64).into()]),
                )
            })?;
            let range_end = if region.region.end_key.is_empty() {
                end.to_vec()
            } else {
                region.region.end_key.clone().min(end.to_vec())
            };
            let request = sst::DuplicateDetectRequest {
                context: Some(kvrpcpb::Context {
                    region_id: region.region.id,
                    region_epoch: region.region.region_epoch,
                    peer: Some(*leader),
                    resource_control_context: Some(kvrpcpb::ResourceControlContext {
                        resource_group_name: cfg.resource_group_name.clone(),
                        ..Default::default()
                    }),
                    request_source: format!("internal_lightning_{}", cfg.task_type),
                    ..Default::default()
                }),
                start_key: start.clone(),
                end_key: range_end.clone(),
                min_commit_ts,
                key_only: false,
            };
            let mut detector = IndexConflictDetector::default();
            let result = client.duplicate_detect(leader.store_id, request, &mut |response| {
                if let Some(error) = ctx.err() {
                    return Err(Error::Other(error.to_string()));
                }
                if let Some(error) = response.region_error {
                    return Err(ingest_api_error(*error));
                }
                if let Some(error) = response.key_error {
                    return Err(Error::Other(format!(
                        "meet key error in duplicate detect response: {}",
                        error.message
                    )));
                }
                for pair in response.pairs {
                    detector.next(pair)?;
                }
                Ok(())
            });
            match result {
                Ok(()) => {
                    completed = Some(range_end);
                    break;
                }
                Err(error @ Error::Duplicate { .. }) => return Err(error),
                Err(error) => {
                    if tidb_ingestor_errdef::is_kv_disk_full_error(&error) {
                        return Err(error);
                    }
                    last_error = Some(error);
                    client.invalidate_region(&region);
                }
            }
        }
        start = completed.ok_or_else(|| last_error.unwrap())?;
    }
    Ok(())
}

/// Go `dupTask`.
#[derive(Clone)]
pub struct DupTask {
    /// Go `KeyRange.StartKey`, encoded for the target keyspace.
    pub start_key: Vec<u8>,
    /// Go `KeyRange.EndKey`, exclusive and encoded for the target keyspace.
    pub end_key: Vec<u8>,
    /// Go `tableID`.
    pub table_id: i64,
    /// Go `indexInfo`; None denotes record rows.
    pub index_info: Option<tidb_model::GoShared<tidb_model::IndexInfo>>,
}
/// Go `physicalTableIDs`: include the logical ID as well as all partitions.
pub fn physical_table_ids(table: &tidb_model::TableInfo) -> Vec<i64> {
    let mut ids = vec![table.id];
    if let Some(partition) = &table.partition {
        ids.extend(
            partition
                .read()
                .definitions
                .snapshot()
                .iter()
                .map(|def| def.id),
        );
    }
    ids
}
/// Go `dupeDetector`'s table/keyspace task planner.
pub struct DupeDetector {
    /// Go `tbl.Meta()`.
    pub table: tidb_model::TableInfo,
    /// Go `indexID`, zero means record keys plus public unique indexes.
    pub index_id: i64,
    /// Go `tikvCodec`.
    pub keyspace: tikv_client::request::Keyspace,
}
impl DupeDetector {
    /// Go `dupeDetector.buildDupTasks` and `buildIndexDupTasks`.
    pub fn build_dup_tasks(&self) -> Result<Vec<DupTask>> {
        use tikv_client::request::{EncodeKeyspace, KeyMode};
        let mut tasks = Vec::new();
        let mut add = |index: Option<tidb_model::GoShared<tidb_model::IndexInfo>>| -> Result<()> {
            for id in physical_table_ids(&self.table) {
                let mut builder = tidb_distsql::RequestBuilder::new();
                match &index {
                    Some(index) => {
                        builder.set_index_ranges(
                            None,
                            id,
                            index.read().id,
                            &tidb_ranger::points::full_range(),
                        );
                    }
                    None => {
                        let ranges = if self.table.is_common_handle {
                            tidb_ranger::points::full_range()
                        } else {
                            tidb_ranger::points::full_int_range(false)
                        };
                        builder.set_handle_ranges(None, id, self.table.is_common_handle, &ranges);
                    }
                }
                let request = builder
                    .build()
                    .map_err(|error| Error::Other(format!("{error:?}")))?;
                for ranges in request.key_ranges.as_ref().unwrap().partitions() {
                    for range in ranges {
                        tasks.push(DupTask {
                            start_key: tikv_client::Key::from(range.start_key.as_slice().to_vec())
                                .encode_keyspace(self.keyspace, KeyMode::Txn)
                                .into(),
                            end_key: tikv_client::Key::from(range.end_key.as_slice().to_vec())
                                .encode_keyspace(self.keyspace, KeyMode::Txn)
                                .into(),
                            table_id: id,
                            index_info: index.clone(),
                        });
                    }
                }
            }
            Ok(())
        };
        if self.index_id == 0 {
            add(None)?;
        }
        for index in self.table.indices.iter_deref() {
            let info = index.read();
            if (self.index_id != 0 && self.index_id == info.id)
                || (self.index_id == 0
                    && info.unique
                    && info.state == tidb_model::SchemaState::PUBLIC)
            {
                drop(info);
                add(Some(index.clone()))?;
            }
        }
        Ok(tasks)
    }
    /// Go `dupeDetector.splitLocalDupTaskByKeys`.
    pub fn split_local_dup_task_by_keys(
        &self,
        task: &DupTask,
        db: &crate::engine::Engine,
        size_limit: u64,
        keys_limit: u64,
    ) -> Result<Vec<DupTask>> {
        let properties = db.get_size_properties()?;
        Ok(crate::local::split_range_by_size_props(
            (task.start_key.clone(), task.end_key.clone()),
            &properties,
            size_limit,
            keys_limit,
        )
        .into_iter()
        .map(|(start_key, end_key)| DupTask {
            start_key,
            end_key,
            table_id: task.table_id,
            index_info: task.index_info.clone(),
        })
        .collect())
    }
}
/// Go `RetrieveKeyAndValueFromErrFoundDuplicateKeys`: the key and value of
/// an `ErrFoundDuplicateKeys` anywhere in `error`'s chain, as the local
/// engine's [`Error::Duplicate`] or a global-sort writer's
/// `simplesst::Error::FoundDuplicateKeys` carries them. Go reads them back
/// from the untyped `terror.Error` arguments.
#[must_use]
pub fn retrieve_key_and_value_from_err_found_duplicate_keys(
    error: &(dyn std::error::Error + 'static),
) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut cause = Some(error);
    while let Some(error) = cause {
        if let Some(Error::Duplicate { key, value }) = error.downcast_ref::<Error>() {
            return Some((key.clone(), value.clone()));
        }
        if let Some((key, value)) = tidb_ingestor_simplesst::duplicate_key_and_value(error) {
            return Some((key.to_vec(), value.to_vec()));
        }
        cause = error.source();
    }
    None
}

/// Go `ConvertToErrFoundConflictRecords`: an `ErrFoundDuplicateKeys` becomes
/// `ErrFoundDataConflictRecords` or `ErrFoundIndexConflictRecords`, naming
/// the conflicting row of `tbl`. `None` when `error` is not one; Go then
/// returns the error unchanged.
#[must_use]
pub fn convert_to_err_found_conflict_records(
    error: &(dyn std::error::Error + 'static),
    tbl: &tidb_table::tables::TableCommon,
) -> Option<Error> {
    let (key, value) = retrieve_key_and_value_from_err_found_duplicate_keys(error)?;
    Some(new_err_found_conflict_records(&key, &value, tbl))
}

/// Go `kv.NewTableKVDecoder`'s session: `SQLMode: mysql.ModeStrictAllTables`.
fn decoder_session() -> tidb_lightning_backend_kv::Session {
    tidb_lightning_backend_kv::Session::new(&tidb_lightning_backend_encode::SessionOptions {
        sql_mode: tidb_mysql::ModeStrictAllTables.0,
        ..Default::default()
    })
}

/// Go `kv.Handle.String()` of a stored handle.
fn handle_string(handle: &tidb_table::tables::TableHandle) -> String {
    match handle {
        tidb_table::tables::TableHandle::Int(h) => h.to_string(),
        tidb_table::tables::TableHandle::Common(encoded) => tidb_txnkv::CommonHandle::new(encoded.clone())
            .map_or_else(|error| format!("{error:?}"), |h| h.to_string()),
    }
}

/// Go `newErrFoundConflictRecords`: the error for a duplicated data or
/// index KV.
fn new_err_found_conflict_records(key: &[u8], value: &[u8], tbl: &tidb_table::tables::TableCommon) -> Error {
    let table_name = tbl.meta().read().name.original().to_owned();
    if tidb_tablecodec::is_record_key(key) {
        // For a data KV.
        let handle = match tidb_tablecodec::table_key::decode_row_key(key) {
            Ok(tidb_tablecodec::table_key::RecordHandle::Int(h)) => tidb_table::tables::TableHandle::Int(h),
            Ok(tidb_tablecodec::table_key::RecordHandle::Common(h)) => tidb_table::tables::TableHandle::Common(h),
            Ok(tidb_tablecodec::table_key::RecordHandle::Partition { .. }) => {
                return Error::Other("partition handle in a row key".to_owned());
            }
            Err(error) => return Error::Other(error.to_string()),
        };
        // Go `TableKVDecoder.DecodeRawRowDataAsStr` over `tbl.Cols()`.
        let session = decoder_session();
        let offsets: Vec<usize> = (0..tbl.columns().len()).collect();
        let row_data = tbl
            .decode_raw_row_data(session.context(), &handle, &offsets, value)
            .map_err(|error| error.into_mysql_error().to_string())
            .and_then(|row: Vec<tidb_datatype::Datum>| {
                tidb_datatype::datums_to_string(&row, true, false).map_err(|error| error.to_string())
            })
            .unwrap_or_else(|error| format!("/* ERROR: {error} */"));
        return Error::Ingest(tidb_lightning_common::errors::ERR_FOUND_DATA_CONFLICT_RECORDS.fast_gen_by_args(&[
            table_name.as_str().into(),
            handle_string(&handle).as_str().into(),
            row_data.as_str().into(),
        ]));
    }
    // For an index KV.
    let index_id = match tidb_tablecodec::table_key::decode_index_key(key) {
        Ok((_, index_id, _)) => index_id,
        Err(error) => return Error::Other(error.to_string()),
    };
    let Some(index) = tbl.indexes().iter().find(|index| index.read().id == index_id) else {
        // Go dereferences the missing index; report it instead.
        return Error::Other(format!("index {index_id} of table {table_name} not found"));
    };
    new_err_found_index_conflict_records(key, value, tbl, index)
}

/// Go `newErrFoundIndexConflictRecords`.
fn new_err_found_index_conflict_records(
    key: &[u8],
    value: &[u8],
    tbl: &tidb_table::tables::TableCommon,
    index: &tidb_table::tables::Index,
) -> Error {
    let table_name = tbl.meta().read().name.original().to_owned();
    let index_name = format!("{table_name}.{}", index.read().name.original());
    let values = match tbl.gen_index_value_from_index(index, key, value) {
        Ok(values) => values,
        Err(error) => {
            tidb_log::warn(
                "decode index key value / column value failed",
                &[
                    tidb_log::Field::new("index", tidb_log::Value::Str(index_name.clone())),
                    tidb_log::Field::new("key", tidb_log::Value::Str(hex::encode(key))),
                    tidb_log::Field::new("value", tidb_log::Value::Str(hex::encode(value))),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.into_mysql_error().to_string())),
                ],
            );
            return Error::Ingest(tidb_lightning_common::errors::ERR_FOUND_INDEX_CONFLICT_RECORDS.fast_gen_by_args(&[
                table_name.as_str().into(),
                index_name.as_str().into(),
                String::from_utf8_lossy(key).as_ref().into(),
                String::from_utf8_lossy(value).as_ref().into(),
            ]));
        }
    };
    // Go `fmt.Sprintf("%s", []string)`.
    let value_str =
        format!("[{}]", values.iter().map(|v| String::from_utf8_lossy(v)).collect::<Vec<_>>().join(" "));
    // Go `TableKVDecoder.DecodeHandleFromIndex`.
    let handle = match tidb_tablecodec::decode_index_handle(key, value, index.column_offsets.len()) {
        Ok(Some(handle)) => handle,
        Ok(None) => return Error::Other("no handle in index key or value".to_owned()),
        Err(error) => return Error::Other(error.to_string()),
    };
    Error::Ingest(tidb_lightning_common::errors::ERR_FOUND_INDEX_CONFLICT_RECORDS.fast_gen_by_args(&[
        table_name.as_str().into(),
        index_name.as_str().into(),
        value_str.as_str().into(),
        handle.to_string().as_str().into(),
    ]))
}

#[cfg(test)]
mod task_tests {
    use super::*;
    /// Go `TestBuildDupTask table fixture`.
    fn table(unique: bool) -> tidb_model::TableInfo {
        let mut table = tidb_model::TableInfo {
            id: 1,
            ..Default::default()
        };
        table.indices = vec![
            tidb_model::IndexInfo {
                id: 1,
                unique,
                state: tidb_model::SchemaState::PUBLIC,
                ..Default::default()
            },
            tidb_model::IndexInfo {
                id: 2,
                unique: false,
                state: tidb_model::SchemaState::PUBLIC,
                ..Default::default()
            },
        ]
        .into();
        table
    }
    /// Go `TestBuildDupTask`: targeted indexes include nonunique indexes;
    /// whole-table detection includes only public unique indexes plus records.
    #[test]
    fn test_build_dup_task() {
        for (id, record, indices) in [(0, true, vec![]), (1, false, vec![1]), (2, false, vec![2])] {
            let tasks = DupeDetector {
                table: table(false),
                index_id: id,
                keyspace: tikv_client::request::Keyspace::Disable,
            }
            .build_dup_tasks()
            .unwrap();
            assert_eq!(tasks.iter().any(|t| t.index_info.is_none()), record);
            assert_eq!(
                tasks
                    .iter()
                    .filter_map(|t| t.index_info.as_ref().map(|i| i.read().id))
                    .collect::<Vec<_>>(),
                indices
            );
            for task in tasks {
                assert_eq!(task.table_id, 1);
                assert!(task.start_key < task.end_key);
            }
        }
        let tasks = DupeDetector {
            table: table(true),
            index_id: 0,
            keyspace: tikv_client::request::Keyspace::Disable,
        }
        .build_dup_tasks()
        .unwrap();
        assert!(tasks[0].index_info.is_none());
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[1].index_info.as_ref().unwrap().read().id, 1);
    }
    /// Go `TestBuildIndexDupTaskEncodesKeyspaceRange`.
    #[test]
    fn test_build_index_dup_task_encodes_keyspace_range() {
        let tasks = DupeDetector {
            table: table(false),
            index_id: 1,
            keyspace: tikv_client::request::Keyspace::Enable { keyspace_id: 42 },
        }
        .build_dup_tasks()
        .unwrap();
        assert_eq!(tasks.len(), 1);
        let codec =
            tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, 42).unwrap();
        let (start, _) = codec
            .decode_range(&tasks[0].start_key, &tasks[0].end_key)
            .unwrap();
        assert_eq!(tidb_tablecodec::table_key::decode_table_id(&start), 1);
        assert_eq!(
            tidb_tablecodec::table_key::decode_index_id(&start).unwrap(),
            1
        );
        assert_eq!(&tasks[0].start_key[..4], &[b'x', 0, 0, 42]);
    }
    /// Go `TestRetrieveKeyAndValueFromErrFoundDuplicateKeys`, with the
    /// error behind the lightning backend's cause and a global-sort writer's
    /// duplicate.
    #[test]
    fn test_retrieve_key_and_value_from_err_found_duplicate_keys() {
        let local = tidb_lightning_backend::Error::from_cause(Error::Duplicate {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        });
        assert_eq!(
            retrieve_key_and_value_from_err_found_duplicate_keys(&local),
            Some((b"key".to_vec(), b"value".to_vec()))
        );
        let external = Error::Source(std::sync::Arc::new(tidb_ingestor_simplesst::found_duplicate_keys(b"k", b"v")));
        assert_eq!(
            retrieve_key_and_value_from_err_found_duplicate_keys(&external),
            Some((b"k".to_vec(), b"v".to_vec()))
        );
        assert_eq!(retrieve_key_and_value_from_err_found_duplicate_keys(&Error::Other("other error".into())), None);
    }
}
