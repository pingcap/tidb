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
/// Go `collectDuplicateRowsFromTiKV` / `NewRemoteDupKVStream` for one index range.
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
                start_key: client.encode_key(&start),
                end_key: client.encode_key(&range_end),
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
/// Go `RetrieveKeyAndValueFromErrFoundDuplicateKeys`; Rust retains typed KV
/// arguments, rather than extracting untyped arguments from terror.Error.
pub fn retrieve_key_and_value_from_err_found_duplicate_keys(
    error: Error,
) -> Result<(Vec<u8>, Vec<u8>)> {
    match error {
        Error::Duplicate { key, value } => Ok((key, value)),
        error => Err(error),
    }
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
    /// Go `TestRetrieveKeyAndValueFromErrFoundDuplicateKeys`.
    #[test]
    fn test_retrieve_key_and_value_from_err_found_duplicate_keys() {
        assert_eq!(
            retrieve_key_and_value_from_err_found_duplicate_keys(Error::Duplicate {
                key: b"key".to_vec(),
                value: b"value".to_vec()
            })
            .unwrap(),
            (b"key".to_vec(), b"value".to_vec())
        );
        assert_eq!(
            retrieve_key_and_value_from_err_found_duplicate_keys(Error::Other(
                "other error".into()
            ))
            .unwrap_err()
            .to_string(),
            "other error"
        );
    }
}
