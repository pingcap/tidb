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
                    return Err(ingest_api_error(error));
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
