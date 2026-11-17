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


//! Go `streamhelper/client.go`: the log backup metadata in PD's etcd, the
//! parts TiDB reads and writes. Go pages prefix scans (`prefixScanner`);
//! the etcd client here reads a prefix in one range request.

use std::sync::Arc;

use prost::Message as _;
use tidb_proto::backup::StreamBackupTaskInfo;

use super::models::{pause, prefix_of_task, ranges_of, Range};
use super::prefix_scanner::scan_etcd_prefix;
use crate::errors::{self, Result};

/// Go `SeverityError`.
pub const SEVERITY_ERROR: &str = "ERROR";
/// Go `SeverityManual`.
pub const SEVERITY_MANUAL: &str = "MANUAL";

/// A prefix read: the pairs in key order and the response revision.
pub type PrefixRead = (Vec<(Vec<u8>, Vec<u8>)>, i64);

/// The `clientv3.Client` calls log backup makes: reads with the response
/// revision, puts, deletes and prefix watches.
pub trait MetaStore: Send + Sync {
    /// Go `Get(key)`: the value, `None` when absent.
    ///
    /// # Errors
    /// The request error.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    /// Go `Get(ctx, key)` under a ctx that times out after `timeout`.
    ///
    /// # Errors
    /// The request error; a timeout is a [`RetryableMetaError`].
    fn get_with_timeout(&self, key: &[u8], timeout: std::time::Duration) -> Result<Option<Vec<u8>>>;
    /// Go `Get(prefix, WithPrefix())`: the pairs in key order and the
    /// response header revision.
    ///
    /// # Errors
    /// The request error.
    fn get_prefix(&self, prefix: &[u8]) -> Result<PrefixRead>;
    /// Go `Get(from, WithRange(to), WithLimit(limit))`: at most `limit`
    /// pairs of `[from, to)` in key order, and the response's `More`.
    ///
    /// # Errors
    /// The request error.
    fn scan(&self, from: &[u8], to: &[u8], limit: usize) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)>;
    /// Go `Put`.
    ///
    /// # Errors
    /// The request error.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    /// Go `Put(ctx, key, value)` under a ctx that times out after `timeout`.
    ///
    /// # Errors
    /// The request error; a timeout is a [`RetryableMetaError`].
    fn put_with_timeout(&self, key: &[u8], value: &[u8], timeout: std::time::Duration) -> Result<()>;
    /// Go `Delete`.
    ///
    /// # Errors
    /// The request error.
    fn delete(&self, key: &[u8]) -> Result<()>;
    /// Go `Watch(prefix, WithPrefix(), WithRev(rev))`: each response handed
    /// to `on_response` until the handle is dropped or `is_cancelled`.
    ///
    /// # Errors
    /// The watch cannot be created.
    fn watch_prefix(
        &self,
        prefix: &[u8],
        start_revision: i64,
        is_cancelled: Box<dyn Fn() -> bool + Send>,
        on_response: Box<dyn Fn(&tidb_pd_client::WatchResponse) + Send>,
    ) -> Result<Box<dyn Send>>;
}

/// An etcd request that timed out or found no reachable member: Go's
/// `context.DeadlineExceeded` and `codes.Unavailable`, which the advancer's
/// metadata requests retry.
#[derive(Debug)]
pub struct RetryableMetaError(pub String);

impl std::fmt::Display for RetryableMetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RetryableMetaError {}

fn etcd_error(e: tidb_pd_client::EtcdError) -> errors::Error {
    match &e {
        tidb_pd_client::EtcdError::Unreachable { code, .. }
            if ["timeout", "DeadlineExceeded", "Unavailable"].contains(&code.as_str()) =>
        {
            Arc::new(RetryableMetaError(e.to_string()))
        }
        _ => errors::errorf(e.to_string()),
    }
}

impl MetaStore for tidb_pd_client::EtcdClient {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Self::get(self, key).map_err(etcd_error)
    }

    fn get_with_timeout(&self, key: &[u8], timeout: std::time::Duration) -> Result<Option<Vec<u8>>> {
        Self::get_with_timeout(self, key, timeout).map_err(etcd_error)
    }

    fn get_prefix(&self, prefix: &[u8]) -> Result<PrefixRead> {
        let (kvs, revision) = self.get_prefix_metadata_with_revision(prefix).map_err(etcd_error)?;
        Ok((kvs.into_iter().map(|kv| (kv.key, kv.value)).collect(), revision))
    }

    fn scan(&self, from: &[u8], to: &[u8], limit: usize) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, bool)> {
        Self::scan(self, from, to, i64::try_from(limit).unwrap_or(i64::MAX)).map_err(etcd_error)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Self::put(self, key, value).map_err(etcd_error)
    }

    fn put_with_timeout(&self, key: &[u8], value: &[u8], timeout: std::time::Duration) -> Result<()> {
        Self::put_with_timeout(self, key, value, timeout).map_err(etcd_error)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        Self::delete(self, key).map_err(etcd_error)
    }

    fn watch_prefix(
        &self,
        prefix: &[u8],
        start_revision: i64,
        is_cancelled: Box<dyn Fn() -> bool + Send>,
        on_response: Box<dyn Fn(&tidb_pd_client::WatchResponse) + Send>,
    ) -> Result<Box<dyn Send>> {
        let watcher = self
            .watch_prefix_responses(prefix.to_vec(), start_revision, is_cancelled, on_response)
            .map_err(etcd_error)?;
        Ok(Box::new(watcher))
    }
}

/// Go `PauseV2`: why and by whom a task was paused, stored as the pause
/// key's JSON value.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PauseV2 {
    /// Go `Severity`.
    pub severity: String,
    /// Go `OperatorHostName`.
    #[serde(rename = "operation_hostname")]
    pub operator_host_name: String,
    /// Go `OperatorPID`.
    #[serde(rename = "operation_pid")]
    pub operator_pid: i64,
    /// Go `OperationTime`: RFC 3339.
    pub operation_time: String,
    /// Go `PayloadType`.
    pub payload_type: String,
    /// Go `Payload`, base64 in JSON as Go's `[]byte`.
    #[serde(with = "base64_bytes")]
    pub payload: Vec<u8>,
}

mod base64_bytes {
    use base64::Engine as _;
    use serde::{Deserialize as _, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if v.is_empty() {
            return s.serialize_none();
        }
        s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let v: Option<String> = Option::deserialize(d)?;
        v.map_or(Ok(Vec::new()), |s| {
            base64::engine::general_purpose::STANDARD.decode(s).map_err(serde::de::Error::custom)
        })
    }
}

/// Go `os.Hostname`, with Go's `[err: ...]` text on failure.
fn hostname() -> String {
    tidb_sem::operating_system_hostname().unwrap_or_else(|| "[err: hostname unavailable]".to_owned())
}

/// Go `RFC3339Time.String` of `time.Now()` in the local zone.
fn now_rfc3339() -> String {
    chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

impl PauseV2 {
    /// Go `NewLocalPauseV2`.
    #[must_use]
    pub fn new_local() -> Self {
        Self {
            severity: SEVERITY_MANUAL.to_owned(),
            operator_host_name: hostname(),
            operator_pid: i64::from(std::process::id()),
            operation_time: now_rfc3339(),
            ..Default::default()
        }
    }

    /// Go `SetTextMessage`.
    pub fn set_text_message(&mut self, t: &str) {
        "text/plain;charset=UTF-8".clone_into(&mut self.payload_type);
        self.payload = t.as_bytes().to_vec();
    }
}

/// Go `PauseTaskOption`.
pub type PauseTaskOption = Box<dyn FnOnce(&mut PauseV2) + Send>;

/// Go `PauseWithMessage`.
pub fn pause_with_message(m: String) -> PauseTaskOption {
    Box::new(move |p| p.set_text_message(&m))
}

/// Go `PauseWithErrorSeverity`.
pub fn pause_with_error_severity() -> PauseTaskOption {
    Box::new(|p| SEVERITY_ERROR.clone_into(&mut p.severity))
}

/// Go `MetaDataClient`: the log backup metadata over PD's etcd.
#[derive(Clone)]
pub struct MetaDataClient {
    /// Go's embedded `*clientv3.Client`.
    pub client: Arc<dyn MetaStore>,
}

/// Go `Task`: a task read from the metadata, bound to its client.
#[derive(Clone)]
pub struct Task {
    cli: MetaDataClient,
    /// Go `Info`.
    pub info: StreamBackupTaskInfo,
}

impl MetaDataClient {
    /// Go `NewMetaDataClient`.
    #[must_use]
    pub fn new(client: Arc<dyn MetaStore>) -> Self {
        Self { client }
    }

    /// Go `PauseTask`.
    ///
    /// # Errors
    /// The write error.
    pub fn pause_task(&self, task_name: &str, opts: Vec<PauseTaskOption>) -> Result<()> {
        let mut p = PauseV2::new_local();
        for opt in opts {
            opt(&mut p);
        }
        let data = serde_json::to_vec(&p).map_err(|e| errors::errorf(e.to_string()))?;
        self.client
            .put(pause(task_name).as_bytes(), &data)
            .map_err(|e| errors::annotate(e, format!("failed to pause task {task_name}")))
    }

    /// Go `TaskByInfo`.
    #[must_use]
    pub fn task_by_info(&self, info: StreamBackupTaskInfo) -> Task {
        Task { cli: self.clone(), info }
    }

    /// Go `GetAllTasksWithRevision`.
    ///
    /// # Errors
    /// The read or decode error.
    pub fn get_all_tasks_with_revision(&self) -> Result<(Vec<Task>, i64)> {
        let (kvs, revision) = self.client.get_prefix(prefix_of_task().as_bytes())?;
        let tasks = kvs
            .into_iter()
            .map(|(_, v)| {
                StreamBackupTaskInfo::decode(v.as_slice())
                    .map(|info| self.task_by_info(info))
                    .map_err(|e| errors::errorf(e.to_string()))
            })
            .collect::<Result<_>>()?;
        Ok((tasks, revision))
    }

    /// Go `GetAllTasks`.
    ///
    /// # Errors
    /// The read or decode error.
    pub fn get_all_tasks(&self) -> Result<Vec<Task>> {
        self.get_all_tasks_with_revision().map(|(t, _)| t)
    }
}

impl Task {
    /// Go `Ranges`: the task's ranges from its range keys.
    ///
    /// # Errors
    /// The read error.
    pub fn ranges(&self) -> Result<Vec<Range>> {
        let prefix = ranges_of(&self.info.name);
        let kvs = scan_etcd_prefix(&*self.cli.client, &prefix)
            .all_pages(64)
            .map_err(|e| errors::annotate(e, format!("failed to fetch ranges of task {}", self.info.name)))?;
        Ok(kvs
            .into_iter()
            .map(|(k, v)| Range { start_key: k[prefix.len()..].to_vec(), end_key: v })
            .collect())
    }
}
