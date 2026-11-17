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


//! Go `streamhelper/advancer_cliext.go`: the task events the advancer
//! follows, from etcd watches over the task and pause keys, and the global
//! checkpoint it reads and writes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use prost::Message as _;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::backup::StreamBackupTaskInfo;

use super::client::{MetaDataClient, PauseTaskOption, RetryableMetaError};
use super::models::{global_checkpoint_of, prefix_of_pause, prefix_of_task, Range};
use crate::errors::{self, Result, ERR_INVALID_ARGUMENT, ERR_PITR_MALFORMED_METADATA};

/// Go `EventType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    /// Go `EventAdd`.
    Add,
    /// Go `EventDel`.
    Del,
    /// Go `EventErr`.
    Err,
    /// Go `EventPause`.
    Pause,
    /// Go `EventResume`.
    Resume,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Add => "Add",
            Self::Del => "Del",
            Self::Err => "Err",
            Self::Pause => "Pause",
            Self::Resume => "Resume",
        })
    }
}

/// Go `TaskEvent`.
#[derive(Clone, Debug)]
pub struct TaskEvent {
    /// Go `Type`.
    pub type_: EventType,
    /// Go `Name`.
    pub name: String,
    /// Go `Info`.
    pub info: Option<StreamBackupTaskInfo>,
    /// Go `Ranges`.
    pub ranges: Vec<Range>,
    /// Go `Err`.
    pub err: Option<errors::Error>,
}

impl std::fmt::Display for TaskEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.err {
            Some(err) => write!(f, "{}({}, err = {err})", self.type_, self.name),
            None => write!(f, "{}({})", self.type_, self.name),
        }
    }
}

/// Go `errorEvent`.
fn error_event(err: errors::Error) -> TaskEvent {
    TaskEvent { type_: EventType::Err, name: String::new(), info: None, ranges: Vec::new(), err: Some(err) }
}

/// Go `StreamMeta`.
pub trait StreamMeta: Send + Sync {
    /// Go `Begin`: the current tasks, then their changes, sent to `ch`.
    ///
    /// # Errors
    /// The initial read error.
    fn begin(&self, ctx: &Context, ch: crossbeam_channel::Sender<TaskEvent>) -> Result<()>;
    /// Go `UploadV3GlobalCheckpointForTask`.
    ///
    /// # Errors
    /// The write error.
    fn upload_v3_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str, checkpoint: u64) -> Result<()>;
    /// Go `GetGlobalCheckpointForTask`.
    ///
    /// # Errors
    /// The read error.
    fn get_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str) -> Result<u64>;
    /// Go `ClearV3GlobalCheckpointForTask`.
    ///
    /// # Errors
    /// The delete error.
    fn clear_v3_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str) -> Result<()>;
    /// Go `PauseTask`.
    ///
    /// # Errors
    /// The write error.
    fn pause_task(&self, ctx: &Context, task_name: &str, opts: Vec<PauseTaskOption>) -> Result<()>;
}

/// Go `metadataRequestTimeouts`' length: the attempts of one request.
const METADATA_REQUEST_ATTEMPTS: usize = 3;

/// Go `runMetadataRequestWithRetry`: three attempts, retried only on a
/// timeout or an unreachable etcd. The etcd client bounds each attempt
/// with its own timeout, where Go uses 5s, 10s and 15s.
fn run_metadata_request_with_retry<T>(
    ctx: &Context,
    warn_message: &str,
    fields: &[Field],
    mut request: impl FnMut() -> Result<T>,
) -> Result<T> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        match request() {
            Ok(v) => return Ok(v),
            Err(err) => {
                let retryable = attempt < METADATA_REQUEST_ATTEMPTS
                    && ctx.err().is_none()
                    && errors::cause(err.as_ref()).downcast_ref::<RetryableMetaError>().is_some();
                let mut log_fields = vec![Field::new("category", Value::Str("log backup advancer".into()))];
                log_fields.extend_from_slice(fields);
                log_fields.push(Field::new("attempt", Value::U64(attempt as u64)));
                log_fields.push(Field::new("max-attempts", Value::U64(METADATA_REQUEST_ATTEMPTS as u64)));
                log_fields.push(Field::new("retry", Value::Bool(retryable)));
                log_fields.push(Field::new("error", Value::Str(err.to_string())));
                tidb_log::warn(warn_message, &log_fields);
                if !retryable {
                    return Err(err);
                }
            }
        }
    }
}

/// Go `parseGlobalCheckpointValue`.
fn parse_global_checkpoint_value(value: &[u8]) -> Result<u64> {
    let bytes: [u8; 8] = value.try_into().map_err(|_| {
        errors::annotatef(
            &ERR_PITR_MALFORMED_METADATA,
            format!(
                "the global checkpoint isn't 64bits (it is {} bytes, value = {})",
                value.len(),
                tidb_redact::key(value)
            ),
        )
    })?;
    Ok(u64::from_be_bytes(bytes))
}

/// Go `AdvancerExt`.
#[derive(Clone)]
pub struct AdvancerExt {
    /// Go's embedded `MetaDataClient`.
    pub meta: MetaDataClient,
}

impl AdvancerExt {
    /// Go `toTaskEvent`.
    fn to_task_event(&self, event: &tidb_pd_client::EtcdWatchEvent) -> Result<TaskEvent> {
        let (task_prefix, pause_prefix) = (prefix_of_task(), prefix_of_pause());
        let key = String::from_utf8_lossy(&event.key).into_owned();
        let (prefix, name) = if let Some(n) = key.strip_prefix(&task_prefix) {
            (task_prefix.as_str(), n.to_owned())
        } else if let Some(n) = key.strip_prefix(&pause_prefix) {
            (pause_prefix.as_str(), n.to_owned())
        } else {
            return Err(errors::annotatef(&ERR_INVALID_ARGUMENT, format!("the path isn't a task/pause path ({key})")));
        };
        let is_task = prefix == task_prefix;
        let type_ = match (event.deleted, is_task) {
            (false, true) => EventType::Add,
            (true, true) => EventType::Del,
            (false, false) => EventType::Pause,
            (true, false) => EventType::Resume,
        };
        let mut te = TaskEvent { type_, name, info: None, ranges: Vec::new(), err: None };
        // Go decodes the task of a delete event too (its value is empty).
        if is_task {
            let info = StreamBackupTaskInfo::decode(event.value.as_slice()).map_err(|e| errors::errorf(e.to_string()))?;
            te.ranges = self.meta.task_by_info(info.clone()).ranges()?;
            te.info = Some(info);
        }
        Ok(te)
    }

    /// Go `eventFromWatch`.
    fn event_from_watch(&self, resp: &tidb_pd_client::WatchResponse) -> Result<Vec<TaskEvent>> {
        if resp.canceled {
            return Err(errors::errorf(format!(
                "watch canceled (compact revision {}): {}",
                resp.compact_revision, resp.cancel_reason
            )));
        }
        Ok(resp
            .events
            .iter()
            .map(|event| {
                self.to_task_event(event).unwrap_or_else(|err| TaskEvent {
                    type_: EventType::Err,
                    name: String::new(),
                    info: None,
                    ranges: Vec::new(),
                    err: Some(err),
                })
            })
            .collect())
    }

    /// Go `startListen`: the task and pause watches from `rev`, until `ctx`
    /// is done or a watch fails; the failure (or `ctx`'s error) is the last
    /// event.
    fn start_listen(&self, ctx: &Context, rev: i64, ch: crossbeam_channel::Sender<TaskEvent>) -> Result<()> {
        let stopped = Arc::new(AtomicBool::new(false));
        let handler = |ext: Self, ch: crossbeam_channel::Sender<TaskEvent>, stopped: Arc<AtomicBool>| {
            move |resp: &tidb_pd_client::WatchResponse| {
                if stopped.load(Ordering::SeqCst) {
                    return;
                }
                match ext.event_from_watch(resp) {
                    Ok(events) => {
                        for event in events {
                            let _ = ch.send(event);
                        }
                    }
                    Err(err) => {
                        tidb_log::warn(
                            "Meet error during receiving the task event.",
                            &[
                                Field::new("category", Value::Str("log backup advancer".into())),
                                Field::new("error", Value::Str(err.to_string())),
                            ],
                        );
                        stopped.store(true, Ordering::SeqCst);
                        let _ = ch.send(error_event(err));
                    }
                }
            }
        };
        let cancelled = |ctx: Context, stopped: Arc<AtomicBool>| move || ctx.err().is_some() || stopped.load(Ordering::SeqCst);
        let task_watch = self.meta.client.watch_prefix(
            prefix_of_task().as_bytes(),
            rev,
            Box::new(cancelled(ctx.clone(), Arc::clone(&stopped))),
            Box::new(handler(self.clone(), ch.clone(), Arc::clone(&stopped))),
        )?;
        let pause_watch = self.meta.client.watch_prefix(
            prefix_of_pause().as_bytes(),
            rev,
            Box::new(cancelled(ctx.clone(), Arc::clone(&stopped))),
            Box::new(handler(self.clone(), ch.clone(), Arc::clone(&stopped))),
        )?;
        let ctx = ctx.clone();
        // The watches live until the context ends; then the listener sees
        // the context's error, as Go's `ch <- errorEvent(ctx.Err())`.
        std::thread::Builder::new()
            .name("log-backup-task-watch".into())
            .spawn(move || {
                let _ = ctx.done().recv();
                stopped.store(true, Ordering::SeqCst);
                drop((task_watch, pause_watch));
                let err = ctx.err().map_or_else(|| errors::errorf("EOF"), |e| Arc::new(e) as errors::Error);
                let _ = ch.send(error_event(err));
            })
            .map_err(|e| errors::errorf(e.to_string()))?;
        Ok(())
    }

    /// Go `getFullTasksAsEvent`.
    fn get_full_tasks_as_event(&self) -> Result<(Vec<TaskEvent>, i64)> {
        let (tasks, rev) = self.meta.get_all_tasks_with_revision()?;
        let mut events = Vec::with_capacity(tasks.len());
        for task in tasks {
            let ranges = task.ranges()?;
            events.push(TaskEvent {
                type_: EventType::Add,
                name: task.info.name.clone(),
                info: Some(task.info),
                ranges,
                err: None,
            });
        }
        Ok((events, rev))
    }

    /// Go `Begin`.
    ///
    /// # Errors
    /// The initial read or the watch creation failed.
    pub fn begin(&self, ctx: &Context, ch: crossbeam_channel::Sender<TaskEvent>) -> Result<()> {
        let (initial, rev) = self.get_full_tasks_as_event()?;
        for task in initial {
            let _ = ch.send(task);
        }
        self.start_listen(ctx, rev + 1, ch)
    }

    /// Go `GetGlobalCheckpointForTask`: 0 when unset.
    ///
    /// # Errors
    /// The read error, or a malformed value.
    pub fn get_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str) -> Result<u64> {
        let key = global_checkpoint_of(task_name);
        let value = run_metadata_request_with_retry(
            ctx,
            "failed to get global checkpoint from metadata store",
            &[
                Field::new("key", Value::Str(tidb_redact::key(key.as_bytes()))),
                Field::new("task", Value::Str(task_name.to_owned())),
            ],
            || self.meta.client.get(key.as_bytes()),
        )?;
        value.map_or(Ok(0), |v| parse_global_checkpoint_value(&v))
    }

    /// Go `UploadV3GlobalCheckpointForTask`: never moves the checkpoint back.
    ///
    /// # Errors
    /// The read or write error.
    pub fn upload_v3_global_checkpoint_for_task(&self, ctx: &Context, task_name: &str, checkpoint: u64) -> Result<()> {
        let key = global_checkpoint_of(task_name);
        let old = self.get_global_checkpoint_for_task(ctx, task_name)?;
        if checkpoint < old {
            tidb_log::warn(
                "skipping upload global checkpoint",
                &[
                    Field::new("category", Value::Str("log backup advancer".into())),
                    Field::new("old", Value::U64(old)),
                    Field::new("new", Value::U64(checkpoint)),
                ],
            );
            return Ok(());
        }
        run_metadata_request_with_retry(
            ctx,
            "failed to upload global checkpoint to metadata store",
            &[
                Field::new("key", Value::Str(tidb_redact::key(key.as_bytes()))),
                Field::new("task", Value::Str(task_name.to_owned())),
                Field::new("checkpoint", Value::U64(checkpoint)),
            ],
            || self.meta.client.put(key.as_bytes(), &checkpoint.to_be_bytes()),
        )
    }

    /// Go `ClearV3GlobalCheckpointForTask`.
    ///
    /// # Errors
    /// The delete error.
    pub fn clear_v3_global_checkpoint_for_task(&self, _ctx: &Context, task_name: &str) -> Result<()> {
        self.meta.client.delete(global_checkpoint_of(task_name).as_bytes())
    }

    /// Go `MetaDataClient.PauseTask`.
    ///
    /// # Errors
    /// The write error.
    pub fn pause_task(&self, _ctx: &Context, task_name: &str, opts: Vec<PauseTaskOption>) -> Result<()> {
        self.meta.pause_task(task_name, opts)
    }
}
