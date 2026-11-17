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


//! Go `streamhelper/collector.go`: collecting region checkpoints, one
//! collector per leader store batching its regions into
//! `GetLastFlushTSOfRegion` calls.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use crossbeam_channel::{Receiver, Sender};
use tidb_gocontext::{CancelFunc, Context};
use tidb_log::{Field, Value};
use tidb_proto::logbackup;

use super::advancer_env::LogBackupService;
use super::models::Range;
use super::regioniter::RegionWithLeader;
use crate::errors::{self, Result};

/// Go `defaultBatchSize`.
const DEFAULT_BATCH_SIZE: usize = 1024;

/// Go `onSuccessHook`.
pub type OnSuccessHook = Arc<dyn Fn(u64, Range) + Send + Sync>;

/// Go `StoreCheckpoints`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StoreCheckpoints {
    /// Go `HasCheckpoint`.
    pub has_checkpoint: bool,
    /// Go `Checkpoint`.
    pub checkpoint: u64,
    /// Go `FailureSubRanges`.
    pub failure_sub_ranges: Vec<Range>,
}

impl StoreCheckpoints {
    /// Go `merge`.
    fn merge(&mut self, other: Self) {
        if other.has_checkpoint && (other.checkpoint < self.checkpoint || !self.has_checkpoint) {
            self.checkpoint = other.checkpoint;
            self.has_checkpoint = true;
        }
        self.failure_sub_ranges.extend(other.failure_sub_ranges);
    }
}

impl std::fmt::Display for StoreCheckpoints {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoreCheckpoints:")?;
        if self.has_checkpoint {
            write!(f, "{}", self.checkpoint)?;
        } else {
            f.write_str("none")?;
        }
        write!(f, ":(remaining {} ranges)", self.failure_sub_ranges.len())
    }
}

/// Go `storeCollector`: the fields its receive loop owns.
struct StoreCollector {
    store_id: u64,
    batch_size: usize,
    service: Arc<dyn LogBackupService>,
    on_success: Option<OnSuccessHook>,
    current_request: logbackup::GetLastFlushTsOfRegionRequest,
    checkpoint: u64,
    inconsistent: Vec<Range>,
    region_map: HashMap<u64, Range>,
}

impl StoreCollector {
    /// Go `recvLoop`.
    fn recv_loop(&mut self, ctx: &Context, input: &Receiver<RegionWithLeader>) -> Result<()> {
        loop {
            crossbeam_channel::select! {
                recv(ctx.done()) -> _ => {
                    return Err(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled)));
                }
                recv(input) -> r => {
                    let Ok(r) = r else { return self.send_pending_requests(ctx) };
                    if r.leader.store_id != self.store_id {
                        tidb_log::warn(
                            "trying to request to store which isn't the leader of region.",
                            &[
                                Field::new("region", Value::U64(r.region.id)),
                                Field::new("target-store", Value::U64(self.store_id)),
                                Field::new("leader", Value::U64(r.leader.store_id)),
                            ],
                        );
                    }
                    self.region_map.insert(
                        r.region.id,
                        Range { start_key: r.region.start_key.clone(), end_key: r.region.end_key.clone() },
                    );
                    self.current_request.regions.push(logbackup::RegionIdentity {
                        id: r.region.id,
                        epoch_version: r.region.region_epoch.as_ref().map_or(0, |e| e.version),
                    });
                    if self.current_request.regions.len() >= self.batch_size {
                        self.send_pending_requests(ctx)?;
                    }
                }
            }
        }
    }

    /// Go `sendPendingRequests`.
    fn send_pending_requests(&mut self, ctx: &Context) -> Result<()> {
        let cli = self.service.get_log_backup_client(ctx, self.store_id)?;
        let cps = match cli.get_last_flush_ts_of_region(ctx, std::mem::take(&mut self.current_request)) {
            Ok(cps) => cps,
            Err(err) => {
                // Disable the connection cache after an error.
                let _ = self.service.clear_cache(ctx, self.store_id);
                return Err(err);
            }
        };
        for checkpoint in cps.checkpoints {
            let id = checkpoint.region.as_ref().map_or(0, |r| r.id);
            let range = self.region_map.get(&id).cloned().unwrap_or_default();
            if let Some(err) = &checkpoint.err {
                tidb_log::debug("failed to get region checkpoint", &[Field::new("err", Value::Str(format!("{err:?}")))]);
                self.inconsistent.push(range);
            } else {
                if let Some(hook) = &self.on_success {
                    hook(checkpoint.checkpoint, range);
                }
                // The checkpoint is never zero, which is the placeholder.
                if checkpoint.checkpoint < self.checkpoint || self.checkpoint == 0 {
                    self.checkpoint = checkpoint.checkpoint;
                }
            }
        }
        Ok(())
    }
}

/// Go `utils.PanicToErr`'s message.
fn panic_message(p: &(dyn std::any::Any + Send)) -> String {
    p.downcast_ref::<&str>()
        .map(|s| (*s).to_owned())
        .or_else(|| p.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic".to_owned())
}

/// Go `runningStoreCollector`.
struct RunningStoreCollector {
    input: Option<Sender<RegionWithLeader>>,
    /// Go `err` and the fields read after the loop: set when it exits.
    result: Arc<OnceLock<Result<StoreCheckpoints>>>,
    done: Receiver<()>,
}

impl RunningStoreCollector {
    /// Go `spawn`'s wait function.
    fn wait(&mut self, ctx: &Context) -> Result<StoreCheckpoints> {
        self.input = None;
        crossbeam_channel::select! {
            recv(ctx.done()) -> _ => return Err(Arc::new(ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled))),
            recv(self.done) -> _ => {}
        }
        self.result.get().cloned().unwrap_or_else(|| Err(errors::errorf("store collector exited without a result")))
    }
}

/// Go `clusterCollector`.
pub struct ClusterCollector {
    inner: Mutex<ClusterCollectorInner>,
    on_success: Option<OnSuccessHook>,
    master_ctx: Context,
    cancel: CancelFunc,
    srv: Arc<dyn LogBackupService>,
}

struct ClusterCollectorInner {
    collectors: HashMap<u64, RunningStoreCollector>,
    no_leaders: Vec<Range>,
}

impl ClusterCollector {
    /// Go `NewClusterCollector`.
    #[must_use]
    pub fn new(ctx: &Context, srv: Arc<dyn LogBackupService>) -> Self {
        let (master_ctx, cancel) = Context::with_cancel(ctx);
        Self {
            inner: Mutex::new(ClusterCollectorInner { collectors: HashMap::new(), no_leaders: Vec::new() }),
            on_success: None,
            master_ctx,
            cancel,
            srv,
        }
    }

    /// Go `SetOnSuccessHook`.
    pub fn set_on_success_hook(&mut self, hook: OnSuccessHook) {
        self.on_success = Some(hook);
    }

    fn spawn(&self, leader: u64) -> RunningStoreCollector {
        let (input, rx) = crossbeam_channel::bounded(DEFAULT_BATCH_SIZE);
        let (done_tx, done) = crossbeam_channel::bounded::<()>(0);
        let result = Arc::new(OnceLock::new());
        let mut coll = StoreCollector {
            store_id: leader,
            batch_size: DEFAULT_BATCH_SIZE,
            service: Arc::clone(&self.srv),
            on_success: self.on_success.clone(),
            current_request: logbackup::GetLastFlushTsOfRegionRequest::default(),
            checkpoint: 0,
            inconsistent: Vec::new(),
            region_map: HashMap::new(),
        };
        let ctx = self.master_ctx.clone();
        let res = Arc::clone(&result);
        std::thread::spawn(move || {
            let r = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| coll.recv_loop(&ctx, &rx))) {
                Ok(r) => r,
                Err(p) => Err(errors::errorf(format!("panicked: {}", panic_message(p.as_ref())))),
            };
            let r = r.map(|()| StoreCheckpoints {
                has_checkpoint: coll.checkpoint != 0,
                checkpoint: coll.checkpoint,
                failure_sub_ranges: std::mem::take(&mut coll.inconsistent),
            });
            if let Err(err) = &r {
                tidb_log::warn("collector loop meet error", &[Field::new("error", Value::Str(err.to_string()))]);
            }
            let _ = res.set(r);
            drop(done_tx);
        });
        RunningStoreCollector { input: Some(input), result, done }
    }

    /// Go `CollectRegion`.
    ///
    /// # Errors
    /// The leader's collector already failed.
    pub fn collect_region(&self, r: RegionWithLeader) -> Result<()> {
        let mut inner = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.master_ctx.err().is_some() {
            return Ok(());
        }
        if r.leader.store_id == 0 {
            tidb_log::warn("there are regions without leader", &[Field::new("region", Value::U64(r.region.id))]);
            inner.no_leaders.push(Range { start_key: r.region.start_key, end_key: r.region.end_key });
            return Ok(());
        }
        let leader = r.leader.store_id;
        let sc = &*inner.collectors.entry(leader).or_insert_with(|| self.spawn(leader));
        let input = sc.input.as_ref().expect("collector input is open while collecting");
        crossbeam_channel::select! {
            send(input, r) -> _ => Ok(()),
            recv(sc.done) -> _ => {
                let err = sc.result.get().and_then(|r| r.as_ref().err().cloned());
                match err {
                    Some(err) => {
                        self.cancel.cancel();
                        Err(err)
                    }
                    None => Ok(()),
                }
            }
        }
    }

    /// Go `Finish`.
    ///
    /// # Errors
    /// A store collector's error.
    pub fn finish(&self, ctx: &Context) -> Result<StoreCheckpoints> {
        let mut inner = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut result = StoreCheckpoints { failure_sub_ranges: std::mem::take(&mut inner.no_leaders), ..Default::default() };
        let res = (|| {
            for (id, coll) in &mut inner.collectors {
                let r = coll.wait(ctx).map_err(|e| errors::annotate(e, format!("store {id}")))?;
                tidb_log::debug("get checkpoint", &[Field::new("checkpoint", Value::Str(r.to_string()))]);
                result.merge(r);
            }
            Ok(result)
        })();
        self.cancel.cancel();
        res
    }
}
