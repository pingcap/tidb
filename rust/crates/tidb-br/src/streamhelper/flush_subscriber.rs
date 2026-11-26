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


//! Go `streamhelper/flush_subscriber.go`: the `SubscribeFlushEvent` streams
//! of every TiKV store, merged into one channel of flushed spans.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use tidb_gocontext::{CancelFunc, Context};
use tidb_log::{Field, Value};
use tidb_proto::logbackup;

use super::advancer_env::{FlushEventStream, LogBackupService, TiKVClusterMeta};
use super::regioniter::Store;
use crate::errors::{self, Result, ERR_UNKNOWN};
use crate::spans::sorted::{Span, Valued};

/// Go `clearSubscriberTimeOut`.
const CLEAR_SUBSCRIBER_TIMEOUT: Duration = Duration::from_secs(60);
/// Go `subscriptionIdleTimeout`: how long a subscription may stay open
/// without an event; keepalive only proves the transport is alive.
pub const SUBSCRIPTION_IDLE_TIMEOUT: Duration = Duration::from_secs(10 * 60);

/// Go `FlushSubscriber`.
pub struct FlushSubscriber {
    dialer: Arc<dyn LogBackupService>,
    cluster: Arc<dyn TiKVClusterMeta>,
    subscriptions: HashMap<u64, Subscription>,
    events_tx: Sender<Valued>,
    events_rx: Receiver<Valued>,
    master_ctx: Context,
    subscription_idle_timeout: Duration,
}

/// Go `SubscriberConfig`.
pub type SubscriberConfig = Box<dyn FnOnce(&mut FlushSubscriber)>;

/// Go `WithMasterContext`.
pub fn with_master_context(ctx: Context) -> SubscriberConfig {
    Box::new(move |fs| fs.master_ctx = ctx)
}

/// Go `WithSubscriptionIdleTimeout`: zero disables the idle check.
pub fn with_subscription_idle_timeout(timeout: Duration) -> SubscriberConfig {
    Box::new(move |fs| fs.subscription_idle_timeout = timeout)
}

impl FlushSubscriber {
    /// Go `NewSubscriber`.
    #[must_use]
    pub fn new(
        dialer: Arc<dyn LogBackupService>,
        cluster: Arc<dyn TiKVClusterMeta>,
        configs: Vec<SubscriberConfig>,
    ) -> Self {
        let (events_tx, events_rx) = crossbeam_channel::bounded(1024);
        let mut subs = Self {
            dialer,
            cluster,
            subscriptions: HashMap::new(),
            events_tx,
            events_rx,
            master_ctx: Context::background(),
            subscription_idle_timeout: SUBSCRIPTION_IDLE_TIMEOUT,
        };
        for c in configs {
            c(&mut subs);
        }
        subs
    }

    /// Go `UpdateStoreTopology`: subscribes new and restarted stores and
    /// drops removed ones.
    ///
    /// # Errors
    /// The store list cannot be read.
    pub fn update_store_topology(&mut self, ctx: &Context) -> Result<()> {
        let stores = self.cluster.stores(ctx).map_err(|e| errors::annotate(e, "failed to get store list"))?;
        let mut store_set = HashSet::new();
        for store in stores {
            match self.subscriptions.get_mut(&store.id) {
                None => {
                    let mut sub = Subscription::new(store, self.events_tx.clone(), self.subscription_idle_timeout);
                    sub.connect(&self.master_ctx, self.dialer.as_ref());
                    self.subscriptions.insert(store.id, sub);
                }
                Some(sub) if sub.store_boot_at != store.boot_at => {
                    sub.store_boot_at = store.boot_at;
                    sub.connect(&self.master_ctx, self.dialer.as_ref());
                }
                Some(_) => {}
            }
            store_set.insert(store.id);
        }
        let removed: Vec<u64> = self.subscriptions.keys().filter(|id| !store_set.contains(id)).copied().collect();
        for id in removed {
            self.remove_subscription(ctx, id);
        }
        Ok(())
    }

    /// Go `Clear`.
    pub fn clear(&mut self) {
        let mut timeout = CLEAR_SUBSCRIBER_TIMEOUT;
        tidb_failpoint::inject(
            "github.com/pingcap/tidb/br/pkg/streamhelper/FlushSubscriber.Clear.timeoutMs",
            |v| timeout = Duration::from_millis(u64::try_from(v.int()).unwrap_or(0)),
        );
        tidb_log::info(
            "Clearing.",
            &[
                Field::new("category", Value::Str("log backup flush subscriber".into())),
                Field::new("timeout", Value::Str(format!("{timeout:?}"))),
            ],
        );
        let (cx, cancel) = Context::with_timeout(&Context::background(), timeout);
        let ids: Vec<u64> = self.subscriptions.keys().copied().collect();
        for id in ids {
            self.remove_subscription(&cx, id);
        }
        cancel.cancel();
    }

    /// Go `Drop`: the subscriber is unusable afterwards. Go closes the
    /// events channel; here the receivers see it closed once the
    /// subscriber and its subscriptions are gone.
    pub fn drop_subscriber(mut self) {
        self.clear();
    }

    /// Go `HandleErrors`: reconnects the stores whose error can be retried.
    pub fn handle_errors(&mut self) {
        for (id, sub) in &mut self.subscriptions {
            if let Some(err) = sub.load_error() {
                let retry = can_be_retried(&err);
                tidb_log::warn(
                    "Meet error.",
                    &[
                        Field::new("category", Value::Str("log backup flush subscriber".into())),
                        Field::new("error", Value::Str(err.to_string())),
                        Field::new("store", Value::U64(*id)),
                    ],
                );
                if retry {
                    if let Err(err) = self.dialer.clear_cache(&self.master_ctx, *id) {
                        tidb_log::warn(
                            "failed to clear cached store connection before retrying subscription",
                            &[
                                Field::new("category", Value::Str("log backup flush subscriber".into())),
                                Field::new("store", Value::U64(*id)),
                                Field::new("error", Value::Str(err.to_string())),
                            ],
                        );
                    }
                    tidb_log::info(
                        "retry connecting to store to add subscription",
                        &[
                            Field::new("category", Value::Str("log backup flush subscriber".into())),
                            Field::new("store", Value::U64(*id)),
                        ],
                    );
                    sub.connect(&self.master_ctx, self.dialer.as_ref());
                }
            }
        }
    }

    /// Whether any store is subscribed (Go reads `len(subscriptions)`).
    #[must_use]
    pub fn has_subscriptions(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    /// Go `Events`.
    #[must_use]
    pub fn events(&self) -> Receiver<Valued> {
        self.events_rx.clone()
    }

    fn remove_subscription(&mut self, ctx: &Context, store: u64) {
        if let Some(mut subs) = self.subscriptions.remove(&store) {
            tidb_log::info(
                "Removing subscription.",
                &[
                    Field::new("category", Value::Str("log backup subscription manager".into())),
                    Field::new("store", Value::U64(store)),
                ],
            );
            subs.close(ctx);
        }
    }

    /// Go `PendingErrors`: every subscription's error, joined.
    ///
    /// # Errors
    /// The joined errors.
    pub fn pending_errors(&self) -> Result<()> {
        let errs: Vec<String> = self
            .subscriptions
            .values()
            .filter_map(|s| s.load_error().map(|e| format!("store {} has error: {e}", s.store_id)))
            .collect();
        if errs.is_empty() {
            return Ok(());
        }
        Err(errors::errorf(errs.join("; ")))
    }
}

/// Go `canBeRetried`: every error but `Unimplemented`.
fn can_be_retried(err: &errors::Error) -> bool {
    errors::grpc_code(errors::cause(err.as_ref())) != tonic::Code::Unimplemented
}

/// Go `subscription`: IDLE until `connect`, CONNECTED with a worker, ERROR
/// once the worker or a connect fails.
struct Subscription {
    cancel: Option<CancelFunc>,
    background: Option<Receiver<()>>,
    err: Arc<Mutex<Option<errors::Error>>>,
    store_id: u64,
    /// The store's boot time: a restarted store is reconnected even after
    /// an error that cannot be retried.
    store_boot_at: u64,
    idle_timeout: Duration,
    output: Sender<Valued>,
}

impl Subscription {
    fn new(to_store: Store, output: Sender<Valued>, idle_timeout: Duration) -> Self {
        Self {
            cancel: None,
            background: None,
            err: Arc::new(Mutex::new(None)),
            store_id: to_store.id,
            store_boot_at: to_store.boot_at,
            idle_timeout,
            output,
        }
    }

    fn emit_error(err_slot: &Mutex<Option<errors::Error>>, err: errors::Error) {
        *err_slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(err);
    }

    fn load_error(&self) -> Option<errors::Error> {
        self.err.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
    }

    fn connect(&mut self, ctx: &Context, dialer: &dyn LogBackupService) {
        if let Err(err) = self.do_connect(ctx, dialer) {
            Self::emit_error(&self.err, err);
        }
    }

    fn do_connect(&mut self, ctx: &Context, dialer: &dyn LogBackupService) -> Result<()> {
        let client_id = uuid::Uuid::new_v4().to_string();
        tidb_log::info(
            "Adding subscription.",
            &[
                Field::new("category", Value::Str("log backup subscription manager".into())),
                Field::new("store", Value::U64(self.store_id)),
                Field::new("boot", Value::U64(self.store_boot_at)),
                Field::new("client-id", Value::Str(client_id.clone())),
            ],
        );
        // Stop the old worker first, so an error it yields while stopping
        // isn't carried into the next run.
        self.close(ctx);
        *self.err.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        let c = dialer
            .get_log_backup_client(ctx, self.store_id)
            .map_err(|e| errors::annotate(e, "failed to get log backup client"))?;
        let (cx, cancel) = Context::with_cancel(ctx);
        let cli = match c.subscribe_flush_event(&cx, logbackup::SubscribeFlushEventRequest { client_id }) {
            Ok(cli) => cli,
            Err(err) => {
                cancel.cancel();
                let _ = dialer.clear_cache(ctx, self.store_id);
                return Err(errors::annotate(err, "failed to subscribe events"));
            }
        };
        let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(0);
        let worker = Listener {
            store_id: self.store_id,
            idle_timeout: self.idle_timeout,
            output: self.output.clone(),
            err: Arc::clone(&self.err),
            cancel: cancel.clone(),
        };
        std::thread::Builder::new()
            .name(format!("log-backup-flush-sub-{}", self.store_id))
            .spawn(move || {
                worker.listen_over(&cx, cli);
                drop(done_tx);
            })
            .map_err(|e| errors::errorf(e.to_string()))?;
        self.cancel = Some(cancel);
        self.background = Some(done_rx);
        Ok(())
    }

    /// Go `close`: cancels the worker and waits for it within `ctx`. The
    /// shared output channel stays open.
    fn close(&mut self, ctx: &Context) {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
            if let Some(bg) = self.background.take() {
                crossbeam_channel::select! {
                    recv(bg) -> _ => {}
                    recv(ctx.done()) -> _ => tidb_log::warn("join handle timed out.", &[]),
                }
            }
        }
    }
}

/// The worker half of Go `subscription.listenOver`.
struct Listener {
    store_id: u64,
    idle_timeout: Duration,
    output: Sender<Valued>,
    err: Arc<Mutex<Option<errors::Error>>>,
    cancel: CancelFunc,
}

impl Listener {
    /// Go `startTimeoutWatcher`: cancels the stream after `idle_timeout`
    /// without activity.
    fn start_timeout_watcher(
        &self,
        ctx: &Context,
        watcher_done: &Receiver<()>,
        activity: &Receiver<()>,
        idle_timed_out: &AtomicBool,
    ) {
        loop {
            crossbeam_channel::select! {
                recv(ctx.done()) -> _ => return,
                recv(watcher_done) -> _ => return,
                recv(activity) -> _ => {}
                default(self.idle_timeout) => {
                    idle_timed_out.store(true, Ordering::SeqCst);
                    tidb_log::warn(
                        "Listen idle timeout.",
                        &[
                            Field::new("store", Value::U64(self.store_id)),
                            Field::new("idle-timeout", Value::Str(format!("{:?}", self.idle_timeout))),
                        ],
                    );
                    self.cancel.cancel();
                    return;
                }
            }
        }
    }

    fn listen_over(&self, ctx: &Context, mut cli: Box<dyn FlushEventStream>) {
        let store_id = self.store_id;
        tidb_log::info(
            "Listen starting.",
            &[
                Field::new("store", Value::U64(store_id)),
                Field::new("idle-timeout", Value::Str(format!("{:?}", self.idle_timeout))),
            ],
        );
        let (activity_tx, activity_rx) = crossbeam_channel::bounded::<()>(1);
        let (watcher_done_tx, watcher_done_rx) = crossbeam_channel::bounded::<()>(0);
        let idle_timed_out = AtomicBool::new(false);
        std::thread::scope(|scope| {
            if !self.idle_timeout.is_zero() {
                let (activity_rx, watcher_done_rx, idle) = (&activity_rx, &watcher_done_rx, &idle_timed_out);
                scope.spawn(move || self.start_timeout_watcher(ctx, watcher_done_rx, activity_rx, idle));
            }
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.recv_loop(ctx, cli.as_mut(), &activity_tx, &idle_timed_out);
            }));
            drop(watcher_done_tx);
            if let Err(p) = res {
                let msg = p
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| p.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                tidb_log::warn(
                    "Subscriber paniked.",
                    &[Field::new("store", Value::U64(store_id)), Field::new("panic-data", Value::Str(msg.clone()))],
                );
                Subscription::emit_error(
                    &self.err,
                    errors::annotatef(&ERR_UNKNOWN, format!("panic during executing: {msg}")),
                );
            }
        });
    }

    fn recv_loop(
        &self,
        ctx: &Context,
        cli: &mut dyn FlushEventStream,
        activity: &Sender<()>,
        idle_timed_out: &AtomicBool,
    ) {
        let store_id = self.store_id;
        loop {
            let msg = match cli.recv() {
                Ok(msg) => msg,
                Err(err) => {
                    tidb_log::info(
                        "Listen stopped.",
                        &[Field::new("store", Value::U64(store_id)), Field::new("error", Value::Str(err.to_string()))],
                    );
                    Subscription::emit_error(&self.err, errors::annotate(err, format!("while receiving from store id {store_id}")));
                    return;
                }
            };
            let _ = activity.try_send(());
            tidb_log::debug("Sending events.", &[Field::new("size", Value::U64(msg.events.len() as u64))]);
            for m in msg.events {
                let start = match decode_key(&m.start_key) {
                    Ok(k) => k,
                    Err(err) => {
                        tidb_log::warn(
                            "start key not encoded, skipping",
                            &[
                                Field::new(
                                    "event",
                                    Value::Str(tidb_redact::key_owned(&m.start_key)),
                                ),
                                Field::new("error", Value::Str(err)),
                            ],
                        );
                        continue;
                    }
                };
                let end = match decode_key(&m.end_key) {
                    Ok(k) => k,
                    Err(err) => {
                        tidb_log::warn(
                            "end key not encoded, skipping",
                            &[
                                Field::new(
                                    "event",
                                    Value::Str(tidb_redact::key_owned(&m.end_key)),
                                ),
                                Field::new("error", Value::Str(err)),
                            ],
                        );
                        continue;
                    }
                };
                tidb_failpoint::inject("github.com/pingcap/tidb/br/pkg/streamhelper/subscription.listenOver.aboutToSend", |_| {});
                let evt = Valued { key: Span { start_key: start, end_key: end }, value: m.checkpoint };
                crossbeam_channel::select! {
                    send(self.output, evt) -> _ => {}
                    recv(ctx.done()) -> _ => {
                        tidb_log::warn("Context canceled while sending events.", &[Field::new("store", Value::U64(store_id))]);
                        if idle_timed_out.load(Ordering::SeqCst) {
                            Subscription::emit_error(
                                &self.err,
                                errors::errorf(format!(
                                    "flush subscription from store id {store_id} has no activity for {:?}: context deadline exceeded",
                                    self.idle_timeout
                                )),
                            );
                        }
                        return;
                    }
                }
            }
        }
    }
}

/// Go `decodeKey`: TiKV reports region ranges memcomparable-encoded.
fn decode_key(key: &[u8]) -> std::result::Result<Vec<u8>, String> {
    if key.is_empty() {
        return Ok(Vec::new());
    }
    tidb_codec::decode_bytes(key).map(|(_, data)| data).map_err(|e| e.to_string())
}
