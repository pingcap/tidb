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

//! Go `pkg/ttl/client/notification.go`.
//!
//! Go hands out etcd's `clientv3.WatchChan`; its only reader, the job
//! manager's loop, takes a receive as "something changed" and a closed
//! channel as "watch again", so each watch response is one `()` here.

use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;

use crossbeam_channel::Receiver;
use tidb_log::{Field, Value};
use tidb_pd_client::{EtcdClient, KEY_OP_DEFAULT_TIMEOUT, KEY_OP_RETRY_INTERVAL};

use crate::command::{EtcdCommandClient, MockClient};
use crate::context::Context;

/// Go `ttlNotificationPrefix`.
pub const TTL_NOTIFICATION_PREFIX: &str = "/tidb/ttl/notification/";

/// Go `NotificationClient`: notifies other TTL workers.
pub trait NotificationClient: Send + Sync {
    /// Go `Notify`.
    fn notify(&self, ctx: &Context, typ: &str, data: &str) -> Result<(), String>;
    /// Go `WatchNotification`: one value per notification; the channel
    /// closes when `ctx` ends.
    fn watch_notification(&self, ctx: &Context, typ: &str) -> Receiver<()>;
}

/// Go `NewNotificationClient`.
pub fn new_notification_client(etcd_cli: Arc<EtcdClient>) -> Arc<dyn NotificationClient> {
    Arc::new(EtcdCommandClient { etcd_cli })
}

impl NotificationClient for EtcdCommandClient {
    /// Go `util.PutKVToEtcd(ctx, c.etcdCli, 1, ttlNotificationPrefix+typ, data)`.
    fn notify(&self, ctx: &Context, typ: &str, data: &str) -> Result<(), String> {
        let key = format!("{TTL_NOTIFICATION_PREFIX}{typ}");
        let mut last = String::new();
        for i in 0..1 {
            if let Some(error) = ctx.err() {
                return Err(error.to_string());
            }
            match self
                .etcd_cli
                .put_with_timeout(key.as_bytes(), data.as_bytes(), KEY_OP_DEFAULT_TIMEOUT)
            {
                Ok(()) => return Ok(()),
                Err(error) => {
                    tidb_logutil::bg_logger().warn(
                        "etcd-cli put kv failed",
                        &[
                            Field::new("key", Value::Str(key.clone())),
                            Field::new("value", Value::Str(data.to_owned())),
                            Field::new("error", Value::Str(error.to_string())),
                            Field::new("retryCnt", Value::I64(i)),
                        ],
                    );
                    last = error.to_string();
                    std::thread::sleep(KEY_OP_RETRY_INTERVAL);
                }
            }
        }
        Err(last)
    }

    fn watch_notification(&self, ctx: &Context, typ: &str) -> Receiver<()> {
        let (ch_tx, ch_rx) = crossbeam_channel::unbounded();
        let (ctx, cancel) = Context::with_cancel(ctx);
        let done = ctx.done();
        let watcher = self.etcd_cli.watch_key_responses(
            format!("{TTL_NOTIFICATION_PREFIX}{typ}"),
            0,
            move || ctx.err().is_some(),
            move |_| {
                let _ = ch_tx.send(());
            },
        );
        match watcher {
            Ok(watcher) => {
                std::thread::Builder::new()
                    .name("ttl-notify-watch".to_owned())
                    .spawn(move || {
                        let _ = done.recv();
                        drop(watcher);
                        drop(cancel);
                    })
                    .expect("ttl notification watch thread spawns");
            }
            Err(_) => cancel.cancel(),
        }
        ch_rx
    }
}

/// Go `NewMockNotificationClient`.
pub fn new_mock_notification_client() -> Arc<dyn NotificationClient> {
    Arc::new(MockClient::default())
}

impl NotificationClient for MockClient {
    fn notify(&self, ctx: &Context, typ: &str, _data: &str) -> Result<(), String> {
        let state = self.lock();
        let Some(watchers) = state.notification_watchers.get(typ) else {
            return Ok(());
        };
        let mut unsent = Vec::new();
        for (index, ch) in watchers.iter().enumerate() {
            if let Some(error) = ctx.err() {
                return Err(error.to_string());
            }
            if let Err(crossbeam_channel::TrySendError::Full(_)) = ch.try_send(()) {
                unsent = watchers[index..].to_vec();
                break;
            }
        }
        if !unsent.is_empty() {
            let ctx = ctx.clone();
            std::thread::Builder::new()
                .name("ttl-mock-notify".to_owned())
                .spawn(move || {
                    for ch in unsent {
                        crossbeam_channel::select! {
                            recv(ctx.done()) -> _ => return,
                            send(ch, ()) -> _ => {}
                        }
                    }
                })
                .expect("ttl mock notify thread spawns");
        }
        Ok(())
    }

    fn watch_notification(&self, _ctx: &Context, typ: &str) -> Receiver<()> {
        let mut state = self.lock();
        let (ch_tx, ch_rx) = crossbeam_channel::bounded(8);
        state
            .notification_watchers
            .entry(typ.to_owned())
            .or_default()
            .push(ch_tx);
        ch_rx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mock_notifications_reach_every_watcher_of_the_type() {
        let cli = new_mock_notification_client();
        let ctx = Context::background();
        let scan = cli.watch_notification(&ctx, "scan");
        let scan2 = cli.watch_notification(&ctx, "scan");
        let other = cli.watch_notification(&ctx, "other");
        cli.notify(&ctx, "scan", "job1").unwrap();
        assert!(scan.recv_timeout(Duration::from_secs(5)).is_ok());
        assert!(scan2.recv_timeout(Duration::from_secs(5)).is_ok());
        assert!(other.try_recv().is_err());
        // With nobody watching, a notification is dropped.
        cli.notify(&ctx, "none", "job1").unwrap();
    }
}
