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

//! Go `pkg/domain/globalconfigsync`: a bounded notification channel and the
//! single-item PD store boundary. The domain keeper owns consumption/lifetime.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, SyncSender},
    Arc,
};

pub use tidb_proto::pdpb::{EventType, GlobalConfigItem};

/// The PD method used by this package. No retry policy belongs here.
pub trait GlobalConfigStore: Send + Sync {
    /// Store the supplied items under Go's config path (empty uses PD's default).
    fn store_global_config(
        &self,
        config_path: &str,
        items: Vec<GlobalConfigItem>,
    ) -> Result<(), String>;
}

/// Producer/store authority. Only the domain keeper receives notifications.
pub struct GlobalConfigSyncer {
    pd: Option<Arc<dyn GlobalConfigStore>>,
    notify: SyncSender<Option<GlobalConfigItem>>,
    stopped: Arc<AtomicBool>,
}

/// Go's NotifyCh receive capability, moved into exactly one domain keeper.
pub struct Notifications {
    receive: Receiver<Option<GlobalConfigItem>>,
    stopped: Arc<AtomicBool>,
}

impl GlobalConfigSyncer {
    /// Construct Go's eight-item blocking notification channel.
    pub fn new(pd: Option<Arc<dyn GlobalConfigStore>>) -> (Arc<Self>, Notifications) {
        let (notify, receive) = mpsc::sync_channel(8);
        let stopped = Arc::new(AtomicBool::new(false));
        (
            Arc::new(Self {
                pd,
                notify,
                stopped: Arc::clone(&stopped),
            }),
            Notifications { receive, stopped },
        )
    }

    /// Block on a full queue, as Go Notify does. False means the keeper closed.
    pub fn notify(&self, item: GlobalConfigItem) -> bool {
        !self.stopped.load(Ordering::Acquire) && self.notify.send(Some(item)).is_ok()
    }

    /// Store exactly one item; a nil PD client is a successful no-op in Go.
    pub fn store_global_config(&self, item: GlobalConfigItem) -> Result<(), String> {
        let Some(pd) = &self.pd else {
            return Ok(());
        };
        let name = item.name.clone();
        let value = item.value.clone();
        pd.store_global_config("", vec![item])?;
        tracing::info!(name, value, "store global config");
        Ok(())
    }

    /// Signal the domain's exit and wake an idle keeper without waiting behind
    /// queued work. Dropping its receiver releases any blocked producers.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        let _ = self.notify.try_send(None);
    }
}

impl Notifications {
    /// Receive one item, or stop when the domain exits or the producer closes.
    pub fn recv(&self) -> Option<GlobalConfigItem> {
        if self.stopped.load(Ordering::Acquire) {
            return None;
        }
        let item = self.receive.recv().ok().flatten();
        if self.stopped.load(Ordering::Acquire) {
            None
        } else {
            item
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Default)]
    struct Store(Mutex<Vec<(String, Vec<GlobalConfigItem>)>>);
    impl GlobalConfigStore for Store {
        fn store_global_config(
            &self,
            path: &str,
            items: Vec<GlobalConfigItem>,
        ) -> Result<(), String> {
            self.0.lock().unwrap().push((path.to_owned(), items));
            Ok(())
        }
    }
    fn item(name: &str) -> GlobalConfigItem {
        GlobalConfigItem {
            name: name.to_owned(),
            value: "b".into(),
            ..Default::default()
        }
    }

    /// Original Go TestGlobalConfigSyncer: notify, consume, persist under the
    /// default PD path and preserve name/value. Wire storage is tested at PD.
    #[test]
    fn global_config_syncer_stores_one_notification() {
        let store = Arc::new(Store::default());
        let (syncer, notifications) = GlobalConfigSyncer::new(Some(store.clone()));
        assert!(syncer.notify(item("a")));
        syncer
            .store_global_config(notifications.recv().unwrap())
            .unwrap();
        assert_eq!(
            *store.0.lock().unwrap(),
            vec![(String::new(), vec![item("a")])]
        );
        let (nil, _) = GlobalConfigSyncer::new(None);
        nil.store_global_config(item("a")).unwrap();
    }

    #[test]
    fn global_config_syncer_propagates_store_errors_without_retry() {
        struct Failing(std::sync::atomic::AtomicUsize);
        impl GlobalConfigStore for Failing {
            fn store_global_config(&self, _: &str, _: Vec<GlobalConfigItem>) -> Result<(), String> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err("store failed".into())
            }
        }
        let store = Arc::new(Failing(std::sync::atomic::AtomicUsize::new(0)));
        let (syncer, _) = GlobalConfigSyncer::new(Some(store.clone()));
        assert_eq!(
            syncer.store_global_config(item("a")),
            Err("store failed".into())
        );
        assert_eq!(store.0.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn global_config_notifications_block_at_eight_and_close_releases_producers() {
        let (syncer, notifications) = GlobalConfigSyncer::new(None);
        for index in 0..8 {
            assert!(syncer.notify(item(&index.to_string())));
        }
        let producer = Arc::clone(&syncer);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            done_tx.send(producer.notify(item("ninth"))).unwrap();
        });
        entered_rx.recv().unwrap();
        assert_eq!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        assert_eq!(notifications.recv().unwrap().name, "0");
        assert!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        thread.join().unwrap();
        // The queue is full again. Closing the receiving owner must release
        // a producer already blocked in send, not just reject future sends.
        let producer = Arc::clone(&syncer);
        let (entered_tx, entered_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let blocked = std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            done_tx
                .send(producer.notify(item("blocked-at-close")))
                .unwrap();
        });
        entered_rx.recv().unwrap();
        assert_eq!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        syncer.stop();
        assert!(notifications.recv().is_none());
        drop(notifications);
        assert!(!done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        blocked.join().unwrap();
        assert!(!syncer.notify(item("closed")));
    }
}
