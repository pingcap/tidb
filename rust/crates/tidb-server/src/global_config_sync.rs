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

//! Go Domain.globalConfigSyncerKeeper and its process lifetime.

use std::sync::Arc;
use std::thread::JoinHandle;
use tidb_domain::globalconfigsync::{GlobalConfigItem, GlobalConfigStore, GlobalConfigSyncer};

struct PdStore(tidb_pd_client::PdClient);

impl GlobalConfigStore for PdStore {
    fn store_global_config(&self, path: &str, items: Vec<GlobalConfigItem>) -> Result<(), String> {
        self.0
            .store_global_config(path, items)
            .map_err(|error| error.to_string())
    }
}

/// A single node-owned keeper, joined before the PD process authority closes.
pub(crate) struct GlobalConfigKeeper {
    syncer: Arc<GlobalConfigSyncer>,
    worker: Option<JoinHandle<()>>,
}

impl GlobalConfigKeeper {
    pub(crate) fn start(pd: Option<tidb_pd_client::PdClient>) -> std::io::Result<Self> {
        Self::with_store(pd.map(|pd| Arc::new(PdStore(pd)) as Arc<dyn GlobalConfigStore>))
    }

    fn with_store(pd: Option<Arc<dyn GlobalConfigStore>>) -> std::io::Result<Self> {
        let (syncer, notifications) = GlobalConfigSyncer::new(pd);
        let running = Arc::clone(&syncer);
        let worker = std::thread::Builder::new()
            .name("global-config-syncer".into())
            .spawn(move || {
                while let Some(item) = notifications.recv() {
                    if let Err(error) = running.store_global_config(item) {
                        // Go logs and continues. Retrying here could overwrite a
                        // newer value published by another TiDB node.
                        eprintln!("{}", serde_json::json!({"event": "global_config_syncer_store_failed", "error": error}));
                    }
                }
            })?;
        Ok(Self {
            syncer,
            worker: Some(worker),
        })
    }

    pub(crate) fn syncer(&self) -> Arc<GlobalConfigSyncer> {
        Arc::clone(&self.syncer)
    }
}

impl Drop for GlobalConfigKeeper {
    fn drop(&mut self) {
        self.syncer.stop();
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                eprintln!("global config syncer panicked");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;
    struct Store(mpsc::Sender<GlobalConfigItem>);
    impl GlobalConfigStore for Store {
        fn store_global_config(
            &self,
            path: &str,
            items: Vec<GlobalConfigItem>,
        ) -> Result<(), String> {
            assert_eq!(path, "");
            assert_eq!(items.len(), 1);
            self.0.send(items[0].clone()).unwrap();
            if items[0].name == "failed" {
                Err("PD failed".into())
            } else {
                Ok(())
            }
        }
    }

    /// Original Go TestStoreGlobalConfig, including the domain keeper.
    #[test]
    fn global_config_keeper_consumes_sql_and_continues_after_a_failed_store() {
        let (send, receive) = mpsc::channel();
        let keeper = GlobalConfigKeeper::with_store(Some(Arc::new(Store(send)))).unwrap();
        let syncer = keeper.syncer();
        syncer.notify(GlobalConfigItem {
            name: "failed".into(),
            ..Default::default()
        });
        let mut session = tidb_session::Session::new();
        session.set_global_config_syncer(Arc::clone(&syncer));
        session.run("SET GLOBAL tidb_enable_top_sql=1").unwrap();
        session.run("SET GLOBAL tidb_source_id=2").unwrap();
        let actual: Vec<_> = (0..3)
            .map(|_| {
                let item = receive.recv_timeout(Duration::from_secs(2)).unwrap();
                (item.name, item.value)
            })
            .collect();
        assert_eq!(
            actual,
            vec![
                ("failed".into(), String::new()),
                ("enable_resource_metering".into(), "true".into()),
                ("source_id".into(), "2".into())
            ]
        );
        drop(keeper);
        assert!(!syncer.notify(GlobalConfigItem::default()));
        assert!(receive.try_recv().is_err());
    }
    #[test]
    fn global_config_keeper_close_waits_for_the_in_flight_store() {
        struct BlockingStore {
            started: mpsc::Sender<()>,
            finish: std::sync::Mutex<mpsc::Receiver<()>>,
        }
        impl GlobalConfigStore for BlockingStore {
            fn store_global_config(&self, _: &str, _: Vec<GlobalConfigItem>) -> Result<(), String> {
                self.started.send(()).unwrap();
                self.finish.lock().unwrap().recv().unwrap();
                Ok(())
            }
        }
        let (started_tx, started_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let keeper = GlobalConfigKeeper::with_store(Some(Arc::new(BlockingStore {
            started: started_tx,
            finish: std::sync::Mutex::new(finish_rx),
        })))
        .unwrap();
        let syncer = keeper.syncer();
        assert!(syncer.notify(GlobalConfigItem::default()));
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let closer = std::thread::spawn(move || {
            drop(keeper);
            done_tx.send(()).unwrap();
        });
        assert_eq!(
            done_rx.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        );
        finish_tx.send(()).unwrap();
        done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        closer.join().unwrap();
        assert!(!syncer.notify(GlobalConfigItem::default()));
    }
}
