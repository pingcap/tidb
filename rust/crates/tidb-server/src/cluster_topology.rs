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

//! Go Domain.closestReplicaReadCheckLoop: immediate check, minute cadence, joined shutdown.

use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Duration;
use tidb_domain::cluster_topology::ClusterTopology;
use tidb_session::GlobalSysvars;

pub(crate) struct ReplicaReadChecker {
    stop: mpsc::Sender<()>,
    worker: Option<JoinHandle<()>>,
}

impl ReplicaReadChecker {
    pub(crate) fn start(
        topology: Arc<ClusterTopology>,
        globals: GlobalSysvars,
    ) -> std::io::Result<Self> {
        Self::start_with_interval(topology, globals, Duration::from_secs(60))
    }

    fn start_with_interval(
        topology: Arc<ClusterTopology>,
        globals: GlobalSysvars,
        interval: Duration,
    ) -> std::io::Result<Self> {
        let (stop, stopped) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("closest-replica-read".into())
            .spawn(move || loop {
                let setting = globals.get("tidb_replica_read").unwrap_or_default();
                if let Err(error) = topology.check_replica_read(&setting) {
                    eprintln!(
                        "{}",
                        serde_json::json!({"event": "check_replica_read_failed", "error": error})
                    );
                }
                match stopped.recv_timeout(interval) {
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    _ => break,
                }
            })?;
        Ok(Self {
            stop,
            worker: Some(worker),
        })
    }
}

impl Drop for ReplicaReadChecker {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                eprintln!("closest replica-read checker panicked");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_domain::cluster_topology::{ClusterDiscovery, ClusterServer, ClusterStore};
    struct Discovery(mpsc::Sender<()>);
    impl ClusterDiscovery for Discovery {
        fn pd_servers(&self, _: &mut Vec<String>) -> Result<Vec<ClusterServer>, String> {
            unreachable!()
        }
        fn stores(&self) -> Result<Vec<ClusterStore>, String> {
            self.0.send(()).unwrap();
            Ok(Vec::new())
        }
        fn microservice_servers(
            &self,
            _: &str,
            _: &mut Vec<String>,
        ) -> Result<Vec<ClusterServer>, String> {
            unreachable!()
        }
    }
    #[test]
    fn domain_checker_reads_live_global_setting_and_joins() {
        let mut info = tidb_domain::serverinfo::ServerInfo::default();
        info.dynamic_info.labels.insert("zone".into(), "z1".into());
        let syncer = Arc::new(tidb_domain::serverinfo_syncer::Syncer::new(info, None));
        let (called, calls) = mpsc::channel();
        let topology = Arc::new(ClusterTopology::new(
            syncer,
            Some(Arc::new(Discovery(called))),
        ));
        let globals = GlobalSysvars::default();
        globals
            .set("tidb_replica_read", "closest-adaptive".into())
            .unwrap();
        let worker = ReplicaReadChecker::start_with_interval(
            topology.clone(),
            globals.clone(),
            Duration::from_millis(20),
        )
        .unwrap();
        calls.recv_timeout(Duration::from_secs(2)).unwrap();
        calls.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(!topology.adaptive_enabled());
        globals.set("tidb_replica_read", "leader".into()).unwrap();
        drop(worker);
        while calls.try_recv().is_ok() {}
        assert!(matches!(
            calls.recv_timeout(Duration::from_millis(60)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        assert!(!topology.adaptive_enabled());
    }
}
