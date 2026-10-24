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

//! The node's `getServerInfo`, and the tests of the production etcd binding
//! ([`EtcdClientOps`], which lives in `tidb-ddl` beside the schema-version
//! syncer that shares it).

use std::sync::Arc;

pub use tidb_ddl::etcd_ops::EtcdClientOps;

/// Go `getServerInfo` over the node's own configuration.
///
/// Reuses the domain constructor over the canonical effective config, including
/// advertised address, lease text and labels, instead of a second projection.
pub fn node_server_info(
    config: &crate::domain::node_config::NodeConfig,
) -> tidb_domain::serverinfo::ServerInfo {
    tidb_domain::serverinfo_syncer::server_info_from_config(
        &tidb_domain::serverinfo_syncer::new_node_id(),
        &config.global_config,
        tidb_util::versioninfo::TIDB_GIT_HASH,
        config
            .global_config
            .enable_global_kill
            .then(|| Arc::new(|| 1u64) as Arc<dyn Fn() -> u64 + Send + Sync>),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs() as i64),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tidb_domain::server_id::{ServerIdAuthority, ServerIdIntervals, ServerIdKeeper};
    use tidb_domain::serverinfo_syncer::{EtcdOps, Syncer};

    // Go domain/serverinfo tests embed etcd. The Rust integration uses the
    // official executable, explicitly selected so ordinary unit runs need no service.
    #[test]
    #[ignore = "requires etcd on PATH; run with --include-ignored"]
    fn cluster_lifecycle_batch_real_etcd_claims_recovery_and_minimum_lease() {
        struct EtcdProcess {
            child: std::process::Child,
            path: std::path::PathBuf,
        }
        impl Drop for EtcdProcess {
            fn drop(&mut self) {
                let _ = self.child.kill();
                let _ = self.child.wait();
                let _ = std::fs::remove_dir_all(&self.path);
            }
        }
        fn endpoint() -> String {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            format!("http://{}", listener.local_addr().unwrap())
        }
        let address = endpoint();
        let peer = endpoint();
        let path = std::env::temp_dir().join(format!("tidb-identity-etcd-{}", std::process::id()));
        std::fs::create_dir(&path).expect("unique integration data directory");
        let child = std::process::Command::new("etcd")
            .args([
                "--data-dir",
                path.to_str().unwrap(),
                "--listen-client-urls",
                &address,
                "--advertise-client-urls",
                &address,
                "--listen-peer-urls",
                &peer,
                "--initial-advertise-peer-urls",
                &peer,
                "--initial-cluster",
                &format!("default={peer}"),
                "--log-level",
                "error",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let _process = EtcdProcess { child, path };
        let client =
            tidb_pd_client::EtcdClient::connect([address], std::time::Duration::from_secs(1))
                .unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while client.get(b"/ready").is_err() {
            assert!(std::time::Instant::now() < deadline, "etcd startup failed");
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let etcd = Arc::new(EtcdClientOps::new(Arc::new(client.clone())));
        let create = |name: &str| {
            let identity = ServerIdAuthority::new(Some(etcd.clone()), true);
            let get = identity.clone();
            let mut info = tidb_domain::serverinfo::ServerInfo::default();
            info.static_info.id = name.into();
            info.static_info.server_id_getter = Some(Arc::new(move || get.id()));
            let syncer = Arc::new(Syncer::new_with_status_endpoint_claim(
                info,
                Some(etcd.clone()),
                false,
            ));
            syncer.new_session_and_store_server_info().unwrap();
            let keeper = ServerIdKeeper::start(
                identity.clone(),
                syncer.clone(),
                ServerIdIntervals {
                    refresh: std::time::Duration::from_millis(20),
                    retry: std::time::Duration::from_millis(20),
                    ..Default::default()
                },
            )
            .unwrap();
            (identity, syncer, keeper)
        };
        let (first, info, keeper) = create("first");
        let (second, _, other_keeper) = create("second");
        assert_ne!(first.id(), second.id());
        let key = format!("/tidb/server_id/{}", first.id());
        let claim = client
            .get_prefix_metadata(key.as_bytes())
            .unwrap()
            .into_iter()
            .find(|kv| kv.key == key.as_bytes())
            .unwrap();
        assert!(!etcd
            .create_if_absent_with_lease(&key, b"0", info.session_lease().unwrap())
            .unwrap());
        let kills = Arc::new(AtomicUsize::new(0));
        let observed = kills.clone();
        first.set_connection_killer(Some(Arc::new(move || {
            observed.fetch_add(1, Ordering::SeqCst);
        })));
        client.lease_revoke(claim.lease).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if kills.load(Ordering::SeqCst) == 1 && !first.is_lost() {
                let bytes = client
                    .get(info.server_info_path().as_bytes())
                    .unwrap()
                    .unwrap();
                let published: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(published["server_id"], first.id());
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "identity recovery failed"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        info.store_min_start_ts(12345).unwrap();
        let minimum = b"/tidb/server/minstartts/first";
        assert_eq!(client.get(minimum).unwrap().unwrap(), b"12345");
        client.lease_revoke(info.session_lease().unwrap()).unwrap();
        assert!(client.get(minimum).unwrap().is_none());
        info.restart().unwrap();
        info.store_min_start_ts(23456).unwrap();
        assert_eq!(client.get(minimum).unwrap().unwrap(), b"23456");
        let retired = format!("/tidb/server_id/{}", first.id());
        drop(keeper);
        assert!(first.is_lost());
        assert!(client.get(retired.as_bytes()).unwrap().is_none());
        assert!(!second.is_lost());
        drop(other_keeper);
        info.remove_min_start_ts();
        assert!(client.get(minimum).unwrap().is_none());
    }
}
