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

//! The TiFlash replica availability poller: Go
//! `pkg/ddl/ddl_tiflash_api.go` supplies its lifecycle and availability policy.
//! This existing classic (NULL-keyspace, API V1) path still retains legacy rule
//! reconciliation pending migration to the DDL/GC owners.
//!
//! Every tick, for each table whose `TiFlashReplica` is set but not yet
//! available:
//!
//! 1. the table's region count comes from PD HTTP
//!    `/stats/region?start_key=&end_key=` over the record range
//!    (`infosync.GetRegionCountFromPD`);
//! 2. each live TiFlash store's STATUS address answers
//!    `/tiflash/sync-status/keyspace/{NullspaceID}/table/{id}`
//!    (`helper.CollectTiFlashStatusWithCtx`); the response lists the
//!    regions that already carry a learner peer on that store;
//! 3. `availProgress = |regions with >= 1 tiflash peer| / regionCount`
//!    (`calculateTiFlashProgress`, tiflash_manager.go:135); availability is
//!    `availProgress >= 1.0` (ddl_tiflash_api.go:500);
//! 4. the flip publishes through the same DDL transaction path Go uses
//!    (`UpdateTableReplicaInfo` -> `ActionUpdateTiFlashReplicaStatus`).

use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::time::Duration;

use tidb_pd_client::{PdStore, PdStoreState};

/// Go `NullspaceID` (`tikv/keyspace`): the keyspace id a classic API-V1
/// cluster dispatches and reports under.
pub const NULL_KEYSPACE_ID: u32 = 4294967295;

/// Go `DefaultTiFlashPollInterval` (ddl_tiflash_api.go: `2 * time.Second`).
const POLL_INTERVAL: Duration = Duration::from_secs(2);
// PD's HTTP client and TiDB's InternalHTTPClient have different deadlines.
const PD_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
const TIFLASH_HTTP_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// DDL capabilities used by the poller; the DDL executor owns publication.
pub trait TiFlashReplicaControl: Send + Sync {
    /// Whether this node currently owns DDL work.
    fn is_owner(&self) -> bool;
    /// Go UpdateTableReplicaInfo through the ordinary DDL execution path.
    fn update_replica_status(&self, table_id: i64, available: bool) -> Result<(), String>;
}

/// One poller over the node's catalog and PD membership. It owns no transaction.
pub struct TiFlashReplicaManager {
    catalog: Arc<crate::catalog_watch::SharedCatalog>,
    stores: Box<dyn Fn() -> Result<Vec<PdStore>, String> + Send + Sync>,
    ddl: Arc<dyn TiFlashReplicaControl>,
    pd_http: String,
    http: reqwest::blocking::Client,
}

/// Node-owned worker, stopped and joined before DDL and PD are shut down.
#[must_use = "retain the poller until node shutdown; dropping it stops the worker"]
pub struct TiFlashReplicaPoller {
    stop: mpsc::Sender<()>,
    worker: Option<JoinHandle<()>>,
}

impl TiFlashReplicaPoller {
    fn start(interval: Duration, mut poll: impl FnMut() + Send + 'static) -> Self {
        let (stop, stopped) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("tiflash-replica-poll".to_owned())
            .spawn(move || {
                while matches!(
                    stopped.recv_timeout(interval),
                    Err(mpsc::RecvTimeoutError::Timeout)
                ) {
                    poll();
                }
            })
            .expect("the TiFlash replica poller starts");
        Self {
            stop,
            worker: Some(worker),
        }
    }

    /// Wakes an idle poller and joins any current pass. Idempotent.
    pub fn shutdown(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                eprintln!("TiFlash replica poll worker panicked");
            }
        }
    }
}

impl Drop for TiFlashReplicaPoller {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl TiFlashReplicaManager {
    /// Binds discovery and DDL capabilities without opening another transaction path.
    pub fn new(
        catalog: Arc<crate::catalog_watch::SharedCatalog>,
        stores: impl Fn() -> Result<Vec<PdStore>, String> + Send + Sync + 'static,
        ddl: Arc<dyn TiFlashReplicaControl>,
        pd_http: String,
    ) -> Result<Self, String> {
        // Reuse framing and connections. Each call supplies the deadline of
        // its Go owner: PD HTTP client or TiDB util.InternalHTTPClient.
        let http = reqwest::blocking::Client::builder()
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            catalog,
            stores: Box::new(stores),
            ddl,
            pd_http,
            http,
        })
    }

    /// Starts one worker whose returned owner must be retained until node shutdown.
    pub fn spawn(self) -> TiFlashReplicaPoller {
        TiFlashReplicaPoller::start(POLL_INTERVAL, move || {
            if let Err(error) = self.poll_once() {
                eprintln!("tiflash replica poll: {error}");
            }
        })
    }

    fn http_call(
        &self,
        method: &str,
        url: &str,
        body: Option<&str>,
        timeout: Duration,
    ) -> Result<(u16, String), String> {
        let method =
            reqwest::Method::from_bytes(method.as_bytes()).map_err(|error| error.to_string())?;
        let mut request = self.http.request(method, url).timeout(timeout);
        if let Some(body) = body {
            request = request
                .header("Content-Type", "application/json")
                .body(body.to_owned());
        }
        let response = request.send().map_err(|error| error.to_string())?;
        let status = response.status().as_u16();
        let body = response.text().map_err(|error| error.to_string())?;
        Ok((status, body))
    }

    /// One poll pass: sync placement rules for replica tables, then flip
    /// availability for tables whose learners are all in place.
    fn poll_once(&self) -> Result<(), String> {
        if !self.ddl.is_owner() {
            return Ok(());
        }
        let endpoint = self.pd_http.trim_end_matches('/').to_owned();
        let catalog = self.catalog.load();
        let stores: Vec<_> = (self.stores)()?
            .into_iter()
            .filter(|store| {
                store.state == PdStoreState::Up
                    && store.labels.iter().any(|(key, value)| {
                        key.eq_ignore_ascii_case("engine") && value.eq_ignore_ascii_case("tiflash")
                    })
            })
            .collect();
        if stores.is_empty() {
            return Ok(());
        }
        // Collect EVERY replica-carrying table first: the rule pass needs the
        // full desired set (available tables keep their rule too), while the
        // progress pass only advances not-yet-available ones.
        let mut replica_tables: Vec<(i64, u64, Vec<String>, bool)> = Vec::new();
        for database in &catalog.databases {
            for stored in &database.tables {
                let Some(replica) = stored.tiflash_replica.as_ref() else {
                    continue;
                };
                let replica = replica.read();
                if replica.count == 0 {
                    continue;
                }
                replica_tables.push((
                    stored.id,
                    replica.count,
                    replica.location_labels.iter().cloned().collect(),
                    replica.available,
                ));
            }
        }

        // Legacy rule reconciliation remains here until configuration and
        // cleanup migrate to the DDL/GC owners. Go classic clusters do not
        // run refreshTiFlashPlacementRules in this poller.
        let (rules_status, rules_body) = self.http_call(
            "GET",
            &format!("{endpoint}/pd/api/v1/config/rules/group/tiflash"),
            None,
            PD_HTTP_TIMEOUT,
        )?;
        if rules_status == 200 {
            // PD may return null for an empty rules group.
            let existing: Vec<serde_json::Value> = match serde_json::from_str(&rules_body) {
                Ok(list) => list,
                Err(_) => Vec::new(),
            };
            for rule in existing {
                let id = rule
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .to_owned();
                let desired = replica_tables
                    .iter()
                    .any(|(table_id, _, _, _)| *id == format!("table-{table_id}-r"));
                if desired || id.is_empty() {
                    continue;
                }
                let (delete_status, delete_body) = self.http_call(
                    "DELETE",
                    &format!("{endpoint}/pd/api/v1/config/rule/tiflash/{id}"),
                    None,
                    PD_HTTP_TIMEOUT,
                )?;
                eprintln!(
                    "{{\"event\":\"tiflash_rule_removed\",\"rule\":{id:?},\"status\":{delete_status},\"detail\":{delete_body:?}}}"
                );
            }
        }

        if !replica_tables.is_empty() {
            self.ensure_rule_group(&endpoint)?;
        }
        for (table_id, count, labels, available) in &replica_tables {
            let table_id = *table_id;
            let count = *count;

            // Set one rule, like infosync.SetPlacementRule. A bundle for
            // the shared "tiflash" group would replace every sibling rule.
            let rule = new_tiflash_rule(table_id, count, labels);
            let rule_body = serde_json::to_string(&rule).map_err(|error| error.to_string())?;
            let (status, _) = self.http_call(
                "POST",
                &format!("{endpoint}/pd/api/v1/config/rule"),
                Some(&rule_body),
                PD_HTTP_TIMEOUT,
            )?;
            if status != 200 {
                return Err(format!("placement rule sync refused by PD ({status})"));
            }

            // Go `PostAccelerateScheduleBatch` (tiflash_manager.go:317):
            // nudge PD to schedule the table's regions onto the TiFlash
            // store; without this the learner peers can wait on the
            // balance loop.
            // The accelerate-schedule range is the SAME
            // `EncodeBytes`-escaped range the placement rule carries.
            let (accel_start, accel_end) = table_region_range(table_id);
            let (accel_status, _) = self.http_call(
                "POST",
                &format!("{endpoint}/pd/api/v1/regions/accelerate-schedule/batch"),
                Some(&format!(
                    "[{{\"start_key\":\"{}\",\"end_key\":\"{}\"}}]",
                    hex_upper(&accel_start),
                    hex_upper(&accel_end)
                )),
                PD_HTTP_TIMEOUT,
            )?;
            if accel_status != 200 {
                eprintln!("{{\"event\":\"tiflash_accelerate_refused\",\"status\":{accel_status}}}");
            }

            if *available {
                continue;
            }
            let (one_replica_progress, full_progress) =
                match self.progress(table_id, count, &endpoint, &stores) {
                    Ok(progress) => progress,
                    Err(error) => {
                        eprintln!("TiFlash progress for table {table_id}: {error}");
                        continue;
                    }
                };
            // Go ddl_tiflash_api.go:500: `avail = availProgress >= 1.0`,
            // where availProgress is the ONE-replica progress: every
            // region carries at least one learner peer.
            let available = one_replica_progress >= 1.0;
            eprintln!(
                "{{\"event\":\"tiflash_replica_progress\",\"table_id\":{table_id},\
                     \"available\":{available},\"one\":{one_replica_progress},\
                     \"full\":{full_progress}}}"
            );
            if available {
                if let Err(error) = self.ddl.update_replica_status(table_id, true) {
                    eprintln!("updating TiFlash replica status for table {table_id}: {error}");
                }
            }
        }
        Ok(())
    }

    /// Go `calculateTiFlashProgress`: `(one-replica progress, full progress)`.
    fn progress(
        &self,
        table_id: i64,
        replica_count: u64,
        endpoint: &str,
        stores: &[tidb_pd_client::PdStore],
    ) -> Result<(f64, f64), String> {
        let (start_key, end_key) = table_region_range(table_id);
        let url = format!(
            "{endpoint}/pd/api/v1/stats/region?start_key={}&end_key={}&count",
            escape_key(&start_key),
            escape_key(&end_key),
        );
        let (status, body) = self.http_call("GET", &url, None, PD_HTTP_TIMEOUT)?;
        if status != 200 {
            return Err(format!("stats/region answered {status}"));
        }
        let body: serde_json::Value =
            serde_json::from_str(&body).map_err(|error| format!("stats/region body: {error}"))?;
        let region_count = body
            .get("count")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or_default();
        if region_count == 0 {
            return Err("region count getting from PD is 0".to_owned());
        }

        // Go `getTiFlashPeerWithoutLagCount`: union the per-store region
        // reports; a region counts once even when several stores hold it.
        let mut regions_with_peer = std::collections::HashSet::new();
        let mut peer_count = 0usize;
        for store in stores {
            let url = format!(
                "http://{}/tiflash/sync-status/keyspace/{NULL_KEYSPACE_ID}/table/{table_id}",
                store.status_address,
            );
            // CollectTiFlashStatusWithCtx parses the body regardless of HTTP status.
            let (_, sync_body) = self.http_call("GET", &url, None, TIFLASH_HTTP_TIMEOUT)?;
            // Go counts unique regions per store, then unions across stores.
            let store_regions: std::collections::HashSet<_> =
                parse_sync_status(&sync_body)?.into_iter().collect();
            peer_count += store_regions.len();
            regions_with_peer.extend(store_regions);
        }
        let region_count = region_count as f64;
        let one_replica_progress = regions_with_peer.len() as f64 / region_count;
        let full_progress = (peer_count as f64 / (region_count * replica_count as f64)).min(1.0);
        Ok((one_replica_progress, full_progress))
    }

    // Go infosync.SetTiFlashGroupConfig: group priority must precede individual rules.
    fn ensure_rule_group(&self, endpoint: &str) -> Result<(), String> {
        let (status, body) = self.http_call(
            "GET",
            &format!("{endpoint}/pd/api/v1/config/rule_group/tiflash"),
            None,
            PD_HTTP_TIMEOUT,
        )?;
        if status != 200 {
            return Err(format!("TiFlash rule group lookup answered {status}"));
        }
        let group: Option<RuleGroup> =
            serde_json::from_str(&body).map_err(|error| error.to_string())?;
        if group.is_some_and(|group| {
            group.index == tidb_placement::RULE_INDEX_TIFLASH && !group.r#override
        }) {
            return Ok(());
        }
        let body = serde_json::to_string(&RuleGroup {
            id: tidb_placement::TIFLASH_RULE_GROUP_ID.to_owned(),
            index: tidb_placement::RULE_INDEX_TIFLASH,
            r#override: false,
        })
        .map_err(|error| error.to_string())?;
        let (status, _) = self.http_call(
            "POST",
            &format!("{endpoint}/pd/api/v1/config/rule_group"),
            Some(&body),
            PD_HTTP_TIMEOUT,
        )?;
        if status != 200 {
            return Err(format!("TiFlash rule group update answered {status}"));
        }
        Ok(())
    }
}

#[derive(Default, serde::Deserialize, serde::Serialize)]
#[serde(default)]
struct RuleGroup {
    id: String,
    index: i64,
    r#override: bool,
}

/// Go helper.ComputeTiFlashStatus: a count line followed by space-separated IDs.
fn parse_sync_status(body: &str) -> Result<Vec<i64>, String> {
    let mut lines = body.split_inclusive('\n');
    let mut next_line = || {
        lines
            .next()
            .filter(|line| line.ends_with('\n'))
            .map(|line| line.trim_matches(['\r', '\n', '\t']))
            .ok_or_else(|| "incomplete TiFlash sync-status response".to_owned())
    };
    let expected: i64 = next_line()?
        .parse()
        .map_err(|error| format!("TiFlash region count: {error}"))?;
    let regions: Vec<i64> = next_line()?
        .split(' ')
        .filter(|id| !id.is_empty())
        .map(|id| {
            id.parse()
                .map_err(|error| format!("TiFlash region ID: {error}"))
        })
        .collect::<Result<_, _>>()?;
    // Go warns on count disagreement but accepts the successfully parsed IDs.
    if expected != regions.len() as i64 {
        eprintln!(
            "TiFlash sync-status count check failed: claimed {expected}, read {}",
            regions.len()
        );
    }
    Ok(regions)
}

fn table_region_range(table_id: i64) -> (Vec<u8>, Vec<u8>) {
    let mut start = Vec::new();
    let mut end = Vec::new();
    tidb_codec::encode_bytes(&mut start, &tidb_codec::gen_table_record_prefix(table_id));
    tidb_codec::encode_bytes(
        &mut end,
        &tidb_codec::table_key::encode_table_prefix(table_id + 1),
    );
    (start, end)
}

/// Go infosync.MakeNewRule followed by the classic codec's EncodeRegionRange.
fn new_tiflash_rule(table_id: i64, count: u64, labels: &[String]) -> tidb_placement::pd::Rule {
    let (start, end) = table_region_range(table_id);
    tidb_placement::pd::Rule {
        group_id: tidb_placement::TIFLASH_RULE_GROUP_ID.to_owned(),
        id: format!("table-{table_id}-r"),
        index: tidb_placement::RULE_INDEX_TIFLASH,
        start_key_hex: hex_upper(&start),
        end_key_hex: hex_upper(&end),
        role: tidb_placement::pd::PeerRoleType::LEARNER,
        is_witness: false,
        count: count as i64,
        label_constraints: vec![tidb_placement::pd::LabelConstraint {
            key: tidb_placement::ENGINE_LABEL_KEY.to_owned(),
            op: tidb_placement::pd::LabelConstraintOp::IN,
            values: vec![tidb_placement::ENGINE_LABEL_TIFLASH.to_owned()],
        }],
        location_labels: labels.to_vec(),
    }
}

// The PD region-stats query takes escaped binary keys, not hex strings.
// Percent-encode bytes directly; converting the memcomparable keys to UTF-8
// would replace marker bytes and change the range.
fn escape_key(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("%{byte:02X}")).collect()
}

fn hex_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Ddl {
        owner: AtomicBool,
        updates: Mutex<Vec<(i64, bool)>>,
    }
    impl TiFlashReplicaControl for Ddl {
        fn is_owner(&self) -> bool {
            self.owner.load(Ordering::SeqCst)
        }
        fn update_replica_status(&self, table_id: i64, available: bool) -> Result<(), String> {
            self.updates.lock().unwrap().push((table_id, available));
            Ok(())
        }
    }

    fn manager(ddl: Arc<Ddl>, calls: Arc<AtomicUsize>) -> TiFlashReplicaManager {
        let catalog = Arc::new(crate::catalog_watch::SharedCatalog::new(
            crate::cluster_catalog::ClusterCatalog {
                schema_version: 1,
                databases: vec![],
            },
        ));
        TiFlashReplicaManager::new(
            catalog,
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![])
            },
            ddl,
            "http://127.0.0.1:1".to_owned(),
        )
        .unwrap()
    }

    #[test]
    fn only_the_current_ddl_owner_polls_and_ownership_can_change() {
        let ddl = Arc::new(Ddl::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let poller = manager(ddl.clone(), calls.clone());
        poller.poll_once().unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "followers must not poll or publish"
        );
        ddl.owner.store(true, Ordering::SeqCst);
        poller.poll_once().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        ddl.owner.store(false, Ordering::SeqCst);
        poller.poll_once().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// Go helper.TestComputeTiFlashStatus: the first line is a count, not a region.
    #[test]
    fn status_response_uses_a_count_then_a_space_separated_region_line() {
        assert!(parse_sync_status("0\n\n").unwrap().is_empty());
        assert_eq!(
            parse_sync_status("2\n1009 1010 \n").unwrap(),
            vec![1009, 1010]
        );
        let regions = (1000..3000)
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(" ");
        assert_eq!(
            parse_sync_status(&format!("2000\n{regions} \n")).unwrap(),
            (1000..3000).collect::<Vec<_>>()
        );
    }

    struct HttpFixture {
        address: String,
        stop: mpsc::Sender<()>,
        worker: Option<JoinHandle<()>>,
        requests: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl HttpFixture {
        fn new(reply: impl Fn(&str) -> (u16, String) + Send + 'static) -> Self {
            use std::io::{BufRead, Read, Write};
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap().to_string();
            listener.set_nonblocking(true).unwrap();
            let (stop, stopped) = mpsc::channel();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let recorded = requests.clone();
            let worker = std::thread::spawn(move || loop {
                let (mut stream, _) = match listener.accept() {
                    Ok(stream) => stream,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if !matches!(
                            stopped.recv_timeout(Duration::from_millis(5)),
                            Err(mpsc::RecvTimeoutError::Timeout)
                        ) {
                            break;
                        }
                        continue;
                    }
                    Err(error) => panic!("accept: {error}"),
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut reader = std::io::BufReader::new(&mut stream);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    assert!(
                        reader.read_line(&mut line).unwrap() > 0,
                        "incomplete HTTP headers"
                    );
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let path = request.split_whitespace().nth(1).unwrap().to_owned();
                recorded
                    .lock()
                    .unwrap()
                    .push((path.clone(), String::from_utf8(body).unwrap()));
                let (status, body) = reply(&path);
                // Exercise response framing that the removed raw TCP reader could not decode.
                write!(stream, "HTTP/1.1 {status} fixture\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n", body.len()).unwrap();
            });
            Self {
                address,
                stop,
                worker: Some(worker),
                requests,
            }
        }

        fn store(&self) -> PdStore {
            PdStore {
                id: 1,
                address: String::new(),
                status_address: self.address.clone(),
                state: PdStoreState::Up,
                node_state: tidb_pd_client::PdNodeState::Serving,
                labels: vec![("engine".into(), "tiflash".into())],
            }
        }
    }

    impl Drop for HttpFixture {
        fn drop(&mut self) {
            let _ = self.stop.send(());
            self.worker.take().unwrap().join().unwrap();
        }
    }

    fn replica_catalog(tables: &[(i64, bool)]) -> Arc<crate::catalog_watch::SharedCatalog> {
        Arc::new(crate::catalog_watch::SharedCatalog::new(
            crate::cluster_catalog::ClusterCatalog {
                schema_version: 1,
                databases: vec![crate::cluster_catalog::LoadedDatabase {
                    info: tidb_model::db::DBInfo::default(),
                    tables: tables
                        .iter()
                        .map(|&(id, available)| tidb_model::table_info::TableInfo {
                            id,
                            tiflash_replica: Some(tidb_model::GoShared::new(
                                tidb_model::table::TiFlashReplicaInfo {
                                    count: 2,
                                    available,
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        })
                        .collect(),
                }],
            },
        ))
    }

    fn successful_response(path: &str) -> (u16, String) {
        (
            200,
            if path.starts_with("/pd/api/v1/stats/region?") {
                r#"{"count":2}"#.to_owned()
            } else if path.starts_with("/tiflash/sync-status/") {
                "2\n1009 1010 \n".to_owned()
            } else if path == "/pd/api/v1/config/rule_group/tiflash" {
                "null".to_owned()
            } else {
                "[]".to_owned()
            },
        )
    }

    fn fixture_manager(
        http: &HttpFixture,
        ddl: Arc<Ddl>,
        tables: &[(i64, bool)],
    ) -> TiFlashReplicaManager {
        let store = http.store();
        TiFlashReplicaManager::new(
            replica_catalog(tables),
            move || Ok(vec![store.clone()]),
            ddl,
            format!("http://{}", http.address),
        )
        .unwrap()
    }

    #[test]
    fn per_table_rule_updates_preserve_sibling_rules_in_the_tiflash_group() {
        let http = HttpFixture::new(successful_response);
        let ddl = Arc::new(Ddl::default());
        ddl.owner.store(true, Ordering::SeqCst);
        fixture_manager(&http, ddl, &[(7, false), (8, false)])
            .poll_once()
            .unwrap();
        let requests = http.requests.lock().unwrap();
        assert!(
            !requests
                .iter()
                .any(|(path, _)| path.contains("placement-rule")),
            "a group bundle replaces sibling table rules"
        );
        let rules: Vec<serde_json::Value> = requests
            .iter()
            .filter(|(path, _)| path == "/pd/api/v1/config/rule")
            .map(|(_, body)| serde_json::from_str(body).unwrap())
            .collect();
        assert_eq!(rules.len(), 2);
        let group_index = requests
            .iter()
            .position(|(path, _)| path == "/pd/api/v1/config/rule_group")
            .unwrap();
        let first_rule_index = requests
            .iter()
            .position(|(path, _)| path == "/pd/api/v1/config/rule")
            .unwrap();
        assert!(group_index < first_rule_index);
        let group: serde_json::Value = serde_json::from_str(&requests[group_index].1).unwrap();
        assert_eq!(
            group,
            serde_json::json!({"id": "tiflash", "index": 120, "override": false})
        );
        assert_eq!(rules[0]["id"], "table-7-r");
        assert_eq!(rules[1]["id"], "table-8-r");
        for rule in rules {
            assert_eq!(rule["group_id"], "tiflash");
            assert_eq!(rule["index"], 120);
            assert_eq!(rule["count"], 2);
        }
    }

    #[test]
    fn availability_publication_uses_ddl_and_skips_already_available_tables() {
        let http = HttpFixture::new(successful_response);
        let ddl = Arc::new(Ddl::default());
        ddl.owner.store(true, Ordering::SeqCst);
        fixture_manager(&http, ddl.clone(), &[(7, true), (8, false)])
            .poll_once()
            .unwrap();
        assert_eq!(*ddl.updates.lock().unwrap(), vec![(8, true)]);
        assert!(
            !http
                .requests
                .lock()
                .unwrap()
                .iter()
                .any(|(path, _)| path.ends_with("/table/7")),
            "available tables must not be published again"
        );
    }

    #[test]
    fn progress_counts_each_store_region_once_and_uses_replica_count() {
        let http = HttpFixture::new(|path| {
            if path.starts_with("/tiflash/") {
                (200, "3\n1009 1009 1010\n".to_owned())
            } else {
                successful_response(path)
            }
        });
        let poller = fixture_manager(&http, Arc::new(Ddl::default()), &[]);
        assert_eq!(
            poller
                .progress(7, 2, &poller.pd_http, &[http.store()])
                .unwrap(),
            (1.0, 0.5)
        );
        assert_eq!(
            poller
                .progress(7, 1, &poller.pd_http, &[http.store(), http.store()])
                .unwrap(),
            (1.0, 1.0)
        );
        // Go's stats API consumes escaped binary memcomparable keys, not raw-key hex.
        let requests = http.requests.lock().unwrap();
        assert!(requests[0].0.contains("%FF"));
        assert!(requests[0].0.ends_with("&count"));
    }

    #[test]
    fn sync_status_uses_the_body_even_for_a_non_ok_http_status_like_go() {
        // helper.CollectTiFlashStatusWithCtx always passes the body to
        // ComputeTiFlashStatus; it does not add an HTTP status-code policy.
        let http = HttpFixture::new(|path| {
            if path.starts_with("/tiflash/") {
                (503, "2\n1009 1010\n".to_owned())
            } else {
                successful_response(path)
            }
        });
        let poller = fixture_manager(&http, Arc::new(Ddl::default()), &[]);
        assert_eq!(
            poller
                .progress(7, 2, &poller.pd_http, &[http.store()])
                .unwrap(),
            (1.0, 0.5),
        );
    }

    #[test]
    fn up_store_errors_prevent_availability_publication() {
        let http = HttpFixture::new(|path| {
            if path.starts_with("/tiflash/") {
                (503, "unavailable".to_owned())
            } else {
                successful_response(path)
            }
        });
        let poller = fixture_manager(&http, Arc::new(Ddl::default()), &[]);
        assert!(poller
            .progress(7, 2, &poller.pd_http, &[http.store()])
            .is_err());
    }

    #[test]
    fn shutdown_wakes_an_idle_worker_and_releases_its_capabilities() {
        let capability = Arc::new(());
        let retained = Arc::downgrade(&capability);
        let mut poller = TiFlashReplicaPoller::start(Duration::from_secs(3600), move || {
            let _ = &capability;
            panic!("an idle worker must be stopped before its interval");
        });
        let started = std::time::Instant::now();
        poller.shutdown();
        poller.shutdown();
        assert!(started.elapsed() < Duration::from_secs(2));
        assert!(retained.upgrade().is_none());
    }

    #[test]
    fn shutdown_joins_the_current_pass_before_releasing_its_capabilities() {
        let (started, running) = mpsc::channel();
        let (release, blocked) = mpsc::channel();
        let (finished, done) = mpsc::channel();
        let capability = Arc::new(());
        let retained = Arc::downgrade(&capability);
        let mut poller = TiFlashReplicaPoller::start(Duration::from_millis(1), move || {
            let _ = &capability;
            started.send(()).unwrap();
            blocked.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        running.recv_timeout(Duration::from_secs(5)).unwrap();
        // Queue shutdown while the pass is blocked; the next pass must not run.
        poller.stop.send(()).unwrap();
        let joining = std::thread::spawn(move || {
            poller.shutdown();
            finished.send(()).unwrap();
        });
        assert!(done.try_recv().is_err());
        assert!(retained.upgrade().is_some());
        release.send(()).unwrap();
        done.recv_timeout(Duration::from_secs(5)).unwrap();
        joining.join().unwrap();
        assert!(retained.upgrade().is_none());
        assert!(running.try_recv().is_err());
    }

    #[test]
    fn status_rejects_malformed_or_incomplete_reports_but_tolerates_count_mismatch() {
        for body in [
            "",
            "0\n",
            "2\n1 2",
            "x\n1\n",
            " 1\n1\n",
            "1\ninvalid\n",
            "1\n9223372036854775808\n",
        ] {
            assert!(parse_sync_status(body).is_err(), "accepted {body:?}");
        }
        assert_eq!(parse_sync_status("99\n1 2\nignored\n").unwrap(), vec![1, 2]);
        assert_eq!(
            parse_sync_status("\t2\r\n\t+1 -2 \t\r\n").unwrap(),
            vec![1, -2]
        );
    }

    #[test]
    fn existing_group_priority_needs_no_write_and_failure_prevents_rule_updates() {
        let http = HttpFixture::new(|path| {
            if path == "/pd/api/v1/config/rule_group/tiflash" {
                (
                    200,
                    r#"{"id":"tiflash","index":120,"override":false}"#.to_owned(),
                )
            } else {
                successful_response(path)
            }
        });
        let ddl = Arc::new(Ddl::default());
        ddl.owner.store(true, Ordering::SeqCst);
        fixture_manager(&http, ddl, &[(7, false)])
            .poll_once()
            .unwrap();
        assert!(!http
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(path, _)| path == "/pd/api/v1/config/rule_group"));

        let http = HttpFixture::new(|path| {
            if path == "/pd/api/v1/config/rule_group" {
                (503, "failed".to_owned())
            } else {
                successful_response(path)
            }
        });
        let ddl = Arc::new(Ddl::default());
        ddl.owner.store(true, Ordering::SeqCst);
        assert!(fixture_manager(&http, ddl.clone(), &[(7, false)])
            .poll_once()
            .is_err());
        assert!(ddl.updates.lock().unwrap().is_empty());
        assert!(!http
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|(path, _)| path == "/pd/api/v1/config/rule"));
    }
}
