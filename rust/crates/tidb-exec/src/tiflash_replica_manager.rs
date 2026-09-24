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
//! `pkg/ddl/ddl_tiflash_api.go`'s `PollTiFlashRoutine` narrowed to the
//! classic (NULL-keyspace, API V1) cluster.
//!
//! Every tick, for each table whose `TiFlashReplica` is set but not yet
//! available:
//!
//! 1. the table's region count comes from PD HTTP
//!    `/stats/region?start_key=&end_key=` over the record range
//!    (`GetRegionCountFromPD`, info.go:1109);
//! 2. each live TiFlash store's STATUS address answers
//!    `/tiflash/sync-status/keyspace/{NullspaceID}/table/{id}`
//!    (`helper.go:906 CollectTiFlashStatusWithCtx`); the response lists the
//!    regions that already carry a learner peer on that store;
//! 3. `availProgress = |regions with >= 1 tiflash peer| / regionCount`
//!    (`calculateTiFlashProgress`, tiflash_manager.go:135); availability is
//!    `availProgress >= 1.0` (ddl_tiflash_api.go:500);
//! 4. the flip publishes through the same DDL transaction path Go uses
//!    (`UpdateTableReplicaInfo` -> `ActionUpdateTiFlashReplicaStatus`).

use std::sync::Arc;
use std::time::Duration;

use tidb_pd_client::{PdClient, PdStoreState};
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};

use crate::cluster_ddl::DdlStatement;
use crate::real_tikv_ddl::commit_cluster_ddl;

/// Go `NullspaceID` (`tikv/keyspace`): the keyspace id a classic API-V1
/// cluster dispatches and reports under.
pub const NULL_KEYSPACE_ID: u32 = 4294967295;

/// Go `DefaultTiFlashPollInterval` (ddl_tiflash_api.go: `2 * time.Second`).
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// One poller over the node's catalog watch, transaction opener, and PD
/// membership. Generic in the opener's transport traits exactly like
/// `commit_cluster_ddl`, which the flip runs through.
pub struct TiFlashReplicaManager<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability> {
    opener: crate::real_tikv_read::RealOptimisticTransactionOpener<C, L, P>,
    catalog: Arc<crate::catalog_watch::SharedCatalog>,
    pd: PdClient,
    /// The PD HTTP endpoint (`http://host:client-port`), where the region
    /// stats live.
    pd_http: String,
}

/// One raw HTTP/1.1 call over TCP, `Connection: close`.
///
/// Why not reqwest: PD answers this module's placement-rule POST with an
/// empty `502 Bad Gateway` when the request arrives over a keep-alive
/// HTTP/1.1 connection with the full header set (verified against the live
/// playground: the identical body over a raw close-delimited POST answers
/// `200 "Update rules and groups successfully."`). Classic plaintext PD only;
/// a TLS cluster needs the TLS-configured client from the cluster security
/// settings and is deferred with the rest of TLS support.
fn http_call(method: &str, url: &str, body: Option<&str>) -> Result<(u16, String), String> {
    use std::io::{Read, Write};
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("only plaintext PD/store HTTP is supported: {url}"))?;
    let (host_port, path) = match rest.split_once('/') {
        Some((host, path)) => (host.to_owned(), format!("/{path}")),
        None => (rest.to_owned(), "/".to_owned()),
    };
    let mut stream = std::net::TcpStream::connect(&host_port)
        .map_err(|error| format!("connect {host_port}: {error}"))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {host_port}\r\nContent-Type: application/json\r\nConnection: close\r\n"
    );
    if let Some(body) = body {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .map_err(|error| error.to_string())?;
    if let Some(body) = body {
        stream
            .write_all(body.as_bytes())
            .map_err(|error| error.to_string())?;
    }
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|error| error.to_string())?;
    let status = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| format!("malformed HTTP response: {response:?}"))?;
    let body = response
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

impl<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>
    TiFlashReplicaManager<C, L, P>
{
    /// Creates the manager; spawn it with [`Self::spawn`].
    pub fn new(
        opener: crate::real_tikv_read::RealOptimisticTransactionOpener<C, L, P>,
        catalog: Arc<crate::catalog_watch::SharedCatalog>,
        pd: PdClient,
        pd_http: String,
    ) -> Self {
        Self {
            opener,
            catalog,
            pd,
            pd_http,
        }
    }

    /// Runs the poll loop until the process stops. Never returns `Ok`.
    pub fn spawn(self) -> std::thread::JoinHandle<()> {
        std::thread::Builder::new()
            .name("tiflash-replica-poll".to_owned())
            .spawn(move || loop {
                std::thread::sleep(POLL_INTERVAL);
                if let Err(error) = self.poll_once() {
                    eprintln!("{{\"event\":\"tiflash_replica_poll_error\",\"error\":\"{error}\"}}");
                }
            })
            .expect("the TiFlash replica poller starts")
    }

    /// One poll pass: sync placement rules for replica tables, then flip
    /// availability for tables whose learners are all in place.
    fn poll_once(&self) -> Result<(), String> {
        let endpoint = self.pd_http.trim_end_matches('/').to_owned();
        let catalog = self.catalog.load();
        let stores: Vec<_> = self
            .pd
            .all_stores()
            .map_err(|error| error.to_string())?
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
        let mut replica_tables: Vec<(i64, u64, Vec<String>)> = Vec::new();
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
                ));
            }
        }

        // Go `refreshTiFlashPlacementRules`: rules whose tables no longer
        // carry a replica (reset, dropped, or gone) are removed, so stale
        // learners stop being placed.
        let (rules_status, rules_body) = http_call(
            "GET",
            &format!("{endpoint}/pd/api/v1/config/rules/group/tiflash"),
            None,
        )?;
        if rules_status == 200 {
            // An empty rules group answers JSON `null`, not `[]`.
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
                    .any(|(table_id, _, _)| *id == format!("table-{table_id}-r"));
                if desired || id.is_empty() {
                    continue;
                }
                let (delete_status, delete_body) = http_call(
                    "DELETE",
                    &format!("{endpoint}/pd/api/v1/config/rule/tiflash/{id}"),
                    None,
                )?;
                eprintln!(
                    "{{\"event\":\"tiflash_rule_removed\",\"rule\":{id:?},\"status\":{delete_status},\"detail\":{delete_body:?}}}"
                );
            }
        }

        for (table_id, count, labels) in &replica_tables {
            let table_id = *table_id;
            let count = *count;
            let labels = labels.clone();

            // Go `syncTiFlashTableRule`: ensure the ONE learner rule
            // (`table-{id}-r`) exists with the requested count — the
            // manager repairs it every tick; the DDL job itself only
            // persists metadata. Delivered over raw close-delimited
            // HTTP/1.1: PD answers this POST over a keep-alive client
            // with an empty 502 (live-verified).
            let bundle = tidb_placement::new_tiflash_bundle(table_id, count, &labels);
            let bundle_body =
                serde_json::to_string(&vec![bundle]).map_err(|error| error.to_string())?;
            let (status, _) = http_call(
                "POST",
                &format!("{endpoint}/pd/api/v1/config/placement-rule?partial=true"),
                Some(&bundle_body),
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
            let raw_start = tidb_codec::gen_table_record_prefix(table_id);
            let mut accel_start = Vec::new();
            tidb_codec::encode_bytes(&mut accel_start, &raw_start);
            let raw_end = tidb_codec::table_key::encode_table_prefix(table_id + 1);
            let mut accel_end = Vec::new();
            tidb_codec::encode_bytes(&mut accel_end, &raw_end);
            let (accel_status, _) = http_call(
                "POST",
                &format!("{endpoint}/pd/api/v1/regions/accelerate-schedule/batch"),
                Some(&format!(
                    "[{{\"start_key\":\"{}\",\"end_key\":\"{}\"}}]",
                    hex_upper(&accel_start),
                    hex_upper(&accel_end)
                )),
            )?;
            if accel_status != 200 {
                eprintln!("{{\"event\":\"tiflash_accelerate_refused\",\"status\":{accel_status}}}");
            }

            let Ok((one_replica_progress, full_progress)) =
                self.progress(table_id, &endpoint, &stores)
            else {
                // Transient stats/region or sync-status failures cost one
                // tick: Go keeps the table in the backoff set and
                // retries on the next poll.
                continue;
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
                self.flip(table_id)?;
            }
        }
        Ok(())
    }

    /// Go `calculateTiFlashProgress`: `(one-replica progress, full progress)`.
    fn progress(
        &self,
        table_id: i64,
        endpoint: &str,
        stores: &[tidb_pd_client::PdStore],
    ) -> Result<(f64, f64), String> {
        let start_key = tidb_codec::gen_table_record_prefix(table_id);
        let end_key = tidb_codec::table_key::encode_table_prefix(table_id + 1);
        let url = format!(
            "{endpoint}/pd/api/v1/stats/region?start_key={}&end_key={}",
            hex_upper(&start_key),
            hex_upper(&end_key),
        );
        let (status, body) = http_call("GET", &url, None)?;
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
            let Ok((status, sync_body)) = http_call("GET", &url, None) else {
                // Go skips stores it cannot reach; a store with a dead
                // status port simply contributes no progress this tick.
                continue;
            };
            if status != 200 {
                continue;
            }
            for region_id in parse_sync_status(&sync_body) {
                peer_count += 1;
                regions_with_peer.insert(region_id);
            }
        }
        let region_count = region_count as f64;
        let one_replica_progress = regions_with_peer.len() as f64 / region_count;
        let full_progress = peer_count as f64 / (region_count * 1.0);
        Ok((one_replica_progress, full_progress))
    }

    /// Go `UpdateTableReplicaInfo`: publish the availability flip through the
    /// same DDL transaction path every catalog change uses.
    fn flip(&self, table_id: i64) -> Result<(), String> {
        let statement = DdlStatement::UpdateTiFlashReplicaStatus {
            table_id,
            available: true,
        };
        match crate::real_tikv_ddl::commit_cluster_ddl(
            &self.opener,
            &statement,
            Duration::from_secs(30),
            None,
        ) {
            Ok(_) => Ok(()),
            Err(error) => Err(format!("replica status flip: {error}")),
        }
    }
}

/// Go `helper.ComputeTiFlashStatus`: the body is newline-separated region
/// ids — every listed region already carries a learner peer on that store.
fn parse_sync_status(body: &str) -> Vec<u64> {
    body.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| line.parse().ok())
        .collect()
}

fn hex_upper(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}
