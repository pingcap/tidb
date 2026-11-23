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

//! Go `pkg/infoschema/perfschema/tables.go`'s `perfSchemaTable.getRows`:
//! the rows of the virtual tables Go fills when they are read, with
//! `infoschema.GetDataFromSessionConnectAttrs` and `GetDataFromStatusByConn`
//! (`pkg/infoschema/tables.go`).

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_domain::cluster_topology::ClusterServer;
use tidb_profile::Collector;
use tidb_sessmgr::processinfo::SessionManager;

const TABLE_NAME_TIDB_PROFILE_CPU: &str = "tidb_profile_cpu";
const TABLE_NAME_TIDB_PROFILE_MEMORY: &str = "tidb_profile_memory";
const TABLE_NAME_TIDB_PROFILE_MUTEX: &str = "tidb_profile_mutex";
const TABLE_NAME_TIDB_PROFILE_ALLOCS: &str = "tidb_profile_allocs";
const TABLE_NAME_TIDB_PROFILE_BLOCK: &str = "tidb_profile_block";
const TABLE_NAME_TIDB_PROFILE_GOROUTINES: &str = "tidb_profile_goroutines";
const TABLE_NAME_TIKV_PROFILE_CPU: &str = "tikv_profile_cpu";
const TABLE_NAME_PD_PROFILE_CPU: &str = "pd_profile_cpu";
const TABLE_NAME_PD_PROFILE_MEMORY: &str = "pd_profile_memory";
const TABLE_NAME_PD_PROFILE_MUTEX: &str = "pd_profile_mutex";
const TABLE_NAME_PD_PROFILE_ALLOCS: &str = "pd_profile_allocs";
const TABLE_NAME_PD_PROFILE_BLOCK: &str = "pd_profile_block";
const TABLE_NAME_PD_PROFILE_GOROUTINES: &str = "pd_profile_goroutines";
const TABLE_NAME_SESSION_VARIABLES: &str = "session_variables";
const TABLE_NAME_SESSION_CONNECT_ATTRS: &str = "session_connect_attrs";
const TABLE_NAME_SESSION_ACCOUNT_CONNECT_ATTRS: &str = "session_account_connect_attrs";
const TABLE_NAME_STATUS_BY_CONNECTION: &str = "status_by_connection";

/// The tables `getRows` fills; every other `performance_schema` table has no
/// rows.
const FILLED_TABLES: [&str; 17] = [
    TABLE_NAME_TIDB_PROFILE_CPU,
    TABLE_NAME_TIDB_PROFILE_MEMORY,
    TABLE_NAME_TIDB_PROFILE_MUTEX,
    TABLE_NAME_TIDB_PROFILE_ALLOCS,
    TABLE_NAME_TIDB_PROFILE_BLOCK,
    TABLE_NAME_TIDB_PROFILE_GOROUTINES,
    TABLE_NAME_TIKV_PROFILE_CPU,
    TABLE_NAME_PD_PROFILE_CPU,
    TABLE_NAME_PD_PROFILE_MEMORY,
    TABLE_NAME_PD_PROFILE_MUTEX,
    TABLE_NAME_PD_PROFILE_ALLOCS,
    TABLE_NAME_PD_PROFILE_BLOCK,
    TABLE_NAME_PD_PROFILE_GOROUTINES,
    TABLE_NAME_SESSION_VARIABLES,
    TABLE_NAME_SESSION_CONNECT_ATTRS,
    TABLE_NAME_SESSION_ACCOUNT_CONNECT_ATTRS,
    TABLE_NAME_STATUS_BY_CONNECTION,
];

/// Whether Go's `getRows` fills `table` (by its `Name.O`).
#[must_use]
pub fn fills_table(table: &str) -> bool {
    FILLED_TABLES.contains(&table)
}

/// What Go's `getRows` reads from its `sessionctx.Context`.
pub trait PerfSchemaContext {
    /// Go `GetSessionVars().ConnectionID`.
    fn connection_id(&self) -> u64;
    /// Go `GetSessionVars().User`: the user name and host name.
    fn user(&self) -> Option<(String, String)>;
    /// Go `GetSessionVars().User.LoginString()`.
    fn login_string(&self) -> Option<String>;
    /// Go `GetSessionVars().ConnectionInfo.ClientIP`.
    fn client_ip(&self) -> Option<String>;
    /// Go `GetSessionManager()`.
    fn session_manager(&self) -> Option<Arc<dyn SessionManager>>;
    /// Go `infoschema.GetDataFromSessionVariables`.
    fn get_data_from_session_variables(&self) -> Result<Vec<Vec<Datum>>, String>;
    /// Go `infoschema.GetStoreServerInfo(ctx.GetStore())`.
    fn get_store_server_info(&self) -> Result<Vec<ClusterServer>, String>;
    /// Go `infoschema.GetPDServerInfo(ctx)`.
    fn get_pd_server_info(&self) -> Result<Vec<ClusterServer>, String>;
    /// Go `GetSessionVars().StmtCtx.AppendWarning`.
    fn append_warning(&self, message: String);
}

/// Go `logTiDBProfileRequest`.
fn log_tidb_profile_request(sctx: &dyn PerfSchemaContext, table_name: &str) {
    let mut fields = vec![
        tidb_log::Field::new("table", tidb_log::Value::Str(format!("performance_schema.{table_name}"))),
        tidb_log::Field::new("conn", tidb_log::Value::U64(sctx.connection_id())),
    ];
    if let Some(user) = sctx.login_string() {
        fields.push(tidb_log::Field::new("user", tidb_log::Value::Str(user)));
    }
    if let Some(client_ip) = sctx.client_ip() {
        fields.push(tidb_log::Field::new("client-ip", tidb_log::Value::Str(client_ip)));
    }
    tidb_logutil::bg_logger().info("profiling request received", &fields);
}

/// Go `perfSchemaTable.getRows`: every column of the table, as the memory
/// table reader projects the columns it reads.
pub fn get_rows(table_name: &str, sctx: &dyn PerfSchemaContext) -> Result<Vec<Vec<Datum>>, String> {
    let collector = Collector;
    let interval = tidb_profile::cpu_profile_interval();
    match table_name {
        TABLE_NAME_TIDB_PROFILE_CPU => {
            log_tidb_profile_request(sctx, TABLE_NAME_TIDB_PROFILE_CPU);
            collector.profile_graph("cpu")
        }
        TABLE_NAME_TIDB_PROFILE_MEMORY => {
            log_tidb_profile_request(sctx, TABLE_NAME_TIDB_PROFILE_MEMORY);
            collector.profile_graph("heap")
        }
        TABLE_NAME_TIDB_PROFILE_MUTEX => {
            log_tidb_profile_request(sctx, TABLE_NAME_TIDB_PROFILE_MUTEX);
            collector.profile_graph("mutex")
        }
        TABLE_NAME_TIDB_PROFILE_ALLOCS => {
            log_tidb_profile_request(sctx, TABLE_NAME_TIDB_PROFILE_ALLOCS);
            collector.profile_graph("allocs")
        }
        TABLE_NAME_TIDB_PROFILE_BLOCK => {
            log_tidb_profile_request(sctx, TABLE_NAME_TIDB_PROFILE_BLOCK);
            collector.profile_graph("block")
        }
        TABLE_NAME_TIDB_PROFILE_GOROUTINES => {
            log_tidb_profile_request(sctx, TABLE_NAME_TIDB_PROFILE_GOROUTINES);
            collector.profile_graph("goroutine")
        }
        TABLE_NAME_TIKV_PROFILE_CPU => data_for_remote_profile(
            sctx,
            "tikv",
            &format!("/debug/pprof/profile?seconds={}", interval.as_secs()),
            false,
        ),
        // Go `pd.PProfProfileAPIWithInterval`, `PProfHeap`, `PProfMutex`,
        // `PProfAllocs`, `PProfBlock`, `PProfGoroutineWithDebugLevel(2)`.
        TABLE_NAME_PD_PROFILE_CPU => data_for_remote_profile(
            sctx,
            "pd",
            &format!("/pd/api/v1/debug/pprof/profile?seconds={}", interval.as_secs()),
            false,
        ),
        TABLE_NAME_PD_PROFILE_MEMORY => data_for_remote_profile(sctx, "pd", "/pd/api/v1/debug/pprof/heap", false),
        TABLE_NAME_PD_PROFILE_MUTEX => data_for_remote_profile(sctx, "pd", "/pd/api/v1/debug/pprof/mutex", false),
        TABLE_NAME_PD_PROFILE_ALLOCS => data_for_remote_profile(sctx, "pd", "/pd/api/v1/debug/pprof/allocs", false),
        TABLE_NAME_PD_PROFILE_BLOCK => data_for_remote_profile(sctx, "pd", "/pd/api/v1/debug/pprof/block", false),
        TABLE_NAME_PD_PROFILE_GOROUTINES => {
            data_for_remote_profile(sctx, "pd", "/pd/api/v1/debug/pprof/goroutine?debug=2", true)
        }
        TABLE_NAME_SESSION_VARIABLES => sctx.get_data_from_session_variables(),
        TABLE_NAME_SESSION_CONNECT_ATTRS => Ok(get_data_from_session_connect_attrs(sctx, false)),
        TABLE_NAME_SESSION_ACCOUNT_CONNECT_ATTRS => Ok(get_data_from_session_connect_attrs(sctx, true)),
        TABLE_NAME_STATUS_BY_CONNECTION => Ok(get_data_from_status_by_conn(sctx)),
        _ => Ok(Vec::new()),
    }
}

/// Go `infoschema.GetDataFromSessionConnectAttrs`.
#[must_use]
pub fn get_data_from_session_connect_attrs(sctx: &dyn PerfSchemaContext, same_account: bool) -> Vec<Vec<Datum>> {
    let Some(sm) = sctx.session_manager() else {
        return Vec::new();
    };
    let user = if same_account { sctx.user() } else { None };
    let all_attrs = sm.get_con_attrs(user.as_ref().map(|(name, host)| (name.as_str(), host.as_str())));
    let mut rows = Vec::with_capacity(all_attrs.len() * 10);
    for (pid, attrs) in all_attrs {
        // Go sorts the attribute names and numbers them in that order, so
        // ORDINAL_POSITION is stable across queries; the map is sorted.
        for (ord, (attrkey, value)) in attrs.iter().enumerate() {
            rows.push(vec![
                Datum::UInt(pid),
                Datum::new_string_datum(attrkey.as_str()),
                Datum::new_string_datum(value.as_str()),
                Datum::Int(ord as i64),
            ]);
        }
    }
    rows
}

/// Go `infoschema.GetDataFromStatusByConn`.
#[must_use]
pub fn get_data_from_status_by_conn(sctx: &dyn PerfSchemaContext) -> Vec<Vec<Datum>> {
    let Some(sm) = sctx.session_manager() else {
        return Vec::new();
    };
    let status_vars = sm.get_status_vars();
    let mut rows = Vec::with_capacity(2 * status_vars.len());
    for (pid, svar) in status_vars {
        for (varkey, varval) in svar {
            rows.push(vec![Datum::UInt(pid), Datum::new_string_datum(varkey), Datum::new_string_datum(varval)]);
        }
    }
    rows
}

/// Go `util.InternalHTTPSchema`.
fn internal_http_schema() -> &'static str {
    if tidb_config::config_tree::config::get_global_config().security.cluster_ssl_ca.is_empty() {
        "http"
    } else {
        "https"
    }
}

/// Go `util.InternalHTTPClient()`.
fn internal_http_client() -> Result<reqwest::blocking::Client, String> {
    let security = &tidb_config::config_tree::config::get_global_config().security;
    let security = tidb_pd_client::ClusterSecurity::new(
        security.cluster_ssl_ca.clone(),
        security.cluster_ssl_cert.clone(),
        security.cluster_ssl_key.clone(),
        Vec::new(),
    );
    tidb_store_driver_txn::cluster_http::internal_http_client(&security)
}

/// Go `dataForRemoteProfile`: every server of `node_type` is asked for its
/// profile at `uri` concurrently; the rows carry the server's address first.
fn data_for_remote_profile(
    sctx: &dyn PerfSchemaContext,
    node_type: &str,
    uri: &str,
    is_goroutine: bool,
) -> Result<Vec<Vec<Datum>>, String> {
    let mut servers = match node_type {
        "tikv" => sctx.get_store_server_info(),
        "pd" => sctx.get_pd_server_info(),
        _ => return Err(format!("{node_type} does not support profile remote component")),
    };
    if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/infoschema/perfschema/mockRemoteNodeStatusAddress") {
        // The cluster topology is injected by the failpoint expression with
        // no extra checks for it.
        let s = value.string();
        if !s.is_empty() {
            let mut injected = Vec::new();
            for server in s.split(';') {
                let parts: Vec<&str> = server.split(',').collect();
                if parts[0] != node_type {
                    continue;
                }
                injected.push(ClusterServer {
                    server_type: parts[0].to_owned(),
                    address: parts[1].to_owned(),
                    status_address: parts[2].to_owned(),
                    ..ClusterServer::default()
                });
            }
            // erase error
            servers = Ok(injected);
        }
    }
    let servers = servers?;

    let mut handles = Vec::new();
    for server in servers {
        let status_addr = server.status_address;
        if status_addr.is_empty() {
            sctx.append_warning(format!("TiKV node {} does not contain status address", server.address));
            continue;
        }
        let uri = uri.to_owned();
        handles.push(std::thread::spawn(move || {
            let url = format!("{}://{status_addr}{uri}", internal_http_schema());
            let fetched = (|| {
                let resp = internal_http_client()?
                    .get(&url)
                    // Forbidden PD follower proxy
                    .header("PD-Allow-follower-handle", "true")
                    // TiKV output svg format in default
                    .header("Content-Type", "application/protobuf")
                    .send()
                    .map_err(|error| error.to_string())?;
                if resp.status() != reqwest::StatusCode::OK {
                    return Err(format!("request {url} failed: {}", resp.status()));
                }
                let body = resp.bytes().map_err(|error| error.to_string())?;
                let collector = Collector;
                let rows = if is_goroutine {
                    collector.parse_goroutines(&body)?
                } else {
                    collector.profile_reader_to_datums(&body)?
                };
                Ok(rows)
            })();
            (status_addr, fetched)
        }));
    }

    let mut results = Vec::new();
    for handle in handles {
        let (addr, fetched) = handle.join().map_err(|_| "remote profile worker panicked".to_owned())?;
        match fetched {
            Err(error) => sctx.append_warning(error),
            Ok(rows) => results.push((addr, rows)),
        }
    }
    // Keep the result sorted by address.
    results.sort_by(|i, j| i.0.cmp(&j.0));
    let mut final_rows = Vec::new();
    for (addr, rows) in results {
        for row in rows {
            let mut with_addr = Vec::with_capacity(row.len() + 1);
            with_addr.push(Datum::new_string_datum(addr.as_str()));
            with_addr.extend(row);
            final_rows.push(with_addr);
        }
    }
    Ok(final_rows)
}

#[cfg(test)]
#[path = "tables_tests.rs"]
mod tests;
