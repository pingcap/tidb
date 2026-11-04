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

//! Go's shared status HTTP/gRPC listener, with one joined shutdown owner.
use crate::sql_node::ConnectionTracker;
#[cfg(test)]
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

/// Owns the status socket, connections and runtime until node shutdown.
pub struct StatusServer {
    local_addr: std::net::SocketAddr,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}
impl StatusServer {
    /// The address actually bound by this server.
    #[must_use]
    pub const fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }
}
impl Drop for StatusServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// Reads the catalog the status server answers `/schema` from.
///
/// A closure rather than a snapshot because the node's catalog moves as DDL
/// lands, and Go answers `/schema` from `GetLatest()` each time.
pub type SchemaSource = Arc<dyn Fn() -> tidb_infoschema::SessionExtendedInfoSchema + Send + Sync>;

/// Binds and serves `GET /status` in a background thread.
pub fn start_status_listener(
    host: &str,
    port: u16,
    tracker: Arc<ConnectionTracker>,
    version: String,
    git_hash: String,
) -> std::io::Result<StatusServer> {
    start_status_listener_with_routes(
        host,
        port,
        tracker,
        version,
        git_hash,
        StatusRoutes::default(),
    )
}

/// The routes a node can serve beyond `/status`, each `None` when this node
/// has no source for it.
#[derive(Default, Clone)]
pub struct StatusRoutes {
    /// Go's `/schema` family, read from the live catalog.
    pub schema: Option<SchemaSource>,
    /// Live configuration and the node's shared runtime settings owners.
    pub settings: Option<crate::http_settings::Settings>,
    /// Local generated TiKV service shared by peer KILL and memory-table scans.
    pub peer: Option<crate::peer_rpc::PeerService>,
    /// Cluster TLS policy, shared with outgoing transports.
    pub security: tidb_pd_client::ClusterSecurity,
    /// Go `infosync`'s server-info syncer, which `/labels` republishes the
    /// labels through.
    pub server_info: Option<Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>>,
}

/// Bind HTTP and generated gRPC routes on Go's shared status port.
pub fn start_status_listener_with_routes(
    host: &str,
    port: u16,
    tracker: Arc<ConnectionTracker>,
    version: String,
    git_hash: String,
    routes: StatusRoutes,
) -> std::io::Result<StatusServer> {
    crate::server_metrics::init();
    let tls = crate::status_transport::tls_acceptor(&routes.security)?;
    let listener = TcpListener::bind((host, port))?;
    listener.set_nonblocking(true)?;
    let local_addr = listener.local_addr()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let listener = {
        let _entered = runtime.enter();
        tokio::net::TcpListener::from_std(listener)?
    };
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let global = tidb_config::config_tree::config::get_global_config();
    let config = global.status.clone();
    // Go `NewRPCServer`: the diagnostics service searches the configured log file.
    let diagnostics = tidb_sysutil::DiagnosticsServer::new(global.log.file.filename.clone());
    let worker = std::thread::Builder::new()
        .name("tidb-status".into())
        .spawn(move || {
            runtime.block_on(async move {
                let max_send = config.grpc_max_send_msg_size.max(0) as usize;
                let mut grpc = tonic::service::Routes::new(
                    tidb_proto::diagnosticspb::diagnostics_server::DiagnosticsServer::new(
                        diagnostics,
                    )
                    .max_encoding_message_size(max_send),
                );
                if let Some(peer) = routes.peer.clone() {
                    grpc = grpc.add_service(
                        tidb_proto::tikvpb::tikv_server::TikvServer::new(peer)
                            .max_encoding_message_size(max_send),
                    );
                }
                let router = grpc.into_axum_router();
                let router = router.fallback(move |request: axum::extract::Request| {
                    let (tracker, version, git_hash, routes) = (
                        tracker.clone(),
                        version.clone(),
                        git_hash.clone(),
                        routes.clone(),
                    );
                    async move { http_response(request, tracker, version, git_hash, routes).await }
                });
                // Go closes HTTP and stops gRPC immediately. The transport owns
                // every connection and incomplete TLS handshake until shutdown.
                tokio::select! {
                result = crate::status_transport::serve(listener, tls, router, config) => {
                    if let Err(error) = result { eprintln!("status listener failed: {error}"); }
                }
                _ = stopped => {}
                }
            });
        })?;
    Ok(StatusServer {
        local_addr,
        stop: Some(stop),
        worker: Some(worker),
    })
}

async fn http_response(
    request: axum::extract::Request,
    tracker: Arc<ConnectionTracker>,
    version: String,
    git_hash: String,
    routes: StatusRoutes,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let (parts, body) = request.into_parts();
    let bytes = match axum::body::to_bytes(body, 10 << 20).await {
        Ok(body) => body,
        Err(error) => {
            return (axum::http::StatusCode::BAD_REQUEST, error.to_string()).into_response();
        }
    };
    let body = match std::str::from_utf8(&bytes) {
        Ok(body) => body,
        Err(error) => {
            return (axum::http::StatusCode::BAD_REQUEST, error.to_string()).into_response();
        }
    };
    let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
    let mut request = format!("{} {} HTTP/1.1\r\n", parts.method, path);
    for (name, value) in &parts.headers {
        if name != "transfer-encoding" && name != "content-length" {
            if let Ok(value) = value.to_str() {
                request.push_str(&format!("{name}: {value}\r\n"));
            }
        }
    }
    request.push_str(&format!("Content-Length: {}\r\n\r\n{body}", body.len()));
    let path = path.to_owned();
    // Existing settings handlers may open an internal synchronous SQL session.
    match tokio::task::spawn_blocking(move || {
        route_response(&path, &request, &tracker, &version, &git_hash, &routes)
    })
    .await
    {
        Ok(response) => response,
        Err(error) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            error.to_string(),
        )
            .into_response(),
    }
}

fn route_response(
    path: &str,
    request: &str,
    tracker: &ConnectionTracker,
    version: &str,
    git_hash: &str,
    routes: &StatusRoutes,
) -> axum::response::Response {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    let (code, content_type, body) = if route == "/status" {
        let percentage = tidb_stats_handle_initstats::INIT_STATS_PERCENTAGE.load();
        let percentage = if percentage.is_nan() {
            percentage
        } else {
            percentage.min(100.0)
        };
        (
            200,
            "application/json",
            format!(
                "{{\"connections\":{},\"version\":\"{}\",\"git_hash\":\"{}\",\"status\":{{\"init_stats_percentage\":{}}}}}",
                tracker.active(),
                version,
                git_hash,
                percentage
            ),
        )
    } else if route == "/metrics" {
        let mut body = prometheus::TextEncoder::new()
            .encode_to_string(&prometheus::gather())
            .expect("registered metrics encode as Prometheus text");
        body.push_str(&tidb_metrics::client_go_metrics::gather_text());
        // rust-prometheus omits registered vectors without children; Go exposes
        // their HELP/TYPE headers. Both client and server use their shared owners.
        for (name, help) in crate::server_metrics::family_catalog() {
            if !body.contains(name.as_str()) {
                body.push_str(&format!("# HELP {name} {help}\n# TYPE {name} histogram\n"));
            }
        }
        (200, "text/plain; version=0.0.4", body)
    } else if let Some(answer) = settings_response(path, request, routes.settings.as_ref()) {
        match answer {
            Ok(body) => (200, "application/json", body),
            Err(message) => (400, "text/plain", message),
        }
    } else if let Some(answer) = routes
        .schema
        .as_ref()
        .and_then(|source| schema_response(path, source.as_ref()))
    {
        match answer {
            Ok(body) => (200, "application/json", body),
            // Go `handler.WriteError`: 400 with the error text.
            Err(message) => (400, "text/plain", message),
        }
    } else if let Some(answer) = routes
        .schema
        .as_ref()
        .and_then(|source| db_table_response(path, source.as_ref()))
        .or_else(|| value_response(path))
        .or_else(|| labels_response(path, request, routes.server_info.as_deref()))
    {
        match answer {
            Ok(body) => (200, "application/json", body),
            Err(message) => (400, "text/plain", message),
        }
    } else {
        (404, "text/plain", String::new())
    };
    axum::http::Response::builder()
        .status(code)
        .header("content-type", content_type)
        .header("content-length", body.len())
        .header("connection", "close")
        .body(axum::body::Body::from(body))
        .expect("status response headers")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_host_batch_drop_releases_the_listener() {
        let server = start_status_listener(
            "127.0.0.1",
            0,
            Arc::new(ConnectionTracker::default()),
            "test".into(),
            "test".into(),
        )
        .unwrap();
        let address = server.local_addr();
        drop(server);
        assert!(
            TcpListener::bind(address).is_ok(),
            "status owner leaked its listening socket"
        );
    }

    /// Go `NewRPCServer` registers the diagnostics service on the status
    /// port: the server-info cluster tables read this node through it, and a
    /// log search reports the node's missing log location as a warning.
    #[test]
    fn status_port_serves_diagnostics_to_cluster_tables() {
        let server = start_status_listener(
            "127.0.0.1",
            0,
            Arc::new(ConnectionTracker::default()),
            "test".into(),
            "test".into(),
        )
        .unwrap();
        let info = tidb_domain_serverinfo::serverinfo::ServerInfo {
            static_info: tidb_domain_serverinfo::serverinfo::StaticInfo {
                id: "diagnostics".to_owned(),
                ip: "127.0.0.1".to_owned(),
                port: 4000,
                status_port: usize::from(server.local_addr().port()),
                server_id_getter: Some(Arc::new(|| 1)),
                ..Default::default()
            },
            ..Default::default()
        };
        let syncer = Arc::new(tidb_domain_serverinfo::serverinfo_syncer::Syncer::new(info, None));
        let mut session = tidb_session::Session::new();
        session.set_server_info_syncer(syncer);
        session.set_cluster_diagnostics_client(Arc::new(
            tidb_executor::cluster_diagnostics::ClusterDiagnosticsClient::new(&Default::default())
                .unwrap(),
        ));
        let rows = |session: &mut tidb_session::Session, sql: &str| -> Vec<Vec<String>> {
            let tidb_session::StmtResult::Rows(rows) = session.run(sql).unwrap() else {
                panic!("rows")
            };
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|datum| match (datum.as_raw_bytes(), datum) {
                            (Some(bytes), _) => String::from_utf8_lossy(bytes).into_owned(),
                            (None, tidb_datatype::Datum::Int(value)) => value.to_string(),
                            (None, tidb_datatype::Datum::UInt(value)) => value.to_string(),
                            (None, other) => format!("{other:?}"),
                        })
                        .collect()
                })
                .collect()
        };
        let systeminfo = rows(
            &mut session,
            "SELECT type, instance, system_type, system_name FROM information_schema.cluster_systeminfo",
        );
        assert!(!systeminfo.is_empty());
        assert!(systeminfo
            .iter()
            .all(|row| row[0] == "tidb" && row[1] == "127.0.0.1:4000" && row[2] == "system"));
        let hardware = rows(
            &mut session,
            "SELECT device_type, name FROM information_schema.cluster_hardware WHERE device_type = 'cpu'",
        );
        let names: Vec<&str> = hardware.iter().map(|row| row[1].as_str()).collect();
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert_eq!(
                names,
                [
                    "cpu-arch",
                    "cpu-logical-cores",
                    "cpu-physical-cores",
                    "cpu-frequency",
                    "cache"
                ]
            );
        }
        assert_eq!(
            rows(
                &mut session,
                "SELECT count(*) FROM information_schema.cluster_load WHERE type = 'tikv'"
            ),
            [["0"]]
        );
        assert!(rows(
            &mut session,
            "SELECT * FROM information_schema.cluster_log WHERE time > '2020-01-01 00:00:00' \
             AND time < '2020-01-02 00:00:00' AND message LIKE '%x%'"
        )
        .is_empty());
        let warnings = rows(&mut session, "SHOW WARNINGS");
        assert_eq!(
            warnings,
            [[
                "Warning",
                "1105",
                "rpc error: code = Unknown desc = empty log file location configuration"
            ]]
        );
    }

    #[test]
    fn peer_host_batch_status_port_accepts_http2() {
        let server = start_status_listener(
            "127.0.0.1",
            0,
            Arc::new(ConnectionTracker::default()),
            "test".into(),
            "test".into(),
        )
        .unwrap();
        let mut stream = std::net::TcpStream::connect(server.local_addr()).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        stream
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00")
            .unwrap();
        let mut frame = [0; 9];
        stream.read_exact(&mut frame).unwrap();
        assert_eq!(
            frame[3], 4,
            "first HTTP/2 server frame must be SETTINGS, received {frame:?}"
        );
    }

    #[test]
    fn peer_host_batch_http_framing_and_shutdown_release_route_owners() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = calls.clone();
        let settings = crate::http_settings::Settings::new(
            tidb_domain::global_sysvars::GlobalSysvars::default(),
            Arc::new(move |name, value| {
                captured
                    .lock()
                    .unwrap()
                    .push((name.to_owned(), value.to_owned()));
                Ok(())
            }),
        );
        let server = start_status_listener_with_routes(
            "127.0.0.1",
            0,
            Arc::new(ConnectionTracker::default()),
            "test".into(),
            "test".into(),
            StatusRoutes {
                settings: Some(settings),
                ..Default::default()
            },
        )
        .unwrap();
        let send = |raw: &[u8]| {
            let mut socket = std::net::TcpStream::connect(server.local_addr()).unwrap();
            socket
                .set_read_timeout(Some(std::time::Duration::from_secs(3)))
                .unwrap();
            for part in raw.chunks(3) {
                socket.write_all(part).unwrap();
            }
            socket.shutdown(std::net::Shutdown::Write).unwrap();
            let mut response = String::new();
            socket.read_to_string(&mut response).unwrap();
            response
        };
        let body = "tidb_enable_1pc=1";
        let raw = format!(
            "POST /settings HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        );
        assert!(send(raw.as_bytes()).starts_with("HTTP/1.1 200"));
        let raw = format!(
            "POST /settings HTTP/1.1\r\nHost: x\r\nContent-Type: application/x-www-form-urlencoded\r\nTransfer-Encoding: chunked\r\n\r\n{:x};x=1\r\n{body}\r\n0\r\nX-Trailer: yes\r\n\r\n",
            body.len()
        );
        assert!(send(raw.as_bytes()).starts_with("HTTP/1.1 200"));
        assert_eq!(
            calls.lock().unwrap().as_slice(),
            &vec![("tidb_enable_1pc".into(), "ON".into()); 2]
        );
        for raw in [
            "POST /settings HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nContent-Length: 2\r\n\r\na=1",
            "POST /settings HTTP/1.1\r\nHost: x\r\nContent-Length: 10485761\r\n\r\n",
            "POST /settings HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n3\r\na=1xx",
        ] {
            let response = send(raw.as_bytes());
            assert!(
                response.is_empty() || response.starts_with("HTTP/1.1 400"),
                "{response}"
            );
        }
        drop(server);
        assert_eq!(
            Arc::strong_count(&calls),
            1,
            "status tasks retained route owners"
        );
    }

    #[test]
    fn status_answers_gos_shape_and_other_paths_answer_404() {
        let tracker = Arc::new(ConnectionTracker::default());
        let server = start_status_listener(
            "127.0.0.1",
            0,
            Arc::clone(&tracker),
            "8.0.11-TiDB-test".to_owned(),
            "abc123".to_owned(),
        )
        .expect("binds");
        let addr = server.local_addr();

        let fetch = |path: &str| {
            let mut stream = std::net::TcpStream::connect(addr).expect("connects");
            stream
                .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
                .expect("writes");
            let mut response = String::new();
            stream.read_to_string(&mut response).expect("reads");
            response
        };

        let healthy = tidb_stats_handle_metrics::stats_healthy_gauges();
        healthy[tidb_stats_handle_metrics::STATS_HEALTHY_BUCKET_TOTAL].set(37.0);
        let metrics = fetch("/metrics");
        assert!(
            metrics.contains("tidb_statistics_stats_healthy{type=\"[0,100]\"} 37"),
            "{metrics}"
        );

        let status = fetch("/status");
        assert!(status.starts_with("HTTP/1.1 200 OK"), "{status}");
        assert!(
            status.contains(
                "{\"connections\":0,\"version\":\"8.0.11-TiDB-test\",\"git_hash\":\"abc123\",\
                 \"status\":{\"init_stats_percentage\":0}}"
            ),
            "Go's field order, exactly: {status}"
        );
        assert!(fetch("/nosuch").starts_with("HTTP/1.1 404"));
    }
}

/// Go SettingsHandler reads live configuration and applies form fields in source order.
fn settings_response(
    path: &str,
    request: &str,
    settings: Option<&crate::http_settings::Settings>,
) -> Option<Result<String, String>> {
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    if !matches!(route, "/settings" | "/config") {
        return None;
    }
    let settings = settings?;
    let method = request.split_whitespace().next().unwrap_or_default();
    if route == "/config" || method != "POST" {
        return Some(settings.read());
    }
    let (headers, body) = request.split_once("\r\n\r\n").unwrap_or((request, ""));
    let is_form = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .any(|(key, value)| {
            key.eq_ignore_ascii_case("Content-Type")
                && value.trim().split(';').next().is_some_and(|kind| {
                    kind.eq_ignore_ascii_case("application/x-www-form-urlencoded")
                })
        });
    Some(
        crate::http_settings::parse_form(query, if is_form { body } else { "" }).and_then(|form| {
            settings.apply(&form)?;
            Ok(String::new())
        }),
    )
}

/// Go `LabelHandler.ServeHTTP` (`/labels`): a POSTed JSON object of labels
/// is merged over the configured ones, republished through the server-info
/// syncer (a failure is logged, as Go does) and stored in the global config;
/// the answer is the config's labels.
fn labels_response(
    path: &str,
    request: &str,
    server_info: Option<&tidb_domain_serverinfo::serverinfo_syncer::Syncer>,
) -> Option<Result<String, String>> {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    if route != "/labels" {
        return None;
    }
    if request.split_whitespace().next() != Some("POST") {
        return Some(Err("This api only support POST method".to_owned()));
    }
    let body = request.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    let mut labels: std::collections::HashMap<String, String> = match serde_json::from_str(body) {
        Ok(labels) => labels,
        Err(error) => return Some(Err(error.to_string())),
    };
    if !labels.is_empty() {
        let mut config = (*tidb_config::config_tree::config::get_global_config()).clone();
        for (key, value) in &config.labels {
            labels.entry(key.clone()).or_insert_with(|| value.clone());
        }
        if let Some(syncer) = server_info {
            if let Err(error) = syncer.update_server_label(&labels) {
                tidb_logutil::bg_logger().error(
                    "update etcd labels failed",
                    &[tidb_log::Field::new("error", tidb_log::Value::Str(error))],
                );
            }
        }
        config.labels = labels;
        tidb_config::config_tree::config::store_global_config(config);
    }
    // Go `WriteData` of the map: its keys sorted.
    let labels: std::collections::BTreeMap<_, _> =
        tidb_config::config_tree::config::get_global_config().labels.clone().into_iter().collect();
    Some(serde_json::to_string(&labels).map_err(|error| error.to_string()))
}

/// Go `DBTableHandler.ServeHTTP` (`/db-table/{tableID}`): the database and
/// table owning a table or partition ID, with the schema version, as
/// `{"db_info", "table_info", "schema_version"}`.
fn db_table_response(
    path: &str,
    source: &dyn Fn() -> tidb_infoschema::SessionExtendedInfoSchema,
) -> Option<Result<String, String>> {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    let table_id = route.strip_prefix("/db-table/").filter(|id| !id.contains('/'))?;
    let Ok(physical_id) = table_id.parse::<i64>() else {
        return Some(Err(format!("Wrong tableID: {table_id}")));
    };
    let catalog = source();
    // Go `TableByID`, then `FindTableByPartitionID` for a partition's ID.
    let found = catalog.stored_schemas().into_iter().find_map(|database| {
        let table = database.read().deprecated_tables.iter_deref().find(|table| {
            let table = table.read();
            table.id == physical_id
                || table.get_partition_info().is_some_and(|partitions| {
                    partitions
                        .read()
                        .definitions
                        .with_visible(|definitions| definitions.iter().any(|definition| definition.id == physical_id))
                })
        });
        table.map(|table| (database.clone(), table))
    });
    let Some((database, table)) = found else {
        return Some(Err(format!("[schema:1146]Table which ID = {table_id} does not exist.")));
    };
    let encode = |bytes: Result<Vec<u8>, String>| -> Result<String, String> {
        bytes.and_then(|body| String::from_utf8(body).map_err(|error| error.to_string()))
    };
    let db_info = encode(tidb_infoschema::cluster_catalog::stored_db_info_json(&database.read()));
    let table_info = encode(tidb_infoschema::cluster_catalog::stored_table_info_json(&table.read()));
    Some(db_info.and_then(|db_info| {
        table_info.map(|table_info| {
            format!(
                "{{\"db_info\":{db_info},\"table_info\":{table_info},\"schema_version\":{}}}",
                catalog.schema_meta_version()
            )
        })
    }))
}

/// Go `strconv.ParseInt(s, 0, 64)`: base from the prefix (`0x`, `0o`, `0b`,
/// a leading `0` for octal), `_` separators allowed after a prefix.
fn go_parse_int_base0(s: &str) -> Result<i64, String> {
    let syntax = || format!("strconv.ParseInt: parsing {}: invalid syntax", tidb_go_strconv::quote_bytes(s.as_bytes()));
    let (negative, digits) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let lower = digits.to_ascii_lowercase();
    let (radix, body, prefixed) = if let Some(rest) = lower.strip_prefix("0x") {
        (16, rest.to_owned(), true)
    } else if let Some(rest) = lower.strip_prefix("0o") {
        (8, rest.to_owned(), true)
    } else if let Some(rest) = lower.strip_prefix("0b") {
        (2, rest.to_owned(), true)
    } else if lower.len() > 1 && lower.starts_with('0') {
        (8, lower[1..].to_owned(), true)
    } else {
        (10, lower.clone(), false)
    };
    if body.is_empty() || (!prefixed && body.contains('_')) || body.starts_with('_') || body.ends_with('_') || body.contains("__") {
        return Err(syntax());
    }
    let magnitude = u64::from_str_radix(&body.replace('_', ""), radix).map_err(|error| match error.kind() {
        std::num::IntErrorKind::PosOverflow => {
            format!("strconv.ParseInt: parsing {}: value out of range", tidb_go_strconv::quote_bytes(s.as_bytes()))
        }
        _ => syntax(),
    })?;
    let range = || format!("strconv.ParseInt: parsing {}: value out of range", tidb_go_strconv::quote_bytes(s.as_bytes()));
    if negative {
        if magnitude > i64::MAX as u64 + 1 {
            return Err(range());
        }
        Ok((magnitude as i64).wrapping_neg())
    } else {
        i64::try_from(magnitude).map_err(|_| range())
    }
}

/// Go `ValueHandler.ServeHTTP` (`/tables/{colID}/{colTp}/{colFlag}/{colLen}`):
/// decodes column `colID` of the base64 `rowBin` row as a column of that
/// type (decimal 6, UTC) and answers its `ToString` as a JSON string.
fn value_response(path: &str) -> Option<Result<String, String>> {
    let (route, query) = path.split_once('?').unwrap_or((path, ""));
    let parts: Vec<&str> = route.strip_prefix("/tables/")?.split('/').collect();
    let [col_id, col_tp, col_flag, col_len] = parts.as_slice() else {
        return None;
    };
    Some((|| {
        let col_id = go_parse_int_base0(col_id)?;
        let col_tp = go_parse_int_base0(col_tp)?;
        let col_flag = go_parse_int_base0(col_flag)
            .map_err(|error| error.replace("ParseInt", "ParseUint"))?;
        let col_len = go_parse_int_base0(col_len)?;
        // Go `parseQuery(RawQuery, values, false)`: `&` or `;` separated,
        // values left escaped.
        let row_bin: Vec<&str> = query
            .split(['&', ';'])
            .filter_map(|pair| pair.split_once('=').filter(|(key, _)| *key == "rowBin").map(|(_, value)| value))
            .collect();
        let [bin] = row_bin.as_slice() else {
            return Err(format!(
                "Invalid Query:[{}]",
                row_bin.join(" ")
            ));
        };
        use base64::Engine as _;
        let value_data = base64::engine::general_purpose::STANDARD
            .decode(bin)
            .map_err(|error| format!("illegal base64 data at input byte {}", base64_error_offset(&error)))?;
        // Go `byte(colTp)`.
        let code = tidb_datatype::FieldTypeCode::from_mysql_type(col_tp as u8);
        let mut field_type = tidb_datatype::FieldType::new(code);
        field_type.set_flag(col_flag as u32);
        field_type.set_flen(col_len);
        field_type.set_decimal(6);
        let columns = std::collections::BTreeMap::from([(col_id, field_type)]);
        let values = tidb_tablecodec::decode_table_row_to_map(
            &value_data,
            &columns,
            Some(&tidb_datatype::SessionTimeZone::utc()),
        )
        .map_err(|error| error.to_string())?;
        let value = values.get(&col_id).cloned().unwrap_or(tidb_datatype::Datum::Null);
        let text = value.sql_string().map_err(|error| error.to_string())?;
        serde_json::to_string(&text).map_err(|error| error.to_string())
    })())
}

/// The input offset Go's `base64.CorruptInputError` reports.
fn base64_error_offset(error: &base64::DecodeError) -> usize {
    match error {
        base64::DecodeError::InvalidByte(offset, _) | base64::DecodeError::InvalidLastSymbol(offset, _) => *offset,
        base64::DecodeError::InvalidLength(length) => *length,
        base64::DecodeError::InvalidPadding => 0,
    }
}

/// Go getTableByIDStr searches all schemas by globally unique table ID.
fn find_table_by_id(
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    id: i64,
) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
    catalog
        .stored_schemas()
        .iter()
        .find_map(|database| database.read().deprecated_tables.iter_deref().find(|table| table.read().id == id))
}

/// Go `SchemaHandler.ServeHTTP` (`tikvhandler/tikv_handler.go`): the three
/// `/schema` routes and the three form values they read, answered from the
/// node's current catalog.
///
/// `None` means the path is not a schema route at all, which the caller
/// answers 404 as before. `Some(Err(..))` is Go's `WriteError`, which it uses
/// for a database or table the catalog does not hold.
///
/// The bodies are the SAME `DBInfo`/`TableInfo` JSON the catalog stores, so
/// what this serves and what a peer reads back cannot drift: both go through
/// `tidb_meta::value`'s serializers.
fn schema_response(
    path: &str,
    source: &dyn Fn() -> tidb_infoschema::SessionExtendedInfoSchema,
) -> Option<Result<String, String>> {
    // Go's mux strips the query string before matching, and the handler then
    // reads its own form values from it.
    let (path, query) = match path.split_once('?') {
        Some((path, query)) => (path, query),
        None => (path, ""),
    };
    let form = |name: &str| {
        query.split('&').find_map(|pair| {
            pair.split_once('=')
                .filter(|(key, _)| *key == name)
                .map(|(_, value)| value)
        })
    };
    let rest = path.strip_prefix("/schema")?;
    let parts: Vec<&str> = rest.split('/').filter(|part| !part.is_empty()).collect();
    if !rest.is_empty() && !rest.starts_with('/') {
        // `/schema_storage` and friends are different routes, not this one.
        return None;
    }
    let catalog = source();
    let encode = |bytes: Result<Vec<u8>, String>| -> Result<String, String> {
        bytes.and_then(|body| String::from_utf8(body).map_err(|error| error.to_string()))
    };
    // Go reads `table_id`/`table_ids` BEFORE falling through to all
    // databases, so they only apply to the bare `/schema` route.
    if parts.is_empty() {
        if let Some(id) = form("table_id") {
            return Some(match id.parse::<i64>() {
                Ok(id) => match find_table_by_id(&catalog, id) {
                    Some(found) => {
                        encode(tidb_infoschema::cluster_catalog::stored_table_info_json(&found.read()))
                    }
                    // Go `getTableByIDStr`'s own message.
                    None => Err(format!(
                        "[schema:1146]Table which ID = {id} does not exist."
                    )),
                },
                Err(error) => Err(error.to_string()),
            });
        }
        if let Some(ids) = form("table_ids") {
            // Go builds a map keyed by the id, skipping ids it cannot find,
            // and errors only when NONE resolved.
            let mut out = String::from("{");
            let mut found_any = false;
            for id in ids.split(',').filter_map(|id| id.parse::<i64>().ok()) {
                let Some(found) = find_table_by_id(&catalog, id) else {
                    continue;
                };
                let found = found.read();
                match encode(tidb_infoschema::cluster_catalog::stored_table_info_json(&found)) {
                    Ok(body) => {
                        if found_any {
                            out.push(',');
                        }
                        out.push_str(&format!("\"{id}\":{body}"));
                        found_any = true;
                    }
                    Err(error) => return Some(Err(error)),
                }
            }
            out.push('}');
            return Some(if found_any {
                Ok(out)
            } else {
                Err("All tables are not found".to_owned())
            });
        }
    }
    match parts.as_slice() {
        // All databases' schemas: Go `WriteData(w, schema.AllSchemas())`.
        [] => {
            let mut out = String::from("[");
            for (index, database) in catalog.stored_schemas().iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                match encode(tidb_infoschema::cluster_catalog::stored_db_info_json(
                    &database.read(),
                )) {
                    Ok(body) => out.push_str(&body),
                    Err(error) => return Some(Err(error)),
                }
            }
            out.push(']');
            Some(Ok(out))
        }
        // One database's tables: Go `WriteDBTablesData`, a JSON array of
        // TableInfo, and an EMPTY array rather than null when it has none.
        [database] => {
            let Some(found) = catalog.stored_schemas().into_iter().find(|candidate| {
                candidate
                    .read().name
                    .original()
                    .eq_ignore_ascii_case(database)
            }) else {
                return Some(Err(format!("[schema:1049]Unknown database '{database}'")));
            };
            // Go `SchemaSimpleTableInfos`: `{id, name}` per table, for a
            // caller that wants the listing without every column.
            if form("id_name_only") == Some("true") {
                let mut out = String::from("[");
                for (index, table) in found.read().deprecated_tables.iter_deref().enumerate() {
                    let table = table.read();
                    if index > 0 {
                        out.push(',');
                    }
                    let name = table.name.original();
                    out.push_str(&format!(
                        "{{\"id\":{},\"name\":{{\"O\":\"{name}\",\"L\":\"{}\"}}}}",
                        table.id,
                        table.name.lowercase(),
                    ));
                }
                out.push(']');
                return Some(Ok(out));
            }
            let mut out = String::from("[");
            for (index, table) in found.read().deprecated_tables.iter_deref().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                match encode(tidb_infoschema::cluster_catalog::stored_table_info_json(&table.read())) {
                    Ok(body) => out.push_str(&body),
                    Err(error) => return Some(Err(error)),
                }
            }
            out.push(']');
            Some(Ok(out))
        }
        // One table: Go `WriteData(w, data.Meta())`.
        [database, table] => {
            let found = catalog
                .stored_schemas()
                .iter()
                .find(|candidate| {
                    candidate
                        .read().name
                        .original()
                        .eq_ignore_ascii_case(database)
                })
                .and_then(|database| {
                    database
                        .read().deprecated_tables
                        .iter_deref()
                        .find(|candidate| candidate.read().name.original().eq_ignore_ascii_case(table))
                });
            match found {
                Some(found) => Some(encode(tidb_infoschema::cluster_catalog::stored_table_info_json(
                    &found.read(),
                ))),
                None => Some(Err(format!(
                    "[schema:1146]Table '{database}.{table}' doesn't exist"
                ))),
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod schema_route_tests {
    use super::*;

    fn catalog_with(databases: Vec<tidb_model::db::DBInfo>) -> SchemaSource {
        let catalog = tidb_infoschema::SessionExtendedInfoSchema::init_with_db_infos(
            databases.into_iter().map(tidb_model::GoShared::new).collect(),
            Vec::new(),
            Vec::new(),
            7,
        );
        Arc::new(move || catalog.clone())
    }

    fn database(
        name: &str,
        tables: Vec<tidb_model::TableInfo>,
    ) -> tidb_model::db::DBInfo {
        tidb_model::DBInfo {
            id: 2,
            name: tidb_ast::CiString::new(name),
            charset: "utf8mb4".to_owned(),
            collate: "utf8mb4_bin".to_owned(),
            state: tidb_model::SchemaState::PUBLIC,
            deprecated_tables: tables.into(),
            ..tidb_model::DBInfo::default()
        }
    }

    fn table(name: &str) -> tidb_model::TableInfo {
        // Distinct ids, so a lookup by id proves it found the right one.
        let id = 30 + i64::from(name.as_bytes().last().copied().unwrap_or(b'0') - b'0');
        tidb_model::TableInfo {
            id,
            name: tidb_ast::CiString::new(name),
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::TableInfo::default()
        }
    }

    /// Go `DBTableHandler`: `/db-table/{tableID}` answers the owning
    /// database, the table and the schema version; an unknown ID or a
    /// non-number is Go's `WriteError` text.
    #[test]
    fn the_db_table_route_answers_the_owner_of_a_table_id() {
        let source = catalog_with(vec![database("test", vec![table("t1"), table("t2")])]);
        let body = db_table_response("/db-table/32", source.as_ref()).unwrap().unwrap();
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["db_info"]["db_name"]["O"], "test", "{body}");
        assert_eq!(value["table_info"]["id"], 32, "{body}");
        assert_eq!(value["table_info"]["name"]["O"], "t2", "{body}");
        assert_eq!(value["schema_version"], 7, "{body}");
        assert_eq!(
            db_table_response("/db-table/999", source.as_ref()).unwrap().unwrap_err(),
            "[schema:1146]Table which ID = 999 does not exist."
        );
        assert_eq!(db_table_response("/db-table/x", source.as_ref()).unwrap().unwrap_err(), "Wrong tableID: x");
        assert!(db_table_response("/db-table/1/2", source.as_ref()).is_none());
    }

    /// Go's handlers answer an error with `handler.WriteError`: 400 and the
    /// error text.
    #[test]
    fn a_route_error_is_gos_write_error_400() {
        let routes = StatusRoutes {
            schema: Some(catalog_with(vec![database("test", vec![table("t1")])])),
            ..StatusRoutes::default()
        };
        let tracker = ConnectionTracker::default();
        for path in ["/schema/nosuch", "/db-table/999", "/tables/x/8/0/20?rowBin=AA=="] {
            let response = route_response(path, "", &tracker, "v", "h", &routes);
            assert_eq!(response.status(), 400, "{path}");
        }
        assert_eq!(route_response("/db-table/31", "", &tracker, "v", "h", &routes).status(), 200);
    }

    /// Go `LabelHandler`: only POST; the posted labels merge over the
    /// configured ones into the global config, which the answer shows.
    #[test]
    fn the_labels_route_merges_posted_labels_into_the_config() {
        let mut config = (*tidb_config::config_tree::config::get_global_config()).clone();
        config.labels = std::collections::HashMap::from([("host".to_owned(), "h1".to_owned())]);
        tidb_config::config_tree::config::store_global_config(config);
        let post = |body: &str| format!("POST /labels HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}", body.len());
        assert_eq!(
            labels_response("/labels", "GET /labels HTTP/1.1\r\n\r\n", None).unwrap().unwrap_err(),
            "This api only support POST method"
        );
        assert_eq!(
            labels_response("/labels", &post(r#"{"zone":"z1","host":"h2"}"#), None).unwrap().unwrap(),
            r#"{"host":"h2","zone":"z1"}"#
        );
        assert_eq!(tidb_config::config_tree::config::get_global_config().labels["zone"], "z1");
        // An empty object changes nothing and answers the labels.
        assert_eq!(labels_response("/labels", &post("{}"), None).unwrap().unwrap(), r#"{"host":"h2","zone":"z1"}"#);
        assert!(labels_response("/labels", &post("nope"), None).unwrap().is_err());
        assert!(labels_response("/labelsx", &post("{}"), None).is_none());
    }

    /// Go `TestDecodeColumnValue`: each column of a new-format row decodes
    /// through `/tables/{colID}/{colTp}/{colFlag}/{colLen}?rowBin=` to its
    /// `ToString` as a JSON string.
    #[test]
    fn the_value_route_decodes_one_column_of_a_row() {
        use base64::Engine as _;
        let utc = tidb_datatype::SessionTimeZone::utc();
        let row = [
            tidb_datatype::Datum::Int(100),
            tidb_datatype::Datum::Bytes(b"abc".to_vec()),
            tidb_datatype::Datum::Decimal(tidb_datatype::Decimal::from_literal("1")),
        ];
        let bytes = tidb_tablecodec::encode_table_row(Some(&utc), &row, &[1, 2, 3], true, None).unwrap();
        let bin = base64::engine::general_purpose::STANDARD.encode(bytes);
        let decode = |column: &str| value_response(&format!("/tables/{column}?rowBin={bin}")).unwrap();
        assert_eq!(decode("1/8/0/20").unwrap(), "\"100\"");
        assert_eq!(decode("2/15/0/-1").unwrap(), "\"abc\"");
        assert_eq!(decode("3/246/0/-1").unwrap(), "\"1\"");
        // Go `strconv.ParseInt(s, 0, 64)` takes a base prefix.
        assert_eq!(decode("0x1/0x8/0/20").unwrap(), "\"100\"");
        assert_eq!(
            decode("a/8/0/20").unwrap_err(),
            "strconv.ParseInt: parsing \"a\": invalid syntax"
        );
        assert_eq!(value_response("/tables/1/8/0/20").unwrap().unwrap_err(), "Invalid Query:[]");
        assert!(value_response("/tables/test/t/regions").is_none());
    }

    /// Go reads three form values on `/schema`, and each changes the answer.
    ///
    /// `table_id`/`table_ids` search across databases, because a table id is
    /// unique cluster-wide; `id_name_only` narrows a database listing to
    /// `{id, name}` for a caller that does not want every column.
    #[test]
    fn the_schema_form_values_answer_gos_shapes() {
        let source = catalog_with(vec![database("test", vec![table("t1"), table("t2")])]);

        // `id_name_only` is the listing, not the definitions.
        let listing = schema_response("/schema/test?id_name_only=true", source.as_ref())
            .expect("a schema route")
            .expect("serialises");
        assert!(
            listing.contains("\"id\":31") && listing.contains("\"t1\""),
            "{listing}"
        );
        assert!(
            !listing.contains("\"cols\""),
            "id_name_only must not carry column definitions: {listing}"
        );

        // A single id answers a bare TableInfo.
        let one = schema_response("/schema?table_id=31", source.as_ref())
            .expect("a schema route")
            .expect("serialises");
        assert!(one.starts_with('{') && one.contains("\"t1\""), "{one}");

        // Several ids answer a map KEYED by id, skipping ones not found.
        let many = schema_response("/schema?table_ids=31,32,999999", source.as_ref())
            .expect("a schema route")
            .expect("serialises");
        assert!(many.starts_with("{\"31\":"), "{many}");
        assert!(many.contains("\"32\":"), "{many}");
        assert!(!many.contains("999999"), "a missing id is skipped: {many}");

        // Go errors only when NONE of the ids resolved.
        let none = schema_response("/schema?table_ids=999999", source.as_ref())
            .expect("a schema route")
            .expect_err("refused");
        assert!(none.contains("All tables are not found"), "{none}");

        let missing = schema_response("/schema?table_id=999999", source.as_ref())
            .expect("a schema route")
            .expect_err("refused");
        assert!(
            missing.contains("1146") && missing.contains("999999"),
            "{missing}"
        );
    }

    /// Go `SchemaHandler.ServeHTTP`: three routes over the live catalog.
    ///
    /// The bodies are the same `DBInfo`/`TableInfo` JSON the catalog stores,
    /// so what this serves and what a peer reads back cannot drift.
    #[test]
    fn the_schema_routes_answer_gos_three_shapes() {
        let source = catalog_with(vec![database("test", vec![table("t1"), table("t2")])]);

        // All databases: an array of DBInfo.
        let all = schema_response("/schema", source.as_ref())
            .expect("a schema route")
            .expect("serialises");
        assert!(all.starts_with('[') && all.contains("\"db_name\""), "{all}");

        // One database: an array of TableInfo, not of names.
        let db = schema_response("/schema/test", source.as_ref())
            .expect("a schema route")
            .expect("serialises");
        assert!(db.starts_with('['), "{db}");
        assert!(db.contains("\"t1\"") && db.contains("\"t2\""), "{db}");

        // One table: a bare TableInfo object.
        let one = schema_response("/schema/test/t1", source.as_ref())
            .expect("a schema route")
            .expect("serialises");
        assert!(one.starts_with('{') && one.contains("\"t1\""), "{one}");
        assert!(!one.contains("\"t2\""), "{one}");

        // The lookups are case-insensitive, as Go's CIStr comparison is.
        assert!(schema_response("/schema/TEST/T1", source.as_ref())
            .expect("a schema route")
            .is_ok());
    }

    /// Go `handler.WriteError` for a name the catalog does not hold, with the
    /// error numbers its own infoschema errors carry.
    #[test]
    fn a_missing_schema_name_reports_gos_error() {
        let source = catalog_with(vec![database("test", vec![table("t1")])]);

        let missing_db = schema_response("/schema/nosuch", source.as_ref())
            .expect("a schema route")
            .expect_err("refused");
        assert!(
            missing_db.contains("1049") && missing_db.contains("nosuch"),
            "{missing_db}"
        );

        let missing_table = schema_response("/schema/test/nosuch", source.as_ref())
            .expect("a schema route")
            .expect_err("refused");
        assert!(
            missing_table.contains("1146") && missing_table.contains("test.nosuch"),
            "{missing_table}"
        );
    }

    /// An empty database answers an empty ARRAY, which is Go's
    /// `manualWriteJSONArray` behaviour -- not `null`, which a client
    /// iterating the result would trip over.
    #[test]
    fn an_empty_database_answers_an_empty_array() {
        let source = catalog_with(vec![database("empty", Vec::new())]);
        assert_eq!(
            schema_response("/schema/empty", source.as_ref())
                .expect("a schema route")
                .expect("serialises"),
            "[]"
        );
    }

    /// Paths that merely start with the same letters are other routes, and
    /// must keep falling through to 404 rather than being answered here.
    #[test]
    fn neighbouring_paths_are_not_schema_routes() {
        let source = catalog_with(vec![database("test", Vec::new())]);
        for path in [
            "/schema_storage",
            "/schema_storage/test",
            "/status",
            "/metrics",
        ] {
            assert!(
                schema_response(path, source.as_ref()).is_none(),
                "{path} was answered as a schema route"
            );
        }
        // A query string is stripped before matching, as Go's mux does.
        assert!(schema_response("/schema?id_name_only=true", source.as_ref()).is_some());
    }
}
