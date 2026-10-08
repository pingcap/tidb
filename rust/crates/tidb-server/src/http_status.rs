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
pub type SchemaSource = Arc<dyn Fn() -> tidb_exec::cluster_catalog::ClusterCatalog + Send + Sync>;

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
    let config = tidb_config::config_tree::config::get_global_config()
        .status
        .clone();
    let worker = std::thread::Builder::new()
        .name("tidb-status".into())
        .spawn(move || {
            runtime.block_on(async move {
                let router = if let Some(peer) = routes.peer.clone() {
                    tonic::service::Routes::new(
                        tidb_proto::tikvpb::tikv_server::TikvServer::new(peer)
                            .max_encoding_message_size(
                                config.grpc_max_send_msg_size.max(0) as usize
                            ),
                    )
                    .into_axum_router()
                } else {
                    axum::Router::new()
                };
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
        body.push_str(&tidb_txnkv::client_go_metrics::gather_text());
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
            Err(message) => (500, "text/plain", message),
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
            tidb_session::GlobalSysvars::default(),
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

/// Go getTableByIDStr searches all schemas by globally unique table ID.
fn find_table_by_id(
    catalog: &tidb_exec::cluster_catalog::ClusterCatalog,
    id: i64,
) -> Option<&tidb_model::TableInfo> {
    catalog
        .databases
        .iter()
        .find_map(|database| database.tables.iter().find(|table| table.id == id))
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
    source: &dyn Fn() -> tidb_exec::cluster_catalog::ClusterCatalog,
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
                        encode(tidb_exec::cluster_catalog::stored_table_info_json(found))
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
                match encode(tidb_exec::cluster_catalog::stored_table_info_json(found)) {
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
            for (index, database) in catalog.databases.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                match encode(tidb_exec::cluster_catalog::stored_db_info_json(
                    &database.info,
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
            let Some(found) = catalog.databases.iter().find(|candidate| {
                candidate
                    .info
                    .name
                    .original()
                    .eq_ignore_ascii_case(database)
            }) else {
                return Some(Err(format!("[schema:1049]Unknown database '{database}'")));
            };
            // Go `SchemaSimpleTableInfos`: `{id, name}` per table, for a
            // caller that wants the listing without every column.
            if form("id_name_only") == Some("true") {
                let mut out = String::from("[");
                for (index, table) in found.tables.iter().enumerate() {
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
            for (index, table) in found.tables.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                match encode(tidb_exec::cluster_catalog::stored_table_info_json(table)) {
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
                .databases
                .iter()
                .find(|candidate| {
                    candidate
                        .info
                        .name
                        .original()
                        .eq_ignore_ascii_case(database)
                })
                .and_then(|database| {
                    database
                        .tables
                        .iter()
                        .find(|candidate| candidate.name.original().eq_ignore_ascii_case(table))
                });
            match found {
                Some(found) => Some(encode(tidb_exec::cluster_catalog::stored_table_info_json(
                    found,
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

    fn catalog_with(databases: Vec<tidb_exec::cluster_catalog::LoadedDatabase>) -> SchemaSource {
        let catalog = tidb_exec::cluster_catalog::ClusterCatalog {
            schema_version: 7,
            databases,
        };
        Arc::new(move || catalog.clone())
    }

    fn database(
        name: &str,
        tables: Vec<tidb_model::TableInfo>,
    ) -> tidb_exec::cluster_catalog::LoadedDatabase {
        tidb_exec::cluster_catalog::LoadedDatabase {
            info: tidb_model::DBInfo {
                id: 2,
                name: tidb_ast::CiString::new(name),
                charset: "utf8mb4".to_owned(),
                collate: "utf8mb4_bin".to_owned(),
                state: tidb_model::SchemaState::PUBLIC,
                ..tidb_model::DBInfo::default()
            },
            tables,
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
