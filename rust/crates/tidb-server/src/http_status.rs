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

//! The status HTTP listener: the `/status` slice of Go's
//! `pkg/server/http_status.go`.
//!
//! Go's status server mounts dozens of handlers; the one every client and
//! `cmd/tidb-server`'s own `main_test.go` reach first is `/status`
//! (`http_status.go:689`), whose body is the `Status` struct
//! (`:675`): `{connections, version, git_hash, status:{init_stats_percentage}}`
//! in exactly that field order. This module serves that endpoint over a
//! hand-rolled HTTP/1.1 loop and answers 404 for every other path.
//!
//! # Narrowings, each naming its Go symbol
//!
//! * `/metrics` (the prometheus registry) and the rest of the router —
//!   `/settings`, `/schema`, pprof — are unported handlers; they answer 404
//!   here where Go serves them.
//! * `s.health.Load()` — the 500-during-shutdown arm — narrows with the
//!   graceful-shutdown integration; this listener lives for the process.
//! * `initstats.InitStatsPercentage` reads 100 here: this node loads its
//!   statistics before the SQL listener opens, which is the state Go's
//!   gauge reports as 100 once init completes.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

use crate::sql_node::ConnectionTracker;

/// A running status listener; the accept thread lives for the process,
/// as Go's does until shutdown.
pub struct StatusServer {
    local_addr: std::net::SocketAddr,
}

impl StatusServer {
    /// The bound address, for logs and tests.
    #[must_use]
    pub const fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
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
    start_status_listener_with_schema(host, port, tracker, version, git_hash, None)
}

/// [`start_status_listener`] also answering Go's `/schema` routes.
pub fn start_status_listener_with_schema(
    host: &str,
    port: u16,
    tracker: Arc<ConnectionTracker>,
    version: String,
    git_hash: String,
    schema: Option<SchemaSource>,
) -> std::io::Result<StatusServer> {
    let listener = TcpListener::bind((host, port))?;
    let local_addr = listener.local_addr()?;
    std::thread::Builder::new()
        .name("tidb-status-http".to_owned())
        .spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { continue };
                let tracker = Arc::clone(&tracker);
                let version = version.clone();
                let git_hash = git_hash.clone();
                let schema = schema.clone();
                // One short-lived thread per request keeps the accept loop
                // from stalling on a slow client without an executor.
                let _ = std::thread::Builder::new()
                    .name("tidb-status-conn".to_owned())
                    .spawn(move || {
                        let mut buffer = [0_u8; 4096];
                        let Ok(read) = stream.read(&mut buffer) else {
                            return;
                        };
                        let request = String::from_utf8_lossy(&buffer[..read]);
                        let path = request
                            .lines()
                            .next()
                            .and_then(|line| line.split_whitespace().nth(1))
                            .unwrap_or("");
                        let response = if path == "/status" {
                            // Go `handleStatus`: the Status struct's field
                            // order, `Content-Type: application/json`.
                            let body = format!(
                                "{{\"connections\":{},\"version\":\"{}\",\"git_hash\":\"{}\",\
                                 \"status\":{{\"init_stats_percentage\":100}}}}",
                                tracker.active(),
                                version,
                                git_hash,
                            );
                            format!(
                                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                 Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                body.len(),
                            )
                        } else if let Some(body) = schema
                            .as_ref()
                            .and_then(|source| schema_response(path, source.as_ref()))
                        {
                            match body {
                                Ok(body) => format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len(),
                                ),
                                // Go `handler.WriteError`: the message as
                                // text, under 500.
                                Err(message) => format!(
                                    "HTTP/1.1 500 Internal Server Error\r\n\
                                     Content-Type: text/plain\r\nContent-Length: {}\r\n\
                                     Connection: close\r\n\r\n{message}",
                                    message.len(),
                                ),
                            }
                        } else {
                            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\
                             Connection: close\r\n\r\n"
                                .to_owned()
                        };
                        let _ = stream.write_all(response.as_bytes());
                    });
            }
        })?;
    Ok(StatusServer { local_addr })
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let status = fetch("/status");
        assert!(status.starts_with("HTTP/1.1 200 OK"), "{status}");
        assert!(
            status.contains(
                "{\"connections\":0,\"version\":\"8.0.11-TiDB-test\",\"git_hash\":\"abc123\",\
                 \"status\":{\"init_stats_percentage\":100}}"
            ),
            "Go's field order, exactly: {status}"
        );
        assert!(fetch("/nosuch").starts_with("HTTP/1.1 404"));
    }
}

/// Go `SchemaHandler.ServeHTTP` (`tikvhandler/tikv_handler.go`): the three
/// `/schema` routes, answered from the node's current catalog.
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
    // Go's mux strips the query string before matching; so does this.
    let path = path.split('?').next().unwrap_or(path);
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
    match parts.as_slice() {
        // All databases' schemas: Go `WriteData(w, schema.AllSchemas())`.
        [] => {
            let mut out = String::from("[");
            for (index, database) in catalog.databases.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                match encode(tidb_exec::cluster_catalog::stored_db_info_json(&database.info)) {
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
            let Some(found) = catalog
                .databases
                .iter()
                .find(|candidate| candidate.info.name.original().eq_ignore_ascii_case(database))
            else {
                return Some(Err(format!("[schema:1049]Unknown database '{database}'")));
            };
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
                .find(|candidate| candidate.info.name.original().eq_ignore_ascii_case(database))
                .and_then(|database| {
                    database
                        .tables
                        .iter()
                        .find(|candidate| candidate.name.original().eq_ignore_ascii_case(table))
                });
            match found {
                Some(found) => Some(encode(tidb_exec::cluster_catalog::stored_table_info_json(found))),
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

    fn database(name: &str, tables: Vec<tidb_model::TableInfo>) -> tidb_exec::cluster_catalog::LoadedDatabase {
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
        tidb_model::TableInfo {
            id: 30,
            name: tidb_ast::CiString::new(name),
            state: tidb_model::SchemaState::PUBLIC,
            ..tidb_model::TableInfo::default()
        }
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
        assert!(missing_db.contains("1049") && missing_db.contains("nosuch"), "{missing_db}");

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
        for path in ["/schema_storage", "/schema_storage/test", "/status", "/metrics"] {
            assert!(
                schema_response(path, source.as_ref()).is_none(),
                "{path} was answered as a schema route"
            );
        }
        // A query string is stripped before matching, as Go's mux does.
        assert!(schema_response("/schema?id_name_only=true", source.as_ref()).is_some());
    }
}
