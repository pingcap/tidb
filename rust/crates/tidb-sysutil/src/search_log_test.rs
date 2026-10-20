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

//! Go `search_log_test.go`, over a real gRPC server and client.

extern crate test;

use std::io::Write;
use std::path::Path;
use std::time::Duration;

use tidb_proto::diagnosticspb as pb;

use crate::search_log::{
    parse_log_item, parse_log_level, parse_time_stamp, read_last_lines, resolve_files,
};
use crate::DiagnosticsServer;

/// Go `searchLogSuite`: a diagnostics server over `rpc.tidb.log` in a
/// temporary directory.
pub(crate) struct SearchLogSuite {
    pub(crate) tmp_dir: tempfile::TempDir,
    pub(crate) address: std::net::SocketAddr,
    pub(crate) runtime: tokio::runtime::Runtime,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Drop for SearchLogSuite {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

/// Go `createSearchLogSuite`.
pub(crate) fn create_search_log_suite() -> SearchLogSuite {
    let tmp_dir = tempfile::Builder::new()
        .prefix("sysutil")
        .tempdir()
        .unwrap();
    let log_file = tmp_dir.path().join("rpc.tidb.log");
    let server = DiagnosticsServer::new(log_file.to_string_lossy().into_owned());
    serve(tmp_dir, server)
}

/// Serves `server` on an available local port until the suite drops.
pub(crate) fn serve(tmp_dir: tempfile::TempDir, server: DiagnosticsServer) -> SearchLogSuite {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let listener = runtime
        .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
        .expect("cannot find available port");
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    runtime.spawn(async move {
        tonic::transport::Server::builder()
            .add_service(pb::diagnostics_server::DiagnosticsServer::new(server))
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                async {
                    let _ = stopped.await;
                },
            )
            .await
            .expect("failed to serve");
    });
    SearchLogSuite {
        tmp_dir,
        address,
        runtime,
        stop: Some(stop),
    }
}

impl SearchLogSuite {
    fn path(&self, filename: &str) -> std::path::PathBuf {
        self.tmp_dir.path().join(filename)
    }

    /// Go `writeTmpFile`: the lines joined by newlines, no trailing one.
    fn write_tmp_file(&self, filename: &str, lines: &[&str]) {
        std::fs::write(self.path(filename), lines.join("\n"))
            .unwrap_or_else(|error| panic!("write tmp file {filename} failed: {error}"));
    }

    /// Go `writeTmpGzipFile`.
    fn write_tmp_gzip_file(&self, filename: &str, lines: &[&str]) {
        let file = std::fs::File::create(self.path(filename)).unwrap();
        let mut gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        gz.write_all(lines.join("\n").as_bytes()).unwrap();
        gz.finish().unwrap();
    }

    /// Streams one `SearchLog` request and gathers every message.
    fn search(&self, req: pb::SearchLogRequest) -> Vec<pb::LogMessage> {
        let address = format!("http://{}", self.address);
        self.runtime.block_on(async move {
            let mut client = pb::diagnostics_client::DiagnosticsClient::connect(address)
                .await
                .unwrap();
            let mut stream = tokio::time::timeout(Duration::from_secs(1), client.search_log(req))
                .await
                .unwrap()
                .unwrap()
                .into_inner();
            let mut messages = Vec::new();
            while let Some(res) = stream.message().await.unwrap() {
                messages.extend(res.messages);
            }
            messages
        })
    }
}

fn ts(s: &str) -> i64 {
    parse_time_stamp(s).unwrap()
}

fn never() -> bool {
    false
}

#[test]
#[allow(clippy::type_complexity)]
fn test_resolve_files() {
    let s = create_search_log_suite();
    s.write_tmp_file(
        "tidb.log",
        &[
            r#"20/08/26 06:19:13.011 -04:00 [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            "hello TiDB",
            r#"[2019/08/26 06:19:13.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:19:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:19:15.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:19:16.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:19:17.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"20/08/26 06:19:13.011 -04:00 [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            "hello TiDB",
        ],
    );
    // single line file
    s.write_tmp_file(
        "tidb-1.log",
        &[r#"[2019/08/26 06:20:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#],
    );
    // two lines file
    s.write_tmp_file(
        "tidb-2.log",
        &[
            r#"[2019/08/26 06:21:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:21:15.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
        ],
    );
    // empty file
    s.write_tmp_file("tidb-3.log", &[""]);
    s.write_tmp_file(
        "tidb-4.log",
        &[
            r#"[2019/08/26 06:22:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:15.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:16.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:17.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
        ],
    );

    let all = [
        (
            "2019/08/26 06:19:13.011 -04:00",
            "2019/08/26 06:19:17.011 -04:00",
        ),
        (
            "2019/08/26 06:20:14.011 -04:00",
            "2019/08/26 06:20:14.011 -04:00",
        ),
        (
            "2019/08/26 06:21:14.011 -04:00",
            "2019/08/26 06:21:15.011 -04:00",
        ),
        (
            "2019/08/26 06:22:14.011 -04:00",
            "2019/08/26 06:22:17.011 -04:00",
        ),
    ];
    let cases: Vec<((&str, &str), Vec<(&str, &str)>)> = vec![
        // 0: all files
        (
            (
                "2019/08/26 06:19:13.011 -04:00",
                "2019/08/26 06:22:17.011 -04:00",
            ),
            all.to_vec(),
        ),
        // 1: emptys
        (
            (
                "2019/08/26 06:29:13.011 -04:00",
                "2019/08/26 06:32:17.011 -04:00",
            ),
            vec![],
        ),
        // 2: single line
        (
            (
                "2019/08/26 06:20:14.011 -04:00",
                "2019/08/26 06:20:14.011 -04:00",
            ),
            vec![all[1]],
        ),
        // 3
        (
            (
                "2019/08/26 06:21:14.011 -04:00",
                "2019/08/26 06:21:15.011 -04:00",
            ),
            vec![all[2]],
        ),
        // 4
        (
            (
                "2019/08/26 06:20:14.011 -04:00",
                "2019/08/26 06:21:15.011 -04:00",
            ),
            vec![all[1], all[2]],
        ),
        // 5
        (
            (
                "2019/08/26 06:20:14.011 -04:00",
                "2019/08/26 06:22:17.011 -04:00",
            ),
            vec![all[1], all[2], all[3]],
        ),
        // 6
        (
            (
                "2019/08/26 06:20:14.011 -04:00",
                "2019/08/26 06:22:15.011 -04:00",
            ),
            vec![all[1], all[2], all[3]],
        ),
        // 7
        (
            (
                "2019/08/26 06:19:15.011 -04:00",
                "2019/08/26 06:22:15.011 -04:00",
            ),
            all.to_vec(),
        ),
        // 8
        (
            (
                "2019/08/26 06:22:14.011 -04:00",
                "2019/08/26 06:22:16.011 -04:00",
            ),
            vec![all[3]],
        ),
    ];
    let log_file = s.path("tidb.log").to_string_lossy().into_owned();
    for (i, ((start, end), expect)) in cases.into_iter().enumerate() {
        let log_files = resolve_files(&never, &log_file, ts(start), ts(end)).unwrap();
        assert_eq!(
            log_files.len(),
            expect.len(),
            "search range (index: {i}): {start} {end}"
        );
        for (j, (b, e)) in expect.into_iter().enumerate() {
            assert_eq!(
                log_files[j].begin_time(),
                ts(b),
                "case index: {i}, expect index: {j}"
            );
            assert_eq!(
                log_files[j].end_time(),
                ts(e),
                "case index: {i}, expect index: {j}"
            );
        }
    }
}

const WELCOME: &str = r#"[printer.go:41] ["Welcome to TiDB."]"#;

#[test]
fn test_log_iterator() {
    let s = create_search_log_suite();
    s.write_tmp_file(
        "rpc.tidb.log",
        &[
            r#"[2019/08/26 06:19:13.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:19:14.011 -04:00] [WARN] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:19:15.011 -04:00] [ERROR] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:19:16.011 -04:00] [DEBUG] [printer.go:41] ["Welcome to TiDB."]"#,
            "This is an invalid log blablabla][",
            r#"[2019/08/26 06:19:17.011 -04:00] ] [INFO] invalid log"]"#,
            r#"[2019/08/26 06:19:17.011 -04:00] [TRACE] [printer.go:41] ["Welcome to TiDB."]"#,
        ],
    );
    // single line file
    s.write_tmp_file(
        "rpc.tidb-1.log",
        &[r#"[2019/08/26 06:20:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#],
    );
    // two lines file
    s.write_tmp_file(
        "rpc.tidb-2.log",
        &[
            r#"[2019/08/26 06:21:14.011 -04:00] [WARN] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:21:15.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
        ],
    );
    // empty file
    s.write_tmp_file("rpc.tidb-3.log", &[""]);
    s.write_tmp_file(
        "rpc.tidb-4.log",
        &[
            r#"[2019/08/26 06:22:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            "This is also an invalid log contains partern ...",
            r#"[2019/08/26 06:22:14.011 -04:00] [WARN] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:15.011 -04:00] [ERROR] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:16.011 -04:00] [DEBUG] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:17.011 -04:00] [TRACE] [printer.go:41] ["Welcome to TiDB."]"#,
        ],
    );
    s.write_tmp_file(
        "rpc.tidb-5.log",
        &[
            r#"[2019/08/26 06:23:14.011 -04:00] [INFO] [printer.go:41] ["partern test to TiDB."]"#,
            r#"[2019/08/27 06:23:14.011 -04:00] [INFO] [printer.go:41] ["partern test txn to TiDB."]"#,
        ],
    );

    let l = |t: &str, level: &str, rest: &str| format!("[{t}] [{level}] {rest}");
    let first: Vec<String> = vec![
        l("2019/08/26 06:19:13.011 -04:00", "INFO", WELCOME),
        l("2019/08/26 06:19:14.011 -04:00", "WARN", WELCOME),
        l("2019/08/26 06:19:15.011 -04:00", "ERROR", WELCOME),
        l("2019/08/26 06:19:16.011 -04:00", "DEBUG", WELCOME),
        l(
            "2019/08/26 06:19:16.011 -04:00",
            "DEBUG",
            "This is an invalid log blablabla][",
        ),
        l(
            "2019/08/26 06:19:16.011 -04:00",
            "DEBUG",
            r#"[2019/08/26 06:19:17.011 -04:00] ] [INFO] invalid log"]"#,
        ),
        l("2019/08/26 06:19:17.011 -04:00", "TRACE", WELCOME),
    ];
    let middle: Vec<String> = vec![
        l("2019/08/26 06:20:14.011 -04:00", "INFO", WELCOME),
        l("2019/08/26 06:21:14.011 -04:00", "WARN", WELCOME),
        l("2019/08/26 06:21:15.011 -04:00", "INFO", WELCOME),
        l("2019/08/26 06:22:14.011 -04:00", "INFO", WELCOME),
        l(
            "2019/08/26 06:22:14.011 -04:00",
            "INFO",
            "This is also an invalid log contains partern ...",
        ),
        l("2019/08/26 06:22:14.011 -04:00", "WARN", WELCOME),
        l("2019/08/26 06:22:15.011 -04:00", "ERROR", WELCOME),
        l("2019/08/26 06:22:16.011 -04:00", "DEBUG", WELCOME),
        l("2019/08/26 06:22:17.011 -04:00", "TRACE", WELCOME),
    ];
    let last: Vec<String> = vec![
        l(
            "2019/08/26 06:23:14.011 -04:00",
            "INFO",
            r#"[printer.go:41] ["partern test to TiDB."]"#,
        ),
        l(
            "2019/08/27 06:23:14.011 -04:00",
            "INFO",
            r#"[printer.go:41] ["partern test txn to TiDB."]"#,
        ),
    ];
    let concat = |parts: &[&[String]]| -> Vec<String> { parts.concat() };

    struct Case {
        search: (&'static str, &'static str),
        expect: Vec<String>,
        levels: Vec<pb::LogLevel>,
        patterns: Vec<&'static str>,
    }
    let case = |search, expect| Case {
        search,
        expect,
        levels: vec![],
        patterns: vec![],
    };
    let cases = vec![
        // 0
        case(
            (
                "2019/08/26 06:19:13.011 -04:00",
                "2019/08/26 06:22:17.011 -04:00",
            ),
            concat(&[&first, &middle]),
        ),
        // 1
        case(
            (
                "2000/08/26 06:19:13.011 -04:00",
                "2099/08/26 06:22:17.011 -04:00",
            ),
            concat(&[&first, &middle, &last]),
        ),
        // 2
        case(
            (
                "2000/08/26 06:19:13.011 -04:00",
                "2019/08/26 06:19:17.011 -04:00",
            ),
            first.clone(),
        ),
        // 3
        case(
            (
                "2019/08/26 06:20:14.011 -04:00",
                "2099/08/26 06:22:17.011 -04:00",
            ),
            concat(&[&middle, &last]),
        ),
        // 4
        case(
            (
                "2019/08/26 06:19:15.011 -04:00",
                "2019/08/26 06:22:14.011 -04:00",
            ),
            concat(&[&first[2..], &middle[..6]]),
        ),
        // 5
        case(
            (
                "2019/08/26 06:19:14.011 -04:00",
                "2019/08/26 06:19:15.011 -04:00",
            ),
            first[1..3].to_vec(),
        ),
        // 6
        case(
            (
                "2019/08/26 06:19:14.011 -04:00",
                "2019/08/26 06:19:16.011 -04:00",
            ),
            first[1..6].to_vec(),
        ),
        // 7
        case(
            (
                "2019/08/26 06:19:14.011 -04:00",
                "2019/08/26 06:19:16.012 -04:00",
            ),
            first[1..6].to_vec(),
        ),
        // 8
        Case {
            levels: vec![pb::LogLevel::Debug],
            ..case(
                (
                    "2019/08/26 06:19:15.011 -04:00",
                    "2019/08/26 06:22:14.011 -04:00",
                ),
                first[3..6].to_vec(),
            )
        },
        // 9
        Case {
            levels: vec![pb::LogLevel::Trace],
            ..case(
                (
                    "2019/08/26 06:19:15.011 -04:00",
                    "2019/08/26 06:22:14.011 -04:00",
                ),
                vec![first[6].clone()],
            )
        },
        // 10
        Case {
            levels: vec![pb::LogLevel::Info],
            ..case(
                (
                    "2019/08/26 06:19:15.011 -04:00",
                    "2019/08/26 06:22:14.011 -04:00",
                ),
                vec![
                    middle[0].clone(),
                    middle[2].clone(),
                    middle[3].clone(),
                    middle[4].clone(),
                ],
            )
        },
        // 11
        Case {
            levels: vec![pb::LogLevel::Warn],
            ..case(
                (
                    "2019/08/26 06:19:15.011 -04:00",
                    "2019/08/26 06:22:14.011 -04:00",
                ),
                vec![middle[1].clone(), middle[5].clone()],
            )
        },
        // 12
        Case {
            levels: vec![pb::LogLevel::Error],
            ..case(
                (
                    "2019/08/26 06:19:14.011 -04:00",
                    "2019/08/26 06:19:16.012 -04:00",
                ),
                vec![first[2].clone()],
            )
        },
        // 13
        Case {
            levels: vec![pb::LogLevel::Info],
            patterns: vec![".*partern.*"],
            ..case(
                (
                    "2019/08/26 06:19:14.011 -04:00",
                    "2019/08/27 06:23:17.011 -04:00",
                ),
                vec![middle[4].clone(), last[0].clone(), last[1].clone()],
            )
        },
        // 14
        Case {
            levels: vec![pb::LogLevel::Info],
            patterns: vec![".*partern.*", ".*txn.*"],
            ..case(
                (
                    "2019/08/26 06:19:14.011 -04:00",
                    "2019/08/27 06:23:17.011 -04:00",
                ),
                vec![last[1].clone()],
            )
        },
    ];

    for (i, cas) in cases.into_iter().enumerate() {
        let req = pb::SearchLogRequest {
            start_time: ts(cas.search.0),
            end_time: ts(cas.search.1),
            levels: cas.levels.iter().map(|l| *l as i32).collect(),
            patterns: cas.patterns.iter().map(|p| (*p).to_owned()).collect(),
            target: 0,
        };
        let messages = s.search(req);
        let items: Vec<pb::LogMessage> = cas
            .expect
            .iter()
            .map(|line| parse_log_item(line).unwrap())
            .collect();
        assert_eq!(
            items.len(),
            messages.len(),
            "search log (index: {i}) failed"
        );
        assert_eq!(items, messages, "search log (index: {i}) failed");
    }
}

#[test]
fn test_parse_log_level() {
    let cases = [
        ("debug", pb::LogLevel::Debug),
        ("DEBUG", pb::LogLevel::Debug),
        ("info", pb::LogLevel::Info),
        ("INFO", pb::LogLevel::Info),
        ("warn", pb::LogLevel::Warn),
        ("WARN", pb::LogLevel::Warn),
        ("trace", pb::LogLevel::Trace),
        ("TRACE", pb::LogLevel::Trace),
        ("critical", pb::LogLevel::Critical),
        ("CRITICAL", pb::LogLevel::Critical),
        ("error", pb::LogLevel::Error),
        ("ERROR", pb::LogLevel::Error),
        ("invalid", pb::LogLevel::Unknown),
    ];
    for (s, l) in cases {
        let got = parse_log_level(s);
        assert_eq!(l, got, "parse {s}, expected: {l:?}, got: {got:?}");
    }
}

#[test]
fn test_parse_log_item() {
    let raw = r#"[2019/08/26 06:19:15.011 -04:00] [ERROR] [printer.go:41] ["Welcome to TiDB."]"#;
    let item = parse_log_item(raw).unwrap();
    assert_eq!(item.level, pb::LogLevel::Error as i32);
    assert_eq!(item.time, ts("2019/08/26 06:19:15.011 -04:00"));
    assert_eq!(item.message, r#"[printer.go:41] ["Welcome to TiDB."]"#);
}

/// The layout engine's rules, which the Go tests exercise only through
/// valid stamps: Go `time.Parse` semantics for the unified log layout.
#[test]
fn parse_time_stamp_follows_go_time_parse() {
    // 2019-08-26T10:19:15.011Z
    assert_eq!(ts("2019/08/26 06:19:15.011 -04:00"), 1_566_814_755_011);
    assert_eq!(ts("2019/08/26 18:19:15.011 +08:00"), 1_566_814_755_011);
    // A one-digit hour is accepted, extra spaces match one.
    assert_eq!(ts("2019/08/26 6:19:15.011 -04:00"), 1_566_814_755_011);
    assert_eq!(ts("2019/08/26  06:19:15.011  -04:00"), 1_566_814_755_011);
    // ',' separates the fraction too.
    assert_eq!(ts("2019/08/26 06:19:15,011 -04:00"), 1_566_814_755_011);
    // Before the epoch, Go's division truncates toward zero.
    assert_eq!(ts("1969/12/31 23:59:59.999 +00:00"), -1);
    for bad in [
        "2019/02/29 06:19:15.011 -04:00",
        "2019/13/01 06:19:15.011 -04:00",
        "2019/08/26 24:19:15.011 -04:00",
        "2019/08/26 06:19:15.01 -04:00",
        "2019/08/26 06:19:15.011 -0400",
        "2019/08/26 06:19:15.011 Z",
        "2019/08/26 06:19:15.011 -04:00 ",
        "20/08/26 06:19:13.011 -04:00",
        "2019/8/26 06:19:15.011 -04:00",
        "2019/08/26 06:19:15.é11 -04:00",
    ] {
        assert!(parse_time_stamp(bad).is_err(), "{bad}");
    }
    assert_eq!(ts("2020/02/29 00:00:00.000 +00:00"), 1_582_934_400_000);
}

#[test]
fn test_read_last_lines_huge() {
    let s = create_search_log_suite();
    // step 1. initial a log file with lastHugeLine
    let cap = 1024 + 512;
    let mut last_huge_line: Vec<u8> = Vec::with_capacity(cap);
    last_huge_line.extend_from_slice(b"[2019/08/26 06:22:20.011 -04:00] [INFO] [printer.go:41] ");
    while last_huge_line.len() < cap {
        if last_huge_line.len() == 1023 || last_huge_line.len() == cap - 1 {
            last_huge_line.push(b'\n');
        } else {
            last_huge_line.push(b'a');
        }
    }
    let huge = String::from_utf8(last_huge_line).unwrap();
    s.write_tmp_file(
        "tidb.log",
        &[
            r#"[2019/08/26 06:22:17.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            &huge,
        ],
    );

    // step 2. open it as read only mode
    let path = s.path("tidb.log");
    let mut file = std::fs::File::open(&path).unwrap();
    let size = file.metadata().unwrap().len() as i64;
    let (last_lines, _) = read_last_lines(&never, &mut file, size).unwrap();
    assert_eq!(last_lines.len(), 4);
}

#[test]
fn test_read_and_append_log_file() {
    let s = create_search_log_suite();
    // step 1. initial a log file
    let last = format!(
        "{}\n",
        r#"[2019/08/26 06:22:17.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#
    );
    s.write_tmp_file(
        "tidb.log",
        &[
            r#"[2019/08/26 06:22:13.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:15.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            r#"[2019/08/26 06:22:16.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
            &last,
        ],
    );

    // step 2. open it as read only mode
    let path = s.path("tidb.log");
    let mut file = std::fs::File::open(&path).unwrap();
    let filesize = file.metadata().unwrap().len() as i64;

    // step 3. run a thread to append some new logs to the file
    let append_path = path.clone();
    let appender = std::thread::spawn(move || {
        let mut file_append = std::fs::OpenOptions::new()
            .append(true)
            .open(&append_path)
            .unwrap();
        for i in 40..45 {
            let line = format!("[2020/07/07 06:{i}:17.011 -04:00] [INFO] appended logs\n");
            file_append.write_all(line.as_bytes()).unwrap();
            std::thread::sleep(Duration::from_millis(10));
        }
    });

    // step 4. keep calling ReadLastLines until reach the beginning
    let welcome = |t: &str| format!("[{t}] [INFO] [printer.go:41] [\"Welcome to TiDB.\"]\n");
    let expected = [
        [
            welcome("2019/08/26 06:22:14.011 -04:00"),
            welcome("2019/08/26 06:22:15.011 -04:00"),
            welcome("2019/08/26 06:22:16.011 -04:00"),
            welcome("2019/08/26 06:22:17.011 -04:00"),
        ]
        .concat(),
        welcome("2019/08/26 06:22:13.011 -04:00"),
    ];
    let mut i = 0;
    let mut end_cursor = filesize;
    loop {
        let (lines, read_bytes) = read_last_lines(&never, &mut file, end_cursor).unwrap();
        // read out the file
        if read_bytes == 0 {
            break;
        }
        assert_eq!(
            expected[i],
            lines.join("\n"),
            "expected: {}, got: {lines:?}",
            expected[i]
        );
        i += 1;
        end_cursor -= read_bytes as i64;
        std::thread::sleep(Duration::from_millis(15));
    }
    appender.join().unwrap();
}

#[test]
fn test_compress_log() {
    let s = create_search_log_suite();
    let w = |t: &str| format!("[{t}] [INFO] [printer.go:41] [\"Welcome to TiDB.\"]");
    let seconds = |range: std::ops::RangeInclusive<u32>| -> Vec<String> {
        range
            .map(|sec| w(&format!("2019/08/26 06:22:{sec:02}.011 -04:00")))
            .collect()
    };
    fn refs(lines: &[String]) -> Vec<&str> {
        lines.iter().map(String::as_str).collect()
    }
    s.write_tmp_gzip_file("rpc.tidb-2.log.gz", &refs(&seconds(8..=12)));
    s.write_tmp_gzip_file("rpc.tidb-1.log.gz", &refs(&seconds(13..=17)));
    s.write_tmp_file("rpc.tidb.log", &refs(&seconds(18..=22)));

    let cases = [
        (
            (
                "2019/08/26 06:22:14.000 -04:00",
                "2019/08/26 06:22:16.000 -04:00",
            ),
            1,
            14..=15,
        ),
        (
            (
                "2019/08/26 06:22:10.000 -04:00",
                "2019/08/26 06:22:16.000 -04:00",
            ),
            2,
            10..=15,
        ),
        (
            (
                "2019/08/26 06:22:14.000 -04:00",
                "2019/08/26 06:22:20.000 -04:00",
            ),
            2,
            14..=19,
        ),
        (
            (
                "2019/08/26 06:22:10.000 -04:00",
                "2019/08/26 06:22:20.000 -04:00",
            ),
            3,
            10..=19,
        ),
        // When file1.endtime < search.start < file2.begintime,
        // it will scan one more file, because we don't now the last item for a compressed file.
        (
            (
                "2019/08/26 06:22:13.000 -04:00",
                "2019/08/26 06:22:20.000 -04:00",
            ),
            3,
            13..=19,
        ),
    ];
    let log_file = s.path("rpc.tidb.log").to_string_lossy().into_owned();
    for (i, ((start, end), expect_file_num, expect)) in cases.into_iter().enumerate() {
        let (begin_time, end_time) = (ts(start), ts(end));
        let logfile = resolve_files(&never, &log_file, begin_time, end_time).unwrap();
        assert_eq!(logfile.len(), expect_file_num, "case {i}");

        let messages = s.search(pb::SearchLogRequest {
            start_time: begin_time,
            end_time,
            levels: vec![pb::LogLevel::Info as i32],
            patterns: vec![".*TiDB.*".to_owned()],
            target: 0,
        });
        let items: Vec<pb::LogMessage> = seconds(expect)
            .iter()
            .map(|line| parse_log_item(line).unwrap())
            .collect();
        assert_eq!(
            items.len(),
            messages.len(),
            "search log (index: {i}) failed"
        );
        assert_eq!(items, messages, "search log (index: {i}) failed");
    }
}

/// Go's search over a configured location that is not set answers the
/// configuration error; a missing directory answers the open error.
#[test]
fn search_log_errors_reach_the_client_as_unknown() {
    let tmp_dir = tempfile::tempdir().unwrap();
    for (log_file, want) in [
        (
            String::new(),
            "empty log file location configuration".to_owned(),
        ),
        (
            "/nonexistent-sysutil-dir/tidb.log".to_owned(),
            "open /nonexistent-sysutil-dir: no such file or directory".to_owned(),
        ),
    ] {
        let s = serve(
            tempfile::tempdir().unwrap(),
            DiagnosticsServer::new(log_file),
        );
        let address = format!("http://{}", s.address);
        let status = s.runtime.block_on(async move {
            let mut client = pb::diagnostics_client::DiagnosticsClient::connect(address)
                .await
                .unwrap();
            let mut stream = client
                .search_log(pb::SearchLogRequest::default())
                .await
                .unwrap()
                .into_inner();
            stream.message().await.unwrap_err()
        });
        assert_eq!(status.code(), tonic::Code::Unknown);
        assert_eq!(status.message(), want);
    }
    drop(tmp_dir);
}

fn bench_suite(lines: &[&str]) -> (SearchLogSuite, std::fs::File, i64) {
    let s = create_search_log_suite();
    s.write_tmp_file("tidb.log", lines);
    let file = std::fs::File::open(Path::new(&s.path("tidb.log"))).unwrap();
    let size = file.metadata().unwrap().len() as i64;
    (s, file, size)
}

/// Go `BenchmarkReadLastLines`.
#[bench]
fn benchmark_read_last_lines(b: &mut test::Bencher) {
    let (_s, mut file, filesize) = bench_suite(&[
        r#"[2019/08/26 06:22:13.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
        r#"[2019/08/26 06:22:14.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
        r#"[2019/08/26 06:22:15.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
        r#"[2019/08/26 06:22:16.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
        r#"[2019/08/26 06:22:17.011 -04:00] [INFO] [printer.go:41] ["Welcome to TiDB."]"#,
    ]);
    b.iter(|| read_last_lines(&never, &mut file, filesize).map(|(lines, _)| lines.len()));
}

/// Go `BenchmarkReadLastLinesOfHugeLine`.
#[bench]
fn benchmark_read_last_lines_of_huge_line(b: &mut test::Bencher) {
    let huge: String = (0..1024 * 1024 * 10)
        .map(|i| char::from(b'a' + (i % 26) as u8))
        .collect();
    let (_s, mut file, filesize) = bench_suite(&[&huge]);
    b.iter(|| read_last_lines(&never, &mut file, filesize).map(|(lines, _)| lines.len()));
}
