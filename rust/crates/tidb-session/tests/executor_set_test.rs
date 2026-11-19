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

//! Go `pkg/executor/set_test.go` (`TestSetClusterConfig`), on the mock store
//! through the session, as Go's testkit runs it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tidb_domain::cluster_topology::ClusterServer;

use crate::support::testkit::*;

fn server(server_type: &str, address: &str, status_address: &str) -> ClusterServer {
    ClusterServer {
        server_type: server_type.to_owned(),
        address: address.to_owned(),
        status_address: status_address.to_owned(),
        version: String::new(),
        git_hash: String::new(),
        start_timestamp: 0,
        server_id: 0,
    }
}

/// Go `TestSetClusterConfig`.
#[test]
fn test_set_cluster_config() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");

    let servers_info = vec![
        server("tidb", "127.0.0.1:1111", "127.0.0.1:1111"),
        server("tidb", "127.0.0.1:2222", "127.0.0.1:2222"),
        server("pd", "127.0.0.1:3333", "127.0.0.1:3333"),
        server("pd", "127.0.0.1:4444", "127.0.0.1:4444"),
        server("tikv", "127.0.0.1:5555", "127.0.0.1:5555"),
        server("tikv", "127.0.0.1:6666", "127.0.0.1:6666"),
        server("tiflash", "127.0.0.1:3933", "127.0.0.1:7777"),
        server("tso", "127.0.0.1:22379", "127.0.0.1:22379"),
        server("scheduling", "127.0.0.1:22479", "127.0.0.1:22479"),
    ];
    tk.session()
        .set_test_set_config_server_info(Arc::new(move || Ok(servers_info.clone())));

    let err = |tk: &mut TestKit, sql: &str| tk.exec_to_err(sql).expect("the statement fails").message;
    assert_eq!(err(&mut tk, "set config xxx log.level='info'"), "unknown type xxx");
    assert_eq!(err(&mut tk, "set config tidb log.level='info'"), "TiDB doesn't support to change configs online, please use SQL variables");
    assert_eq!(err(&mut tk, "set config tso log.level='info'"), "tso doesn't support to change configs online");
    assert_eq!(err(&mut tk, "set config scheduling log.level='info'"), "scheduling doesn't support to change configs online");
    assert_eq!(err(&mut tk, "set config '127.0.0.1:1111' log.level='info'"), "TiDB doesn't support to change configs online, please use SQL variables");
    assert_eq!(err(&mut tk, "set config '127.a.b.c:1234' log.level='info'"), "invalid instance 127.a.b.c:1234"); // name doesn't resolve.
    assert_eq!(err(&mut tk, "set config 'example.com:1111' log.level='info'"), "instance example.com:1111 is not found in this cluster"); // name resolves.
    assert_eq!(err(&mut tk, "set config tikv log.level=null"), "can't set config to null");
    assert_eq!(err(&mut tk, "set config '1.1.1.1:1111' log.level='info'"), "instance 1.1.1.1:1111 is not found in this cluster");
    assert_eq!(err(&mut tk, "set config tikv `raftstore.max-peer-down-duration`=DEFAULT"), "Unknown DEFAULT for SET CONFIG");
    assert!(err(&mut tk, "set config tiflash `server.snap-max-write-bytes-per-sec`='500MB'")
        .contains("This command can only change config items begin with 'raftstore-proxy'"));

    let http_cnt = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&http_cnt);
    tk.session().set_test_set_config_http_handler(Arc::new(move |_, _| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok((200, "200 OK".to_owned(), String::new()))
    }));
    tk.must_exec("set config tikv log.level='info'");
    assert_eq!(http_cnt.load(Ordering::SeqCst), 2);

    http_cnt.store(0, Ordering::SeqCst);
    tk.must_exec("set config '127.0.0.1:5555' log.level='info'");
    assert_eq!(http_cnt.load(Ordering::SeqCst), 1);

    http_cnt.store(0, Ordering::SeqCst);
    let counter = Arc::clone(&http_cnt);
    tk.session().set_test_set_config_http_handler(Arc::new(move |_, body| {
        counter.fetch_add(1, Ordering::SeqCst);
        // The `raftstore.` prefix is stripped.
        let got: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(got, serde_json::json!({"server.snap-max-write-bytes-per-sec": "500MB"}));
        Ok((200, "200 OK".to_owned(), String::new()))
    }));
    tk.must_exec("set config tiflash `raftstore-proxy.server.snap-max-write-bytes-per-sec`='500MB'");
    assert_eq!(http_cnt.load(Ordering::SeqCst), 1);

    http_cnt.store(0, Ordering::SeqCst);
    tk.session()
        .set_test_set_config_http_handler(Arc::new(|_, _| Err("something wrong".to_owned())));
    tk.must_exec("set config tikv log.level='info'");
    tk.must_query("show warnings").check(["Warning 1105 something wrong", "Warning 1105 something wrong"]);

    tk.session().set_test_set_config_http_handler(Arc::new(|_, _| {
        Ok((400, "400 Bad Request".to_owned(), "WRONG".to_owned()))
    }));
    tk.must_exec("set config tikv log.level='info'");
    tk.must_query("show warnings").check([
        "Warning 1105 bad request to http://127.0.0.1:5555/config: WRONG",
        "Warning 1105 bad request to http://127.0.0.1:6666/config: WRONG",
    ]);
}
