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

//! Go `pkg/infoschema/perfschema/tables_test.go` and
//! `pkg/util/profile/profile_test.go`, over [`get_rows`].

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::{Read as _, Write as _};
use std::sync::{Arc, Mutex};

use super::*;

/// Go `testkit.MockSessionManager` with its `ConAttrs`.
#[derive(Default)]
struct MockSessionManager {
    con_attrs: HashMap<u64, BTreeMap<String, String>>,
}

impl SessionManager for MockSessionManager {
    fn show_process_list(&self) -> Vec<Arc<tidb_sessmgr::processinfo::ProcessInfo>> {
        Vec::new()
    }
    fn get_process_info(&self, _: u64) -> Option<Arc<tidb_sessmgr::processinfo::ProcessInfo>> {
        None
    }
    fn kill(&self, _: u64, _: bool, _: bool, _: bool) {}
    fn get_con_attrs(&self, _user: Option<(&str, &str)>) -> HashMap<u64, BTreeMap<String, String>> {
        self.con_attrs.clone()
    }
}

#[derive(Default)]
struct MockContext {
    session_manager: Option<Arc<dyn SessionManager>>,
    warnings: Mutex<Vec<String>>,
}

impl PerfSchemaContext for MockContext {
    fn connection_id(&self) -> u64 {
        1
    }
    fn user(&self) -> Option<(String, String)> {
        Some(("root".to_owned(), "127.0.0.1".to_owned()))
    }
    fn login_string(&self) -> Option<String> {
        Some("root@127.0.0.1".to_owned())
    }
    fn client_ip(&self) -> Option<String> {
        Some("127.0.0.1".to_owned())
    }
    fn session_manager(&self) -> Option<Arc<dyn SessionManager>> {
        self.session_manager.clone()
    }
    fn get_data_from_session_variables(&self) -> Result<Vec<Vec<Datum>>, String> {
        Ok(Vec::new())
    }
    fn get_store_server_info(&self) -> Result<Vec<ClusterServer>, String> {
        Err("pd unavailable".to_owned())
    }
    fn get_pd_server_info(&self) -> Result<Vec<ClusterServer>, String> {
        Ok(Vec::new())
    }
    fn append_warning(&self, message: String) {
        self.warnings.lock().unwrap().push(message);
    }
}

fn text(datum: &Datum) -> String {
    match datum {
        Datum::Int(value) => value.to_string(),
        Datum::UInt(value) => value.to_string(),
        other => String::from_utf8(other.go_bytes().to_vec()).expect("utf-8"),
    }
}

fn testdata(name: &str) -> Vec<u8> {
    std::fs::read(format!("{}/testdata/{name}", env!("CARGO_MANIFEST_DIR"))).expect(name)
}

/// The mock-topology failpoint is process-wide; Go's tests in the package
/// run one at a time.
static FAILPOINT_LOCK: Mutex<()> = Mutex::new(());

/// Go's `httptest.NewServer` over a router: each request path (without the
/// query) answers its handler's bytes, and the paths that answered are
/// recorded.
struct MockServer {
    addr: String,
    accessed: Arc<Mutex<BTreeSet<String>>>,
}

impl MockServer {
    fn start(routes: HashMap<&'static str, Vec<u8>>) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr").to_string();
        let accessed = Arc::new(Mutex::new(BTreeSet::new()));
        let seen = Arc::clone(&accessed);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    match stream.read(&mut buf) {
                        Ok(0) | Err(_) => break,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split(' ').nth(1).unwrap_or("").split('?').next().unwrap_or("").to_owned();
                let (status, body) = match routes.get(path.as_str()) {
                    Some(body) => {
                        seen.lock().unwrap().insert(path);
                        ("200 OK", body.clone())
                    }
                    None => ("404 Not Found", Vec::new()),
                };
                let head = format!("HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(&body);
            }
        });
        Self { addr, accessed }
    }
}

/// Go `TestTiKVProfileCPU`.
#[test]
fn test_tikv_profile_cpu() {
    // We can use a Go profile to mock the profiles of PD because PD has the
    // same way of retrieving profile with TiDB; the purpose is to make sure
    // all profile HTTP APIs have been accessed.
    let goroutines = b"goroutine 1 [running]:\nmain.main()\n\t/x/main.go:10 +0x1d".to_vec();
    let server = MockServer::start(HashMap::from([
        ("/debug/pprof/profile", testdata("tikv.cpu.profile")),
        ("/pd/api/v1/debug/pprof/profile", testdata("test.pprof")),
        ("/pd/api/v1/debug/pprof/heap", testdata("test.pprof")),
        ("/pd/api/v1/debug/pprof/mutex", testdata("test.pprof")),
        ("/pd/api/v1/debug/pprof/allocs", testdata("test.pprof")),
        ("/pd/api/v1/debug/pprof/block", testdata("test.pprof")),
        ("/pd/api/v1/debug/pprof/goroutine", goroutines),
    ]));
    let _serial = FAILPOINT_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let mock_addr = server.addr.clone();
    let fp_expr = [format!("tikv,{mock_addr},{mock_addr}"), format!("pd,{mock_addr},{mock_addr}")].join(";");
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/infoschema/perfschema/mockRemoteNodeStatusAddress",
        &format!("return(\"{fp_expr}\")"),
    );

    let sctx = MockContext::default();
    let rows = get_rows(TABLE_NAME_TIKV_PROFILE_CPU, &sctx).expect("tikv_profile_cpu");
    assert!(sctx.warnings.lock().unwrap().is_empty(), "{:?}", sctx.warnings.lock().unwrap());
    // `select function, percent_abs, percent_rel from tikv_profile_cpu where depth < 3`
    let result: Vec<String> = rows
        .iter()
        .filter(|row| matches!(row[5], Datum::Int(depth) if depth < 3))
        .map(|row| format!("{} {} {}", text(&row[1]), text(&row[2]), text(&row[3])))
        .collect();
    assert!(rows.iter().all(|row| text(&row[0]) == mock_addr));
    assert_eq!(
        result,
        [
            "root 100% 100%",
            "├─tikv::server::load_statistics::linux::ThreadLoadStatistics::record::h59facb8d680e7794 75.00% 75.00%",
            "│ └─procinfo::pid::stat::stat_task::h69e1aa2c331aebb6 75.00% 100%",
            "├─nom::nom::digit::h905aaaeff7d8ec8e 16.07% 16.07%",
            "│ ├─<core::iter::adapters::Enumerate<I> as core::iter::traits::iterator::Iterator>::next::h16936f9061bb75e4 6.25% 38.89%",
            "│ ├─Unknown 3.57% 22.22%",
            "│ ├─<&u8 as nom::traits::AsChar>::is_dec_digit::he9eacc3fad26ab81 2.68% 16.67%",
            "│ ├─<&[u8] as nom::traits::InputIter>::iter_indices::h6192338433683bff 1.79% 11.11%",
            "│ └─<&[T] as nom::traits::Slice<core::ops::range::RangeFrom<usize>>>::slice::h38d31f11f84aa302 1.79% 11.11%",
            "├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::realloc::h5199c50710ab6f9d 1.79% 1.79%",
            "│ └─rallocx 1.79% 100%",
            "├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::dealloc::hea83459aa98dd2dc 1.79% 1.79%",
            "│ └─sdallocx 1.79% 100%",
            "├─<jemallocator::Jemalloc as core::alloc::GlobalAlloc>::alloc::hc7962e02169a5c56 0.89% 0.89%",
            "│ └─mallocx 0.89% 100%",
            "├─engine::rocks::util::engine_metrics::flush_engine_iostall_properties::h64a7661c95aa1db7 0.89% 0.89%",
            "│ └─rocksdb::rocksdb::DB::get_map_property_cf::h9722f9040411af44 0.89% 100%",
            "├─core::ptr::real_drop_in_place::h8def0d99e7136f33 0.89% 0.89%",
            "│ └─<alloc::raw_vec::RawVec<T,A> as core::ops::drop::Drop>::drop::h9b59b303bffde02c 0.89% 100%",
            "├─tikv_util::metrics::threads_linux::ThreadInfoStatistics::record::ha8cc290b3f46af88 0.89% 0.89%",
            "│ └─procinfo::pid::stat::stat_task::h69e1aa2c331aebb6 0.89% 100%",
            "├─crossbeam_utils::backoff::Backoff::snooze::h5c121ef4ce616a3c 0.89% 0.89%",
            "│ └─core::iter::range::<impl core::iter::traits::iterator::Iterator for core::ops::range::Range<A>>::next::hdb23ceb766e7a91f 0.89% 100%",
            "└─<hashbrown::raw::bitmask::BitMaskIter as core::iter::traits::iterator::Iterator>::next::he129c78b3deb639d 0.89% 0.89%",
            "  └─Unknown 0.89% 100%",
        ]
    );

    for table in [
        TABLE_NAME_PD_PROFILE_CPU,
        TABLE_NAME_PD_PROFILE_MEMORY,
        TABLE_NAME_PD_PROFILE_MUTEX,
        TABLE_NAME_PD_PROFILE_ALLOCS,
        TABLE_NAME_PD_PROFILE_BLOCK,
        TABLE_NAME_PD_PROFILE_GOROUTINES,
    ] {
        let sctx = MockContext::default();
        let rows = get_rows(table, &sctx).expect(table);
        assert!(sctx.warnings.lock().unwrap().is_empty(), "{table}: {:?}", sctx.warnings.lock().unwrap());
        assert!(!rows.is_empty(), "{table}");
    }
    let accessed = server.accessed.lock().unwrap();
    for name in ["heap", "mutex", "allocs", "block", "goroutine"] {
        assert!(accessed.contains(&format!("/pd/api/v1/debug/pprof/{name}")), "{name}: {accessed:?}");
    }
}

/// A remote profile request that fails is a warning, and the server is left
/// out (Go `dataForRemoteProfile`'s result channel).
#[test]
fn remote_profile_failure_is_a_warning() {
    let server = MockServer::start(HashMap::new());
    let _serial = FAILPOINT_LOCK.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let fp_expr = format!("tikv,{0},{0}", server.addr);
    let _fp = tidb_failpoint::enable_for_test(
        "github.com/pingcap/tidb/pkg/infoschema/perfschema/mockRemoteNodeStatusAddress",
        &format!("return(\"{fp_expr}\")"),
    );
    let sctx = MockContext::default();
    let rows = get_rows(TABLE_NAME_TIKV_PROFILE_CPU, &sctx).expect("tikv_profile_cpu");
    assert!(rows.is_empty());
    let warnings = sctx.warnings.lock().unwrap();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].ends_with("failed: 404 Not Found"), "{warnings:?}");
}

/// Go `TestSessionConnectAttrs`.
#[test]
fn test_session_connect_attrs() {
    let sm = MockSessionManager {
        con_attrs: HashMap::from([(123_456, BTreeMap::from([("_client_name".to_owned(), "libmysql".to_owned())]))]),
    };
    let sctx = MockContext { session_manager: Some(Arc::new(sm)), ..MockContext::default() };
    let rows = get_rows(TABLE_NAME_SESSION_CONNECT_ATTRS, &sctx).expect("session_connect_attrs");
    let rows: Vec<Vec<String>> = rows.iter().map(|row| row.iter().map(text).collect()).collect();
    assert_eq!(rows, vec![vec!["123456", "_client_name", "libmysql", "0"]]);
}

/// Go `TestProfiles` (`pkg/util/profile`): every TiDB profile table reads.
#[test]
fn test_profiles() {
    let old = tidb_profile::cpu_profile_interval();
    *tidb_profile::CPU_PROFILE_INTERVAL.write().unwrap() = std::time::Duration::from_secs(2);
    tidb_cpuprofile::start_cpu_profiler().expect("cpu profiler");
    let sctx = MockContext::default();
    for table in [
        TABLE_NAME_TIDB_PROFILE_CPU,
        TABLE_NAME_TIDB_PROFILE_MEMORY,
        TABLE_NAME_TIDB_PROFILE_ALLOCS,
        TABLE_NAME_TIDB_PROFILE_MUTEX,
        TABLE_NAME_TIDB_PROFILE_BLOCK,
        TABLE_NAME_TIDB_PROFILE_GOROUTINES,
    ] {
        get_rows(table, &sctx).expect(table);
    }
    *tidb_profile::CPU_PROFILE_INTERVAL.write().unwrap() = old;
}
