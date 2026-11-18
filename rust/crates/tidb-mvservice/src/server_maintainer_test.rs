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

//! Go master `pkg/mvservice/server_maintainer_test.go`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tidb_gocontext::Context;

use crate::server_maintainer::{AvailableKey, ServerConsistentHash, ServerDiscovery, ServerInfo};
use crate::test_helpers_test::serial;

#[derive(Default)]
pub(crate) struct MockServerHelper {
    pub(crate) self_info: ServerInfo,
    pub(crate) self_err: Option<String>,
    pub(crate) all_infos: HashMap<String, ServerInfo>,
    pub(crate) all_err: Option<String>,
    pub(crate) filter_fn: Option<Box<dyn Fn(&ServerInfo) -> bool + Send + Sync>>,
    pub(crate) calls: Mutex<(i64, i64)>,
}

impl ServerDiscovery for MockServerHelper {
    fn server_filter(&self, server: &ServerInfo) -> bool {
        self.filter_fn.as_ref().is_none_or(|filter| filter(server))
    }

    fn get_server_info(&self) -> Result<ServerInfo, String> {
        self.calls.lock().unwrap().0 += 1;
        match &self.self_err {
            Some(err) => Err(err.clone()),
            None => Ok(self.self_info.clone()),
        }
    }

    fn get_all_server_info(&self, _: &Context) -> Result<HashMap<String, ServerInfo>, String> {
        self.calls.lock().unwrap().1 += 1;
        match &self.all_err {
            Some(err) => Err(err.clone()),
            None => Ok(self.all_infos.clone()),
        }
    }
}

fn info(id: &str) -> ServerInfo {
    ServerInfo { id: id.to_owned(), ..ServerInfo::default() }
}

fn mapping_hash(mapping: &[(&str, u32)]) -> impl Fn(&[u8]) -> u32 + Send + Sync + 'static {
    let mapping: HashMap<Vec<u8>, u32> = mapping.iter().map(|(k, v)| (k.as_bytes().to_vec(), *v)).collect();
    move |data: &[u8]| *mapping.get(data).unwrap_or_else(|| panic!("missing hash mapping for {data:?}"))
}

/// Go `newServerConsistentHashForTest`.
fn new_server_consistent_hash_for_test(
    mapping: &[(&str, u32)],
    helper: Arc<dyn ServerDiscovery>,
    self_id: &str,
    server_ids: &[&str],
) -> ServerConsistentHash {
    let sch = ServerConsistentHash::new(Context::background(), 1, helper);
    let mut state = sch.mu.write().unwrap();
    state.chash.set_hash_func(mapping_hash(mapping));
    state.id = self_id.to_owned();
    ServerConsistentHash::rebuild_locked(&mut state, server_ids.iter().map(|id| ((*id).to_owned(), info(id))).collect());
    drop(state);
    sch
}

fn owner(sch: &ServerConsistentHash, key: &[u8]) -> String {
    sch.mu.read().unwrap().chash.get_node(key).to_owned()
}

fn node_count(sch: &ServerConsistentHash) -> usize {
    sch.mu.read().unwrap().chash.node_count()
}

#[test]
fn server_consistent_hash_rebuild_maintains_sorted_server_ids_and_ring() {
    let mapping = [("nodeA#0", 10), ("nodeB#0", 20), ("key-low", 5), ("key-mid", 15)];
    let sch = new_server_consistent_hash_for_test(&mapping, Arc::new(MockServerHelper::default()), "nodeB", &["nodeA", "nodeB"]);
    {
        let state = sch.mu.read().unwrap();
        assert_eq!(state.servers.len(), 2);
        assert_eq!(state.server_ids, ["nodeA", "nodeB"]);
    }
    assert_eq!(node_count(&sch), 2);
    assert_eq!(owner(&sch, b"key-low"), "nodeA");
    assert_eq!(owner(&sch, b"key-mid"), "nodeB");

    ServerConsistentHash::rebuild_locked(&mut sch.mu.write().unwrap(), HashMap::from([("nodeA".to_owned(), info("nodeA"))]));
    {
        let state = sch.mu.read().unwrap();
        assert_eq!(state.servers.len(), 1);
        assert_eq!(state.server_ids, ["nodeA"]);
    }
    assert_eq!(node_count(&sch), 1);
    assert_eq!(owner(&sch, b"key-mid"), "nodeA");
}

#[test]
fn server_consistent_hash_available_supports_different_key_types() {
    let key: i64 = 123_456_789;
    let key_bytes = key.to_be_bytes();
    let cases: Vec<(&str, AvailableKey<'_>, Box<dyn Fn(&[u8]) -> u32 + Send + Sync>)> = vec![
        (
            "int64",
            AvailableKey::Int64(key),
            Box::new(move |data: &[u8]| match data {
                b"nodeA#0" => 10,
                b"nodeB#0" => 20,
                b"123456789" => panic!("int64 key should use binary bytes instead of decimal text"),
                data if data == key_bytes => 15,
                data => panic!("unexpected hash input: {data:?}"),
            }),
        ),
        (
            "bytes",
            AvailableKey::Bytes(b"key-mid"),
            Box::new(|data: &[u8]| match data {
                b"nodeA#0" => 10,
                b"nodeB#0" => 20,
                b"key-mid" => 15,
                data => panic!("unexpected hash input: {data:?}"),
            }),
        ),
    ];
    for (name, key, hash) in cases {
        let sch = ServerConsistentHash::new(Context::background(), 1, Arc::new(MockServerHelper::default()));
        {
            let mut state = sch.mu.write().unwrap();
            state.chash.set_hash_func(hash);
            ServerConsistentHash::rebuild_locked(
                &mut state,
                HashMap::from([("nodeA".to_owned(), info("nodeA")), ("nodeB".to_owned(), info("nodeB"))]),
            );
        }
        sch.mu.write().unwrap().id = "nodeB".to_owned();
        assert!(sch.available(key), "{name} on nodeB");
        sch.mu.write().unwrap().id = "nodeA".to_owned();
        assert!(!sch.available(key), "{name} on nodeA");
    }
}

#[test]
fn server_consistent_hash_fetch_applies_filter() {
    let mapping = [("nodeA#0", 10), ("nodeB#0", 20), ("key-mid", 15)];
    let helper = MockServerHelper {
        all_infos: HashMap::from([("nodeA".to_owned(), info("nodeA")), ("nodeB".to_owned(), info("nodeB"))]),
        filter_fn: Some(Box::new(|s: &ServerInfo| s.id == "nodeA")),
        ..MockServerHelper::default()
    };
    let sch = new_server_consistent_hash_for_test(&mapping, Arc::new(helper), "", &[]);
    sch.refresh().unwrap();
    {
        let state = sch.mu.read().unwrap();
        assert_eq!(state.servers.len(), 1);
        assert!(state.servers.contains_key("nodeA"));
    }
    assert_eq!(node_count(&sch), 1);
    assert_eq!(owner(&sch, b"key-mid"), "nodeA");
}

#[test]
fn server_consistent_hash_fetch_no_change() {
    let mapping = [("nodeA#0", 10), ("key-mid", 15)];
    let helper = Arc::new(MockServerHelper {
        all_infos: HashMap::from([("nodeA".to_owned(), info("nodeA"))]),
        ..MockServerHelper::default()
    });
    let sch = new_server_consistent_hash_for_test(&mapping, helper.clone(), "", &[]);
    assert!(sch.refresh().unwrap(), "expected changed=true in first refresh");
    let before = (node_count(&sch), sch.mu.read().unwrap().chash.ring.len());
    assert!(!sch.refresh().unwrap(), "expected changed=false in second refresh");
    assert_eq!((node_count(&sch), sch.mu.read().unwrap().chash.ring.len()), before);
    assert_eq!(helper.calls.lock().unwrap().1, 2);
    assert_eq!(owner(&sch, b"key-mid"), "nodeA");
}

#[test]
fn server_consistent_hash_init() {
    let _serial = serial();
    let mapping = [("nodeA#0", 10), ("key-mid", 15)];
    let helper = Arc::new(MockServerHelper {
        self_info: info("nodeA"),
        all_infos: HashMap::from([("nodeA".to_owned(), info("nodeA"))]),
        ..MockServerHelper::default()
    });
    let sch = new_server_consistent_hash_for_test(&mapping, helper.clone(), "", &[]);
    assert!(sch.init());
    assert_eq!(sch.id(), "nodeA");
    assert_eq!(node_count(&sch), 0);
    let calls = *helper.calls.lock().unwrap();
    assert!(calls.0 >= 1);
    assert_eq!(calls.1, 0, "getAllServerInfo not called in init");
}

#[test]
fn server_consistent_hash_init_canceled() {
    let _serial = serial();
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    cancel.cancel();
    let helper = Arc::new(MockServerHelper {
        self_err: Some("boom".to_owned()),
        all_err: Some("boom".to_owned()),
        ..MockServerHelper::default()
    });
    let sch = ServerConsistentHash::new(ctx, 1, helper.clone());
    let start = Instant::now();
    assert!(!sch.init(), "expected init failed when context canceled");
    assert!(start.elapsed() < std::time::Duration::from_secs(1));
    assert!(helper.calls.lock().unwrap().0 <= 1);
}

#[test]
fn server_consistent_hash_fetch_error() {
    let helper = MockServerHelper { all_err: Some("boom".to_owned()), ..MockServerHelper::default() };
    let sch = ServerConsistentHash::new(Context::background(), 1, Arc::new(helper));
    assert!(sch.refresh().is_err());
}
