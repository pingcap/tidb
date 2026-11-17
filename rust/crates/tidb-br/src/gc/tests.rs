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

use super::*;
use crate::test_util::{go_records, MockPdClient};

/// Go `CheckGCSafePoint` and `StartServiceSafePointKeeper`'s argument
/// check, against Go's error texts.
#[test]
fn check_gc_safe_point_matches_go() {
    let ctx = Context::background();
    let pd = Arc::new(MockPdClient::default());
    *pd.gc_safe_point.lock().unwrap() = 100;
    let mgr = new_manager(pd.clone(), NULLSPACE_ID);
    for r in go_records("gc_check") {
        let got = check_gc_safe_point(&ctx, mgr.as_ref(), r["ts"].as_u64().unwrap()).err().map(|e| e.to_string());
        assert_eq!(got.unwrap_or_default(), r["err"].as_str().unwrap(), "{r}");
    }
    let keeper = go_records("gc_keeper");
    for (sp, r) in [
        BrServiceSafePoint { id: String::new(), ttl: 10, backup_ts: 5 },
        BrServiceSafePoint { id: "br-x".into(), ttl: 0, backup_ts: 5 },
    ]
    .into_iter()
    .zip(keeper)
    {
        let err = start_service_safe_point_keeper(&ctx, sp, Arc::clone(&mgr)).err().unwrap();
        assert_eq!(err.to_string(), r["err"].as_str().unwrap());
    }
}

/// Go `globalManager`: a safe point is one behind the backup TS, deleting
/// it sets a zero TTL; `MakeSafePointID` is `br-<uuid>`.
#[test]
fn global_manager_sets_and_deletes() {
    let ctx = Context::background();
    let pd = Arc::new(MockPdClient::default());
    let mgr = new_manager(pd.clone(), NULLSPACE_ID);
    let id = make_safe_point_id();
    assert!(id.starts_with("br-") && id.len() == 39, "{id}");
    let sp = BrServiceSafePoint { id: id.clone(), ttl: 300, backup_ts: 1000 };
    mgr.set_service_safe_point(&ctx, &sp).unwrap();
    mgr.delete_service_safe_point(&ctx, &sp).unwrap();
    assert_eq!(*pd.service_calls.lock().unwrap(), [(id.clone(), 300, 999), (id, 0, 0)]);
}

/// Go `keyspaceManager`: a GC barrier one behind the backup TS; a zero TTL
/// deletes it.
#[test]
fn keyspace_manager_uses_barriers() {
    let ctx = Context::background();
    let pd = Arc::new(MockPdClient::default());
    let mgr = new_manager(pd.clone(), 1);
    mgr.set_service_safe_point(&ctx, &BrServiceSafePoint { id: "br-a".into(), ttl: 300, backup_ts: 1000 }).unwrap();
    mgr.set_service_safe_point(&ctx, &BrServiceSafePoint { id: "br-a".into(), ttl: 0, backup_ts: 1000 }).unwrap();
    assert_eq!(
        *pd.service_calls.lock().unwrap(),
        [("barrier br-a".to_owned(), 300, 999), ("delete br-a".to_owned(), 0, 0)]
    );
}

/// Go `StartServiceSafePointKeeper`: sets the safe point at once and keeps
/// it until the context ends.
#[test]
fn keeper_sets_then_exits() {
    let pd = Arc::new(MockPdClient::default());
    let mgr = new_manager(pd.clone(), NULLSPACE_ID);
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let h = start_service_safe_point_keeper(&ctx, BrServiceSafePoint { id: "br-k".into(), ttl: 3, backup_ts: 10 }, mgr)
        .unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    cancel.cancel();
    assert!(h.join().unwrap().is_none());
    let calls = pd.service_calls.lock().unwrap().clone();
    assert!(calls.len() >= 2 && calls.iter().all(|c| c == &("br-k".to_owned(), 3, 9)), "{calls:?}");
}
