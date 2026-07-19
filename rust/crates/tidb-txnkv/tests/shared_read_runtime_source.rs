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

use std::sync::{Arc, Barrier};

use tidb_txnkv::region::{
    KeyRange, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionQuery,
    RegionQueryLoader, RegionQueryOptions, StoreMetadata,
};
use tidb_txnkv::SharedReadAuthority;

#[derive(Clone, Debug)]
struct ClientHandle(u64);

#[derive(Clone, Debug)]
struct EmptyLoader;

impl RegionLoader for EmptyLoader {
    fn cluster_id(&self) -> u64 {
        42
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new("unexpected-load", "load not expected"))
    }
}

impl RegionQueryLoader for EmptyLoader {
    fn query_region(
        &mut self,
        _query: RegionQuery<'_>,
        _options: RegionQueryOptions,
    ) -> Result<RegionLocation, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-query",
            "query not expected",
        ))
    }

    fn scan_regions_once(
        &mut self,
        _range: &KeyRange,
        _limit: usize,
        _options: RegionQueryOptions,
    ) -> Result<Vec<RegionLocation>, RegionLoadError> {
        Err(RegionLoadError::new("unexpected-scan", "scan not expected"))
    }

    fn load_store(&mut self, _store_id: u64) -> Result<Option<StoreMetadata>, RegionLoadError> {
        Err(RegionLoadError::new(
            "unexpected-store",
            "store not expected",
        ))
    }
}

#[test]
fn sessions_share_process_authority_but_not_client_cells() {
    let authority = SharedReadAuthority::start(ClientHandle(7), RegionCache::new(EmptyLoader))
        .expect("authority");
    let first = authority.open_session().expect("first session");
    let second = authority.open_session().expect("second session");

    assert_ne!(
        first.client_handle().as_ptr(),
        second.client_handle().as_ptr()
    );
    assert_eq!(first.client().borrow().0, 7);
    assert_eq!(second.client().borrow().0, 7);
    assert_eq!(first.authority_id(), authority.authority_id());
    assert_eq!(second.authority_id(), authority.authority_id());
    assert_eq!(first.cluster_id(), second.cluster_id());

    drop(first);
    assert_eq!(second.client().borrow().0, 7);
    drop(second);
    authority.shutdown().expect("unique authority shutdown");
}

#[test]
fn authority_is_send_and_sync_while_sessions_are_opened_in_workers() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<SharedReadAuthority<ClientHandle, EmptyLoader>>();

    let authority = Arc::new(
        SharedReadAuthority::start(ClientHandle(11), RegionCache::new(EmptyLoader))
            .expect("authority"),
    );
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for _ in 0..2 {
        let authority = Arc::clone(&authority);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let session = authority.open_session().expect("worker session");
            barrier.wait();
            (session.authority_id(), session.client().borrow().0)
        }));
    }
    barrier.wait();
    for worker in workers {
        assert_eq!(
            worker.join().expect("worker"),
            (authority.authority_id(), 11)
        );
    }
    Arc::try_unwrap(authority)
        .ok()
        .expect("all session workers drained")
        .shutdown()
        .expect("authority shutdown");
}
