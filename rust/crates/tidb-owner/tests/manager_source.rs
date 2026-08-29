// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_owner::{
    acquire_distributed_lock, get_owner_op_value, watch_owner_for_test, Context, Listener,
    ListenersWrapper, Manager, OpType, OwnerManager, OwnerStore, OwnerWatch,
    WAIT_TIME_ON_FORCE_OWNER_MILLIS,
};
use tidb_pd_client::EtcdKeyValue;

#[derive(Default)]
struct StoreState {
    revision: i64,
    next_lease: i64,
    leases: HashSet<i64>,
    entries: HashMap<Vec<u8>, EtcdKeyValue>,
}

#[derive(Clone, Default)]
struct FakeStore {
    state: Arc<Mutex<StoreState>>,
    fail_grant: Arc<AtomicBool>,
}

struct FakeWatch {
    store: FakeStore,
    key: Vec<u8>,
    start_revision: i64,
}

impl OwnerWatch for FakeWatch {
    fn wait_deleted(&mut self, timeout: Duration) -> Result<bool, String> {
        let changed = self
            .store
            .get_prefix_metadata(&self.key)?
            .into_iter()
            .find(|entry| entry.key == self.key)
            .is_none_or(|entry| entry.create_revision >= self.start_revision);
        if !changed {
            std::thread::sleep(timeout);
        }
        Ok(changed)
    }
}

impl FakeStore {
    fn revoke(&self, lease: i64) {
        let _ = self.lease_revoke(lease);
    }

    fn owner_lease(&self, prefix: &[u8]) -> Option<i64> {
        self.get_prefix_metadata(prefix)
            .unwrap()
            .first()
            .map(|entry| entry.lease)
    }
}

impl OwnerStore for FakeStore {
    fn lease_grant(&self, _ttl: i64) -> Result<i64, String> {
        if self.fail_grant.load(Ordering::Acquire) {
            return Err("new session failed".to_owned());
        }
        let mut state = self.state.lock().unwrap();
        state.next_lease += 1;
        let lease = state.next_lease;
        state.leases.insert(lease);
        Ok(lease)
    }

    fn lease_keep_alive_once(&self, lease: i64) -> Result<(), String> {
        let state = self.state.lock().unwrap();
        state
            .leases
            .contains(&lease)
            .then_some(())
            .ok_or_else(|| "lease not found".to_owned())
    }

    fn lease_revoke(&self, lease: i64) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.leases.remove(&lease);
        state.entries.retain(|_, entry| entry.lease != lease);
        state.revision += 1;
        Ok(())
    }

    fn create_with_lease(&self, key: &[u8], value: &[u8], lease: i64) -> Result<bool, String> {
        let mut state = self.state.lock().unwrap();
        if !state.leases.contains(&lease) {
            return Err("lease not found".to_owned());
        }
        if state.entries.contains_key(key) {
            return Ok(false);
        }
        state.revision += 1;
        let revision = state.revision;
        state.entries.insert(
            key.to_vec(),
            EtcdKeyValue {
                key: key.to_vec(),
                value: value.to_vec(),
                create_revision: revision,
                mod_revision: revision,
                lease,
            },
        );
        Ok(true)
    }

    fn get_prefix_metadata(&self, prefix: &[u8]) -> Result<Vec<EtcdKeyValue>, String> {
        let state = self.state.lock().unwrap();
        let mut entries = state
            .entries
            .values()
            .filter(|entry| entry.key.starts_with(prefix))
            .cloned()
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.create_revision);
        Ok(entries)
    }

    fn delete(&self, key: &[u8]) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        state.entries.remove(key);
        state.revision += 1;
        Ok(())
    }

    fn compare_and_put_with_lease(
        &self,
        key: &[u8],
        expected_mod_revision: i64,
        value: &[u8],
        lease: i64,
    ) -> Result<bool, String> {
        let mut state = self.state.lock().unwrap();
        if state
            .entries
            .get(key)
            .is_none_or(|entry| entry.mod_revision != expected_mod_revision)
        {
            return Ok(false);
        }
        state.revision += 1;
        let revision = state.revision;
        let entry = state.entries.get_mut(key).unwrap();
        entry.value = value.to_vec();
        entry.mod_revision = revision;
        entry.lease = lease;
        Ok(true)
    }

    fn delete_keys_and_put_with_lease(
        &self,
        delete_keys: Vec<Vec<u8>>,
        key: &[u8],
        value: &[u8],
        lease: i64,
    ) -> Result<(), String> {
        let mut state = self.state.lock().unwrap();
        for key in delete_keys {
            state.entries.remove(&key);
        }
        state.revision += 1;
        let revision = state.revision;
        state.entries.insert(
            key.to_vec(),
            EtcdKeyValue {
                key: key.to_vec(),
                value: value.to_vec(),
                create_revision: revision,
                mod_revision: revision,
                lease,
            },
        );
        Ok(())
    }

    fn watch(&self, key: &[u8], start_revision: i64) -> Result<Box<dyn OwnerWatch>, String> {
        Ok(Box::new(FakeWatch {
            store: self.clone(),
            key: key.to_vec(),
            start_revision,
        }))
    }
}

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !predicate() {
        assert!(Instant::now() < deadline, "condition did not become true");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn manager(store: Arc<FakeStore>, id: &str, key: &str) -> OwnerManager {
    OwnerManager::new(Context::background(), store, "owner-test", id, key)
}

#[test]
fn test_force_to_be_owner() {
    WAIT_TIME_ON_FORCE_OWNER_MILLIS.store(0, Ordering::Release);
    let store = Arc::new(FakeStore::default());
    let first = manager(Arc::clone(&store), "first", "/owner");
    first.campaign_owner(&[2]).unwrap();
    wait_until(|| first.is_owner());
    let forced = manager(Arc::clone(&store), "forced", "/owner");
    forced.force_to_be_owner(&Context::background()).unwrap();
    forced.campaign_owner(&[2]).unwrap();
    wait_until(|| forced.is_owner() && !first.is_owner());
    first.close();
    forced.close();
}

#[test]
fn test_single() {
    let store = Arc::new(FakeStore::default());
    let owner = manager(Arc::clone(&store), "one", "/owner-single");
    owner.campaign_owner(&[1]).unwrap();
    wait_until(|| owner.is_owner());
    let lease = store.owner_lease(b"/owner-single").unwrap();
    store.revoke(lease);
    wait_until(|| {
        owner.is_owner()
            && store
                .owner_lease(b"/owner-single")
                .is_some_and(|new| new != lease)
    });
    owner.close();
}

#[test]
fn test_set_and_get_owner_op_value() {
    let store = Arc::new(FakeStore::default());
    let owner = manager(Arc::clone(&store), "one", "/owner-op");
    owner.campaign_owner(&[2]).unwrap();
    wait_until(|| owner.is_owner());
    owner
        .set_owner_op_value(&Context::background(), OpType::SYNC_UPGRADING_STATE)
        .unwrap();
    assert_eq!(
        get_owner_op_value(&Context::background(), Some(&*store), "/owner-op").unwrap(),
        OpType::SYNC_UPGRADING_STATE
    );
    assert_eq!(owner.get_owner_id(&Context::background()).unwrap(), "one");
    owner.close();
}

#[test]
fn test_get_owner_op_value_before_set() {
    let store = Arc::new(FakeStore::default());
    let owner = manager(Arc::clone(&store), "one", "/owner-none");
    owner.campaign_owner(&[2]).unwrap();
    wait_until(|| owner.is_owner());
    assert_eq!(
        get_owner_op_value(&Context::background(), Some(&*store), "/owner-none").unwrap(),
        OpType::NONE
    );
    owner.close();
}

#[test]
fn test_cluster() {
    let store = Arc::new(FakeStore::default());
    let first = manager(Arc::clone(&store), "first", "/owner-cluster");
    let second = manager(Arc::clone(&store), "second", "/owner-cluster");
    first.campaign_owner(&[2]).unwrap();
    second.campaign_owner(&[2]).unwrap();
    wait_until(|| first.is_owner() ^ second.is_owner());
    let first_was_owner = first.is_owner();
    if first_was_owner {
        first.close();
        wait_until(|| second.is_owner());
    } else {
        second.close();
        wait_until(|| first.is_owner());
    }
    first.close();
    second.close();
}

#[test]
fn test_watch_owner() {
    let store = Arc::new(FakeStore::default());
    let owner = manager(Arc::clone(&store), "one", "/owner-watch");
    owner.campaign_owner(&[2]).unwrap();
    wait_until(|| owner.is_owner());
    let watched = store.get_prefix_metadata(b"/owner-watch").unwrap()[0].clone();
    let key = watched.key.clone();
    let revision = watched.create_revision;
    let watching_store = Arc::clone(&store);
    let watching_key = String::from_utf8(key.clone()).unwrap();
    let watcher = std::thread::spawn(move || {
        watch_owner_for_test(
            &Context::background(),
            watching_store.as_ref(),
            &watching_key,
            revision,
        )
        .unwrap();
    });
    store.delete(&key).unwrap();
    watcher.join().unwrap();
    owner.close();
}

#[test]
fn test_watch_owner_after_delete_owner_key() {
    let store = Arc::new(FakeStore::default());
    let owner = manager(Arc::clone(&store), "one", "/owner-watch-deleted");
    owner.campaign_owner(&[2]).unwrap();
    wait_until(|| owner.is_owner());
    let watched = store.get_prefix_metadata(b"/owner-watch-deleted").unwrap()[0].clone();
    let key = watched.key.clone();
    let revision = watched.create_revision;
    store.delete(&key).unwrap();
    watch_owner_for_test(
        &Context::background(),
        store.as_ref(),
        &String::from_utf8(key).unwrap(),
        revision,
    )
    .unwrap();
    owner.close();
}

#[test]
fn test_immediately_cancel() {
    let store = Arc::new(FakeStore::default());
    let owner = manager(store, "one", "/owner-cancel");
    owner.campaign_owner(&[2]).unwrap();
    owner.campaign_cancel();
    assert!(!owner.is_owner());
}

#[test]
fn test_acquire_distributed_lock() {
    let store = Arc::new(FakeStore::default());
    let first = acquire_distributed_lock(
        &Context::background(),
        Arc::clone(&store) as Arc<dyn OwnerStore>,
        "/lock",
        2,
    )
    .unwrap();
    let acquired = Arc::new(AtomicBool::new(false));
    let acquired_in_thread = Arc::clone(&acquired);
    let thread_store = Arc::clone(&store);
    let waiter = std::thread::spawn(move || {
        let second =
            acquire_distributed_lock(&Context::background(), thread_store, "/lock", 2).unwrap();
        acquired_in_thread.store(true, Ordering::Release);
        second();
    });
    std::thread::sleep(Duration::from_millis(100));
    assert!(!acquired.load(Ordering::Acquire));
    first();
    wait_until(|| acquired.load(Ordering::Acquire));
    waiter.join().unwrap();
}

#[derive(Default)]
struct CountingListener {
    became: AtomicUsize,
    retired: AtomicUsize,
}

impl Listener for CountingListener {
    fn on_become_owner(&self) {
        self.became.fetch_add(1, Ordering::AcqRel);
    }

    fn on_retire_owner(&self) {
        self.retired.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn test_listeners_wrapper() {
    let first = Arc::new(CountingListener::default());
    let second = Arc::new(CountingListener::default());
    let wrapper = ListenersWrapper::new(vec![first.clone(), second.clone()]);
    wrapper.on_become_owner();
    wrapper.on_retire_owner();
    assert_eq!(first.became.load(Ordering::Acquire), 1);
    assert_eq!(first.retired.load(Ordering::Acquire), 1);
    assert_eq!(second.became.load(Ordering::Acquire), 1);
    assert_eq!(second.retired.load(Ordering::Acquire), 1);
}

#[test]
fn test_fail_new_session() {
    let store = Arc::new(FakeStore::default());
    store.fail_grant.store(true, Ordering::Release);
    let owner = manager(store, "one", "/owner-fail");
    assert_eq!(
        owner.campaign_owner(&[1]).unwrap_err(),
        "new session failed"
    );
}
