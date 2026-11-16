//! Go `br/pkg/utils/storewatch/watching_test.go`.

use std::cell::Cell;
use std::sync::Mutex;

use tidb_gocontext::Context;
use tidb_proto::metapb::{self, StoreState};

use super::*;
use crate::errors::Result;

/// Go `SequentialReturningStoreMeta`: each call answers the next store list.
struct SequentialReturningStoreMeta {
    sequence: Mutex<Vec<Vec<metapb::Store>>>,
}

impl SequentialReturningStoreMeta {
    fn new(mut sequence: Vec<Vec<metapb::Store>>) -> Self {
        sequence.reverse();
        Self {
            sequence: Mutex::new(sequence),
        }
    }
}

impl PdClient for SequentialReturningStoreMeta {
    fn get_cluster_id(&self, _: &Context) -> u64 {
        0
    }
    fn get_all_stores(&self, _: &Context, _: bool) -> Result<Vec<metapb::Store>> {
        self.sequence
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| errors::errorf("too many call to `GetAllStores` in test"))
    }
    fn get_ts(&self, _: &Context) -> Result<(i64, i64)> {
        unreachable!()
    }
    fn update_gc_safe_point(&self, _: &Context, _: u64) -> Result<u64> {
        unreachable!()
    }
    fn update_service_gc_safe_point(&self, _: &Context, _: &str, _: i64, _: u64) -> Result<u64> {
        unreachable!()
    }
    fn get_keyspace_gc_safe_point(&self, _: &Context, _: u32) -> Result<u64> {
        unreachable!()
    }
    fn set_gc_barrier(
        &self,
        _: &Context,
        _: u32,
        _: &str,
        _: u64,
        _: std::time::Duration,
    ) -> Result<(String, u64, std::time::Duration)> {
        unreachable!()
    }
    fn delete_gc_barrier(&self, _: &Context, _: u32, _: &str) -> Result<()> {
        unreachable!()
    }
}

fn store(id: u64, state: StoreState, start_timestamp: i64) -> metapb::Store {
    metapb::Store {
        id,
        state: state as i32,
        start_timestamp,
        ..Default::default()
    }
}

/// Go `TestOnRegister`.
#[test]
fn on_register() {
    let seq = SequentialReturningStoreMeta::new(vec![vec![store(1, StoreState::Up, 0)]]);
    let called = Cell::new(false);
    let cb = Callback {
        on_new_store_registered: Some(Box::new(|_| called.set(true))),
        ..Default::default()
    };
    let mut watcher = Watcher::new(&seq, cb);
    watcher.step(&Context::background()).unwrap();
    assert!(called.get());
}

/// Go `TestOnOffline`.
#[test]
fn on_offline() {
    let seq = SequentialReturningStoreMeta::new(vec![
        vec![store(1, StoreState::Up, 0)],
        vec![store(1, StoreState::Offline, 0)],
    ]);
    let called = Cell::new(false);
    let cb = Callback {
        on_disconnect: Some(Box::new(|_| called.set(true))),
        ..Default::default()
    };
    let mut watcher = Watcher::new(&seq, cb);
    watcher.step(&Context::background()).unwrap();
    watcher.step(&Context::background()).unwrap();
    assert!(called.get());
}

/// Go `TestOnReboot`.
#[test]
fn on_reboot() {
    let seq = SequentialReturningStoreMeta::new(vec![
        vec![store(1, StoreState::Up, 1)],
        vec![store(1, StoreState::Offline, 1)],
        vec![store(1, StoreState::Up, 2)],
    ]);
    let called = Cell::new(false);
    let cb = Callback {
        on_reboot: Some(Box::new(|_| called.set(true))),
        ..Default::default()
    };
    let mut watcher = Watcher::new(&seq, cb);
    for _ in 0..3 {
        watcher.step(&Context::background()).unwrap();
    }
    assert!(called.get());
}
