// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use async_trait::async_trait;

use crate::proto::kvrpcpb;
use crate::Result;

pub const LOCK_ALWAYS_WAIT: i64 = i64::MAX;
pub const LOCK_NO_WAIT: i64 = -1;

/// Value returned by a pessimistic-lock operation and its lock state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReturnedValue {
    pub value: Vec<u8>,
    pub exists: bool,
    pub locked_with_conflict_ts: u64,
    pub already_locked: bool,
}

/// Marker for lock statistics owned by the observability layer.
pub trait LockStatistics: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
}
impl<T: Any + Send + Sync> LockStatistics for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Deadlock payload plus source retry classification.
#[derive(Clone, Debug, PartialEq)]
pub struct DeadlockError {
    pub deadlock: kvrpcpb::Deadlock,
    pub is_retryable: bool,
}

pub type ResourceGroupTagger =
    Arc<dyn Fn(&kvrpcpb::PessimisticLockRequest) -> Vec<u8> + Send + Sync>;
pub type DeadlockHandler = Arc<dyn Fn(&DeadlockError) + Send + Sync>;

/// Options and result collection for pessimistic locking.
pub struct LockContext {
    pub killed: Option<Arc<AtomicU32>>,
    pub for_update_ts: u64,
    lock_wait_time: Option<i64>,
    pub wait_start_time: Option<SystemTime>,
    pub return_values: bool,
    pub check_existence: bool,
    pub lock_only_if_exists: bool,
    pub in_share_mode: bool,
    values: Mutex<HashMap<Vec<u8>, ReturnedValue>>,
    pub max_locked_with_conflict_ts: u64,
    pub lock_expired: Option<Arc<AtomicU32>>,
    pub stats: Option<Arc<dyn LockStatistics>>,
    pub resource_group_tag: Vec<u8>,
    pub resource_group_tagger: Option<ResourceGroupTagger>,
    pub on_deadlock: Option<DeadlockHandler>,
    pub max_execution_deadline: Option<SystemTime>,
}

impl Default for LockContext {
    fn default() -> Self {
        Self {
            killed: None,
            for_update_ts: 0,
            lock_wait_time: None,
            wait_start_time: None,
            return_values: false,
            check_existence: false,
            lock_only_if_exists: false,
            in_share_mode: false,
            values: Mutex::new(HashMap::new()),
            max_locked_with_conflict_ts: 0,
            lock_expired: None,
            stats: None,
            resource_group_tag: Vec::new(),
            resource_group_tagger: None,
            on_deadlock: None,
            max_execution_deadline: None,
        }
    }
}

impl LockContext {
    pub fn new(for_update_ts: u64, lock_wait_time: i64, wait_start_time: SystemTime) -> Self {
        Self {
            for_update_ts,
            lock_wait_time: Some(lock_wait_time),
            wait_start_time: Some(wait_start_time),
            ..Self::default()
        }
    }

    pub fn lock_wait_time(&mut self) -> i64 {
        *self.lock_wait_time.get_or_insert(LOCK_ALWAYS_WAIT)
    }

    pub fn init_return_values(&mut self, capacity: usize) {
        self.return_values = true;
        self.values.lock().unwrap().reserve(capacity);
    }

    pub fn init_check_existence(&mut self, capacity: usize) {
        self.check_existence = true;
        self.values.lock().unwrap().reserve(capacity);
    }

    pub fn insert_returned_value(&self, key: Vec<u8>, value: ReturnedValue) {
        self.values.lock().unwrap().insert(key, value);
    }

    /// Returns `(value, true)` unless the key was already locked.
    pub fn value_not_locked(&self, key: &[u8]) -> (Option<Vec<u8>>, bool) {
        match self.values.lock().unwrap().get(key) {
            Some(value) if value.already_locked => (None, false),
            Some(value) => (Some(value.value.clone()), true),
            None => (None, true),
        }
    }

    pub fn for_each_value_not_locked(&self, mut function: impl FnMut(&[u8], &[u8])) {
        let values = self.values.lock().unwrap();
        for (key, value) in values.iter() {
            if !value.already_locked {
                function(key, &value.value);
            }
        }
    }
}

/// A value and its optional commit timestamp.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ValueEntry {
    pub value: Vec<u8>,
    pub commit_ts: u64,
}

impl ValueEntry {
    pub fn new(value: Vec<u8>, commit_ts: u64) -> Self {
        Self { value, commit_ts }
    }

    pub fn is_value_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn size(&self) -> usize {
        size_of::<Self>() + self.value.len()
    }
}

/// Options common to point and batch gets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GetOption {
    ReturnCommitTs,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GetOptions {
    return_commit_ts: bool,
}

impl GetOptions {
    pub fn apply(&mut self, options: &[GetOption]) {
        self.return_commit_ts |= options.contains(&GetOption::ReturnCommitTs);
    }

    pub const fn return_commit_ts(&self) -> bool {
        self.return_commit_ts
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BatchGetOptions {
    return_commit_ts: bool,
}

impl BatchGetOptions {
    pub fn apply(&mut self, options: &[GetOption]) {
        self.return_commit_ts |= options.contains(&GetOption::ReturnCommitTs);
    }

    pub const fn return_commit_ts(&self) -> bool {
        self.return_commit_ts
    }
}

pub fn batch_get_to_get_options(options: &[GetOption]) -> Vec<GetOption> {
    options.to_vec()
}

#[async_trait]
pub trait Getter: Send + Sync {
    async fn get(&self, key: &[u8], options: &[GetOption]) -> Result<ValueEntry>;
}

#[async_trait]
pub trait BatchGetter: Send + Sync {
    async fn batch_get(
        &self,
        keys: &[Vec<u8>],
        options: &[GetOption],
    ) -> Result<HashMap<Vec<u8>, ValueEntry>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_options_and_value_entries_match_source_behavior() {
        let mut get = GetOptions::default();
        get.apply(&[]);
        assert!(!get.return_commit_ts());
        get.apply(&batch_get_to_get_options(&[GetOption::ReturnCommitTs]));
        assert!(get.return_commit_ts());

        let mut batch = BatchGetOptions::default();
        batch.apply(&[GetOption::ReturnCommitTs]);
        assert!(batch.return_commit_ts());

        assert!(ValueEntry::default().is_value_empty());
        assert!(ValueEntry::new(Vec::new(), 123).is_value_empty());
        assert!(!ValueEntry::new(vec![b'x'], 123).is_value_empty());
        assert!(!ValueEntry::new(vec![b'x'], 0).is_value_empty());
        assert_eq!(
            ValueEntry::new(vec![1, 2], 3).size(),
            size_of::<ValueEntry>() + 2
        );
    }

    #[test]
    fn lock_context_defaults_and_returned_values_match_source_behavior() {
        let mut context = LockContext::default();
        assert_eq!(context.lock_wait_time(), LOCK_ALWAYS_WAIT);
        context.init_return_values(2);
        assert!(context.return_values);
        context.insert_returned_value(
            b"open".to_vec(),
            ReturnedValue {
                value: b"value".to_vec(),
                ..ReturnedValue::default()
            },
        );
        context.insert_returned_value(
            b"locked".to_vec(),
            ReturnedValue {
                already_locked: true,
                ..ReturnedValue::default()
            },
        );
        assert_eq!(
            context.value_not_locked(b"open"),
            (Some(b"value".to_vec()), true)
        );
        assert_eq!(context.value_not_locked(b"locked"), (None, false));
        assert_eq!(context.value_not_locked(b"missing"), (None, true));

        let mut seen = Vec::new();
        context.for_each_value_not_locked(|key, value| seen.push((key.to_vec(), value.to_vec())));
        assert_eq!(seen, vec![(b"open".to_vec(), b"value".to_vec())]);
    }
}
