//! Stateful TiKV-compatible MVCC and raw-KV engine used by mock protocol adapters.
//!
//! The behavior is transcreated from client-go's pinned
//! `internal/mockstore/mocktikv` package. Protocol-specific protobuf conversion
//! remains in the consuming client crate so this storage engine stays reusable.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::fs;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const SHORT_VALUE_MAX_LEN: usize = 64;
const MAX_MARSHALLED_SLICE: usize = 10 * 1024 * 1024;
type PersistedRawEntries = Vec<(Vec<u8>, Vec<u8>)>;
type PersistedRawColumnFamilies = Vec<(String, PersistedRawEntries)>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[repr(i32)]
pub enum Op {
    Put = 0,
    Delete = 1,
    Lock = 2,
    Rollback = 3,
    Insert = 4,
    PessimisticLock = 5,
    CheckNotExists = 6,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IsolationLevel {
    #[default]
    SnapshotIsolation,
    ReadCommitted,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Assertion {
    #[default]
    None,
    Exist,
    NotExist,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AssertionLevel {
    #[default]
    Off,
    Fast,
    Strict,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PessimisticAction {
    #[default]
    Skip,
    DoCheck,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PessimisticWakeUpMode {
    #[default]
    Normal,
    ForceLock,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Action {
    #[default]
    NoAction,
    TtlExpireRollback,
    LockNotExistRollback,
    MinCommitTsPushed,
    TtlExpirePessimisticRollback,
    LockNotExistDoNothing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxnMutation {
    pub op: Op,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub assertion: Assertion,
}

impl TxnMutation {
    pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            op: Op::Put,
            key: key.into(),
            value: value.into(),
            assertion: Assertion::None,
        }
    }

    pub fn delete(key: impl Into<Vec<u8>>) -> Self {
        Self {
            op: Op::Delete,
            key: key.into(),
            value: Vec::new(),
            assertion: Assertion::None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PrewriteRequest {
    pub mutations: Vec<TxnMutation>,
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub ttl: u64,
    pub txn_size: u64,
    pub for_update_ts: u64,
    pub min_commit_ts: u64,
    pub pessimistic_actions: Vec<PessimisticAction>,
    pub assertion_level: AssertionLevel,
    pub resolved_locks: Vec<u64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PessimisticLockRequest {
    pub mutations: Vec<TxnMutation>,
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub for_update_ts: u64,
    pub ttl: u64,
    pub min_commit_ts: u64,
    pub wait_timeout: i64,
    pub return_values: bool,
    pub check_existence: bool,
    pub lock_only_if_exists: bool,
    pub wake_up_mode: PessimisticWakeUpMode,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PessimisticLockKeyResultType {
    Normal,
    LockedWithConflict,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PessimisticLockKeyResult {
    pub result_type: PessimisticLockKeyResultType,
    pub value: Vec<u8>,
    pub existence: bool,
    pub locked_with_conflict_ts: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[repr(i64)]
pub enum WriteType {
    Put = 0,
    Delete = 1,
    Rollback = 2,
    Lock = 3,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WriteRecord {
    pub write_type: WriteType,
    pub start_ts: u64,
    pub commit_ts: u64,
    pub value: Vec<u8>,
}

impl WriteRecord {
    pub fn marshal_binary(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(32 + self.value.len());
        output.extend_from_slice(&(self.write_type as i64).to_le_bytes());
        output.extend_from_slice(&self.start_ts.to_le_bytes());
        output.extend_from_slice(&self.commit_ts.to_le_bytes());
        write_slice(&mut output, &self.value);
        output
    }

    pub fn unmarshal_binary(mut input: &[u8]) -> Result<Self, MockError> {
        let write_type = match read_i64(&mut input)? {
            0 => WriteType::Put,
            1 => WriteType::Delete,
            2 => WriteType::Rollback,
            3 => WriteType::Lock,
            value => return Err(MockError::Decode(format!("invalid write type {value}"))),
        };
        Ok(Self {
            write_type,
            start_ts: read_u64(&mut input)?,
            commit_ts: read_u64(&mut input)?,
            value: read_slice(&mut input)?,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LockRecord {
    pub start_ts: u64,
    pub primary: Vec<u8>,
    pub value: Vec<u8>,
    pub op: Op,
    pub ttl: u64,
    pub for_update_ts: u64,
    pub txn_size: u64,
    pub min_commit_ts: u64,
}

impl LockRecord {
    pub fn marshal_binary(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(64 + self.primary.len() + self.value.len());
        output.extend_from_slice(&self.start_ts.to_le_bytes());
        write_slice(&mut output, &self.primary);
        write_slice(&mut output, &self.value);
        output.extend_from_slice(&(self.op as i32).to_le_bytes());
        output.extend_from_slice(&self.ttl.to_le_bytes());
        output.extend_from_slice(&self.for_update_ts.to_le_bytes());
        output.extend_from_slice(&self.txn_size.to_le_bytes());
        output.extend_from_slice(&self.min_commit_ts.to_le_bytes());
        output
    }

    pub fn unmarshal_binary(mut input: &[u8]) -> Result<Self, MockError> {
        let start_ts = read_u64(&mut input)?;
        let primary = read_slice(&mut input)?;
        let value = read_slice(&mut input)?;
        let op = match read_i32(&mut input)? {
            0 => Op::Put,
            1 => Op::Delete,
            2 => Op::Lock,
            3 => Op::Rollback,
            4 => Op::Insert,
            5 => Op::PessimisticLock,
            6 => Op::CheckNotExists,
            value => return Err(MockError::Decode(format!("invalid lock op {value}"))),
        };
        Ok(Self {
            start_ts,
            primary,
            value,
            op,
            ttl: read_u64(&mut input)?,
            for_update_ts: read_u64(&mut input)?,
            txn_size: read_u64(&mut input)?,
            min_commit_ts: read_u64(&mut input)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pair {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub commit_ts: u64,
    pub error: Option<MockError>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockInfo {
    pub primary: Vec<u8>,
    pub start_ts: u64,
    pub key: Vec<u8>,
    pub ttl: u64,
    pub txn_size: u64,
    pub lock_type: Op,
    pub for_update_ts: u64,
    pub min_commit_ts: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvccWrite {
    pub write_type: Op,
    pub start_ts: u64,
    pub commit_ts: u64,
    pub short_value: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvccValue {
    pub start_ts: u64,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MvccInfo {
    pub lock: Option<LockRecord>,
    pub writes: Vec<MvccWrite>,
    pub values: Vec<MvccValue>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum MockError {
    #[error("key is locked, key: {key:?}, primary: {primary:?}, txnStartTS: {start_ts}, forUpdateTs: {for_update_ts}, LockType: {lock_type:?}")]
    Locked {
        key: Vec<u8>,
        primary: Vec<u8>,
        start_ts: u64,
        for_update_ts: u64,
        ttl: u64,
        txn_size: u64,
        lock_type: Op,
        min_commit_ts: u64,
    },
    #[error("key already exist, key: {key:?}")]
    KeyAlreadyExists { key: Vec<u8> },
    #[error("retryable: {0}")]
    Retryable(String),
    #[error("abort: {0}")]
    Abort(String),
    #[error("txn already committed")]
    AlreadyCommitted { commit_ts: u64 },
    #[error("txn={start_ts} on key={key:?} is already rolled back")]
    AlreadyRolledBack { start_ts: u64, key: Vec<u8> },
    #[error("write conflict")]
    Conflict {
        start_ts: u64,
        conflict_start_ts: u64,
        conflict_commit_ts: u64,
        key: Vec<u8>,
        can_force_lock: bool,
    },
    #[error("deadlock")]
    Deadlock {
        lock_ts: u64,
        lock_key: Vec<u8>,
        deadlock_key_hash: u64,
    },
    #[error("commit ts expired")]
    CommitTsExpired {
        start_ts: u64,
        attempted_commit_ts: u64,
        key: Vec<u8>,
        min_commit_ts: u64,
    },
    #[error("txn not found")]
    TxnNotFound { start_ts: u64, primary: Vec<u8> },
    #[error("AssertionFailed {{ StartTS: {start_ts}, Key: {key:?}, Assertion: {assertion:?}, ExistingStartTS: {existing_start_ts}, ExistingCommitTS: {existing_commit_ts} }}")]
    AssertionFailed {
        start_ts: u64,
        key: Vec<u8>,
        assertion: Assertion,
        existing_start_ts: u64,
        existing_commit_ts: u64,
    },
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Decode(String),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct Entry {
    writes: Vec<WriteRecord>,
    lock: Option<LockRecord>,
}

impl Entry {
    fn txn_write(&self, start_ts: u64) -> Option<&WriteRecord> {
        self.writes.iter().find(|write| write.start_ts == start_ts)
    }

    fn visible_write(&self, read_ts: u64) -> Option<&WriteRecord> {
        self.writes
            .iter()
            .filter(|write| {
                write.commit_ts <= read_ts
                    && matches!(write.write_type, WriteType::Put | WriteType::Delete)
            })
            .max_by_key(|write| write.commit_ts)
    }

    fn sort_writes(&mut self) {
        self.writes
            .sort_by_key(|write| std::cmp::Reverse(write.commit_ts));
    }
}

#[derive(Default)]
struct State {
    entries: BTreeMap<Vec<u8>, Entry>,
    raw_cfs: HashMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
    waits_for: HashMap<u64, (u64, u64)>,
    closed: bool,
}

#[derive(Deserialize, Serialize)]
struct PersistentState {
    entries: Vec<(Vec<u8>, Entry)>,
    raw_cfs: PersistedRawColumnFamilies,
}

impl PersistentState {
    fn from_state(state: &State) -> Self {
        Self {
            entries: state
                .entries
                .iter()
                .map(|(key, entry)| (key.clone(), entry.clone()))
                .collect(),
            raw_cfs: state
                .raw_cfs
                .iter()
                .map(|(cf, entries)| {
                    (
                        cf.clone(),
                        entries
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                    )
                })
                .collect(),
        }
    }

    fn into_state(self) -> State {
        State {
            entries: self.entries.into_iter().collect(),
            raw_cfs: self
                .raw_cfs
                .into_iter()
                .map(|(cf, entries)| (cf, entries.into_iter().collect()))
                .collect(),
            ..State::default()
        }
    }
}

#[derive(Clone, Default)]
pub struct MockEngine {
    state: Arc<RwLock<State>>,
    persistence_path: Option<Arc<PathBuf>>,
}

impl fmt::Debug for MockEngine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("MockEngine").finish_non_exhaustive()
    }
}

impl MockEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Opens a directory-backed mock store. State is restored on open and
    /// atomically snapshotted when the engine is closed.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, MockError> {
        let path = path.as_ref();
        fs::create_dir_all(path).map_err(io_error)?;
        let persistence_path = path.join("mocktikv-state.json");
        let state = if persistence_path.exists() {
            let bytes = fs::read(&persistence_path).map_err(io_error)?;
            serde_json::from_slice::<PersistentState>(&bytes)
                .map_err(|error| MockError::Decode(error.to_string()))?
                .into_state()
        } else {
            State::default()
        };
        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            persistence_path: Some(Arc::new(persistence_path)),
        })
    }

    pub fn get(
        &self,
        key: &[u8],
        read_ts: u64,
        isolation: IsolationLevel,
        resolved_locks: &[u64],
    ) -> Result<Option<(Vec<u8>, u64)>, MockError> {
        let state = self.state.read().expect("mock engine lock poisoned");
        ensure_open(&state)?;
        get_from_state(&state, key, read_ts, isolation, resolved_locks)
    }

    pub fn batch_get(
        &self,
        keys: &[Vec<u8>],
        read_ts: u64,
        isolation: IsolationLevel,
        resolved_locks: &[u64],
    ) -> Vec<Pair> {
        keys.iter()
            .filter_map(
                |key| match self.get(key, read_ts, isolation, resolved_locks) {
                    Ok(Some((value, commit_ts))) => Some(Pair {
                        key: key.clone(),
                        value,
                        commit_ts,
                        error: None,
                    }),
                    Ok(None) => None,
                    Err(error) => Some(Pair {
                        key: key.clone(),
                        value: Vec::new(),
                        commit_ts: 0,
                        error: Some(error),
                    }),
                },
            )
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn scan(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        read_ts: u64,
        isolation: IsolationLevel,
        resolved_locks: &[u64],
        reverse: bool,
    ) -> Vec<Pair> {
        if limit == 0 {
            return Vec::new();
        }
        let state = self.state.read().expect("mock engine lock poisoned");
        if ensure_open(&state).is_err() {
            return Vec::new();
        }
        let lower = start;
        let upper = end;
        let range = state.entries.range::<[u8], _>((
            Bound::Included(lower),
            if upper.is_empty() {
                Bound::Unbounded
            } else {
                Bound::Excluded(upper)
            },
        ));
        let mut pairs = Vec::with_capacity(limit.min(state.entries.len()));
        let mut visit = |key: &Vec<u8>| {
            match get_from_state(&state, key, read_ts, isolation, resolved_locks) {
                Ok(Some((value, commit_ts))) => pairs.push(Pair {
                    key: key.clone(),
                    value,
                    commit_ts,
                    error: None,
                }),
                Err(error) => pairs.push(Pair {
                    key: key.clone(),
                    value: Vec::new(),
                    commit_ts: 0,
                    error: Some(error),
                }),
                Ok(None) => {}
            }
            pairs.len() == limit
        };
        if reverse {
            for (key, _) in range.rev() {
                if visit(key) {
                    break;
                }
            }
        } else {
            for (key, _) in range {
                if visit(key) {
                    break;
                }
            }
        }
        pairs
    }

    pub fn prewrite(&self, request: &PrewriteRequest) -> Vec<Option<MockError>> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        if let Err(error) = ensure_open(&state) {
            return vec![Some(error); request.mutations.len()];
        }
        let original = state.entries.clone();
        let mut errors = Vec::with_capacity(request.mutations.len());
        for (index, mutation) in request.mutations.iter().enumerate() {
            if mutation.op == Op::CheckNotExists {
                let error = check_insert_or_not_exists(&state, mutation, request);
                errors.push(error);
                continue;
            }
            if matches!(mutation.op, Op::Insert) && request.for_update_ts == 0 {
                if let Some(error) = check_insert_or_not_exists(&state, mutation, request) {
                    errors.push(Some(error));
                    continue;
                }
            }
            let action = request
                .pessimistic_actions
                .get(index)
                .copied()
                .unwrap_or_default();
            let error = prewrite_mutation(&mut state, mutation, request, action);
            errors.push(error);
        }
        if errors.iter().any(Option::is_some) {
            state.entries = original;
        }
        errors
    }

    pub fn pessimistic_lock(
        &self,
        request: &PessimisticLockRequest,
    ) -> (Vec<Option<MockError>>, Vec<PessimisticLockKeyResult>) {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let original = state.entries.clone();
        let mut errors = Vec::with_capacity(request.mutations.len());
        let mut results = Vec::with_capacity(request.mutations.len());
        for mutation in &request.mutations {
            let outcome = pessimistic_lock_mutation(&mut state, mutation, request);
            match outcome {
                Ok(result) => {
                    errors.push(None);
                    results.push(result);
                }
                Err(error) => {
                    errors.push(Some(error));
                    results.push(PessimisticLockKeyResult {
                        result_type: PessimisticLockKeyResultType::Failed,
                        value: Vec::new(),
                        existence: false,
                        locked_with_conflict_ts: 0,
                    });
                    if request.wait_timeout < 0 {
                        break;
                    }
                }
            }
        }
        let any_error = errors.iter().any(Option::is_some);
        if any_error && request.wake_up_mode != PessimisticWakeUpMode::ForceLock {
            state.entries = original;
        }
        (errors, results)
    }

    pub fn pessimistic_rollback(
        &self,
        start: &[u8],
        end: &[u8],
        keys: &[Vec<u8>],
        start_ts: u64,
        for_update_ts: u64,
    ) -> Vec<Option<MockError>> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let keys = if keys.is_empty() {
            state
                .entries
                .range::<[u8], _>(range_bounds(start, end))
                .filter_map(|(key, entry)| {
                    entry
                        .lock
                        .as_ref()
                        .filter(|lock| {
                            lock.op == Op::PessimisticLock
                                && lock.start_ts == start_ts
                                && lock.for_update_ts <= for_update_ts
                        })
                        .map(|_| key.clone())
                })
                .collect()
        } else {
            keys.to_vec()
        };
        for key in &keys {
            if let Some(entry) = state.entries.get_mut(key.as_slice()) {
                if entry.lock.as_ref().is_some_and(|lock| {
                    lock.op == Op::PessimisticLock
                        && lock.start_ts == start_ts
                        && lock.for_update_ts <= for_update_ts
                }) {
                    entry.lock = None;
                }
            }
        }
        vec![None; keys.len()]
    }

    pub fn commit(&self, keys: &[Vec<u8>], start_ts: u64, commit_ts: u64) -> Result<(), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let original = state.entries.clone();
        for key in keys {
            if let Err(error) = commit_key(&mut state, key, start_ts, commit_ts) {
                state.entries = original;
                return Err(error);
            }
        }
        state.waits_for.remove(&start_ts);
        Ok(())
    }

    pub fn rollback(&self, keys: &[Vec<u8>], start_ts: u64) -> Result<(), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let original = state.entries.clone();
        for key in keys {
            if let Err(error) = rollback_key(&mut state, key, start_ts) {
                state.entries = original;
                return Err(error);
            }
        }
        state.waits_for.remove(&start_ts);
        Ok(())
    }

    pub fn cleanup(&self, key: &[u8], start_ts: u64, current_ts: u64) -> Result<(), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        if let Some(lock) = state.entries.get(key).and_then(|entry| entry.lock.clone()) {
            if lock.start_ts == start_ts {
                if physical(lock.start_ts).saturating_add(lock.ttl) < physical(current_ts) {
                    return rollback_key(&mut state, key, start_ts);
                }
                return Err(lock_error(key, &lock));
            }
        }
        if let Some(write) = state
            .entries
            .get(key)
            .and_then(|entry| entry.txn_write(start_ts))
        {
            return if write.write_type == WriteType::Rollback {
                Ok(())
            } else {
                Err(MockError::AlreadyCommitted {
                    commit_ts: write.commit_ts,
                })
            };
        }
        rollback_key(&mut state, key, start_ts)
    }

    pub fn check_txn_status(
        &self,
        primary: &[u8],
        lock_ts: u64,
        caller_start_ts: u64,
        current_ts: u64,
        rollback_if_not_found: bool,
        resolving_pessimistic_lock: bool,
    ) -> Result<(u64, u64, Action), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        if let Some(lock) = state
            .entries
            .get(primary)
            .and_then(|entry| entry.lock.clone())
        {
            if lock.start_ts == lock_ts {
                if physical(lock.start_ts).saturating_add(lock.ttl) < physical(current_ts) {
                    if resolving_pessimistic_lock && lock.op == Op::PessimisticLock {
                        if let Some(entry) = state.entries.get_mut(primary) {
                            entry.lock = None;
                        }
                        return Ok((0, 0, Action::TtlExpirePessimisticRollback));
                    }
                    rollback_key(&mut state, primary, lock_ts)?;
                    return Ok((0, 0, Action::TtlExpireRollback));
                }
                let mut action = Action::NoAction;
                if caller_start_ts == u64::MAX {
                    action = Action::MinCommitTsPushed;
                } else if lock.min_commit_ts > 0 {
                    action = Action::MinCommitTsPushed;
                    if lock.min_commit_ts < caller_start_ts.saturating_add(1) {
                        let entry = state.entries.get_mut(primary).expect("lock entry exists");
                        let lock = entry.lock.as_mut().expect("lock exists");
                        lock.min_commit_ts = caller_start_ts.saturating_add(1).max(current_ts);
                    }
                }
                let ttl = state
                    .entries
                    .get(primary)
                    .and_then(|entry| entry.lock.as_ref())
                    .map_or(0, |lock| lock.ttl);
                return Ok((ttl, 0, action));
            }
        }
        if let Some(write) = state
            .entries
            .get(primary)
            .and_then(|entry| entry.txn_write(lock_ts))
        {
            return if write.write_type == WriteType::Rollback {
                Ok((0, 0, Action::NoAction))
            } else {
                Ok((0, write.commit_ts, Action::NoAction))
            };
        }
        if rollback_if_not_found {
            if resolving_pessimistic_lock {
                return Ok((0, 0, Action::LockNotExistDoNothing));
            }
            write_rollback(&mut state, primary, lock_ts);
            return Ok((0, 0, Action::LockNotExistRollback));
        }
        Err(MockError::TxnNotFound {
            start_ts: lock_ts,
            primary: primary.to_vec(),
        })
    }

    pub fn txn_heartbeat(
        &self,
        key: &[u8],
        start_ts: u64,
        advise_ttl: u64,
    ) -> Result<u64, MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let lock = state
            .entries
            .get_mut(key)
            .and_then(|entry| entry.lock.as_mut())
            .filter(|lock| lock.start_ts == start_ts)
            .ok_or_else(|| MockError::Invalid("lock doesn't exist".to_owned()))?;
        if lock.primary != key {
            return Err(MockError::Invalid(
                "txnHeartBeat on non-primary key, the code should not run here".to_owned(),
            ));
        }
        lock.ttl = lock.ttl.max(advise_ttl);
        Ok(lock.ttl)
    }

    pub fn scan_locks(
        &self,
        start: &[u8],
        end: &[u8],
        max_ts: u64,
    ) -> Result<Vec<LockInfo>, MockError> {
        let state = self.state.read().expect("mock engine lock poisoned");
        ensure_open(&state)?;
        Ok(state
            .entries
            .range::<[u8], _>(range_bounds(start, end))
            .filter_map(|(key, entry)| {
                entry
                    .lock
                    .as_ref()
                    .filter(|lock| lock.start_ts <= max_ts)
                    .map(|lock| lock_info(key, lock))
            })
            .collect())
    }

    pub fn resolve_lock(
        &self,
        start: &[u8],
        end: &[u8],
        start_ts: u64,
        commit_ts: u64,
    ) -> Result<(), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let keys: Vec<Vec<u8>> = state
            .entries
            .range::<[u8], _>(range_bounds(start, end))
            .filter_map(|(key, entry)| {
                entry
                    .lock
                    .as_ref()
                    .filter(|lock| lock.start_ts == start_ts)
                    .map(|_| key.clone())
            })
            .collect();
        for key in keys {
            if commit_ts == 0 {
                rollback_key(&mut state, &key, start_ts)?;
            } else {
                commit_key(&mut state, &key, start_ts, commit_ts)?;
            }
        }
        state.waits_for.remove(&start_ts);
        Ok(())
    }

    pub fn batch_resolve_lock(
        &self,
        start: &[u8],
        end: &[u8],
        txn_status: &HashMap<u64, u64>,
    ) -> Result<(), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let locks: Vec<(Vec<u8>, u64, u64)> = state
            .entries
            .range::<[u8], _>(range_bounds(start, end))
            .filter_map(|(key, entry)| {
                let lock = entry.lock.as_ref()?;
                txn_status
                    .get(&lock.start_ts)
                    .map(|commit_ts| (key.clone(), lock.start_ts, *commit_ts))
            })
            .collect();
        for (key, start_ts, commit_ts) in locks {
            if commit_ts == 0 {
                rollback_key(&mut state, &key, start_ts)?;
            } else {
                commit_key(&mut state, &key, start_ts, commit_ts)?;
            }
            state.waits_for.remove(&start_ts);
        }
        Ok(())
    }

    pub fn gc(&self, start: &[u8], end: &[u8], safe_point: u64) -> Result<(), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let keys: Vec<Vec<u8>> = state
            .entries
            .range::<[u8], _>(range_bounds(start, end))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            let entry = state.entries.get_mut(&key).expect("entry exists");
            if entry
                .lock
                .as_ref()
                .is_some_and(|lock| lock.start_ts <= safe_point)
            {
                return Err(MockError::Invalid(format!(
                    "key {key:?} has lock with startTs {} which is under safePoint {safe_point}",
                    entry.lock.as_ref().expect("checked").start_ts
                )));
            }
            entry.sort_writes();
            let mut kept_old_put = false;
            entry.writes.retain(|write| {
                if write.commit_ts > safe_point {
                    return true;
                }
                match write.write_type {
                    WriteType::Put if !kept_old_put => {
                        kept_old_put = true;
                        true
                    }
                    WriteType::Delete => {
                        kept_old_put = true;
                        false
                    }
                    _ => false,
                }
            });
        }
        state
            .entries
            .retain(|_, entry| entry.lock.is_some() || !entry.writes.is_empty());
        Ok(())
    }

    pub fn delete_range(&self, start: &[u8], end: &[u8]) {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let keys: Vec<Vec<u8>> = state
            .entries
            .range::<[u8], _>(range_bounds(start, end))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            state.entries.remove(&key);
        }
    }

    pub fn raw_get(&self, cf: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.state
            .read()
            .expect("mock engine lock poisoned")
            .raw_cfs
            .get(cf)
            .and_then(|map| map.get(key).cloned())
    }

    pub fn raw_batch_get(&self, cf: &str, keys: &[Vec<u8>]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|key| self.raw_get(cf, key)).collect()
    }

    pub fn raw_put(&self, cf: &str, key: Vec<u8>, value: Vec<u8>) {
        self.state
            .write()
            .expect("mock engine lock poisoned")
            .raw_cfs
            .entry(cf.to_owned())
            .or_default()
            .insert(key, value);
    }

    pub fn raw_batch_put(&self, cf: &str, pairs: impl IntoIterator<Item = (Vec<u8>, Vec<u8>)>) {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let map = state.raw_cfs.entry(cf.to_owned()).or_default();
        map.extend(pairs);
    }

    pub fn raw_delete(&self, cf: &str, key: &[u8]) {
        if let Some(map) = self
            .state
            .write()
            .expect("mock engine lock poisoned")
            .raw_cfs
            .get_mut(cf)
        {
            map.remove(key);
        }
    }

    pub fn raw_batch_delete(&self, cf: &str, keys: &[Vec<u8>]) {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        if let Some(map) = state.raw_cfs.get_mut(cf) {
            for key in keys {
                map.remove(key.as_slice());
            }
        }
    }

    pub fn raw_scan(
        &self,
        cf: &str,
        start: &[u8],
        end: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Vec<Pair> {
        if limit == 0 {
            return Vec::new();
        }
        let state = self.state.read().expect("mock engine lock poisoned");
        let Some(map) = state.raw_cfs.get(cf) else {
            return Vec::new();
        };
        let lower = if reverse { end } else { start };
        let upper = if reverse { start } else { end };
        let range = map.range::<[u8], _>((
            Bound::Included(lower),
            if upper.is_empty() {
                Bound::Unbounded
            } else {
                Bound::Excluded(upper)
            },
        ));
        let pair = |(key, value): (&Vec<u8>, &Vec<u8>)| Pair {
            key: key.clone(),
            value: value.clone(),
            commit_ts: 0,
            error: None,
        };
        if reverse {
            range.rev().take(limit).map(pair).collect()
        } else {
            range.take(limit).map(pair).collect()
        }
    }

    pub fn raw_delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<(), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let map = state
            .raw_cfs
            .get_mut(cf)
            .ok_or_else(|| MockError::Invalid(format!("{cf} not exist")))?;
        let keys: Vec<Vec<u8>> = map
            .range::<[u8], _>(range_bounds(start, end))
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            map.remove(&key);
        }
        Ok(())
    }

    pub fn raw_compare_and_swap(
        &self,
        cf: &str,
        key: &[u8],
        expected: &[u8],
        value: Vec<u8>,
    ) -> Result<(Vec<u8>, bool), MockError> {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        let map = state.raw_cfs.entry(cf.to_owned()).or_default();
        let old = map
            .get(key)
            .cloned()
            .ok_or_else(|| MockError::Invalid("leveldb: not found".to_owned()))?;
        if old != expected {
            return Ok((old, false));
        }
        map.insert(key.to_vec(), value);
        Ok((old, true))
    }

    pub fn raw_checksum(&self, cf: &str, start: &[u8], end: &[u8]) -> (u64, u64, u64) {
        let state = self.state.read().expect("mock engine lock poisoned");
        let Some(map) = state.raw_cfs.get(cf) else {
            return (0, 0, 0);
        };
        map.range::<[u8], _>(range_bounds(start, end)).fold(
            (0, 0, 0),
            |(checksum, count, bytes), (key, value)| {
                let mut joined = Vec::with_capacity(key.len() + value.len());
                joined.extend_from_slice(key);
                joined.extend_from_slice(value);
                (
                    checksum ^ crc64_ecma(&joined),
                    count + 1,
                    bytes + (key.len() + value.len()) as u64,
                )
            },
        )
    }

    pub fn mvcc_get_by_key(&self, key: &[u8]) -> MvccInfo {
        let state = self.state.read().expect("mock engine lock poisoned");
        let Some(entry) = state.entries.get(key) else {
            return MvccInfo::default();
        };
        let mut writes = entry.writes.clone();
        writes.sort_by_key(|write| std::cmp::Reverse(write.commit_ts));
        MvccInfo {
            lock: entry.lock.clone(),
            writes: writes
                .iter()
                .map(|write| MvccWrite {
                    write_type: match write.write_type {
                        WriteType::Put => Op::Put,
                        WriteType::Delete => Op::Delete,
                        WriteType::Rollback => Op::Rollback,
                        WriteType::Lock => Op::Lock,
                    },
                    start_ts: write.start_ts,
                    commit_ts: write.commit_ts,
                    short_value: short_value(&write.value),
                })
                .collect(),
            values: writes
                .into_iter()
                .map(|write| MvccValue {
                    start_ts: write.start_ts,
                    value: write.value,
                })
                .collect(),
        }
    }

    pub fn mvcc_get_by_start_ts(&self, start_ts: u64) -> (MvccInfo, Vec<u8>) {
        let state = self.state.read().expect("mock engine lock poisoned");
        let key = state
            .entries
            .iter()
            .find(|(_, entry)| entry.writes.iter().any(|write| write.start_ts == start_ts))
            .map_or_else(Vec::new, |(key, _)| key.clone());
        drop(state);
        (self.mvcc_get_by_key(&key), key)
    }

    pub fn close(&self) {
        let mut state = self.state.write().expect("mock engine lock poisoned");
        if let Some(path) = self.persistence_path.as_deref() {
            let bytes = serde_json::to_vec(&PersistentState::from_state(&state))
                .expect("mock MVCC state must serialize");
            let temporary = path.with_extension("json.tmp");
            fs::write(&temporary, bytes).expect("mock MVCC state must persist");
            fs::rename(&temporary, path).expect("mock MVCC state snapshot must install");
        }
        state.closed = true;
    }
}

fn io_error(error: std::io::Error) -> MockError {
    MockError::Invalid(error.to_string())
}

fn ensure_open(state: &State) -> Result<(), MockError> {
    (!state.closed)
        .then_some(())
        .ok_or_else(|| MockError::Invalid("mock MVCC store is closed".to_owned()))
}

fn physical(timestamp: u64) -> u64 {
    timestamp >> 18
}

fn get_from_state(
    state: &State,
    key: &[u8],
    mut read_ts: u64,
    isolation: IsolationLevel,
    resolved_locks: &[u64],
) -> Result<Option<(Vec<u8>, u64)>, MockError> {
    let Some(entry) = state.entries.get(key) else {
        return Ok(None);
    };
    if isolation == IsolationLevel::SnapshotIsolation {
        if let Some(lock) = entry.lock.as_ref() {
            if lock.start_ts <= read_ts
                && !matches!(lock.op, Op::Lock | Op::PessimisticLock)
                && !resolved_locks.contains(&lock.start_ts)
            {
                if read_ts == u64::MAX && lock.primary == key {
                    read_ts = lock.start_ts.saturating_sub(1);
                } else {
                    return Err(lock_error(key, lock));
                }
            }
        }
    }
    Ok(entry.visible_write(read_ts).and_then(|write| {
        (write.write_type == WriteType::Put).then(|| (write.value.clone(), write.commit_ts))
    }))
}

fn lock_error(key: &[u8], lock: &LockRecord) -> MockError {
    MockError::Locked {
        key: key.to_vec(),
        primary: lock.primary.clone(),
        start_ts: lock.start_ts,
        for_update_ts: lock.for_update_ts,
        ttl: lock.ttl,
        txn_size: lock.txn_size,
        lock_type: lock.op,
        min_commit_ts: lock.min_commit_ts,
    }
}

fn lock_info(key: &[u8], lock: &LockRecord) -> LockInfo {
    LockInfo {
        primary: lock.primary.clone(),
        start_ts: lock.start_ts,
        key: key.to_vec(),
        ttl: lock.ttl,
        txn_size: lock.txn_size,
        lock_type: lock.op,
        for_update_ts: lock.for_update_ts,
        min_commit_ts: lock.min_commit_ts,
    }
}

fn check_insert_or_not_exists(
    state: &State,
    mutation: &TxnMutation,
    request: &PrewriteRequest,
) -> Option<MockError> {
    match get_from_state(
        state,
        &mutation.key,
        request.start_ts,
        IsolationLevel::SnapshotIsolation,
        &request.resolved_locks,
    ) {
        Ok(Some(_)) => Some(MockError::KeyAlreadyExists {
            key: mutation.key.clone(),
        }),
        Err(error) => Some(error),
        Ok(None) => None,
    }
}

fn prewrite_mutation(
    state: &mut State,
    mutation: &TxnMutation,
    request: &PrewriteRequest,
    pessimistic_action: PessimisticAction,
) -> Option<MockError> {
    let entry = state.entries.entry(mutation.key.clone()).or_default();
    let mut ttl = request.ttl;
    let mut min_commit_ts = request.min_commit_ts;
    if let Some(lock) = entry.lock.as_ref() {
        if lock.start_ts != request.start_ts {
            let mut lock = lock.clone();
            if pessimistic_action == PessimisticAction::DoCheck {
                lock.ttl = 0;
            }
            return Some(lock_error(&mutation.key, &lock));
        }
        if lock.op != Op::PessimisticLock {
            return None;
        }
        ttl = ttl.max(lock.ttl);
        min_commit_ts = min_commit_ts.max(lock.min_commit_ts);
    } else if pessimistic_action == PessimisticAction::DoCheck {
        return Some(MockError::Abort("pessimistic lock not found".to_owned()));
    }
    if let Some(error) = check_conflict_and_assertion(
        entry,
        mutation,
        request.start_ts,
        request.start_ts,
        request.assertion_level,
        false,
        false,
    )
    .1
    {
        return Some(error);
    }
    let op = if mutation.op == Op::Insert {
        Op::Put
    } else {
        mutation.op
    };
    entry.lock = Some(LockRecord {
        start_ts: request.start_ts,
        primary: request.primary.clone(),
        value: mutation.value.clone(),
        op,
        ttl,
        for_update_ts: request.for_update_ts,
        txn_size: request.txn_size,
        min_commit_ts: if request.primary == mutation.key {
            min_commit_ts
        } else {
            0
        },
    });
    None
}

fn pessimistic_lock_mutation(
    state: &mut State,
    mutation: &TxnMutation,
    request: &PessimisticLockRequest,
) -> Result<PessimisticLockKeyResult, MockError> {
    if request.lock_only_if_exists && !request.return_values {
        return Err(MockError::Invalid(
            "LockOnlyIfExists is set for LockKeys but ReturnValues is not set".to_owned(),
        ));
    }
    if let Some(lock) = state
        .entries
        .get(&mutation.key)
        .and_then(|entry| entry.lock.as_ref())
        .cloned()
    {
        if lock.start_ts != request.start_ts {
            let key_hash = farmhash::fingerprint64(&mutation.key);
            state
                .waits_for
                .insert(request.start_ts, (lock.start_ts, key_hash));
            if let Some(deadlock_key_hash) = deadlock_cycle(&state.waits_for, request.start_ts) {
                return Err(MockError::Deadlock {
                    lock_ts: lock.start_ts,
                    lock_key: mutation.key.clone(),
                    deadlock_key_hash,
                });
            }
            return Err(lock_error(&mutation.key, &lock));
        }
    }
    let entry = state.entries.entry(mutation.key.clone()).or_default();
    let (value, conflict) = check_conflict_and_assertion(
        entry,
        mutation,
        request.for_update_ts,
        request.start_ts,
        AssertionLevel::Off,
        request.lock_only_if_exists,
        request.wake_up_mode == PessimisticWakeUpMode::ForceLock,
    );
    let conflict_commit_ts = match conflict {
        Some(MockError::Conflict {
            conflict_commit_ts,
            can_force_lock: true,
            ..
        }) => conflict_commit_ts,
        Some(error) => return Err(error),
        None => 0,
    };
    let exists = value.is_some();
    if !(request.lock_only_if_exists && !exists) {
        let prior_for_update_ts = entry.lock.as_ref().map_or(0, |lock| lock.for_update_ts);
        if entry.lock.is_none() || prior_for_update_ts < request.for_update_ts {
            entry.lock = Some(LockRecord {
                start_ts: request.start_ts,
                primary: request.primary.clone(),
                value: Vec::new(),
                op: Op::PessimisticLock,
                ttl: request.ttl,
                for_update_ts: request.for_update_ts.max(conflict_commit_ts),
                txn_size: 0,
                min_commit_ts: request.min_commit_ts,
            });
        }
    }
    Ok(PessimisticLockKeyResult {
        result_type: if conflict_commit_ts == 0 {
            PessimisticLockKeyResultType::Normal
        } else {
            PessimisticLockKeyResultType::LockedWithConflict
        },
        value: value.unwrap_or_default(),
        existence: exists,
        locked_with_conflict_ts: conflict_commit_ts,
    })
}

fn check_conflict_and_assertion(
    entry: &Entry,
    mutation: &TxnMutation,
    conflict_ts: u64,
    start_ts: u64,
    assertion_level: AssertionLevel,
    lock_only_if_exists: bool,
    allow_lock_with_conflict: bool,
) -> (Option<Vec<u8>>, Option<MockError>) {
    let mut writes = entry.writes.iter().collect::<Vec<_>>();
    writes.sort_by_key(|write| std::cmp::Reverse(write.commit_ts));
    let newest = writes.first().copied();
    let conflict = newest
        .filter(|write| write.commit_ts > conflict_ts)
        .map(|write| MockError::Conflict {
            start_ts: conflict_ts,
            conflict_start_ts: write.start_ts,
            conflict_commit_ts: write.commit_ts,
            key: mutation.key.clone(),
            can_force_lock: allow_lock_with_conflict,
        });
    if conflict.is_some() && !allow_lock_with_conflict {
        return (None, conflict);
    }
    for write in &writes {
        if write.write_type == WriteType::Rollback && write.commit_ts == start_ts {
            return (
                None,
                Some(MockError::AlreadyRolledBack {
                    start_ts,
                    key: mutation.key.clone(),
                }),
            );
        }
        if write.commit_ts < start_ts {
            break;
        }
    }
    let latest_value = writes
        .iter()
        .find(|write| matches!(write.write_type, WriteType::Put | WriteType::Delete))
        .and_then(|write| (write.write_type == WriteType::Put).then(|| write.value.clone()));
    let exists = latest_value.is_some();
    if mutation.op == Op::PessimisticLock && mutation.assertion == Assertion::NotExist && exists {
        return (
            None,
            conflict.or_else(|| {
                Some(MockError::KeyAlreadyExists {
                    key: mutation.key.clone(),
                })
            }),
        );
    }
    if assertion_level != AssertionLevel::Off && mutation.op != Op::PessimisticLock {
        let failed = match mutation.assertion {
            Assertion::Exist => !exists,
            Assertion::NotExist => exists,
            Assertion::None => false,
        };
        if failed {
            return (
                None,
                Some(MockError::AssertionFailed {
                    start_ts,
                    key: mutation.key.clone(),
                    assertion: mutation.assertion,
                    existing_start_ts: newest.map_or(0, |write| write.start_ts),
                    existing_commit_ts: newest.map_or(0, |write| write.commit_ts),
                }),
            );
        }
    }
    if lock_only_if_exists && !exists && conflict.is_some() {
        return (None, conflict);
    }
    (latest_value, conflict)
}

fn commit_key(
    state: &mut State,
    key: &[u8],
    start_ts: u64,
    commit_ts: u64,
) -> Result<(), MockError> {
    let entry = state.entries.entry(key.to_vec()).or_default();
    let Some(lock) = entry.lock.clone().filter(|lock| lock.start_ts == start_ts) else {
        return match entry.txn_write(start_ts) {
            Some(write) if write.write_type != WriteType::Rollback => Ok(()),
            _ => Err(MockError::Retryable("txn not found".to_owned())),
        };
    };
    if lock.min_commit_ts > commit_ts {
        return Err(MockError::CommitTsExpired {
            start_ts,
            attempted_commit_ts: commit_ts,
            key: key.to_vec(),
            min_commit_ts: lock.min_commit_ts,
        });
    }
    let write_type = match lock.op {
        Op::Put | Op::Insert => WriteType::Put,
        Op::Lock => WriteType::Lock,
        _ => WriteType::Delete,
    };
    entry.writes.push(WriteRecord {
        write_type,
        start_ts,
        commit_ts,
        value: lock.value,
    });
    entry.sort_writes();
    entry.lock = None;
    Ok(())
}

fn rollback_key(state: &mut State, key: &[u8], start_ts: u64) -> Result<(), MockError> {
    let entry = state.entries.entry(key.to_vec()).or_default();
    if entry
        .lock
        .as_ref()
        .is_some_and(|lock| lock.start_ts == start_ts)
    {
        entry.lock = None;
        write_rollback(state, key, start_ts);
        return Ok(());
    }
    if let Some(write) = entry.txn_write(start_ts) {
        return if write.write_type == WriteType::Rollback {
            Ok(())
        } else {
            Err(MockError::AlreadyCommitted {
                commit_ts: write.commit_ts,
            })
        };
    }
    write_rollback(state, key, start_ts);
    Ok(())
}

fn write_rollback(state: &mut State, key: &[u8], start_ts: u64) {
    let entry = state.entries.entry(key.to_vec()).or_default();
    if entry.txn_write(start_ts).is_none() {
        entry.writes.push(WriteRecord {
            write_type: WriteType::Rollback,
            start_ts,
            commit_ts: start_ts,
            value: Vec::new(),
        });
        entry.sort_writes();
    }
}

fn deadlock_cycle(waits_for: &HashMap<u64, (u64, u64)>, start: u64) -> Option<u64> {
    let mut current = start;
    let mut seen = HashSet::new();
    while seen.insert(current) {
        let (next, key_hash) = waits_for.get(&current).copied()?;
        if next == start {
            return Some(key_hash);
        }
        current = next;
    }
    None
}

fn range_bounds<'a>(start: &'a [u8], end: &'a [u8]) -> (Bound<&'a [u8]>, Bound<&'a [u8]>) {
    (
        Bound::Included(start),
        if end.is_empty() {
            Bound::Unbounded
        } else {
            Bound::Excluded(end)
        },
    )
}

fn short_value(value: &[u8]) -> Vec<u8> {
    if value.len() <= SHORT_VALUE_MAX_LEN {
        value.to_vec()
    } else {
        Vec::new()
    }
}

fn write_slice(output: &mut Vec<u8>, value: &[u8]) {
    let mut length = value.len() as u64;
    while length >= 0x80 {
        output.push((length as u8) | 0x80);
        length >>= 7;
    }
    output.push(length as u8);
    output.extend_from_slice(value);
}

fn read_slice(input: &mut &[u8]) -> Result<Vec<u8>, MockError> {
    let mut length = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *input
            .first()
            .ok_or_else(|| MockError::Decode("unexpected EOF".to_owned()))?;
        *input = &input[1..];
        length |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            let length = usize::try_from(length)
                .map_err(|_| MockError::Decode("slice length overflow".to_owned()))?;
            if length > MAX_MARSHALLED_SLICE {
                return Err(MockError::Decode(
                    "too large slice, maybe something wrong".to_owned(),
                ));
            }
            if input.len() < length {
                return Err(MockError::Decode("unexpected EOF".to_owned()));
            }
            let value = input[..length].to_vec();
            *input = &input[length..];
            return Ok(value);
        }
    }
    Err(MockError::Decode("invalid varint".to_owned()))
}

fn read_u64(input: &mut &[u8]) -> Result<u64, MockError> {
    let bytes = read_array::<8>(input)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_i64(input: &mut &[u8]) -> Result<i64, MockError> {
    let bytes = read_array::<8>(input)?;
    Ok(i64::from_le_bytes(bytes))
}

fn read_i32(input: &mut &[u8]) -> Result<i32, MockError> {
    let bytes = read_array::<4>(input)?;
    Ok(i32::from_le_bytes(bytes))
}

fn read_array<const N: usize>(input: &mut &[u8]) -> Result<[u8; N], MockError> {
    if input.len() < N {
        return Err(MockError::Decode("unexpected EOF".to_owned()));
    }
    let (head, tail) = input.split_at(N);
    *input = tail;
    Ok(head.try_into().expect("slice length checked"))
}

fn crc64_ecma(bytes: &[u8]) -> u64 {
    // Go's hash/crc64 ECMA table is the reflected polynomial and Update uses
    // complement-in/complement-out, i.e. CRC-64/XZ parameters.
    const REFLECTED_POLY: u64 = 0xc96c_5795_d787_0f42;
    let mut crc = u64::MAX;
    for byte in bytes {
        crc ^= u64::from(*byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ REFLECTED_POLY
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(engine: &MockEngine, key: &[u8], value: &[u8], start_ts: u64, commit_ts: u64) {
        let errors = engine.prewrite(&PrewriteRequest {
            mutations: vec![TxnMutation::put(key, value)],
            primary: key.to_vec(),
            start_ts,
            ..Default::default()
        });
        assert_eq!(errors, vec![None]);
        engine.commit(&[key.to_vec()], start_ts, commit_ts).unwrap();
    }

    fn delete(engine: &MockEngine, key: &[u8], start_ts: u64, commit_ts: u64) {
        let errors = engine.prewrite(&PrewriteRequest {
            mutations: vec![TxnMutation::delete(key)],
            primary: key.to_vec(),
            start_ts,
            min_commit_ts: start_ts + 1,
            ..Default::default()
        });
        assert_eq!(errors, vec![None]);
        engine.commit(&[key.to_vec()], start_ts, commit_ts).unwrap();
    }

    fn prewrite(
        engine: &MockEngine,
        pairs: &[(&[u8], &[u8])],
        primary: &[u8],
        start_ts: u64,
        ttl: u64,
    ) -> Vec<Option<MockError>> {
        engine.prewrite(&PrewriteRequest {
            mutations: pairs
                .iter()
                .map(|(key, value)| TxnMutation::put(*key, *value))
                .collect(),
            primary: primary.to_vec(),
            start_ts,
            ttl,
            min_commit_ts: start_ts + 1,
            ..Default::default()
        })
    }

    fn get(engine: &MockEngine, key: &[u8], ts: u64) -> Result<Option<Vec<u8>>, MockError> {
        engine
            .get(key, ts, IsolationLevel::SnapshotIsolation, &[])
            .map(|value| value.map(|value| value.0))
    }

    fn assert_scan(
        engine: &MockEngine,
        start: &[u8],
        end: &[u8],
        limit: usize,
        ts: u64,
        reverse: bool,
        expected: &[(&[u8], &[u8])],
    ) {
        let pairs = engine.scan(
            start,
            end,
            limit,
            ts,
            IsolationLevel::SnapshotIsolation,
            &[],
            reverse,
        );
        assert_eq!(pairs.len(), expected.len());
        for (pair, (key, value)) in pairs.iter().zip(expected) {
            assert_eq!(pair.error, None);
            assert_eq!(&pair.key, key);
            assert_eq!(&pair.value, value);
        }
    }

    #[test]
    fn lock_and_write_binary_formats_round_trip() {
        let lock = LockRecord {
            start_ts: 47,
            primary: b"abc".to_vec(),
            value: b"de".to_vec(),
            op: Op::Put,
            ttl: 444,
            for_update_ts: 555,
            txn_size: 2,
            min_commit_ts: 666,
        };
        assert_eq!(
            LockRecord::unmarshal_binary(&lock.marshal_binary()).unwrap(),
            lock
        );
        let write = WriteRecord {
            write_type: WriteType::Put,
            start_ts: 42,
            commit_ts: 55,
            value: b"de".to_vec(),
        };
        assert_eq!(
            WriteRecord::unmarshal_binary(&write.marshal_binary()).unwrap(),
            write
        );
    }

    #[test]
    fn source_raw_checksum_uses_go_crc64_ecma() {
        assert_eq!(crc64_ecma(b"123456789"), 0x995d_c9bb_df19_39fa);
        let engine = MockEngine::new();
        engine.raw_put("default", b"1234".to_vec(), b"56789".to_vec());
        assert_eq!(
            engine.raw_checksum("default", b"", b""),
            (0x995d_c9bb_df19_39fa, 1, 9)
        );
    }

    #[test]
    fn optimistic_visibility_locks_rollback_resolution_gc_and_raw_kv() {
        let engine = MockEngine::new();
        put(&engine, b"a", b"v1", 1, 2);
        put(&engine, b"a", b"v2", 3, 4);
        assert_eq!(
            engine
                .get(b"a", 2, IsolationLevel::SnapshotIsolation, &[])
                .unwrap(),
            Some((b"v1".to_vec(), 2))
        );

        let errors = engine.prewrite(&PrewriteRequest {
            mutations: vec![TxnMutation::put(b"a", b"v3"), TxnMutation::put(b"b", b"v3")],
            primary: b"a".to_vec(),
            start_ts: 5,
            ttl: 100,
            min_commit_ts: 6,
            ..Default::default()
        });
        assert_eq!(errors, vec![None, None]);
        assert!(matches!(
            engine.get(b"b", 10, IsolationLevel::SnapshotIsolation, &[]),
            Err(MockError::Locked { .. })
        ));
        engine.resolve_lock(b"", b"", 5, 8).unwrap();
        assert_eq!(
            engine
                .get(b"b", 8, IsolationLevel::SnapshotIsolation, &[])
                .unwrap(),
            Some((b"v3".to_vec(), 8))
        );
        engine.gc(b"", b"", 7).unwrap();
        assert_eq!(
            engine
                .get(b"a", 2, IsolationLevel::SnapshotIsolation, &[])
                .unwrap(),
            None
        );

        engine.raw_put("default", b"k".to_vec(), b"v".to_vec());
        assert_eq!(engine.raw_get("default", b"k"), Some(b"v".to_vec()));
        assert_eq!(
            engine
                .raw_compare_and_swap("default", b"k", b"v", b"n".to_vec())
                .unwrap(),
            (b"v".to_vec(), true)
        );
        assert_eq!(
            engine.raw_scan("default", b"", b"", 10, false)[0].value,
            b"n".to_vec()
        );
    }

    #[test]
    fn source_test_get() {
        let engine = MockEngine::new();
        assert_eq!(get(&engine, b"x", 10).unwrap(), None);
        put(&engine, b"x", b"x", 5, 10);
        assert_eq!(get(&engine, b"x", 9).unwrap(), None);
        assert_eq!(get(&engine, b"x", 10).unwrap(), Some(b"x".to_vec()));
        assert_eq!(get(&engine, b"x", 11).unwrap(), Some(b"x".to_vec()));
    }

    #[test]
    fn source_test_get_with_lock() {
        let engine = MockEngine::new();
        put(&engine, b"key", b"value", 5, 10);
        let errors = engine.prewrite(&PrewriteRequest {
            mutations: vec![TxnMutation {
                op: Op::Lock,
                key: b"key".to_vec(),
                value: Vec::new(),
                assertion: Assertion::None,
            }],
            primary: b"key".to_vec(),
            start_ts: 20,
            min_commit_ts: 21,
            ..Default::default()
        });
        assert_eq!(errors, vec![None]);
        assert_eq!(get(&engine, b"key", 25).unwrap(), Some(b"value".to_vec()));
        engine.commit(&[b"key".to_vec()], 20, 30).unwrap();

        assert_eq!(
            prewrite(
                &engine,
                &[(b"key", b"value2"), (b"key2", b"v5")],
                b"key",
                40,
                0,
            ),
            vec![None, None]
        );
        assert!(matches!(
            get(&engine, b"key", 41),
            Err(MockError::Locked { .. })
        ));
        assert!(matches!(
            get(&engine, b"key2", u64::MAX),
            Err(MockError::Locked { .. })
        ));
        assert_eq!(
            get(&engine, b"key", u64::MAX).unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn source_test_delete() {
        let engine = MockEngine::new();
        put(&engine, b"x", b"x5-10", 5, 10);
        delete(&engine, b"x", 15, 20);
        assert_eq!(get(&engine, b"x", 5).unwrap(), None);
        assert_eq!(get(&engine, b"x", 9).unwrap(), None);
        assert_eq!(get(&engine, b"x", 10).unwrap(), Some(b"x5-10".to_vec()));
        assert_eq!(get(&engine, b"x", 19).unwrap(), Some(b"x5-10".to_vec()));
        assert_eq!(get(&engine, b"x", 20).unwrap(), None);
    }

    #[test]
    fn source_test_cleanup_rollback() {
        let engine = MockEngine::new();
        put(&engine, b"secondary", b"s-0", 1, 2);
        assert_eq!(
            prewrite(
                &engine,
                &[(b"primary", b"p-5"), (b"secondary", b"s-5")],
                b"primary",
                5,
                0,
            ),
            vec![None, None]
        );
        assert!(get(&engine, b"secondary", 8).is_err());
        engine.commit(&[b"primary".to_vec()], 5, 10).unwrap();
        assert!(matches!(
            engine.rollback(&[b"primary".to_vec()], 5),
            Err(MockError::AlreadyCommitted { commit_ts: 10 })
        ));
    }

    #[test]
    fn source_test_forward_and_reverse_scan_tables() {
        let engine = MockEngine::new();
        put(&engine, b"A", b"A10", 5, 10);
        put(&engine, b"C", b"C10", 5, 10);
        put(&engine, b"E", b"E10", 5, 10);

        assert_scan(&engine, b"", b"", 0, 10, false, &[]);
        assert_scan(
            &engine,
            b"",
            b"",
            2,
            10,
            false,
            &[(b"A", b"A10"), (b"C", b"C10")],
        );
        assert_scan(
            &engine,
            b"A\0",
            b"",
            3,
            10,
            false,
            &[(b"C", b"C10"), (b"E", b"E10")],
        );
        assert_scan(
            &engine,
            b"",
            b"E",
            5,
            10,
            false,
            &[(b"A", b"A10"), (b"C", b"C10")],
        );
        assert_scan(
            &engine,
            b"",
            b"Z",
            2,
            10,
            true,
            &[(b"E", b"E10"), (b"C", b"C10")],
        );
        assert_scan(
            &engine,
            b"",
            b"C\0",
            4,
            10,
            true,
            &[(b"C", b"C10"), (b"A", b"A10")],
        );
        assert_scan(&engine, b"A\0", b"C", 5, 10, true, &[]);

        put(&engine, b"B", b"B20", 15, 20);
        put(&engine, b"D", b"D20", 15, 20);
        assert_scan(
            &engine,
            b"",
            b"",
            5,
            20,
            false,
            &[
                (b"A", b"A10"),
                (b"B", b"B20"),
                (b"C", b"C10"),
                (b"D", b"D20"),
                (b"E", b"E10"),
            ],
        );
        assert_scan(
            &engine,
            b"B",
            b"D",
            5,
            20,
            true,
            &[(b"C", b"C10"), (b"B", b"B20")],
        );

        delete(&engine, b"A", 25, 30);
        delete(&engine, b"D", 25, 30);
        assert_scan(
            &engine,
            b"",
            b"",
            5,
            30,
            false,
            &[(b"B", b"B20"), (b"C", b"C10"), (b"E", b"E10")],
        );
        assert_scan(
            &engine,
            b"",
            b"Z",
            5,
            30,
            true,
            &[(b"E", b"E10"), (b"C", b"C10"), (b"B", b"B20")],
        );

        delete(&engine, b"B", 35, 40);
        put(&engine, b"C", b"C40", 35, 40);
        put(&engine, b"D", b"D40", 35, 40);
        assert_scan(
            &engine,
            b"",
            b"",
            5,
            100,
            false,
            &[(b"C", b"C40"), (b"D", b"D40"), (b"E", b"E10")],
        );
        assert_scan(
            &engine,
            b"",
            b"Z",
            5,
            100,
            true,
            &[(b"E", b"E10"), (b"D", b"D40"), (b"C", b"C40")],
        );
    }

    #[test]
    fn source_test_batch_get() {
        let engine = MockEngine::new();
        put(&engine, b"k1", b"v1", 1, 2);
        put(&engine, b"k2", b"v2", 1, 2);
        put(&engine, b"k2", b"v2", 3, 4);
        put(&engine, b"k3", b"v3", 1, 2);
        let pairs = engine.batch_get(
            &[b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
            5,
            IsolationLevel::SnapshotIsolation,
            &[],
        );
        assert_eq!(
            pairs
                .iter()
                .map(|pair| pair.value.as_slice())
                .collect::<Vec<_>>(),
            vec![b"v1", b"v2", b"v3"]
        );
    }

    #[test]
    fn source_test_scan_lock_and_resolved_lock() {
        let engine = MockEngine::new();
        put(&engine, b"k1", b"v1", 1, 2);
        assert_eq!(
            prewrite(&engine, &[(b"p1", b"v5"), (b"s1", b"v5")], b"p1", 5, 0),
            vec![None, None]
        );
        assert_eq!(
            prewrite(&engine, &[(b"p2", b"v10"), (b"s2", b"v10")], b"p1", 5, 0),
            vec![None, None]
        );
        assert_eq!(
            prewrite(&engine, &[(b"p3", b"v20"), (b"s3", b"v20")], b"p3", 20, 0),
            vec![None, None]
        );
        let locks = engine.scan_locks(b"a", b"r", 12).unwrap();
        assert_eq!(
            locks
                .iter()
                .map(|lock| (lock.key.as_slice(), lock.start_ts))
                .collect::<Vec<_>>(),
            vec![(b"p1".as_slice(), 5), (b"p2".as_slice(), 5)]
        );

        let pairs = engine.scan(
            b"p1",
            b"",
            3,
            10,
            IsolationLevel::SnapshotIsolation,
            &[],
            false,
        );
        assert!(pairs[0].error.is_some());
        assert!(pairs[1].error.is_some());
        let pairs = engine.scan(
            b"p1",
            b"",
            3,
            10,
            IsolationLevel::SnapshotIsolation,
            &[5],
            false,
        );
        assert!(pairs.iter().all(|pair| pair.error.is_none()));
    }

    #[test]
    fn source_test_commit_conflict_and_idempotence() {
        let engine = MockEngine::new();
        assert_eq!(prewrite(&engine, &[(b"x", b"A")], b"x", 5, 0), vec![None]);
        assert!(matches!(
            prewrite(&engine, &[(b"x", b"B")], b"x", 10, 0)[0],
            Some(MockError::Locked { .. })
        ));
        engine.rollback(&[b"x".to_vec()], 5).unwrap();
        assert!(engine.commit(&[b"x".to_vec()], 5, 10).is_err());
        assert_eq!(prewrite(&engine, &[(b"x", b"B")], b"x", 10, 0), vec![None]);
        assert!(engine.commit(&[b"x".to_vec()], 5, 20).is_err());
        engine.commit(&[b"x".to_vec()], 10, 20).unwrap();
        engine.commit(&[b"x".to_vec()], 10, 20).unwrap();
    }

    #[test]
    fn source_test_resolve_and_batch_resolve_lock() {
        let engine = MockEngine::new();
        assert_eq!(
            prewrite(&engine, &[(b"p1", b"v5"), (b"s1", b"v5")], b"p1", 5, 0),
            vec![None, None]
        );
        assert_eq!(
            prewrite(&engine, &[(b"p2", b"v10"), (b"s2", b"v10")], b"p2", 10, 0),
            vec![None, None]
        );
        engine.resolve_lock(b"", b"", 5, 0).unwrap();
        engine.resolve_lock(b"", b"", 10, 20).unwrap();
        assert_eq!(get(&engine, b"p1", 20).unwrap(), None);
        assert_eq!(get(&engine, b"s2", 30).unwrap(), Some(b"v10".to_vec()));

        for (ts, pairs) in [
            (
                11,
                vec![
                    (b"p11".as_slice(), b"v".as_slice()),
                    (b"s11".as_slice(), b"v".as_slice()),
                ],
            ),
            (
                12,
                vec![
                    (b"p12".as_slice(), b"v".as_slice()),
                    (b"s12".as_slice(), b"v".as_slice()),
                ],
            ),
            (
                15,
                vec![
                    (b"p15".as_slice(), b"v".as_slice()),
                    (b"s15".as_slice(), b"v".as_slice()),
                ],
            ),
        ] {
            assert!(prewrite(&engine, &pairs, pairs[0].0, ts, 0)
                .iter()
                .all(Option::is_none));
        }
        engine
            .batch_resolve_lock(b"", b"", &HashMap::from([(11, 0), (12, 22)]))
            .unwrap();
        assert_eq!(get(&engine, b"p11", 30).unwrap(), None);
        assert_eq!(get(&engine, b"s12", 30).unwrap(), Some(b"v".to_vec()));
        assert_eq!(engine.scan_locks(b"", b"", 30).unwrap().len(), 2);
        engine
            .batch_resolve_lock(b"", b"", &HashMap::from([(15, 0)]))
            .unwrap();
        assert!(engine.scan_locks(b"", b"", 30).unwrap().is_empty());
    }

    #[test]
    fn source_test_gc() {
        let engine = MockEngine::new();
        put(&engine, b"k1", b"v1", 1, 2);
        put(&engine, b"k1", b"v2", 11, 12);
        put(&engine, b"k2", b"v1", 1, 2);
        put(&engine, b"k2", b"v2", 11, 12);
        put(&engine, b"k2", b"v3", 101, 102);
        put(&engine, b"k3", b"v1", 1, 2);
        put(&engine, b"k3", b"v2", 11, 12);
        delete(&engine, b"k3", 101, 102);
        put(&engine, b"k4", b"v1", 1, 2);
        delete(&engine, b"k4", 11, 12);
        engine.gc(b"", b"", 100).unwrap();
        assert_eq!(get(&engine, b"k1", 5).unwrap(), None);
        assert_eq!(get(&engine, b"k1", 15).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(get(&engine, b"k2", 105).unwrap(), Some(b"v3".to_vec()));
        assert_eq!(get(&engine, b"k3", 15).unwrap(), Some(b"v2".to_vec()));
        assert_eq!(get(&engine, b"k3", 105).unwrap(), None);
        assert_eq!(get(&engine, b"k4", 5).unwrap(), None);
    }

    #[test]
    fn source_test_rollback_and_write_conflict() {
        let engine = MockEngine::new();
        put(&engine, b"test", b"test", 1, 3);
        let errors = prewrite(
            &engine,
            &[(b"lock", b"lock"), (b"test", b"test1")],
            b"test",
            2,
            2,
        );
        assert!(matches!(errors[1], Some(MockError::Conflict { .. })));
        put(&engine, b"test", b"test2", 5, 8);
        engine.cleanup(b"test", 2, u64::MAX).unwrap();
        assert!(matches!(
            prewrite(&engine, &[(b"test", b"test3")], b"test", 6, 1)[0],
            Some(MockError::Conflict { .. })
        ));
    }

    #[test]
    fn source_test_delete_range() {
        let engine = MockEngine::new();
        for index in 1..=5_u64 {
            let key = index.to_string().into_bytes();
            let mut value = b"v".to_vec();
            value.extend_from_slice(&key);
            put(&engine, &key, &value, 1 + 2 * index, 2 + 2 * index);
        }
        engine.delete_range(b"2", b"4");
        assert_scan(
            &engine,
            b"0",
            b"",
            10,
            30,
            false,
            &[(b"1", b"v1"), (b"4", b"v4"), (b"5", b"v5")],
        );
        engine.delete_range(b"5", b"5");
        engine.delete_range(b"41", b"42");
        engine.delete_range(b"4\0", b"5\0");
        assert_scan(
            &engine,
            b"0",
            b"",
            10,
            60,
            false,
            &[(b"1", b"v1"), (b"4", b"v4")],
        );
        engine.delete_range(b"0", b"9");
        assert_scan(&engine, b"0", b"", 10, 70, false, &[]);
    }

    #[test]
    fn source_test_read_committed() {
        let engine = MockEngine::new();
        put(&engine, b"key", b"v1", 5, 10);
        assert_eq!(
            prewrite(&engine, &[(b"key", b"v2")], b"key", 15, 0),
            vec![None]
        );
        assert!(get(&engine, b"key", 20).is_err());
        assert_eq!(
            engine
                .get(b"key", 20, IsolationLevel::ReadCommitted, &[])
                .unwrap(),
            Some((b"v1".to_vec(), 10))
        );
    }

    #[test]
    fn source_test_check_txn_status_and_reject_commit_ts() {
        let engine = MockEngine::new();
        let start_ts = 5_u64 << 18;
        assert_eq!(
            prewrite(&engine, &[(b"pk", b"val")], b"pk", start_ts, 666),
            vec![None]
        );
        assert_eq!(
            engine
                .check_txn_status(b"pk", start_ts, start_ts + 100, 666, false, false)
                .unwrap(),
            (666, 0, Action::MinCommitTsPushed)
        );
        assert_eq!(
            engine
                .check_txn_status(b"pk", start_ts, u64::MAX, 666, false, false)
                .unwrap()
                .2,
            Action::MinCommitTsPushed
        );
        engine
            .commit(&[b"pk".to_vec()], start_ts, start_ts + 101)
            .unwrap();
        assert_eq!(
            engine
                .check_txn_status(b"pk", start_ts, 0, 666, false, false)
                .unwrap()
                .1,
            start_ts + 101
        );

        assert_eq!(
            prewrite(&engine, &[(b"pk1", b"val")], b"pk1", start_ts, 666),
            vec![None]
        );
        engine.rollback(&[b"pk1".to_vec()], start_ts).unwrap();
        assert_eq!(
            engine
                .check_txn_status(b"pk1", start_ts, 0, 666, false, false)
                .unwrap(),
            (0, 0, Action::NoAction)
        );

        assert_eq!(
            prewrite(&engine, &[(b"pk2", b"val")], b"pk2", start_ts, 666),
            vec![None]
        );
        assert_eq!(
            engine
                .check_txn_status(b"pk2", start_ts, 0, 777_u64 << 18, false, false)
                .unwrap()
                .2,
            Action::TtlExpireRollback
        );
        assert!(matches!(
            engine.check_txn_status(b"missing", 5, 0, 666, false, false),
            Err(MockError::TxnNotFound { .. })
        ));
        assert_eq!(
            engine
                .check_txn_status(b"missing", 5, 0, 666, true, false)
                .unwrap()
                .2,
            Action::LockNotExistRollback
        );
        let errors = engine.prewrite(&PrewriteRequest {
            mutations: vec![TxnMutation::put(b"missing", b"val")],
            primary: b"missing".to_vec(),
            start_ts: 4,
            min_commit_ts: 6,
            ..Default::default()
        });
        assert!(errors[0].is_some());

        assert_eq!(prewrite(&engine, &[(b"x", b"A")], b"x", 5, 0), vec![None]);
        engine
            .check_txn_status(b"x", 5, 100, 100, false, false)
            .unwrap();
        assert!(matches!(
            engine.commit(&[b"x".to_vec()], 5, 10),
            Err(MockError::CommitTsExpired {
                min_commit_ts: 101,
                ..
            })
        ));
    }

    #[test]
    fn source_test_mvcc_debug_and_heartbeat() {
        let engine = MockEngine::new();
        assert_eq!(
            prewrite(&engine, &[(b"q1", b"v5")], b"p1", 5, 0),
            vec![None]
        );
        let info = engine.mvcc_get_by_key(b"q1");
        let lock = info.lock.unwrap();
        assert_eq!(lock.op, Op::Put);
        assert_eq!(lock.start_ts, 5);
        assert_eq!(lock.primary, b"p1");
        assert_eq!(lock.value, b"v5");

        assert_eq!(
            prewrite(&engine, &[(b"pk", b"val")], b"pk", 6, 666),
            vec![None]
        );
        assert_eq!(engine.txn_heartbeat(b"pk", 6, 888).unwrap(), 888);
        assert_eq!(engine.txn_heartbeat(b"pk", 6, 300).unwrap(), 888);
        engine.cleanup(b"pk", 6, u64::MAX).unwrap();
        assert!(engine.txn_heartbeat(b"pk", 6, 1000).is_err());
    }

    #[test]
    fn source_pessimistic_lock_values_existence_force_lock_rollback_and_deadlock() {
        let engine = MockEngine::new();
        put(&engine, b"exists", b"value", 1, 2);
        let (errors, results) = engine.pessimistic_lock(&PessimisticLockRequest {
            mutations: vec![TxnMutation {
                op: Op::PessimisticLock,
                key: b"exists".to_vec(),
                value: Vec::new(),
                assertion: Assertion::None,
            }],
            primary: b"exists".to_vec(),
            start_ts: 5,
            for_update_ts: 5,
            ttl: 100,
            return_values: true,
            ..Default::default()
        });
        assert_eq!(errors, vec![None]);
        assert_eq!(results[0].value, b"value");
        assert!(results[0].existence);
        assert_eq!(engine.pessimistic_rollback(b"", b"", &[], 5, 5), vec![None]);

        let lock = |engine: &MockEngine, key: &[u8], start_ts, for_update_ts| {
            engine.pessimistic_lock(&PessimisticLockRequest {
                mutations: vec![TxnMutation {
                    op: Op::PessimisticLock,
                    key: key.to_vec(),
                    value: Vec::new(),
                    assertion: Assertion::None,
                }],
                primary: key.to_vec(),
                start_ts,
                for_update_ts,
                ..Default::default()
            })
        };
        assert!(lock(&engine, b"a", 10, 10).0[0].is_none());
        assert!(lock(&engine, b"b", 20, 20).0[0].is_none());
        assert!(matches!(
            lock(&engine, b"b", 10, 21).0[0],
            Some(MockError::Locked { .. })
        ));
        assert!(matches!(
            lock(&engine, b"a", 20, 22).0[0],
            Some(MockError::Deadlock { .. })
        ));
    }

    #[test]
    fn source_nonempty_path_restores_committed_and_raw_state_after_close() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "client-rust-mocktikv-{}-{unique}",
            std::process::id()
        ));
        let engine = MockEngine::open(&path).unwrap();
        put(&engine, b"mvcc", b"value", 1, 2);
        engine.raw_put("default", b"raw".to_vec(), b"value".to_vec());
        engine.close();

        let reopened = MockEngine::open(&path).unwrap();
        assert_eq!(get(&reopened, b"mvcc", 3).unwrap(), Some(b"value".to_vec()));
        assert_eq!(reopened.raw_get("default", b"raw"), Some(b"value".to_vec()));
        reopened.close();
        std::fs::remove_dir_all(path).unwrap();
    }
}
