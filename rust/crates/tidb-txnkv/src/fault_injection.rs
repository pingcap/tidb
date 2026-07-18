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

//! KV fault-injection wrappers translated from `pkg/kv/fault_injection.go`.
//!
//! The Go implementation wraps the wide TiDB storage interfaces. This module
//! keeps that behavior behind the crate's canonical [`Getter`] and
//! [`BatchGetter`] read contracts: get and batch-get fail before reaching the
//! wrapped object when a get error is configured, commit does the same for a
//! commit error, and clearing either error restores delegation. Begin and
//! snapshot creation are deliberately delegated; injection of those operations
//! is not part of the source contract.
//!
//! No production storage implements [`KvStorage`] yet. These wrappers are an
//! authority consolidation and future client enabler, not a connected TiKV
//! transaction path.

use std::sync::{Arc, RwLock};

use crate::{BatchGetError, BatchGetOptions, BatchGetter, Getter, Key, ValueEntry, Version};

/// A transaction read surface with a commit operation.
pub trait KvTransaction: Getter + BatchGetter<Error = <Self as Getter>::Error> {
    /// Commits the transaction.
    fn commit(&mut self) -> Result<(), <Self as Getter>::Error>;
}

/// A snapshot read surface.
pub trait KvSnapshot: Getter + BatchGetter<Error = <Self as Getter>::Error> {}

/// The storage operations required by [`InjectedStore`].
pub trait KvStorage {
    /// The storage error identity.
    type Error: BatchGetError;
    /// The transaction returned by [`KvStorage::begin`].
    type Transaction: KvTransaction + Getter<Error = Self::Error>;
    /// The snapshot returned by [`KvStorage::get_snapshot`].
    type Snapshot: KvSnapshot + Getter<Error = Self::Error>;

    /// Begins a transaction.
    fn begin(&self) -> Result<Self::Transaction, Self::Error>;
    /// Creates a snapshot at `version`.
    fn get_snapshot(&self, version: Version) -> Self::Snapshot;
}

#[derive(Debug)]
struct InjectionState<E> {
    get_error: Option<E>,
    commit_error: Option<E>,
}

impl<E> Default for InjectionState<E> {
    fn default() -> Self {
        Self {
            get_error: None,
            commit_error: None,
        }
    }
}

/// Shared, thread-safe injection configuration.
///
/// A clone shares state with all wrappers created from it, matching the Go
/// pointer to one `InjectionConfig`.  Passing `None` clears an error, matching
/// `SetGetError(nil)` and `SetCommitError(nil)`.
#[derive(Debug, Clone)]
pub struct InjectionConfig<E> {
    state: Arc<RwLock<InjectionState<E>>>,
}

impl<E> Default for InjectionConfig<E> {
    fn default() -> Self {
        Self {
            state: Arc::new(RwLock::new(InjectionState::default())),
        }
    }
}

impl<E: Clone> InjectionConfig<E> {
    /// Creates an empty configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets or clears the error returned by every get and batch-get wrapper.
    pub fn set_get_error(&self, error: Option<E>) {
        self.state
            .write()
            .expect("injection config lock poisoned")
            .get_error = error;
    }

    /// Sets or clears the error returned by every transaction commit wrapper.
    pub fn set_commit_error(&self, error: Option<E>) {
        self.state
            .write()
            .expect("injection config lock poisoned")
            .commit_error = error;
    }
}

/// A storage wrapper that injects errors into transactions and snapshots.
#[derive(Debug)]
pub struct InjectedStore<S: KvStorage> {
    storage: S,
    config: InjectionConfig<S::Error>,
}

/// Wraps a storage with a shared fault-injection configuration.
#[must_use]
pub fn new_injected_store<S>(storage: S, config: &InjectionConfig<S::Error>) -> InjectedStore<S>
where
    S: KvStorage,
    S::Error: Clone,
{
    InjectedStore {
        storage,
        config: config.clone(),
    }
}

impl<S> KvStorage for InjectedStore<S>
where
    S: KvStorage,
    S::Error: Clone,
{
    type Error = S::Error;
    type Transaction = InjectedTransaction<S::Transaction>;
    type Snapshot = InjectedSnapshot<S::Snapshot>;

    fn begin(&self) -> Result<Self::Transaction, Self::Error> {
        self.storage.begin().map(|transaction| InjectedTransaction {
            transaction,
            config: self.config.clone(),
        })
    }

    fn get_snapshot(&self, version: Version) -> Self::Snapshot {
        InjectedSnapshot {
            snapshot: self.storage.get_snapshot(version),
            config: self.config.clone(),
        }
    }
}

/// A transaction wrapper that injects get and commit failures.
pub struct InjectedTransaction<T: KvTransaction> {
    transaction: T,
    config: InjectionConfig<<T as Getter>::Error>,
}

impl<T> Getter for InjectedTransaction<T>
where
    T: KvTransaction,
{
    type Error = <T as Getter>::Error;

    fn get(&mut self, key: &Key, options: BatchGetOptions) -> Result<ValueEntry, Self::Error> {
        let state = self
            .config
            .state
            .read()
            .expect("injection config lock poisoned");
        match state.get_error.as_ref() {
            Some(error) => Err(error.clone()),
            None => self.transaction.get(key, options),
        }
    }
}

impl<T> BatchGetter for InjectedTransaction<T>
where
    T: KvTransaction,
{
    type Error = <T as Getter>::Error;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<std::collections::HashMap<Key, ValueEntry>, Self::Error> {
        let state = self
            .config
            .state
            .read()
            .expect("injection config lock poisoned");
        match state.get_error.as_ref() {
            Some(error) => Err(error.clone()),
            None => self.transaction.batch_get(keys, options),
        }
    }
}

impl<T> KvTransaction for InjectedTransaction<T>
where
    T: KvTransaction,
{
    fn commit(&mut self) -> Result<(), <Self as Getter>::Error> {
        let state = self
            .config
            .state
            .read()
            .expect("injection config lock poisoned");
        match state.commit_error.as_ref() {
            Some(error) => Err(error.clone()),
            None => self.transaction.commit(),
        }
    }
}

/// A snapshot wrapper that injects get failures.
pub struct InjectedSnapshot<S: KvSnapshot> {
    snapshot: S,
    config: InjectionConfig<<S as Getter>::Error>,
}

impl<S> Getter for InjectedSnapshot<S>
where
    S: KvSnapshot,
{
    type Error = <S as Getter>::Error;

    fn get(&mut self, key: &Key, options: BatchGetOptions) -> Result<ValueEntry, Self::Error> {
        let state = self
            .config
            .state
            .read()
            .expect("injection config lock poisoned");
        match state.get_error.as_ref() {
            Some(error) => Err(error.clone()),
            None => self.snapshot.get(key, options),
        }
    }
}

impl<S> BatchGetter for InjectedSnapshot<S>
where
    S: KvSnapshot,
{
    type Error = <S as Getter>::Error;

    fn batch_get(
        &mut self,
        keys: &[Key],
        options: BatchGetOptions,
    ) -> Result<std::collections::HashMap<Key, ValueEntry>, Self::Error> {
        let state = self
            .config
            .state
            .read()
            .expect("injection config lock poisoned");
        match state.get_error.as_ref() {
            Some(error) => Err(error.clone()),
            None => self.snapshot.batch_get(keys, options),
        }
    }
}

impl<S: KvSnapshot> KvSnapshot for InjectedSnapshot<S> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fmt;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl std::error::Error for TestError {}

    impl BatchGetError for TestError {
        fn is_not_found(&self) -> bool {
            self.0 == "not exist"
        }
    }

    #[derive(Debug)]
    struct TestTransaction {
        commit_error: Option<TestError>,
    }

    impl Getter for TestTransaction {
        type Error = TestError;

        fn get(
            &mut self,
            _key: &Key,
            _options: BatchGetOptions,
        ) -> Result<ValueEntry, Self::Error> {
            Ok(ValueEntry::default())
        }
    }

    impl BatchGetter for TestTransaction {
        type Error = TestError;

        fn batch_get(
            &mut self,
            keys: &[Key],
            _options: BatchGetOptions,
        ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
            Ok(keys
                .iter()
                .map(|key| (key.clone(), ValueEntry::new(key.as_bytes(), 0)))
                .collect())
        }
    }

    impl KvTransaction for TestTransaction {
        fn commit(&mut self) -> Result<(), <Self as Getter>::Error> {
            self.commit_error.take().map_or(Ok(()), Err)
        }
    }

    #[derive(Debug)]
    struct TestSnapshot;

    impl Getter for TestSnapshot {
        type Error = TestError;

        fn get(
            &mut self,
            _key: &Key,
            _options: BatchGetOptions,
        ) -> Result<ValueEntry, Self::Error> {
            Err(TestError("not exist"))
        }
    }

    impl BatchGetter for TestSnapshot {
        type Error = TestError;

        fn batch_get(
            &mut self,
            _keys: &[Key],
            _options: BatchGetOptions,
        ) -> Result<HashMap<Key, ValueEntry>, Self::Error> {
            Ok(HashMap::new())
        }
    }

    impl KvSnapshot for TestSnapshot {}

    #[derive(Debug)]
    struct TestStorage;

    impl KvStorage for TestStorage {
        type Error = TestError;
        type Transaction = TestTransaction;
        type Snapshot = TestSnapshot;

        fn begin(&self) -> Result<Self::Transaction, Self::Error> {
            Ok(TestTransaction {
                commit_error: Some(TestError("txn retryable")),
            })
        }

        fn get_snapshot(&self, _version: Version) -> Self::Snapshot {
            TestSnapshot
        }
    }

    #[test]
    fn fault_injection_matches_source_error_precedence_and_clear() {
        let config = InjectionConfig::new();
        let injected_error = TestError("foo");
        config.set_get_error(Some(injected_error.clone()));
        config.set_commit_error(Some(injected_error.clone()));

        let storage = new_injected_store(TestStorage, &config);
        let mut transaction = storage.begin().expect("begin succeeds");
        let _second_transaction = storage.begin().expect("repeated begin succeeds");
        let mut snapshot = storage.get_snapshot(Version::new(1));
        let key = Key::from_bytes(b"a");
        let options = BatchGetOptions::default();
        let empty_keys: &[Key] = &[];

        assert_eq!(transaction.get(&key, options), Err(injected_error.clone()));
        assert_eq!(snapshot.get(&key, options), Err(injected_error.clone()));
        assert_eq!(
            snapshot.batch_get(empty_keys, options),
            Err(injected_error.clone())
        );
        assert_eq!(
            transaction.batch_get(empty_keys, options),
            Err(injected_error.clone())
        );
        assert_eq!(transaction.commit(), Err(injected_error.clone()));

        config.set_get_error(None);
        config.set_commit_error(None);

        let storage = new_injected_store(TestStorage, &config);
        let mut transaction = storage.begin().expect("begin succeeds");
        let mut snapshot = storage.get_snapshot(Version::new(1));
        assert_eq!(transaction.get(&key, options), Ok(ValueEntry::default()));
        assert_eq!(
            transaction.batch_get(empty_keys, options),
            Ok(HashMap::new())
        );
        assert_eq!(snapshot.get(&key, options), Err(TestError("not exist")));
        assert_eq!(
            snapshot.batch_get(std::slice::from_ref(&key), options),
            Ok(HashMap::new())
        );
        assert_eq!(transaction.commit(), Err(TestError("txn retryable")));
    }
}
