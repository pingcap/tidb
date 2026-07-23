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
//! Get and batch-get fail before reaching the wrapped object when a get error
//! is configured, commit does the same for a commit error, and every other
//! canonical storage/transaction/snapshot operation delegates unchanged.

use std::sync::{Arc, RwLock};

use crate::{
    BatchGetError, BatchGetOptions, BatchGetter, FairLockingController, GetOptions, Getter, Key,
    Mutator, Retriever, Snapshot, Storage, Transaction, ValueEntry, Version,
};

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
pub struct InjectedStore<S, E> {
    storage: S,
    config: InjectionConfig<E>,
}

/// Wraps a storage with a shared fault-injection configuration.
#[must_use]
pub fn new_injected_store<S>(
    storage: S,
    config: &InjectionConfig<<S as KvStorage>::Error>,
) -> InjectedStore<S, <S as KvStorage>::Error>
where
    S: KvStorage,
    <S as KvStorage>::Error: Clone,
{
    InjectedStore {
        storage,
        config: config.clone(),
    }
}

impl<S> KvStorage for InjectedStore<S, <S as KvStorage>::Error>
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
pub struct InjectedTransaction<T: Getter> {
    transaction: T,
    config: InjectionConfig<<T as Getter>::Error>,
}

impl<T> Getter for InjectedTransaction<T>
where
    T: Getter,
    <T as Getter>::Error: Clone,
{
    type Error = <T as Getter>::Error;

    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error> {
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
    T: Getter + BatchGetter<Error = <T as Getter>::Error>,
    <T as Getter>::Error: Clone,
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
pub struct InjectedSnapshot<S: Getter> {
    snapshot: S,
    config: InjectionConfig<<S as Getter>::Error>,
}

impl<S> Getter for InjectedSnapshot<S>
where
    S: Getter,
    <S as Getter>::Error: Clone,
{
    type Error = <S as Getter>::Error;

    fn get(&mut self, key: &Key, options: GetOptions) -> Result<ValueEntry, Self::Error> {
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
    S: Getter + BatchGetter<Error = <S as Getter>::Error>,
    <S as Getter>::Error: Clone,
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

impl<S> KvSnapshot for InjectedSnapshot<S>
where
    S: KvSnapshot,
    <S as Getter>::Error: Clone,
{
}

/// Wraps the complete canonical storage interface.
#[must_use]
pub fn new_injected_storage<S>(
    storage: S,
    config: &InjectionConfig<<<S as Storage>::Transaction as Getter>::Error>,
) -> InjectedStore<S, <<S as Storage>::Transaction as Getter>::Error>
where
    S: Storage,
    <S as Storage>::Snapshot: Getter<Error = <<S as Storage>::Transaction as Getter>::Error>,
    <<S as Storage>::Transaction as Getter>::Error: Clone,
{
    InjectedStore {
        storage,
        config: config.clone(),
    }
}

impl<T> Retriever for InjectedTransaction<T>
where
    T: Transaction,
    <T as Getter>::Error: Clone,
{
    type Iterator = <T as Retriever>::Iterator;

    fn iter(
        &mut self,
        key: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        self.transaction.iter(key, upper_bound)
    }

    fn iter_reverse(
        &mut self,
        key: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        self.transaction.iter_reverse(key, lower_bound)
    }
}

impl<T> Mutator for InjectedTransaction<T>
where
    T: Transaction,
    <T as Getter>::Error: Clone,
{
    type Error = <T as Getter>::Error;

    fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), Self::Error> {
        self.transaction.set(key, value)
    }

    fn delete(&mut self, key: Key) -> Result<(), Self::Error> {
        self.transaction.delete(key)
    }
}

impl<T> FairLockingController for InjectedTransaction<T>
where
    T: Transaction,
    <T as Getter>::Error: Clone,
{
    type Context = <T as FairLockingController>::Context;
    type Error = <T as Getter>::Error;

    fn start_fair_locking(&mut self) -> Result<(), Self::Error> {
        self.transaction.start_fair_locking()
    }

    fn retry_fair_locking(&mut self, context: &Self::Context) -> Result<(), Self::Error> {
        self.transaction.retry_fair_locking(context)
    }

    fn cancel_fair_locking(&mut self, context: &Self::Context) -> Result<(), Self::Error> {
        self.transaction.cancel_fair_locking(context)
    }

    fn done_fair_locking(&mut self, context: &Self::Context) -> Result<(), Self::Error> {
        self.transaction.done_fair_locking(context)
    }

    fn is_in_fair_locking_mode(&self) -> bool {
        self.transaction.is_in_fair_locking_mode()
    }
}

impl<T> Transaction for InjectedTransaction<T>
where
    T: Transaction,
    <T as Getter>::Error: Clone,
{
    type LockContext = T::LockContext;
    type OptionValue = T::OptionValue;
    type Variables = T::Variables;
    type TableInfo = T::TableInfo;
    type DiskFullOption = T::DiskFullOption;
    type Checkpoint = T::Checkpoint;
    type Buffer = T::Buffer;
    type Snapshot = T::Snapshot;

    fn size(&self) -> usize {
        self.transaction.size()
    }

    fn memory_usage(&self) -> u64 {
        self.transaction.memory_usage()
    }

    fn set_memory_footprint_hook(&mut self, hook: Box<dyn FnMut(u64) + Send>) {
        self.transaction.set_memory_footprint_hook(hook);
    }

    fn memory_hook_is_set(&self) -> bool {
        self.transaction.memory_hook_is_set()
    }

    fn len(&self) -> usize {
        self.transaction.len()
    }

    fn commit(
        &mut self,
        context: &<Self as FairLockingController>::Context,
    ) -> Result<(), <Self as Getter>::Error> {
        let state = self
            .config
            .state
            .read()
            .expect("injection config lock poisoned");
        match state.commit_error.as_ref() {
            Some(error) => Err(error.clone()),
            None => self.transaction.commit(context),
        }
    }

    fn rollback(&mut self) -> Result<(), <Self as Getter>::Error> {
        self.transaction.rollback()
    }

    fn diagnostic_string(&self) -> String {
        self.transaction.diagnostic_string()
    }

    fn lock_keys(
        &mut self,
        context: &<Self as FairLockingController>::Context,
        lock_context: &mut Self::LockContext,
        keys: &[Key],
    ) -> Result<(), <Self as Getter>::Error> {
        self.transaction.lock_keys(context, lock_context, keys)
    }

    fn lock_keys_with(
        &mut self,
        context: &<Self as FairLockingController>::Context,
        lock_context: &mut Self::LockContext,
        before_unlock: &mut dyn FnMut(),
        keys: &[Key],
    ) -> Result<(), <Self as Getter>::Error> {
        self.transaction
            .lock_keys_with(context, lock_context, before_unlock, keys)
    }

    fn set_option(&mut self, option: crate::OptionKey, value: Option<Self::OptionValue>) {
        self.transaction.set_option(option, value);
    }

    fn option(&self, option: crate::OptionKey) -> Option<&Self::OptionValue> {
        self.transaction.option(option)
    }

    fn is_read_only(&self) -> bool {
        self.transaction.is_read_only()
    }

    fn start_ts(&self) -> u64 {
        self.transaction.start_ts()
    }

    fn commit_ts(&self) -> u64 {
        self.transaction.commit_ts()
    }

    fn valid(&self) -> bool {
        self.transaction.valid()
    }

    fn mem_buffer(&mut self) -> &mut Self::Buffer {
        self.transaction.mem_buffer()
    }

    fn snapshot(&mut self) -> &mut Self::Snapshot {
        self.transaction.snapshot()
    }

    fn set_variables(&mut self, variables: Self::Variables) {
        self.transaction.set_variables(variables);
    }

    fn variables(&self) -> &Self::Variables {
        self.transaction.variables()
    }

    fn is_pessimistic(&self) -> bool {
        self.transaction.is_pessimistic()
    }

    fn cache_table_info(&mut self, id: i64, info: Self::TableInfo) {
        self.transaction.cache_table_info(id, info);
    }

    fn table_info(&self, id: i64) -> Option<&Self::TableInfo> {
        self.transaction.table_info(id)
    }

    fn set_disk_full_option(&mut self, option: Self::DiskFullOption) {
        self.transaction.set_disk_full_option(option);
    }

    fn clear_disk_full_option(&mut self) {
        self.transaction.clear_disk_full_option();
    }

    fn mem_db_checkpoint(&self) -> Self::Checkpoint {
        self.transaction.mem_db_checkpoint()
    }

    fn rollback_mem_db_to_checkpoint(&mut self, checkpoint: &Self::Checkpoint) {
        self.transaction.rollback_mem_db_to_checkpoint(checkpoint);
    }

    fn is_pipelined(&self) -> bool {
        self.transaction.is_pipelined()
    }

    fn may_flush(&mut self) -> Result<(), <Self as Getter>::Error> {
        self.transaction.may_flush()
    }
}

impl<S> Retriever for InjectedSnapshot<S>
where
    S: Snapshot,
    <S as Getter>::Error: Clone,
{
    type Iterator = <S as Retriever>::Iterator;

    fn iter(
        &mut self,
        key: Option<&Key>,
        upper_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        self.snapshot.iter(key, upper_bound)
    }

    fn iter_reverse(
        &mut self,
        key: Option<&Key>,
        lower_bound: Option<&Key>,
    ) -> Result<Self::Iterator, <Self as Getter>::Error> {
        self.snapshot.iter_reverse(key, lower_bound)
    }
}

impl<S> Snapshot for InjectedSnapshot<S>
where
    S: Snapshot,
    <S as Getter>::Error: Clone,
{
    type OptionValue = S::OptionValue;

    fn set_option(&mut self, option: crate::OptionKey, value: Option<Self::OptionValue>) {
        self.snapshot.set_option(option, value);
    }
}

impl<S> Storage for InjectedStore<S, <<S as Storage>::Transaction as Getter>::Error>
where
    S: Storage,
    <S as Storage>::Snapshot: Getter<Error = <<S as Storage>::Transaction as Getter>::Error>,
    <<S as Storage>::Transaction as Getter>::Error: Clone,
{
    type Error = S::Error;
    type TransactionOption = S::TransactionOption;
    type Transaction = InjectedTransaction<S::Transaction>;
    type Snapshot = InjectedSnapshot<S::Snapshot>;
    type Client = S::Client;
    type MppClient = S::MppClient;
    type Oracle = S::Oracle;
    type Status = S::Status;
    type LockWait = S::LockWait;
    type Codec = S::Codec;
    type OptionKey = S::OptionKey;
    type OptionValue = S::OptionValue;
    type Context = S::Context;

    fn begin(
        &mut self,
        options: &[Self::TransactionOption],
    ) -> Result<Self::Transaction, Self::Error> {
        self.storage
            .begin(options)
            .map(|transaction| InjectedTransaction {
                transaction,
                config: self.config.clone(),
            })
    }

    fn snapshot(&self, version: Version) -> Self::Snapshot {
        InjectedSnapshot {
            snapshot: self.storage.snapshot(version),
            config: self.config.clone(),
        }
    }

    fn client(&self) -> &Self::Client {
        self.storage.client()
    }

    fn mpp_client(&self) -> &Self::MppClient {
        self.storage.mpp_client()
    }

    fn close(&mut self) -> Result<(), Self::Error> {
        self.storage.close()
    }

    fn uuid(&self) -> &str {
        self.storage.uuid()
    }

    fn current_version(&self, txn_scope: &str) -> Result<Version, Self::Error> {
        self.storage.current_version(txn_scope)
    }

    fn oracle(&self) -> &Self::Oracle {
        self.storage.oracle()
    }

    fn supports_delete_range(&self) -> bool {
        self.storage.supports_delete_range()
    }

    fn name(&self) -> &str {
        self.storage.name()
    }

    fn describe(&self) -> &str {
        self.storage.describe()
    }

    fn show_status(&self, context: &Self::Context, key: &str) -> Result<Self::Status, Self::Error> {
        self.storage.show_status(context, key)
    }

    fn memory_cache(&self) -> &crate::CacheDb {
        self.storage.memory_cache()
    }

    fn min_safe_ts(&self, txn_scope: &str) -> u64 {
        self.storage.min_safe_ts(txn_scope)
    }

    fn lock_waits(&self) -> Result<Vec<Self::LockWait>, Self::Error> {
        self.storage.lock_waits()
    }

    fn codec(&self) -> &Self::Codec {
        self.storage.codec()
    }

    fn set_storage_option(&mut self, key: Self::OptionKey, value: Self::OptionValue) {
        self.storage.set_storage_option(key, value);
    }

    fn storage_option(&self, key: &Self::OptionKey) -> Option<&Self::OptionValue> {
        self.storage.storage_option(key)
    }

    fn cluster_id(&self) -> u64 {
        self.storage.cluster_id()
    }

    fn keyspace(&self) -> &str {
        self.storage.keyspace()
    }
}

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

        fn get(&mut self, _key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
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

        fn get(&mut self, _key: &Key, _options: GetOptions) -> Result<ValueEntry, Self::Error> {
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
        let get_options = GetOptions::default();
        let batch_get_options = BatchGetOptions::default();
        let empty_keys: &[Key] = &[];

        assert_eq!(
            transaction.get(&key, get_options),
            Err(injected_error.clone())
        );
        assert_eq!(snapshot.get(&key, get_options), Err(injected_error.clone()));
        assert_eq!(
            snapshot.batch_get(empty_keys, batch_get_options),
            Err(injected_error.clone())
        );
        assert_eq!(
            transaction.batch_get(empty_keys, batch_get_options),
            Err(injected_error.clone())
        );
        assert_eq!(transaction.commit(), Err(injected_error.clone()));

        config.set_get_error(None);
        config.set_commit_error(None);

        let storage = new_injected_store(TestStorage, &config);
        let mut transaction = storage.begin().expect("begin succeeds");
        let mut snapshot = storage.get_snapshot(Version::new(1));
        assert_eq!(
            transaction.get(&key, get_options),
            Ok(ValueEntry::default())
        );
        assert_eq!(
            transaction.batch_get(empty_keys, batch_get_options),
            Ok(HashMap::new())
        );
        assert_eq!(snapshot.get(&key, get_options), Err(TestError("not exist")));
        assert_eq!(
            snapshot.batch_get(std::slice::from_ref(&key), batch_get_options),
            Ok(HashMap::new())
        );
        assert_eq!(transaction.commit(), Err(TestError("txn retryable")));
    }
}
