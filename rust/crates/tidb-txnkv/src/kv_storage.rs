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

//! Go `pkg/kv`'s `Storage` and `Transaction`, the part a transaction of its
//! own (`kv.RunInNewTxn`) uses: begin at a fresh timestamp, read a key at the
//! transaction's start, commit a mutation set. The store driver implements
//! them; packages below it read and write meta through them, as Go's
//! `meta/autoid` does through `kv.Storage`.

use crate::rpc::UnaryCallContext;
use crate::transaction::{
    BufferMutation, OptimisticCommitOutcome, OptimisticCoordinatorError, ReadOnlyTransaction,
};

/// The rows served by one snapshot region.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotScanRegion {
    /// Region that served these rows.
    pub region: tikv_client::RegionVerId,
    /// Exclusive region boundary observed by the response.
    pub end_key: Vec<u8>,
    /// Rows from this serving region.
    pub pairs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Go `kv.Transaction`, narrowed to a transaction of its own.
pub trait Transaction: Send {
    /// Go `StartTS`.
    fn start_ts(&self) -> u64;

    /// Go `Get` at the transaction's start timestamp.
    fn get(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, OptimisticCoordinatorError>;

    /// Go `Iter(start, end)` drained, stopping after `limit` pairs when set.
    fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: Option<usize>,
        call: &UnaryCallContext,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, OptimisticCoordinatorError>;

    /// [`Transaction::scan`] of `[start, end)` grouped by the regions that
    /// served it.
    fn scan_regions(
        &mut self,
        start: &[u8],
        end: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Vec<SnapshotScanRegion>, OptimisticCoordinatorError>;

    /// Go `Commit` of `mutations`.
    fn commit(
        self: Box<Self>,
        mutations: Vec<BufferMutation>,
        call: &UnaryCallContext,
    ) -> Result<OptimisticCommitOutcome, OptimisticCoordinatorError>;

    /// Go `Rollback` of a transaction that wrote nothing.
    fn finish_without_writes(
        self: Box<Self>,
    ) -> Result<ReadOnlyTransaction, OptimisticCoordinatorError>;
}

/// Go `kv.Storage`, narrowed to `Begin` and `GetSnapshot`.
pub trait Storage: Send + Sync {
    /// Go `Begin`: a writable transaction at a fresh timestamp.
    fn begin(&self) -> Result<Box<dyn Transaction>, OptimisticCoordinatorError>;

    /// Go `GetSnapshot(CurrentVersion)`: a transaction at a fresh timestamp
    /// that may only read.
    fn begin_read_only(&self) -> Result<Box<dyn Transaction>, OptimisticCoordinatorError>;
}

/// A shared storage handle is the storage, as Go's `kv.Storage` interface
/// value is whatever pointer carries it.
impl<T: Storage + ?Sized> Storage for std::sync::Arc<T> {
    fn begin(&self) -> Result<Box<dyn Transaction>, OptimisticCoordinatorError> {
        (**self).begin()
    }

    fn begin_read_only(&self) -> Result<Box<dyn Transaction>, OptimisticCoordinatorError> {
        (**self).begin_read_only()
    }
}
