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

/// Go `kv.Transaction`, narrowed to a transaction of its own.
pub trait Transaction: Send {
    /// Go `Get` at the transaction's start timestamp.
    fn get(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, OptimisticCoordinatorError>;

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

/// Go `kv.Storage`, narrowed to `Begin`.
pub trait Storage: Send + Sync {
    /// Go `Begin`: a writable transaction at a fresh timestamp.
    fn begin(&self) -> Result<Box<dyn Transaction>, OptimisticCoordinatorError>;
}
