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

//! Prefix-scoped transaction utilities translated from
//! `pkg/util/prefix_helper.go`.

use std::ops::{Deref, DerefMut};

use crate::driver::read::{
    SnapshotInterceptor, TransactionBuffer, TransactionReadDriver, TransactionReadError,
    TransactionSnapshot,
};
use crate::{Getter, Key, KvIterator};

/// Scans the merged transaction view over exactly the supplied key prefix.
///
/// Returning `false` from `filter` stops successfully at the current key. The
/// iterator is closed on every exit after successful construction, matching
/// the source `defer iter.Close()` lifecycle.
pub fn scan_meta_with_prefix<B, S, I, F>(
    transaction: &mut TransactionReadDriver<B, S, I>,
    prefix: &Key,
    mut filter: F,
) -> Result<(), <B as Getter>::Error>
where
    B: TransactionBuffer,
    <B as Getter>::Error: TransactionReadError,
    S: TransactionSnapshot + Getter<Error = <B as Getter>::Error>,
    I: SnapshotInterceptor<S>,
    F: FnMut(&Key, &[u8]) -> bool,
{
    let upper_bound = prefix.prefix_next();
    let mut iterator = CloseOnDrop::new(transaction.iter(prefix, Some(&upper_bound))?);
    while iterator.valid() && iterator.key().has_prefix(prefix) {
        if !filter(iterator.key(), iterator.value()) {
            break;
        }
        iterator.next()?;
    }
    Ok(())
}

/// Deletes every key in the merged transaction view having `prefix`.
///
/// Keys are cloned during the scan and deleted only after the scan completes,
/// preserving Go's two-phase collect-before-mutate algorithm. Rust additionally
/// closes the iterator before applying the deletes through its lexical RAII
/// scope, and the dirty buffer receives canonical deletion tombstones through
/// [`TransactionReadDriver::delete`].
pub fn del_key_with_prefix<B, S, I>(
    transaction: &mut TransactionReadDriver<B, S, I>,
    prefix: &Key,
) -> Result<(), <B as Getter>::Error>
where
    B: TransactionBuffer,
    <B as Getter>::Error: TransactionReadError,
    S: TransactionSnapshot + Getter<Error = <B as Getter>::Error>,
    I: SnapshotInterceptor<S>,
{
    let keys = {
        let upper_bound = prefix.prefix_next();
        let mut iterator = CloseOnDrop::new(transaction.iter(prefix, Some(&upper_bound))?);
        let mut keys = Vec::new();
        while iterator.valid() && iterator.key().has_prefix(prefix) {
            keys.push(iterator.key().clone());
            iterator.next()?;
        }
        keys
    };

    for key in keys {
        transaction.delete(key)?;
    }
    Ok(())
}

struct CloseOnDrop<I: KvIterator>(I);

impl<I: KvIterator> CloseOnDrop<I> {
    fn new(iterator: I) -> Self {
        Self(iterator)
    }
}

impl<I: KvIterator> Deref for CloseOnDrop<I> {
    type Target = I;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<I: KvIterator> DerefMut for CloseOnDrop<I> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<I: KvIterator> Drop for CloseOnDrop<I> {
    fn drop(&mut self) {
        self.0.close();
    }
}
