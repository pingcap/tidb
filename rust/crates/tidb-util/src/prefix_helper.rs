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

//! Go `pkg/util/prefix_helper.go`: prefix-scoped scans and deletes over a
//! `kv.Retriever` / `kv.RetrieverMutator`.

use std::ops::{Deref, DerefMut};

use tidb_txnkv::{Getter, Key, KvIterator, Retriever, RetrieverMutator};

/// Go `ScanMetaWithPrefix`: scans `retriever` over exactly the supplied key
/// prefix.
///
/// Returning `false` from `filter` stops successfully at the current key. The
/// iterator is closed on every exit after successful construction, matching
/// the source `defer iter.Close()` lifecycle.
pub fn scan_meta_with_prefix<R, F>(
    retriever: &mut R,
    prefix: &Key,
    mut filter: F,
) -> Result<(), <R as Getter>::Error>
where
    R: Retriever,
    F: FnMut(&Key, &[u8]) -> bool,
{
    let upper_bound = prefix.prefix_next();
    let mut iterator = CloseOnDrop::new(retriever.iter(Some(prefix), Some(&upper_bound))?);
    while iterator.valid() && iterator.key().has_prefix(prefix) {
        if !filter(iterator.key(), iterator.value()) {
            break;
        }
        iterator.next()?;
    }
    Ok(())
}

/// Go `DelKeyWithPrefix`: deletes every key of `rm` having `prefix`.
///
/// Keys are cloned during the scan and deleted only after the scan completes,
/// preserving Go's two-phase collect-before-mutate algorithm. Rust additionally
/// closes the iterator before applying the deletes through its lexical RAII
/// scope.
pub fn del_key_with_prefix<R>(rm: &mut R, prefix: &Key) -> Result<(), <R as Getter>::Error>
where
    R: RetrieverMutator,
{
    let keys = {
        let upper_bound = prefix.prefix_next();
        let mut iterator = CloseOnDrop::new(rm.iter(Some(prefix), Some(&upper_bound))?);
        let mut keys = Vec::new();
        while iterator.valid() && iterator.key().has_prefix(prefix) {
            keys.push(iterator.key().clone());
            iterator.next()?;
        }
        keys
    };

    for key in keys {
        rm.delete(key)?;
    }
    Ok(())
}

/// Go `RowKeyPrefixFilter`: keys inside the captured prefix return `false`,
/// while the first key outside it returns `true`. The result composes
/// directly with [`tidb_txnkv::next_until`].
pub fn row_key_prefix_filter(row_key_prefix: Key) -> impl Fn(&Key) -> bool {
    move |current_key| !current_key.has_prefix(&row_key_prefix)
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
