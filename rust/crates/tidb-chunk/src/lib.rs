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

//! `pkg/util/chunk`: the columnar row container that every executor produces
//! and every expression `Eval*` consumes.
//!
//! [`column`] provides packed fixed- and variable-width storage, guarded byte
//! views, resize/reserve/reset operations, and the typed values consumed by
//! expression evaluation. [`chunk`] owns a batch of columns and [`row`] is its
//! read cursor. Go strings remain byte-authoritative and are represented by
//! `tidb_datatype::GoString` at the value boundary.
//!
//! [`mutrow`] adds Go's `MutRow`, the mutable one-row chunk that partition
//! pruning and ranger detachment evaluate expressions against.
//!
//! [`compare`] adds Go `compare.go` whole: `GetCompareFunc`, `Compare` against
//! a `Datum`, and `Chunk::lower_bound`/`upper_bound`. [`list`] and [`iterator`]
//! add Go `list.go`/`iterator.go` whole -- the unbounded in-memory chunk
//! sequence and the five standalone iterators. [`row_container`] adds the
//! shared in-memory/disk storage root and quota spill action, while
//! [`row_container_reader`] provides its chunk-at-a-time forward reader.
//!
//! [`chunk_in_disk`] adds the spill-to-disk container `DataInDiskByChunks`
//! (Go `chunk_in_disk.go`) over the checksum-framed temporary file in
//! [`chunk_util`]; [`row_in_disk`] carries the read-while-writing reader it
//! needs.

pub mod alloc;
pub mod chunk;
pub mod chunk_in_disk;
pub mod chunk_util;
pub mod codec;
pub mod column;
mod column_slot;
mod column_view;
pub mod compare;
pub mod iterator;
pub mod list;
pub mod mutrow;
pub mod pool;
pub mod row;
pub mod row_container;
pub mod row_container_reader;
pub mod row_in_disk;
mod shared_bytes;
pub mod sorted_row_container;

#[cfg(test)]
mod chunk_identity_tests;
#[cfg(test)]
mod tests_alloc;
#[cfg(test)]
mod tests_chunk_util;
#[cfg(test)]
mod tests_codec;
#[cfg(test)]
mod tests_iterator;
#[cfg(test)]
mod tests_list;
#[cfg(test)]
mod tests_mutrow;
#[cfg(test)]
mod tests_pool;

pub use column_slot::{ColumnHandle, ColumnRead, ColumnWrite};
pub use column_view::{CellBytes, ColumnBytes};

/// The spill tests all point the process-wide temporary-storage path at their
/// own scratch directory, so exactly one of them may run at a time. ONE lock
/// for the whole crate: a per-module lock does not serialise modules against
/// each other, which is how a test in one module used to delete the directory
/// another was writing into.
#[cfg(test)]
pub(crate) mod test_temp_storage {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, OnceLock};

    use tidb_util::disk::{SpillEncryptionMethod, SpillStorage, SpillStorageSpec};

    /// One plaintext authority shared by ordinary unit tests. Production never
    /// has an implicit process fallback; this exists only to keep test setup
    /// focused on the chunk behavior under test.
    pub(crate) fn storage() -> Arc<SpillStorage> {
        static STORAGE: OnceLock<Arc<SpillStorage>> = OnceLock::new();
        Arc::clone(
            STORAGE.get_or_init(|| isolated_storage("shared", SpillEncryptionMethod::Plaintext)),
        )
    }

    /// A separately configured authority for path/encryption-sensitive tests.
    pub(crate) fn isolated_storage(
        name: &str,
        encryption: SpillEncryptionMethod,
    ) -> Arc<SpillStorage> {
        isolated_storage_with_quota(name, encryption, -1)
    }

    /// A separately configured authority with an exact quota.
    pub(crate) fn isolated_storage_with_quota(
        name: &str,
        encryption: SpillEncryptionMethod,
        quota_bytes: i64,
    ) -> Arc<SpillStorage> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let ordinal = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "tidb_rust_spill_test_{}_{}_{}_{}",
            std::process::id(),
            name,
            ordinal,
            encryption.as_config_value()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        Arc::new(
            SpillStorage::open(SpillStorageSpec {
                path: dir,
                quota_bytes,
                encryption,
            })
            .expect("test spill storage"),
        )
    }
}
