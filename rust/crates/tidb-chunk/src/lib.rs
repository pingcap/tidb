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
//! SEED SCOPE (grown incrementally): [`column`] ports the `Column` columnar
//! storage (fixed-length int/real + variable-length string/bytes + the
//! `getFixedLen`/`NewColumn` type dispatch); [`chunk`] the `Chunk` batch; and
//! [`row`] the `Row` cursor that expression evaluation reads. DEFERRED
//! (documented per module): the typed appends/getters for
//! `VectorFloat32`, `Reset(EvalType)`, the growth/pool/disk paths,
//! and a `str`-typed `GetString`.
//!
//! [`mutrow`] adds Go's `MutRow`, the mutable one-row chunk that partition
//! pruning and ranger detachment evaluate expressions against.
//!
//! [`compare`] adds Go `compare.go` whole: `GetCompareFunc`, `Compare` against
//! a `Datum`, and `Chunk::lower_bound`/`upper_bound`. [`list`] and [`iterator`]
//! add Go `list.go`/`iterator.go` whole -- the unbounded in-memory chunk
//! sequence and the five iterators that do not need a `RowContainer`. Together
//! they are the in-memory half of what `row_container.go` will need.
//!
//! [`chunk_in_disk`] adds the spill-to-disk container `DataInDiskByChunks`
//! (Go `chunk_in_disk.go`) over the checksum-framed temporary file in
//! [`chunk_util`]; [`row_in_disk`] carries the read-while-writing reader it
//! needs.

pub mod chunk;
pub mod chunk_in_disk;
pub mod chunk_util;
pub mod codec;
pub mod column;
pub mod compare;
pub mod iterator;
pub mod list;
pub mod mutrow;
pub mod row;
pub mod row_container;
pub mod row_container_reader;
pub mod row_in_disk;

/// The spill tests all point the process-wide temporary-storage path at their
/// own scratch directory, so exactly one of them may run at a time. ONE lock
/// for the whole crate: a per-module lock does not serialise modules against
/// each other, which is how a test in one module used to delete the directory
/// another was writing into.
#[cfg(test)]
pub(crate) mod test_temp_storage {
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard, PoisonError};

    static LOCK: Mutex<()> = Mutex::new(());

    /// Held for the duration of a test that sets the temporary-storage path.
    pub(crate) fn guard() -> MutexGuard<'static, ()> {
        LOCK.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A fresh scratch directory named after the test.
    pub(crate) fn scratch_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tidb_rust_spill_test_{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch temp dir");
        dir
    }
}
