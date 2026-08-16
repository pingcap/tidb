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

//! Unistore's in-memory lock machinery: two Go packages land here as complete
//! packages.
//!
//! - `pkg/store/mockstore/unistore/lockstore` — the arena-backed skiplist
//!   unistore keeps its in-memory locks in.
//! - `pkg/store/mockstore/unistore/util/lockwaiter` — the manager that parks a
//!   pessimistic-lock waiter until the lock holder commits, the deadlock
//!   detector answers, or the wait times out. See [`lockwaiter`] for its own
//!   boundaries; the rest of this header is about `lockstore`.
//!
//! Neither Go package has internal TiDB imports beyond a config struct, and no
//! external ones beyond logging, error wrapping and one kvproto message; this
//! crate has no dependencies at all. `lockstore` is a self-contained ordered
//! map from byte string to byte string, with a bounded memory footprint: nodes
//! are carved out of fixed-size arena blocks and drained blocks are recycled
//! rather than dropped.
//!
//! File mapping (one Rust module per Go file):
//! - [`lockstore`] <- `lockstore.go`
//! - [`arena`] <- `arena.go`
//! - [`iterator`] <- `iterator.go`
//! - [`load_dump`] <- `load_dump.go`
//! - [`lockwaiter`] <- `util/lockwaiter/lockwaiter.go`
//!
//! # The one structural narrowing: no `unsafe`
//!
//! Go implements this with raw pointer arithmetic. A node is a variable-length
//! struct overlaid directly on arena bytes; a link is a `*node` reconstructed
//! from an `unsafe.Pointer`; a level's next pointer is reached by adding
//! `idx * 8` to the address of a struct field; and readers walk the whole list
//! with no lock, publishing and observing links through `atomic.LoadUint64` /
//! `atomic.StoreUint64` while a single writer mutates.
//!
//! This workspace forbids `unsafe`, so:
//!
//! - **Addresses replace pointers.** Every link is an
//!   [`arena::ArenaAddr`] — Go's own `blockIdx+1 << 32 | blockOffset`
//!   packing, unchanged — and node fields are read out of the block's
//!   `Vec<u8>` with explicit little-endian decoding. The byte layout is Go's
//!   exactly (see [`lockstore`]'s module docs), because node size decides when
//!   an arena block overflows and Go's own tests assert on the resulting block
//!   counts.
//! - **Exclusivity replaces atomics.** `&mut self` for writes and `&self` for
//!   reads gives the compiler the single-writer/many-reader invariant Go
//!   documents in prose, so the link loads and stores are plain reads and
//!   writes. Callers that share the store across threads wrap it exactly as
//!   Go's own `TestMemStoreConcurrent` does — in an `RwLock`.
//! - **In-place growth replaces arena swapping.** Go allocates a whole new
//!   `arena` and swaps a pointer to it so a lock-free reader mid-walk keeps a
//!   consistent block slice; [`arena::Arena::grow`] appends in place, which is
//!   observationally identical once no reader can be mid-walk.
//!
//! What is therefore **not** claimed: Go's memory layout in the C sense. There
//! are no raw pointers, no structs overlaid on memory, and no lock-free
//! reader. [`arena::REUSE_SAFE_DURATION`], the delay Go needs so a lock-free
//! reader cannot observe a recycled block, is nonetheless kept — it decides
//! when block reuse begins and so is visible in the arena block count that
//! Go's `TestMemStore` asserts on.
//!
//! Everything else is observable behavior and is reproduced: key ordering,
//! `Get`/`Put`/`Delete`/`replace` results, hint-based splice reuse, iterator
//! seek/next/prev/valid, arena block accounting and delayed reuse, and the
//! dump file format.
//!
//! # Other narrowings
//!
//! - [`lockstore::MemStore::with_seed`] is not a Go constructor. Go always
//!   seeds node heights from `time.Now().Unix()` with `math/rand`; no
//!   random-number crate is reachable offline, so heights come from splitmix64
//!   with the same p = 1/4 geometric distribution, and the extra constructor
//!   lets a test fix the list's shape. Individual node heights are not
//!   observable through any API.
//! - `Get` takes the caller's reusable buffer as `&mut Vec<u8>` and returns
//!   `Option<&[u8]>`, where Go appends into a `[]byte` and returns `nil` on a
//!   miss.
//! - Go's `LoadFromFile` swallows its own errors through a `defer` that
//!   overwrites the named return; see [`lockstore::MemStore::load_from_file`].
//!
//! # Not ported
//!
//! `lockstore_test.go`'s four benchmarks — `BenchmarkMemStoreDeleteInsertGet`,
//! `BenchmarkMemStoreIterate`, `BenchmarkPutWithHint`, and `BenchmarkPut` —
//! are skipped. They are timing harnesses, not tests: each measures the same
//! `Put`/`Delete`/`Get`/iterate calls the ported tests already assert on, and
//! `cargo bench` on stable has no counterpart to `testing.B`.
//!
//! `main_test.go` is likewise skipped: it is `goleak` plus TiDB's global
//! `testsetup` for the Go test binary, neither of which has an analogue here.

pub mod arena;
pub mod iterator;
pub mod load_dump;
pub mod lockstore;
pub mod lockwaiter;
#[cfg(test)]
mod testutil;

pub use arena::{Arena, ArenaAddr, NULL_ARENA_ADDR, REUSE_SAFE_DURATION};
pub use iterator::Iterator;
pub use lockstore::{Hint, MemStore, MAX_HEIGHT, NODE_HEADER_SIZE};
