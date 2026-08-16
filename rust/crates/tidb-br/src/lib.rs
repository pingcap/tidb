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

//! BR's key-range algebra: the interval maps, range trees, and prefix rewrites
//! that backup and restore reason about key space with.
//!
//! Three Go packages land here, each as one Rust module, each a *complete
//! package*:
//!
//! | Rust module | Go package | Labeling |
//! | --- | --- | --- |
//! | [`spans`] | `br/pkg/streamhelper/spans` | complete package |
//! | [`rtree`] | `br/pkg/rtree` | complete package |
//! | [`restore_utils`] | `br/pkg/restore/utils` | complete package |
//!
//! Every production symbol of each Go package is present, and every Go test
//! function is ported; the two Go benchmark sets and the one Go fuzz target
//! are accounted for at their own definition sites (`BenchmarkRangeTreeUpdate`
//! and `BenchmarkMergeRanges*` as named `#[ignore]` tests, `FuzzMerge` as a
//! table-driven test over its `f.Add` seed corpus).
//!
//! They are one crate because they are one subject and they depend on each
//! other in exactly that order: `restore_utils` rewrites the [`rtree::Range`]s
//! that a backup produced, and `spans` is the same interval reasoning applied
//! to log backup's resolved timestamps.
//!
//! # Dependencies and boundaries
//!
//! No external crate is used; the only dependencies are workspace paths
//! (`tidb-codec`, `tidb-model`, `tidb-util`). Go's `github.com/google/btree`
//! maps onto [`std::collections::BTreeMap`] throughout.
//!
//! Go's `pkg/tablecodec` import is served by `tidb-codec`'s `table_key` and
//! `row_index` modules, which is where this workspace put the table-key framing
//! (`tidb-tablecodec` itself re-exports `table_key` from there); depending on
//! `tidb-codec` directly avoids reaching for the same functions through two
//! different crate paths.
//!
//! Each module header names the boundaries it declares. In summary:
//!
//! - kvproto (`brpb.File`, `import_sstpb.RewriteRule`, `kvrpcpb.KeyRange`) is
//!   never linked. [`rtree::Range`] is generic over its file payload; the two
//!   flat message shapes this subject reads are declared in
//!   [`restore_utils::proto`]; `kvrpcpb.KeyRange` becomes [`rtree::KeyRange`].
//! - `br/pkg/metautil` (`MetaWriter`, `ChecksumStats`, `Table`) is narrowed to
//!   the [`rtree::MetaSink`] trait, the flat [`rtree::ChecksumStats`] struct,
//!   and an opaque payload on [`restore_utils::CreatedTable`].
//! - `br/pkg/logutil` and `go.uber.org/zap` carry no semantics; the two
//!   renderings that tests assert on are reproduced directly
//!   ([`rtree::zap_ranges`], [`spans::stringify_range`]).
//! - `pkg/kv`'s `KeyRange` is taken from `tidb-util`'s already-landed
//!   `br/pkg/utils/key.go` port rather than by depending upward on
//!   `tidb-txnkv`, which would invert the layering for a two-field struct.
//! - `client-go`'s API-V2 keyspace codec is a four-byte prefix split, done
//!   inline in [`rtree`].
//! - `br/pkg/errors`' normalized sentinels become
//!   [`restore_utils::RestoreErrorKind`] and [`rtree::RtreeError`].

pub mod restore_utils;
pub mod rtree;
pub mod spans;
