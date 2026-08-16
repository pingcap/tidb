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

//! Go `br/pkg/streamhelper/spans` lands as a complete package: the interval map
//! that log backup uses to track a resolved timestamp per sub key space, plus
//! the value-sorted index that answers "which span is lagging furthest behind".
//!
//! File mapping (one Rust module per Go file):
//! - [`sorted`] <- `sorted.go`
//! - [`utils`] <- `utils.go`
//! - [`value_sorted`] <- `value_sorted.go`
//!
//! The whole package is one idea: a [`ValuedFull`] partitions a key space into
//! adjacent, non-overlapping [`Span`]s, each carrying a [`Value`].
//! [`ValuedFull::merge`] writes a new value over an arbitrary interval, and the
//! overlap is resolved by `join` — the *upper bound* of the two values, so a
//! merge never lowers a span that is already further ahead. Adjacent spans that
//! end up with the same value are re-fused, which is why the tree stays small
//! under a stream of small updates.
//!
//! # Narrowings and boundaries
//!
//! - `github.com/google/btree` becomes [`std::collections::BTreeMap`]. Go's
//!   `Valued.Less` orders solely by `Key.StartKey`, so the start key *is* the
//!   map key; `btree.Item` identity and `ReplaceOrInsert`/`Delete` map onto
//!   `insert`/`remove` unchanged. `value_sorted.go`'s second index orders by
//!   `(Value, StartKey)`, which becomes the tuple key of a second map.
//! - boundary: Go's `Span = kv.KeyRange` is spelled here as
//!   [`tidb_util::br_key_utils::KeyRange`], the two-field
//!   `{start_key, end_key}` range that `br/pkg/utils/key.go` already landed
//!   with. `pkg/kv` sits *above* a leaf BR crate in the Rust workspace, so
//!   importing `tidb-txnkv` for a two-field struct would invert the layering.
//!   The struct is field-identical and the same `compare_bytes_ext` operates
//!   on it, so nothing observable changes.
//! - boundary: `br/pkg/utils.CompareBytesExt` is already landed as
//!   [`tidb_util::br_key_utils::compare_bytes_ext`] and is used directly.
//! - boundary: `br/pkg/logutil.StringifyRange` is not pulled in as a zap
//!   helper; its exact rendering (`[<hex start>, <hex end or "inf">)` through
//!   `redact.Key`/`redact.Value`) is reproduced by [`utils::stringify_range`],
//!   because this package only ever uses it inside `Valued.String`.
//! - Go's `ValuedSetEquals` sorts its two argument slices *in place*. Rust
//!   takes them by shared slice and sorts private copies: no caller in Go or
//!   in the tests depends on the post-call ordering, and the comparison result
//!   is identical.
//! - `Debug` writes to stdout with Go's `%s`-over-slice rendering
//!   (`[(a, 1) (b, 2)]`); it is a debugging aid with no return value, so the
//!   Rust version prints the same shape.

pub mod sorted;
pub mod utils;
pub mod value_sorted;

pub use sorted::{Span, Value, Valued, ValuedFull};
pub use utils::{collapse, full, overlaps, stringify_range, valued_set_equals};
pub use value_sorted::{debug, sorted as value_sorted_wrap, ValueSortedFull};
