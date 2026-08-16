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

//! Go `br/pkg/rtree` lands as a complete package: the non-overlapping range
//! trees BR uses to record which key ranges a backup has already covered, to
//! compute the gaps that still need requesting, and to fuse small adjacent
//! ranges into region-sized chunks before a restore splits regions.
//!
//! File mapping (one Rust module per Go file):
//! - [`rtree`] <- `rtree.go`
//! - [`logging`] <- `logging.go`
//!
//! # Narrowings and boundaries
//!
//! - KEY NARROWING: Go's `Range` carries `Files []*backuppb.File`, a kvproto
//!   message. Every use inside this package reads exactly three scalars from
//!   it (`TotalKvs`, `TotalBytes`, `Crc64Xor`) and otherwise moves the slice
//!   around untouched. [`Range`] is therefore generic over its payload type
//!   `F: RangeFile`, and [`RangeFile`] is that three-method contract. No
//!   protobuf dependency crosses into this crate; the concrete `brpb.File`
//!   shape is declared once, next to the code that actually needs its other
//!   fields, in [`crate::restore_utils::File`].
//! - boundary: `br/pkg/metautil`'s `MetaWriter` reaches object storage and
//!   serializes backup metafiles — entirely outside this package's subject.
//!   `ProgressRangeTree` only ever calls `Send(files, AppendDataFile)` on it,
//!   so it is narrowed to the one-method [`MetaSink`] trait object.
//!   `metautil.ChecksumStats` is a flat three-`uint64` struct and is declared
//!   locally as [`ChecksumStats`].
//! - boundary: `GetIncompleteRange`/`GetIncompleteRanges` return
//!   `[]*kvrpcpb.KeyRange` in Go — another kvproto message with the same two
//!   byte fields as this package's own [`KeyRange`]. They return [`KeyRange`]
//!   here.
//! - boundary: `NeedsMerge` trims an API-V2 keyspace prefix through
//!   `tikv.DecodeKey` from `client-go`. That call is a four-byte split guarded
//!   by the mode byte (`'x'` txn / `'r'` raw), which
//!   [`rtree::decode_keyspace_key`] performs directly; no TiKV client comes
//!   across. The same constants are visible in Go at
//!   `pkg/util/rowcodec/common.go` (`keyspacePrefixLen = 4`,
//!   `apiV2TxnModePrefix = 'x'`).
//! - `github.com/google/btree`'s `BTreeG[T]` with a `Less` on `StartKey`
//!   becomes [`std::collections::BTreeMap`] keyed by the start key.
//!   `NewRangeTreeWithFreeListG`'s `FreeListG` is a Go allocation-reuse knob
//!   with no observable behavior, so only its `physicalID` argument survives,
//!   as [`rtree::RangeTree::new_with_physical_id`].
//! - `logging.go`'s `ZapRanges` builds a zap field. Rust has no zap; the
//!   package's own test asserts the *rendered* console-encoder text, so
//!   [`logging::zap_ranges`] returns exactly that string.

pub mod logging;
#[allow(clippy::module_inception)]
pub mod rtree;

pub use logging::zap_ranges;
pub use rtree::{
    needs_merge, ChecksumStats, KeyRange, MetaSink, ProgressRange, ProgressRangeTree, Range,
    RangeFile, RangeStats, RangeStatsTree, RangeTree, RtreeError,
};
