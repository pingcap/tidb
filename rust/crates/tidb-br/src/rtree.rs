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

//! Go `br/pkg/rtree`: the non-overlapping range
//! trees BR uses to record which key ranges a backup has already covered, to
//! compute the gaps that still need requesting, and to fuse small adjacent
//! ranges into region-sized chunks before a restore splits regions.
//!
//! File mapping (one Rust module per Go file):
//! - [`rtree`] <- `rtree.go`
//! - [`logging`] <- `logging.go`
//!
//! Progress records have shared identity: insertion and lookup retain the same
//! [`ProgressRangeRef`], which survives completion independently of membership.
//! Release its mutex guard before invoking tree methods.
//!
//! # Narrowings and boundaries
//!
//! - Go's `Range` carries `Files []*backuppb.File`. All range/progress trees
//!   and metadata sinks use `Arc<tidb_proto::backup::File>` directly, preserving
//!   complete generated metadata and file identity when containers are cloned.
//! - boundary: `br/pkg/metautil`'s `MetaWriter` reaches object storage and
//!   serializes backup metafiles — entirely outside this package's subject.
//!   `ProgressRangeTree` only ever calls `Send(files, AppendDataFile)` on it,
//!   so it is narrowed to the one-method [`MetaSink`] trait object.
//!   `metautil.ChecksumStats` is a flat three-`uint64` struct and is declared
//!   locally as [`ChecksumStats`].
//! - `GetIncompleteRange`/`GetIncompleteRanges` return generated
//!   `tidb_proto::kvrpcpb::KeyRange` values, matching Go's RPC boundary. The
//!   distinct local [`KeyRange`] remains the algebra/logging type Go defines.
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
    needs_merge, ChecksumStats, KeyRange, MetaSink, ProgressRange, ProgressRangeRef,
    ProgressRangeTree, Range, RangeStats, RangeStatsTree, RangeTree, RtreeError,
};
