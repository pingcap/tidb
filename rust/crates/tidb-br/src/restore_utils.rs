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

//! Go `br/pkg/restore/utils` lands as a complete package: the rewrite rules
//! that translate a backup's table and index key prefixes into the IDs the
//! restore target actually allocated, and the range merging that turns a
//! backup's many small SST ranges into region-sized split points.
//!
//! File mapping (one Rust module per Go file):
//! - [`rewrite_rule`] <- `rewrite_rule.go`
//! - [`merge`] <- `merge.go`
//! - [`misc`] <- `misc.go`
//! - [`common`] <- `common.go`
//! - [`proto`] holds the two kvproto messages this package treats as flat
//!   structs; see below.
//!
//! # Narrowings and boundaries
//!
//! - NARROWING: `import_sstpb.RewriteRule` and `brpb.File` are flat protobuf
//!   messages that this package only ever reads and writes field by field.
//!   They are declared locally in [`proto`] with `// boundary:` notes, so no
//!   protobuf runtime or generated code crosses into this crate. Go's
//!   generated `GetXxx()` accessors return the zero value for a `nil` message;
//!   Rust spells the same thing with `Option<&RewriteRule>` plus the
//!   [`proto::RewriteRule::get_old_key_prefix`] family of nil-tolerant helpers.
//! - boundary: `br/pkg/errors`' normalized sentinels become
//!   [`rewrite_rule::RestoreErrorKind`] carried inside
//!   [`rewrite_rule::RestoreError`]. `errors.Annotate(kind, msg).Error()`
//!   renders as `"{msg}: {kind message}"`, byte-identical to `pingcap/errors`,
//!   and `errors.Cause(err) == berrors.X` becomes
//!   [`rewrite_rule::RestoreError::kind`].
//! - boundary: `metautil.Table`, the pre-restore schema snapshot carried on
//!   `CreatedTable.OldTable`, belongs to the metafile package. Go's
//!   `CreatedTable` never inspects it, so [`common::CreatedTable`] carries it
//!   as an opaque generic payload.
//! - `util.ProtoV1Clone` inside `(*RewriteRules).Clone` is protobuf deep-copy;
//!   [`rewrite_rule::RewriteRules::go_clone`] is that same deep copy, and (as
//!   in Go) deliberately leaves the three timestamp fields at zero.
//! - Go's `filesMap map[string][]*backuppb.File` iterates in random order.
//!   Rust uses a `BTreeMap`, which only makes the (already order-independent)
//!   result deterministic: duplicate ranges are an error either way.
//! - `log.Panic` on two files sharing a start key but not an end key becomes a
//!   Rust `panic!` with the same three values.
//! - `zap`/`logutil` logging calls carry no semantics and are dropped; the
//!   branches they sit in are preserved.

pub mod common;
pub mod merge;
pub mod misc;
pub mod proto;
pub mod rewrite_rule;

pub use common::CreatedTable;
pub use merge::{merge_and_rewrite_file_ranges, MergeRangesStat};
pub use misc::{
    encode_key_prefix, get_index_id_map, get_partition_id_map, get_table_id_map, truncate_ts,
    DEFAULT_CF_NAME, WRITE_CF_NAME,
};
pub use proto::{File, RewriteRule};
pub use rewrite_rule::{
    empty_rewrite_rule, empty_rewrite_rules_map, find_matched_rewrite_rule,
    get_rewrite_encoded_keys, get_rewrite_raw_keys, get_rewrite_rule_of_table, get_rewrite_rules,
    get_rewrite_rules_map, get_rewrite_table_id, rewrite_and_encode_raw_key, rewrite_range,
    set_time_range_filter, validate_file_rewrite_rule, AppliedFile, RestoreError, RestoreErrorKind,
    RewriteRules, RewrittenKeys, TableIdRemap,
};
