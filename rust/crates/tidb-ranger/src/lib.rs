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

//! Go `pkg/util/ranger`: compiled-expression scan-range construction — the
//! machinery that turns predicates into index/table key ranges.
//!
//! * [`types`] — `types.go`: the `Range`/`Ranges` model, the Go-format
//!   printer (`strconv.Quote` over raw bytes), and `MemUsage` with Go's
//!   real struct sizes. `types_test.go` transcreated.
//! * [`checker`] — `checker.go`: access-condition admission, collation
//!   gates, prefix/single-scan rules.
//! * [`points`] — `points.go`: the point model, the whole builder,
//!   prefix cutting, the union/intersection sweeps.
//! * [`ranger`] — `ranger.go`: points-to-ranges assembly, `UnionRanges`,
//!   the append family with Go's memory-quota estimates,
//!   `AppendRanges2PointRanges`, `BuildTableRange`/`BuildColumnRange`.
//! * [`detacher`] — `detacher.go`: eq/in extraction (including the
//!   unconditional NULL-valued-access drop), CNF/DNF detachment with both
//!   fix-control arms, the best-CNF-item machinery, `MergeDNFItems4Col`,
//!   and the shard-index GC-column family (`AddExpr4EqAndInCondition`).
//!
//! `ranger_test.go`'s table-driven suites are transcreated in
//! [`go_cases`]: TableRange, ColumnRange, UnsignedAndOverflow, Year,
//! PrefixIndexRangeScan, IndexRange, PrefixIndexRange,
//! ShardIndexFuncSuites, MinAccessCondsForDNFCond, the three
//! RangeFallback ladders, MemUsage, BinCollation, and issues 40997/50051.
//!
//! Native adaptations:
//! * `ranges_to_string` and `range_single_col_to_string` restore range bounds
//!   with typed datum comparison and literal restoration.
//! * Context-aware `_in` entry points evaluate retained expressions using the
//!   current execution. Static entry points reject unbound parameters.
//!   `ValueInfo` preserves parameter and deferred-expression mutability.
//!   This module is not a whole-package completion claim.
//! * `TestTableShardIndex` and `TestRangeFallback*`'s warning surface are
//!   testkit/plan-level: their ranger-observable cores are pinned here,
//!   and the plan-level halves belong to the driver's rule track.

pub mod checker;
pub mod detacher;
mod go_cases;
pub mod points;
pub mod ranger;
pub mod types;

pub use ranger::{range_single_col_to_string, ranges_to_string};
pub use types::{HasFullRange, Range, Ranges};

