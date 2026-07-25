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

//! Transcreation of Go `pkg/util/memory`'s tracker core (`tracker.go` +
//! `action.go`): the hierarchical memory-consumption tracker and the
//! OOM-action chain executors bind to it.
//!
//! IN-PROGRESS PACKAGE: `pkg/util/memory` is one atomic transcreation unit;
//! this module currently covers the tracker/action core. Still to land
//! within the same package claim: `arbitrator.go`/`global_arbitrator.go`
//! (the tracker's `MemArbitrator` hook and `MemUsageTop1Tracker` global
//! integrate there), `pool.go`, and `utils.go`; `meminfo.go`/`memstats.go`
//! are OS/Go-runtime probes whose Rust shape follows the server crate.
//!
//! Faithful adaptations in the core, none changing observable behavior:
//! - Go's `*Tracker` graph becomes `Arc<Tracker>` with a `Weak` parent and
//!   `Arc::ptr_eq` for the identity comparisons `remove`/`ReplaceChild`/
//!   `UnbindActionFromHardLimit` perform.
//! - Prometheus gauges (`metrics.MemoryUsage`) are observability side
//!   effects with no behavioral contract: not ported.
//! - GC-aware release (`EnableGCAwareMemoryTrack` + `runtime.SetFinalizer`)
//!   defers the release decrement until a Go GC cycle — Go-runtime
//!   machinery with no Rust counterpart; `Release` therefore takes the
//!   default (flag-off) path, `Consume(-bytes)`, and the flag is not
//!   surfaced.

mod action;
mod tracker;

pub use action::{
    ActionOnExceed, BaseOomAction, LogOnExceed, PanicOnExceed, DEF_CURSOR_FETCH_SPILL_PRIORITY,
    DEF_LOG_PRIORITY, DEF_PANIC_PRIORITY, DEF_RATE_LIMIT_PRIORITY, DEF_SPILL_PRIORITY,
};
pub use tracker::{
    bytes_to_string, format_bytes, Tracker, DEF_MEM_QUOTA_QUERY, LABEL_FOR_GLOBAL_ANALYZE_MEMORY,
    LABEL_FOR_MEM_DB, LABEL_FOR_SQL_TEXT, TRACK_MEM_WHEN_EXCEEDS,
};
