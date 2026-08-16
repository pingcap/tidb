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

//! Go `pkg/util/stmtsummary/v2`: the persistent statement-summary backend.
//!
//! This is a thin module root; each Go file of the package keeps its own Rust
//! module, and each module header states its own complete-vs-SEED status:
//!
//! - [`record`] — Go `record.go`: complete.
//! - [`self::column`] — Go `column.go`: complete.
//! - [`stmtsummary`] — Go `stmtsummary.go`: complete.
//!
//! The v2 package as a whole is therefore NOT complete: Go `v2/reader.go`,
//! `v2/logger.go`, and `v2/tests/` are not ported, so this is not a package
//! claim — only three of its five production files land here.
//!
//! Two carve-outs from `v2/logger.go` exist because the three ported files
//! cannot stand without them, and both are SEED evidence for `logger.go` rather
//! than a port of it:
//!
//! - `marshalStmtRecord` / `marshalEvictedStmtRecord` /
//!   `marshalStmtRecordWithEvicted` live in [`record`], because `record.go`'s
//!   own upstream test drives them.
//! - `stmtLogStorage` is reduced to [`stmtsummary::StmtLogStorage`] over the
//!   [`stmtsummary::StmtLogWriter`] boundary, because `NewStmtSummary` in
//!   `stmtsummary.go` constructs one. `logger.go`'s zap encoder, its
//!   lumberjack rotation, and its `metrics.StmtSummaryEvictedLogCounter`
//!   wiring are absent.

pub mod column;
pub mod record;
pub mod stmtsummary;
