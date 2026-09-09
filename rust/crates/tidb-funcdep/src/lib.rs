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

//! Go `pkg/planner/funcdep`, plus the `pkg/planner/util.IsNullRejected` proof
//! that feeds it: which columns of a query block determine which others, and
//! which predicates prove a column NOT NULL.
//!
//! # Why this is its own crate
//!
//! Go keeps `funcdep` under `pkg/planner`, below every rule that reads it.
//! This workspace had it in `tidb-executor`'s SQL driver, which is ABOVE
//! `tidb-planner` in the dependency order -- so the logical-optimization rules
//! that need functional dependencies (outer-join elimination, aggregate
//! elimination, `ONLY_FULL_GROUP_BY`) could not reach it without inverting the
//! edge. This crate is a leaf over `tidb-util` (the column set) and
//! `tidb-expr` (the expression tree), so BOTH `tidb-planner` and
//! `tidb-executor` may depend on it, matching Go's own layering.
//!
//! The graph owns dependency projection, conditional outer-join dependencies,
//! and scalar-expression identities. Native logical operators in
//! `tidb-planner` derive these sets from resolved expressions; the legacy SQL
//! driver still has its AST-facing adapter until native SQL planning lands.
//! Null rejection delegates to `tidb_expr::expression::is_null_rejected`.
//!
//! Graph tests originate in Go. Full SQL-to-FD tests remain explicit ignored
//! gaps: native derivation alone does not establish whole-package parity.

pub mod fd_graph;
pub mod null_reject;
pub mod tests_extract_fd;

pub use fd_graph::{find_common_equiv_classes, FdSet, OuterJoinOptions};

/// Go `intset.FastIntSet`: the column-id set every dependency edge is built
/// from. Re-exported so a consumer needs only this crate to speak to the
/// graph.
pub use tidb_util::intset::FastIntSet as ColSet;
