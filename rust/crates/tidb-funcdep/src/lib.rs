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
//! # What is here, and how it got here
//!
//! * [`fd_graph`] is a VERBATIM relocation of an existing, Go-verified port
//!   (`fd_graph.go`), including its Go citations and its complete test table.
//!   It is pure [`ColSet`] arithmetic and references no expression or AST type.
//!   The only edits are mechanical: `pub(crate)` widened to `pub` so the graph
//!   is usable across the crate boundary, doc comments added on the now-public
//!   [`fd_graph::OuterJoinOptions`] fields, and the `#[cfg(test)]` gates
//!   dropped from the two public accessors
//!   ([`FdSet::closure_of_lax`](fd_graph::FdSet::closure_of_lax) and
//!   [`FdSet::constant_cols`](fd_graph::FdSet::constant_cols)) that consumers
//!   outside the crate must be able to call. No algorithm, comment, or test
//!   assertion changed.
//!
//! * [`null_reject`] is RETARGETED, not relocated. Go's `IsNullRejected`
//!   operates on `expression.Expression`, and the executor's copy was typed
//!   against the written `tidb_ast::Expr` because that tier had no expression
//!   tree. This crate's version works on [`tidb_expr::expression::Expression`],
//!   which moves it TOWARD Go rather than away, and delegates the proof itself
//!   to `tidb-expr`'s own complete transcreation of the Go function (which
//!   carries Go's full `null_misc_builtins.go` table and the nullify-then-fold
//!   bridge). What this module adds is the funcdep-facing shape -- one column
//!   versus a whole nullified child schema -- and the boundary tests.
//!
//! # One proof, reached three ways
//!
//! The executor's duplicate copies have since been retired. `fd_graph` was a
//! drop-in there, and `tidb-executor/src/driver/funcdep/null_reject.rs` now
//! holds only a syntax-to-expression translation over
//! [`null_reject::is_null_rejected_by`], because its callers pass
//! `tidb_ast::Expr` with no schema in reach to resolve against.
//!
//! So the null-rejection proof exists once, in
//! `tidb_expr::expression::is_null_rejected`. This module is the
//! funcdep-facing shape over it, and the executor's module is the
//! syntax-facing one. Neither carries proof logic of its own, which is the
//! state `AGENTS.md` requires.

pub mod fd_graph;
pub mod null_reject;

pub use fd_graph::{FdSet, OuterJoinOptions};

/// Go `intset.FastIntSet`: the column-id set every dependency edge is built
/// from. Re-exported so a consumer needs only this crate to speak to the
/// graph.
pub use tidb_util::intset::FastIntSet as ColSet;
