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

//! Go `pkg/planner/funcdep`: functional dependencies used by logical planning.
//!
//! For a rigorous treatment of functional dependencies and SQL operators, see
//! Norman Paulley and Glenn's *Exploiting Functional Dependence in Query
//! Optimization* (2000).
//!
//! This package uses TiDB's definition of a lax dependency: a lax dependency
//! becomes strict when every determinant column is known NOT NULL; dependent
//! columns do not also need to be NOT NULL. Outer joins retain predicate facts
//! that depend on rejecting NULL-extended rows as conditional dependencies.
//! [`null_reject`] provides the `pkg/planner/util.IsNullRejected` proof that
//! supplies those NOT NULL facts.

pub mod fd_graph;
pub mod null_reject;

pub use fd_graph::{find_common_equiv_classes, FdSet, OuterJoinOptions};

/// Go `intset.FastIntSet`: the column-id set every dependency edge is built
/// from. Re-exported so a consumer needs only this crate to speak to the
/// graph.
pub use tidb_util::intset::FastIntSet as ColSet;
