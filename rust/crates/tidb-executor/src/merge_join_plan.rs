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

//! Executor receipt for a planner-selected physical merge join.

/// A join key as a pair of column offsets, one in each side's own row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MergeJoinKey {
    /// Column offset within the left child's row.
    pub(crate) left: usize,
    /// Column offset within the right child's row.
    pub(crate) right: usize,
}

/// The key order retained by Go's `PhysicalMergeJoin`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MergeJoinPlan {
    /// The key pairs, in the order both sides are sorted by.
    pub(crate) keys: Vec<MergeJoinKey>,
    /// Whether both sides run descending.
    pub(crate) desc: bool,
}
