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

//! Go `handleColHelper` (`planbuilder.go:375-420`): the bottom-up stack of
//! per-table handle columns the build maintains.
//!
//! Go's own comment on `PlanBuilder.handleHelper` states the protocol, and it
//! is reproduced exactly by [`HandleColHelper`]'s method set:
//!
//! > Since we build the plan tree from bottom to top, we maintain a stack to
//! > record the current handle information. If it's a dataSource/tableDual
//! > node, we create a new map. If it's an aggregation, we pop the map and
//! > push a nil map since no handle information left. If it's a union, we pop
//! > all children's and push a nil map. If it's a join, we pop its children's
//! > out then merge them and push the new map to stack. If we meet a subquery
//! > or CTE ... we just pop one map out when we finish building it.
//!
//! # Restructuring, named
//!
//! Go carries `id2HandleMapStack []map[...]` beside a redundant `stackTail`
//! and truncates the slice on every pop. A `Vec` and its own length are the
//! same thing, so `stackTail` is dropped; `resetForReuse`'s
//! `id2HandleMapStack[:0]` becomes [`HandleColHelper::reset_for_reuse`].
//!
//! Go's `popMap` and `tailMap` index `[stackTail-1]` and PANIC on an empty
//! stack — a builder invariant violation. Both return `Option` here, so a
//! caller that reaches them out of order gets a `None` it must handle rather
//! than an unwind; the SELECT spine's call sites treat that as
//! [`PlanError`](crate::plan_base::PlanError).
//!
//! # The element type
//!
//! Go's value is `util.HandleCols`, an interface over an int handle and a
//! common handle. [`crate::handle_cols`] already models both identities, so
//! [`PlanHandleCols`] is the two-variant sum of them rather than a third
//! spelling. That keeps the `Hash64`/`Equals` behaviour the cascades memo
//! needs on the same values the helper stores.

use std::collections::BTreeMap;

use crate::handle_cols::{CommonHandleIdentity, IntHandleIdentity};

/// Go `util.HandleCols`, as the closed sum of the two implementations
/// [`crate::handle_cols`] already carries.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum PlanHandleCols {
    /// Go `util.IntHandleCols`.
    Int(IntHandleIdentity),
    /// Go `util.CommonHandleCols`.
    Common(CommonHandleIdentity),
}

impl PlanHandleCols {
    /// Go `HandleCols.IsInt()`.
    #[must_use]
    pub const fn is_int(&self) -> bool {
        matches!(self, Self::Int(_))
    }

    /// Go `HandleCols.Hash64(h)`, dispatched to the identity that owns it.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        match self {
            Self::Int(identity) => identity.hash64(),
            Self::Common(identity) => identity.hash64(),
        }
    }
}

/// Go's `map[int64][]util.HandleCols`: table id to that table's handles.
///
/// `BTreeMap` rather than `HashMap` so a merge and any diagnostic walk are
/// deterministic; Go's map order is unobserved by the ported bodies, which
/// only ever look a table id up.
pub type HandleColMap = BTreeMap<i64, Vec<PlanHandleCols>>;

/// Go `handleColHelper` (`planbuilder.go:375`).
#[derive(Clone, Debug, Default)]
pub struct HandleColHelper {
    stack: Vec<HandleColMap>,
}

impl HandleColHelper {
    /// An empty helper, Go's zero value.
    #[must_use]
    pub const fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// Go `resetForReuse` (`planbuilder.go:381`), which keeps the allocation.
    pub fn reset_for_reuse(&mut self) {
        self.stack.clear();
    }

    /// How many maps are stacked; Go's `stackTail`.
    #[must_use]
    pub fn depth(&self) -> usize {
        self.stack.len()
    }

    /// Go `pushMap` (`planbuilder.go:394`).
    pub fn push_map(&mut self, map: HandleColMap) {
        self.stack.push(map);
    }

    /// Go's "push a nil map since no handle information left", used by the
    /// aggregation and union arms of the protocol above.
    pub fn push_empty(&mut self) {
        self.stack.push(HandleColMap::new());
    }

    /// Go `popMap` (`planbuilder.go:387`), returning `None` where Go panics.
    pub fn pop_map(&mut self) -> Option<HandleColMap> {
        self.stack.pop()
    }

    /// Go `tailMap` (`planbuilder.go:415`), returning `None` where Go panics.
    #[must_use]
    pub fn tail_map(&self) -> Option<&HandleColMap> {
        self.stack.last()
    }

    /// Go `mergeAndPush(m1, m2)` (`planbuilder.go:399`): a table present in
    /// both sides keeps `m1`'s handles followed by `m2`'s, in that order.
    ///
    /// Go deep-copies each slice because the two argument maps stay live; here
    /// they are taken BY VALUE, so the entries move and nothing is copied.
    pub fn merge_and_push(&mut self, left: HandleColMap, right: HandleColMap) {
        let mut merged = left;
        for (table_id, handles) in right {
            merged.entry(table_id).or_default().extend(handles);
        }
        self.push_map(merged);
    }

    /// Go's join arm in one call: pop the two children's maps, merge them, and
    /// push the result. `None` when the stack does not hold two maps.
    pub fn pop_two_and_merge(&mut self) -> Option<()> {
        // Popped right-then-left because the right child was built and pushed
        // last, and Go's `mergeAndPush(lMap, rMap)` orders the handles left
        // first.
        let right = self.pop_map()?;
        let left = self.pop_map()?;
        self.merge_and_push(left, right);
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::{HandleColHelper, HandleColMap, PlanHandleCols};
    use crate::handle_cols::{CommonHandleIdentity, HandleColumnIdentity, IntHandleIdentity};

    fn int_handle(unique_id: i64) -> PlanHandleCols {
        PlanHandleCols::Int(IntHandleIdentity::new(Some(HandleColumnIdentity::new(
            1, unique_id, 0,
        ))))
    }

    fn common_handle(unique_id: i64) -> PlanHandleCols {
        PlanHandleCols::Common(CommonHandleIdentity::new(
            None,
            None,
            Some(vec![HandleColumnIdentity::new(1, unique_id, 0)]),
        ))
    }

    fn map_of(entries: &[(i64, PlanHandleCols)]) -> HandleColMap {
        let mut map = HandleColMap::new();
        for (table_id, handle) in entries {
            map.entry(*table_id).or_default().push(handle.clone());
        }
        map
    }

    #[test]
    fn test_push_and_pop_is_a_stack() {
        let mut helper = HandleColHelper::new();
        assert_eq!(helper.depth(), 0);
        assert!(helper.tail_map().is_none());
        assert!(helper.pop_map().is_none());

        helper.push_map(map_of(&[(1, int_handle(10))]));
        helper.push_map(map_of(&[(2, int_handle(20))]));
        assert_eq!(helper.depth(), 2);
        assert!(helper.tail_map().is_some_and(|m| m.contains_key(&2)));

        let top = helper.pop_map().expect("two maps were pushed");
        assert!(top.contains_key(&2));
        assert!(helper.tail_map().is_some_and(|m| m.contains_key(&1)));
        assert_eq!(helper.depth(), 1);
    }

    #[test]
    fn test_aggregation_arm_pushes_an_empty_map() {
        // Go: "If it's an aggregation, we pop the map and push a nil map since
        // no handle information left."
        let mut helper = HandleColHelper::new();
        helper.push_map(map_of(&[(1, int_handle(10))]));
        helper.pop_map();
        helper.push_empty();
        assert_eq!(helper.depth(), 1);
        assert!(helper.tail_map().is_some_and(HandleColMap::is_empty));
    }

    #[test]
    fn test_merge_keeps_left_handles_first_and_unions_tables() {
        let mut helper = HandleColHelper::new();
        let left = map_of(&[(1, int_handle(10)), (3, int_handle(30))]);
        let right = map_of(&[(1, common_handle(11)), (2, int_handle(20))]);
        helper.merge_and_push(left, right);

        let merged = helper.pop_map().expect("merge pushed one map");
        assert_eq!(merged.len(), 3);
        let shared = &merged[&1];
        assert_eq!(shared.len(), 2);
        assert!(shared[0].is_int());
        assert!(!shared[1].is_int());
        assert_eq!(merged[&2].len(), 1);
        assert_eq!(merged[&3].len(), 1);
    }

    #[test]
    fn test_join_arm_pops_two_and_pushes_one() {
        let mut helper = HandleColHelper::new();
        helper.push_map(map_of(&[(1, int_handle(10))]));
        helper.push_map(map_of(&[(2, int_handle(20))]));
        assert!(helper.pop_two_and_merge().is_some());
        assert_eq!(helper.depth(), 1);
        let merged = helper.tail_map().expect("merge pushed one map");
        assert!(merged.contains_key(&1) && merged.contains_key(&2));

        // One map left: the join arm reports failure rather than unwinding.
        assert!(helper.pop_two_and_merge().is_none());
    }

    #[test]
    fn test_reset_for_reuse_empties_the_stack() {
        let mut helper = HandleColHelper::new();
        helper.push_empty();
        helper.push_empty();
        helper.reset_for_reuse();
        assert_eq!(helper.depth(), 0);
    }
}
