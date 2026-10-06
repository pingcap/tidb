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

//! Dependency-closed tests for `pkg/planner/util/byitem.go:22`.
//!
//! The Go planner anchor is `TestSortByItemsPruning` at
//! `pkg/planner/core/logical_plans_test.go:679`; these vectors isolate the
//! reusable ORDER BY metadata contract from expression and plan ownership.

use tidb_planner::by_item::{stringify_by_items, ByItem};

#[test]
fn by_items_preserve_expression_and_direction_identity() {
    let ascending = ByItem::new("a", false);
    let descending = ByItem::new("a", true);
    assert_eq!(ascending.expression(), Some("a"));
    assert!(!ascending.is_desc());
    assert_eq!(ascending.display(), "a");
    assert_eq!(descending.display(), "a true");
    assert_ne!(ascending, descending);
    assert_eq!(ascending.clone(), ascending);
}

#[test]
fn by_item_lists_and_memory_usage_match_source_shape() {
    let items = [ByItem::new("a", false), ByItem::new("b", true)];
    assert_eq!(stringify_by_items(&items), "[a b true]");
    assert_eq!(items[0].memory_usage(), 1 + 1);
    assert_eq!(items[1].memory_usage(), 1 + 1);
    assert_eq!(ByItem::empty(false).expression(), None);
    assert_eq!(stringify_by_items(&[]), "[]");
}
