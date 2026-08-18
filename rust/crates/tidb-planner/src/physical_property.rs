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

//! Dependency-closed physical-property classifications from
//! `pkg/planner/property/physical_property.go`.
//!
//! This leaf ports only the integer classification and boolean matching
//! contracts. The source's expression columns, protobuf exchange enum,
//! functional-dependency sets, and physical-property construction remain
//! owned by future planner layers.

/// MPP exchange partitioning requirement.
///
/// `Any` is the default: Go's `AnyType` is the iota zero value
/// (`property/physical_property.go:110`), which is what a zero
/// `MppTask.partTp` reads as.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum MppPartitionType {
    /// No special partitioning requirement.
    #[default]
    Any,
    /// Broadcast rows to every MPP worker.
    Broadcast,
    /// Hash-partition rows by exchange columns.
    Hash,
    /// Send all rows to one worker.
    SinglePartition,
    /// Unknown source integer, retained for compatibility.
    Unknown(i32),
}

/// Exchange wire classification produced by the source mapping.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExchangeKind {
    /// Broadcast exchange.
    Broadcast,
    /// Hash exchange.
    Hash,
    /// Pass-through exchange, including `Any` and single-partition fallback.
    PassThrough,
}

impl MppPartitionType {
    /// Converts source integer values to a typed partition requirement.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        match raw {
            0 => Self::Any,
            1 => Self::Broadcast,
            2 => Self::Hash,
            3 => Self::SinglePartition,
            other => Self::Unknown(other),
        }
    }

    /// Returns the source integer value.
    #[must_use]
    pub const fn raw(self) -> i32 {
        match self {
            Self::Any => 0,
            Self::Broadcast => 1,
            Self::Hash => 2,
            Self::SinglePartition => 3,
            Self::Unknown(raw) => raw,
        }
    }

    /// Returns the source `ToExchangeType` mapping.
    #[must_use]
    pub const fn exchange_kind(self) -> ExchangeKind {
        match self {
            Self::Broadcast => ExchangeKind::Broadcast,
            Self::Hash => ExchangeKind::Hash,
            Self::Any | Self::SinglePartition | Self::Unknown(_) => ExchangeKind::PassThrough,
        }
    }
}

/// Whether a physical property matched directly or needs a merge sort.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PhysicalPropMatchResult {
    /// Required order cannot be satisfied.
    NotMatched,
    /// Required order is satisfied directly.
    Matched,
    /// Required order is satisfied after a merge sort.
    MatchedNeedMergeSort,
}

impl PhysicalPropMatchResult {
    /// Returns whether the property is considered matched by the source.
    #[must_use]
    pub const fn matched(self) -> bool {
        matches!(self, Self::Matched | Self::MatchedNeedMergeSort)
    }
}

/// Ordering work a source index path would have to perform for a task.
///
/// `findBestTask4LogicalDataSource` permits several ordering forms.  The
/// bounded index-only transition has no `KeepOrder`, partial-order, or
/// range-group merge-sort attachment yet, so callers must describe those
/// requests and receive an explicit invalid task instead of losing them.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum IndexOrderingRequirement {
    /// The parent has no ordering requirement.
    None,
    /// The index scan must preserve full order.
    KeepOrder,
    /// The source partial-order optimization is required.
    PartialOrder,
    /// The source grouped-range merge-sort path is required.
    MergeSort,
}

/// The type of execution task a required property demands.
///
/// `pkg/planner/property/task_type.go` declares ONE `TaskType`, and Go uses
/// that single type both here -- as `PhysicalProperty.TaskTp` -- and in the
/// cost functions. This module used to declare a second, four-variant copy of
/// it, which meant one Go file was ported twice at two fidelities: the copy
/// could not represent Go's `String()` fallthrough for an unrecognised integer,
/// and [`find_best_task`](crate::find_best_task) had to import both and alias
/// them apart. They are now the same type, as in Go.
///
/// Two tasks of different types cannot be compared by cost directly -- a cop
/// task must still be finished, and the finishing cost is not in its number yet
/// -- so the type travels WITH the required property rather than beside it.
pub use crate::task_type::TaskType;

/// Go `property.cteProducerStatus` (`physical_property.go:240-245`): whether
/// every CTE producer under this property can run in MPP mode.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub enum CteProducerStatus {
    /// Go `NoCTEOrAllProducerCanMPP`, the iota zero value.
    #[default]
    NoCteOrAllProducerCanMpp,
    /// Go `SomeCTEFailedMpp`.
    SomeCteFailedMpp,
    /// Go `AllCTECanMpp`.
    AllCteCanMpp,
}

/// One column of a required order, and the direction it is required in.
///
/// `property.SortItem`.  Go holds an `*expression.Column` and compares it with
/// `EqualColumn`, which is `UniqueID` equality and nothing else
/// (`pkg/expression/column.go:327`); the identity is therefore carried here as
/// that id, so two sort items compare exactly as Go's do.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SortItem {
    /// The column's `UniqueID`.
    pub col: i64,
    /// Whether the order is descending.
    pub desc: bool,
}

impl SortItem {
    /// A required order on `col`, ascending when `desc` is false.
    #[must_use]
    pub const fn new(col: i64, desc: bool) -> Self {
        Self { col, desc }
    }
}

impl std::fmt::Display for SortItem {
    /// `SortItem.String()`: `{col asc}` or `{col desc}`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let direction = if self.desc { "desc" } else { "asc" };
        write!(f, "{{{} {direction}}}", self.col)
    }
}

/// The physical property a parent requires of a child.
///
/// `property.PhysicalProperty`.  This port carries the three fields that
/// decide a plan's SHAPE -- the required order, the task type, and the row
/// cap -- because those are the ones a required-order flow reads.  The
/// source's MPP partitioning, CTE producer status, vector-search, index-join
/// runtime, partial-order and advisory-order fields belong to planner layers
/// that are not built here, and are deliberately absent rather than stubbed:
/// an absent field cannot be read as a wrong answer.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalProperty {
    /// The required sort attributes, outermost first.
    pub sort_items: Vec<SortItem>,
    /// The task type the parent requires.
    pub task_tp: TaskType,
    /// The parent may close this operator after this many rows.
    pub expected_cnt: f64,
    /// Whether a sort enforcer may be added to satisfy this property.
    pub can_add_enforcer: bool,
    /// Go `SortItemsForPartition`: "these sort only need to sort the data of
    /// one partition, instead of global" — the MPP window paths fill it;
    /// everywhere else it stays empty, which is exactly Go's zero value.
    pub sort_items_for_partition: Vec<SortItem>,
    /// Go `CTEProducerStatus`: threaded through every child property a CTE
    /// consumer builds, so a producer that cannot run MPP poisons the whole
    /// sequence's MPP choice.
    pub cte_producer_status: CteProducerStatus,
}

impl Default for PhysicalProperty {
    /// The empty root property: no order, no cap.
    ///
    /// Go spells this `&property.PhysicalProperty{ExpectedCnt: math.MaxFloat64}`
    /// at every entry point, so the cap defaults to "no cap" rather than zero.
    fn default() -> Self {
        Self {
            sort_items: Vec::new(),
            task_tp: TaskType::Root,
            expected_cnt: f64::MAX,
            can_add_enforcer: false,
            sort_items_for_partition: Vec::new(),
            cte_producer_status: CteProducerStatus::default(),
        }
    }
}

impl PhysicalProperty {
    /// `property.NewPhysicalProperty`: a required order over `cols`, all in
    /// the same direction.
    #[must_use]
    pub fn new(
        task_tp: TaskType,
        cols: &[i64],
        desc: bool,
        expected_cnt: f64,
        enforced: bool,
    ) -> Self {
        Self {
            sort_items: cols.iter().map(|&col| SortItem::new(col, desc)).collect(),
            task_tp,
            expected_cnt,
            can_add_enforcer: enforced,
            sort_items_for_partition: Vec::new(),
            cte_producer_status: CteProducerStatus::default(),
        }
    }

    /// `CloneEssentialFields` (`physical_property.go:713`), over the ported
    /// field set. Two absences are Go's own: `CanAddEnforcer` is NOT copied
    /// (the clone defaults to false — an enforcer admission never rides
    /// down to a child property), and `indexJoinProp` is "default not to
    /// clone" (unported here anyway).
    #[must_use]
    pub fn clone_essential_fields(&self) -> Self {
        Self {
            sort_items: self.sort_items.clone(),
            sort_items_for_partition: self.sort_items_for_partition.clone(),
            task_tp: self.task_tp,
            expected_cnt: self.expected_cnt,
            can_add_enforcer: false,
            cte_producer_status: self.cte_producer_status,
        }
    }

    /// `IsSortItemEmpty`: whether the order property is empty.
    #[must_use]
    pub fn is_sort_item_empty(&self) -> bool {
        self.sort_items.is_empty()
    }

    /// `IsSortItemAllForPartition` (`physical_property.go:565`): whether
    /// `SortItems` is the same list as `SortItemsForPartition`, item by item
    /// — same column (`EqualColumn` is `UniqueID` equality) and same
    /// direction. Both empty answers true, which is why the empty-sort check
    /// must run FIRST wherever Go runs it first.
    #[must_use]
    pub fn is_sort_item_all_for_partition(&self) -> bool {
        self.sort_items_for_partition == self.sort_items
    }

    /// `NeedKeepOrder`: whether the property requires maintaining order.
    ///
    /// Go also answers true for a `PartialOrderInfo`, which this port does not
    /// carry; with that field absent the two spellings coincide.
    #[must_use]
    pub fn need_keep_order(&self) -> bool {
        !self.is_sort_item_empty()
    }

    /// `AllSameOrder`: whether every item runs the same direction, and which.
    ///
    /// An EMPTY property answers `(true, false)`, which is what makes an
    /// unordered parent demand an ASCENDING child order rather than no answer.
    #[must_use]
    pub fn all_same_order(&self) -> (bool, bool) {
        let Some(first) = self.sort_items.first() else {
            return (true, false);
        };
        if self.sort_items.iter().any(|item| item.desc != first.desc) {
            return (false, false);
        }
        (true, first.desc)
    }

    /// `IsPrefix`: whether this order is a prefix of `other`'s.
    #[must_use]
    pub fn is_prefix(&self, other: &Self) -> bool {
        if self.sort_items.len() > other.sort_items.len() {
            return false;
        }
        self.sort_items
            .iter()
            .zip(&other.sort_items)
            .all(|(mine, theirs)| mine == theirs)
    }

    /// `GetSortDescForKeepOrder`: the direction a keep-order scan must run.
    #[must_use]
    pub fn sort_desc_for_keep_order(&self) -> bool {
        self.all_same_order().1
    }
}

impl std::fmt::Display for PhysicalProperty {
    /// `PhysicalProperty.String()`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let items: Vec<String> = self.sort_items.iter().map(ToString::to_string).collect();
        write!(
            f,
            "Prop{{cols: [{}], TaskTp: {}, expectedCount: {}}}",
            items.join(" "),
            self.task_tp.as_str(),
            self.expected_cnt
        )
    }
}

#[cfg(test)]
mod required_property_tests {
    use super::{PhysicalProperty, SortItem, TaskType};

    /// `AllSameOrder` answers `(true, false)` for the EMPTY property, which is
    /// what lets a parent with no order of its own still demand an ascending
    /// child order from a merge join.  Go: `if len(p.SortItems) == 0 { return
    /// true, false }`.
    #[test]
    fn an_empty_property_is_same_order_and_ascending() {
        let prop = PhysicalProperty::default();
        assert!(prop.is_sort_item_empty());
        assert!(!prop.need_keep_order());
        assert_eq!(prop.all_same_order(), (true, false));
        assert!(!prop.sort_desc_for_keep_order());
    }

    /// A mixed-direction property is NOT all-same-order, and the second
    /// answer is then meaningless -- Go returns the zero value for it.
    #[test]
    fn mixed_directions_are_not_all_same_order() {
        let prop = PhysicalProperty {
            sort_items: vec![SortItem::new(1, false), SortItem::new(2, true)],
            ..PhysicalProperty::default()
        };
        assert_eq!(prop.all_same_order(), (false, false));
        let desc = PhysicalProperty::new(TaskType::Root, &[1, 2], true, f64::MAX, false);
        assert_eq!(desc.all_same_order(), (true, true));
        assert!(desc.sort_desc_for_keep_order());
    }

    /// `IsPrefix` compares column AND direction pairwise, and a longer
    /// property is never a prefix of a shorter one.
    #[test]
    fn is_prefix_compares_column_and_direction() {
        let short = PhysicalProperty::new(TaskType::Root, &[1], false, f64::MAX, false);
        let long = PhysicalProperty::new(TaskType::Root, &[1, 2], false, f64::MAX, false);
        assert!(short.is_prefix(&long));
        assert!(!long.is_prefix(&short));
        let flipped = PhysicalProperty::new(TaskType::Root, &[1, 2], true, f64::MAX, false);
        assert!(!short.is_prefix(&flipped));
    }

    /// The printed form is Go's, so a property in a trace reads the same on
    /// both sides.
    #[test]
    fn printed_form_is_gos() {
        let prop = PhysicalProperty::new(TaskType::CopSingleRead, &[7], true, 10.0, false);
        assert_eq!(
            prop.to_string(),
            "Prop{cols: [{7 desc}], TaskTp: copSingleReadTask, expectedCount: 10}"
        );
    }
}
