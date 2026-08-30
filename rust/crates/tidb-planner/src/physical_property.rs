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
//! Full expression columns are retained for structural hashing and equality;
//! order and partition matching use their Go `UniqueID` where the source does.

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
/// `property.SortItem`. Go's `Hash64`/`Equals` use the complete expression
/// column, while physical-order matching methods explicitly use column
/// `UniqueID`; retaining the complete column preserves both behaviors.
#[derive(Clone, Debug)]
pub struct SortItem {
    /// Required column.
    pub col: tidb_expr::column::Column,
    /// Whether the order is descending.
    pub desc: bool,
}

/// Go `MPPPartitionColumn`: one hash-exchange key and its collation id.
#[derive(Clone, Debug)]
pub struct MppPartitionColumn {
    /// The expression column.
    pub col: tidb_expr::column::Column,
    /// Go `CollateID`.
    pub collate_id: i32,
}

impl MppPartitionColumn {
    /// Creates one partition column.
    #[must_use]
    pub fn new(col: i64, collate_id: i32) -> Self {
        let mut column = tidb_expr::column::Column::default();
        column.unique_id = col;
        Self {
            col: column,
            collate_id,
        }
    }

    /// Go `MPPPartitionColumn.Equal`.
    #[must_use]
    pub fn equal(&self, other: &Self) -> bool {
        (self.collate_id >= 0 || self.collate_id == other.collate_id)
            && self.col.unique_id == other.col.unique_id
    }

    /// Go `MPPPartitionColumn.ResolveIndices`.
    pub fn resolve_indices(
        &self,
        schema: &tidb_expr::schema::Schema,
    ) -> Result<Self, &'static str> {
        let Some(column) = self.col.resolve_indices_by_virtual_expr(schema) else {
            return Err("MPP partition column not found in schema");
        };
        Ok(Self {
            col: column,
            collate_id: self.collate_id,
        })
    }

    /// Go `MPPPartitionColumn.hashCode`.
    #[must_use]
    pub fn hash_code(&self) -> Vec<u8> {
        let mut code = Vec::new();
        encode_column_id(&mut code, self.col.unique_id);
        tidb_codec::encode_int(
            &mut code,
            if self.collate_id < 0 {
                i64::from(self.collate_id)
            } else {
                1
            },
        );
        code
    }

    /// Source-shaped owned memory accounting.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.col.orig_name.capacity()
    }
}

/// Select partition columns at the source-provided match offsets.
#[must_use]
pub fn choose_partition_keys(
    keys: &[MppPartitionColumn],
    matches: &[usize],
) -> Vec<MppPartitionColumn> {
    matches.iter().map(|&index| keys[index].clone()).collect()
}

/// Go `GetCollateIDByNameForPartition`.
#[must_use]
pub fn collate_id_for_partition(name: &str) -> i32 {
    tidb_datatype::rewrite_new_collation_id_if_needed(tidb_datatype::collation_name_to_id(name))
}

/// Go `GetCollateNameByIDForPartition`.
#[must_use]
pub fn collate_name_for_partition(id: i32) -> String {
    tidb_datatype::collation_id_to_name(tidb_datatype::restore_collation_id_if_needed(id))
}

/// Go `IndexJoinRuntimeProp`: the lookup facts carried from an index-join
/// candidate to the inner data source while `findBestTask` plans that child.
#[derive(Clone, Debug)]
pub struct IndexJoinRuntimeProp {
    /// Conditions that may complete the final lookup-range column.
    pub other_conditions: Vec<tidb_expr::expression::Expression>,
    /// The driving join keys on the outer child.
    pub outer_join_keys: Vec<tidb_expr::column::Column>,
    /// The lookup keys on the inner child.
    pub inner_join_keys: Vec<tidb_expr::column::Column>,
    /// Estimated inner rows returned for one outer row.
    pub avg_inner_row_count: f64,
    /// Whether this candidate probes the clustered/table handle rather than
    /// a secondary index.
    pub table_range_scan: bool,
}

impl IndexJoinRuntimeProp {
    /// Go `IndexJoinRuntimeProp.CloneEssentialFields`.
    #[must_use]
    pub fn clone_essential_fields(&self) -> Self {
        self.clone()
    }
}

/// Go `PartialOrderInfo`: ORDER BY items eligible for prefix-index ordering.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PartialOrderInfo {
    /// The TopN ORDER BY items.
    pub sort_items: Vec<SortItem>,
}

impl PartialOrderInfo {
    /// Go `PartialOrderInfo.AllSameOrder`.
    #[must_use]
    pub fn all_same_order(&self) -> (bool, bool) {
        all_same_order(&self.sort_items)
    }

    /// Source-shaped owned memory accounting.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.sort_items.capacity() * std::mem::size_of::<SortItem>()
    }
}

/// Go `PartialOrderMatchResult`.
#[derive(Clone, Debug, Default)]
pub struct PartialOrderMatchResult {
    /// Whether an access path supplies the requested partial order.
    pub matched: bool,
    /// Unique ID of the final prefix-index column, when present.
    pub prefix_col: Option<tidb_expr::column::Column>,
    /// Prefix length in bytes.
    pub prefix_len: usize,
}

impl PartialOrderMatchResult {
    /// Source-shaped owned memory accounting.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .prefix_col
                .as_ref()
                .map_or(0, |column| column.orig_name.capacity())
    }
}

impl PartialEq for PartialOrderMatchResult {
    fn eq(&self, other: &Self) -> bool {
        self.matched == other.matched
            && self.prefix_len == other.prefix_len
            && match (&self.prefix_col, &other.prefix_col) {
                (Some(left), Some(right)) => left.equals(right),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for PartialOrderMatchResult {}

/// Go `expression.VSInfo`, retained by `PhysicalProperty.VectorProp`.
#[derive(Clone, Debug, Default)]
pub struct VectorSearchInfo {
    /// Distance function name.
    pub distance_fn_name: String,
    /// Pushdown function signature code.
    pub fn_pb_code: i32,
    /// Query vector constant. Go deliberately excludes it from property hash.
    pub vector: tidb_datatype::VectorFloat32,
    /// Vector column.
    pub column: tidb_expr::column::Column,
}

impl PartialEq for VectorSearchInfo {
    fn eq(&self, other: &Self) -> bool {
        self.distance_fn_name == other.distance_fn_name
            && self.fn_pb_code == other.fn_pb_code
            && self.vector == other.vector
            && self.column.equals(&other.column)
    }
}

/// Go's anonymous `PhysicalProperty.VectorProp` field.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct VectorProperty {
    /// Vector-search requirement, when present.
    pub search_info: Option<VectorSearchInfo>,
    /// Requested TopK count. Go does not include this in property hashing.
    pub top_k: u32,
}

impl PartialEq for IndexJoinRuntimeProp {
    fn eq(&self, other: &Self) -> bool {
        self.table_range_scan == other.table_range_scan
            && self.avg_inner_row_count == other.avg_inner_row_count
            && self.other_conditions.len() == other.other_conditions.len()
            && self
                .other_conditions
                .iter()
                .zip(&other.other_conditions)
                .all(|(left, right)| left.equal(right))
            && self
                .outer_join_keys
                .iter()
                .map(|column| column.unique_id)
                .eq(other.outer_join_keys.iter().map(|column| column.unique_id))
            && self
                .inner_join_keys
                .iter()
                .map(|column| column.unique_id)
                .eq(other.inner_join_keys.iter().map(|column| column.unique_id))
    }
}

impl PartialEq for SortItem {
    fn eq(&self, other: &Self) -> bool {
        self.col.equals(&other.col) && self.desc == other.desc
    }
}

impl Eq for SortItem {}

impl std::hash::Hash for SortItem {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.col.hash64());
        state.write_u8(u8::from(self.desc));
    }
}

impl PartialEq for MppPartitionColumn {
    fn eq(&self, other: &Self) -> bool {
        self.col.equals(&other.col) && self.collate_id == other.collate_id
    }
}

impl Eq for MppPartitionColumn {}

impl SortItem {
    /// A required order on `col`, ascending when `desc` is false.
    #[must_use]
    pub fn new(col: i64, desc: bool) -> Self {
        let mut column = tidb_expr::column::Column::default();
        column.unique_id = col;
        Self { col: column, desc }
    }

    /// Build a sort item from the complete source column.
    #[must_use]
    pub const fn from_column(col: tidb_expr::column::Column, desc: bool) -> Self {
        Self { col, desc }
    }

    /// Source-shaped owned memory accounting.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>() + self.col.orig_name.capacity()
    }

    /// Go `SortItem.Hash64`'s structural identity.
    #[must_use]
    pub fn hash64(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    /// Go `SortItem.Equals`.
    #[must_use]
    pub fn equals(&self, other: &Self) -> bool {
        self == other
    }
}

impl std::fmt::Display for SortItem {
    /// `SortItem.String()`: `{col asc}` or `{col desc}`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let direction = if self.desc { "desc" } else { "asc" };
        write!(f, "{{{} {direction}}}", column_string(&self.col))
    }
}

fn column_string(column: &tidb_expr::column::Column) -> String {
    if column.orig_name.is_empty() {
        format!("Column#{}", column.unique_id)
    } else {
        column.orig_name.clone()
    }
}

/// Go `ExplainPartitionBy` with the source normalized-column behavior.
#[must_use]
pub fn explain_partition_by(partition_by: &[SortItem], normalized: bool) -> String {
    if partition_by.is_empty() {
        return String::new();
    }
    let columns = partition_by
        .iter()
        .map(|item| {
            if normalized {
                tidb_expr::expr_util::column_explain_info_normalized(&item.col)
            } else {
                column_string(&item.col)
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("partition by {columns}")
}

/// Go `ExplainColumnList`.
#[must_use]
pub fn explain_column_list(columns: &[MppPartitionColumn]) -> Vec<u8> {
    columns
        .iter()
        .map(|column| {
            let collate = if tidb_datatype::new_collation_enabled() {
                collate_name_for_partition(column.collate_id)
            } else {
                "N/A".to_owned()
            };
            format!("[name: {}, collate: {collate}]", column_string(&column.col))
        })
        .collect::<Vec<_>>()
        .join(", ")
        .into_bytes()
}

/// The physical property a parent requires of a child.
///
/// `property.PhysicalProperty`. This port carries the fields that decide a
/// plan's shape -- including Go's root-only aggregation gate -- and the
/// additional fields consumed by the wired planner.
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
    /// Hash-partition columns required from an MPP child.
    pub mpp_partition_cols: Vec<MppPartitionColumn>,
    /// Required MPP partitioning kind.
    pub mpp_partition_tp: MppPartitionType,
    /// Go `SortItemsForPartition`: "these sort only need to sort the data of
    /// one partition, instead of global" — the MPP window paths fill it;
    /// everywhere else it stays empty, which is exactly Go's zero value.
    pub sort_items_for_partition: Vec<SortItem>,
    /// Go `CTEProducerStatus`: threaded through every child property a CTE
    /// consumer builds, so a producer that cannot run MPP poisons the whole
    /// sequence's MPP choice.
    pub cte_producer_status: CteProducerStatus,
    /// Vector-search requirement carried only by an MPP property.
    pub vector_prop: VectorProperty,
    /// Go `NoCopPushDown`: force aggregation to stay at the root. This is an
    /// essential property and must survive every child-property clone.
    pub no_cop_push_down: bool,
    /// Go `AdvisorySortItems`: order preferred by a DataSource for TopN over
    /// IndexMerge, but not required for property satisfaction.
    pub advisory_sort_items: Vec<SortItem>,
    /// Go `IndexJoinProp`; present only while planning an index join's inner
    /// child and the pass-through operators admitted by Go.
    pub index_join_prop: Option<IndexJoinRuntimeProp>,
    /// Prefix-index partial-order request.
    pub partial_order_info: Option<PartialOrderInfo>,
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
            mpp_partition_cols: Vec::new(),
            mpp_partition_tp: MppPartitionType::default(),
            sort_items_for_partition: Vec::new(),
            cte_producer_status: CteProducerStatus::default(),
            vector_prop: VectorProperty::default(),
            no_cop_push_down: false,
            advisory_sort_items: Vec::new(),
            index_join_prop: None,
            partial_order_info: None,
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
            mpp_partition_cols: Vec::new(),
            mpp_partition_tp: MppPartitionType::default(),
            sort_items_for_partition: Vec::new(),
            cte_producer_status: CteProducerStatus::default(),
            vector_prop: VectorProperty::default(),
            no_cop_push_down: false,
            advisory_sort_items: Vec::new(),
            index_join_prop: None,
            partial_order_info: None,
        }
    }

    /// Go `NewPhysicalProperty`, retaining complete column values.
    #[must_use]
    pub fn new_from_columns(
        task_tp: TaskType,
        cols: &[tidb_expr::column::Column],
        desc: bool,
        expected_cnt: f64,
        enforced: bool,
    ) -> Self {
        Self {
            sort_items: sort_items_from_columns(cols, desc),
            task_tp,
            expected_cnt,
            can_add_enforcer: enforced,
            ..Self::default()
        }
    }

    /// `CloneEssentialFields` (`physical_property.go:713`), over the ported
    /// field set. Two absences are Go's own: `CanAddEnforcer` is NOT copied
    /// (the clone defaults to false — an enforcer admission never rides
    /// down to a child property), and `indexJoinProp` is "default not to
    /// clone".
    #[must_use]
    pub fn clone_essential_fields(&self) -> Self {
        Self {
            sort_items: self.sort_items.clone(),
            sort_items_for_partition: self.sort_items_for_partition.clone(),
            task_tp: self.task_tp,
            expected_cnt: self.expected_cnt,
            can_add_enforcer: false,
            mpp_partition_cols: self.mpp_partition_cols.clone(),
            mpp_partition_tp: self.mpp_partition_tp,
            cte_producer_status: self.cte_producer_status,
            // Go intentionally does not clone VectorProp.
            vector_prop: VectorProperty::default(),
            no_cop_push_down: self.no_cop_push_down,
            advisory_sort_items: self.advisory_sort_items.clone(),
            index_join_prop: None,
            partial_order_info: self.partial_order_info.clone(),
        }
    }

    /// Go `NeedMPPExchangeByEquivalence`: whether a child hash key lies
    /// outside every equivalence closure of the required partition keys.
    #[must_use]
    pub fn need_mpp_exchange_by_equivalence(
        &self,
        current_partition_columns: &[MppPartitionColumn],
        fd: &tidb_funcdep::FdSet,
    ) -> bool {
        let required: Vec<_> = self
            .mpp_partition_cols
            .iter()
            .map(|column| {
                let closure =
                    fd.closure_of_equivalence(&tidb_funcdep::ColSet::new(&[column.col.unique_id]));
                (column, closure)
            })
            .collect();

        current_partition_columns.iter().any(|key| {
            !required.iter().any(|(required, closure)| {
                closure.has(key.col.unique_id)
                    && (key.collate_id >= 0 || key.collate_id == required.collate_id)
            })
        })
    }

    /// Go `IsSubsetOf`: return source-key offsets matching every required
    /// partition column, or `None` when any requirement is absent.
    #[must_use]
    pub fn is_subset_of(&self, keys: &[MppPartitionColumn]) -> Option<Vec<usize>> {
        if self.mpp_partition_cols.len() > keys.len() {
            return None;
        }
        self.mpp_partition_cols
            .iter()
            .map(|required| keys.iter().position(|key| required.equal(key)))
            .collect()
    }

    /// Go `AllColsFromSchema`.
    #[must_use]
    pub fn all_cols_from_schema(&self, schema: &tidb_expr::schema::Schema) -> bool {
        self.sort_items.iter().all(|item| {
            schema
                .columns
                .iter()
                .any(|column| column.unique_id == item.col.unique_id)
        })
    }

    /// Go `IsFlashProp`.
    #[must_use]
    pub const fn is_flash_prop(&self) -> bool {
        matches!(self.task_tp, TaskType::Mpp)
    }

    /// Go `GetAllPossibleChildTaskTypes`.
    #[must_use]
    pub fn all_possible_child_task_types(&self) -> Vec<TaskType> {
        if self.task_tp == TaskType::Root {
            vec![
                TaskType::CopSingleRead,
                TaskType::CopMultiRead,
                TaskType::Root,
            ]
        } else {
            vec![self.task_tp]
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
        self.sort_items_for_partition.len() == self.sort_items.len()
            && self
                .sort_items_for_partition
                .iter()
                .zip(&self.sort_items)
                .all(|(partition, order)| {
                    partition.col.unique_id == order.col.unique_id && partition.desc == order.desc
                })
    }

    /// `NeedKeepOrder`: whether the property requires maintaining order.
    ///
    #[must_use]
    pub fn need_keep_order(&self) -> bool {
        !self.is_sort_item_empty() || self.partial_order_info.is_some()
    }

    /// `AllSameOrder`: whether every item runs the same direction, and which.
    ///
    /// An EMPTY property answers `(true, false)`, which is what makes an
    /// unordered parent demand an ASCENDING child order rather than no answer.
    #[must_use]
    pub fn all_same_order(&self) -> (bool, bool) {
        all_same_order(&self.sort_items)
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
            .all(|(mine, theirs)| {
                mine.col.unique_id == theirs.col.unique_id && mine.desc == theirs.desc
            })
    }

    /// `GetSortDescForKeepOrder`: the direction a keep-order scan must run.
    #[must_use]
    pub fn sort_desc_for_keep_order(&self) -> bool {
        self.partial_order_info
            .as_ref()
            .filter(|partial| !partial.sort_items.is_empty())
            .map_or_else(
                || self.all_same_order().1,
                |partial| partial.all_same_order().1,
            )
    }

    /// Go `GetSortItemsForKeepOrder`, prioritizing partial order.
    #[must_use]
    pub fn sort_items_for_keep_order(&self) -> Vec<SortItem> {
        self.partial_order_info
            .as_ref()
            .filter(|partial| !partial.sort_items.is_empty())
            .map_or_else(
                || self.sort_items.clone(),
                |partial| partial.sort_items.clone(),
            )
    }

    /// Go `HashCode`, with every source field included in the same order.
    #[must_use]
    pub fn hash_code(&self) -> Vec<u8> {
        let mut code = Vec::new();
        encode_bool(&mut code, self.can_add_enforcer);
        tidb_codec::encode_int(&mut code, i64::from(self.task_tp.raw()));
        tidb_codec::encode_float(&mut code, self.expected_cnt);
        encode_sort_items(&mut code, &self.sort_items);
        if self.task_tp == TaskType::Mpp {
            tidb_codec::encode_int(&mut code, i64::from(self.mpp_partition_tp.raw()));
            for column in &self.mpp_partition_cols {
                encode_column_id(&mut code, column.col.unique_id);
                tidb_codec::encode_int(
                    &mut code,
                    if column.collate_id < 0 {
                        i64::from(column.collate_id)
                    } else {
                        1
                    },
                );
            }
            if let Some(vector) = &self.vector_prop.search_info {
                encode_column_id(&mut code, vector.column.unique_id);
                tidb_codec::encode_int(&mut code, i64::from(vector.fn_pb_code));
            }
        }
        tidb_codec::encode_int(&mut code, self.cte_producer_status as i64);
        if let Some(index_join) = &self.index_join_prop {
            for expression in &index_join.other_conditions {
                let mut expression = expression.clone();
                code.extend_from_slice(expression.hash_code());
            }
            for column in &index_join.outer_join_keys {
                encode_column_id(&mut code, column.unique_id);
            }
            for column in &index_join.inner_join_keys {
                encode_column_id(&mut code, column.unique_id);
            }
            tidb_codec::encode_float(&mut code, index_join.avg_inner_row_count);
            encode_bool(&mut code, index_join.table_range_scan);
        }
        encode_bool(&mut code, self.no_cop_push_down);
        if let Some(partial) = &self.partial_order_info {
            tidb_codec::encode_int(&mut code, 1);
            encode_sort_items(&mut code, &partial.sort_items);
        } else {
            tidb_codec::encode_int(&mut code, 0);
        }
        encode_sort_items(&mut code, &self.advisory_sort_items);
        code
    }

    /// Source-shaped owned memory accounting for the package fields.
    #[must_use]
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self.sort_items.capacity() * std::mem::size_of::<SortItem>()
            + self.sort_items_for_partition.capacity() * std::mem::size_of::<SortItem>()
            + self.mpp_partition_cols.capacity() * std::mem::size_of::<MppPartitionColumn>()
            + self.advisory_sort_items.capacity() * std::mem::size_of::<SortItem>()
            + self
                .partial_order_info
                .as_ref()
                .map_or(0, PartialOrderInfo::memory_usage)
    }
}

/// Go `SortItemsFromCols`.
#[must_use]
pub fn sort_items_from_columns(columns: &[tidb_expr::column::Column], desc: bool) -> Vec<SortItem> {
    columns
        .iter()
        .cloned()
        .map(|column| SortItem::from_column(column, desc))
        .collect()
}

fn all_same_order(items: &[SortItem]) -> (bool, bool) {
    let Some(first) = items.first() else {
        return (true, false);
    };
    if items.iter().any(|item| item.desc != first.desc) {
        return (false, false);
    }
    (true, first.desc)
}

fn encode_bool(code: &mut Vec<u8>, value: bool) {
    tidb_codec::encode_int(code, i64::from(value));
}

fn encode_column_id(code: &mut Vec<u8>, unique_id: i64) {
    // Go `Column.HashCode`: column flag followed by mem-comparable UniqueID.
    code.push(1);
    tidb_codec::encode_int(code, unique_id);
}

fn encode_sort_items(code: &mut Vec<u8>, items: &[SortItem]) {
    for item in items {
        encode_column_id(code, item.col.unique_id);
        encode_bool(code, item.desc);
    }
}

/// Go `NeedEnforceExchanger`.
#[must_use]
pub fn need_enforce_exchanger(
    current_type: MppPartitionType,
    current_hash_cols: &[MppPartitionColumn],
    required: &PhysicalProperty,
    fd: Option<&tidb_funcdep::FdSet>,
) -> bool {
    match required.mpp_partition_tp {
        MppPartitionType::Any => false,
        MppPartitionType::Broadcast => true,
        MppPartitionType::SinglePartition => current_type != MppPartitionType::SinglePartition,
        MppPartitionType::Hash | MppPartitionType::Unknown(_) => {
            if current_type != MppPartitionType::Hash {
                return true;
            }
            if let Some(fd) = fd.filter(|_| !current_hash_cols.is_empty()) {
                return required.need_mpp_exchange_by_equivalence(current_hash_cols, fd);
            }
            required.mpp_partition_cols.len() != current_hash_cols.len()
                || required
                    .mpp_partition_cols
                    .iter()
                    .zip(current_hash_cols)
                    .any(|(left, right)| !left.equal(right))
        }
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
    use super::{
        need_enforce_exchanger, CteProducerStatus, MppPartitionColumn, MppPartitionType,
        PartialOrderInfo, PhysicalProperty, SortItem, TaskType,
    };
    use tidb_funcdep::{ColSet, FdSet};

    fn cols(values: &[i64]) -> ColSet {
        ColSet::new(values)
    }

    fn partition_cols(values: &[i64]) -> Vec<MppPartitionColumn> {
        values
            .iter()
            .map(|value| MppPartitionColumn::new(*value, 0))
            .collect()
    }

    fn tpch_q3_fd() -> FdSet {
        let mut fd = FdSet::new();
        fd.add_equivalence(cols(&[1, 10]), cols(&[1, 10]));
        fd.add_strict(cols(&[1]), cols(&[2, 3, 4, 5, 6, 8]));
        fd.add_strict(cols(&[]), cols(&[7]));
        fd.add_strict(cols(&[9]), cols(&[10, 11, 12, 13, 14, 15, 16, 17]));
        fd.add_strict(
            cols(&[10, 21]),
            cols(&[19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33]),
        );
        fd.add_equivalence(cols(&[9]), cols(&[18]));
        fd.add_equivalence(cols(&[1]), cols(&[10]));
        fd
    }

    fn fd_2_3_equivalent_to_4() -> FdSet {
        let mut fd = FdSet::new();
        fd.add_equivalence(cols(&[2]), cols(&[4]));
        fd.add_equivalence(cols(&[3]), cols(&[4]));
        fd
    }

    fn fd_2_equivalent_to_4_5() -> FdSet {
        let mut fd = FdSet::new();
        fd.add_equivalence(cols(&[2]), cols(&[4]));
        fd.add_equivalence(cols(&[2]), cols(&[5]));
        fd
    }

    /// Pinned Go `TestNeedEnforceExchangerWithHashByEquivalence`, all six rows.
    #[test]
    fn need_mpp_exchange_by_equivalence_six_case_fd_table() {
        let cases = [
            (tpch_q3_fd(), &[18, 13, 16][..], &[9][..], false),
            (tpch_q3_fd(), &[18, 13, 16][..], &[9, 13][..], false),
            (tpch_q3_fd(), &[18, 13, 16][..], &[9, 17][..], true),
            (tpch_q3_fd(), &[18, 13, 16][..], &[1, 17][..], true),
            (
                fd_2_3_equivalent_to_4(),
                &[1, 2, 3][..],
                &[1, 2, 4, 5][..],
                true,
            ),
            (fd_2_equivalent_to_4_5(), &[1, 2][..], &[1, 2, 5][..], false),
        ];

        for (fd, required, current, expected) in cases {
            let property = PhysicalProperty {
                mpp_partition_cols: partition_cols(required),
                ..PhysicalProperty::default()
            };
            assert_eq!(
                property.need_mpp_exchange_by_equivalence(&partition_cols(current), &fd),
                expected,
                "required={required:?} current={current:?}"
            );
        }
    }

    #[test]
    fn mpp_exchange_equivalence_obeys_collation_id_rule() {
        let property = PhysicalProperty {
            mpp_partition_cols: vec![MppPartitionColumn::new(1, -45)],
            ..PhysicalProperty::default()
        };
        let fd = FdSet::new();
        assert!(
            !property.need_mpp_exchange_by_equivalence(&[MppPartitionColumn::new(1, -45)], &fd,)
        );
        assert!(property.need_mpp_exchange_by_equivalence(&[MppPartitionColumn::new(1, -46)], &fd,));
        assert!(!property.need_mpp_exchange_by_equivalence(&[MppPartitionColumn::new(1, 45)], &fd,));
    }

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
            "Prop{cols: [{Column#7 desc}], TaskTp: copSingleReadTask, expectedCount: 10}"
        );
    }

    #[test]
    fn essential_clone_preserves_cte_and_no_cop_but_not_enforcer_or_index_join() {
        let prop = PhysicalProperty {
            can_add_enforcer: true,
            mpp_partition_cols: vec![MppPartitionColumn::new(8, -45)],
            mpp_partition_tp: super::MppPartitionType::Hash,
            cte_producer_status: CteProducerStatus::AllCteCanMpp,
            no_cop_push_down: true,
            ..PhysicalProperty::default()
        };
        let cloned = prop.clone_essential_fields();
        assert_eq!(cloned.cte_producer_status, CteProducerStatus::AllCteCanMpp);
        assert!(cloned.no_cop_push_down);
        assert!(!cloned.can_add_enforcer);
        assert!(cloned.index_join_prop.is_none());
        assert_eq!(cloned.mpp_partition_cols, prop.mpp_partition_cols);
        assert_eq!(cloned.mpp_partition_tp, prop.mpp_partition_tp);
    }

    #[test]
    fn partial_order_is_prioritized_for_keep_order() {
        let prop = PhysicalProperty {
            sort_items: vec![SortItem::new(1, false)],
            partial_order_info: Some(PartialOrderInfo {
                sort_items: vec![SortItem::new(2, true)],
            }),
            ..PhysicalProperty::default()
        };
        assert!(prop.need_keep_order());
        assert!(prop.sort_desc_for_keep_order());
        assert_eq!(prop.sort_items_for_keep_order()[0].col.unique_id, 2);
        assert!(prop.clone_essential_fields().partial_order_info.is_some());
    }

    #[test]
    fn exchanger_enforcement_matches_source_switch() {
        let hash = MppPartitionColumn::new(1, 0);
        let mut required = PhysicalProperty::default();
        assert!(!need_enforce_exchanger(
            MppPartitionType::Any,
            &[],
            &required,
            None
        ));
        required.mpp_partition_tp = MppPartitionType::Broadcast;
        assert!(need_enforce_exchanger(
            MppPartitionType::Broadcast,
            &[],
            &required,
            None
        ));
        required.mpp_partition_tp = MppPartitionType::SinglePartition;
        assert!(!need_enforce_exchanger(
            MppPartitionType::SinglePartition,
            &[],
            &required,
            None
        ));
        required.mpp_partition_tp = MppPartitionType::Hash;
        required.mpp_partition_cols = vec![hash.clone()];
        assert!(!need_enforce_exchanger(
            MppPartitionType::Hash,
            &[hash],
            &required,
            None
        ));
        assert!(need_enforce_exchanger(
            MppPartitionType::Broadcast,
            &[],
            &required,
            None
        ));
    }

    #[test]
    fn hash_identity_includes_source_hashed_fields() {
        let base = PhysicalProperty::default();
        let base_hash = base.hash_code();
        for changed in [
            PhysicalProperty {
                can_add_enforcer: true,
                ..base.clone()
            },
            PhysicalProperty {
                no_cop_push_down: true,
                ..base.clone()
            },
            PhysicalProperty {
                advisory_sort_items: vec![SortItem::new(1, false)],
                ..base.clone()
            },
            PhysicalProperty {
                partial_order_info: Some(PartialOrderInfo::default()),
                ..base.clone()
            },
        ] {
            assert_ne!(changed.hash_code(), base_hash);
        }
    }
}
