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

//! `pkg/util/chunk/chunk.go`: the `Chunk`, a batch of rows in columnar layout.
//!
//! A `Chunk` holds one [`Column`] per output field; row `i` is the `i`-th cell
//! of every column. Executors produce chunks and expression evaluation reads
//! rows out of them (see [`crate::row::Row`]).
//!
//! Ported: construction, required-row/capacity growth, selection-aware row
//! access, typed append paths, range and projected batch appends, truncate and
//! reconstruct transforms, deep/selected copies, whole-column aliases,
//! alias-preserving swaps, column-vector swapping, and allocator/global-pool
//! ownership transfer.

use crate::chunk_util::MSG_ERR_SEL_NOT_NIL;
use crate::column::Column;
use crate::column_slot::{ColumnHandle, ColumnRead, ColumnSlot, ColumnWrite};
use crate::compare::{compare, sort_search};
use crate::row::Row;
use std::cmp::Ordering;
use tidb_datatype::{
    Datum, FieldType, GoString, GoStringSource, MyDecimal, MySqlDuration, Time, VectorFloat32,
};

/// Go `chunk.InitialCapacity`: the capacity a chunk grows to when it is renewed
/// from a chunk that had no capacity of its own.
pub const INITIAL_CAPACITY: usize = 32;

/// Go `chunk.ZeroCapacity`: the public executor-builder sentinel requesting a
/// first batch that grows from zero capacity.
pub const ZERO_CAPACITY: usize = 0;

/// Go `chunk.Chunk`: a columnar batch of rows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Chunk {
    /// Go `sel`: the selected physical row indices, or `None` when all rows are
    /// selected.
    pub(crate) sel: Option<Vec<usize>>,
    pub(crate) columns: Vec<ColumnSlot>,
    /// Go distinguishes a nil `columns` slice from a non-nil zero-length
    /// slice. `Vec::is_empty` cannot represent that distinction, so keep the
    /// construction state explicitly. Renewal of a valid zero-column chunk
    /// must preserve its capacity policy; only a literal zero-value Chunk has
    /// nil-column semantics.
    columns_initialized: bool,
    /// Go `numVirtualRows`: the row count when the chunk holds no columns.
    pub(crate) num_virtual_rows: usize,
    /// Go `capacity`: the max rows this chunk was sized for.
    pub(crate) capacity: usize,
    /// Go `requiredRows`: how many rows the parent executor wants.
    pub(crate) required_rows: usize,
    /// Go `inCompleteChunk`: some columns are intentionally unfilled.
    in_complete_chunk: bool,
}

impl Chunk {
    /// Go `New`: a chunk for `fields`, capped at `min(capacity, max_chunk_size)`
    /// rows, with `required_rows = max_chunk_size`.
    #[must_use]
    pub fn new(fields: &[FieldType], capacity: usize, max_chunk_size: usize) -> Self {
        let capacity = capacity.min(max_chunk_size);
        Chunk {
            sel: None,
            columns: fields
                .iter()
                .map(|f| Column::new_column(f, capacity))
                .map(ColumnSlot::new)
                .collect(),
            columns_initialized: true,
            num_virtual_rows: 0,
            capacity,
            required_rows: max_chunk_size,
            in_complete_chunk: false,
        }
    }

    /// Go `NewChunkWithCapacity`.
    #[must_use]
    pub fn new_with_capacity(fields: &[FieldType], capacity: usize) -> Self {
        Chunk::new(fields, capacity, capacity)
    }

    /// Go `NewEmptyChunk`: columns typed for `fields` with no preallocation.
    #[must_use]
    pub fn new_empty(fields: &[FieldType]) -> Self {
        Chunk {
            columns: fields
                .iter()
                .map(Column::new_empty_column)
                .map(ColumnSlot::new)
                .collect(),
            columns_initialized: true,
            ..Chunk::default()
        }
    }

    /// Builds a source-shaped initialized chunk from columns transferred out
    /// of an allocator or pool.
    pub(crate) fn from_reusable_columns(
        columns: Vec<Column>,
        capacity: usize,
        required_rows: usize,
    ) -> Self {
        Chunk {
            sel: None,
            columns: columns.into_iter().map(ColumnSlot::new).collect(),
            columns_initialized: true,
            num_virtual_rows: 0,
            capacity,
            required_rows,
            in_complete_chunk: false,
        }
    }

    /// Builds a stable metadata snapshot whose columns retain the same owners.
    ///
    /// Row-container reads use this to release the container lock before
    /// returning. A later spill may drop the list's chunk, while this snapshot
    /// keeps the exact column data alive without copying it.
    pub(crate) fn alias_snapshot(&mut self) -> Self {
        Chunk {
            sel: self.sel.clone(),
            columns: self.columns.iter_mut().map(ColumnSlot::alias).collect(),
            columns_initialized: self.columns_initialized,
            num_virtual_rows: self.num_virtual_rows,
            capacity: self.capacity,
            required_rows: self.required_rows,
            in_complete_chunk: self.in_complete_chunk,
        }
    }

    /// Go `NumCols`.
    #[must_use]
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    /// Go `Chunk.MemoryUsage`: the bytes this chunk's columns hold, summed
    /// over `Column::memory_usage`.
    ///
    /// This is the number Go's memory-tracked operators consume per chunk, so
    /// it is capacity-based rather than length-based: an operator that keeps a
    /// chunk keeps its whole allocation, not just the rows in use.
    #[must_use]
    pub fn memory_usage(&self) -> i64 {
        self.columns
            .iter()
            .map(|column| column.read().memory_usage())
            .sum()
    }

    /// Go `RequiredRows`.
    #[must_use]
    pub fn required_rows(&self) -> usize {
        self.required_rows
    }

    /// Go `SetRequiredRows`. Values outside `1..=max_chunk_size` normalize to
    /// `max_chunk_size`.
    pub fn set_required_rows(&mut self, required_rows: isize, max_chunk_size: usize) -> &mut Self {
        self.required_rows = match usize::try_from(required_rows) {
            Ok(value) if value > 0 && value <= max_chunk_size => value,
            _ => max_chunk_size,
        };
        self
    }

    /// Go `IsFull`.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.num_rows() >= self.required_rows
    }

    /// Go `SetInCompleteChunk`.
    pub fn set_incomplete_chunk(&mut self, incomplete: bool) {
        self.in_complete_chunk = incomplete;
    }

    /// Go `IsInCompleteChunk`.
    #[must_use]
    pub fn is_incomplete_chunk(&self) -> bool {
        self.in_complete_chunk
    }

    /// Go `NumRows`: the logical row count (selection aware; virtual for a
    /// column-less or incomplete chunk).
    #[must_use]
    pub fn num_rows(&self) -> usize {
        if let Some(sel) = &self.sel {
            return sel.len();
        }
        if self.in_complete_chunk || self.num_cols() == 0 {
            return self.num_virtual_rows;
        }
        self.columns[0].read().rows()
    }

    /// Go `Column`: the column at `col_idx`.
    #[must_use]
    pub fn column(&self, col_idx: usize) -> ColumnRead<'_> {
        self.columns[col_idx].read()
    }

    /// A mutable borrow of the column at `col_idx`.
    pub fn column_mut(&mut self, col_idx: usize) -> ColumnWrite<'_> {
        self.columns[col_idx].write()
    }

    /// Go `Prune`: retain the requested column owners in the requested order.
    /// Duplicate indices remain duplicate aliases. Safe lazy promotion requires
    /// a mutable source borrow, but the source's values and metadata do not
    /// change.
    #[must_use]
    pub fn prune(&mut self, used_col_idxs: &[usize]) -> Chunk {
        let columns = used_col_idxs
            .iter()
            .map(|&index| self.columns[index].alias())
            .collect();
        Chunk {
            sel: self.sel.clone(),
            columns,
            columns_initialized: true,
            num_virtual_rows: self.num_virtual_rows,
            capacity: self.capacity,
            required_rows: self.required_rows,
            in_complete_chunk: self.in_complete_chunk,
        }
    }

    /// Go `MakeRef`: make `destination` designate `source`'s column owner.
    pub fn make_ref(&mut self, source: usize, destination: usize) {
        let alias = self.columns[source].alias();
        self.columns[destination] = alias;
    }

    /// Obtain a transferable handle to a column owner. The first handle lazily
    /// promotes an owned slot; subsequent handles clone only the owner `Arc`.
    pub fn column_handle(&mut self, index: usize) -> ColumnHandle {
        ColumnHandle {
            slot: self.columns[index].alias(),
        }
    }

    /// Go `MakeRefTo`: make one destination slot designate a source-chunk
    /// owner. Neither chunk may carry a selection vector.
    pub fn make_ref_to(
        &mut self,
        destination: usize,
        source: &mut Chunk,
        source_index: usize,
    ) -> Result<(), &'static str> {
        if self.sel.is_some() || source.sel.is_some() {
            return Err(MSG_ERR_SEL_NOT_NIL);
        }
        let alias = source.columns[source_index].alias();
        self.columns[destination] = alias;
        Ok(())
    }

    /// Go `SetCol`: replace one owner and return the displaced owner. Installing
    /// the same identity is a no-op and returns `None`.
    pub fn set_col(&mut self, index: usize, column: ColumnHandle) -> Option<ColumnHandle> {
        if self.columns[index].same_identity(&column.slot) {
            return None;
        }
        Some(ColumnHandle {
            slot: std::mem::replace(&mut self.columns[index], column.slot),
        })
    }

    /// Whether two slots, possibly in different chunks, designate one mutable
    /// column owner. Value equality alone does not imply identity.
    #[must_use]
    pub fn columns_share_identity(&self, index: usize, other: &Chunk, other_index: usize) -> bool {
        self.columns[index].same_identity(&other.columns[other_index])
    }

    fn leftmost_alias(&self, index: usize) -> usize {
        (0..index)
            .find(|&candidate| self.columns[candidate].same_identity(&self.columns[index]))
            .unwrap_or(index)
    }

    fn alias_indexes(&self, owner_index: usize) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| {
                slot.same_identity(&self.columns[owner_index])
                    .then_some(index)
            })
            .collect()
    }

    fn rebuild_aliases(&mut self, owner_index: usize, indexes: &[usize]) {
        for &index in indexes {
            if index != owner_index {
                self.make_ref(owner_index, index);
            }
        }
    }

    /// Go private `swapColumn`, for two distinct chunks.
    pub(crate) fn swap_column_with(
        &mut self,
        index: usize,
        other: &mut Chunk,
        other_index: usize,
    ) -> Result<(), &'static str> {
        if self.sel.is_some() || other.sel.is_some() {
            return Err(MSG_ERR_SEL_NOT_NIL);
        }
        let owner_index = self.leftmost_alias(index);
        let other_owner_index = other.leftmost_alias(other_index);
        if self.columns[owner_index].same_identity(&other.columns[other_owner_index]) {
            return Ok(());
        }
        let indexes = self.alias_indexes(owner_index);
        let other_indexes = other.alias_indexes(other_owner_index);
        std::mem::swap(
            &mut self.columns[owner_index],
            &mut other.columns[other_owner_index],
        );
        self.rebuild_aliases(owner_index, &indexes);
        other.rebuild_aliases(other_owner_index, &other_indexes);
        Ok(())
    }

    /// Go private `swapColumn`, when both indexes belong to this chunk.
    pub fn swap_column(&mut self, index: usize, other_index: usize) -> Result<(), &'static str> {
        if self.sel.is_some() {
            return Err(MSG_ERR_SEL_NOT_NIL);
        }
        let owner_index = self.leftmost_alias(index);
        let other_owner_index = self.leftmost_alias(other_index);
        if owner_index == other_owner_index {
            return Ok(());
        }
        let indexes = self.alias_indexes(owner_index);
        let other_indexes = self.alias_indexes(other_owner_index);
        self.columns.swap(owner_index, other_owner_index);
        self.rebuild_aliases(owner_index, &indexes);
        self.rebuild_aliases(other_owner_index, &other_indexes);
        Ok(())
    }

    /// Go `GetRow`: the logical row at `idx`, mapped through the selection.
    #[must_use]
    pub fn get_row(&self, idx: usize) -> Row<'_> {
        let physical = match &self.sel {
            Some(sel) => sel[idx],
            None => idx,
        };
        Row::new(self, physical)
    }

    /// Go `LowerBound`: on the non-decreasing column `col_idx`, the smallest
    /// index whose value is not less than `d`, plus whether a probed row was
    /// equal to `d`.
    ///
    /// Go reads the last row before searching, so this panics on an empty
    /// chunk exactly as Go does.
    #[must_use]
    pub fn lower_bound(&self, col_idx: usize, d: &Datum) -> (usize, bool) {
        if compare(self.get_row(self.num_rows() - 1), col_idx, d) == Ordering::Less {
            return (self.num_rows(), false);
        }
        let mut matched = false;
        let index = sort_search(self.num_rows(), |i| {
            let ordering = compare(self.get_row(i), col_idx, d);
            if ordering == Ordering::Equal {
                matched = true;
            }
            ordering != Ordering::Less
        });
        (index, matched)
    }

    /// Go `UpperBound`: on the non-decreasing column `col_idx`, the smallest
    /// index whose value is larger than `d`.
    #[must_use]
    pub fn upper_bound(&self, col_idx: usize, d: &Datum) -> usize {
        sort_search(self.num_rows(), |i| {
            compare(self.get_row(i), col_idx, d) == Ordering::Greater
        })
    }

    /// Go `SetSel`: install (or, with `None`, drop) the selection vector.
    pub fn set_sel(&mut self, sel: Option<Vec<usize>>) {
        self.sel = sel;
    }

    /// Go `SwapColumns`: swap only selection, column ownership, and virtual
    /// row count. Capacity, required rows, and incomplete state belong to the
    /// receiving chunk and deliberately remain in place.
    pub fn swap_columns(&mut self, other: &mut Chunk) {
        std::mem::swap(&mut self.sel, &mut other.sel);
        std::mem::swap(&mut self.columns, &mut other.columns);
        std::mem::swap(
            &mut self.columns_initialized,
            &mut other.columns_initialized,
        );
        std::mem::swap(&mut self.num_virtual_rows, &mut other.num_virtual_rows);
    }

    /// Go `Sel`: the installed selection vector, if any.
    #[must_use]
    pub fn sel(&self) -> Option<&[usize]> {
        self.sel.as_deref()
    }

    /// Go `reCalcCapacity`: a full chunk doubles (from `INITIAL_CAPACITY` when
    /// it had none), capped at `max_chunk_size`; an unfilled chunk keeps its
    /// capacity.
    #[must_use]
    fn re_calc_capacity(&self, max_chunk_size: usize) -> usize {
        if self.num_rows() < self.capacity {
            return self.capacity;
        }
        let new_capacity = self.capacity * 2;
        let new_capacity = if new_capacity == 0 {
            INITIAL_CAPACITY
        } else {
            new_capacity
        };
        new_capacity.min(max_chunk_size)
    }

    /// Go `renewWithCapacity`: a new, EMPTY chunk with this chunk's column
    /// shape (each column's `typeSize`, not its field type) at `capacity`.
    ///
    /// Go's `chk.columns == nil` short-circuit returns a chunk that carries
    /// only `inCompleteChunk`; an empty `columns` vector reproduces it.
    #[must_use]
    pub fn renew_with_capacity(&self, capacity: usize, required_rows: usize) -> Chunk {
        if !self.columns_initialized {
            return Chunk {
                in_complete_chunk: self.in_complete_chunk,
                ..Chunk::default()
            };
        }
        Chunk {
            sel: None,
            columns: self
                .columns
                .iter()
                .map(|col| {
                    ColumnSlot::new(Column::new_column_with_type_size(
                        col.read().type_size(),
                        capacity,
                    ))
                })
                .collect(),
            columns_initialized: true,
            num_virtual_rows: 0,
            capacity,
            required_rows,
            in_complete_chunk: self.in_complete_chunk,
        }
    }

    /// Go `Renew`: [`Chunk::renew_with_capacity`] at the grown capacity.
    #[must_use]
    pub fn renew(&self, max_chunk_size: usize) -> Chunk {
        self.renew_with_capacity(self.re_calc_capacity(max_chunk_size), max_chunk_size)
    }

    /// Go `numVirtualRows`: the field itself, which the join copy helpers and
    /// their tests assert on directly.
    #[must_use]
    pub fn num_virtual_rows(&self) -> usize {
        self.num_virtual_rows
    }

    /// Go `SetNumVirtualRows`.
    pub fn set_num_virtual_rows(&mut self, num_virtual_rows: usize) {
        self.num_virtual_rows = num_virtual_rows;
    }

    /// Go `Capacity`.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Go `Reset`: clear all rows, keeping the columns' element types so the
    /// memory can be reused.
    pub fn reset(&mut self) {
        self.sel = None;
        // Go returns immediately for a literal nil `columns` slice, retaining
        // `numVirtualRows`. Constructed zero-column chunks are non-nil and do
        // clear it, so the explicit initialization bit is the discriminator.
        if !self.columns_initialized {
            return;
        }
        for col in &mut self.columns {
            col.write().reset();
        }
        self.num_virtual_rows = 0;
    }

    /// Go `resetForReuse`: decouple all column owners and restore the literal
    /// zero-value chunk metadata while returning each UNIQUE column owner to
    /// the caller. Aliased slots yield one owner; a cross-chunk live owner is
    /// not reusable yet.
    pub(crate) fn take_columns_for_reuse(&mut self) -> Vec<Column> {
        self.sel = None;
        self.num_virtual_rows = 0;
        self.capacity = 0;
        self.required_rows = 0;
        self.in_complete_chunk = false;
        self.columns_initialized = false;
        let slots = std::mem::take(&mut self.columns);
        let mut unique_slots: Vec<ColumnSlot> = Vec::with_capacity(slots.len());
        for slot in slots {
            if unique_slots
                .iter()
                .any(|existing| existing.same_identity(&slot))
            {
                continue;
            }
            unique_slots.push(slot);
        }
        unique_slots
            .into_iter()
            .filter_map(|slot| slot.into_unique_column().ok())
            .collect()
    }

    /// Go allocator `resetForReuse`: drop column owners (which independently
    /// enqueue their registered columns) while preserving the empty slot
    /// allocation cached with the chunk shell.
    pub(crate) fn clear_columns_for_allocator(&mut self) {
        self.sel = None;
        self.num_virtual_rows = 0;
        self.capacity = 0;
        self.required_rows = 0;
        self.in_complete_chunk = false;
        self.columns_initialized = true;
        self.columns.clear();
    }

    /// Remove allocator provenance before transferring a lease into an
    /// ordinary independently owned chunk.
    pub(crate) fn detach_allocator_registrations(&mut self) {
        for column in &mut self.columns {
            column.detach_registration();
        }
    }

    pub(crate) fn attach_allocator_registrations(
        &mut self,
        registrations: impl IntoIterator<Item = Option<crate::alloc::ColumnRecycleRegistration>>,
    ) {
        for (column, registration) in self.columns.iter_mut().zip(registrations) {
            if let Some(registration) = registration {
                column.attach_registration(registration);
            }
        }
    }

    /// Populate a chunk shell recovered from the allocator's free list.
    pub(crate) fn restore_reusable_columns(
        &mut self,
        columns: Vec<Column>,
        capacity: usize,
        required_rows: usize,
    ) {
        debug_assert!(self.columns.is_empty());
        self.columns
            .extend(columns.into_iter().map(ColumnSlot::new));
        self.columns_initialized = true;
        self.capacity = capacity;
        self.required_rows = required_rows;
    }

    /// Go `Chunk.Destroy`: return this chunk's columns to the global pool for
    /// `init_capacity`.
    pub fn destroy(&mut self, init_capacity: usize, fields: &[FieldType]) {
        crate::pool::put_chunk_from_pool(init_capacity, fields, self);
    }

    /// Go `GrowAndReset`: grow only when the current chunk is full, otherwise
    /// retain its allocation and clear rows.
    pub fn grow_and_reset(&mut self, max_chunk_size: usize) {
        self.sel = None;
        if !self.columns_initialized {
            return;
        }
        let new_capacity = self.re_calc_capacity(max_chunk_size);
        if new_capacity <= self.capacity {
            self.reset();
            return;
        }
        self.columns = self
            .columns
            .iter()
            .map(|column| {
                ColumnSlot::new(Column::new_column_with_type_size(
                    column.read().type_size(),
                    new_capacity,
                ))
            })
            .collect();
        self.capacity = new_capacity;
        self.num_virtual_rows = 0;
        self.required_rows = max_chunk_size;
    }

    /// Go `appendSel`: when appending to column 0 of a selection-carrying chunk,
    /// record the new physical row as selected.
    ///
    /// Column 0 is only consulted when a selection is present (Go's
    /// `colIdx == 0 && c.sel != nil`); a column-less chunk never carries a
    /// selection, so this must not touch `columns[0]` otherwise.
    fn append_sel(&mut self, col_idx: usize) {
        if col_idx == 0 {
            if let Some(sel) = &mut self.sel {
                let len = self.columns[0].read().rows();
                sel.push(len);
            }
        }
    }

    /// Go `AppendNull`.
    pub fn append_null(&mut self, col_idx: usize) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_null();
    }

    /// Go `AppendInt64`.
    pub fn append_int64(&mut self, col_idx: usize, value: i64) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_int64(value);
    }

    /// Go `AppendUint64`.
    pub fn append_uint64(&mut self, col_idx: usize, value: u64) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_uint64(value);
    }

    /// Go `AppendFloat32`.
    pub fn append_float32(&mut self, col_idx: usize, value: f32) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_float32(value);
    }

    /// Go `AppendFloat64`.
    pub fn append_float64(&mut self, col_idx: usize, value: f64) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_float64(value);
    }

    /// Go `AppendTime`.
    pub fn append_time(&mut self, col_idx: usize, value: Time) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_time(value);
    }

    /// Go `AppendDuration` (fsp is ignored, as in Go).
    pub fn append_duration(&mut self, col_idx: usize, value: MySqlDuration) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_duration(value);
    }

    /// Go `AppendMyDecimal`.
    pub fn append_my_decimal(&mut self, col_idx: usize, value: &MyDecimal) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_my_decimal(value);
    }

    /// Go `AppendString`.
    pub fn append_string(&mut self, col_idx: usize, value: impl GoStringSource) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_string(value);
    }

    /// Go `AppendBytes`.
    pub fn append_bytes(&mut self, col_idx: usize, value: &[u8]) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_bytes(value);
    }

    /// Go `AppendJSON`: a JSON cell is the var-length byte string
    /// `type code || value`, exactly the encoding `BinaryJSON` carries on the
    /// wire and in a row value.
    pub fn append_json(&mut self, col_idx: usize, value: &tidb_datatype::BinaryJSON) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_json(value);
    }

    /// Go `AppendEnum`.
    pub fn append_enum(&mut self, col_idx: usize, value: &tidb_datatype::MysqlEnum) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_enum(value);
    }

    /// Go `AppendSet`.
    pub fn append_set(&mut self, col_idx: usize, value: &tidb_datatype::MysqlSet) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_set(value);
    }

    /// Go `AppendVectorFloat32`.
    pub fn append_vector_float32(&mut self, col_idx: usize, value: &VectorFloat32) {
        self.append_sel(col_idx);
        self.columns[col_idx].write().append_vector_float32(value);
    }

    /// Go `AppendDatum`: append a [`Datum`] value into column `col_idx`,
    /// dispatching on its kind (the inverse of [`Row::get_datum`]).
    ///
    /// A `Datum::Decimal` carries the digit-string `Decimal`, so it reaches
    /// the raw 40-byte cell through `MyDecimal::from_string` over its
    /// canonical text -- the same text `Row::get_datum` reads back out. A
    /// value too large for the `MyDecimal` buffer panics rather than being
    /// silently truncated into the cell; callers holding a `MyDecimal`
    /// already should use the exact [`Chunk::append_my_decimal`].
    ///
    /// Supports the kinds whose column storage exists (NULL, int/uint, real/
    /// float32, string/bytes, binary literal, time, duration, decimal, JSON,
    /// enum, set). The two range-only sentinels have no source switch arm and
    /// are exact no-ops; Rust has no untyped `KindInterface` datum variant.
    pub fn append_datum(&mut self, col_idx: usize, datum: &Datum) {
        match datum {
            Datum::Null => self.append_null(col_idx),
            Datum::Int(i) => self.append_int64(col_idx, *i),
            Datum::UInt(u) => self.append_uint64(col_idx, *u),
            Datum::Real(f) => self.append_float64(col_idx, *f),
            Datum::Float32(f) => {
                self.append_sel(col_idx);
                self.columns[col_idx].write().append_float32(*f as f32);
            }
            Datum::String(s) => self.append_bytes(col_idx, s.bytes()),
            Datum::Bytes(b) | Datum::Raw(b) => self.append_bytes(col_idx, b),
            // A hex or bit literal lives in a binary `VarString` column, so
            // its cell is the literal's own bytes -- which is how Go stores
            // `KindBinaryLiteral`/`KindMysqlBit` in a chunk too.
            Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
                self.append_bytes(col_idx, literal.as_bytes());
            }
            Datum::Json(value) => self.append_json(col_idx, value),
            Datum::Enum(value, _) => self.append_enum(col_idx, value),
            Datum::Set(value, _) => self.append_set(col_idx, value),
            Datum::VectorFloat32(value) => self.append_vector_float32(col_idx, value),
            Datum::Time(t) => self.append_time(col_idx, *t),
            Datum::Duration(d) => self.append_duration(col_idx, *d),
            Datum::Decimal(dec) => {
                let text = dec.to_string();
                let (value, err) = MyDecimal::from_string(text.as_bytes());
                assert!(
                    err.is_none(),
                    "Chunk::append_datum: decimal {text} does not fit a MyDecimal cell ({err:?})"
                );
                self.append_my_decimal(col_idx, &value);
            }
            Datum::MinNotNull | Datum::MaxValue => {}
        }
    }

    /// Go `AppendPartialRow`: append `row`'s cells into this chunk's columns
    /// starting at `col_off`.
    pub fn append_partial_row(&mut self, col_off: usize, row: Row<'_>) {
        self.append_sel(col_off);
        let source = row.chunk().expect("cannot append the empty Row sentinel");
        for (i, src_col) in source.columns.iter().enumerate() {
            Self::append_cell_between(&mut self.columns[col_off + i], src_col, row.idx());
        }
    }

    fn append_cell_between(destination: &mut ColumnSlot, source: &ColumnSlot, row: usize) {
        let (not_null, source_is_fixed, cell) = {
            let source = source.read();
            let raw = source.get_raw(row);
            let cell = raw.to_vec();
            (!source.is_null(row), source.is_fixed(), cell)
        };
        destination
            .write()
            .append_prepared_cell(not_null, source_is_fixed, &cell);
    }

    /// Go `AppendRow`: append a whole row (from another chunk) to this chunk.
    pub fn append_row(&mut self, row: Row<'_>) {
        self.append_partial_row(0, row);
        self.num_virtual_rows += 1;
    }

    /// Go `AppendPartialRowByColIdxs`. `None` is Go's nil slice (all columns),
    /// while `Some(&[])` deliberately appends no physical cells.
    pub fn append_partial_row_by_col_idxs(
        &mut self,
        col_off: usize,
        row: Row<'_>,
        col_idxs: Option<&[usize]>,
    ) -> usize {
        let Some(col_idxs) = col_idxs else {
            self.append_partial_row(col_off, row);
            return row.len();
        };
        self.append_sel(col_off);
        for (dst_offset, &src_index) in col_idxs.iter().enumerate() {
            Self::append_cell_between(
                &mut self.columns[col_off + dst_offset],
                &row.chunk()
                    .expect("cannot append the empty Row sentinel")
                    .columns[src_index],
                row.idx(),
            );
        }
        col_idxs.len()
    }

    /// Go `AppendRowByColIdxs`.
    pub fn append_row_by_col_idxs(&mut self, row: Row<'_>, col_idxs: Option<&[usize]>) -> usize {
        let width = self.append_partial_row_by_col_idxs(0, row, col_idxs);
        self.num_virtual_rows += 1;
        width
    }

    /// Go `AppendRowsByColIdxs`.
    pub fn append_rows_by_col_idxs(
        &mut self,
        rows: &[Row<'_>],
        col_idxs: Option<&[usize]>,
    ) -> usize {
        if col_idxs.is_none() {
            if rows.is_empty() {
                return 0;
            }
            self.append_rows(rows);
            return rows[0].len().saturating_mul(rows.len());
        }
        let col_idxs = col_idxs.expect("checked Some");
        for &row in rows {
            self.append_partial_row_by_col_idxs(0, row, Some(col_idxs));
        }
        self.num_virtual_rows += rows.len();
        col_idxs.len().saturating_mul(rows.len())
    }

    /// Go `AppendPartialRows`.
    pub fn append_partial_rows(&mut self, col_off: usize, rows: &[Row<'_>]) {
        for dst_offset in 0..self.columns.len().saturating_sub(col_off) {
            for &row in rows {
                if dst_offset == 0 {
                    self.append_sel(col_off);
                }
                Self::append_cell_between(
                    &mut self.columns[col_off + dst_offset],
                    &row.chunk()
                        .expect("cannot append the empty Row sentinel")
                        .columns[dst_offset],
                    row.idx(),
                );
            }
        }
    }

    /// Go `AppendRows`.
    pub fn append_rows(&mut self, rows: &[Row<'_>]) {
        self.append_partial_rows(0, rows);
        self.num_virtual_rows += rows.len();
    }

    /// Go `Append(other, begin, end)` for distinct chunks.
    pub fn append_range_from(&mut self, other: &Chunk, begin: usize, end: usize) {
        assert!(begin <= end && end <= other.physical_num_rows());
        for row in begin..end {
            // Go `Chunk.Append` indexes the physical column arrays directly;
            // it intentionally ignores the source selection vector.
            self.append_row(Row::new(other, row));
        }
    }

    /// The self-overlap case Go permits (`c.Append(c, begin, end)`). Snapshot
    /// the source range before mutating so reallocation cannot invalidate it.
    pub fn append_own_range(&mut self, begin: usize, end: usize) {
        assert!(begin <= end && end <= self.physical_num_rows());
        let snapshot = self.copy_construct();
        self.append_range_from(&snapshot, begin, end);
    }

    fn physical_num_rows(&self) -> usize {
        self.columns
            .first()
            .map_or(self.num_virtual_rows, |column| column.read().rows())
    }

    /// Go `Reconstruct`: materialize the installed selection and remove it.
    pub fn reconstruct(&mut self) {
        let Some(selection) = self.sel.take() else {
            return;
        };
        for column in &mut self.columns {
            column.write().reconstruct(&selection);
        }
        self.num_virtual_rows = selection.len();
    }

    /// Go `TruncateTo`.
    pub fn truncate_to(&mut self, num_rows: usize) {
        self.reconstruct();
        for column in &mut self.columns {
            let mut column = column.write();
            assert!(num_rows <= column.rows());
            if column.is_fixed() {
                let elem_buffer_len = column.elem_buffer_len();
                column.data.truncate(num_rows * elem_buffer_len);
            } else {
                let data_len = usize::try_from(column.offsets[num_rows])
                    .expect("column offset is non-negative");
                column.data.truncate(data_len);
                column.offsets.truncate(num_rows + 1);
            }
            column.length = num_rows;
            column.null_bitmap.truncate((num_rows + 7) >> 3);
            if num_rows & 7 != 0 {
                let last = column
                    .null_bitmap
                    .last_mut()
                    .expect("non-zero row count has bitmap");
                *last &= ((1u16 << (num_rows & 7)) - 1) as u8;
            }
        }
        self.num_virtual_rows = num_rows;
    }

    /// Go `CopyConstructSel`.
    #[must_use]
    pub fn copy_construct_sel(&self) -> Chunk {
        let Some(selection) = &self.sel else {
            return self.copy_construct();
        };
        let mut copy = self.renew_with_capacity(self.capacity, self.required_rows);
        for &row in selection {
            for (dst, src) in copy.columns.iter_mut().zip(&self.columns) {
                Self::append_cell_between(dst, src, row);
            }
        }
        copy
    }

    /// Go `CloneEmpty`.
    #[must_use]
    pub fn clone_empty(&self, max_capacity: usize) -> Chunk {
        self.renew_with_capacity(max_capacity, max_capacity)
    }

    /// Go `CopyConstruct`: a new chunk with a deep copy of this chunk's data.
    #[must_use]
    pub fn copy_construct(&self) -> Chunk {
        Chunk {
            sel: self.sel.clone(),
            columns: self.columns.iter().map(ColumnSlot::deep_copy).collect(),
            // Go assigns make([]*Column, len(c.columns)); even a nil source
            // becomes a non-nil zero-length slice in the copy.
            columns_initialized: true,
            num_virtual_rows: self.num_virtual_rows,
            capacity: self.capacity,
            required_rows: self.required_rows,
            in_complete_chunk: self.in_complete_chunk,
        }
    }

    /// Go `Chunk.ToString`: render every logical row and terminate each row
    /// with one newline. The byte-authoritative result can contain invalid
    /// UTF-8 through source string, ENUM, or SET cells.
    #[must_use]
    pub fn to_string(&self, field_types: &[FieldType]) -> GoString {
        let mut output = Vec::with_capacity(self.num_rows().saturating_mul(2));
        for row_idx in 0..self.num_rows() {
            output.extend_from_slice(self.get_row(row_idx).to_string(field_types).as_bytes());
            output.push(b'\n');
        }
        output.into()
    }

    /// The chunk's private ownership slots for dependency-closed helpers.
    pub(crate) fn column_slots(&self) -> &[ColumnSlot] {
        &self.columns
    }

    pub(crate) fn column_is_shared(&self, index: usize) -> bool {
        self.columns[index].is_shared()
    }

    /// Go `c.sel != nil`: whether a selection vector is installed.
    pub(crate) fn has_sel(&self) -> bool {
        self.sel.is_some()
    }

    /// Go `numVirtualRows += n`.
    pub(crate) fn add_virtual_rows(&mut self, n: usize) {
        self.num_virtual_rows += n;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn int_str_fields() -> Vec<FieldType> {
        vec![
            FieldType::new(FieldTypeCode::Long),
            FieldType::new(FieldTypeCode::VarString),
        ]
    }

    /// Go's own `Chunk.MemoryUsage` for the same chunks, captured from
    /// `pkg/util/chunk` in-process (`chunk.New(fields, cap, 1024)`):
    ///
    /// | chunk              | Go    |
    /// |--------------------|-------|
    /// | 1 bigint, cap 0    | 120   |
    /// | 1 bigint, cap 32   | 380   |
    /// | 1 bigint, cap 1024 | 8440  |
    ///
    /// A fixed-length column agrees exactly, which pins both the per-column
    /// struct size (112 bytes either way) and the capacity terms.
    #[test]
    fn memory_usage_of_a_fixed_length_column_matches_go() {
        let bigint = vec![FieldType::new(FieldTypeCode::LongLong)];
        assert_eq!(Chunk::new(&bigint, 0, 1024).memory_usage(), 120);
        assert_eq!(Chunk::new(&bigint, 32, 1024).memory_usage(), 380);
        assert_eq!(Chunk::new(&bigint, 1024, 1024).memory_usage(), 8440);
    }

    /// Go `newVarLenColumn` pre-reserves `estimatedElemLen*capacity` data bytes.
    /// That allocation is part of the memory-tracker/spill contract, not an
    /// implementation detail: a fresh single-VARCHAR chunk at capacity 32 is
    /// exactly 636 bytes in both implementations.
    #[test]
    fn memory_usage_of_a_var_length_column_matches_go() {
        let varchar = vec![FieldType::new(FieldTypeCode::VarString)];
        let chk = Chunk::new(&varchar, 32, 1024);
        // 112 struct + 4 null bitmap + 33*8 offsets + 32*8 data + 0 elemBuf.
        assert_eq!(chk.memory_usage(), 636);
    }

    /// The tracked number must GROW as rows land, or an operator that fills a
    /// chunk would be accounted as if it were still empty.
    #[test]
    fn memory_usage_grows_past_the_initial_capacity() {
        let fields = int_str_fields();
        let mut chk = Chunk::new(&fields, 8, 1024);
        let empty = chk.memory_usage();
        for i in 0..64 {
            chk.append_int64(0, i);
            chk.append_string(1, "abcdefgh");
        }
        assert!(chk.memory_usage() > empty);
    }

    /// Go `Renew`/`reCalcCapacity`, measured against the real
    /// `pkg/util/chunk` (`chunk.Renew(chk, maxChunkSize).Capacity()`):
    ///
    /// | old chunk            | maxChunkSize | Go |
    /// |----------------------|--------------|----|
    /// | capacity 0, empty    | 1024         | 32 |
    /// | capacity 32, FULL    | 1024         | 64 |
    /// | capacity 32, FULL    | 40           | 40 |
    /// | capacity 32, half    | 1024         | 32 |
    ///
    /// Only a FULL chunk doubles, the cap is `maxChunkSize`, and a chunk with
    /// no capacity of its own starts at `INITIAL_CAPACITY`.
    #[test]
    fn renew_capacity_ladder_matches_go() {
        let bigint = vec![FieldType::new(FieldTypeCode::LongLong)];

        let zero = Chunk::new(&bigint, 0, 1024);
        assert_eq!(zero.renew(1024).capacity(), 32);

        let mut full = Chunk::new(&bigint, 32, 1024);
        for _ in 0..32 {
            full.append_int64(0, 1);
        }
        assert_eq!(full.renew(1024).capacity(), 64);
        assert_eq!(full.renew(40).capacity(), 40);

        let mut half = Chunk::new(&bigint, 32, 1024);
        for _ in 0..16 {
            half.append_int64(0, 1);
        }
        assert_eq!(half.renew(1024).capacity(), 32);

        // The renewed chunk keeps the column SHAPE but holds no rows.
        let renewed = full.renew(1024);
        assert_eq!(renewed.num_cols(), 1);
        assert_eq!(renewed.num_rows(), 0);

        // Go distinguishes a nil columns slice from a non-nil empty one. A
        // constructed zero-column chunk keeps renewal policy even though its
        // logical column count is zero.
        let zero_columns = Chunk::new(&[], 7, 31);
        let zero_columns = zero_columns.renew_with_capacity(13, 31);
        assert_eq!(zero_columns.capacity(), 13);
        assert_eq!(zero_columns.required_rows, 31);
    }

    #[test]
    fn build_and_read_rows() {
        let fields = int_str_fields();
        let mut chk = Chunk::new_with_capacity(&fields, 8);
        assert_eq!(chk.num_cols(), 2);
        assert_eq!(chk.num_rows(), 0);

        chk.append_int64(0, 10);
        chk.append_string(1, "a");
        chk.append_null(0);
        chk.append_string(1, "b");
        assert_eq!(chk.num_rows(), 2);

        let r0 = chk.get_row(0);
        assert_eq!(r0.len(), 2);
        assert_eq!(r0.get_int64(0), 10);
        assert!(!r0.is_null(0));
        assert_eq!(r0.get_bytes(1), b"a");

        let r1 = chk.get_row(1);
        assert!(r1.is_null(0));
        assert_eq!(r1.get_bytes(1), b"b");
    }

    #[test]
    fn append_row_copies_between_chunks() {
        let fields = int_str_fields();
        let mut src = Chunk::new_with_capacity(&fields, 4);
        src.append_int64(0, 7);
        src.append_string(1, "hi");

        let mut dst = Chunk::new_with_capacity(&fields, 4);
        dst.append_row(src.get_row(0));
        assert_eq!(dst.num_rows(), 1);
        let r = dst.get_row(0);
        assert_eq!(r.get_int64(0), 7);
        assert_eq!(r.get_bytes(1), b"hi");
    }

    #[test]
    fn reset_reuses_columns() {
        let fields = int_str_fields();
        let mut chk = Chunk::new_with_capacity(&fields, 4);
        chk.append_int64(0, 1);
        chk.append_string(1, "x");
        chk.reset();
        assert_eq!(chk.num_rows(), 0);
        chk.append_int64(0, 2);
        chk.append_string(1, "y");
        assert_eq!(chk.get_row(0).get_int64(0), 2);
    }

    #[test]
    fn get_datum_by_type() {
        use tidb_datatype::Datum;
        let fields = vec![
            FieldType::new(FieldTypeCode::Long),
            FieldType::new(FieldTypeCode::VarString),
            FieldType::new(FieldTypeCode::Double),
        ];
        let mut chk = Chunk::new_with_capacity(&fields, 4);
        chk.append_int64(0, 42);
        chk.append_string(1, "hi");
        chk.append_float64(2, 2.5);
        // second row: null int
        chk.append_null(0);
        chk.append_string(1, "");
        chk.append_float64(2, 0.0);

        let r0 = chk.get_row(0);
        assert_eq!(r0.get_datum(0, &fields[0]), Datum::Int(42));
        assert_eq!(r0.get_datum(2, &fields[2]), Datum::Real(2.5));
        match r0.get_datum(1, &fields[1]) {
            Datum::String(_) => {}
            other => panic!("expected string datum, got {other:?}"),
        }
        // null cell -> Datum::Null regardless of type
        assert_eq!(chk.get_row(1).get_datum(0, &fields[0]), Datum::Null);
    }

    /// A decimal datum must survive `append_datum` -> `get_datum` unchanged,
    /// which is the path an INSERT of a decimal literal takes.
    #[test]
    fn decimal_datum_round_trips_through_append_datum() {
        use tidb_datatype::{Decimal, FieldTypeCode};
        let ft = FieldType::new(FieldTypeCode::NewDecimal);
        let mut chunk = Chunk::new(std::slice::from_ref(&ft), 4, 8);
        for text in ["1.50", "-273.15", "0", "12345678901234567890.123456789"] {
            chunk.append_datum(0, &Datum::Decimal(Decimal::from_literal(text)));
        }
        chunk.append_null(0);

        let texts: Vec<String> = (0..4)
            .map(|i| match chunk.get_row(i).get_datum(0, &ft) {
                Datum::Decimal(d) => d.to_string(),
                other => panic!("expected a decimal, got {other:?}"),
            })
            .collect();
        assert_eq!(
            texts,
            ["1.50", "-273.15", "0", "12345678901234567890.123456789"]
        );
        assert!(chunk.get_row(4).is_null(0));
    }

    #[test]
    fn decimal_cells_round_trip_as_raw_struct_bytes() {
        use tidb_datatype::{FieldTypeCode, MyDecimal};
        let ft = FieldType::new(FieldTypeCode::NewDecimal);
        let mut chk = Chunk::new_with_capacity(std::slice::from_ref(&ft), 4);
        let a = MyDecimal::from_int(12345);
        let b = MyDecimal::from_int(-7);
        chk.append_my_decimal(0, &a);
        chk.append_null(0);
        chk.append_my_decimal(0, &b);
        assert_eq!(chk.num_rows(), 3);

        // The cell is the exact 40-byte struct.
        assert_eq!(chk.get_row(0).get_my_decimal(0), a);
        assert_eq!(chk.get_row(2).get_my_decimal(0), b);
        assert_eq!(chk.column(0).get_raw(0), &a.to_raw_bytes()[..]);
        // And reads back as a decimal datum with the same text.
        match chk.get_row(0).get_datum(0, &ft) {
            Datum::Decimal(d) => assert_eq!(d.to_string(), "12345"),
            other => panic!("expected decimal datum, got {other:?}"),
        }
        assert_eq!(chk.get_row(1).get_datum(0, &ft), Datum::Null);
    }

    #[test]
    fn decimal_get_datum_preserves_declared_shape_and_effective_fraction() {
        let fields = [
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(10)
                .with_decimal(4),
            FieldType::new(FieldTypeCode::NewDecimal).with_flen(10),
        ];
        let mut chunk = Chunk::new_with_capacity(&fields, 1);
        chunk.append_my_decimal(0, &MyDecimal::from_string(b"12.3").0);
        chunk.append_my_decimal(1, &MyDecimal::from_string(b"12.340").0);

        let row = chunk.get_row(0);
        let shapes = std::array::from_fn(|column| match row.get_datum(column, &fields[column]) {
            Datum::Decimal(decimal) => decimal.declared_shape(),
            other => panic!("expected decimal datum, got {other:?}"),
        });
        assert_eq!(shapes, [Some((10, 4)), Some((10, 3))]);
    }

    #[test]
    fn decimal_get_datum_preserves_hidden_fraction_words_and_result_scale() {
        let field = FieldType::new(FieldTypeCode::NewDecimal).with_flen(20);
        let mut raw = MyDecimal::from_string(b"1.234567890").0.to_raw_bytes();
        raw[2] = 7;
        let stored = MyDecimal::from_raw_bytes(raw).expect("valid decimal layout");
        let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 1);
        chunk.append_my_decimal(0, &stored);

        match chunk.get_row(0).get_datum(0, &field) {
            Datum::Decimal(decimal) => {
                assert_eq!(decimal.to_string(), "1.2345679");
                assert_eq!(decimal.storage_string(), "1.234567890");
                assert_eq!(decimal.declared_shape(), Some((20, 9)));
            }
            other => panic!("expected decimal datum, got {other:?}"),
        }
    }

    #[test]
    fn datum_row_buffer_is_overwritten_and_reused() {
        let fields = [
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(10)
                .with_decimal(4),
        ];
        let mut chunk = Chunk::new_with_capacity(&fields, 1);
        chunk.append_int64(0, 7);
        chunk.append_my_decimal(1, &MyDecimal::from_string(b"12.3").0);

        let row = chunk.get_row(0);
        let mut buffer = vec![Datum::Null, Datum::Int(99)];
        let returned = row.get_datum_row_with_buffer(&fields, &mut buffer);
        assert_eq!(returned[0], Datum::Int(7));
        match &returned[1] {
            Datum::Decimal(decimal) => {
                assert_eq!(decimal.to_string(), "12.3");
                assert_eq!(decimal.declared_shape(), Some((10, 4)));
            }
            other => panic!("expected decimal datum, got {other:?}"),
        }
        assert_eq!(row.get_datum_row(&fields), returned);

        let mut cell = Datum::new_string("stale");
        row.datum_with_buffer(0, &fields[0], &mut cell);
        assert_eq!(cell, Datum::Int(7));
    }

    #[test]
    fn time_duration_datum_roundtrip() {
        use tidb_datatype::{CoreTime, Datum, TimeType};
        let fields = vec![
            FieldType::new(FieldTypeCode::Datetime),
            FieldType::new(FieldTypeCode::Duration).with_decimal(3),
        ];
        let t = Time::new(
            CoreTime::from_date(2026, 7, 25, 8, 30, 15, 500_000),
            TimeType::DateTime,
            6,
        )
        .unwrap();
        let d = MySqlDuration::new(1, 2, 3, 400_000, 3).unwrap();

        let mut chk = Chunk::new_with_capacity(&fields, 4);
        chk.append_datum(0, &Datum::Time(t));
        chk.append_datum(1, &Datum::Duration(d));
        chk.append_datum(0, &Datum::Null);
        chk.append_datum(1, &Datum::Null);

        let r0 = chk.get_row(0);
        assert_eq!(r0.get_time(0), t);
        assert_eq!(r0.get_datum(0, &fields[0]), Datum::Time(t));
        // Duration fsp is refilled from the field type's decimal (Go
        // tp.GetDecimal()), matching what was appended here.
        assert_eq!(r0.get_duration(1, 3), d);
        assert_eq!(r0.get_datum(1, &fields[1]), Datum::Duration(d));
        let r1 = chk.get_row(1);
        assert_eq!(r1.get_datum(0, &fields[0]), Datum::Null);
        assert_eq!(r1.get_datum(1, &fields[1]), Datum::Null);
    }

    /// Go `pkg/util/chunk/chunk_test.go`'s `newAllTypes`, ported WHOLE: every
    /// field type the chunk tests build a column for, in Go's own order.
    ///
    /// The point of the whole table is that a column's SHAPE (fixed vs
    /// variable length) and the datum kind its cell reads back as must agree
    /// for EVERY type, not for the ones someone remembered. A single wrong
    /// pairing is either a panic (an 8-byte append into a var-length column,
    /// or `append_bytes` into a fixed one) or a silently wrong value.
    fn go_all_types() -> Vec<FieldType> {
        use tidb_datatype::FieldTypeCode as C;
        vec![
            FieldType::new(C::Tiny),
            FieldType::new(C::Short),
            FieldType::new(C::Int24),
            FieldType::new(C::Long),
            FieldType::new(C::LongLong),
            FieldType::new(C::LongLong).with_unsigned(true),
            FieldType::new(C::Year),
            FieldType::new(C::Float),
            FieldType::new(C::Double),
            FieldType::new(C::String),
            FieldType::new(C::VarString),
            FieldType::new(C::Varchar),
            FieldType::new(C::Blob),
            FieldType::new(C::TinyBlob),
            FieldType::new(C::MediumBlob),
            FieldType::new(C::LongBlob),
            FieldType::new(C::Date),
            FieldType::new(C::Datetime),
            FieldType::new(C::Timestamp),
            FieldType::new(C::Duration),
            FieldType::new(C::NewDecimal),
            FieldType::new(C::Set)
                .with_unsigned(true)
                .with_elems(["a", "b"]),
            FieldType::new(C::Enum)
                .with_unsigned(true)
                .with_elems(["a", "b"]),
            FieldType::new(C::Bit),
            FieldType::new(C::Json),
        ]
    }

    /// The value Go's `TestCompare`/`TestCopyTo` append for each type, as the
    /// datum this port's `append_datum` takes.
    fn go_all_types_value(field_type: &FieldType, k: u64) -> Datum {
        use tidb_datatype::Collation;
        // The same collation `Row::get_datum` stamps on an enum/set datum.
        fn collation_of(field_type: &FieldType) -> Collation {
            field_type.collation()
        }
        use tidb_datatype::{
            BinaryJSON, BinaryLiteral, CoreTime, Decimal, FieldTypeCode as C, MysqlEnum, MysqlSet,
            TimeType,
        };
        match field_type.code() {
            C::Tiny | C::Short | C::Int24 | C::Long | C::LongLong | C::Year => {
                if field_type.is_unsigned() {
                    Datum::UInt(k)
                } else {
                    Datum::Int(k as i64)
                }
            }
            C::Float => Datum::Float32(k as f64),
            C::Double => Datum::Real(k as f64),
            C::String
            | C::VarString
            | C::Varchar
            | C::Blob
            | C::TinyBlob
            | C::MediumBlob
            // Go appends the text and reads it back with `d.SetString(...,
            // tp.GetCollate())`, so the round-tripped datum is a
            // collation-tagged string.
            | C::LongBlob => {
                let mut d = Datum::Null;
                d.set_string(k.to_string().into_bytes(), collation_of(field_type));
                d
            }
            C::Date | C::Datetime | C::Timestamp => Datum::Time(
                Time::new(
                    CoreTime::from_date(2000, 1, 1, 0, 0, u8::try_from(k).unwrap(), 0),
                    match field_type.code() {
                        C::Date => TimeType::Date,
                        C::Timestamp => TimeType::Timestamp,
                        _ => TimeType::DateTime,
                    },
                    0,
                )
                .unwrap(),
            ),
            C::Duration => Datum::Duration(MySqlDuration::from_raw_parts(
                i64::try_from(k).unwrap() * 1_000_000_000,
                field_type.decimal(),
            )),
            C::NewDecimal => Datum::Decimal(Decimal::from_literal(&k.to_string())),
            // Go appends `types.Set{Name: "a", Value: k}` verbatim, without
            // asking the field type's elems to agree.
            C::Set => Datum::Set(
                MysqlSet::new("a".to_owned(), k),
                collation_of(field_type),
            ),
            C::Enum => Datum::Enum(
                MysqlEnum::new("a".to_owned(), k),
                collation_of(field_type),
            ),
            // Go: `chunk.AppendBytes(i, []byte{byte(k)})` -- a BIT cell is the
            // literal's own bytes in a VARIABLE-length column.
            C::Bit => Datum::Bit(BinaryLiteral::from(vec![u8::try_from(k & 0xff).unwrap()])),
            C::Json => Datum::Json(BinaryJSON::parse(&k.to_string()).unwrap()),
            other => panic!("type not handled: {other:?}"),
        }
    }

    /// Every type in Go's `newAllTypes` table survives
    /// `append_datum` -> `get_datum` with its kind and value intact, and a
    /// NULL cell in each reads back NULL.
    #[test]
    fn every_go_all_types_column_round_trips_a_datum() {
        let fields = go_all_types();
        let mut chunk = Chunk::new(&fields, 8, 128);
        for (i, field_type) in fields.iter().enumerate() {
            chunk.append_null(i);
            for k in 0..3u64 {
                chunk.append_datum(i, &go_all_types_value(field_type, k));
            }
        }
        for (i, field_type) in fields.iter().enumerate() {
            assert_eq!(
                chunk.get_row(0).get_datum(i, field_type),
                Datum::Null,
                "{field_type:?}"
            );
            for k in 0..3u64 {
                let expected = go_all_types_value(field_type, k);
                let actual = chunk
                    .get_row(usize::try_from(k).unwrap() + 1)
                    .get_datum(i, field_type);
                assert_eq!(actual, expected, "{field_type:?} at k={k}");
            }
        }
    }

    #[test]
    fn empty_chunk_virtual_rows() {
        let mut chk = Chunk::new_empty(&[]);
        assert_eq!(chk.num_cols(), 0);
        chk.set_num_virtual_rows(5);
        assert_eq!(chk.num_rows(), 5);
        assert!(!chk.get_row(0).is_empty());
        assert!(Row::empty().is_empty());
    }

    #[test]
    fn required_rows_fullness_and_grow_reset_follow_source_boundaries() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let mut chunk = Chunk::new(&fields, 2, 8);
        assert_eq!(chunk.required_rows(), 8);
        chunk.set_required_rows(2, 8);
        assert_eq!(chunk.required_rows(), 2);
        assert!(!chunk.is_full());
        chunk.append_int64(0, 1);
        chunk.append_int64(0, 2);
        assert!(chunk.is_full());
        chunk.grow_and_reset(8);
        assert_eq!(chunk.capacity(), 4);
        assert_eq!(chunk.required_rows(), 8);
        assert_eq!(chunk.num_rows(), 0);

        chunk.set_required_rows(0, 8).set_required_rows(9, 8);
        assert_eq!(chunk.required_rows(), 8);
        chunk.set_required_rows(-1, 8);
        assert_eq!(chunk.required_rows(), 8);
    }

    #[test]
    fn selection_append_copy_reconstruct_and_truncate_match_go() {
        let fields = int_str_fields();
        let mut chunk = Chunk::new_with_capacity(&fields, 8);
        for row in 0..4 {
            chunk.append_int64(0, row);
            chunk.append_string(1, format!("s{row}"));
        }
        chunk.set_sel(Some(vec![1, 3]));
        chunk.append_int64(0, 9);
        chunk.append_string(1, "s9");
        assert_eq!(chunk.sel(), Some(&[1, 3, 4][..]));

        let selected = chunk.copy_construct_sel();
        assert_eq!(selected.num_rows(), 3);
        assert_eq!(selected.get_row(0).get_int64(0), 1);
        assert_eq!(selected.get_row(1).get_int64(0), 3);
        assert_eq!(selected.get_row(2).get_int64(0), 9);

        chunk.reconstruct();
        assert!(chunk.sel().is_none());
        assert_eq!(chunk.num_rows(), 3);
        assert_eq!(chunk.get_row(2).get_bytes(1), b"s9");
        chunk.truncate_to(2);
        assert_eq!(chunk.num_rows(), 2);
        assert_eq!(chunk.get_row(1).get_int64(0), 3);
    }

    #[test]
    fn append_ranges_rows_and_projection_preserve_width_and_order() {
        let fields = int_str_fields();
        let mut source = Chunk::new_with_capacity(&fields, 6);
        for row in 0..3 {
            source.append_int64(0, row);
            source.append_string(1, format!("s{row}"));
        }

        let mut range = Chunk::new_with_capacity(&fields, 8);
        range.append_range_from(&source, 1, 3);
        assert_eq!(range.num_rows(), 2);
        assert_eq!(range.get_row(0).get_int64(0), 1);
        range.append_own_range(0, 2);
        assert_eq!(range.num_rows(), 4);
        assert_eq!(range.get_row(3).get_bytes(1), b"s2");

        let rows = [source.get_row(2), source.get_row(0)];
        let mut projected = Chunk::new_with_capacity(
            &[
                FieldType::new(FieldTypeCode::VarString),
                FieldType::new(FieldTypeCode::LongLong),
            ],
            4,
        );
        assert_eq!(projected.append_rows_by_col_idxs(&rows, Some(&[1, 0])), 4);
        assert_eq!(projected.get_row(0).get_bytes(0), b"s2");
        assert_eq!(projected.get_row(0).get_int64(1), 2);
        assert_eq!(projected.get_row(1).get_bytes(0), b"s0");

        let mut virtual_only = Chunk::new_empty(&[]);
        assert_eq!(virtual_only.append_rows_by_col_idxs(&rows, Some(&[])), 0);
        assert_eq!(virtual_only.num_rows(), 2);
    }

    #[test]
    fn reset_distinguishes_nil_and_constructed_zero_column_chunks() {
        let mut nil_columns = Chunk::default();
        nil_columns.set_num_virtual_rows(7);
        nil_columns.reset();
        assert_eq!(nil_columns.num_virtual_rows(), 7);

        let mut initialized_empty = Chunk::new_empty(&[]);
        initialized_empty.set_num_virtual_rows(7);
        initialized_empty.reset();
        assert_eq!(initialized_empty.num_virtual_rows(), 0);
    }

    /// Go `Chunk.Append` indexes physical columns and does not apply `sel` to
    /// the source range.
    #[test]
    fn append_range_ignores_source_selection() {
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let mut source = Chunk::new_with_capacity(&fields, 3);
        for value in [10, 20, 30] {
            source.append_int64(0, value);
        }
        source.set_sel(Some(vec![2, 0]));

        let mut target = Chunk::new_with_capacity(&fields, 2);
        target.append_range_from(&source, 0, 2);
        assert_eq!(target.get_row(0).get_int64(0), 10);
        assert_eq!(target.get_row(1).get_int64(0), 20);

        source.append_own_range(1, 3);
        assert_eq!(source.column(0).get_int64(3), 20);
        assert_eq!(source.column(0).get_int64(4), 30);
        assert_eq!(source.sel(), Some(&[2, 0, 3, 4][..]));
    }

    #[test]
    fn raw_datum_appends_as_a_variable_cell() {
        let field = FieldType::new(FieldTypeCode::VarString);
        let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 1);
        chunk.append_datum(0, &Datum::Raw(vec![0, 255]));
        assert_eq!(chunk.get_row(0).get_bytes(0), &[0, 255]);
    }

    #[test]
    fn vector_float32_cell_and_datum_round_trip() {
        let field = FieldType::new(FieldTypeCode::VectorFloat32);
        let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 3);
        let empty = VectorFloat32::default();
        let values = VectorFloat32::must_create(vec![-1.25, 0.0, 3.5]);
        chunk.append_vector_float32(0, &empty);
        chunk.append_datum(0, &Datum::VectorFloat32(values.clone()));
        chunk.append_null(0);

        assert_eq!(chunk.get_row(0).get_vector_float32(0), empty);
        assert_eq!(
            chunk.get_row(1).get_datum(0, &field),
            Datum::VectorFloat32(values)
        );
        assert_eq!(chunk.get_row(2).get_datum(0, &field), Datum::Null);
    }
}
