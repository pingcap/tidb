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

//! Private whole-column ownership for [`crate::chunk::Chunk`].
//!
//! A newly constructed column stays in the lock-free `Owned` variant. Only an
//! operation that creates another owner (`Prune`, `MakeRef`, or `MakeRefTo`)
//! promotes it to `Shared`.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::sync::{
    Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError,
};

use crate::alloc::ColumnRecycleRegistration;
use crate::column::Column;
use crate::column_view::ColumnBytes;

/// A cache-admitted column whose address can be reused across leases.
pub(crate) struct RegisteredColumn {
    column: Option<Box<Column>>,
    recycle: Option<ColumnRecycleRegistration>,
}

impl RegisteredColumn {
    fn get(&self) -> &Column {
        self.column.as_deref().expect("registered column is present")
    }

    fn get_mut(&mut self) -> &mut Column {
        self.column.as_deref_mut().expect("registered column is present")
    }

    fn take_parts(&mut self) -> (Box<Column>, Option<ColumnRecycleRegistration>) {
        let column = self.column.take().expect("registered column is present");
        (column, self.recycle.take())
    }
}

impl Drop for RegisteredColumn {
    fn drop(&mut self) {
        if let (Some(column), Some(recycle)) = (self.column.take(), self.recycle.take()) {
            recycle.recycle(column);
        }
    }
}

pub(crate) struct SharedOwner {
    column: RwLock<Column>,
    recycle: Mutex<Option<ColumnRecycleRegistration>>,
}

impl SharedOwner {
    fn new(column: Column, recycle: Option<ColumnRecycleRegistration>) -> Self {
        Self {
            column: RwLock::new(column),
            recycle: Mutex::new(recycle),
        }
    }

    fn take_unregistered_column(&mut self) -> Column {
        *self
            .recycle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner) = None;
        std::mem::take(
            self.column
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    fn detach_registration(&self) {
        *self.recycle.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

impl Drop for SharedOwner {
    fn drop(&mut self) {
        let recycle = self
            .recycle
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(recycle) = recycle {
            recycle.recycle(Box::new(std::mem::take(
                self.column
                    .get_mut()
                    .unwrap_or_else(PoisonError::into_inner),
            )));
        }
    }
}

/// One whole-column slot in a chunk.
pub(crate) enum ColumnSlot {
    Owned(Column),
    Registered(RegisteredColumn),
    Shared(Arc<SharedOwner>),
}

impl ColumnSlot {
    pub(crate) fn new(column: Column) -> Self {
        Self::Owned(column)
    }

    pub(crate) fn with_registration(
        column: Box<Column>,
        recycle: ColumnRecycleRegistration,
    ) -> Self {
        Self::Registered(RegisteredColumn {
            column: Some(column),
            recycle: Some(recycle),
        })
    }

    pub(crate) fn owned_mut(&mut self) -> Option<&mut Column> {
        match self {
            Self::Owned(column) => Some(column),
            Self::Registered(owner) => Some(owner.get_mut()),
            Self::Shared(_) => None,
        }
    }

    pub(crate) fn take_owned_registration(
        &mut self,
        matches: impl FnOnce(&ColumnRecycleRegistration) -> bool,
    ) -> Option<(Box<Column>, ColumnRecycleRegistration)> {
        let Self::Registered(owner) = self else {
            return None;
        };
        if !owner.recycle.as_ref().is_some_and(matches) {
            return None;
        }
        let registration = owner.recycle.take().expect("matched registration");
        Some((
            owner.column.take().expect("registered column is present"),
            registration,
        ))
    }

    pub(crate) fn recycle_registration(&self) -> Option<&ColumnRecycleRegistration> {
        match self {
            Self::Registered(owner) => owner.recycle.as_ref(),
            _ => None,
        }
    }

    #[inline]
    pub(crate) fn read(&self) -> ColumnRead<'_> {
        let inner = match self {
            Self::Owned(column) => ColumnReadInner::Owned(column),
            Self::Registered(owner) => ColumnReadInner::Owned(owner.get()),
            Self::Shared(owner) => {
                ColumnReadInner::Shared(owner.column.read().unwrap_or_else(PoisonError::into_inner))
            }
        };
        ColumnRead { inner }
    }

    pub(crate) fn try_read(&self) -> Option<ColumnRead<'_>> {
        let Self::Shared(owner) = self else {
            return Some(self.read());
        };
        let column = match owner.column.try_read() {
            Ok(column) => column,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        Some(ColumnRead {
            inner: ColumnReadInner::Shared(column),
        })
    }

    pub(crate) fn write(&mut self) -> ColumnWrite<'_> {
        let inner = match self {
            Self::Owned(column) => ColumnWriteInner::Owned(column),
            Self::Registered(owner) => ColumnWriteInner::Owned(owner.get_mut()),
            Self::Shared(owner) => ColumnWriteInner::Shared(
                owner.column.write().unwrap_or_else(PoisonError::into_inner),
            ),
        };
        ColumnWrite { inner }
    }

    #[inline]
    pub(crate) fn with_write(&mut self, write: impl FnOnce(&mut Column)) {
        let column = match self {
            Self::Owned(column) => column,
            Self::Registered(owner) => owner.get_mut(),
            Self::Shared(owner) => return Self::write_shared(owner, write),
        };
        write(column);
    }

    #[cold]
    fn write_shared(owner: &SharedOwner, write: impl FnOnce(&mut Column)) {
        let mut column = owner.column.write().unwrap_or_else(PoisonError::into_inner);
        write(&mut column);
    }

    pub(crate) fn alias(&mut self) -> Self {
        if let Self::Shared(owner) = self {
            return Self::Shared(Arc::clone(owner));
        }

        let (mut column, recycle) = match std::mem::take(self) {
            Self::Owned(column) => (column, None),
            Self::Registered(mut owner) => {
                let (column, recycle) = owner.take_parts();
                (*column, recycle)
            }
            Self::Shared(_) => unreachable!("shared case returned above"),
        };
        column.retain_data_for_alias();
        let owner = Arc::new(SharedOwner::new(column, recycle));
        *self = Self::Shared(Arc::clone(&owner));
        Self::Shared(owner)
    }

    pub(crate) fn same_identity(&self, other: &Self) -> bool {
        if std::ptr::eq(self, other) {
            return true;
        }
        matches!((self, other), (Self::Shared(left), Self::Shared(right)) if Arc::ptr_eq(left, right))
    }

    pub(crate) fn copy_from(
        &mut self,
        source: &Self,
        copy: impl FnOnce(&mut Column, &Column),
        copy_within: impl FnOnce(&mut Column),
    ) {
        let copy = |destination: &mut Column, source: &Column| {
            // Shallow row views in separate columns may still share a
            // mutable byte buffer. Do not read and write that buffer at once.
            if destination.has_mutably_shared_bytes() && source.has_mutably_shared_bytes() {
                copy(destination, &source.copy_construct());
            } else {
                copy(destination, source);
            }
        };
        let (Self::Shared(destination), Self::Shared(source)) = (&mut *self, source) else {
            copy(&mut self.write(), &source.read());
            return;
        };
        if Arc::ptr_eq(destination, source) {
            let mut column = destination.column.write().unwrap_or_else(PoisonError::into_inner);
            copy_within(&mut column);
            return;
        }

        let source = source.column.read().unwrap_or_else(PoisonError::into_inner);
        // Trying the second lock lets distinct owners copy without a snapshot
        // and avoids waiting with the source locked during a reverse copy.
        match destination.column.try_write() {
            Ok(mut destination) => copy(&mut destination, &source),
            Err(TryLockError::Poisoned(error)) => copy(&mut error.into_inner(), &source),
            Err(TryLockError::WouldBlock) => {
                let snapshot = source.copy_construct();
                drop(source);
                let mut destination = destination.column.write().unwrap_or_else(PoisonError::into_inner);
                copy(&mut destination, &snapshot);
            }
        }
    }

    pub(crate) fn append_cell_from(&mut self, source: &Self, row: usize) {
        if let (Some(destination), Some(source)) = (self.owned_mut(), source.owned_column()) {
            destination.append_cell_by_cell(source, row);
            return;
        }

        // A shared source may be the same owner as the destination. Snapshot
        // that one cell before taking the write lock so an alias append never
        // tries to read-lock a column it already write-locks.
        if self.same_identity(source) {
            let (not_null, source_is_fixed, cell) = {
                let source = source.read();
                let raw = source.get_raw(row);
                let cell = raw.to_vec();
                (!source.is_null(row), source.is_fixed(), cell)
            };
            self.write()
                .append_prepared_cell(not_null, source_is_fixed, &cell);
            return;
        }

        let source = source.read();
        let raw = source.get_raw(row);
        self.write()
            .append_prepared_cell(!source.is_null(row), source.is_fixed(), &raw);
    }

    pub(crate) fn append_range_from(&mut self, source: &Self, begin: usize, end: usize) {
        if let (Some(destination), Some(source)) = (self.owned_mut(), source.owned_column()) {
            destination.append_range_from(source, begin, end);
            return;
        }
        if self.same_identity(source) {
            if begin != end {
                self.write().append_own_range(begin, end);
            }
            return;
        }
        let source = source.read();
        let mut destination = self.write();
        destination.append_range_from(&source, begin, end);
    }

    pub(crate) fn append_rows_from(
        &mut self,
        source: &Self,
        rows: impl ExactSizeIterator<Item = usize> + Clone,
        contiguous_start: Option<usize>,
    ) {
        if self.same_identity(source) {
            for row in rows {
                self.append_cell_from(source, row);
            }
            return;
        }
        let source = source.read();
        let mut destination = self.write();
        if let Some(begin) = contiguous_start {
            if destination.append_range_if_fits(&source, begin, begin + rows.len()) {
                return;
            }
        }
        if destination.is_fixed() == source.is_fixed() {
            destination.copy_row_ids_from(&source, rows);
        } else {
            let source: &Column = &source;
            let destination: &mut Column = &mut destination;
            for row in rows {
                destination.append_cell_by_cell(source, row);
            }
        }
    }

    /// Identity of a shared owner for comparisons while the column slots live.
    pub(crate) fn shared_owner(&self) -> Option<std::ptr::NonNull<SharedOwner>> {
        match self {
            Self::Shared(owner) => Some(std::ptr::NonNull::from(owner.as_ref())),
            Self::Owned(_) | Self::Registered(_) => None,
        }
    }

    pub(crate) fn deep_copy(&self) -> Self {
        Self::new(self.read().clone_exact())
    }

    #[inline]
    pub(crate) fn try_borrow_cell_bytes(&self, row: usize) -> Option<&[u8]> {
        self.owned_column()?.try_borrow_cell_bytes(row)
    }

    #[inline]
    pub(crate) fn owned_column(&self) -> Option<&Column> {
        match self {
            Self::Owned(column) => Some(column),
            Self::Registered(owner) => Some(owner.get()),
            Self::Shared(_) => None,
        }
    }

    pub(crate) fn get_bytes(&self, row: usize) -> ColumnBytes<'_> {
        match self {
            Self::Owned(column) => column.get_bytes(row),
            Self::Registered(owner) => owner.get().get_bytes(row),
            Self::Shared(owner) => {
                let column = owner.column.read().unwrap_or_else(PoisonError::into_inner);
                column.snapshot_var_bytes(row)
            }
        }
    }

    pub(crate) fn get_raw(&self, row: usize) -> ColumnBytes<'_> {
        match self {
            Self::Owned(column) => column.get_raw(row),
            Self::Registered(owner) => owner.get().get_raw(row),
            Self::Shared(owner) => {
                let column = owner.column.read().unwrap_or_else(PoisonError::into_inner);
                if column.is_fixed() {
                    ColumnBytes::copied(&column.get_raw(row))
                } else {
                    column.snapshot_var_bytes(row)
                }
            }
        }
    }

    pub(crate) fn detach_registration(&mut self) {
        match self {
            Self::Owned(_) => {}
            Self::Registered(owner) => *self = Self::Owned(*owner.take_parts().0),
            Self::Shared(owner) => owner.detach_registration(),
        }
    }

    pub(crate) fn into_unique_column(self) -> Result<Column, Box<Self>> {
        match self {
            Self::Owned(column) => Ok(column),
            Self::Registered(mut owner) => Ok(*owner.take_parts().0),
            Self::Shared(owner) => match Arc::try_unwrap(owner) {
                Ok(mut owner) => Ok(owner.take_unregistered_column()),
                Err(owner) => Err(Box::new(Self::Shared(owner))),
            },
        }
    }
}

impl Default for ColumnSlot {
    fn default() -> Self {
        Self::new(Column::default())
    }
}

impl Clone for ColumnSlot {
    fn clone(&self) -> Self {
        self.deep_copy()
    }
}

impl fmt::Debug for ColumnSlot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.read().fmt(formatter)
    }
}

impl PartialEq for ColumnSlot {
    fn eq(&self, other: &Self) -> bool {
        if self.same_identity(other) {
            return true;
        }
        let left = self.read().copy_construct();
        left == *other.read()
    }
}

impl Eq for ColumnSlot {}

enum ColumnReadInner<'a> {
    Owned(&'a Column),
    Shared(RwLockReadGuard<'a, Column>),
}

/// A chunk-bound column address. Resolving it once avoids repeated slot
/// indexing, without promoting the column or extending a shared read lock
/// across other input evaluation.
#[derive(Clone, Copy)]
pub struct ColumnRef<'a> {
    pub(crate) slot: &'a ColumnSlot,
}

impl<'a> ColumnRef<'a> {
    /// Borrows a stable column without holding a lock during argument evaluation.
    pub fn try_borrow_stable(&self) -> Option<&'a Column> {
        self.slot
            .owned_column()
            .filter(|column| !column.data.is_shared())
    }

    /// Borrow the column for one operation. Shared aliases retain the same
    /// synchronization and visibility as `Chunk::column`.
    pub fn read(&self) -> ColumnRead<'_> {
        self.slot.read()
    }
}

/// A read borrow of a chunk column. Owned slots remain an ordinary borrowed
/// reference; promoted slots retain their shared-owner read guard.
pub struct ColumnRead<'a> {
    inner: ColumnReadInner<'a>,
}

impl Deref for ColumnRead<'_> {
    type Target = Column;

    #[inline]
    fn deref(&self) -> &Self::Target {
        match &self.inner {
            ColumnReadInner::Owned(column) => column,
            ColumnReadInner::Shared(column) => column,
        }
    }
}

impl fmt::Debug for ColumnRead<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(formatter)
    }
}

impl PartialEq for ColumnRead<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.deref() == other.deref()
    }
}

impl Eq for ColumnRead<'_> {}

enum ColumnWriteInner<'a> {
    Owned(&'a mut Column),
    Shared(RwLockWriteGuard<'a, Column>),
}

/// A mutable borrow of a chunk column. Owned slots remain an ordinary mutable
/// reference; promoted slots retain their shared-owner write guard.
pub struct ColumnWrite<'a> {
    inner: ColumnWriteInner<'a>,
}

impl Deref for ColumnWrite<'_> {
    type Target = Column;

    fn deref(&self) -> &Self::Target {
        match &self.inner {
            ColumnWriteInner::Owned(column) => column,
            ColumnWriteInner::Shared(column) => column,
        }
    }
}

impl DerefMut for ColumnWrite<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match &mut self.inner {
            ColumnWriteInner::Owned(column) => column,
            ColumnWriteInner::Shared(column) => column,
        }
    }
}

impl fmt::Debug for ColumnWrite<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.deref().fmt(formatter)
    }
}

/// An opaque transferable whole-column owner used by [`crate::chunk::Chunk::set_col`].
pub struct ColumnHandle {
    pub(crate) slot: ColumnSlot,
}

impl ColumnHandle {
    /// Wrap an independently owned column.
    #[must_use]
    pub fn new(column: Column) -> Self {
        Self {
            slot: ColumnSlot::new(column),
        }
    }

    /// Read the handled column.
    #[must_use]
    pub fn read(&self) -> ColumnRead<'_> {
        self.slot.read()
    }

    /// Mutate the handled column.
    pub fn write(&mut self) -> ColumnWrite<'_> {
        self.slot.write()
    }

    /// Whether two handles designate the same mutable column owner.
    #[must_use]
    pub fn same_identity(&self, other: &Self) -> bool {
        self.slot.same_identity(&other.slot)
    }

    /// Recover the column when this is its sole owner.
    pub fn into_column(self) -> Result<Column, Box<Self>> {
        self.slot
            .into_unique_column()
            .map_err(|slot| Box::new(Self { slot: *slot }))
    }
}

impl From<Column> for ColumnHandle {
    fn from(column: Column) -> Self {
        Self::new(column)
    }
}

impl fmt::Debug for ColumnHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.slot.fmt(formatter)
    }
}
