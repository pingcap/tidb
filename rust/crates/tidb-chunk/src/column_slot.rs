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
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::alloc::ColumnRecycleRegistration;
use crate::column::Column;
use crate::column_view::ColumnBytes;

pub(crate) struct OwnedColumn {
    column: Column,
    recycle: Option<ColumnRecycleRegistration>,
}

impl OwnedColumn {
    fn new(column: Column) -> Self {
        Self {
            column,
            recycle: None,
        }
    }

    fn take_parts(&mut self) -> (Column, Option<ColumnRecycleRegistration>) {
        (std::mem::take(&mut self.column), self.recycle.take())
    }

    fn take_unregistered_column(&mut self) -> Column {
        self.recycle = None;
        std::mem::take(&mut self.column)
    }
}

impl Drop for OwnedColumn {
    fn drop(&mut self) {
        if let Some(recycle) = self.recycle.take() {
            recycle.recycle(std::mem::take(&mut self.column));
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
            recycle.recycle(std::mem::take(
                self.column
                    .get_mut()
                    .unwrap_or_else(PoisonError::into_inner),
            ));
        }
    }
}

/// One whole-column slot in a chunk.
pub(crate) enum ColumnSlot {
    Owned(OwnedColumn),
    Shared(Arc<SharedOwner>),
}

impl ColumnSlot {
    pub(crate) fn new(column: Column) -> Self {
        Self::Owned(OwnedColumn::new(column))
    }

    pub(crate) fn read(&self) -> ColumnRead<'_> {
        let inner = match self {
            Self::Owned(owner) => ColumnReadInner::Owned(&owner.column),
            Self::Shared(owner) => {
                ColumnReadInner::Shared(owner.column.read().unwrap_or_else(PoisonError::into_inner))
            }
        };
        ColumnRead { inner }
    }

    pub(crate) fn write(&mut self) -> ColumnWrite<'_> {
        let inner = match self {
            Self::Owned(owner) => ColumnWriteInner::Owned(&mut owner.column),
            Self::Shared(owner) => ColumnWriteInner::Shared(
                owner.column.write().unwrap_or_else(PoisonError::into_inner),
            ),
        };
        ColumnWrite { inner }
    }

    pub(crate) fn alias(&mut self) -> Self {
        if let Self::Shared(owner) = self {
            return Self::Shared(Arc::clone(owner));
        }

        let mut owned = match std::mem::take(self) {
            Self::Owned(owned) => owned,
            Self::Shared(_) => unreachable!("shared case returned above"),
        };
        let (column, recycle) = owned.take_parts();
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

    pub(crate) fn append_cell_from(&mut self, source: &Self, row: usize) {
        if let (Self::Owned(destination), Self::Owned(source)) = (&mut *self, source) {
            destination.column.append_cell_from(&source.column, row);
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

    pub(crate) fn is_shared(&self) -> bool {
        matches!(self, Self::Shared(_))
    }

    pub(crate) fn deep_copy(&self) -> Self {
        Self::new(self.read().copy_construct())
    }

    pub(crate) fn get_bytes(&self, row: usize) -> ColumnBytes<'_> {
        match self {
            Self::Owned(owner) => owner.column.get_bytes(row),
            Self::Shared(owner) => {
                let column = owner.column.read().unwrap_or_else(PoisonError::into_inner);
                let bytes = column.get_bytes(row).to_vec();
                drop(column);
                ColumnBytes::owned(bytes)
            }
        }
    }

    pub(crate) fn get_raw(&self, row: usize) -> ColumnBytes<'_> {
        match self {
            Self::Owned(owner) => owner.column.get_raw(row),
            Self::Shared(owner) => {
                let column = owner.column.read().unwrap_or_else(PoisonError::into_inner);
                let bytes = column.get_raw(row).to_vec();
                drop(column);
                ColumnBytes::owned(bytes)
            }
        }
    }

    pub(crate) fn attach_registration(&mut self, registration: ColumnRecycleRegistration) {
        match self {
            Self::Owned(owner) => {
                debug_assert!(owner.recycle.is_none());
                owner.recycle = Some(registration);
            }
            Self::Shared(owner) => {
                let mut recycle = owner.recycle.lock().unwrap_or_else(PoisonError::into_inner);
                debug_assert!(recycle.is_none());
                *recycle = Some(registration);
            }
        }
    }

    pub(crate) fn detach_registration(&mut self) {
        match self {
            Self::Owned(owner) => owner.recycle = None,
            Self::Shared(owner) => owner.detach_registration(),
        }
    }

    pub(crate) fn into_unique_column(self) -> Result<Column, Box<Self>> {
        match self {
            Self::Owned(mut owner) => Ok(owner.take_unregistered_column()),
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

/// A read borrow of a chunk column. Owned slots remain an ordinary borrowed
/// reference; promoted slots retain their shared-owner read guard.
pub struct ColumnRead<'a> {
    inner: ColumnReadInner<'a>,
}

impl Deref for ColumnRead<'_> {
    type Target = Column;

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
