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

//! Borrow-scoped views over packed column bytes.

use std::fmt;
use std::ops::Deref;

use crate::shared_bytes::SharedBytesRead;

pub(crate) enum ColumnBytesStorage<'a> {
    Borrowed(SharedBytesRead<'a>),
    Owned(Vec<u8>),
}

/// A read guard over a contiguous region of a column's packed bytes.
///
/// The guard keeps shared storage stable for the duration of the borrow and
/// behaves like a byte slice through [`Deref`] and [`AsRef`].
pub struct ColumnBytes<'a> {
    pub(crate) storage: ColumnBytesStorage<'a>,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// A row-cell byte view returned by [`crate::column::Column`].
pub type CellBytes<'a> = ColumnBytes<'a>;

impl<'a> ColumnBytes<'a> {
    pub(crate) fn owned(bytes: Vec<u8>) -> Self {
        let end = bytes.len();
        Self {
            storage: ColumnBytesStorage::Owned(bytes),
            start: 0,
            end,
        }
    }
}

impl Deref for ColumnBytes<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        let bytes = match &self.storage {
            ColumnBytesStorage::Borrowed(bytes) => bytes.as_ref(),
            ColumnBytesStorage::Owned(bytes) => bytes.as_slice(),
        };
        &bytes[self.start..self.end]
    }
}

impl AsRef<[u8]> for ColumnBytes<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl fmt::Debug for ColumnBytes<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(formatter)
    }
}

impl PartialEq for ColumnBytes<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for ColumnBytes<'_> {}

impl PartialEq<&[u8]> for ColumnBytes<'_> {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_ref() == *other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for ColumnBytes<'_> {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_ref() == other.as_slice()
    }
}

impl<const N: usize> PartialEq<[u8; N]> for ColumnBytes<'_> {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_ref() == other.as_slice()
    }
}

impl PartialEq<Vec<u8>> for ColumnBytes<'_> {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_ref() == other.as_slice()
    }
}
