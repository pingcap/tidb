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
use std::ops::{Deref, Range};

use crate::shared_bytes::{RetainedBytes, SharedBytesRead};

pub(crate) const INLINE_COPY_BYTES: usize = 64;

enum ColumnBytesStorage<'a> {
    Borrowed(&'a [u8]),
    Locked(SharedBytesRead<'a>, Range<usize>),
    Owned(Vec<u8>),
    Retained(RetainedBytes),
    Inline([u8; INLINE_COPY_BYTES], u8),
}

/// A read guard over a contiguous region of a column's packed bytes.
///
/// The guard keeps shared storage stable for the duration of the borrow and
/// behaves like a byte slice through [`Deref`] and [`AsRef`].
pub struct ColumnBytes<'a> {
    storage: ColumnBytesStorage<'a>,
}

/// A row-cell byte view returned by [`crate::column::Column`].
pub type CellBytes<'a> = ColumnBytes<'a>;

impl<'a> ColumnBytes<'a> {
    pub(crate) fn borrowed(bytes: SharedBytesRead<'a>, range: Range<usize>) -> Self {
        let storage = match bytes {
            SharedBytesRead::Owned(bytes) => ColumnBytesStorage::Borrowed(&bytes[range]),
            bytes => ColumnBytesStorage::Locked(bytes, range),
        };
        Self { storage }
    }

    pub(crate) fn copied(bytes: &[u8]) -> Self {
        let end = bytes.len();
        let storage = if end <= INLINE_COPY_BYTES {
            let mut copied = [0; INLINE_COPY_BYTES];
            copied[..end].copy_from_slice(bytes);
            ColumnBytesStorage::Inline(copied, end as u8)
        } else {
            ColumnBytesStorage::Owned(bytes.to_vec())
        };
        Self { storage }
    }

    pub(crate) fn retained(bytes: RetainedBytes) -> Self {
        Self {
            storage: ColumnBytesStorage::Retained(bytes),
        }
    }

    /// Converts part of an already retained cell to a string with the same owner.
    /// Returns `None` for byte views that do not own a retained cell.
    pub fn into_retained_string(
        self,
        range: Range<usize>,
        collation: tidb_charset::Collation,
    ) -> Option<tidb_datatype::StringDatum> {
        match self.storage {
            ColumnBytesStorage::Retained(bytes) => Some(bytes.into_string(range, collation)),
            _ => None,
        }
    }
}

impl Deref for ColumnBytes<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        match &self.storage {
            ColumnBytesStorage::Borrowed(bytes) => bytes,
            ColumnBytesStorage::Locked(bytes, range) => &bytes[range.clone()],
            ColumnBytesStorage::Owned(bytes) => bytes.as_slice(),
            ColumnBytesStorage::Retained(bytes) => bytes.as_ref(),
            ColumnBytesStorage::Inline(bytes, len) => &bytes[..usize::from(*len)],
        }
    }
}

impl AsRef<[u8]> for ColumnBytes<'_> {
    #[inline]
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
