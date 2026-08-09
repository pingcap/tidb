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

//! Packed bytes that become shared only when `MutRow` creates a shallow cell.
//!
//! Ordinary columns retain a plain `Vec<u8>` fast path. Creating a shallow
//! alias promotes that vector to synchronized shared storage; subsequent
//! growth of either view detaches it using Rust's native allocation policy.
//! This is an aliasing abstraction, not an emulation of Go slice headers.

use std::fmt;
use std::mem;
use std::ops::{Deref, Range};
use std::sync::{Arc, RwLock, RwLockReadGuard};

enum Backing {
    Owned(Vec<u8>),
    Shared(Arc<RwLock<Vec<u8>>>),
}

pub(crate) struct SharedBytes {
    backing: Backing,
    start: usize,
    len: usize,
}

pub(crate) enum SharedBytesRead<'a> {
    Owned(&'a [u8]),
    Shared {
        backing: RwLockReadGuard<'a, Vec<u8>>,
        start: usize,
        len: usize,
    },
}

impl Deref for SharedBytesRead<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared {
                backing,
                start,
                len,
            } => &backing[*start..*start + *len],
        }
    }
}

impl AsRef<[u8]> for SharedBytesRead<'_> {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Default for SharedBytes {
    fn default() -> Self {
        Self::from_vec(Vec::new())
    }
}

impl SharedBytes {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::from_vec(Vec::with_capacity(capacity))
    }

    pub(crate) fn from_vec(bytes: Vec<u8>) -> Self {
        let len = bytes.len();
        Self {
            backing: Backing::Owned(bytes),
            start: 0,
            len,
        }
    }

    pub(crate) fn zeros(len: usize) -> Self {
        Self::from_vec(vec![0; len])
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn capacity(&self) -> usize {
        match &self.backing {
            Backing::Owned(bytes) => bytes.capacity().saturating_sub(self.start),
            Backing::Shared(backing) => Self::read_shared(backing)
                .capacity()
                .saturating_sub(self.start),
        }
    }

    pub(crate) fn read(&self) -> SharedBytesRead<'_> {
        match &self.backing {
            Backing::Owned(bytes) => {
                SharedBytesRead::Owned(&bytes[self.start..self.start + self.len])
            }
            Backing::Shared(backing) => SharedBytesRead::Shared {
                backing: Self::read_shared(backing),
                start: self.start,
                len: self.len,
            },
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<u8> {
        self.read().to_vec()
    }

    pub(crate) fn reset(&mut self) {
        self.len = 0;
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        assert!(len <= self.len, "truncate cannot grow SharedBytes");
        self.len = len;
    }

    pub(crate) fn advance(&mut self, count: usize) {
        assert!(count <= self.len, "advance exceeds SharedBytes length");
        self.start += count;
        self.len -= count;
    }

    pub(crate) fn reserve(&mut self, additional: usize) {
        let required = self
            .len
            .checked_add(additional)
            .expect("SharedBytes capacity overflow");
        if required <= self.capacity() {
            return;
        }
        if self.start == 0 {
            if let Backing::Owned(bytes) = &mut self.backing {
                bytes.reserve(required - bytes.len());
                return;
            }
        }
        self.detach_with_capacity(required);
    }

    pub(crate) fn resize_preserving(&mut self, len: usize) {
        if self.start == 0 {
            if let Backing::Owned(bytes) = &mut self.backing {
                if len > bytes.len() {
                    bytes.resize(len, 0);
                }
                self.len = len;
                return;
            }
        }
        if len > self.capacity() {
            self.detach_with_capacity(len);
        }
        self.ensure_initialized(len);
        self.len = len;
    }

    pub(crate) fn extend_from_slice(&mut self, source: &[u8]) {
        if source.is_empty() {
            return;
        }
        let old_len = self.len;
        let new_len = old_len
            .checked_add(source.len())
            .expect("SharedBytes length overflow");
        if self.start == 0 {
            if let Backing::Owned(bytes) = &mut self.backing {
                if new_len > bytes.len() {
                    bytes.resize(new_len, 0);
                }
                bytes[old_len..new_len].copy_from_slice(source);
                self.len = new_len;
                return;
            }
        }
        if new_len > self.capacity() {
            self.detach_with_capacity(new_len);
        }
        self.ensure_initialized(new_len);
        self.len = new_len;
        self.with_write(|bytes| bytes[old_len..new_len].copy_from_slice(source));
    }

    pub(crate) fn fill(&mut self, value: u8) {
        self.with_write(|bytes| bytes.fill(value));
    }

    pub(crate) fn set(&mut self, index: usize, value: u8) {
        self.with_write(|bytes| bytes[index] = value);
    }

    pub(crate) fn copy_from_slice(&mut self, range: Range<usize>, source: &[u8]) -> usize {
        assert!(range.start <= range.end, "invalid SharedBytes copy range");
        assert!(range.end <= self.len, "SharedBytes copy exceeds length");
        let copied = (range.end - range.start).min(source.len());
        self.with_write(|bytes| {
            bytes[range.start..range.start + copied].copy_from_slice(&source[..copied]);
        });
        copied
    }

    pub(crate) fn copy_within(&mut self, source: Range<usize>, destination: usize) {
        self.with_write(|bytes| bytes.copy_within(source, destination));
    }

    pub(crate) fn share_range(&mut self, start: usize, end: usize) -> Self {
        assert!(start <= end, "invalid SharedBytes view range");
        assert!(end <= self.len, "SharedBytes view exceeds length");
        let previous = mem::replace(&mut self.backing, Backing::Owned(Vec::new()));
        let backing = match previous {
            Backing::Owned(bytes) => Arc::new(RwLock::new(bytes)),
            Backing::Shared(backing) => backing,
        };
        self.backing = Backing::Shared(Arc::clone(&backing));
        Self {
            backing: Backing::Shared(backing),
            start: self.start + start,
            len: end - start,
        }
    }

    pub(crate) fn deep_copy(&self) -> Self {
        Self::from_vec(self.snapshot())
    }

    #[cfg(test)]
    pub(crate) fn backing_ptr_eq(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (Backing::Shared(left), Backing::Shared(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn is_shared(&self) -> bool {
        matches!(&self.backing, Backing::Shared(_))
    }

    fn ensure_initialized(&mut self, len: usize) {
        let required = self
            .start
            .checked_add(len)
            .expect("SharedBytes backing length overflow");
        match &mut self.backing {
            Backing::Owned(bytes) => {
                if required > bytes.len() {
                    bytes.resize(required, 0);
                }
            }
            Backing::Shared(backing) => {
                let mut bytes = backing
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if required > bytes.len() {
                    bytes.resize(required, 0);
                }
            }
        }
    }

    fn with_write<R>(&mut self, update: impl FnOnce(&mut [u8]) -> R) -> R {
        let start = self.start;
        let end = start + self.len;
        match &mut self.backing {
            Backing::Owned(bytes) => update(&mut bytes[start..end]),
            Backing::Shared(backing) => {
                let mut bytes = backing
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                update(&mut bytes[start..end])
            }
        }
    }

    fn detach_with_capacity(&mut self, capacity: usize) {
        let visible = self.snapshot();
        let mut replacement = Vec::with_capacity(capacity);
        replacement.extend_from_slice(&visible);
        *self = Self::from_vec(replacement);
    }

    fn read_shared(backing: &RwLock<Vec<u8>>) -> RwLockReadGuard<'_, Vec<u8>> {
        backing
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Clone for SharedBytes {
    fn clone(&self) -> Self {
        self.deep_copy()
    }
}

impl fmt::Debug for SharedBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.read().as_ref(), formatter)
    }
}

impl PartialEq for SharedBytes {
    fn eq(&self, other: &Self) -> bool {
        self.snapshot() == other.snapshot()
    }
}

impl Eq for SharedBytes {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aliases_promote_lazily_and_detach_on_growth() {
        let mut source = SharedBytes::with_capacity(8);
        source.extend_from_slice(b"abc");
        assert!(!source.is_shared());

        let mut alias = source.share_range(1, 3);
        assert!(source.is_shared());
        assert!(source.backing_ptr_eq(&alias));
        alias.set(0, b'X');
        assert_eq!(source.read().as_ref(), b"aXc");

        alias.extend_from_slice(b"defghijk");
        assert!(!alias.backing_ptr_eq(&source));
        alias.set(0, b'Y');
        assert_eq!(source.read().as_ref(), b"aXc");
    }

    #[test]
    fn clone_is_an_owned_deep_copy() {
        let source = SharedBytes::from_vec(b"abc".to_vec());
        let mut copy = source.clone();
        copy.set(0, b'X');
        assert_eq!(source.read().as_ref(), b"abc");
        assert_eq!(copy.read().as_ref(), b"Xbc");
        assert!(!source.is_shared());
    }

    #[test]
    fn ordinary_growth_stays_on_the_owned_vec_fast_path() {
        let mut bytes = SharedBytes::with_capacity(1);
        bytes.extend_from_slice(b"an ordinary append that must grow");
        bytes.reserve(128);
        bytes.resize_preserving(96);
        assert!(!bytes.is_shared());
    }
}
