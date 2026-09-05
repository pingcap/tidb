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

//! Rust counterpart of pinned Go `pkg/util/hack`.
//!
//! It provides zero-copy byte/string views and checkpointed hash-map memory
//! accounting. Rust's ownership model requires owned shared storage for the
//! deliberately mutable string view. The map counterpart models Go's Swiss-map
//! group, table, directory, split, and checkpoint accounting over native keys.

#![allow(unsafe_code)]

mod map;

use std::cell::UnsafeCell;
use std::fmt;
use std::rc::Rc;

pub use map::{
    to_swiss_map, MapType, MapValueLayout, MemAwareMap, SwissMapWrap,
    DEF_BUCKET_MEMORY_USAGE_FOR_MAP_STRING_TO_ANY,
    DEF_BUCKET_MEMORY_USAGE_FOR_MAP_STRING_TO_DECIMAL,
    DEF_BUCKET_MEMORY_USAGE_FOR_MAP_STRING_TO_STRING, DEF_BUCKET_MEMORY_USAGE_FOR_SET_FLOAT64,
    DEF_BUCKET_MEMORY_USAGE_FOR_SET_INT64, DEF_BUCKET_MEMORY_USAGE_FOR_SET_STRING,
};

/// An owned mutable byte buffer whose backing allocation may be shared by
/// string views.
///
/// Moving a `Vec<u8>` into this type is zero-copy. Existing string views keep
/// their allocation when [`append`](Self::append) grows the buffer.
#[derive(Clone)]
pub struct MutableBytes {
    storage: Rc<UnsafeCell<Vec<u8>>>,
}

impl MutableBytes {
    /// Takes ownership of `bytes` without copying it.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self {
            storage: Rc::new(UnsafeCell::new(bytes)),
        }
    }

    fn len(&self) -> usize {
        // SAFETY: `MutableBytes` and `MutableString` are deliberately
        // single-threaded (`Rc`). No reference into the `UnsafeCell` escapes.
        unsafe { (&*self.storage.get()).len() }
    }

    /// Replaces one byte.
    ///
    /// # Panics
    ///
    /// Panics when `index` is outside the current buffer.
    pub fn set(&mut self, index: usize, value: u8) {
        // SAFETY: mutation is serialized through `&mut self`; string views do
        // not expose references into the allocation.
        unsafe {
            (&mut *self.storage.get())[index] = value;
        }
    }

    /// Appends bytes, retaining the current allocation for existing views.
    pub fn append(&mut self, suffix: &[u8]) {
        // Copy only when growth exceeds capacity. This matches Go slice
        // append: aliases share an allocation while it has room, and retain
        // the old allocation when growth reallocates.
        // SAFETY: no reference into the cell escapes.
        let spare_capacity = unsafe {
            let storage = &*self.storage.get();
            storage.capacity() - storage.len()
        };
        if spare_capacity < suffix.len() {
            // SAFETY: no reference into the cell escapes.
            let mut replacement = unsafe { (&*self.storage.get()).clone() };
            replacement.extend_from_slice(suffix);
            self.storage = Rc::new(UnsafeCell::new(replacement));
        } else {
            // SAFETY: shared string views never expose a Rust reference into
            // the allocation, so appending behind their captured prefix is
            // permitted by this type's explicit mutable-view contract.
            unsafe {
                (&mut *self.storage.get()).extend_from_slice(suffix);
            }
        }
    }
}

/// A zero-copy string-like view over [`MutableBytes`].
///
/// The view intentionally observes changes to the shared prefix. It owns the
/// backing allocation, so it never dangles when the byte buffer grows.
#[derive(Clone)]
pub struct MutableString {
    storage: Rc<UnsafeCell<Vec<u8>>>,
    len: usize,
}

/// Creates a zero-copy mutable string view.
pub fn string(bytes: &MutableBytes) -> MutableString {
    MutableString {
        storage: Rc::clone(&bytes.storage),
        len: bytes.len(),
    }
}

impl MutableString {
    fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        // SAFETY: the shared allocation is single-threaded, and the borrowed
        // slice is confined to this call.
        let storage = unsafe { &*self.storage.get() };
        f(&storage[..self.len])
    }
}

impl PartialEq<str> for MutableString {
    fn eq(&self, other: &str) -> bool {
        self.with_bytes(|bytes| bytes == other.as_bytes())
    }
}

impl PartialEq<&str> for MutableString {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl fmt::Debug for MutableString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.with_bytes(|bytes| write!(formatter, "{:?}", String::from_utf8_lossy(bytes)))
    }
}

impl fmt::Display for MutableString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.with_bytes(|bytes| formatter.write_str(&String::from_utf8_lossy(bytes)))
    }
}

/// Returns a zero-copy byte view of a string.
///
/// Go returns a mutable slice here through `unsafe`. TiDB's consumers only
/// read that slice; Rust makes the actual contract explicit and prevents
/// mutation of immutable string storage.
pub const fn slice(value: &str) -> &[u8] {
    value.as_bytes()
}

/// Constructs a byte slice from a raw pointer and a length.
///
/// # Safety
///
/// `pointer` must be non-null and valid for reads of `length` bytes for the
/// returned lifetime. The memory must not be mutated while the returned slice
/// is borrowed. For a zero length, `pointer` must still be aligned and
/// non-null as required by [`std::slice::from_raw_parts`].
pub unsafe fn get_bytes_from_ptr<'a>(pointer: *const u8, length: usize) -> &'a [u8] {
    // SAFETY: the caller owns the complete raw-pointer validity contract.
    unsafe { std::slice::from_raw_parts(pointer, length) }
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[test]
    fn TestString() {
        let mut bytes = MutableBytes::new(b"hello world".to_vec());
        let value = string(&bytes);
        assert_eq!(value, "hello world");

        bytes.set(0, b'a');
        assert_eq!(value, "aello world");

        bytes.append(b"abc");
        assert_eq!(value, "aello world");
    }

    #[test]
    fn TestByte() {
        let value = "hello world";
        assert_eq!(slice(value), b"hello world");
    }

    #[test]
    fn TestMutable() {
        let mut bytes = MutableBytes::new(vec![b'a', b'b', b'c']);
        let mutable = string(&bytes);
        assert_eq!(mutable, "abc");

        bytes.set(0, b's');
        assert_eq!(mutable, "sbc");
    }
}
