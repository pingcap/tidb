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

/// Go `strings.ToLower`: the per-rune SIMPLE lowercase mapping
/// (`unicode.ToLower`), with none of Unicode's conditional special
/// casing -- Greek final sigma stays `σ` at a word's end and `İ` folds
/// to plain `i`, both of which diverge from Rust's `str::to_lowercase`.
/// Verified per rune: U+0130 is the only code point whose full
/// lowercase mapping is multi-character, so first-character + the `İ`
/// entry reproduces Go's table exactly. Delegates to the generated
/// `tidb-mysql::simple_case` table (Go `unicode.CaseRanges`, Unicode
/// 15.0.0), which is the authoritative implementation.
pub fn go_to_lower(input: impl AsRef<str>) -> String {
    tidb_mysql::to_lowercase(input.as_ref())
}

/// Go `strings.ToUpper`: the per-rune SIMPLE uppercase mapping
/// (`unicode.ToUpper`). Rust's full uppercase expands 102 code points
/// to multiple characters (`ß` -> `SS`, the Latin and Armenian
/// ligatures, the Greek iota-subscript vowels); Go's simple table
/// leaves the ligatures and `ß` unchanged and folds the 27 Greek
/// iota-subscript forms to their dropped-subscript vowel. Delegates to
/// the generated `tidb-mysql::simple_case` table (Go
/// `unicode.CaseRanges`, Unicode 15.0.0), the authoritative
/// implementation.
pub fn go_to_upper(input: impl AsRef<str>) -> String {
    tidb_mysql::to_uppercase(input.as_ref())
}

#[cfg(test)]
mod case_tests {
    use super::{go_to_lower, go_to_upper};

    #[test]
    fn go_to_upper_matches_go_simple_mapping() {
        // `straße` stays `STRAßE` (TiDB's captured behavior): the full
        // uppercase of `ß` would be "SS".
        assert_eq!(go_to_upper("stra\u{00DF}e"), "STRA\u{00DF}E");
        assert_ne!("stra\u{00DF}e".to_uppercase(), "STRA\u{00DF}E");
        // The Greek iota-subscript vowels take Go's dropped-subscript
        // simple form, NOT the identity and NOT the multi-char expansion.
        assert_eq!(go_to_upper("\u{1FA4}"), "\u{1FAC}");
        assert_ne!("\u{1FA4}".to_uppercase(), "\u{1FAC}");
        assert_eq!(go_to_upper("\u{1FB3}"), "\u{1FBC}");
        // ASCII and already-uppercase text pass through.
        assert_eq!(go_to_upper("aBc_01"), "ABC_01");
        assert_eq!(go_to_upper("中文"), "中文");
    }

    #[test]
    fn go_to_lower_matches_go_simple_mapping() {
        // The plan's #196 one-line reproduction: Greek capital sigma has
        // a word-end final form that Rust's `str::to_lowercase` picks
        // and Go never does.
        assert_eq!(go_to_lower("ΟΔΟΣ"), "οδοσ");
        assert_ne!("ΟΔΟΣ".to_lowercase(), "οδοσ");
        // Turkish İ: Go's simple table folds to plain `i`; Rust's full
        // mapping appends the combining dot.
        assert_eq!(go_to_lower("\u{0130}"), "i");
        assert_ne!("\u{0130}".to_lowercase(), "i");
        // ASCII and already-lowercase text pass through.
        assert_eq!(go_to_lower("AbC_01"), "abc_01");
        assert_eq!(go_to_lower("中文"), "中文");
    }
}
