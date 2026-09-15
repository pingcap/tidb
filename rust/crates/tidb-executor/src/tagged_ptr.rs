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

//! Top-bit pointer tagging metadata from `pkg/executor/join/tagged_ptr.go`.
//!
//! The Go source stores a raw pointer and a tag in one `uintptr`. This leaf
//! carries the same bit contract over an opaque raw address (`usize`) without
//! creating or dereferencing an unsafe pointer. Join hash-table ownership,
//! allocation, and concurrent access remain outside this value boundary.

/// Maximum number of high bits reserved for a tag in a 64-bit address.
pub const MAX_TAGGED_BITS: u8 = 24;
/// Lower 40 bits that are not reserved for a tag at the maximum width.
pub const MAX_TAGGED_MASK: u64 = 0xffffffffff;

/// An opaque raw address with optional high-bit tag bits set.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TaggedPtr(usize);

impl TaggedPtr {
    /// Returns the encoded raw address.
    #[must_use]
    pub const fn raw(self) -> usize {
        self.0
    }
}

/// Mask helper for extracting and clearing high-bit tags.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TagPtrHelper {
    tagged_mask: u64,
}

impl TagPtrHelper {
    /// Initializes the high-bit mask for a tag of `tagged_bits` width.
    pub fn init(&mut self, tagged_bits: u8) {
        if tagged_bits == 0 {
            self.tagged_mask = 0;
            return;
        }
        let hash_value_tagged_mask = (1_u64 << tagged_bits) - 1;
        let hash_value_tagged_offset = 64 - tagged_bits;
        self.tagged_mask = hash_value_tagged_mask << hash_value_tagged_offset;
    }

    /// Returns the configured high-bit tag portion of a raw value.
    #[must_use]
    pub fn get_tagged_value(&self, hash_value: u64) -> u64 {
        hash_value & self.tagged_mask
    }

    /// Returns the configured mask, useful for source-contract inspection.
    #[must_use]
    pub const fn tagged_mask(&self) -> u64 {
        self.tagged_mask
    }

    /// Encodes a tag into the high bits of a raw address.
    #[must_use]
    pub fn to_tagged_ptr(&self, tagged_value: u64, pointer: usize) -> TaggedPtr {
        TaggedPtr(pointer | tagged_value as usize)
    }

    /// Clears the tag bits and returns the original raw address.
    #[must_use]
    pub fn to_raw_pointer(&self, tagged_ptr: TaggedPtr) -> usize {
        TaggedPtr(tagged_ptr.0 & !(self.tagged_mask as usize)).raw()
    }
}

/// Determines how many leading zero bits can carry a tag on this address.
///
/// The source intentionally disables tagging on non-64-bit pointers and caps
/// the result at [`MAX_TAGGED_BITS`].
#[must_use]
pub fn get_tagged_bits_from_usize(pointer: usize) -> u8 {
    if usize::BITS != 64 {
        return 0;
    }
    (pointer.leading_zeros() as u8).min(MAX_TAGGED_BITS)
}
