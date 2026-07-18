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

//! Source-backed tests for high-bit tagged pointer metadata.

use tidb_exec::tagged_ptr::{
    get_tagged_bits_from_usize, TagPtrHelper, MAX_TAGGED_BITS, MAX_TAGGED_MASK,
};

#[test]
fn tagged_bits_match_source_width_cap() {
    // Source: pkg/executor/join/tagged_ptr.go:72-77.
    // Direct Go coverage: pkg/executor/join/tagged_ptr_test.go:24
    // (TestTaggedBits).
    let mut pointer = 0_usize;
    for index in 0..=64 {
        let expected = (64_i32 - index).min(i32::from(MAX_TAGGED_BITS));
        assert_eq!(
            i32::from(get_tagged_bits_from_usize(pointer)),
            expected,
            "leading-zero width at iteration {index}"
        );
        pointer = (pointer << 1) + 1;
    }
}

#[test]
fn tag_helper_masks_match_source_initialization() {
    // Source: pkg/executor/join/tagged_ptr.go:44-48.
    // Direct Go coverage: pkg/executor/join/tagged_ptr_test.go:33
    // (TestTagHelperInit).
    let mut expected = !MAX_TAGGED_MASK;
    for tagged_bits in (0..=MAX_TAGGED_BITS).rev() {
        let mut helper = TagPtrHelper::default();
        helper.init(tagged_bits);
        assert_eq!(helper.tagged_mask(), expected);
        expected <<= 1;
    }
}

#[test]
fn tagged_pointer_roundtrip_clears_only_tag_bits() {
    // Source: pkg/executor/join/tagged_ptr.go:50-68.
    // Direct Go coverage: pkg/executor/join/tagged_ptr_test.go:43
    // (TestTagHelper). Raw addresses stand in for the source's allocated byte
    // slice; this keeps the bit contract test deterministic and safe.
    let start_pointer = 0x0000_1000_0000_1000_usize;
    let end_pointer = 0x0000_1000_0000_2000_usize;
    let tagged_bits = get_tagged_bits_from_usize(start_pointer | end_pointer);
    let mut helper = TagPtrHelper::default();
    helper.init(tagged_bits);

    let mut tagged_value = 0x1234_u64 << (64 - MAX_TAGGED_BITS);
    while tagged_value & helper.tagged_mask() != tagged_value {
        tagged_value <<= 1;
    }
    assert_ne!(tagged_value, 0);

    for pointer in [start_pointer, end_pointer] {
        let tagged_pointer = helper.to_tagged_ptr(tagged_value, pointer);
        assert_eq!(
            helper.get_tagged_value(tagged_pointer.raw() as u64),
            tagged_value
        );
        assert_eq!(helper.to_raw_pointer(tagged_pointer), pointer);
    }
}
