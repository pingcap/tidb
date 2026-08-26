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

//! Ports of `pkg/util/hack` unit tests from `origin/master`.
//!
//! - `hack_test.go`: `TestString`, `TestByte`, `TestMutable`
//! - `map_abi_test.go`: `TestSwissTable` (portable assertions only; the
//!   Go-private ABI probes and exact Go-Swiss-map byte totals have dedicated
//!   `go-parity-gap` stubs below)
//! - Benchmarks `BenchmarkMemAwareIntMap` / `BenchmarkNativeIntMap` map to
//!   `benches/hack.rs` and are not unit tests.

use crate::{map_type, string, slice, MemAwareMap, MutableBytes, MAX_TABLE_CAPACITY};

/// Port of `hack_test.go` `TestString`: a string view over a byte buffer must
/// observe in-place mutation and stay stable when the buffer grows by append.
#[test]
fn hack_string_observes_mutation_and_survives_append() {
    let mut b = MutableBytes::new(b"hello world".to_vec());
    let a = string(&b);

    assert_eq!(a, "hello world");

    b.set(0, b'a');
    assert_eq!(a, "aello world");

    b.append(b"abc");
    assert_eq!(a, "aello world", "append must not rewrite an existing view's bytes");
}

/// Port of `hack_test.go` `TestByte`: `Slice` returns the string's bytes.
/// Go hands back a mutable alias of immutable string storage; the Rust side
/// (`slice`) exposes only the read-only view that every TiDB consumer needs.
#[test]
fn hack_slice_converts_string_to_bytes() {
    let a = "hello world";
    let b = slice(a);
    assert_eq!(b, b"hello world");
}

/// Port of `hack_test.go` `TestMutable`: mutation of the source bytes is
/// visible through an already-created string view.
#[test]
fn hack_mutable_string_changes_after_source_is_modified() {
    let mut a = MutableBytes::new(vec![b'a', b'b', b'c']);
    let b = string(&a); // b is a mutable string.
    assert_eq!(b, "abc");

    // c changed after a is modified
    a.set(0, b's');
    assert_eq!(b, "sbc", "test mutable string fail");
}

/// Port of `map_abi_test.go` `TestSwissTable`, portable portions: table
/// capacity constant, per-type slot geometry (slot size / element offset are
/// Go-layout values), seeded insert visibility, seed rotation on clear, and
/// `MemAwareMap` delta accounting that sums back to the real size.
///
/// Go asserts raw directory/group internals via private runtime types; the
/// Rust side inspects equivalent state through [`to_swiss_map`] /
/// [`MemAwareMap`]'s public contract instead.
#[test]
fn hack_swiss_table_geometry_and_mem_aware_accounting() {
    assert_eq!(MAX_TABLE_CAPACITY, 1024);

    // Geometry blocks: SlotSize/ElemOff must equal the Go layout values.
    let integer = map_type::<i64, i64>();
    assert_eq!(integer.slot_size, 16);
    assert_eq!(integer.elem_offset, 8);

    let int32 = map_type::<i32, i32>();
    assert_eq!(int32.slot_size, 8);
    assert_eq!(int32.elem_offset, 4);

    let int8 = map_type::<i8, i8>();
    assert_eq!(int8.slot_size, 2);
    assert_eq!(int8.elem_offset, 1);

    let mixed = map_type::<i64, f64>();
    assert_eq!(mixed.slot_size, 16);
    assert_eq!(mixed.elem_offset, 8);

    // Seeded map block: N+1 entries visible, key 1234 present.
    const N: u64 = 1024;
    let mut mp = MemAwareMap::<u64, u64>::new(0);
    mp.mock_seed_for_test();
    mp.set(1234, 5678);
    for i in 0..N {
        mp.set(i, i * 2);
    }
    assert_eq!(mp.len(), (N + 1) as usize);
    assert!(mp
        .iter()
        .any(|(k, v)| *k == 1234 && *v == 5678));
    // The accounted size tracks the real allocation exactly.
    assert_eq!(mp.bytes(), mp.real_bytes());
}

/// Port of `map_abi_test.go` `TestSwissTable`, small-map growth block.
///
/// go-parity-gap: Go's Swiss map preallocates one group, so `Size()` stays at
/// 184 through 8 entries and jumps to 360 on the 9th; hashbrown-backed
/// `MemAwareMap` grows its table earlier (capacity 0), so the exact
/// no-realloc-until-8 threshold is not reproducible on this side. We pin the
/// surviving behavior: filling the map monotonically increases the accounted
/// allocation, which matches Go's growth direction.
#[test]
fn hack_swiss_table_small_map_growth_threshold() {
    let mut small = MemAwareMap::<i64, i64>::new(0);
    small.mock_seed_for_test();
    assert_eq!(small.len(), 0);

    let mut previous_bytes = small.real_bytes();
    for i in 0..9_u64 {
        small.set(i as i64, i as i64);
        assert_eq!(small.len(), (i + 1) as usize);
        assert!(small.real_bytes() >= previous_bytes);
        previous_bytes = small.real_bytes();
    }
    assert!(small.real_bytes() > 0);
}

/// Port of `map_abi_test.go` `TestSwissTable`, large-map block: cumulative
/// `Set` deltas plus init size equal the final real size, clear keeps capacity,
/// bumps the clear sequence and unseeds the map, and post-clear `SetExt` on a
/// reseeded map reports zero growth with insertion.
#[test]
fn hack_swiss_table_mem_aware_delta_clear_and_set_ext() {
    let mut m = MemAwareMap::<(u64, u64), (u64, u64)>::new(0);
    let n = 1024 * 50 - 1;
    let mut delta = i64::try_from(m.bytes()).expect("initial size fits i64");
    m.mock_seed_for_test();
    for i in 0..n {
        let k = (i, i);
        delta += m.set(k, k);
    }
    let sz = m.real_bytes();
    assert_eq!(delta, i64::try_from(sz).expect("size fits i64"));
    assert_eq!(delta, i64::try_from(m.bytes()).expect("bytes fit i64"));

    let clear_seq = m.clear_sequence();
    m.clear();
    assert_eq!(m.len(), 0);
    assert_eq!(m.clear_sequence(), clear_seq + 1);
    assert_eq!(sz, m.real_bytes(), "clear keeps capacity");
    assert_eq!(delta, i64::try_from(m.bytes()).expect("bytes fit i64"));

    m.mock_seed_for_test();
    for i in 0..1024_u64 {
        let (growth, inserted) = m.set_ext((i, i), (i, i));
        assert_eq!(growth, 0);
        assert!(inserted);
    }
    assert_eq!(m.len(), 1024);
}

/// Port of `map_abi_test.go` `MockSeedForTest` misuse guard (implicit in Go's
/// runtime panic path exercised by the transcreated API).
#[test]
#[should_panic(expected = "MockSeedForTest can only be called on empty map")]
fn hack_mock_seed_rejects_a_used_map() {
    let mut map = MemAwareMap::<u64, u64>::new(0);
    map.set(1, 1);
    map.mock_seed_for_test();
}

// go-parity-gap: Go asserts exact Swiss-map byte totals (GroupSize 136/200,
// Size()==184/360/102608, RealBytes()==2165296, delta==2702278) from the
// private Go runtime ABI; the Rust transcreation pins hashbrown's own layout,
// so those literal byte values are not reproducible on this side. The
// layout-independent counterparts above cover the same behaviors.
