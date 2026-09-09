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

//! Port ledger for the tail of `pkg/planner/cascades/base/hash_equaler_test.go`
//! (`pkg/planner.part2` items 61-62 on `origin/master`; item 60,
//! `TestStringLen`, already runs under batch part1's cardinality ports).
//!
//! Both remaining Go tests are REAL functional ports over
//! [`tidb_planner::hash_equaler`], the transcreation of
//! `pkg/planner/cascades/base/hash_equaler.go`: FNV-1a absorption
//! (`offset64 ^ v * prime64`, hash_equaler.go:105-115), per-primitive updates
//! (hash_equaler.go:123-189), string framing as byte-length followed by runes
//! (hash_equaler.go:164-171), and `Reset` restoring the offset basis while
//! keeping the cache allocation (hash_equaler.go:87-91).
//!
//! The two local structs below mirror Go's `SA`/`SB` (hash_equaler_test.go:66,
//! :85): equal fields hash equally because `Hash64` implementations only feed
//! field values into the hasher, never the runtime type tag; the `Equal`
//! surface keeps Go's typed-dispatch semantics via `dyn Any` downcasting, so a
//! cross-type comparison still reports unequal.

use std::any::Any;

use tidb_planner::hash_equaler::{new_hash_equaler, Hasher};

/// Go `SX` interface (hash_equaler_test.go:62-65): dynamic-dispatch hashing
/// plus a typed `Equal` that rejects other implementations.
trait SX {
    fn hash64(&self, hasher: &mut dyn Hasher);
    /// Mirrors Go `Equal(other any) bool` type assertion semantics.
    fn equals(&self, other: &dyn Any) -> bool;
}

/// Go `SA` (hash_equaler_test.go:66-70) and its methods :72-83.
struct SA {
    a: i64,
    b: &'static str,
}

impl SX for SA {
    fn hash64(&self, hasher: &mut dyn Hasher) {
        hasher.hash_int(self.a);
        hasher.hash_string(self.b);
    }

    fn equals(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<SA>()
            .is_some_and(|sa| self.a == sa.a && self.b == sa.b)
    }
}

/// Go `SB` (hash_equaler_test.go:85-89) and its methods :91-102: identical
/// field layout and identical `Hash64` feeding, but a distinct Go type.
struct SB {
    a: i64,
    b: &'static str,
}

impl SX for SB {
    fn hash64(&self, hasher: &mut dyn Hasher) {
        hasher.hash_int(self.a);
        hasher.hash_string(self.b);
    }

    fn equals(&self, other: &dyn Any) -> bool {
        other
            .downcast_ref::<SB>()
            .is_some_and(|sb| self.a == sb.a && self.b == sb.b)
    }
}

/// GO PORT of `pkg/planner/cascades/base/hash_equaler_test.go:88
/// TestStructType`.
///
/// Re-derived contract: `SA{1,"abc"}` and `SB{1,"abc"}` are different Go types
/// with equal fields; both `Hash64`s feed the same primitives, so
/// `Sum64()`s MUST be equal (the hasher intentionally never hashes the
/// reflect-runtime type pointer — comment at hash_equaler_test.go:99-104 and
/// the design note at hash_equaler.go:117-120) — while `a.Equals(&b)` stays
/// false because `Equal` compares concrete types (hash_equaler_test.go:107
/// passes `*SB` to `(*SA).Equal`, failing the `.(*SA)` assertion at :76).
#[test]
fn struct_type_same_fields_share_digest_but_unequal_cross_type() {
    let mut hasher1 = new_hash_equaler();
    let mut hasher2 = new_hash_equaler();
    let a = SA { a: 1, b: "abc" };
    let b = SB { a: 1, b: "abc" };
    a.hash64(&mut hasher1);
    b.hash64(&mut hasher2);
    assert_eq!(hasher1.sum64(), hasher2.sum64());
    // Cross-type dynamic dispatch: a.Equals(&b) == false.
    let b_any: &dyn Any = &b;
    assert!(!a.equals(b_any));
    // Same-type dispatch still works (guards against an implementation that
    // makes every pair unequal).
    let a_again = SA { a: 1, b: "abc" };
    let a_any: &dyn Any = &a_again;
    assert!(a.equals(a_any));
}

/// GO PORT of `pkg/planner/cascades/base/hash_equaler_test.go:113
/// TestHash64a`.
///
/// Re-derived contract: two independent `NewHashEqualer()` digests stay equal
/// after every paired primitive update — `HashBool(true/false)`
/// (hash_equaler.go:123 absorbs 1/0), `HashInt(199)` :133, `HashInt64(1353
/// 452346 2346)` :139, `HashUint64` :145, `HashString("hello")` :164,
/// `HashBytes([]byte("world"))` :178, three `HashRune` updates :157 — and
/// after `Reset()` (hash_equaler.go:87) both round-trip the 62-character
/// alphanumeric string afresh. Equality at each checkpoint pins incremental
/// determinism step-by-step, matching Go's require-per-step shape.
#[test]
fn hash64a_paired_primitive_updates_and_reset_keep_two_digests_equal() {
    let mut hasher1 = new_hash_equaler();
    let mut hasher2 = new_hash_equaler();

    hasher1.hash_bool(true);
    hasher2.hash_bool(true);
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    hasher1.hash_bool(false);
    hasher2.hash_bool(false);
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    hasher1.hash_int(199);
    hasher2.hash_int(199);
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    hasher1.hash_int64(13_534_523_462_346);
    hasher2.hash_int64(13_534_523_462_346);
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    hasher1.hash_uint64(13_534_523_462_346);
    hasher2.hash_uint64(13_534_523_462_346);
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    hasher1.hash_string("hello");
    hasher2.hash_string("hello");
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    hasher1.hash_bytes(b"world");
    hasher2.hash_bytes(b"world");
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    // Go feeds the runes 我/是/谁 (U+6211/U+662F/U+8C01); the crate stores
    // runes as signed code points (hash_equaler.rs hash_rune).
    for rune in ['我', '是', '谁'] {
        hasher1.hash_rune(rune as i32);
    }
    for rune in ['我', '是', '谁'] {
        hasher2.hash_rune(rune as i32);
    }
    assert_eq!(hasher1.sum64(), hasher2.sum64());

    hasher1.reset();
    hasher2.reset();
    const ALPHANUMERIC: &str =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    hasher1.hash_string(ALPHANUMERIC);
    hasher2.hash_string(ALPHANUMERIC);
    assert_eq!(hasher1.sum64(), hasher2.sum64());
}
