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

//! Go `pkg/planner/cascades/base` lands as a complete package.
//!
//! The Go package is four declaration files whose production symbols were
//! already transcreated as separate leaves in this crate, so this module is the
//! package's single named surface rather than a second implementation: it
//! re-exports every production symbol under one path and owns the package's
//! test suite, which no leaf carried.
//!
//! Symbol ownership:
//!
//! - `base.go` -> [`crate::base_traits`] (`Hash64`, `Equals`, `HashEquals`).
//! - `hash_equaler.go` -> [`crate::hash_equaler`] (`Hasher`, `Hash64a`,
//!   `NewHashEqualer`, `NilFlag`, `NotNilFlag`, and the unexported `hasher`
//!   state plus `offset64`/`prime64`).
//! - `task_scheduler_base.go` -> [`crate::scheduler_contract`] (`Scheduler`).
//! - `task_stack_base.go` -> [`crate::stack_contract`] (`Stack`, `Task`).
//!
//! The sole internal Go import, `pkg/planner/cascades/util`, is already ported
//! as [`crate::string_writer`], so nothing in this package is blocked on it.

pub use crate::base_traits::{Equals, Hash64, HashEquals};
pub use crate::hash_equaler::{
    new_hash_equaler, Hash64a, HashEqualer, Hasher, NIL_FLAG, NOT_NIL_FLAG,
};
pub use crate::scheduler_contract::Scheduler;
pub use crate::stack_contract::{Stack, StackTask};

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `hash_equaler_test.go` `TmpStr`.
    struct TmpStr {
        str1: String,
        str2: String,
    }

    impl TmpStr {
        fn hash64(&self, hasher: &mut dyn Hasher) {
            hasher.hash_string(&self.str1);
            hasher.hash_string(&self.str2);
        }
    }

    /// Go `hash_equaler_test.go` `SX`, the shared hash/equal interface.
    trait Sx {
        fn hash64(&self, hasher: &mut dyn Hasher);
        fn equal(&self, other: &dyn Sx) -> bool;
        fn as_any(&self) -> &dyn std::any::Any;
    }

    /// Go `hash_equaler_test.go` `SA`.
    struct Sa {
        a: i64,
        b: String,
    }

    /// Go `hash_equaler_test.go` `SB`, deliberately field-identical to `SA`.
    struct Sb {
        a: i64,
        b: String,
    }

    impl Sx for Sa {
        fn hash64(&self, hasher: &mut dyn Hasher) {
            hasher.hash_int(self.a);
            hasher.hash_string(&self.b);
        }

        fn equal(&self, other: &dyn Sx) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|sa2| self.a == sa2.a && self.b == sa2.b)
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    impl Sx for Sb {
        fn hash64(&self, hasher: &mut dyn Hasher) {
            hasher.hash_int(self.a);
            hasher.hash_string(&self.b);
        }

        fn equal(&self, other: &dyn Sx) -> bool {
            other
                .as_any()
                .downcast_ref::<Self>()
                .is_some_and(|sb2| self.a == sb2.a && self.b == sb2.b)
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    /// Go `base_test.go` `testcase`.
    #[derive(PartialEq, Eq)]
    struct Testcase {
        a: i64,
        b: i64,
        c: String,
    }

    impl Testcase {
        /// Go `testcase.EqualsT`, the statically typed comparison.
        fn equals_t(&self, other: &Self) -> bool {
            self.a == other.a && self.b == other.b && self.c == other.c
        }

        /// Go `testcase.EqualsAny`, the dynamically typed comparison.
        fn equals_any(&self, other: &dyn std::any::Any) -> bool {
            other
                .downcast_ref::<Self>()
                .is_some_and(|tc1| self.a == tc1.a && self.b == tc1.b && self.c == tc1.c)
        }
    }

    /// Go `TestStringLen`.
    ///
    /// The per-string byte-length prefix is what keeps a split pair distinct
    /// from its concatenation; the golden sums pin that framing exactly.
    #[test]
    fn test_string_len() {
        let mut hasher1 = new_hash_equaler();
        let mut hasher2 = new_hash_equaler();
        let a = TmpStr {
            str1: "abc".to_owned(),
            str2: "def".to_owned(),
        };
        let b = TmpStr {
            str1: "abcdef".to_owned(),
            str2: String::new(),
        };
        a.hash64(&mut hasher1);
        b.hash64(&mut hasher2);
        assert_ne!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 3_287_175_144_666_194_158);
        assert_eq!(Hasher::sum64(&hasher2), 12_488_879_987_038_134_242);
    }

    /// Go `TestStructType`.
    ///
    /// Two distinct types with identical fields hash identically on purpose:
    /// the Go type identity is never hashed, and `Equal` resolves the conflict.
    #[test]
    fn test_struct_type() {
        let mut hasher1 = new_hash_equaler();
        let mut hasher2 = new_hash_equaler();
        let a = Sa {
            a: 1,
            b: "abc".to_owned(),
        };
        let b = Sb {
            a: 1,
            b: "abc".to_owned(),
        };
        a.hash64(&mut hasher1);
        b.hash64(&mut hasher2);
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 11_017_142_549_512_311_475);
        assert!(!a.equal(&b));
    }

    /// Go `TestHash64a`.
    ///
    /// Each step asserts the two hashers agree, and additionally pins the
    /// running digest so the primitive update order stays byte-exact.
    #[test]
    fn test_hash64a() {
        let mut hasher1 = new_hash_equaler();
        let mut hasher2 = new_hash_equaler();

        Hasher::hash_bool(&mut hasher1, true);
        Hasher::hash_bool(&mut hasher2, true);
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 12_638_152_016_183_539_244);

        Hasher::hash_bool(&mut hasher1, false);
        Hasher::hash_bool(&mut hasher2, false);
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 589_727_492_704_079_044);

        Hasher::hash_int(&mut hasher1, 199);
        Hasher::hash_int(&mut hasher2, 199);
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 15_033_813_906_414_233_881);

        Hasher::hash_int64(&mut hasher1, 13_534_523_462_346);
        Hasher::hash_int64(&mut hasher2, 13_534_523_462_346);
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 15_579_311_309_279_946_633);

        Hasher::hash_uint64(&mut hasher1, 13_534_523_462_346);
        Hasher::hash_uint64(&mut hasher2, 13_534_523_462_346);
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 1_135_633_338_855_833_817);

        Hasher::hash_string(&mut hasher1, "hello");
        Hasher::hash_string(&mut hasher2, "hello");
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 17_104_544_475_406_809_700);

        Hasher::hash_bytes(&mut hasher1, b"world");
        Hasher::hash_bytes(&mut hasher2, b"world");
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 16_245_941_199_823_296_281);

        for rune in ['我', '是', '谁'] {
            Hasher::hash_rune(&mut hasher1, rune as i32);
            Hasher::hash_rune(&mut hasher2, rune as i32);
        }
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 14_154_188_946_827_163_292);

        Hasher::reset(&mut hasher1);
        Hasher::reset(&mut hasher2);
        assert_eq!(Hasher::sum64(&hasher1), 14_695_981_039_346_656_037);

        let long = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Hasher::hash_string(&mut hasher1, long);
        Hasher::hash_string(&mut hasher2, long);
        assert_eq!(Hasher::sum64(&hasher1), Hasher::sum64(&hasher2));
        assert_eq!(Hasher::sum64(&hasher1), 1_281_742_727_981_961_626);
    }

    /// Go `BenchmarkEqualsT` and `BenchmarkEqualsAny` as behavior tests.
    ///
    /// The Go benchmarks exist to compare the typed and dynamic equality
    /// shapes; Rust keeps the comparison as a correctness check because the
    /// timing question is a Go-runtime dispatch question with no Rust analogue.
    #[test]
    fn test_equals_t_and_equals_any_agree() {
        let tc1 = Testcase {
            a: 1,
            b: 2,
            c: "3".to_owned(),
        };
        let tc2 = Testcase {
            a: 1,
            b: 2,
            c: "3".to_owned(),
        };
        assert!(tc1.equals_t(&tc2));
        assert!(tc1.equals_any(&tc2));

        let other = Testcase {
            a: 1,
            b: 2,
            c: "4".to_owned(),
        };
        assert!(!tc1.equals_t(&other));
        assert!(!tc1.equals_any(&other));
        assert!(!tc1.equals_any(&7_i64));
    }

    /// Go `hash_equaler.go` `HashFloat64`, `HashByte`, and the multi-byte rune
    /// path of `HashString`, which no source test exercises directly.
    #[test]
    fn remaining_primitives_match_source_encoding() {
        let mut hasher = new_hash_equaler();
        Hasher::hash_float64(&mut hasher, 1.25);
        assert_eq!(Hasher::sum64(&hasher), 14_510_524_660_774_451_167);

        // Go hashes the raw IEEE-754 bits, so the two zeroes stay distinct.
        let mut hasher = new_hash_equaler();
        Hasher::hash_float64(&mut hasher, 0.0);
        assert_eq!(Hasher::sum64(&hasher), 12_638_153_115_695_167_455);
        let mut hasher = new_hash_equaler();
        Hasher::hash_float64(&mut hasher, -0.0);
        assert_eq!(Hasher::sum64(&hasher), 3_414_781_078_840_391_647);

        let mut hasher = new_hash_equaler();
        Hasher::hash_byte(&mut hasher, 0x7f);
        assert_eq!(Hasher::sum64(&hasher), 12_638_211_389_811_462_638);

        // "我是谁" is 3 runes over 9 bytes: the length prefix is the byte count.
        let mut hasher = new_hash_equaler();
        Hasher::hash_string(&mut hasher, "我是谁");
        assert_eq!(Hasher::sum64(&hasher), 8_053_452_161_411_667_571);
    }

    /// Go `hash_equaler.go` `Reset`, `SetCache`, and `Cache`.
    #[test]
    fn cache_lifecycle_matches_source() {
        let mut hasher = new_hash_equaler();
        assert!(Hasher::cache(&hasher).is_empty());
        Hasher::set_cache(&mut hasher, vec![1, 2, 3]);
        assert_eq!(Hasher::cache(&hasher), &[1, 2, 3]);
        Hasher::hash_int(&mut hasher, 42);
        Hasher::reset(&mut hasher);
        // Reset restores the offset basis and truncates the cache to length 0.
        assert_eq!(Hasher::sum64(&hasher), 14_695_981_039_346_656_037);
        assert!(Hasher::cache(&hasher).is_empty());
    }

    /// Go `hash_equaler.go` `NilFlag` / `NotNilFlag`.
    #[test]
    fn nil_flags_match_source_values() {
        assert_eq!(NIL_FLAG, 0);
        assert_eq!(NOT_NIL_FLAG, 1);
        // The not-nil marker must not collide with a hashed zero byte.
        let mut nil_hasher = new_hash_equaler();
        Hasher::hash_byte(&mut nil_hasher, NIL_FLAG);
        let mut not_nil_hasher = new_hash_equaler();
        Hasher::hash_byte(&mut not_nil_hasher, NOT_NIL_FLAG);
        Hasher::hash_byte(&mut not_nil_hasher, 0);
        assert_ne!(Hasher::sum64(&nil_hasher), Hasher::sum64(&not_nil_hasher));
    }

    /// `Hash64a` round-trips its raw digest, matching Go's `uint64` newtype.
    #[test]
    fn hash64a_wraps_a_raw_digest() {
        assert_eq!(Hash64a::new(7).raw(), 7);
    }
}
