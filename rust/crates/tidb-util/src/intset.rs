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

//! Transcreation of Go `pkg/util/intset/fast_int_set.go`: `FastIntSet`, a
//! small-int-optimized set used throughout the planner.
//!
//! Faithful adaptation: Go caches values in `[0,64)` in a `uint64` bitmap
//! and spills the rest into `golang.org/x/tools/container/intsets.Sparse`.
//! The Rust port keeps the same `u64` fast path and represents the spill
//! set with a `BTreeSet<i64>` — which provides the same observable
//! ordering, membership, `Min`/`Max`, and `LowerBound` behavior `Sparse`
//! exposes here. Once a value outside `[0,64)` is inserted the set becomes
//! "large" (and stays large even if that value is later removed), exactly
//! as in the source; `equals` handles the large-but-all-small case.

use std::collections::BTreeSet;
use std::fmt;

const SMALL_CUT_OFF: i64 = 64;
/// Go `intsets.MaxInt` sentinel (max platform int; 64-bit here) returned by
/// [`FastIntSet::next`] when no larger element exists.
pub const MAX_INT: i64 = i64::MAX;
/// Go `intsets.MinInt`.
pub const MIN_INT: i64 = i64::MIN;

/// A small-int-optimized set (Go `FastIntSet`).
#[derive(Clone, Default, Debug)]
pub struct FastIntSet {
    // uint64 bitmap of 0..63.
    small: u64,
    // Spill set for values outside [0,64); `Some` once "large".
    large: Option<BTreeSet<i64>>,
}

impl FastIntSet {
    /// Go `NewFastIntSet`.
    pub fn new(values: &[i64]) -> FastIntSet {
        let mut res = FastIntSet::default();
        for &v in values {
            res.insert(v);
        }
        res
    }

    /// Go `Len`.
    pub fn len(&self) -> usize {
        match &self.large {
            None => self.small.count_ones() as usize,
            Some(l) => l.len(),
        }
    }

    /// Go `IsEmpty`.
    pub fn is_empty(&self) -> bool {
        self.small == 0 && self.large.as_ref().is_none_or(|l| l.is_empty())
    }

    /// Go `Only1Zero`.
    pub fn only1_zero(&self) -> bool {
        self.len() == 1 && self.has(0)
    }

    /// Go `Insert`.
    pub fn insert(&mut self, i: i64) {
        let is_small = (0..SMALL_CUT_OFF).contains(&i);
        if is_small {
            self.small |= 1u64 << (i as u64);
        }
        if !is_small && self.large.is_none() {
            self.large = Some(self.to_large());
        }
        if let Some(l) = &mut self.large {
            l.insert(i);
        }
    }

    // Go `toLarge`: materialize the current contents into a BTreeSet.
    fn to_large(&self) -> BTreeSet<i64> {
        if let Some(l) = &self.large {
            return l.clone();
        }
        let mut large = BTreeSet::new();
        let (mut i, mut ok) = self.next(0);
        while ok {
            large.insert(i);
            let n = self.next(i + 1);
            i = n.0;
            ok = n.1;
        }
        large
    }

    /// Go `Next`: the next present value `>= start_val`, only seeking
    /// non-negative values (negatives are skipped, as in the source).
    /// Returns `(MAX_INT, false)` when none.
    pub fn next(&self, start_val: i64) -> (i64, bool) {
        let mut start_val = start_val;
        if start_val < SMALL_CUT_OFF {
            if start_val < 0 {
                start_val = 0;
            }
            let gap = (self.small >> (start_val as u64)).trailing_zeros() as i64;
            if gap < 64 {
                return (gap + start_val, true);
            }
        }
        if let Some(l) = &self.large {
            match l.range(start_val..).next() {
                Some(&res) => return (res, true),
                None => return (MAX_INT, false),
            }
        }
        (MAX_INT, false)
    }

    /// Go `Remove`.
    pub fn remove(&mut self, i: i64) {
        if (0..SMALL_CUT_OFF).contains(&i) {
            self.small &= !(1u64 << (i as u64));
        }
        if let Some(l) = &mut self.large {
            l.remove(&i);
        }
    }

    /// Go `Clear`.
    pub fn clear(&mut self) {
        self.small = 0;
        if let Some(l) = &mut self.large {
            l.clear();
        }
    }

    /// Go `Has`.
    pub fn has(&self, i: i64) -> bool {
        if (0..SMALL_CUT_OFF).contains(&i) {
            return self.small & (1u64 << (i as u64)) != 0;
        }
        match &self.large {
            Some(l) => l.contains(&i),
            None => false,
        }
    }

    /// Go `SortedArray`.
    pub fn sorted_array(&self) -> Vec<i64> {
        if self.is_empty() {
            return Vec::new();
        }
        if let Some(l) = &self.large {
            return l.iter().copied().collect();
        }
        let mut res = Vec::with_capacity(self.len());
        self.for_each(|i| res.push(i));
        res
    }

    /// Go `ForEach` (ascending).
    pub fn for_each(&self, mut f: impl FnMut(i64)) {
        if let Some(l) = &self.large {
            for &x in l {
                f(x);
            }
            return;
        }
        let mut v = self.small;
        while v != 0 {
            let i = v.trailing_zeros() as i64;
            f(i);
            v &= !(1u64 << (i as u64));
        }
    }

    /// Go `Copy`.
    pub fn copy(&self) -> FastIntSet {
        self.clone()
    }

    /// Go `CopyFrom`.
    pub fn copy_from(&mut self, target: &FastIntSet) {
        self.small = target.small;
        match &target.large {
            Some(tl) => {
                self.large = Some(tl.clone());
            }
            None => {
                if let Some(l) = &mut self.large {
                    l.clear();
                }
            }
        }
    }

    /// Go `Equals`.
    pub fn equals(&self, rhs: &FastIntSet) -> bool {
        match (&self.large, &rhs.large) {
            (None, None) => self.small == rhs.small,
            (Some(a), Some(b)) => a == b,
            _ => {
                // One side is large; it may still hold only small values
                // (e.g. insert 1, insert 65, remove 65).
                let (s1, s2, excess) = if self.large.is_some() {
                    let (small, excess) = self.large_to_small();
                    (small, rhs.small, excess)
                } else {
                    let (small, excess) = rhs.large_to_small();
                    (self.small, small, excess)
                };
                !excess && s1 == s2
            }
        }
    }

    // Go `largeToSmall`.
    fn large_to_small(&self) -> (u64, bool) {
        let l = self.large.as_ref().expect("set contains no large");
        let excess = l.iter().next().copied().unwrap_or(0) < 0
            || l.iter().next_back().copied().unwrap_or(-1) >= SMALL_CUT_OFF;
        (self.small, excess)
    }

    /// Go `GetSmallUInt64`.
    pub fn get_small_uint64(&self) -> Result<u64, String> {
        if self.large.is_some() {
            return Err("set contains large values, cannot get small uint64".into());
        }
        Ok(self.small)
    }

    // ---- logic operators ----

    /// Go `Difference`.
    pub fn difference(&self, rhs: &FastIntSet) -> FastIntSet {
        let mut r = self.copy();
        r.difference_with(rhs);
        r
    }

    /// Go `DifferenceWith`.
    pub fn difference_with(&mut self, rhs: &FastIntSet) {
        self.small &= !rhs.small;
        if self.large.is_none() {
            return;
        }
        let rhs_large = rhs.to_large();
        if let Some(l) = &mut self.large {
            for x in &rhs_large {
                l.remove(x);
            }
        }
    }

    /// Go `Union`.
    pub fn union(&self, rhs: &FastIntSet) -> FastIntSet {
        let mut c = self.copy();
        c.union_with(rhs);
        c
    }

    /// Go `UnionWith`.
    pub fn union_with(&mut self, rhs: &FastIntSet) {
        self.small |= rhs.small;
        if self.large.is_none() && rhs.large.is_none() {
            return;
        }
        if self.large.is_none() {
            self.large = Some(self.to_large());
        }
        let mut merged = self.large.take().unwrap();
        match &rhs.large {
            None => {
                let (mut i, mut ok) = rhs.next(0);
                while ok {
                    merged.insert(i);
                    let n = rhs.next(i + 1);
                    i = n.0;
                    ok = n.1;
                }
            }
            Some(rl) => {
                for &x in rl {
                    merged.insert(x);
                }
            }
        }
        self.large = Some(merged);
    }

    /// Go `Intersection`.
    pub fn intersection(&self, rhs: &FastIntSet) -> FastIntSet {
        let mut r = self.copy();
        r.intersection_with(rhs);
        r
    }

    /// Go `IntersectionWith`.
    pub fn intersection_with(&mut self, rhs: &FastIntSet) {
        self.small &= rhs.small;
        if rhs.large.is_none() {
            self.large = None;
        }
        if self.large.is_none() {
            return;
        }
        let rhs_large = rhs.to_large();
        let l = self.large.as_mut().unwrap();
        l.retain(|x| rhs_large.contains(x));
    }

    /// Go `Intersects`.
    pub fn intersects(&self, rhs: &FastIntSet) -> bool {
        if self.small & rhs.small != 0 {
            return true;
        }
        if self.large.is_none() || rhs.large.is_none() {
            return false;
        }
        let rhs_large = rhs.to_large();
        let Some(sl) = &self.large else { return false };
        sl.iter().any(|x| rhs_large.contains(x))
    }

    /// Go `SubsetOf`.
    pub fn subset_of(&self, rhs: &FastIntSet) -> bool {
        if self.large.is_none() {
            return (self.small & rhs.small) == self.small;
        }
        if let (Some(sl), Some(rl)) = (&self.large, &rhs.large) {
            return sl.is_subset(rl);
        }
        // self is large, rhs is small.
        let (_, excess) = self.large_to_small();
        if excess {
            return false;
        }
        (self.small & rhs.small) == self.small
    }

    /// Go `Shift`.
    pub fn shift(&self, delta: i64) -> FastIntSet {
        if self.large.is_none() {
            if delta > 0 {
                if (self.small.leading_zeros() as i64) - (64 - SMALL_CUT_OFF) >= delta {
                    return FastIntSet {
                        small: self.small << (delta as u64),
                        large: None,
                    };
                }
            } else if (self.small.trailing_zeros() as i64) >= -delta {
                return FastIntSet {
                    small: self.small >> ((-delta) as u64),
                    large: None,
                };
            }
        }
        let mut result = FastIntSet::default();
        self.for_each(|i| result.insert(i + delta));
        result
    }

    /// Go `AddRange` (inclusive `[from, to]`).
    pub fn add_range(&mut self, from: i64, to: i64) {
        assert!(to >= from, "invalid range when adding range to FastIntSet");
        let within_small_bounds = from >= 0 && to < SMALL_CUT_OFF;
        if within_small_bounds && self.large.is_none() {
            let n_values = (to - from + 1) as u64;
            // Go defines an over-width shift as 0, so `1<<64 - 1` is all
            // ones; replicate that (n_values == 64 only when from == 0).
            let ones = if n_values >= 64 {
                u64::MAX
            } else {
                (1u64 << n_values) - 1
            };
            self.small |= ones << (from as u64);
        } else {
            for i in from..=to {
                self.insert(i);
            }
        }
    }
}

impl fmt::Display for FastIntSet {
    // Go `String`: contiguous non-negative ranges as `a-b` (or `a,b` for a
    // pair, `a` for a singleton); every negative value is its own singleton.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::fmt::Write as _;
        let mut buf = String::from("(");
        let append_range = |buf: &mut String, start: i64, end: i64| {
            if buf.len() > 1 {
                buf.push(',');
            }
            if start == end {
                let _ = write!(buf, "{start}");
            } else if start + 1 == end {
                let _ = write!(buf, "{start},{end}");
            } else {
                let _ = write!(buf, "{start}-{end}");
            }
        };
        let mut range_start = -1i64;
        let mut range_end = -1i64;
        let mut have_range = false;
        self.for_each(|i| {
            if i < 0 {
                append_range(&mut buf, i, i);
                return;
            }
            if have_range && range_end == i - 1 {
                range_end = i;
            } else {
                if have_range {
                    append_range(&mut buf, range_start, range_end);
                }
                range_start = i;
                range_end = i;
                have_range = true;
            }
        });
        if have_range {
            append_range(&mut buf, range_start, range_end);
        }
        buf.push(')');
        f.write_str(&buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // Deterministic PRNG standing in for Go's math/rand (the Go tests only
    // need internal consistency, not a fixed sequence).
    struct Lcg(u64);
    impl Lcg {
        fn new(seed: u64) -> Lcg {
            Lcg(seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407))
        }
        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 11
        }
        fn intn(&mut self, n: i64) -> i64 {
            (self.next_u64() % n as u64) as i64
        }
        fn perm(&mut self, n: i64) -> Vec<i64> {
            let mut v: Vec<i64> = (0..n).collect();
            for i in (1..n as usize).rev() {
                let j = (self.next_u64() % (i as u64 + 1)) as usize;
                v.swap(i, j);
            }
            v
        }
    }

    #[test]
    fn basic() {
        let mut fis = FastIntSet::default();
        fis.insert(1);
        fis.insert(2);
        fis.insert(3);
        assert_eq!(fis.len(), 3);
        assert!(fis.has(1) && fis.has(2) && fis.has(3));
        fis.remove(2);
        assert_eq!(fis.len(), 2);
        assert!(fis.has(1) && fis.has(3));
        fis.remove(3);
        assert_eq!(fis.len(), 1);
        assert!(fis.has(1));
        fis.remove(1);
        assert_eq!(fis.len(), 0);

        // Next only seeks non-negative values.
        fis.insert(6);
        fis.insert(3);
        fis.insert(0);
        fis.insert(-1);
        fis.insert(77);
        let (mut n, mut ok) = fis.next(MIN_INT);
        assert!(ok && n == 0);
        let r = fis.next(n + 1);
        (n, ok) = r;
        assert!(ok && n == 3);
        let r = fis.next(n + 1);
        (n, ok) = r;
        assert!(ok && n == 6);
        let r = fis.next(n + 1);
        (n, ok) = r;
        assert!(ok && n == 77);
        let r = fis.next(n + 1);
        (n, ok) = r;
        assert!(!ok && n == MAX_INT);

        fis.clear();
        assert_eq!(fis.len(), 0);
        assert!(fis.is_empty());

        fis.insert(1);
        fis.insert(-1);
        fis.insert(77);
        let mut res = Vec::new();
        fis.for_each(|i| res.push(i));
        let res1 = fis.sorted_array();
        assert_eq!(res.len(), 3);
        assert_eq!(res, res1);

        let cp = fis.copy();
        assert_eq!(fis.len(), cp.len());
        assert_eq!(fis.sorted_array(), cp.sorted_array());
        assert!(fis.equals(&cp));

        let mut cpf = FastIntSet::default();
        cpf.insert(100);
        cpf.copy_from(&fis);
        assert_eq!(cpf.len(), cp.len());
        assert_eq!(cpf.sorted_array(), cp.sorted_array());
        assert!(cpf.equals(&cp));
    }

    // Go TestFastIntSet: randomized insert/remove against a reference array,
    // cross-checking Has/IsEmpty/ForEach/Next/SortedArray/Copy/CopyFrom.
    #[test]
    fn randomized() {
        for m in [1i64, 8, 30, 64, 128, 256] {
            let mut rng = Lcg::new(m as u64 + 1);
            let mut inref = vec![false; m as usize];
            let mut fe_res = vec![false; m as usize];
            let mut s = FastIntSet::default();
            for _ in 0..1000 {
                let v = rng.intn(m);
                if rng.intn(2) == 0 {
                    inref[v as usize] = true;
                    s.insert(v);
                } else {
                    inref[v as usize] = false;
                    s.remove(v);
                }
                let mut empty = true;
                for j in 0..m {
                    empty = empty && !inref[j as usize];
                    assert_eq!(inref[j as usize], s.has(j), "Has({j})");
                }
                assert_eq!(empty, s.is_empty());

                for r in fe_res.iter_mut() {
                    *r = false;
                }
                s.for_each(|j| fe_res[j as usize] = true);
                for j in 0..m {
                    assert_eq!(inref[j as usize], fe_res[j as usize], "ForEach {j}");
                }

                let mut vals = Vec::new();
                let (mut i, mut ok) = s.next(0);
                while ok {
                    vals.push(i);
                    let n = s.next(i + 1);
                    i = n.0;
                    ok = n.1;
                }
                assert_eq!(vals, s.sorted_array());

                let assert_same = |orig: &FastIntSet, copied: &mut FastIntSet| {
                    assert!(orig.equals(copied) && copied.equals(orig));
                    if let (col, true) = copied.next(0) {
                        copied.remove(col);
                        assert!(!orig.equals(copied) && !copied.equals(orig));
                        copied.insert(col);
                        assert!(orig.equals(copied) && copied.equals(orig));
                    }
                };
                let mut s2 = s.copy();
                assert_same(&s, &mut s2);
                let mut s3 = FastIntSet::default();
                s3.copy_from(&s);
                assert_same(&s, &mut s3);
                // Go's Shift has a pointer receiver but does not mutate the
                // set (it builds and returns a new one); the result is
                // discarded here, so `s` is unchanged.
                let _ = s.shift(100);
                s.copy_from(&s3);
                let mut s3b = s3.copy();
                assert_same(&s, &mut s3b);
            }
        }
    }

    // Go TestFastIntSetTwoSetOps: union/intersection/difference/subset/shift
    // cross-checked against reference maps.
    #[test]
    fn two_set_ops() {
        let mut rng = Lcg::new(7);
        let gen_set = |rng: &mut Lcg,
                       num_elem: i64,
                       num_removed: i64,
                       min_val: i64,
                       val_range: i64|
         -> (FastIntSet, HashMap<i64, bool>) {
            let mut s = FastIntSet::default();
            let total = (num_elem + num_removed).max(0);
            let perm = rng.perm(val_range.max(total).max(1));
            let vals: Vec<i64> = perm
                .into_iter()
                .take(total as usize)
                .map(|x| x + min_val)
                .collect();
            let mut used: HashMap<i64, bool> = HashMap::new();
            for &i in &vals {
                used.insert(i, true);
                s.insert(i);
            }
            let p = rng.perm(vals.len().max(1) as i64);
            for i in 0..num_removed as usize {
                if i < vals.len() {
                    let k = vals[p[i] as usize];
                    s.remove(k);
                    used.remove(&k);
                }
            }
            (s, used)
        };
        let subset = |a: &HashMap<i64, bool>, b: &HashMap<i64, bool>| -> bool {
            a.keys().all(|k| b.contains_key(k))
        };

        for min_val in [-10i64, -1, 0, 64, 128] {
            for val_range in [0i64, 20, 200] {
                for num1 in [0i64, 1, 5, 10, 20] {
                    for removed1 in [0i64, 1, 3, 8] {
                        let (s1, m1) = gen_set(
                            &mut rng,
                            num1,
                            removed1,
                            min_val,
                            num1 + removed1 + val_range,
                        );
                        for shift in [-100i64, -10, -1, 1, 2, 10, 100] {
                            let shifted = s1.shift(shift);
                            let mut failed = false;
                            s1.for_each(|i| failed = failed || !shifted.has(i + shift));
                            shifted.for_each(|i| failed = failed || !s1.has(i - shift));
                            assert!(!failed, "shift {shift} of {s1}");
                        }
                        for num2 in [0i64, 1, 5, 10, 20] {
                            for removed2 in [0i64, 1, 4, 10] {
                                let (s2, m2) = gen_set(
                                    &mut rng,
                                    num2,
                                    removed2,
                                    min_val,
                                    num2 + removed2 + val_range,
                                );
                                let sub1 = subset(&m1, &m2);
                                assert_eq!(sub1, s1.subset_of(&s2));
                                let sub2 = subset(&m2, &m1);
                                assert_eq!(sub2, s2.subset_of(&s1));
                                let eq = sub1 && sub2;
                                assert_eq!(eq, s1.equals(&s2));
                                assert_eq!(eq, s2.equals(&s1));

                                let mut u = s1.copy();
                                u.union_with(&s2);
                                assert!(u.equals(&s1.union(&s2)));
                                for m in [&m1, &m2] {
                                    for x in m.keys() {
                                        assert!(u.has(*x));
                                    }
                                }

                                let mut u = s1.copy();
                                u.intersection_with(&s2);
                                assert_eq!(s1.intersects(&s2), !u.is_empty());
                                assert_eq!(s2.intersects(&s1), !u.is_empty());
                                assert!(u.equals(&s1.intersection(&s2)));
                                for x in m1.keys() {
                                    if m2.contains_key(x) {
                                        assert!(u.has(*x));
                                    }
                                }

                                let mut u = s1.copy();
                                u.difference_with(&s2);
                                assert!(u.equals(&s1.difference(&s2)));
                                for x in m1.keys() {
                                    if !m2.contains_key(x) {
                                        assert!(u.has(*x));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Go TestFastIntSetAddRange.
    #[test]
    fn add_range() {
        let maxv = 64 + 20;
        for from in -5..=maxv {
            for to in from..=maxv {
                let mut set = FastIntSet::default();
                set.add_range(from, to);
                let mut expected = from;
                set.for_each(|actual| {
                    assert!(actual <= to);
                    assert_eq!(expected, actual);
                    expected += 1;
                });
                assert_eq!(expected, to + 1);
            }
        }
    }

    // Go TestGetSmallUInt64.
    #[test]
    fn get_small_uint64() {
        assert_eq!(FastIntSet::default().get_small_uint64().unwrap(), 0);
        assert_eq!(
            FastIntSet::new(&[0, 1, 3]).get_small_uint64().unwrap(),
            0b1011
        );
        assert_eq!(
            FastIntSet::new(&[0, 1, 2, 3, 4, 5])
                .get_small_uint64()
                .unwrap(),
            63
        );
        assert!(FastIntSet::new(&[64]).get_small_uint64().is_err());
        assert!(FastIntSet::new(&[1, 64]).get_small_uint64().is_err());
    }

    // Go TestFastIntSetString.
    #[test]
    fn string_format() {
        assert_eq!(FastIntSet::new(&[]).to_string(), "()");
        assert_eq!(
            FastIntSet::new(&[-5, -3, -2, -1, 0, 1, 2, 3, 4, 5]).to_string(),
            "(-5,-3,-2,-1,0-5)"
        );
        assert_eq!(FastIntSet::new(&[0, 1, 3, 4, 5]).to_string(), "(0,1,3-5)");
    }
}
