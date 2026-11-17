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


//! Go `br/pkg/utils/key.go`: the range clamping log backup uses.

use std::cmp::Ordering;

use crate::spans::sorted::Span;

/// Go `CompareBytesExt`: an empty key may stand for infinity.
#[must_use]
pub fn compare_bytes_ext(a: &[u8], a_empty_as_inf: bool, b: &[u8], b_empty_as_inf: bool) -> Ordering {
    crate::spans::compare_bytes_ext(a, a_empty_as_inf, b, b_empty_as_inf)
}

/// Go `failedToClampReason`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FailedToClampReason {
    SuccessClamp,
    /// The range is left of the clamp target.
    LeftNotOverlapped,
    /// The range is right of the clamp target.
    RightNotOverlapped,
    BuggyUnknown,
}

/// Go `clampInOneRange`: an empty result counts as a failure too.
fn clamp_in_one_range(rng: &Span, clamp_in: &Span) -> (Span, FailedToClampReason) {
    let mut rng = rng.clone();
    let mut possible_failure_reason = FailedToClampReason::BuggyUnknown;
    if compare_bytes_ext(&rng.start_key, false, &clamp_in.start_key, false) == Ordering::Less {
        rng.start_key.clone_from(&clamp_in.start_key);
        possible_failure_reason = FailedToClampReason::LeftNotOverlapped;
    }
    if compare_bytes_ext(&rng.end_key, true, &clamp_in.end_key, true) == Ordering::Greater {
        rng.end_key.clone_from(&clamp_in.end_key);
        possible_failure_reason = FailedToClampReason::RightNotOverlapped;
    }
    if compare_bytes_ext(&rng.start_key, false, &rng.end_key, true) != Ordering::Less {
        return (Span::default(), possible_failure_reason);
    }
    (rng, FailedToClampReason::SuccessClamp)
}

/// Go `IntersectAll`: the intersection of two sets of segments, each sorted
/// by start key without overlaps (as `spans::collapse` returns). Go mutates
/// `s1`; the slice is taken by value here.
#[must_use]
pub fn intersect_all(mut s1: Vec<Span>, s2: &[Span]) -> Vec<Span> {
    let mut current_clamping = 0;
    let mut current_clamp_target = 0;
    let mut rs = Vec::with_capacity(s1.len());
    while current_clamp_target < s2.len() && current_clamping < s1.len() {
        let cin = &s2[current_clamp_target];
        let crg = &s1[current_clamping];
        let (rng, result) = clamp_in_one_range(crg, cin);
        match result {
            FailedToClampReason::SuccessClamp => {
                rs.push(rng);
                if compare_bytes_ext(&crg.end_key, true, &cin.end_key, true) != Ordering::Greater {
                    current_clamping += 1;
                } else {
                    // The clamped range isn't fully consumed.
                    s1[current_clamping].start_key.clone_from(&cin.end_key);
                }
            }
            FailedToClampReason::LeftNotOverlapped => current_clamping += 1,
            FailedToClampReason::RightNotOverlapped => current_clamp_target += 1,
            FailedToClampReason::BuggyUnknown => {
                // Go `log.L().DPanic`: panics in development builds only.
                debug_assert!(false, "Unreachable path reached");
                tidb_log::error("Unreachable path reached", &[]);
                return rs;
            }
        }
    }
    rs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kr(s: &str, e: &str) -> Span {
        Span { start_key: s.as_bytes().to_vec(), end_key: e.as_bytes().to_vec() }
    }

    /// Go `TestClampKeyRanges`: both argument orders give the result.
    #[test]
    fn test_clamp_key_ranges() {
        let r = kr;
        let cases: Vec<(Vec<Span>, Vec<Span>, Vec<Span>)> = vec![
            (
                vec![r("0001", "0002"), r("0003", "0004"), r("0005", "0008")],
                vec![r("0001", "0004"), r("0006", "0008")],
                vec![r("0001", "0002"), r("0003", "0004"), r("0006", "0008")],
            ),
            (
                vec![r("0001", "0002"), r("00021", "0003"), r("0005", "0009")],
                vec![r("0001", "0004"), r("0005", "0008")],
                vec![r("0001", "0002"), r("00021", "0003"), r("0005", "0008")],
            ),
            (
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "0152")],
                vec![r("0001", "0100"), r("0150", "0200")],
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "0100"), r("0150", "0152")],
            ),
            (
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "0152")],
                vec![r("0001", "0100"), r("0150", "")],
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "0100"), r("0150", "0152")],
            ),
            (
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "")],
                vec![r("0001", "0100"), r("0150", "0200")],
                vec![r("0001", "0050"), r("0051", "0095"), r("0098", "0100"), r("0150", "0200")],
            ),
            (vec![r("", "0050")], vec![r("", "")], vec![r("", "0050")]),
        ];
        let sorted = |mut v: Vec<Span>| {
            v.sort_by(|a, b| a.start_key.cmp(&b.start_key));
            v
        };
        for (i, (ranges, clamp_in, result)) in cases.into_iter().enumerate() {
            assert_eq!(sorted(intersect_all(ranges.clone(), &clamp_in)), result, "#{i}");
            assert_eq!(sorted(intersect_all(clamp_in, &ranges)), result, "#{i} reversed");
        }
    }
}
