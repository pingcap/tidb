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

//! Exact generated cases from
//! `pkg/store/copr/region_cache_test.go::TestValidateLocationCoverage`.

use tidb_distsql::{RegionTaskEnvelope, RegionTaskLocation, RequestKeyRange};

fn kr(start: &str, end: &str) -> RequestKeyRange {
    RequestKeyRange {
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
    }
}

fn kl(start: &str, end: &str, region_id: u64) -> Option<RegionTaskLocation> {
    Some(RegionTaskLocation {
        region_id,
        start_key: start.as_bytes().to_vec(),
        end_key: end.as_bytes().to_vec(),
    })
}

struct Case {
    name: &'static str,
    ranges: Vec<RequestKeyRange>,
    locations: Vec<Option<RegionTaskLocation>>,
    want_valid: bool,
}

#[test]
fn validate_location_coverage_preserves_every_original_generated_case() {
    let cases = vec![
        Case {
            name: "single range, single location - exact match",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "z", 1)],
            want_valid: true,
        },
        Case {
            name: "single range, single location - location covers more",
            ranges: vec![kr("b", "y")],
            locations: vec![kl("a", "z", 1)],
            want_valid: true,
        },
        Case {
            name: "single range split across two locations",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "m", 1), kl("m", "z", 2)],
            want_valid: true,
        },
        Case {
            name: "single range split across three locations",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "h", 1), kl("h", "p", 2), kl("p", "z", 3)],
            want_valid: true,
        },
        Case {
            name: "multiple ranges, single location covers all",
            ranges: vec![kr("b", "d"), kr("f", "h")],
            locations: vec![kl("a", "z", 1)],
            want_valid: true,
        },
        Case {
            name: "multiple ranges, multiple locations - aligned",
            ranges: vec![kr("a", "m"), kr("m", "z")],
            locations: vec![kl("a", "m", 1), kl("m", "z", 2)],
            want_valid: true,
        },
        Case {
            name: "multiple ranges, multiple locations - disjoint ranges don't require covering gaps",
            ranges: vec![kr("b", "d"), kr("f", "h")],
            locations: vec![kl("a", "d", 1), kl("f", "i", 2)],
            want_valid: true,
        },
        Case {
            name: "multiple ranges, multiple locations - overlapping ranges don't require monotonic loc scan",
            ranges: vec![kr("a", "z"), kr("b", "c")],
            locations: vec![kl("a", "m", 1), kl("m", "t", 2), kl("t", "z", 3)],
            want_valid: true,
        },
        Case {
            name: "empty start key - location also empty",
            ranges: vec![kr("", "m")],
            locations: vec![kl("", "m", 1)],
            want_valid: true,
        },
        Case {
            name: "empty start key - location NOT empty",
            ranges: vec![kr("", "m")],
            locations: vec![kl("a", "m", 1)],
            want_valid: false,
        },
        Case {
            name: "empty end key - location also empty",
            ranges: vec![kr("m", "")],
            locations: vec![kl("m", "", 1)],
            want_valid: true,
        },
        Case {
            name: "empty end key - location NOT empty",
            ranges: vec![kr("m", "")],
            locations: vec![kl("m", "z", 1)],
            want_valid: false,
        },
        Case {
            name: "range with empty end - location extends to infinity",
            ranges: vec![kr("m", "")],
            locations: vec![kl("a", "", 1)],
            want_valid: true,
        },
        Case {
            name: "location doesn't cover range start",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("b", "z", 1)],
            want_valid: false,
        },
        Case {
            name: "location doesn't cover range end",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "y", 1)],
            want_valid: false,
        },
        Case {
            name: "gap between locations",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "m", 1), kl("n", "z", 2)],
            want_valid: false,
        },
        Case {
            name: "discrete ranges with gap between locations - valid",
            ranges: vec![kr("a", "b"), kr("c", "d")],
            locations: vec![kl("a", "b", 1), kl("c", "d", 2)],
            want_valid: true,
        },
        Case {
            name: "discrete ranges in larger locations with gap - valid",
            ranges: vec![kr("a", "b"), kr("x", "z")],
            locations: vec![kl("a", "m", 1), kl("t", "z", 2)],
            want_valid: true,
        },
        Case {
            name: "missing range coverage",
            ranges: vec![kr("a", "m"), kr("m", "z")],
            locations: vec![kl("a", "m", 1)],
            want_valid: false,
        },
        Case {
            name: "empty ranges with locations",
            ranges: vec![],
            locations: vec![kl("a", "z", 1)],
            want_valid: false,
        },
        Case {
            name: "empty ranges without locations",
            ranges: vec![],
            locations: vec![],
            want_valid: true,
        },
        Case {
            name: "empty locations",
            ranges: vec![kr("a", "z")],
            locations: vec![],
            want_valid: false,
        },
        Case {
            name: "exact boundary match",
            ranges: vec![kr("a", "m"), kr("m", "z")],
            locations: vec![kl("a", "m", 1), kl("m", "z", 2)],
            want_valid: true,
        },
        Case {
            name: "location boundary equals range start",
            ranges: vec![kr("m", "z")],
            locations: vec![kl("m", "z", 1)],
            want_valid: true,
        },
        Case {
            name: "locations not monotonic",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("m", "z", 1), kl("a", "m", 2)],
            want_valid: false,
        },
        Case {
            name: "locations overlap",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "n", 1), kl("m", "z", 2)],
            want_valid: false,
        },
        Case {
            name: "location extends to infinity and overlaps next - invalid",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "", 1), kl("m", "z", 2)],
            want_valid: false,
        },
        Case {
            name: "extra location not covering any range",
            ranges: vec![kr("a", "b")],
            locations: vec![kl("a", "b", 1), kl("x", "z", 2)],
            want_valid: false,
        },
        Case {
            name: "middle location not covering any range",
            ranges: vec![kr("a", "c")],
            locations: vec![kl("a", "b", 1), kl("b", "c", 2), kl("x", "z", 3)],
            want_valid: false,
        },
        Case {
            name: "current location starts from beginning after non-beginning",
            ranges: vec![kr("a", "z")],
            locations: vec![kl("a", "m", 1), kl("", "z", 2)],
            want_valid: false,
        },
        Case {
            name: "valid: first location starts from beginning",
            ranges: vec![kr("", "z")],
            locations: vec![kl("", "m", 1), kl("m", "z", 2)],
            want_valid: true,
        },
        Case {
            name: "valid: last location extends to infinity",
            ranges: vec![kr("a", "")],
            locations: vec![kl("a", "m", 1), kl("m", "", 2)],
            want_valid: true,
        },
    ];

    assert_eq!(cases.len(), 32, "original generated case count changed");
    for case in cases {
        let task = RegionTaskEnvelope {
            ranges: case.ranges,
            ..Default::default()
        };
        assert_eq!(
            task.locations_cover_ranges(&case.locations),
            case.want_valid,
            "{}",
            case.name
        );
    }
}

#[test]
fn null_locations_are_invalid_even_when_non_null_neighbors_cover_the_range() {
    let task = RegionTaskEnvelope {
        ranges: vec![kr("a", "z")],
        ..Default::default()
    };
    let locations = vec![kl("a", "m", 1), None, kl("m", "z", 2)];
    assert!(!task.locations_cover_ranges(&locations));

    // Go's empty-range fast path compares slice lengths, so one nil location
    // is still an extraneous location rather than an empty location list.
    assert!(!RegionTaskEnvelope::default().locations_cover_ranges(&[None]));
}
