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

//! Callable region-location coverage validation used by child-task construction.

use crate::{RegionTaskEnvelope, RequestKeyRange};

/// Region-cache location boundaries needed before constructing child tasks.
///
/// This is the dependency-closed subset of client-go's `tikv.KeyLocation`
/// consumed by `validateLocationCoverage`. Region lookup, cache lifetime, PD
/// access, and retry state deliberately remain outside this value.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RegionTaskLocation {
    /// Region identifier retained for diagnostics and eventual task metadata.
    pub region_id: u64,
    /// Inclusive region boundary. Empty means the beginning of keyspace.
    pub start_key: Vec<u8>,
    /// Exclusive region boundary. Empty means the end of keyspace.
    pub end_key: Vec<u8>,
}

impl RegionTaskEnvelope {
    /// Validates region-location coverage for a future task builder.
    ///
    /// This is a direct translation of
    /// `pkg/store/copr/region_cache.go::validateLocationCoverage`. It checks
    /// the same three properties: locations are strictly ordered and do not
    /// overlap, every request range is covered without a gap, and every
    /// non-null location participates in at least one request range. The task
    /// construction path now enforces the equivalent coverage invariant over
    /// its normalized region/bucket topology before producing wire envelopes.
    #[must_use]
    pub fn locations_cover_ranges(&self, locations: &[Option<RegionTaskLocation>]) -> bool {
        validate_location_coverage(&self.ranges, locations)
    }
}

fn compare_key_range_boundary(a: &[u8], b: &[u8], a_is_start: bool, b_is_start: bool) -> i8 {
    match (a.is_empty() && !a_is_start, b.is_empty() && !b_is_start) {
        (true, true) => 0,
        (true, false) => 1,
        (false, true) => -1,
        (false, false) => match a.cmp(b) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        },
    }
}

fn location_contains_start_key(location: &RegionTaskLocation, key: &[u8]) -> bool {
    compare_key_range_boundary(key, &location.start_key, true, true) >= 0
        && (location.end_key.is_empty() || key < location.end_key.as_slice())
}

fn location_covers_end_key(location: &RegionTaskLocation, end_key: &[u8]) -> bool {
    if !end_key.is_empty()
        && !location.start_key.is_empty()
        && end_key <= location.start_key.as_slice()
    {
        return false;
    }
    if end_key.is_empty() {
        return location.end_key.is_empty();
    }
    location.end_key.is_empty() || end_key <= location.end_key.as_slice()
}

fn locations_are_ordered(locations: &[Option<RegionTaskLocation>]) -> bool {
    let mut valid = true;
    for (index, current) in locations.iter().enumerate() {
        let Some(current) = current else {
            valid = false;
            continue;
        };
        if index == 0 {
            continue;
        }
        let Some(previous) = &locations[index - 1] else {
            continue;
        };
        if compare_key_range_boundary(&previous.start_key, &current.start_key, true, true) >= 0 {
            valid = false;
        }
        if compare_key_range_boundary(&previous.end_key, &current.start_key, false, true) > 0 {
            valid = false;
        }
    }
    valid
}

fn validate_location_coverage(
    ranges: &[RequestKeyRange],
    locations: &[Option<RegionTaskLocation>],
) -> bool {
    if ranges.is_empty() {
        return locations.is_empty();
    }
    if locations.is_empty() {
        return false;
    }

    let mut valid = locations_are_ordered(locations);
    let mut location_used = vec![false; locations.len()];
    let mut location_index = 0;

    for range in ranges {
        while location_index < locations.len() {
            let Some(location) = &locations[location_index] else {
                location_index += 1;
                continue;
            };
            if !location.end_key.is_empty()
                && compare_key_range_boundary(&location.end_key, &range.start_key, false, true) <= 0
            {
                location_index += 1;
                continue;
            }
            break;
        }

        if location_index >= locations.len()
            || locations[location_index]
                .as_ref()
                .is_none_or(|location| !location_contains_start_key(location, &range.start_key))
        {
            valid = false;
            continue;
        }

        let mut cover_index = location_index;
        location_used[cover_index] = true;
        while !location_covers_end_key(
            locations[cover_index]
                .as_ref()
                .expect("covered location cannot be null"),
            &range.end_key,
        ) {
            let previous_end = locations[cover_index]
                .as_ref()
                .expect("covered location cannot be null")
                .end_key
                .as_slice();
            let Some(next_index) =
                ((cover_index + 1)..locations.len()).find(|index| locations[*index].is_some())
            else {
                valid = false;
                break;
            };

            let next = locations[next_index]
                .as_ref()
                .expect("searched for a non-null location");
            if previous_end != next.start_key.as_slice() {
                valid = false;
                break;
            }
            cover_index = next_index;
            location_used[cover_index] = true;
        }
    }

    for (index, used) in location_used.into_iter().enumerate() {
        if !used && locations[index].is_some() {
            valid = false;
        }
    }
    valid
}
