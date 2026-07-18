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

//! Dependency-closed expected-count range conversion from
//! `pkg/planner/cardinality/cross_estimation.go`.
//!
//! The source helper chooses the range prefix that must be scanned before an
//! ordered scan finds a requested number of rows. It is pure arithmetic over
//! already-built ranger ranges and their estimates; session, statistics,
//! expression, and Datum owners remain outside this leaf. Endpoint identities
//! are opaque caller tokens, while the two unbounded sentinels represent the
//! source's zero-Datum and `MaxValueDatum` boundaries.

/// An endpoint identity supplied by the eventual typed/ranger owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeEndpoint {
    /// The source's lower scan sentinel (`types.Datum{}`).
    UnboundedLow,
    /// The source's upper scan sentinel (`types.MaxValueDatum()`).
    UnboundedHigh,
    /// An opaque already-typed endpoint; no Datum comparison occurs here.
    Opaque(u64),
}

/// A normalized single range with endpoint inclusivity and collator metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScanRange {
    low: RangeEndpoint,
    high: RangeEndpoint,
    low_exclude: bool,
    high_exclude: bool,
    collator: Option<u64>,
}

impl ScanRange {
    /// Creates a normalized range without evaluating its opaque endpoints.
    #[must_use]
    pub const fn new(
        low: RangeEndpoint,
        high: RangeEndpoint,
        low_exclude: bool,
        high_exclude: bool,
        collator: Option<u64>,
    ) -> Self {
        Self {
            low,
            high,
            low_exclude,
            high_exclude,
            collator,
        }
    }

    /// Returns the low endpoint identity.
    #[must_use]
    pub const fn low(&self) -> RangeEndpoint {
        self.low
    }

    /// Returns the high endpoint identity.
    #[must_use]
    pub const fn high(&self) -> RangeEndpoint {
        self.high
    }

    /// Returns whether the low endpoint is excluded.
    #[must_use]
    pub const fn low_exclude(&self) -> bool {
        self.low_exclude
    }

    /// Returns whether the high endpoint is excluded.
    #[must_use]
    pub const fn high_exclude(&self) -> bool {
        self.high_exclude
    }

    /// Returns the caller-owned collator identity.
    #[must_use]
    pub const fn collator(&self) -> Option<u64> {
        self.collator
    }
}

/// A source range paired with its already-computed row estimate.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CountedRange {
    range: ScanRange,
    estimated_rows: f64,
}

impl CountedRange {
    /// Pairs a normalized range with its source row-count estimate.
    #[must_use]
    pub const fn new(range: ScanRange, estimated_rows: f64) -> Self {
        Self {
            range,
            estimated_rows,
        }
    }

    /// Returns the normalized range.
    #[must_use]
    pub const fn range(&self) -> ScanRange {
        self.range
    }

    /// Returns the source estimate used for cumulative selection.
    #[must_use]
    pub const fn estimated_rows(&self) -> f64 {
        self.estimated_rows
    }
}

/// Result of the source expected-count conversion.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExpectedCountConversion {
    converted_range: Option<ScanRange>,
    skipped_rows: f64,
    full_scan: bool,
}

impl ExpectedCountConversion {
    /// Returns the one converted range, or `None` when all input ranges are
    /// needed to satisfy the expected count.
    #[must_use]
    pub const fn converted_range(&self) -> Option<ScanRange> {
        self.converted_range
    }

    /// Returns the cumulative estimate of ranges skipped before the selected
    /// range. The source returns zero when the full scan path is selected.
    #[must_use]
    pub const fn skipped_rows(&self) -> f64 {
        self.skipped_rows
    }

    /// Returns whether the source selected the full input range set.
    #[must_use]
    pub const fn is_full_scan(&self) -> bool {
        self.full_scan
    }
}

/// Converts ordered ranges into the prefix needed to find `expected_count`.
///
/// This mirrors `convertRangeFromExpectedCnt`: ascending scans retain the
/// lower prefix `[unbounded-low, selected.low]`, descending scans retain the
/// upper prefix `[selected.high, unbounded-high]`, and the selected endpoint
/// exclusion is inverted exactly as in the Go source.
#[must_use]
pub fn convert_range_from_expected_cnt(
    ranges: &[CountedRange],
    expected_count: f64,
    descending: bool,
) -> ExpectedCountConversion {
    let mut skipped_rows = 0.0;
    let selected_index = if descending {
        ranges.iter().rposition(|range| {
            if skipped_rows + range.estimated_rows >= expected_count {
                true
            } else {
                skipped_rows += range.estimated_rows;
                false
            }
        })
    } else {
        ranges.iter().position(|range| {
            if skipped_rows + range.estimated_rows >= expected_count {
                true
            } else {
                skipped_rows += range.estimated_rows;
                false
            }
        })
    };

    let Some(index) = selected_index else {
        return ExpectedCountConversion {
            converted_range: None,
            skipped_rows: 0.0,
            full_scan: true,
        };
    };

    let source = ranges[index].range;
    let converted_range = if descending {
        ScanRange::new(
            source.high,
            RangeEndpoint::UnboundedHigh,
            !source.high_exclude,
            false,
            source.collator,
        )
    } else {
        ScanRange::new(
            RangeEndpoint::UnboundedLow,
            source.low,
            false,
            !source.low_exclude,
            source.collator,
        )
    };

    ExpectedCountConversion {
        converted_range: Some(converted_range),
        skipped_rows,
        full_scan: false,
    }
}
