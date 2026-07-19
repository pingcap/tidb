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

//! Physical key encoding for normalized signed integer handle ranges.
//!
//! Logical comparison/range construction belongs to `tidb-planner`. This
//! module is the dependency-clean translation of Go
//! `pkg/distsql/request_builder.go::encodeHandleKey`: it only converts a valid
//! logical range into TiDB's half-open table-record key interval.

use tidb_codec::{encode_int, encode_row_key};
use tidb_txnkv::Key;

use crate::{KvRequestBuildError, RequestKeyRange};

/// One non-empty normalized range over a signed clustered integer handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SignedHandleRange {
    low: i64,
    high: i64,
    low_exclude: bool,
    high_exclude: bool,
}

impl SignedHandleRange {
    /// Constructs a non-empty source range.
    pub fn new(
        low: i64,
        high: i64,
        low_exclude: bool,
        high_exclude: bool,
    ) -> Result<Self, KvRequestBuildError> {
        if low > high || (low == high && (low_exclude || high_exclude)) {
            return Err(KvRequestBuildError::RangeEncoding);
        }
        Ok(Self {
            low,
            high,
            low_exclude,
            high_exclude,
        })
    }

    /// Constructs the common inclusive source range.
    pub fn inclusive(low: i64, high: i64) -> Result<Self, KvRequestBuildError> {
        Self::new(low, high, false, false)
    }

    /// Returns the inclusive source low value before exclusion adjustment.
    #[must_use]
    pub const fn low(self) -> i64 {
        self.low
    }

    /// Returns the inclusive source high value before exclusion adjustment.
    #[must_use]
    pub const fn high(self) -> i64 {
        self.high
    }

    /// Returns whether the source low value is excluded.
    #[must_use]
    pub const fn low_exclude(self) -> bool {
        self.low_exclude
    }

    /// Returns whether the source high value is excluded.
    #[must_use]
    pub const fn high_exclude(self) -> bool {
        self.high_exclude
    }
}

/// Encodes normalized signed handle ranges as ordered half-open record keys.
#[must_use]
pub fn signed_handle_ranges_to_kv_ranges(
    table_id: i64,
    ranges: &[SignedHandleRange],
) -> Vec<RequestKeyRange> {
    ranges
        .iter()
        .map(|range| {
            let mut low = Vec::with_capacity(8);
            encode_int(&mut low, range.low);
            if range.low_exclude {
                low = Key::from_bytes(low).prefix_next().into_bytes();
            }

            let mut high = Vec::with_capacity(8);
            encode_int(&mut high, range.high);
            if !range.high_exclude {
                high = Key::from_bytes(high).prefix_next().into_bytes();
            }

            RequestKeyRange {
                start_key: encode_row_key(table_id, &low),
                end_key: encode_row_key(table_id, &high),
            }
        })
        .collect()
}
