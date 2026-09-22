// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go util/point_response_stats.go: values and response-level coverage.

use crate::proto::kvrpcpb::ScanDetailV2;

/// Storage work reported by point-read responses, with Go's signed counters.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PointReadScanDetail {
    pub total_keys: i64,
    pub processed_keys: i64,
    pub processed_keys_size: i64,
}

/// A value snapshot of Get/BatchGet/BufferBatchGet data and coverage. Zero is
/// valid but has no response coverage. Callers synchronize updates themselves.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PointResponseStats {
    pub scan_detail: PointReadScanDetail,
    /// Logical successful payload, excluding protocol, errors and commit TS.
    pub payload_bytes: u64,
    seen_response: bool,
    missing_scan_detail: bool,
    invalid: bool,
}

impl PointResponseStats {
    pub fn is_valid(&self) -> bool {
        !self.invalid
    }

    /// Every recognized response carried ScanDetailV2, including zero details.
    pub fn scan_detail_complete(&self) -> bool {
        self.is_valid() && self.seen_response && !self.missing_scan_detail
    }

    /// At least one recognized response had its logical payload accounted for.
    pub fn payload_complete(&self) -> bool {
        self.is_valid() && self.seen_response
    }

    /// Invalidity is sticky; subsequent records and merges cannot undo it.
    pub fn invalidate(&mut self) {
        self.invalid = true;
    }

    /// Record after handling transport/region errors, including key errors and
    /// misses. Every physical retry is a separate recognized response.
    pub fn record_response(&mut self, scan_detail: Option<&ScanDetailV2>, payload_bytes: u64) {
        self.merge(Self {
            scan_detail: scan_detail.map_or_else(PointReadScanDetail::default, |detail| {
                PointReadScanDetail {
                    total_keys: detail.total_versions as i64,
                    processed_keys: detail.processed_versions as i64,
                    processed_keys_size: detail.processed_versions_size as i64,
                }
            }),
            payload_bytes,
            seen_response: true,
            missing_scan_detail: scan_detail.is_none(),
            invalid: false,
        });
    }

    /// Invalid input marks this value invalid without changing its totals.
    pub fn merge(&mut self, other: Self) {
        if !self.is_valid() || !other.is_valid() {
            self.invalidate();
            return;
        }
        self.scan_detail.total_keys = self
            .scan_detail
            .total_keys
            .wrapping_add(other.scan_detail.total_keys);
        self.scan_detail.processed_keys = self
            .scan_detail
            .processed_keys
            .wrapping_add(other.scan_detail.processed_keys);
        self.scan_detail.processed_keys_size = self
            .scan_detail
            .processed_keys_size
            .wrapping_add(other.scan_detail.processed_keys_size);
        self.payload_bytes = self.payload_bytes.wrapping_add(other.payload_bytes);
        self.seen_response |= other.seen_response;
        self.missing_scan_detail |= other.missing_scan_detail;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_response_coverage_distinguishes_empty_missing_and_zero_details() {
        let mut stats = PointResponseStats::default();
        assert!(stats.is_valid());
        assert!(!stats.scan_detail_complete());
        assert!(!stats.payload_complete());
        stats.record_response(Some(&ScanDetailV2::default()), 0);
        assert!(stats.scan_detail_complete());
        assert!(stats.payload_complete());
        stats.record_response(
            Some(&ScanDetailV2 {
                total_versions: 11,
                processed_versions: 7,
                processed_versions_size: 70,
                ..Default::default()
            }),
            13,
        );
        let mut independent = stats;
        independent.scan_detail.total_keys = 1000;
        assert_eq!(independent.scan_detail.total_keys, 1000);
        assert_eq!(stats.scan_detail.total_keys, 11);
        stats.record_response(None, 7);
        stats.merge(PointResponseStats::default());
        assert!(stats.is_valid());
        assert!(!stats.scan_detail_complete());
        assert!(stats.payload_complete());
        assert_eq!(stats.payload_bytes, 20);
        assert_eq!(
            stats.scan_detail,
            PointReadScanDetail {
                total_keys: 11,
                processed_keys: 7,
                processed_keys_size: 70,
            }
        );
    }

    #[test]
    fn point_response_merge_preserves_values_and_sticky_invalidity() {
        let mut complete = PointResponseStats::default();
        complete.record_response(
            Some(&ScanDetailV2 {
                total_versions: 5,
                processed_versions: 3,
                processed_versions_size: 30,
                ..Default::default()
            }),
            7,
        );
        let mut missing = PointResponseStats::default();
        missing.record_response(None, 11);
        for (mut stats, other) in [(complete, missing), (missing, complete)] {
            stats.merge(other);
            assert_eq!(stats.payload_bytes, 18);
            assert_eq!(stats.scan_detail, complete.scan_detail);
            assert!(!stats.scan_detail_complete());
            assert!(stats.payload_complete());
        }
        let mut invalid = missing;
        invalid.invalidate();
        for (mut stats, other) in [(complete, invalid), (invalid, complete)] {
            let before = stats;
            stats.merge(other);
            assert!(!stats.is_valid());
            assert!(!stats.payload_complete());
            assert!(!stats.scan_detail_complete());
            assert_eq!(stats.scan_detail, before.scan_detail);
            assert_eq!(stats.payload_bytes, before.payload_bytes);
            stats.record_response(Some(&ScanDetailV2::default()), 999);
            assert!(!stats.is_valid());
            assert_eq!(stats.payload_bytes, before.payload_bytes);
        }
    }

    #[test]
    fn point_response_counters_wrap_like_go_in_debug_builds() {
        let mut stats = PointResponseStats::default();
        stats.record_response(
            Some(&ScanDetailV2 {
                total_versions: u64::MAX,
                processed_versions: i64::MAX as u64,
                processed_versions_size: u64::MAX,
                ..Default::default()
            }),
            u64::MAX,
        );
        stats.record_response(
            Some(&ScanDetailV2 {
                total_versions: 2,
                processed_versions: 1,
                processed_versions_size: 1,
                ..Default::default()
            }),
            2,
        );
        assert_eq!(
            stats.scan_detail,
            PointReadScanDetail {
                total_keys: 1,
                processed_keys: i64::MIN,
                processed_keys_size: 0,
            }
        );
        assert_eq!(stats.payload_bytes, 1);
    }

    #[test]
    fn point_response_merge_matches_all_original_state_pairs() {
        let empty = PointResponseStats::default();
        let mut complete = empty;
        complete.record_response(
            Some(&ScanDetailV2 {
                total_versions: 2,
                ..Default::default()
            }),
            3,
        );
        let mut missing = empty;
        missing.record_response(None, 5);
        let mut invalid = empty;
        invalid.invalidate();
        let states = [
            (empty, true, false, false),
            (complete, true, true, false),
            (missing, true, true, true),
            (invalid, false, false, false),
        ];
        for (left, left_valid, left_seen, left_missing) in states {
            for (right, right_valid, right_seen, right_missing) in states {
                let mut stats = left;
                stats.merge(right);
                let valid = left_valid && right_valid;
                assert_eq!(stats.is_valid(), valid);
                assert_eq!(stats.payload_complete(), valid && (left_seen || right_seen));
                assert_eq!(
                    stats.scan_detail_complete(),
                    valid && (left_seen || right_seen) && !(left_missing || right_missing)
                );
                if valid {
                    assert_eq!(
                        stats.scan_detail.total_keys,
                        left.scan_detail.total_keys + right.scan_detail.total_keys
                    );
                    assert_eq!(
                        stats.payload_bytes,
                        left.payload_bytes + right.payload_bytes
                    );
                } else {
                    assert_eq!(stats.scan_detail, left.scan_detail);
                    assert_eq!(stats.payload_bytes, left.payload_bytes);
                }
            }
        }
        let mut copy = complete;
        copy.merge(empty);
        assert_eq!(copy, complete);
        copy.merge(copy);
        assert_eq!(copy.scan_detail.total_keys, 4);
        assert_eq!(copy.payload_bytes, 6);
        assert_eq!(complete.scan_detail.total_keys, 2);
    }
}
