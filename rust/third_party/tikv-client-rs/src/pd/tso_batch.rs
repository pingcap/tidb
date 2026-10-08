// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go clients/tso response allocation shared by asynchronous and synchronous callers.
//! Header fields are not part of Go's TSO response validation. Rust additionally
//! rejects malformed ranges before signed arithmetic or timestamp composition.

use crate::proto::pdpb;
use std::sync::{Arc, Mutex};

const PHYSICAL_SHIFT_BITS: u32 = 18;
const MAX_LOGICAL: i64 = (1_i64 << PHYSICAL_SHIFT_BITS) - 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchError {
    pub kind: &'static str,
    pub message: String,
}

fn invalid(kind: &'static str, message: impl Into<String>) -> BatchError {
    BatchError {
        kind,
        message: message.into(),
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TimestampParts {
    pub physical: i64,
    pub logical: i64,
}

impl TimestampParts {
    pub fn compose(self) -> Result<u64, BatchError> {
        if self.physical < 0 || self.physical as u64 > (u64::MAX >> PHYSICAL_SHIFT_BITS) {
            return Err(invalid(
                "tso_overflow",
                "TSO physical time does not fit the timestamp layout",
            ));
        }
        if !(0..=MAX_LOGICAL).contains(&self.logical) {
            return Err(invalid(
                "invalid_tso_logical",
                "TSO logical time does not fit the timestamp layout",
            ));
        }
        let timestamp = ((self.physical as u64) << PHYSICAL_SHIFT_BITS) + self.logical as u64;
        if timestamp == 0 {
            return Err(invalid("zero_tso", "PD Tso composed to zero"));
        }
        Ok(timestamp)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TsoBatch {
    physical: i64,
    first_logical: i64,
    suffix_bits: u32,
    count: u32,
}

impl TsoBatch {
    pub fn from_response(
        response: &pdpb::TsoResponse,
        expected_count: u32,
    ) -> Result<Self, BatchError> {
        if response.count != expected_count || expected_count == 0 {
            return Err(invalid(
                "tso_count_mismatch",
                format!(
                    "PD Tso returned count {}, expected {expected_count}",
                    response.count
                ),
            ));
        }
        let timestamp = response
            .timestamp
            .as_ref()
            .ok_or_else(|| invalid("missing_tso_timestamp", "PD Tso omitted its timestamp"))?;
        if timestamp.physical < 0 {
            return Err(invalid(
                "negative_tso_physical",
                "PD Tso returned negative physical time",
            ));
        }
        if !(0..=MAX_LOGICAL).contains(&timestamp.logical) {
            return Err(invalid(
                "invalid_tso_logical",
                "PD Tso logical time is outside its 18-bit field",
            ));
        }
        let first_logical = timestamp.logical - i64::from(expected_count) + 1;
        if first_logical < 0 {
            return Err(invalid(
                "invalid_tso_batch_range",
                "PD Tso batch extends below logical zero",
            ));
        }
        let batch = Self {
            physical: timestamp.physical,
            first_logical,
            suffix_bits: timestamp.suffix_bits,
            count: expected_count,
        };
        // Validate the whole batch before delivering any waiter. checked_shl
        // alone only checks the shift amount, not bits lost from physical.
        batch.split(0).compose()?;
        batch.last().compose()?;
        Ok(batch)
    }

    pub fn split(&self, index: u32) -> TimestampParts {
        assert!(index < self.count);
        TimestampParts {
            physical: self.physical,
            logical: self.first_logical + i64::from(index),
        }
    }

    pub fn timestamp(&self, index: u32) -> pdpb::Timestamp {
        let parts = self.split(index);
        pdpb::Timestamp {
            physical: parts.physical,
            logical: parts.logical,
            suffix_bits: self.suffix_bits,
        }
    }

    pub fn last(&self) -> TimestampParts {
        self.split(self.count - 1)
    }
}

/// Go dispatcher latestTSOInfo outlives individual streams. Capture a snapshot
/// before sending; overlapping RPCs may complete out of order, but each must
/// advance past all allocations completed before it began.
#[derive(Clone, Default)]
pub struct TimestampTracker(Arc<Mutex<Option<TimestampParts>>>);

impl TimestampTracker {
    pub fn snapshot(&self) -> Option<TimestampParts> {
        *self.0.lock().expect("TSO order poisoned")
    }

    pub fn accept(
        &self,
        batch: TsoBatch,
        before_request: Option<TimestampParts>,
    ) -> Result<(), BatchError> {
        if before_request.is_some_and(|previous| batch.split(0) <= previous) {
            return Err(invalid(
                "tso_fallback",
                "PD Tso batch is not after the previously allocated timestamp",
            ));
        }
        let mut latest = self.0.lock().expect("TSO order poisoned");
        if latest.is_none_or(|previous| batch.last() > previous) {
            *latest = Some(batch.last());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(physical: i64, logical: i64, count: u32, suffix_bits: u32) -> pdpb::TsoResponse {
        pdpb::TsoResponse {
            count,
            timestamp: Some(pdpb::Timestamp {
                physical,
                logical,
                suffix_bits,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn source_completion_splits_plain_logical_ranges_and_rejects_invalid_batches() {
        for suffix in [0, 1, 2, 4, 8] {
            let batch = TsoBatch::from_response(&response(100, 8, 3, suffix), 3).unwrap();
            assert_eq!(
                (0..3)
                    .map(|i| batch.timestamp(i).logical)
                    .collect::<Vec<_>>(),
                [6, 7, 8]
            );
            assert_eq!(batch.last().compose().unwrap(), (100 << 18) + 8);
        }
        for (p, l, count, expected, kind) in [
            (100, 1, 2, 1, "tso_count_mismatch"),
            (100, 0, 0, 0, "tso_count_mismatch"),
            (-1, 1, 1, 1, "negative_tso_physical"),
            (100, 1 << 18, 1, 1, "invalid_tso_logical"),
            (100, 0, 2, 2, "invalid_tso_batch_range"),
            (1 << 46, 1, 1, 1, "tso_overflow"),
            (0, 0, 1, 1, "zero_tso"),
        ] {
            assert_eq!(
                TsoBatch::from_response(&response(p, l, count, 0), expected)
                    .unwrap_err()
                    .kind,
                kind
            );
        }
    }

    #[test]
    fn source_completion_checks_pre_request_order_and_keeps_maximum_across_overlaps() {
        let tracker = TimestampTracker::default();
        let batch = |logical| TsoBatch::from_response(&response(100, logical, 2, 0), 2).unwrap();
        tracker.accept(batch(2), None).unwrap();
        let before = tracker.snapshot();
        tracker.accept(batch(8), before).unwrap();
        tracker.accept(batch(4), before).unwrap();
        assert_eq!(tracker.snapshot().unwrap().logical, 8);
        assert_eq!(
            tracker
                .accept(batch(9), tracker.snapshot())
                .unwrap_err()
                .kind,
            "tso_fallback"
        );
        tracker.accept(batch(10), tracker.snapshot()).unwrap();
    }
}
