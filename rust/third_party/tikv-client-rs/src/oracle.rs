//! Timestamp-oracle contracts and timestamp conversion helpers.
//!
//! This module is the Rust counterpart of client-go's `oracle` package. Concrete
//! PD and local oracle implementations belong to the separate `oracle/oracles`
//! package boundary.

pub mod oracles;

use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use thiserror::Error;

const PHYSICAL_SHIFT_BITS: u32 = 18;
const LOGICAL_BITS: u64 = (1 << PHYSICAL_SHIFT_BITS) - 1;

/// The default transaction scope for an oracle service.
pub const GLOBAL_TXN_SCOPE: &str = "global";

/// Options applied to a timestamp-oracle operation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct OracleOption {
    /// Transaction scope used to select the timestamp stream.
    pub txn_scope: String,
}

/// A dynamically typed error returned by an oracle implementation.
pub type OracleError = Box<dyn Error + Send + Sync + 'static>;

/// A result returned by an oracle implementation.
pub type OracleResult<T> = std::result::Result<T, OracleError>;

/// A future that promises to return a timestamp.
#[async_trait]
pub trait TimestampFuture: Send + Sync {
    /// Wait for the promised timestamp.
    async fn wait(&self) -> OracleResult<u64>;
}

/// Validates whether a timestamp is safe to use for a read.
#[async_trait]
pub trait ReadTimestampValidator: Send + Sync {
    /// Check only timestamp-oracle safety; this does not check GC safepoints.
    async fn validate_read_timestamp(
        &self,
        read_timestamp: u64,
        is_stale_read: bool,
        option: &OracleOption,
    ) -> OracleResult<()>;
}

/// Provides strictly ascending timestamps.
///
/// Rust future cancellation replaces Go's `context.Context` cancellation: a
/// caller cancels an in-flight operation by dropping its returned future.
#[async_trait]
pub trait Oracle: ReadTimestampValidator + Send + Sync {
    /// Get a timestamp from the selected transaction scope.
    async fn get_timestamp(&self, option: &OracleOption) -> OracleResult<u64>;

    /// Start fetching a timestamp without waiting for it immediately.
    fn get_timestamp_async(&self, option: &OracleOption) -> Box<dyn TimestampFuture>;

    /// Get the most recently cached, low-resolution timestamp.
    async fn get_low_resolution_timestamp(&self, option: &OracleOption) -> OracleResult<u64>;

    /// Start fetching the cached, low-resolution timestamp.
    fn get_low_resolution_timestamp_async(&self, option: &OracleOption)
        -> Box<dyn TimestampFuture>;

    /// Set the interval used to refresh low-resolution timestamps.
    fn set_low_resolution_timestamp_update_interval(
        &self,
        update_interval: Duration,
    ) -> OracleResult<()>;

    /// Estimate a timestamp `previous_seconds` before the current time.
    ///
    /// The returned timestamp must be validated before it is used to read data.
    async fn get_stale_timestamp(
        &self,
        transaction_scope: &str,
        previous_seconds: u64,
    ) -> OracleResult<u64>;

    /// Return whether the lock's TTL has elapsed.
    fn is_expired(&self, lock_timestamp: u64, ttl: u64, option: &OracleOption) -> bool;

    /// Return the number of milliseconds until the lock expires.
    fn until_expired(&self, lock_timestamp: u64, ttl: u64, option: &OracleOption) -> i64;

    /// Close the oracle and release its resources.
    fn close(&self);

    /// Get the external timestamp.
    async fn get_external_timestamp(&self) -> OracleResult<u64>;

    /// Set the external timestamp.
    async fn set_external_timestamp(&self, timestamp: u64) -> OracleResult<()>;

    /// Get the minimum timestamp across all TSO keyspace groups.
    async fn get_all_tso_keyspace_group_min_timestamp(&self) -> OracleResult<u64>;
}

/// A read-timestamp validator that always succeeds.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopReadTimestampValidator;

#[async_trait]
impl ReadTimestampValidator for NoopReadTimestampValidator {
    async fn validate_read_timestamp(
        &self,
        _read_timestamp: u64,
        _is_stale_read: bool,
        _option: &OracleOption,
    ) -> OracleResult<()> {
        Ok(())
    }
}

/// A read timestamp that is later than the oracle's current timestamp.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error(
    "cannot set read timestamp to a future time, readTS: {read_timestamp}, currentTS: {current_timestamp}"
)]
pub struct FutureTimestampReadError {
    /// Timestamp requested by the reader.
    pub read_timestamp: u64,
    /// Latest timestamp confirmed by the oracle.
    pub current_timestamp: u64,
}

/// An attempt to use the latest-timestamp sentinel for a stale read.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
#[error("cannot set read ts to max uint64 for stale read")]
pub struct LatestStaleReadError;

/// Compose a timestamp from signed physical and logical parts.
///
/// The arithmetic wraps exactly like client-go's `int64` expression before the
/// result is converted to `uint64`.
pub fn compose_timestamp(physical: i64, logical: i64) -> u64 {
    physical
        .wrapping_shl(PHYSICAL_SHIFT_BITS)
        .wrapping_add(logical) as u64
}

/// Extract a timestamp's physical, millisecond component.
pub fn extract_physical(timestamp: u64) -> i64 {
    (timestamp >> PHYSICAL_SHIFT_BITS) as i64
}

/// Extract a timestamp's logical component.
pub fn extract_logical(timestamp: u64) -> i64 {
    (timestamp & LOGICAL_BITS) as i64
}

/// Return Unix time in milliseconds, truncating sub-millisecond precision.
pub fn get_physical(time: SystemTime) -> i64 {
    let nanoseconds = match time.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration_as_nanoseconds(duration),
        Err(error) => -duration_as_nanoseconds(error.duration()),
    };
    (nanoseconds / 1_000_000) as i64
}

/// Convert a timestamp's physical component to a system time.
pub fn get_time_from_timestamp(timestamp: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(extract_physical(timestamp) as u64)
}

/// Convert a system time to a timestamp with a zero logical component.
pub fn system_time_to_timestamp(time: SystemTime) -> u64 {
    get_physical(time).wrapping_shl(PHYSICAL_SHIFT_BITS) as u64
}

/// Return the minimum start timestamp for an uncommitted transaction.
///
/// `max_transaction_time_ms` is the maximum number of milliseconds from begin
/// to commit. A negative value follows Go duration behavior and moves the lower
/// limit forward.
pub fn system_time_to_lower_limit_start_timestamp(
    now: SystemTime,
    max_transaction_time_ms: i64,
) -> u64 {
    let offset = Duration::from_millis(max_transaction_time_ms.unsigned_abs());
    let lower_limit = if max_transaction_time_ms >= 0 {
        now.checked_sub(offset)
    } else {
        now.checked_add(offset)
    }
    .expect("timestamp is outside the range supported by SystemTime");
    system_time_to_timestamp(lower_limit)
}

fn duration_as_nanoseconds(duration: Duration) -> i128 {
    i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_interfaces_are_object_safe() {
        fn accepts_oracle(_: &dyn Oracle) {}
        fn accepts_future(_: &dyn TimestampFuture) {}

        let _ = accepts_oracle;
        let _ = accepts_future;
    }

    #[test]
    fn timestamp_parts_match_client_go() {
        let timestamp = compose_timestamp(1_701_234_567_890, 123_456);
        assert_eq!(extract_physical(timestamp), 1_701_234_567_890);
        assert_eq!(extract_logical(timestamp), 123_456);

        assert_eq!(compose_timestamp(0, -1), u64::MAX);
        assert_eq!(
            compose_timestamp(i64::MAX, 0),
            u64::MAX << PHYSICAL_SHIFT_BITS
        );
        assert_eq!(extract_physical(u64::MAX), (1_i64 << 46) - 1);
        assert_eq!(extract_logical(u64::MAX), (1_i64 << 18) - 1);
    }

    #[test]
    fn system_time_conversions_use_millisecond_precision() {
        let time = UNIX_EPOCH + Duration::new(1_701_234_567, 890_999_999);
        let timestamp = system_time_to_timestamp(time);
        assert_eq!(get_physical(time), 1_701_234_567_890);
        assert_eq!(extract_logical(timestamp), 0);
        assert_eq!(
            get_time_from_timestamp(timestamp),
            time - Duration::from_nanos(999_999)
        );

        assert_eq!(get_physical(UNIX_EPOCH - Duration::from_nanos(1)), 0);
        assert_eq!(
            get_physical(UNIX_EPOCH - Duration::from_nanos(1_000_001)),
            -1
        );
    }

    #[test]
    fn lower_limit_shifts_before_truncating_to_milliseconds() {
        let now = UNIX_EPOCH + Duration::from_micros(500);
        assert_eq!(
            system_time_to_lower_limit_start_timestamp(now, 1),
            system_time_to_timestamp(UNIX_EPOCH - Duration::from_micros(500))
        );
        assert_eq!(
            system_time_to_lower_limit_start_timestamp(now, -1),
            system_time_to_timestamp(UNIX_EPOCH + Duration::from_micros(1_500))
        );
    }

    #[tokio::test]
    async fn noop_validator_always_succeeds() {
        let validator = NoopReadTimestampValidator;
        let option = OracleOption {
            txn_scope: GLOBAL_TXN_SCOPE.to_owned(),
        };
        validator
            .validate_read_timestamp(u64::MAX, true, &option)
            .await
            .unwrap();
    }

    #[test]
    fn oracle_errors_preserve_source_text_and_fields() {
        let future = FutureTimestampReadError {
            read_timestamp: 42,
            current_timestamp: 41,
        };
        assert_eq!(future.read_timestamp, 42);
        assert_eq!(future.current_timestamp, 41);
        assert_eq!(
            future.to_string(),
            "cannot set read timestamp to a future time, readTS: 42, currentTS: 41"
        );
        assert_eq!(
            LatestStaleReadError.to_string(),
            "cannot set read ts to max uint64 for stale read"
        );
    }
}
