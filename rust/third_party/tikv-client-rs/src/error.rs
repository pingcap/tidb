//! Client-go compatible error values and key-error extraction.

use std::error::Error as StdError;
use std::fmt;
use std::time::SystemTime;

use crate::proto::{kvrpcpb, pdpb};

/// Text used by PD when a response belongs to another cluster.
pub const MISMATCH_CLUSTER_ID: &str = "mismatch cluster id";

/// Source-compatible singleton error categories.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticError {
    BodyMissing,
    TiDbShuttingDown,
    NotExist,
    CannotSetNilValue,
    InvalidTransaction,
    TiKvServerTimeout,
    TiFlashServerTimeout,
    QueryInterrupted,
    TiKvStaleCommand,
    TiKvMaxTimestampNotSynced,
    LockAcquireFailedNoWait,
    ResolveLockTimeout,
    LockWaitTimeout,
    TiKvServerBusy,
    TiFlashServerBusy,
    RegionUnavailable,
    RegionDataNotReady,
    RegionNotInitialized,
    TiKvDiskFull,
    CommitTimestampLag,
    RegionRecoveryInProgress,
    RegionFlashbackInProgress,
    RegionFlashbackNotPrepared,
    IsWitness,
    Unknown,
    ResultUndetermined,
}

impl fmt::Display for StaticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BodyMissing => "response body is missing",
            Self::TiDbShuttingDown => "tidb server shutting down",
            Self::NotExist => "not exist",
            Self::CannotSetNilValue => "can not set nil value",
            Self::InvalidTransaction => "invalid transaction",
            Self::TiKvServerTimeout => "tikv server timeout",
            Self::TiFlashServerTimeout => "tiflash server timeout",
            Self::QueryInterrupted => "query interrupted",
            Self::TiKvStaleCommand => "tikv stale command",
            Self::TiKvMaxTimestampNotSynced => "tikv max timestamp not synced",
            Self::LockAcquireFailedNoWait => "lock acquired failed and no wait is set",
            Self::ResolveLockTimeout => "resolve lock timeout",
            Self::LockWaitTimeout => "lock wait timeout",
            Self::TiKvServerBusy => "tikv server busy",
            Self::TiFlashServerBusy => "tiflash server busy",
            Self::RegionUnavailable => "region unavailable",
            Self::RegionDataNotReady => "region data not ready",
            Self::RegionNotInitialized => "region not Initialized",
            Self::TiKvDiskFull => "tikv disk full",
            Self::CommitTimestampLag => "commit timestamp lags behind expected",
            Self::RegionRecoveryInProgress => "region is being online unsafe recovered",
            Self::RegionFlashbackInProgress => "region is in the flashback progress",
            Self::RegionFlashbackNotPrepared => "region is not prepared for the flashback",
            Self::IsWitness => "peer is witness",
            Self::Unknown => "unknown",
            Self::ResultUndetermined => "execution result undetermined",
        })
    }
}

impl StdError for StaticError {}

pub const ERR_BODY_MISSING: StaticError = StaticError::BodyMissing;
pub const ERR_TIDB_SHUTTING_DOWN: StaticError = StaticError::TiDbShuttingDown;
pub const ERR_NOT_EXIST: StaticError = StaticError::NotExist;
pub const ERR_CANNOT_SET_NIL_VALUE: StaticError = StaticError::CannotSetNilValue;
pub const ERR_INVALID_TXN: StaticError = StaticError::InvalidTransaction;
pub const ERR_TIKV_SERVER_TIMEOUT: StaticError = StaticError::TiKvServerTimeout;
pub const ERR_TIFLASH_SERVER_TIMEOUT: StaticError = StaticError::TiFlashServerTimeout;
pub const ERR_QUERY_INTERRUPTED: StaticError = StaticError::QueryInterrupted;
pub const ERR_TIKV_STALE_COMMAND: StaticError = StaticError::TiKvStaleCommand;
pub const ERR_TIKV_MAX_TIMESTAMP_NOT_SYNCED: StaticError = StaticError::TiKvMaxTimestampNotSynced;
pub const ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET: StaticError = StaticError::LockAcquireFailedNoWait;
pub const ERR_RESOLVE_LOCK_TIMEOUT: StaticError = StaticError::ResolveLockTimeout;
pub const ERR_LOCK_WAIT_TIMEOUT: StaticError = StaticError::LockWaitTimeout;
pub const ERR_TIKV_SERVER_BUSY: StaticError = StaticError::TiKvServerBusy;
pub const ERR_TIFLASH_SERVER_BUSY: StaticError = StaticError::TiFlashServerBusy;
pub const ERR_REGION_UNAVAILABLE: StaticError = StaticError::RegionUnavailable;
pub const ERR_REGION_DATA_NOT_READY: StaticError = StaticError::RegionDataNotReady;
pub const ERR_REGION_NOT_INITIALIZED: StaticError = StaticError::RegionNotInitialized;
pub const ERR_TIKV_DISK_FULL: StaticError = StaticError::TiKvDiskFull;
pub const ERR_COMMIT_TS_LAG: StaticError = StaticError::CommitTimestampLag;
pub const ERR_REGION_RECOVERY_IN_PROGRESS: StaticError = StaticError::RegionRecoveryInProgress;
pub const ERR_REGION_FLASHBACK_IN_PROGRESS: StaticError = StaticError::RegionFlashbackInProgress;
pub const ERR_REGION_FLASHBACK_NOT_PREPARED: StaticError = StaticError::RegionFlashbackNotPrepared;
pub const ERR_IS_WITNESS: StaticError = StaticError::IsWitness;
pub const ERR_UNKNOWN: StaticError = StaticError::Unknown;
pub const ERR_RESULT_UNDETERMINED: StaticError = StaticError::ResultUndetermined;

#[derive(Debug)]
pub struct QueryInterruptedWithSignalError {
    pub signal: u32,
}

impl fmt::Display for QueryInterruptedWithSignalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "query interrupted by signal {}", self.signal)
    }
}

impl StdError for QueryInterruptedWithSignalError {}

#[derive(Debug)]
pub struct DeadlockError {
    pub deadlock: kvrpcpb::Deadlock,
    pub is_retryable: bool,
}

impl fmt::Display for DeadlockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&deadlock_text(&self.deadlock))
    }
}

impl StdError for DeadlockError {}

#[derive(Debug)]
pub struct PdError {
    pub error: pdpb::Error,
}

impl fmt::Display for PdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&pd_error_text(&self.error))
    }
}

impl StdError for PdError {}

#[derive(Debug)]
pub struct KeyExistsError {
    pub already_exist: kvrpcpb::AlreadyExist,
    pub value: Vec<u8>,
}

impl fmt::Display for KeyExistsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.already_exist.key.is_empty() {
            return Ok(());
        }
        write!(formatter, "key:{}", protobuf_bytes(&self.already_exist.key))
    }
}

impl StdError for KeyExistsError {}

#[derive(Debug)]
pub struct WriteConflictError {
    pub conflict: kvrpcpb::WriteConflict,
}

impl fmt::Display for WriteConflictError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "write conflict {{ {} }}",
            write_conflict_text(&self.conflict)
        )
    }
}

impl StdError for WriteConflictError {}

pub fn new_write_conflict(conflict: kvrpcpb::WriteConflict) -> WriteConflictError {
    crate::stats::increment_write_conflict();
    WriteConflictError { conflict }
}

pub fn new_write_conflict_with_args(
    start_timestamp: u64,
    conflict_timestamp: u64,
    conflict_commit_timestamp: u64,
    key: Vec<u8>,
    reason: kvrpcpb::write_conflict::Reason,
) -> WriteConflictError {
    new_write_conflict(kvrpcpb::WriteConflict {
        start_ts: start_timestamp,
        conflict_ts: conflict_timestamp,
        key,
        conflict_commit_ts: conflict_commit_timestamp,
        reason: reason as i32,
        ..Default::default()
    })
}

#[derive(Debug)]
pub struct WriteConflictInLatchError {
    pub start_timestamp: u64,
}

impl fmt::Display for WriteConflictInLatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "write conflict in latch,startTS: {}",
            self.start_timestamp
        )
    }
}

impl StdError for WriteConflictInLatchError {}

macro_rules! scalar_error {
    ($name:ident, $field:ident: $type:ty, $format:literal) => {
        #[derive(Debug)]
        pub struct $name {
            pub $field: $type,
        }
        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, $format, self.$field)
            }
        }
        impl StdError for $name {}
    };
}

scalar_error!(RetryableError, message: String, "{}");
scalar_error!(TransactionTooLargeError, size: usize, "txn too large, size: {}.");
scalar_error!(KeyTooLargeError, key_size: usize, "key size too large, size: {}.");
scalar_error!(PdServerTimeoutError, message: String, "{}");
scalar_error!(TokenLimitError, store_id: u64, "Store token is up to the limit, store id = {}.");

#[derive(Debug)]
pub struct EntryTooLargeError {
    pub limit: u64,
    pub size: u64,
}

impl fmt::Display for EntryTooLargeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "entry size too large, size: {},limit: {}.",
            self.size, self.limit
        )
    }
}

impl StdError for EntryTooLargeError {}

#[derive(Debug)]
#[deprecated(note = "use TransactionAbortedByGcError")]
pub struct GcTooEarlyError {
    pub transaction_start: SystemTime,
    pub gc_safe_point: SystemTime,
}

#[allow(deprecated)]
impl fmt::Display for GcTooEarlyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "GC life time is shorter than transaction duration, transaction starts at {}, GC safe point is {}",
            format_system_time(self.transaction_start),
            format_system_time(self.gc_safe_point)
        )
    }
}

#[allow(deprecated)]
impl StdError for GcTooEarlyError {}

#[derive(Debug)]
pub struct TransactionAbortedByGcError {
    pub transaction_start_timestamp: u64,
    pub transaction_start_time: SystemTime,
    pub transaction_safe_point: u64,
    pub transaction_safe_point_time: SystemTime,
}

impl fmt::Display for TransactionAbortedByGcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "GC life time is shorter than transaction duration, transaction start ts is {} ({}), txn safe point is {} ({})",
            self.transaction_start_timestamp,
            format_system_time(self.transaction_start_time),
            self.transaction_safe_point,
            format_system_time(self.transaction_safe_point_time)
        )
    }
}

impl StdError for TransactionAbortedByGcError {}

#[derive(Debug)]
pub struct AssertionFailedError {
    pub assertion_failed: kvrpcpb::AssertionFailed,
}

impl fmt::Display for AssertionFailedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "assertion failed {{ {} }}",
            assertion_failed_text(&self.assertion_failed)
        )
    }
}

impl StdError for AssertionFailedError {}

#[derive(Debug)]
pub struct LockOnlyIfExistsNoReturnValueError {
    pub start_timestamp: u64,
    pub for_update_timestamp: u64,
    pub lock_key: Vec<u8>,
}

impl fmt::Display for LockOnlyIfExistsNoReturnValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LockOnlyIfExists is set for Lock Context, but ReturnValues is not set, StartTs is {{{}}}, ForUpdateTs is {{{}}}, one of lock keys is {{{}}}.",
            self.start_timestamp,
            self.for_update_timestamp,
            crate::redact::key(&self.lock_key)
        )
    }
}

impl StdError for LockOnlyIfExistsNoReturnValueError {}

#[derive(Debug)]
pub struct LockOnlyIfExistsNoPrimaryKeyError {
    pub start_timestamp: u64,
    pub for_update_timestamp: u64,
    pub lock_key: Vec<u8>,
}

impl fmt::Display for LockOnlyIfExistsNoPrimaryKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "LockOnlyIfExists is set for Lock Context, but primary key of current transaction is not set, StartTs is {{{}}}, ForUpdateTs is {{{}}}, one of lock keys is {{{}}}",
            self.start_timestamp,
            self.for_update_timestamp,
            crate::redact::key(&self.lock_key)
        )
    }
}

impl StdError for LockOnlyIfExistsNoPrimaryKeyError {}

#[derive(Debug)]
struct MessageError(String);

impl fmt::Display for MessageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl StdError for MessageError {}

pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// Extract the highest-priority typed error from a TiKV key error.
pub fn extract_key_error(key_error: &mut kvrpcpb::KeyError) -> BoxError {
    if fail::eval("mockRetryableErrorResp", |value| {
        value
            .as_deref()
            .unwrap_or("false")
            .parse::<bool>()
            .unwrap_or(false)
    })
    .unwrap_or(false)
    {
        key_error.conflict = None;
        key_error.retryable = "mock retryable error".to_owned();
    }

    crate::redact::redact_key_error_if_necessary(key_error);
    if let Some(conflict) = key_error.conflict.clone() {
        return Box::new(new_write_conflict(conflict));
    }
    if !key_error.retryable.is_empty() {
        return Box::new(RetryableError {
            message: key_error.retryable.clone(),
        });
    }
    if let Some(assertion_failed) = key_error.assertion_failed.clone() {
        return Box::new(AssertionFailedError { assertion_failed });
    }
    if !key_error.abort.is_empty() {
        let error = MessageError(format!("tikv aborts txn: {}", key_error.abort));
        log::warn!("2PC failed: {error}");
        return Box::new(error);
    }
    if let Some(too_large) = &key_error.commit_ts_too_large {
        let error = MessageError(format!("commit TS {} is too large", too_large.commit_ts));
        log::warn!("2PC failed: {error}");
        return Box::new(error);
    }
    if let Some(not_found) = &key_error.txn_not_found {
        return Box::new(MessageError(format!(
            "txn {} not found",
            not_found.start_ts
        )));
    }
    Box::new(MessageError(format!("unexpected KeyError: {key_error:?}")))
}

pub fn is_not_found(error: &(dyn StdError + 'static)) -> bool {
    has_static_error(error, StaticError::NotExist)
}

pub fn is_error_undetermined(error: &(dyn StdError + 'static)) -> bool {
    has_static_error(error, StaticError::ResultUndetermined)
}

pub fn is_error_commit_timestamp_lag(error: &(dyn StdError + 'static)) -> bool {
    has_static_error(error, StaticError::CommitTimestampLag)
}

pub fn is_key_exists(error: &(dyn StdError + 'static)) -> bool {
    error_chain(error).any(|error| error.is::<KeyExistsError>())
}

pub fn is_write_conflict(error: &(dyn StdError + 'static)) -> bool {
    error_chain(error).any(|error| {
        error.is::<WriteConflictError>()
            || matches!(
                error.downcast_ref::<crate::Error>(),
                Some(crate::Error::WriteConflict(_))
            )
    })
}

fn has_static_error(error: &(dyn StdError + 'static), expected: StaticError) -> bool {
    error_chain(error).any(|error| error.downcast_ref::<StaticError>() == Some(&expected))
}

fn error_chain<'a>(error: &'a (dyn StdError + 'static)) -> ErrorChain<'a> {
    ErrorChain(Some(error))
}

struct ErrorChain<'a>(Option<&'a (dyn StdError + 'static)>);

impl<'a> Iterator for ErrorChain<'a> {
    type Item = &'a (dyn StdError + 'static);

    fn next(&mut self) -> Option<Self::Item> {
        let current = self.0.take()?;
        self.0 = current.source();
        Some(current)
    }
}

/// Log an error when one is present.
pub fn log(error: Option<&(dyn StdError + 'static)>) {
    if let Some(error) = error {
        log::error!("encountered error: {error}");
    }
}

/// Extract protobuf-JSON debug information without mutating the response.
pub fn extract_debug_info_string(key_error: &kvrpcpb::KeyError) -> String {
    let Some(mut debug_info) = key_error.debug_info.clone() else {
        return String::new();
    };
    if crate::redact::need_redact() {
        redact_debug_info(&mut debug_info);
    }
    debug_info_json(&debug_info)
}

fn redact_debug_info(debug_info: &mut kvrpcpb::DebugInfo) {
    for info in &mut debug_info.mvcc_info {
        info.key = b"?".to_vec();
        if let Some(mvcc) = &mut info.mvcc {
            if let Some(lock) = &mut mvcc.lock {
                lock.primary = b"?".to_vec();
                lock.short_value = b"?".to_vec();
                for secondary in &mut lock.secondaries {
                    *secondary = b"?".to_vec();
                }
            }
            for write in &mut mvcc.writes {
                write.short_value = b"?".to_vec();
            }
            for value in &mut mvcc.values {
                value.value = b"?".to_vec();
            }
        }
    }
}

fn write_conflict_text(conflict: &kvrpcpb::WriteConflict) -> String {
    let mut fields = Vec::new();
    push_proto_u64(&mut fields, "start_ts", conflict.start_ts);
    push_proto_u64(&mut fields, "conflict_ts", conflict.conflict_ts);
    push_proto_bytes(&mut fields, "key", &conflict.key);
    push_proto_bytes(&mut fields, "primary", &conflict.primary);
    push_proto_u64(
        &mut fields,
        "conflict_commit_ts",
        conflict.conflict_commit_ts,
    );
    if conflict.reason != 0 {
        let reason = kvrpcpb::write_conflict::Reason::try_from(conflict.reason)
            .map(|reason| reason.as_str_name().to_owned())
            .unwrap_or_else(|_| conflict.reason.to_string());
        fields.push(format!("reason:{reason}"));
    }
    fields.join(" ")
}

fn assertion_failed_text(assertion: &kvrpcpb::AssertionFailed) -> String {
    let mut fields = Vec::new();
    push_proto_u64(&mut fields, "start_ts", assertion.start_ts);
    push_proto_bytes(&mut fields, "key", &assertion.key);
    if assertion.assertion != 0 {
        let value = kvrpcpb::Assertion::try_from(assertion.assertion)
            .map(|value| value.as_str_name().to_owned())
            .unwrap_or_else(|_| assertion.assertion.to_string());
        fields.push(format!("assertion:{value}"));
    }
    push_proto_u64(
        &mut fields,
        "existing_start_ts",
        assertion.existing_start_ts,
    );
    push_proto_u64(
        &mut fields,
        "existing_commit_ts",
        assertion.existing_commit_ts,
    );
    fields.join(" ")
}

fn deadlock_text(deadlock: &kvrpcpb::Deadlock) -> String {
    let mut fields = Vec::new();
    push_proto_u64(&mut fields, "lock_ts", deadlock.lock_ts);
    push_proto_bytes(&mut fields, "lock_key", &deadlock.lock_key);
    push_proto_u64(&mut fields, "deadlock_key_hash", deadlock.deadlock_key_hash);
    for entry in &deadlock.wait_chain {
        fields.push(format!("wait_chain:<{} >", wait_for_entry_text(entry)));
    }
    push_proto_bytes(&mut fields, "deadlock_key", &deadlock.deadlock_key);
    fields.join(" ")
}

fn wait_for_entry_text(entry: &crate::proto::deadlock::WaitForEntry) -> String {
    let mut fields = Vec::new();
    push_proto_u64(&mut fields, "txn", entry.txn);
    push_proto_u64(&mut fields, "wait_for_txn", entry.wait_for_txn);
    push_proto_u64(&mut fields, "key_hash", entry.key_hash);
    push_proto_bytes(&mut fields, "key", &entry.key);
    push_proto_bytes(&mut fields, "resource_group_tag", &entry.resource_group_tag);
    push_proto_u64(&mut fields, "wait_time", entry.wait_time);
    fields.join(" ")
}

fn pd_error_text(error: &pdpb::Error) -> String {
    let mut fields = Vec::new();
    if error.r#type != 0 {
        let value = pdpb::ErrorType::try_from(error.r#type)
            .map(|value| value.as_str_name().to_owned())
            .unwrap_or_else(|_| error.r#type.to_string());
        fields.push(format!("type:{value}"));
    }
    if !error.message.is_empty() {
        fields.push(format!(
            "message:{}",
            protobuf_bytes(error.message.as_bytes())
        ));
    }
    fields.join(" ")
}

fn push_proto_u64(fields: &mut Vec<String>, name: &str, value: u64) {
    if value != 0 {
        fields.push(format!("{name}:{value}"));
    }
}

fn push_proto_bytes(fields: &mut Vec<String>, name: &str, value: &[u8]) {
    if !value.is_empty() {
        fields.push(format!("{name}:{}", protobuf_bytes(value)));
    }
}

fn protobuf_bytes(value: &[u8]) -> String {
    let mut result = String::from("\"");
    for byte in value {
        match *byte {
            b'\\' => result.push_str("\\\\"),
            b'\"' => result.push_str("\\\""),
            b'\n' => result.push_str("\\n"),
            b'\r' => result.push_str("\\r"),
            b'\t' => result.push_str("\\t"),
            0x20..=0x7e => result.push(*byte as char),
            value => result.push_str(&format!("\\x{value:02x}")),
        }
    }
    result.push('"');
    result
}

fn format_system_time(time: SystemTime) -> String {
    let nanoseconds = match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => {
            i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos())
        }
        Err(error) => {
            let duration = error.duration();
            -(i128::from(duration.as_secs()) * 1_000_000_000 + i128::from(duration.subsec_nanos()))
        }
    };
    let seconds = nanoseconds.div_euclid(1_000_000_000);
    let subsecond = nanoseconds.rem_euclid(1_000_000_000) as u32;
    let days = seconds.div_euclid(86_400) as i64;
    let seconds_of_day = seconds.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_date_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = seconds_of_day % 3_600 / 60;
    let second = seconds_of_day % 60;
    let fraction = if subsecond == 0 {
        String::new()
    } else {
        format!(".{subsecond:09}").trim_end_matches('0').to_owned()
    };
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}{fraction} +0000 UTC")
}

fn civil_date_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let shifted = days_since_epoch + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month, day)
}

fn debug_info_json(debug_info: &kvrpcpb::DebugInfo) -> String {
    let fields = if debug_info.mvcc_info.is_empty() {
        Vec::new()
    } else {
        vec![(
            "mvcc_info",
            json_array(debug_info.mvcc_info.iter().map(mvcc_debug_info_json)),
        )]
    };
    json_object(fields)
}

fn mvcc_debug_info_json(info: &kvrpcpb::MvccDebugInfo) -> String {
    let mut fields = Vec::new();
    push_bytes(&mut fields, "key", &info.key);
    if let Some(mvcc) = &info.mvcc {
        fields.push(("mvcc", mvcc_info_json(mvcc)));
    }
    json_object(fields)
}

fn mvcc_info_json(info: &kvrpcpb::MvccInfo) -> String {
    let mut fields = Vec::new();
    if let Some(lock) = &info.lock {
        fields.push(("lock", mvcc_lock_json(lock)));
    }
    if !info.writes.is_empty() {
        fields.push((
            "writes",
            json_array(info.writes.iter().map(mvcc_write_json)),
        ));
    }
    if !info.values.is_empty() {
        fields.push((
            "values",
            json_array(info.values.iter().map(mvcc_value_json)),
        ));
    }
    json_object(fields)
}

fn mvcc_lock_json(lock: &kvrpcpb::MvccLock) -> String {
    let mut fields = Vec::new();
    push_i32(&mut fields, "type", lock.r#type);
    push_u64(&mut fields, "start_ts", lock.start_ts);
    push_bytes(&mut fields, "primary", &lock.primary);
    push_bytes(&mut fields, "short_value", &lock.short_value);
    push_u64(&mut fields, "ttl", lock.ttl);
    push_u64(&mut fields, "for_update_ts", lock.for_update_ts);
    push_u64(&mut fields, "txn_size", lock.txn_size);
    if lock.use_async_commit {
        fields.push(("use_async_commit", "true".to_owned()));
    }
    if !lock.secondaries.is_empty() {
        fields.push((
            "secondaries",
            json_array(lock.secondaries.iter().map(|value| json_bytes(value))),
        ));
    }
    if !lock.rollback_ts.is_empty() {
        fields.push((
            "rollback_ts",
            json_array(lock.rollback_ts.iter().map(ToString::to_string)),
        ));
    }
    push_u64(&mut fields, "last_change_ts", lock.last_change_ts);
    push_u64(
        &mut fields,
        "versions_to_last_change",
        lock.versions_to_last_change,
    );
    json_object(fields)
}

fn mvcc_write_json(write: &kvrpcpb::MvccWrite) -> String {
    let mut fields = Vec::new();
    push_i32(&mut fields, "type", write.r#type);
    push_u64(&mut fields, "start_ts", write.start_ts);
    push_u64(&mut fields, "commit_ts", write.commit_ts);
    push_bytes(&mut fields, "short_value", &write.short_value);
    if write.has_overlapped_rollback {
        fields.push(("has_overlapped_rollback", "true".to_owned()));
    }
    if write.has_gc_fence {
        fields.push(("has_gc_fence", "true".to_owned()));
    }
    push_u64(&mut fields, "gc_fence", write.gc_fence);
    push_u64(&mut fields, "last_change_ts", write.last_change_ts);
    push_u64(
        &mut fields,
        "versions_to_last_change",
        write.versions_to_last_change,
    );
    json_object(fields)
}

fn mvcc_value_json(value: &kvrpcpb::MvccValue) -> String {
    let mut fields = Vec::new();
    push_u64(&mut fields, "start_ts", value.start_ts);
    push_bytes(&mut fields, "value", &value.value);
    json_object(fields)
}

fn push_u64(fields: &mut Vec<(&'static str, String)>, name: &'static str, value: u64) {
    if value != 0 {
        fields.push((name, value.to_string()));
    }
}

fn push_i32(fields: &mut Vec<(&'static str, String)>, name: &'static str, value: i32) {
    if value != 0 {
        fields.push((name, value.to_string()));
    }
}

fn push_bytes(fields: &mut Vec<(&'static str, String)>, name: &'static str, value: &[u8]) {
    if !value.is_empty() {
        fields.push((name, json_bytes(value)));
    }
}

fn json_object(fields: Vec<(&str, String)>) -> String {
    let fields = fields
        .into_iter()
        .map(|(name, value)| format!("{}:{value}", json_string(name)))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{fields}}}")
}

fn json_array(values: impl IntoIterator<Item = String>) -> String {
    format!("[{}]", values.into_iter().collect::<Vec<_>>().join(","))
}

fn json_bytes(value: &[u8]) -> String {
    json_string(&base64(value))
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).expect("serializing a string cannot fail")
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        result.push(ALPHABET[(first >> 2) as usize] as char);
        result.push(ALPHABET[((first & 0x03) << 4 | second >> 4) as usize] as char);
        if chunk.len() > 1 {
            result.push(ALPHABET[((second & 0x0f) << 2 | third >> 6) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[(third & 0x3f) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    struct DisableRedaction;
    impl Drop for DisableRedaction {
        fn drop(&mut self) {
            crate::redact::set_redact_log_enabled(false);
        }
    }

    fn source_debug_info() -> kvrpcpb::DebugInfo {
        kvrpcpb::DebugInfo {
            mvcc_info: vec![kvrpcpb::MvccDebugInfo {
                key: b"byte".to_vec(),
                mvcc: Some(kvrpcpb::MvccInfo {
                    lock: Some(kvrpcpb::MvccLock {
                        r#type: kvrpcpb::Op::Del as i32,
                        start_ts: 128,
                        primary: b"k1".to_vec(),
                        short_value: b"v1".to_vec(),
                        secondaries: vec![b"k1".to_vec(), b"k2".to_vec()],
                        ..Default::default()
                    }),
                    writes: vec![kvrpcpb::MvccWrite {
                        r#type: kvrpcpb::Op::Insert as i32,
                        start_ts: 64,
                        commit_ts: 86,
                        short_value: vec![1, 2, 3, 4, 5, 6],
                        ..Default::default()
                    }],
                    values: vec![kvrpcpb::MvccValue {
                        start_ts: 64,
                        value: vec![0x11, 0x12],
                    }],
                }),
            }],
        }
    }

    #[test]
    fn static_and_structured_errors_preserve_source_text() {
        let all = [
            (ERR_BODY_MISSING, "response body is missing"),
            (ERR_TIDB_SHUTTING_DOWN, "tidb server shutting down"),
            (ERR_NOT_EXIST, "not exist"),
            (ERR_CANNOT_SET_NIL_VALUE, "can not set nil value"),
            (ERR_INVALID_TXN, "invalid transaction"),
            (ERR_TIKV_SERVER_TIMEOUT, "tikv server timeout"),
            (ERR_TIFLASH_SERVER_TIMEOUT, "tiflash server timeout"),
            (ERR_QUERY_INTERRUPTED, "query interrupted"),
            (ERR_TIKV_STALE_COMMAND, "tikv stale command"),
            (
                ERR_TIKV_MAX_TIMESTAMP_NOT_SYNCED,
                "tikv max timestamp not synced",
            ),
            (
                ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET,
                "lock acquired failed and no wait is set",
            ),
            (ERR_RESOLVE_LOCK_TIMEOUT, "resolve lock timeout"),
            (ERR_LOCK_WAIT_TIMEOUT, "lock wait timeout"),
            (ERR_TIKV_SERVER_BUSY, "tikv server busy"),
            (ERR_TIFLASH_SERVER_BUSY, "tiflash server busy"),
            (ERR_REGION_UNAVAILABLE, "region unavailable"),
            (ERR_REGION_DATA_NOT_READY, "region data not ready"),
            (ERR_REGION_NOT_INITIALIZED, "region not Initialized"),
            (ERR_TIKV_DISK_FULL, "tikv disk full"),
            (ERR_COMMIT_TS_LAG, "commit timestamp lags behind expected"),
            (
                ERR_REGION_RECOVERY_IN_PROGRESS,
                "region is being online unsafe recovered",
            ),
            (
                ERR_REGION_FLASHBACK_IN_PROGRESS,
                "region is in the flashback progress",
            ),
            (
                ERR_REGION_FLASHBACK_NOT_PREPARED,
                "region is not prepared for the flashback",
            ),
            (ERR_IS_WITNESS, "peer is witness"),
            (ERR_UNKNOWN, "unknown"),
            (ERR_RESULT_UNDETERMINED, "execution result undetermined"),
        ];
        for (error, expected) in all {
            assert_eq!(error.to_string(), expected);
        }
        assert_eq!(
            QueryInterruptedWithSignalError { signal: 9 }.to_string(),
            "query interrupted by signal 9"
        );
        assert_eq!(
            TransactionTooLargeError { size: 42 }.to_string(),
            "txn too large, size: 42."
        );
        assert_eq!(
            EntryTooLargeError {
                limit: 10,
                size: 11
            }
            .to_string(),
            "entry size too large, size: 11,limit: 10."
        );
        assert_eq!(
            WriteConflictInLatchError { start_timestamp: 7 }.to_string(),
            "write conflict in latch,startTS: 7"
        );
        assert!(is_not_found(&ERR_NOT_EXIST));
        assert!(is_error_undetermined(&ERR_RESULT_UNDETERMINED));
        assert!(is_error_commit_timestamp_lag(&ERR_COMMIT_TS_LAG));

        let key_exists = KeyExistsError {
            already_exist: kvrpcpb::AlreadyExist {
                key: b"key".to_vec(),
            },
            value: b"value".to_vec(),
        };
        assert!(is_key_exists(&key_exists));
        assert_eq!(key_exists.to_string(), r#"key:"key""#);

        let conflict = WriteConflictError {
            conflict: kvrpcpb::WriteConflict {
                start_ts: 1,
                conflict_ts: 2,
                key: b"key".to_vec(),
                conflict_commit_ts: 3,
                reason: kvrpcpb::write_conflict::Reason::Optimistic as i32,
                ..Default::default()
            },
        };
        assert_eq!(
            conflict.to_string(),
            r#"write conflict { start_ts:1 conflict_ts:2 key:"key" conflict_commit_ts:3 reason:Optimistic }"#
        );

        #[allow(deprecated)]
        let gc_error = GcTooEarlyError {
            transaction_start: SystemTime::UNIX_EPOCH,
            gc_safe_point: SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(120_000_000),
        };
        assert_eq!(
            gc_error.to_string(),
            "GC life time is shorter than transaction duration, transaction starts at 1970-01-01 00:00:00 +0000 UTC, GC safe point is 1970-01-01 00:00:00.12 +0000 UTC"
        );
        assert_eq!(
            format_system_time(SystemTime::UNIX_EPOCH - std::time::Duration::from_nanos(1)),
            "1969-12-31 23:59:59.999999999 +0000 UTC"
        );
    }

    #[test]
    #[serial]
    fn key_error_extraction_uses_source_priority_and_typed_results() {
        let mut key_error = kvrpcpb::KeyError {
            conflict: Some(kvrpcpb::WriteConflict {
                start_ts: 1,
                ..Default::default()
            }),
            retryable: "later".to_owned(),
            assertion_failed: Some(kvrpcpb::AssertionFailed::default()),
            ..Default::default()
        };
        let error = extract_key_error(&mut key_error);
        assert!(error.is::<WriteConflictError>());
        assert!(is_write_conflict(error.as_ref()));
        assert!(key_error.conflict.is_some());

        let mut retryable = kvrpcpb::KeyError {
            retryable: "retry me".to_owned(),
            ..Default::default()
        };
        let error = extract_key_error(&mut retryable);
        assert_eq!(error.to_string(), "retry me");
        assert!(error.is::<RetryableError>());

        let mut assertion = kvrpcpb::KeyError {
            assertion_failed: Some(kvrpcpb::AssertionFailed {
                start_ts: 8,
                key: b"key".to_vec(),
                assertion: kvrpcpb::Assertion::Exist as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let error = extract_key_error(&mut assertion);
        assert!(error.is::<AssertionFailedError>());
        assert!(assertion.assertion_failed.is_some());

        for (mut key_error, expected) in [
            (
                kvrpcpb::KeyError {
                    abort: "abort reason".to_owned(),
                    ..Default::default()
                },
                "tikv aborts txn: abort reason",
            ),
            (
                kvrpcpb::KeyError {
                    commit_ts_too_large: Some(kvrpcpb::CommitTsTooLarge { commit_ts: 99 }),
                    ..Default::default()
                },
                "commit TS 99 is too large",
            ),
            (
                kvrpcpb::KeyError {
                    txn_not_found: Some(kvrpcpb::TxnNotFound {
                        start_ts: 101,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                "txn 101 not found",
            ),
        ] {
            assert_eq!(extract_key_error(&mut key_error).to_string(), expected);
        }

        let mut unexpected = kvrpcpb::KeyError::default();
        assert!(extract_key_error(&mut unexpected)
            .to_string()
            .starts_with("unexpected KeyError:"));

        let before = crate::stats::write_conflict_count();
        let _ = new_write_conflict_with_args(
            1,
            2,
            3,
            b"key".to_vec(),
            kvrpcpb::write_conflict::Reason::Optimistic,
        );
        assert_eq!(crate::stats::write_conflict_count(), before + 1);
    }

    #[test]
    #[serial]
    fn failpoint_overrides_a_conflict_with_retryable_error() {
        let scenario = fail::FailScenario::setup();
        fail::cfg("mockRetryableErrorResp", "return(true)").unwrap();
        let mut key_error = kvrpcpb::KeyError {
            conflict: Some(kvrpcpb::WriteConflict::default()),
            ..Default::default()
        };
        let error = extract_key_error(&mut key_error);
        assert!(error.is::<RetryableError>());
        assert_eq!(error.to_string(), "mock retryable error");
        assert!(key_error.conflict.is_none());
        drop(scenario);
    }

    #[test]
    #[serial]
    fn original_debug_info_json_and_redaction_scenario() {
        crate::redact::set_redact_log_enabled(false);
        let _reset = DisableRedaction;
        assert_eq!(
            extract_debug_info_string(&kvrpcpb::KeyError {
                txn_lock_not_found: Some(kvrpcpb::TxnLockNotFound {
                    key: b"byte".to_vec(),
                }),
                ..Default::default()
            }),
            ""
        );
        let debug_info = source_debug_info();
        let error = kvrpcpb::KeyError {
            debug_info: Some(debug_info.clone()),
            ..Default::default()
        };
        assert_eq!(
            extract_debug_info_string(&error),
            r#"{"mvcc_info":[{"key":"Ynl0ZQ==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"azE=","short_value":"djE=","secondaries":["azE=","azI="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"AQIDBAUG"}],"values":[{"start_ts":64,"value":"ERI="}]}}]}"#
        );

        crate::redact::set_redact_log_enabled(true);
        assert_eq!(
            extract_debug_info_string(&error),
            r#"{"mvcc_info":[{"key":"Pw==","mvcc":{"lock":{"type":1,"start_ts":128,"primary":"Pw==","short_value":"Pw==","secondaries":["Pw==","Pw=="]},"writes":[{"type":4,"start_ts":64,"commit_ts":86,"short_value":"Pw=="}],"values":[{"start_ts":64,"value":"Pw=="}]}}]}"#
        );
        assert_eq!(error.debug_info, Some(debug_info));
    }
}
