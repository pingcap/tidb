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

//! Complete `RunInNewTxn` control flow from `pkg/kv/txn.go`.

use std::error::Error;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{
    is_txn_retryable_error, retry_backoff_upper_bound_ms, InnerTxnStartTsBox, KvError, OptionKey,
    RequestSource, GLOBAL_INNER_TXN_START_TS,
};

/// Source maximum number of attempts.
pub const MAX_RETRY_COUNT: u32 = 100;

/// Dynamically typed transaction option values used by `RunInNewTxn`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxnOptionValue {
    /// Boolean option.
    Bool(bool),
    /// String option.
    String(String),
}

/// A transaction whose write RPCs can be assigned to a resource group.
pub trait TxnResourceGroup {
    /// Sets the resource group inherited by this transaction's write RPCs.
    fn set_resource_group_name(&mut self, name: &str);
}

/// Transaction operations needed by `RunInNewTxn`.
pub trait NewTxnTransaction: TxnResourceGroup {
    /// Error identity shared with its storage.
    type Error: NewTxnError;

    /// Returns this attempt's start timestamp.
    fn start_ts(&self) -> u64;
    /// Sets one package option.
    fn set_option(&mut self, option: OptionKey, value: TxnOptionValue);
    /// Rolls back an unsuccessful callback.
    fn rollback(&mut self) -> Result<(), Self::Error>;
    /// Commits a successful callback.
    fn commit(&mut self) -> Result<(), Self::Error>;
}

/// Storage operation needed to begin each attempt.
pub trait NewTxnStorage {
    /// Transaction type.
    type Transaction: NewTxnTransaction<Error = Self::Error>;
    /// Shared error identity.
    type Error: NewTxnError;

    /// Begins a fresh transaction.
    fn begin(&mut self) -> Result<Self::Transaction, Self::Error>;
}

/// Retry classification required by the package loop.
pub trait NewTxnError: Error {
    /// Whether this error has a source retryable transaction identity.
    fn is_retryable(&self) -> bool;
}

impl NewTxnError for KvError {
    fn is_retryable(&self) -> bool {
        is_txn_retryable_error(Some(self))
    }
}

/// Typed context fields consumed by `setRequestSourceForInnerTxn`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RunInNewTxnContext {
    /// Optional request origin.
    pub request_source: Option<RequestSource>,
}

struct TrackedTimestamp<'a> {
    registry: &'a InnerTxnStartTsBox,
    start_ts: u64,
}

impl Drop for TrackedTimestamp<'_> {
    fn drop(&mut self) {
        self.registry.delete_inner_txn_ts(self.start_ts);
    }
}

/// Runs a callback in a fresh internal transaction with source retry behavior.
pub fn run_in_new_txn<S, F>(
    context: &RunInNewTxnContext,
    storage: &mut S,
    retryable: bool,
    callback: F,
) -> Result<(), S::Error>
where
    S: NewTxnStorage,
    F: FnMut(&mut S::Transaction) -> Result<(), S::Error>,
{
    run_in_new_txn_with(
        context,
        storage,
        retryable,
        MAX_RETRY_COUNT,
        &GLOBAL_INNER_TXN_START_TS,
        callback,
        |attempt| {
            let delay = retry_backoff_delay(attempt);
            thread::sleep(delay);
        },
    )
}

/// Injectable form used by source tests and deterministic owners.
pub fn run_in_new_txn_with<S, F, B>(
    context: &RunInNewTxnContext,
    storage: &mut S,
    retryable: bool,
    max_retry_count: u32,
    registry: &InnerTxnStartTsBox,
    mut callback: F,
    mut backoff: B,
) -> Result<(), S::Error>
where
    S: NewTxnStorage,
    F: FnMut(&mut S::Transaction) -> Result<(), S::Error>,
    B: FnMut(u32),
{
    let mut tracked = TrackedTimestamp {
        registry,
        start_ts: 0,
    };
    let mut last_error = None;

    for attempt in 0..max_retry_count {
        let mut transaction = storage.begin()?;
        set_request_source(context, &mut transaction);

        if attempt == 0 {
            tracked.start_ts = transaction.start_ts();
            registry.store_inner_txn_ts(tracked.start_ts);
        }

        if let Err(error) = callback(&mut transaction) {
            let _ = transaction.rollback();
            if retryable && error.is_retryable() {
                last_error = Some(error);
                continue;
            }
            return Err(error);
        }

        match transaction.commit() {
            Ok(()) => return Ok(()),
            Err(error) if retryable && error.is_retryable() => {
                last_error = Some(error);
                backoff(attempt);
            }
            Err(error) => return Err(error),
        }
    }

    match last_error {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn set_request_source<T: NewTxnTransaction>(context: &RunInNewTxnContext, transaction: &mut T) {
    let Some(source) = context.request_source.as_ref() else {
        eprintln!(
            "unexpected no source type context, if you see this warning, the RequestSourceTypeKey is missing in the context"
        );
        return;
    };
    if source.source_type.is_empty() {
        eprintln!(
            "unexpected no source type context, if you see this warning, the RequestSourceTypeKey is missing in the context"
        );
        return;
    }
    if !source.internal {
        eprintln!("`RunInNewTxn` should be used by inner txn only");
    }
    transaction.set_option(
        OptionKey::RequestSourceInternal,
        TxnOptionValue::Bool(source.internal),
    );
    transaction.set_option(
        OptionKey::RequestSourceType,
        TxnOptionValue::String(source.source_type.clone()),
    );
    if !source.explicit_source_type.is_empty() {
        transaction.set_option(
            OptionKey::ExplicitRequestSourceType,
            TxnOptionValue::String(source.explicit_source_type.clone()),
        );
    }
}

/// Updates the transaction's resource-group name.
///
/// The Go failpoint validates the eventual RPC context after this option is
/// consumed. Rust keeps the package boundary exact here; the transaction
/// transport owns the corresponding RPC-context assertion.
pub fn set_txn_resource_group<T: TxnResourceGroup>(transaction: &mut T, name: &str) {
    transaction.set_resource_group_name(name);
}

/// Samples Go's full-jitter delay for one commit retry.
#[must_use]
pub fn retry_backoff_delay(attempt: u32) -> Duration {
    let upper = retry_backoff_upper_bound_ms(attempt);
    if upper == 0 {
        return Duration::ZERO;
    }
    let entropy = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration.as_secs() ^ u64::from(duration.subsec_nanos())
        });
    Duration::from_millis(entropy % upper)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `mustBackOff` (`txn_test.go:35`): the sampled delay never exceeds
    /// the attempt's exponential bound, capped at 100ms.
    #[test]
    fn go_test_back_off() {
        assert!(retry_backoff_delay(1) <= Duration::from_millis(2));
        assert!(retry_backoff_delay(2) <= Duration::from_millis(4));
        assert!(retry_backoff_delay(3) <= Duration::from_millis(8));
        assert!(retry_backoff_delay(100_000) <= Duration::from_millis(100));
        // The bounds themselves follow the source sequence.
        assert_eq!(retry_backoff_upper_bound_ms(1), 2);
        assert_eq!(retry_backoff_upper_bound_ms(2), 4);
        assert_eq!(retry_backoff_upper_bound_ms(3), 8);
        assert_eq!(retry_backoff_upper_bound_ms(100_000), 100);
    }

    /// The mock error of Go's `interface_mock_test.go`: one flavor that the
    /// retry loop classifies as retryable, one that it must not.
    #[derive(Debug, Eq, PartialEq)]
    enum MockTxnError {
        Retryable,
        Fatal(&'static str),
    }

    impl std::fmt::Display for MockTxnError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Retryable => write!(f, "retryable"),
                Self::Fatal(text) => write!(f, "{text}"),
            }
        }
    }

    impl Error for MockTxnError {}

    impl NewTxnError for MockTxnError {
        fn is_retryable(&self) -> bool {
            matches!(self, Self::Retryable)
        }
    }

    /// Go `mockStorage`/`mockTxn`: a storage whose every COMMIT fails
    /// retryably, so a successful callback still exhausts the retry budget.
    #[derive(Default)]
    struct MustFailCommitStorage {
        attempts: std::cell::Cell<u32>,
    }

    struct MustFailTxn;

    impl TxnResourceGroup for MustFailTxn {
        fn set_resource_group_name(&mut self, _name: &str) {}
    }

    impl NewTxnTransaction for MustFailTxn {
        type Error = MockTxnError;
        fn start_ts(&self) -> u64 {
            1
        }
        fn set_option(&mut self, _option: OptionKey, _value: TxnOptionValue) {}
        fn rollback(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
        fn commit(&mut self) -> Result<(), Self::Error> {
            Err(MockTxnError::Retryable)
        }
    }

    impl NewTxnStorage for MustFailCommitStorage {
        type Transaction = MustFailTxn;
        type Error = MockTxnError;
        fn begin(&mut self) -> Result<Self::Transaction, Self::Error> {
            self.attempts.set(self.attempts.get() + 1);
            Ok(MustFailTxn)
        }
    }

    /// A storage whose callback errors are surfaced verbatim.
    #[derive(Default)]
    struct CallbackErrorStorage;

    impl NewTxnStorage for CallbackErrorStorage {
        type Transaction = MustFailTxn;
        type Error = MockTxnError;
        fn begin(&mut self) -> Result<Self::Transaction, Self::Error> {
            Ok(MustFailTxn)
        }
    }

    /// Go `TestRetryExceedCountError` (`txn_test.go:39`): exhausting the
    /// retry budget surfaces an error whatever the callback returned.
    #[test]
    fn go_test_retry_exceed_count_error() {
        let context = RunInNewTxnContext::default();
        let mut no_sleep = |_: u32| {};

        // Every commit fails retryably: the loop runs out and reports.
        let mut storage = MustFailCommitStorage::default();
        let error = run_in_new_txn_with(
            &context,
            &mut storage,
            true,
            5,
            &GLOBAL_INNER_TXN_START_TS,
            |_txn: &mut MustFailTxn| Ok(()),
            &mut no_sleep,
        );
        assert!(error.is_err(), "exhausted retries must report");
        assert_eq!(storage.attempts.get(), 5);

        // A retryable callback error is retried until the budget runs out.
        let mut storage = CallbackErrorStorage;
        let error = run_in_new_txn_with(
            &context,
            &mut storage,
            true,
            5,
            &GLOBAL_INNER_TXN_START_TS,
            |(_txn): &mut MustFailTxn| Err::<(), MockTxnError>(MockTxnError::Retryable),
            &mut no_sleep,
        );
        assert!(error.is_err());

        // "do not retry": a non-retryable callback error returns AT ONCE.
        let attempts = std::cell::Cell::new(0u32);
        let mut storage = CallbackErrorStorage;
        let error = run_in_new_txn_with(
            &context,
            &mut storage,
            true,
            5,
            &GLOBAL_INNER_TXN_START_TS,
            |txn: &mut MustFailTxn| {
                attempts.set(attempts.get() + 1);
                txn.rollback().ok();
                Err::<(), MockTxnError>(MockTxnError::Fatal("do not retry"))
            },
            &mut no_sleep,
        );
        assert!(error.is_err());
        assert_eq!(attempts.get(), 1);
    }
}
