use crate::{
    request::plan::CleanupLocksResult,
    transaction::{
        client::Client, sync_snapshot::SyncSnapshot, sync_transaction::SyncTransaction,
        ResolveLocksOptions,
    },
    BoundRange, Config, Error, Result, Timestamp, TransactionOptions,
};
use std::sync::Arc;

/// Detects whether a Tokio async runtime is already running on the current thread.
///
/// When the synchronous transaction client is used from within an existing async
/// runtime, blocking operations (such as `block_on`) can cause deadlocks or other
/// unexpected blocking behavior. This helper checks for a currently active Tokio
/// runtime and returns `Error::NestedRuntimeError` if one is found, allowing callers
/// to detect and handle incorrect use of the synchronous client from within an
/// existing async runtime instead of risking deadlocks or unexpected blocking.
///
/// Note: checks only for Tokio runtimes, not other async runtimes.
///
/// # Error Handling
///
/// If this function returns `Error::NestedRuntimeError`, callers should:
/// - Use the async [`TransactionClient`](crate::TransactionClient) instead of `SyncTransactionClient`
/// - Move the `SyncTransactionClient` creation and usage outside of the async context
/// - Consider restructuring the code to avoid mixing sync and async execution contexts
pub(crate) fn check_nested_runtime() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::NestedRuntimeError(String::new()));
    }
    Ok(())
}

/// Run a `Result`-returning future on the given Tokio runtime with nested-runtime detection.
///
/// This is a thin wrapper around [`tokio::runtime::Runtime::block_on`] that first checks
/// whether a Tokio runtime is already active in the current context. If a nested runtime
/// is detected, it returns [`Error::NestedRuntimeError`] instead of attempting to block,
/// which helps prevent potential deadlocks and provides clearer error messages when
/// `block_on` is misused from within an existing async runtime.
///
/// # Returns
///
/// - `Ok(T)` with the successful result produced by the provided future when no nested
///   runtime is detected and the future completes successfully.
/// - `Err(Error::NestedRuntimeError)` if a Tokio runtime is already active on the current
///   thread when this function is called.
/// - `Err(e)` for any other [`Error`] produced either by the future itself or by
///   `runtime.block_on`.
pub(crate) fn safe_block_on<F, T>(runtime: &tokio::runtime::Runtime, future: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    check_nested_runtime()?;
    runtime.block_on(future)
}

/// Synchronous TiKV transactional client.
///
/// This is a synchronous wrapper around the async [`TransactionClient`](crate::TransactionClient).
/// All methods block the current thread until completion.
///
/// For async operations, use [`TransactionClient`](crate::TransactionClient) instead.
pub struct SyncTransactionClient {
    client: Client,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl SyncTransactionClient {
    /// Create a synchronous transactional [`SyncTransactionClient`] and connect to the TiKV cluster.
    ///
    /// See usage example in the documentation of [`TransactionClient::new`](crate::TransactionClient::new).
    pub fn new<S: Into<String>>(pd_endpoints: Vec<S>) -> Result<Self> {
        Self::new_with_config(pd_endpoints, Config::default())
    }

    /// Create a synchronous transactional [`SyncTransactionClient`] with a custom configuration.
    ///
    /// See usage example in the documentation of [`TransactionClient::new_with_config`](crate::TransactionClient::new_with_config).
    pub fn new_with_config<S: Into<String>>(pd_endpoints: Vec<S>, config: Config) -> Result<Self> {
        check_nested_runtime()?;

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        );
        let client = runtime.block_on(Client::new_with_config(pd_endpoints, config))?;
        Ok(Self { client, runtime })
    }

    /// Creates a new optimistic [`SyncTransaction`].
    ///
    /// Use the transaction to issue requests like [`get`](SyncTransaction::get) or
    /// [`put`](SyncTransaction::put).
    ///
    /// This is a synchronous version of [`TransactionClient::begin_optimistic`](crate::TransactionClient::begin_optimistic).
    pub fn begin_optimistic(&self) -> Result<SyncTransaction> {
        let inner = safe_block_on(&self.runtime, self.client.begin_optimistic())?;
        Ok(SyncTransaction::new(inner, Arc::clone(&self.runtime)))
    }

    /// Creates a new pessimistic [`SyncTransaction`].
    ///
    /// This is a synchronous version of [`TransactionClient::begin_pessimistic`](crate::TransactionClient::begin_pessimistic).
    pub fn begin_pessimistic(&self) -> Result<SyncTransaction> {
        let inner = safe_block_on(&self.runtime, self.client.begin_pessimistic())?;
        Ok(SyncTransaction::new(inner, Arc::clone(&self.runtime)))
    }

    /// Create a new customized [`SyncTransaction`].
    ///
    /// This is a synchronous version of [`TransactionClient::begin_with_options`](crate::TransactionClient::begin_with_options).
    pub fn begin_with_options(&self, options: TransactionOptions) -> Result<SyncTransaction> {
        let inner = safe_block_on(&self.runtime, self.client.begin_with_options(options))?;
        Ok(SyncTransaction::new(inner, Arc::clone(&self.runtime)))
    }

    /// Create a new read-only [`SyncSnapshot`] at the given [`Timestamp`].
    ///
    /// This is a synchronous version of [`TransactionClient::snapshot`](crate::TransactionClient::snapshot).
    pub fn snapshot(&self, timestamp: Timestamp, options: TransactionOptions) -> SyncSnapshot {
        let inner = self.client.snapshot(timestamp, options);
        SyncSnapshot::new(inner, Arc::clone(&self.runtime))
    }

    /// Retrieve the current [`Timestamp`].
    ///
    /// This is a synchronous version of [`TransactionClient::current_timestamp`](crate::TransactionClient::current_timestamp).
    pub fn current_timestamp(&self) -> Result<Timestamp> {
        safe_block_on(&self.runtime, self.client.current_timestamp())
    }

    /// Request garbage collection (GC) of the TiKV cluster.
    ///
    /// This is a synchronous version of [`TransactionClient::gc`](crate::TransactionClient::gc).
    pub fn gc(&self, safepoint: Timestamp) -> Result<bool> {
        safe_block_on(&self.runtime, self.client.gc(safepoint))
    }

    /// Clean up all locks in the specified range.
    ///
    /// This is a synchronous version of [`TransactionClient::cleanup_locks`](crate::TransactionClient::cleanup_locks).
    pub fn cleanup_locks(
        &self,
        range: impl Into<BoundRange>,
        safepoint: &Timestamp,
        options: ResolveLocksOptions,
    ) -> Result<CleanupLocksResult> {
        safe_block_on(
            &self.runtime,
            self.client.cleanup_locks(range, safepoint, options),
        )
    }

    /// Cleans up all keys in a range and quickly reclaim disk space.
    ///
    /// The range can span over multiple regions.
    ///
    /// Note that the request will directly delete data from RocksDB, and all MVCC will be erased.
    ///
    /// This interface is intended for special scenarios that resemble operations like "drop table" or "drop database" in TiDB.
    ///
    /// This is a synchronous version of [`TransactionClient::unsafe_destroy_range`](crate::TransactionClient::unsafe_destroy_range).
    pub fn unsafe_destroy_range(&self, range: impl Into<BoundRange>) -> Result<()> {
        safe_block_on(&self.runtime, self.client.unsafe_destroy_range(range))
    }

    /// Scan all locks in the specified range.
    ///
    /// This is only available for integration tests.
    ///
    /// Note: `batch_size` must be >= expected number of locks.
    ///
    /// This is a synchronous version of [`TransactionClient::scan_locks`](crate::TransactionClient::scan_locks).
    #[cfg(feature = "integration-tests")]
    pub fn scan_locks(
        &self,
        safepoint: &Timestamp,
        range: impl Into<BoundRange>,
        batch_size: u32,
    ) -> Result<Vec<crate::proto::kvrpcpb::LockInfo>> {
        safe_block_on(
            &self.runtime,
            self.client.scan_locks(safepoint, range, batch_size),
        )
    }
}

impl Clone for SyncTransactionClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            runtime: Arc::clone(&self.runtime),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_nested_runtime_outside_async() {
        // When called outside an async context, should return Ok(())
        let result = check_nested_runtime();
        assert!(
            result.is_ok(),
            "check_nested_runtime should succeed outside async context"
        );
    }

    #[test]
    fn test_check_nested_runtime_inside_async() {
        // When called inside an async context, should return Err
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = check_nested_runtime();
            assert!(
                result.is_err(),
                "check_nested_runtime should fail inside async context"
            );

            // Verify the error type is correct
            match result.unwrap_err() {
                Error::NestedRuntimeError(_) => {
                    // Expected case - test passes
                }
                other => panic!("Expected NestedRuntimeError, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_safe_block_on_outside_async() {
        // safe_block_on should work when called outside an async context
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = safe_block_on(&rt, async { Ok::<_, Error>(42) });
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_safe_block_on_inside_async() {
        // safe_block_on should fail when called inside an async context
        let outer_rt = tokio::runtime::Runtime::new().unwrap();

        // Create the inner runtime OUTSIDE the async context
        let inner_rt = tokio::runtime::Runtime::new().unwrap();

        outer_rt.block_on(async {
            let result = safe_block_on(&inner_rt, async { Ok::<_, Error>(42) });

            assert!(
                result.is_err(),
                "safe_block_on should fail inside async context"
            );

            // Verify the error type is correct
            match result.unwrap_err() {
                Error::NestedRuntimeError(_) => {
                    // Expected case - test passes
                }
                other => panic!("Expected NestedRuntimeError, got: {:?}", other),
            }
        });
    }

    #[test]
    fn test_nested_runtime_error_matching() {
        // Verify that NestedRuntimeError can be matched on programmatically
        let outer_rt = tokio::runtime::Runtime::new().unwrap();

        outer_rt.block_on(async {
            let result = check_nested_runtime();

            assert!(result.is_err());

            // Demonstrate type-safe error matching
            match result.unwrap_err() {
                Error::NestedRuntimeError(_) => {
                    // Expected case - test passes
                }
                other => panic!("Expected NestedRuntimeError, got: {:?}", other),
            }
        });
    }
}
