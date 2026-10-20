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

//! The read path's GC safe-point check as TiDB sees it: client-go's
//! `KVStore.CheckVisibility`, answered by the node's store (client-rust's
//! `KvStore`, which keeps the txn safe point current as client-go's
//! `runTxnSafePointUpdater` does), and its two failures carried to SQL as
//! `pkg/store/driver/error` maps them.
//!
//! # Why the check is after the read, not before it
//!
//! client-go calls `CheckVisibility` *after* a Get or Scan returns
//! (`txnkv/txnsnapshot/snapshot.go:325,773` and `scan.go:287`), not before the
//! snapshot is opened. Checking first would prove nothing: GC can advance while
//! the RPC is in flight. Checking after means every value handed to the caller
//! was either read above the safe point known at that moment, or rejected.
//! Callers must not "optimize" it into a pre-check.

use std::fmt;
use std::sync::Arc;

use crate::StorageDriverError;

/// client-go's `ErrPDServerTimeout` text for a txn safe point too old to
/// prove anything.
pub const STALE_GC_STATE_CACHE_MESSAGE: &str = "start timestamp may fall behind safe point";

/// Why a `start_ts` may not be used to hand data back to a caller.
///
/// Both variants are terminal for the transaction that raised them: retrying
/// the same `start_ts` can only fail again, because a safe point never moves
/// backwards.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VisibilityError {
    /// The cached txn safe point is too old to prove anything.
    StaleGcStateCache,
    /// GC has advanced past this transaction's `start_ts`.
    TxnAbortedByGc {
        /// The transaction's start timestamp.
        txn_start_ts: u64,
        /// The txn safe point that overtook it.
        txn_safe_point: u64,
    },
}

impl fmt::Display for VisibilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StaleGcStateCache => formatter.write_str(STALE_GC_STATE_CACHE_MESSAGE),
            Self::TxnAbortedByGc {
                txn_start_ts,
                txn_safe_point,
            } => write!(
                formatter,
                // Kept verbatim from client-go's `ErrTxnAbortedByGC.Error()`:
                // the wording is inaccurate (the usual cause is a
                // long-running transaction, not a short GC life time) but it
                // is what operators and diagnostic tooling already match on.
                "GC life time is shorter than transaction duration, transaction start ts is {txn_start_ts} ({}), txn safe point is {txn_safe_point} ({})",
                get_time_from_ts_string(*txn_start_ts),
                get_time_from_ts_string(*txn_safe_point),
            ),
        }
    }
}

impl std::error::Error for VisibilityError {}

/// client-go's `TxnStartTSTime` / `TxnSafePointTime` as `%v` prints them:
/// `oracle.GetTimeFromTS(ts)` is a `time.Local` value, so `time.Time.String()`
/// renders it in the process's zone with the zone's abbreviation.
fn get_time_from_ts_string(ts: u64) -> String {
    tidb_util::go_time_layout::go_unix_milli_string((ts >> 18) as i64)
}

impl VisibilityError {
    /// Classifies this failure into the storage-driver tier that carries it to
    /// SQL, mirroring `pkg/store/driver/error/error.go`.
    ///
    /// The times client-go stores in `ErrTxnAbortedByGC` are
    /// `oracle.GetTimeFromTS` of the two timestamps, so they are derived from
    /// the timestamps here, in the process's local zone as Go renders them.
    #[must_use]
    pub fn into_storage_driver_error(self) -> StorageDriverError {
        match self {
            Self::StaleGcStateCache => StorageDriverError::PdServerTimeout {
                message: STALE_GC_STATE_CACHE_MESSAGE.to_owned(),
            },
            Self::TxnAbortedByGc {
                txn_start_ts,
                txn_safe_point,
            } => StorageDriverError::TxnAbortedByGc {
                txn_start_ts,
                txn_start_ts_time: get_time_from_ts_string(txn_start_ts),
                txn_safe_point,
                txn_safe_point_time: get_time_from_ts_string(txn_safe_point),
            },
        }
    }

    /// As [`Self::into_storage_driver_error`], with wall-clock renderings of
    /// the two timestamps supplied by the caller's oracle.
    #[must_use]
    pub fn into_storage_driver_error_with_times(
        self,
        txn_start_ts_time: String,
        txn_safe_point_time: String,
    ) -> StorageDriverError {
        match self {
            Self::StaleGcStateCache => self.into_storage_driver_error(),
            Self::TxnAbortedByGc {
                txn_start_ts,
                txn_safe_point,
            } => StorageDriverError::TxnAbortedByGc {
                txn_start_ts,
                txn_start_ts_time,
                txn_safe_point,
                txn_safe_point_time,
            },
        }
    }
}

impl VisibilityError {
    /// `CheckVisibility`'s two failures as client-rust's store raises them:
    /// `ErrTxnAbortedByGC` and `ErrPDServerTimeout`. A store that answers
    /// anything else has proved nothing, which is the stale-cache answer.
    #[must_use]
    pub fn from_client_error(error: tikv_client::Error) -> Self {
        match error {
            tikv_client::Error::TransactionAbortedByGc(aborted) => Self::TxnAbortedByGc {
                txn_start_ts: aborted.transaction_start_timestamp,
                txn_safe_point: aborted.transaction_safe_point,
            },
            _ => Self::StaleGcStateCache,
        }
    }
}

/// client-go `KVStore.CheckVisibility` as the transactions opened over a store
/// hold it: `Ok` while a start ts is above the store's txn safe point. It
/// keeps the store, and so its safe-point updater, alive.
pub type StoreVisibility = Arc<dyn Fn(u64) -> Result<(), VisibilityError> + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    /// client-rust's store raises client-go's two `CheckVisibility` errors;
    /// each keeps its own tier.
    #[test]
    fn client_store_failures_become_the_driver_tiers() {
        let cache = tikv_client::tikv::TxnSafePointCache::new(450, std::time::SystemTime::now());
        let aborted = cache.check_visibility(449).unwrap_err();
        assert_eq!(
            VisibilityError::from_client_error(aborted),
            VisibilityError::TxnAbortedByGc {
                txn_start_ts: 449,
                txn_safe_point: 450,
            }
        );
        let stale = cache
            .check_visibility_at(
                u64::MAX,
                std::time::SystemTime::now() + std::time::Duration::from_secs(100),
            )
            .unwrap_err();
        assert_eq!(
            VisibilityError::from_client_error(stale),
            VisibilityError::StaleGcStateCache
        );
    }

    #[test]
    fn visibility_failures_classify_into_distinct_driver_tiers() {
        assert_eq!(
            VisibilityError::StaleGcStateCache.into_storage_driver_error(),
            StorageDriverError::PdServerTimeout {
                message: "start timestamp may fall behind safe point".to_owned()
            }
        );
        assert_eq!(
            VisibilityError::TxnAbortedByGc {
                txn_start_ts: 99,
                txn_safe_point: 100,
            }
            .into_storage_driver_error_with_times("t1".to_owned(), "t2".to_owned()),
            StorageDriverError::TxnAbortedByGc {
                txn_start_ts: 99,
                txn_start_ts_time: "t1".to_owned(),
                txn_safe_point: 100,
                txn_safe_point_time: "t2".to_owned(),
            }
        );
    }

    #[test]
    fn the_abort_message_keeps_client_gos_wording() {
        let rendered = VisibilityError::TxnAbortedByGc {
            txn_start_ts: 449,
            txn_safe_point: 450,
        }
        .to_string();
        assert!(
            rendered.starts_with("GC life time is shorter than transaction duration,"),
            "{rendered}"
        );
    }

    /// Go `ToTiDBErr` -> `ErrTxnAbortedByGC` (9006) with client-go's
    /// `TxnStartTSTime` / `TxnSafePointTime`: `oracle.GetTimeFromTS` of each
    /// timestamp, printed by `time.Time.String()` in the local zone. The SQL
    /// message and client-go's own `Error()` text agree.
    #[test]
    fn the_sql_error_carries_gos_local_times() {
        // `AS OF TIMESTAMP '2000-01-01 00:00:00'` under a Pacific-time Go
        // server, and that cluster's txn safe point.
        let (start_ts, safe_point) = (248_175_289_958_400_000, 469_896_279_291_330_560);
        let start_time = tidb_util::go_time_layout::go_unix_milli_string(946_713_600_000);
        let safe_point_time = tidb_util::go_time_layout::go_unix_milli_string(1_792_512_051_740);
        let expected = format!(
            "GC life time is shorter than transaction duration, transaction start ts is {start_ts} ({start_time}), txn safe point is {safe_point} ({safe_point_time})"
        );
        let error = VisibilityError::TxnAbortedByGc {
            txn_start_ts: start_ts,
            txn_safe_point: safe_point,
        };
        assert_eq!(error.to_string(), expected);
        let crate::ConvertedDriverError::Terror(converted) =
            crate::to_tidb_driver_error(&error.into_storage_driver_error())
        else {
            panic!("a GC-aborted read must use the TiKV error catalog")
        };
        assert_eq!(converted.code().value(), 9006);
        assert_eq!(converted.message(), expected);
    }
}
