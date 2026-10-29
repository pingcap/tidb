// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! GC safe-point fidelity on the read path.
//!
//! Sources: client-go `tikv/kv.go` (`CheckVisibility`, answered here by
//! client-rust's store cache) and TiDB `pkg/store/driver/error/error.go`
//! (`ErrTxnAbortedByGC` mapping).

#![allow(missing_docs)]

use std::time::{Duration, SystemTime};

use tidb_terror::TerrorClass;
use tidb_errno as tidb;
use tidb_txnkv::transaction::OptimisticCoordinatorError;
use tidb_txnkv::{StorageDriverError, VisibilityError};
use tidb_store_driver_error::driver_error::to_tidb_driver_error;
use tidb_store_driver_error::driver_error::ConvertedDriverError;
use tikv_client::tikv::{TxnSafePointCache, GC_CPU_TIME_INACCURACY_BOUND, GC_STATE_CACHE_INTERVAL};

#[test]
fn a_read_below_the_safe_point_reaches_sql_as_its_own_error_code() {
    // The whole point of the classification: this must not arrive as a generic
    // transport or region failure that a caller would retry forever.
    let cache = TxnSafePointCache::new(450_000_000_000, SystemTime::now());
    let violation = VisibilityError::from_client_error(
        cache
            .check_visibility(440_000_000_000)
            .expect_err("a start_ts below the txn safe point is not readable"),
    );
    assert_eq!(
        violation,
        VisibilityError::TxnAbortedByGc {
            txn_start_ts: 440_000_000_000,
            txn_safe_point: 450_000_000_000,
        }
    );

    let driver = violation.clone().into_storage_driver_error();
    assert!(matches!(driver, StorageDriverError::TxnAbortedByGc { .. }));
    let ConvertedDriverError::Terror(terror) = to_tidb_driver_error(&driver) else {
        panic!("a GC-aborted read must use the TiKV error catalog")
    };
    assert_eq!(terror.class(), TerrorClass::TiKv);
    assert_eq!(
        terror.code().value(),
        isize::try_from(tidb::errcode::ErrTxnAbortedByGC).unwrap()
    );

    // The coordinator tier keeps it separate from the catch-all read failure.
    let coordinator = OptimisticCoordinatorError::Visibility(violation);
    assert!(!matches!(
        coordinator,
        OptimisticCoordinatorError::SnapshotGet(_)
    ));
    assert!(coordinator
        .to_string()
        .starts_with("GC life time is shorter than transaction duration,"));
}

#[test]
fn a_stale_cache_is_a_pd_timeout_not_a_gc_abort() {
    // client-go returns ErrPDServerTimeout, not ErrTxnAbortedByGC, when it
    // cannot prove either way: the two are diagnosed differently.
    let seeded_at = SystemTime::now();
    let cache = TxnSafePointCache::new(450, seeded_at);
    let violation = VisibilityError::from_client_error(
        cache
            .check_visibility_at(
                u64::MAX,
                seeded_at + GC_STATE_CACHE_INTERVAL - GC_CPU_TIME_INACCURACY_BOUND
                    + Duration::from_secs(1),
            )
            .expect_err("a cache older than the usable window proves nothing"),
    );
    assert_eq!(violation, VisibilityError::StaleGcStateCache);
    assert_eq!(
        violation.into_storage_driver_error(),
        StorageDriverError::PdServerTimeout {
            message: "start timestamp may fall behind safe point".to_owned()
        }
    );
}
