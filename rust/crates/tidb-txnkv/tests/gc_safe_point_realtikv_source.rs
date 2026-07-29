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

//! Live proof that a read pinned below the cluster's txn safe point fails with
//! the GC classification instead of hanging or reporting a generic error.
//!
//! Run against a playground:
//!
//! ```text
//! GC_SAFE_POINT_PD_ADDR=127.0.0.1:2379 \
//!   cargo test -p tidb-txnkv --test all -- --ignored \
//!   gc_safe_point_realtikv_source
//! ```
//!
//! The proof does not advance the cluster's real safe point — advancing it is
//! the GC owner's act, and doing it here could destroy data another test is
//! reading. It instead reads PD's real GC state, seeds a cache with a safe
//! point above a fabricated `start_ts`, and requires the *same* classification
//! path a real overtaken read would take.

use std::time::{Duration, Instant};

use tidb_pd_client::PdClient;
use tidb_txnkv::{GcStateCache, StorageDriverError, VisibilityError};

#[test]
#[ignore = "requires a running playground with GC_SAFE_POINT_PD_ADDR set"]
fn a_start_ts_below_the_clusters_txn_safe_point_is_classified_not_hung() {
    let pd_address = std::env::var("GC_SAFE_POINT_PD_ADDR")
        .expect("runner must provide GC_SAFE_POINT_PD_ADDR");
    let pd = PdClient::connect_seeds([pd_address], Duration::from_secs(10))
        .expect("start the sole real PD authority");
    assert_ne!(pd.cluster_id(), 0);

    // The null keyspace: what a non-keyspace deployment reads under.
    let live = pd
        .get_gc_state(None)
        .expect("a supported PD answers GetGCState; an older one must answer Unimplemented");
    assert!(
        !live.is_keyspace_level_gc,
        "a default playground is not keyspace-level GC"
    );

    // A safe point the cluster has genuinely reached still admits any newer
    // timestamp, so an ordinary read is unaffected by the check being present.
    let cache = GcStateCache::seeded(live.txn_safe_point, Instant::now());
    let now_ts = pd.get_timestamp().expect("allocate a real timestamp");
    assert!(
        cache.check_visibility(now_ts).is_ok(),
        "a fresh timestamp must be above the live txn safe point {}",
        live.txn_safe_point
    );

    // A timestamp older than the safe point is the failure this unit exists
    // for: terminal, named, and never a hang.
    let overtaken = live.txn_safe_point.saturating_sub(1);
    let error = cache
        .check_visibility(overtaken)
        .expect_err("a start_ts below the live txn safe point is unreadable");
    assert_eq!(
        error,
        VisibilityError::TxnAbortedByGc {
            txn_start_ts: overtaken,
            txn_safe_point: live.txn_safe_point,
        }
    );
    assert!(matches!(
        error.into_storage_driver_error(),
        StorageDriverError::TxnAbortedByGc { .. }
    ));
}
