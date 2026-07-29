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

//! Real-TiKV proof that two transactions serialize on one pessimistic lock.
//!
//! The scripted suite proves how the client reacts to TiKV's answers. Only a
//! real cluster proves the answers themselves: that TiKV actually withholds a
//! contended key from a second transaction until the first releases it, and
//! that the released key is then immediately lockable.

use std::collections::BTreeSet;
use std::time::Duration;

use tidb_pd_client::PdClient;
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    LockWaitTime, OptimisticMutation, OptimisticTransactionState, PessimisticLockFailure,
    RealOptimisticTransactionOpener,
};
use tidb_txnkv::{PdRegionLoader, SharedReadAuthority};

const RPC_TIMEOUT: Duration = Duration::from_secs(20);
/// Distinct per-run prefix so a reused cluster cannot leak state between runs.
const CONTENDED_KEY: &[u8] = b"pessimistic-lock-proof-contended";
const UNCONTENDED_KEY: &[u8] = b"pessimistic-lock-proof-uncontended";
/// Key the fair-locking proof contends on, distinct from every other one here.
const FAIR_LOCK_KEY: &[u8] = b"pessimistic-lock-proof-fair-locking";

fn call() -> UnaryCallContext {
    UnaryCallContext::with_timeout(RPC_TIMEOUT)
}

fn no_presumption() -> BTreeSet<Vec<u8>> {
    BTreeSet::new()
}

#[test]
#[ignore = "requires run-realtikv-pessimistic-lock.sh"]
fn two_transactions_serialize_on_one_real_pessimistic_lock() {
    let pd_address = std::env::var("PESSIMISTIC_LOCK_PD_ADDR")
        .expect("runner must provide PESSIMISTIC_LOCK_PD_ADDR");
    let pd_owner = PdClient::connect_seeds([pd_address], Duration::from_secs(10))
        .expect("start sole real PD authority");
    let cluster_id = pd_owner.cluster_id();
    assert_ne!(cluster_id, 0);
    let loader = PdRegionLoader::from_client(pd_owner.clone());
    let transport_owner =
        TonicCoprocessorClient::new().expect("start sole real BatchCommands authority");
    let shared = SharedReadAuthority::start_with_store_liveness(
        transport_owner.clone(),
        RegionCache::new(loader),
    )
    .expect("start sole real RegionCache authority");
    assert_eq!(shared.cluster_id(), cluster_id);
    let opener = RealOptimisticTransactionOpener::from_process_capabilities(
        shared.opener(),
        pd_owner.clone(),
        RPC_TIMEOUT,
    )
    .expect("derive transaction opener without starting another authority");

    // Holder locks the contended key at its own for_update_ts.
    let mut holder = opener
        .begin_pessimistic(4, 4 * 1024)
        .expect("open the holding pessimistic transaction");
    let held = holder
        .acquire_locks(
            &[CONTENDED_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("an uncontended key is lockable");
    assert_eq!(held.keys, vec![CONTENDED_KEY.to_vec()]);
    assert_eq!(held.primary_key, CONTENDED_KEY.to_vec());
    println!(
        "pessimistic_lock_realtikv phase=held start_ts={} for_update_ts={}",
        holder.start_ts(),
        held.for_update_ts
    );

    // The primary is locked, so its TTL must now be refreshed for as long as
    // the transaction lives. A short tick makes the proof finish in a test's
    // lifetime; production uses half the managed TTL.
    let keep_alive = opener
        .start_lock_keep_alive_with_tick(
            held.primary_key.clone(),
            holder.start_ts(),
            Duration::from_millis(200),
        )
        .expect("the keep-alive thread opens its own session");

    // A second transaction must not be able to take the same key. `NOWAIT`
    // makes TiKV answer rather than queue, so the proof needs no timing
    // assumption: the failure itself is the evidence the lock is real.
    let mut waiter = opener
        .begin_pessimistic(4, 4 * 1024)
        .expect("open the blocked pessimistic transaction");
    assert_ne!(waiter.start_ts(), holder.start_ts());
    let blocked = waiter
        .acquire_locks(
            &[CONTENDED_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::NoWait,
            &call(),
        )
        .expect_err("a key locked by a live transaction cannot be taken");
    match &blocked {
        PessimisticLockFailure::LockAcquireFailAndNoWaitSet { key } => {
            assert_eq!(key, CONTENDED_KEY);
        }
        other => panic!("expected a NOWAIT lock failure from real TiKV, got {other:?}"),
    }
    assert!(
        blocked.is_statement_scoped(),
        "contention must cost the statement, not the transaction"
    );
    assert!(
        waiter.locked_keys().is_empty(),
        "a refused lock must not be claimed at Prewrite"
    );
    println!(
        "pessimistic_lock_realtikv phase=blocked waiter_start_ts={} blocker_start_ts={}",
        waiter.start_ts(),
        holder.start_ts()
    );

    // A key the holder never touched stays available: the lock is on the key,
    // not on the region or the store.
    waiter
        .acquire_locks(
            &[UNCONTENDED_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::NoWait,
            &call(),
        )
        .expect("an untouched key is unaffected by the contended one");

    // Give the keep-alive time for several real TxnHeartBeat round trips
    // before the lock is released.
    std::thread::sleep(Duration::from_millis(900));
    let keep_alive_report = keep_alive.close();
    assert!(
        keep_alive_report.confirmed_heart_beats >= 2,
        "TiKV must confirm repeated TxnHeartBeat on a real primary lock: {keep_alive_report:?}"
    );
    assert!(keep_alive_report.last_advised_ttl_ms >= 20_000);
    println!(
        "pessimistic_lock_realtikv phase=kept_alive heart_beats={} advised_ttl_ms={} stop={:?}",
        keep_alive_report.confirmed_heart_beats,
        keep_alive_report.last_advised_ttl_ms,
        keep_alive_report.stop
    );

    // The holder commits through the shared two-phase engine, which releases
    // the pessimistic lock by turning it into a committed version.
    let outcome = holder
        .commit(
            // The playground starts empty, so the locked row is genuinely new
            // and carries the not-exists assertion an INSERT would.
            vec![OptimisticMutation::insert(CONTENDED_KEY.to_vec(), b"held".to_vec()).unwrap()],
            &call(),
        )
        .expect("the holder reaches a terminal outcome");
    assert_eq!(
        outcome.state(),
        OptimisticTransactionState::Committed,
        "holder did not commit: {outcome:?}"
    );
    let commit_ts = outcome.receipt().commit_ts;
    assert!(commit_ts > outcome.receipt().start_ts);
    println!(
        "pessimistic_lock_realtikv phase=released commit_ts={commit_ts} primary_region={}",
        outcome
            .receipt()
            .region_attempts
            .first()
            .map_or(0, |region| region.id)
    );

    // The waiter's original for_update_ts predates the holder's commit, so
    // taking the key now must first advance to a statement timestamp that can
    // see the new version. This is the whole point of statement-level retry.
    let statement_ts = waiter
        .advance_for_update_ts()
        .expect("a retried statement gets a newer timestamp");
    assert!(statement_ts > commit_ts);
    let acquired = waiter
        .acquire_locks(
            &[CONTENDED_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::AlwaysWait,
            &call(),
        )
        .expect("the released key is lockable by the waiter");
    assert_eq!(acquired.for_update_ts, statement_ts);
    assert!(acquired.keys.contains(&CONTENDED_KEY.to_vec()));

    // Releasing without committing must leave nothing behind for the next run.
    waiter
        .pessimistic_rollback(&waiter.locked_keys(), &call())
        .expect("the waiter releases every lock it holds");
    assert!(waiter.locked_keys().is_empty());

    println!(
        "pessimistic_lock_realtikv status=passed cluster_id={cluster_id} \
         holder_commit_ts={commit_ts} waiter_for_update_ts={statement_ts} \
         contended_key={} ",
        String::from_utf8_lossy(CONTENDED_KEY)
    );
}

/// Real-TiKV proof that fair locking grants a lock despite a newer commit.
///
/// The scripted suite proves the client reads `LockResultLockedWithConflict`
/// correctly. Only a real cluster proves TiKV produces it: that a
/// `WakeUpModeForceLock` request whose `for_update_ts` predates a committed
/// version comes back with the lock *taken*, at that version's commit
/// timestamp, instead of the write conflict Normal mode would report.
#[test]
#[ignore = "requires run-realtikv-pessimistic-lock.sh"]
fn fair_locking_takes_the_lock_despite_a_newer_committed_version() {
    let pd_address = std::env::var("PESSIMISTIC_LOCK_PD_ADDR")
        .expect("runner must provide PESSIMISTIC_LOCK_PD_ADDR");
    let pd_owner = PdClient::connect_seeds([pd_address], Duration::from_secs(10))
        .expect("start sole real PD authority");
    let cluster_id = pd_owner.cluster_id();
    let loader = PdRegionLoader::from_client(pd_owner.clone());
    let transport_owner =
        TonicCoprocessorClient::new().expect("start sole real BatchCommands authority");
    let shared = SharedReadAuthority::start_with_store_liveness(
        transport_owner.clone(),
        RegionCache::new(loader),
    )
    .expect("start sole real RegionCache authority");
    let opener = RealOptimisticTransactionOpener::from_process_capabilities(
        shared.opener(),
        pd_owner.clone(),
        RPC_TIMEOUT,
    )
    .expect("derive transaction opener without starting another authority");

    // The fair-locking reader opens first, so its `for_update_ts` is older than
    // the commit that is about to land. That ordering is what makes TiKV take
    // the ForceLock branch at all.
    let mut reader = opener
        .begin_pessimistic(4, 4 * 1024)
        .expect("open the fair-locking pessimistic transaction");
    reader.set_fair_locking(true);
    let reader_for_update_ts = reader.for_update_ts();

    // A second transaction commits a new version of the very key the reader is
    // about to lock.
    let writer = opener
        .begin(4, 4 * 1024)
        .expect("open the writing optimistic transaction");
    let outcome = writer
        .commit(
            vec![OptimisticMutation::insert(FAIR_LOCK_KEY.to_vec(), b"newer".to_vec()).unwrap()],
            &call(),
        )
        .expect("the writer reaches a terminal outcome");
    assert_eq!(outcome.state(), OptimisticTransactionState::Committed);
    let commit_ts = outcome.receipt().commit_ts;
    assert!(commit_ts > reader_for_update_ts);
    println!(
        "fair_locking_realtikv phase=newer_version_committed commit_ts={commit_ts} \
         reader_for_update_ts={reader_for_update_ts}"
    );

    // One key, fair locking armed: this is exactly the shape that goes out in
    // WakeUpModeForceLock.
    let acquired = reader
        .acquire_locks(
            &[FAIR_LOCK_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::NoWait,
            &call(),
        )
        .expect("fair locking takes the lock instead of reporting a write conflict");
    assert_eq!(acquired.keys, vec![FAIR_LOCK_KEY.to_vec()]);
    assert_eq!(
        acquired.locked_with_conflict,
        vec![(FAIR_LOCK_KEY.to_vec(), commit_ts)],
        "TiKV must report the conflicting version's commit timestamp as the lock's own"
    );
    assert_eq!(
        reader.for_update_ts(),
        reader_for_update_ts,
        "the lock does not move the statement's timestamp; the retry allocates its own"
    );
    assert_eq!(reader.max_locked_with_conflict_ts(), commit_ts);
    println!(
        "fair_locking_realtikv phase=locked_with_conflict locked_ts={commit_ts} \
         requested_for_update_ts={reader_for_update_ts}"
    );

    // The lock really exists at the higher timestamp: releasing it at the
    // requested `for_update_ts` would silently leave it behind, so a NOWAIT
    // attempt by a third transaction after the rollback is the proof it went.
    reader
        .pessimistic_rollback(&[FAIR_LOCK_KEY.to_vec()], &call())
        .expect("the fair lock is released at the timestamp it really carries");
    let mut prober = opener
        .begin_pessimistic(4, 4 * 1024)
        .expect("open the probing pessimistic transaction");
    prober
        .acquire_locks(
            &[FAIR_LOCK_KEY.to_vec()],
            &no_presumption(),
            LockWaitTime::NoWait,
            &call(),
        )
        .expect("a released fair lock leaves the key immediately lockable");
    prober
        .pessimistic_rollback(&prober.locked_keys(), &call())
        .expect("the prober releases what it took");
    println!("fair_locking_realtikv phase=released_at_conflict_ts");

    println!(
        "fair_locking_realtikv status=passed cluster_id={cluster_id} \
         locked_with_conflict_ts={commit_ts} requested_for_update_ts={reader_for_update_ts}"
    );
}
