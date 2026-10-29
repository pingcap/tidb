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

//! Real-TiKV recovery and safety checks for locks blocking Prewrite.
//!
//! An expired pessimistic lock must yield to the writer; a live owner's lock
//! must survive the contender's deadline and still permit the owner to commit.
//! An orphaned optimistic secondary must become readable and writable again.
//! Run with `scripts/run-realtikv-pessimistic-prewrite-recovery.sh`, or provide
//! `PESSIMISTIC_PREWRITE_RECOVERY_PD_ADDR` for an existing test cluster.

use std::time::Duration;

use tidb_proto::{
    KvrpcAssertionLevel, KvrpcCommitRequest, KvrpcCommitResponse, KvrpcCommitRole, KvrpcMutation,
    KvrpcOp, KvrpcPessimisticAction, KvrpcPessimisticLockRequest, KvrpcPessimisticLockResponse,
    KvrpcPrewriteRequest, KvrpcPrewriteResponse,
};
use tidb_txnkv::driver::tikv_store::TikvStore;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{BufferMutation, OptimisticCommitOutcome, TransactionCause};
use tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener;

use crate::realtikv_fixture::{route, send, timestamp};

const RPC_TIMEOUT: Duration = Duration::from_secs(20);

/// Short enough that an abandoned lock expires inside a test's lifetime, long
/// enough that the fixture's own round trip cannot outlive it by accident.
const EXPIRING_LOCK_TTL_MS: u64 = 500;
/// Comfortably beyond any heartbeat this test could accidentally send.
const LIVE_LOCK_TTL_MS: u64 = 120_000;

/// One key per claim: a shared key would let one claim's rollback decide
/// another's outcome.
const EXPIRED_LOCK_KEY: &[u8] = b"pessimistic-prewrite-recovery-expired";
const LIVE_LOCK_KEY: &[u8] = b"pessimistic-prewrite-recovery-live";
/// The secondary an orphaned transaction prewrote before it died.
const ORPHAN_SECONDARY_KEY: &[u8] = b"pessimistic-prewrite-recovery-orphan-secondary";
/// The primary that same transaction never got to prewrite, so no lock and no
/// write record for it exists anywhere in the cluster.
const ORPHAN_PRIMARY_KEY: &[u8] = b"pessimistic-prewrite-recovery-orphan-primary";

/// Everything one claim needs: the node's store, which hands out timestamps
/// and carries the fixture's raw kvproto, and the opener the transaction
/// under test uses.
struct Cluster {
    store: TikvStore,
    opener: RealOptimisticTransactionOpener,
    key_prefix: Vec<u8>,
}

impl Cluster {
    fn key(&self, suffix: &[u8]) -> Vec<u8> {
        [self.key_prefix.as_slice(), suffix].concat()
    }
}

fn call() -> UnaryCallContext {
    UnaryCallContext::with_timeout(RPC_TIMEOUT)
}

fn connect() -> Cluster {
    let pd_address = std::env::var("PESSIMISTIC_PREWRITE_RECOVERY_PD_ADDR")
        .expect("runner must provide PESSIMISTIC_PREWRITE_RECOVERY_PD_ADDR");
    let store = crate::realtikv_fixture::connect(&pd_address);
    assert_ne!(store.cluster_id(), 0);
    let opener =
        RealOptimisticTransactionOpener::from_process_capabilities(store.clone(), RPC_TIMEOUT)
            .expect("derive the transaction opener over the store");
    // A fresh timestamp isolates repeated runs without deleting earlier
    // fixtures or requiring a new cluster for each invocation.
    let key_prefix = format!("recovery-test-{}-", timestamp(&store)).into_bytes();
    Cluster {
        store,
        opener,
        key_prefix,
    }
}

/// Leaves a real pessimistic lock on `key`, the way a Go tidb-server's
/// pessimistic DML does: `Op::PessimisticLock`, the key as its own primary, and
/// a TTL the caller chooses so expiry is a decision rather than a race.
fn hold_real_pessimistic_lock(store: &TikvStore, key: &[u8], start_ts: u64, ttl_ms: u64) {
    let (address, context) = route(store, key);
    let request = KvrpcPessimisticLockRequest {
        context: Some(context.clone()),
        mutations: vec![KvrpcMutation {
            op: KvrpcOp::PessimisticLock as i32,
            key: key.to_vec(),
            ..KvrpcMutation::default()
        }],
        primary_lock: key.to_vec(),
        start_version: start_ts,
        lock_ttl: ttl_ms,
        for_update_ts: start_ts,
        is_first_lock: true,
        // Negative means "do not queue": the fixture wants TiKV's answer, not
        // a wait.
        wait_timeout: -1,
        ..KvrpcPessimisticLockRequest::default()
    };
    let response: KvrpcPessimisticLockResponse = send(store, &address, request, RPC_TIMEOUT);
    assert!(response.region_error.is_none());
    assert!(
        response.errors.is_empty(),
        "the fixture key must be lockable: {:?}",
        response.errors
    );
}

/// Completes the fixture transaction the way its owner would: a Prewrite that
/// tells TiKV the pessimistic lock is already held, then a Commit.
///
/// Used only by the LIVE-lock claim, and it is the whole assertion there: it
/// can only succeed if the lock this test's Prewrite met was left alone.
fn commit_real_pessimistic_txn(
    store: &TikvStore,
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    commit_ts: u64,
) {
    let (address, context) = route(store, key);
    let prewrite = KvrpcPrewriteRequest {
        context: Some(context.clone()),
        mutations: vec![KvrpcMutation {
            op: KvrpcOp::Put as i32,
            key: key.to_vec(),
            value: value.to_vec(),
            ..KvrpcMutation::default()
        }],
        primary_lock: key.to_vec(),
        start_version: start_ts,
        lock_ttl: LIVE_LOCK_TTL_MS,
        pessimistic_actions: vec![KvrpcPessimisticAction::DoPessimisticCheck as i32],
        for_update_ts: start_ts,
        txn_size: 1,
        assertion_level: KvrpcAssertionLevel::Off as i32,
        ..KvrpcPrewriteRequest::default()
    };
    let response: KvrpcPrewriteResponse = send(store, &address, prewrite, RPC_TIMEOUT);
    assert!(response.region_error.is_none());
    assert!(
        response.errors.is_empty(),
        "the live lock's owner must still be able to prewrite on it -- a \
         PessimisticLockNotFound here IS the disaster this claim exists to \
         rule out: {:?}",
        response.errors
    );

    let commit = KvrpcCommitRequest {
        context: Some(context.clone()),
        start_version: start_ts,
        keys: vec![key.to_vec()],
        commit_version: commit_ts,
        commit_role: KvrpcCommitRole::Primary as i32,
        primary_key: key.to_vec(),
        ..KvrpcCommitRequest::default()
    };
    let response: KvrpcCommitResponse = send(store, &address, commit, RPC_TIMEOUT);
    assert!(response.region_error.is_none());
    assert!(response.error.is_none());
}

/// Reads a key back through a transaction newer than every commit so far, so
/// the value observed is what TiKV durably holds.
fn read_back(opener: &RealOptimisticTransactionOpener, key: &[u8]) -> Option<Vec<u8>> {
    let mut transaction = opener.begin().expect("allocate a readback snapshot");
    let observed = transaction
        .snapshot_get(key, &call())
        .expect("read the key back through real BatchCommands")
        .value;
    transaction
        .finish_without_writes()
        .expect("finish the readback without writes");
    observed
}

/// Claim 1: an EXPIRED pessimistic lock is resolved and the writer commits.
#[test]
#[ignore = "requires a real TiKV cluster"]
fn an_expired_pessimistic_lock_is_resolved_and_the_writer_commits() {
    let cluster = connect();
    let key = cluster.key(EXPIRED_LOCK_KEY);

    let lock_start_ts = timestamp(&cluster.store);
    hold_real_pessimistic_lock(&cluster.store, &key, lock_start_ts, EXPIRING_LOCK_TTL_MS);
    println!(
        "pessimistic_prewrite_recovery phase=locked key=expired lock_start_ts={lock_start_ts}"
    );

    // The owner is now gone: nothing refreshes this lock, so TiKV will report
    // it expired to the first CheckTxnStatus that asks after the TTL.
    std::thread::sleep(Duration::from_millis(EXPIRING_LOCK_TTL_MS * 3));

    let writer = cluster
        .opener
        .begin()
        .expect("allocate a writer newer than the abandoned lock");
    assert!(writer.start_ts() > lock_start_ts);
    let writer_start_ts = writer.start_ts();
    let outcome = writer
        .commit(
            vec![BufferMutation::set_with_flags(
                key.clone(),
                b"resolved-writer".to_vec(),
                true,
                tidb_txnkv::AssertionOp::AssertNotExist,
            )
            .unwrap()],
            &call(),
        )
        .expect("the Prewrite must reach a verdict, not a transport failure");
    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!("an expired pessimistic lock must not stop an optimistic writer: {outcome:?}");
    };
    assert_eq!(
        read_back(&cluster.opener, &key).as_deref(),
        Some(b"resolved-writer".as_slice()),
        "the writer's value must be what TiKV durably holds"
    );
    println!(
        "pessimistic_prewrite_recovery status=passed claim=expired_resolved \
         cluster_id={} lock_start_ts={lock_start_ts} writer_start_ts={writer_start_ts} \
         commit_ts={}",
        cluster.store.cluster_id(),
        committed.receipt.commit_ts,
    );
}

/// Claim 2, the safety half: a LIVE pessimistic lock is refused, not resolved,
/// and its owner still commits its own value afterwards.
///
/// A resolver that wrongly rolled the
/// lock back would look exactly like a correct one from the caller's side, and
/// only the owner's later Prewrite -- which would come back
/// `PessimisticLockNotFound` -- tells the two apart.
#[test]
#[ignore = "requires a real TiKV cluster"]
fn a_live_pessimistic_lock_survives_the_prewrite_and_still_commits() {
    let cluster = connect();
    let key = cluster.key(LIVE_LOCK_KEY);

    let lock_start_ts = timestamp(&cluster.store);
    hold_real_pessimistic_lock(&cluster.store, &key, lock_start_ts, LIVE_LOCK_TTL_MS);
    println!("pessimistic_prewrite_recovery phase=locked key=live lock_start_ts={lock_start_ts}");

    let writer = cluster
        .opener
        .begin()
        .expect("allocate a writer newer than the live lock");
    assert!(writer.start_ts() > lock_start_ts);
    let writer_start_ts = writer.start_ts();
    let wait_started = std::time::Instant::now();
    let writer_call = UnaryCallContext::with_timeout(Duration::from_secs(5));
    let outcome = writer
        .commit(
            vec![BufferMutation::set_with_flags(
                key.clone(),
                b"must-not-win".to_vec(),
                true,
                tidb_txnkv::AssertionOp::AssertNotExist,
            )
            .unwrap()],
            &writer_call,
        )
        .expect("the Prewrite must reach a verdict, not a transport failure");
    let OptimisticCommitOutcome::RolledBack(refused) = outcome else {
        panic!(
            "a LIVE pessimistic lock must stop the writer; committing here means \
             the resolver rolled a live transaction back: {outcome:?}"
        );
    };
    assert!(
        matches!(refused.cause, TransactionCause::Lock { .. }),
        "the writer must lose to the live lock, not to something else: {:?}",
        refused.cause
    );
    assert!(
        wait_started.elapsed() >= Duration::from_millis(4_500),
        "a live lock waits for the deadline, not a fixed number of attempts: {:?}",
        wait_started.elapsed()
    );
    println!(
        "pessimistic_prewrite_recovery phase=refused key=live writer_start_ts={writer_start_ts}"
    );

    // The owner finishes its transaction. This is the assertion: it can only
    // succeed if its pessimistic lock was still there.
    let commit_ts = timestamp(&cluster.store);
    assert!(commit_ts > lock_start_ts);
    commit_real_pessimistic_txn(
        &cluster.store,
        &key,
        b"live-holder",
        lock_start_ts,
        commit_ts,
    );
    assert_eq!(
        read_back(&cluster.opener, &key).as_deref(),
        Some(b"live-holder".as_slice()),
        "the live owner's value must survive: anything else means its work was \
         destroyed by the resolver"
    );
    println!(
        "pessimistic_prewrite_recovery status=passed claim=live_lock_survived \
         cluster_id={} lock_start_ts={lock_start_ts} writer_start_ts={writer_start_ts} \
         holder_commit_ts={commit_ts}",
        cluster.store.cluster_id(),
    );
}

/// Leaves the canonical orphan: an optimistic prewrite lock on a secondary key
/// naming a primary that was never prewritten.
///
/// This is what a coordinator that died between its secondary and primary
/// batches leaves behind, and it is the exact state that makes TiKV answer
/// CheckTxnStatus on the primary with `TxnNotFound`.
fn hold_orphan_secondary_lock(
    store: &TikvStore,
    primary: &[u8],
    secondary: &[u8],
    start_ts: u64,
    ttl_ms: u64,
) {
    let (address, context) = route(store, secondary);
    let request = KvrpcPrewriteRequest {
        context: Some(context.clone()),
        mutations: vec![KvrpcMutation {
            op: KvrpcOp::Put as i32,
            key: secondary.to_vec(),
            value: b"orphaned".to_vec(),
            ..KvrpcMutation::default()
        }],
        // The primary is a key this fixture deliberately never prewrites.
        primary_lock: primary.to_vec(),
        start_version: start_ts,
        lock_ttl: ttl_ms,
        txn_size: 2,
        assertion_level: KvrpcAssertionLevel::Off as i32,
        ..KvrpcPrewriteRequest::default()
    };
    let response: KvrpcPrewriteResponse = send(store, &address, request, RPC_TIMEOUT);
    assert!(response.region_error.is_none());
    assert!(
        response.errors.is_empty(),
        "the orphan fixture key must be prewritable: {:?}",
        response.errors
    );
}

/// The canonical orphan lock is recoverable, not permanent.
///
/// A secondary prewrite landed and the coordinator died before the primary, so
/// CheckTxnStatus on that primary answers `TxnNotFound` — there is no lock and
/// no write record to report. Go `getTxnStatusFromLock`
/// (`txnkv/txnlock/lock_resolver.go:928-980`) loops: once the lock is past its
/// TTL it re-asks with `rollback_if_not_exist`, TiKV writes the rollback
/// record, and the key becomes readable. Treating `TxnNotFound` as terminal
/// leaves the key unreadable and unwritable *forever*, because every later
/// reader repeats the identical failing query.
///
/// Only TiKV can prove this: the assertion is that TiKV, asked a second time
/// with `rollback_if_not_exist`, really does write the rollback record. No
/// scripted store can answer that, because scripting the answer is assuming it.
#[test]
#[ignore = "requires a real TiKV cluster"]
fn an_orphaned_secondary_prewrite_lock_is_recoverable_by_a_later_reader() {
    let cluster = connect();
    let primary = cluster.key(ORPHAN_PRIMARY_KEY);
    let secondary = cluster.key(ORPHAN_SECONDARY_KEY);

    let orphan_start_ts = timestamp(&cluster.store);
    hold_orphan_secondary_lock(
        &cluster.store,
        &primary,
        &secondary,
        orphan_start_ts,
        EXPIRING_LOCK_TTL_MS,
    );
    println!(
        "pessimistic_prewrite_recovery phase=orphaned key=orphan-secondary \
         orphan_start_ts={orphan_start_ts}"
    );

    // Nothing will ever refresh or resolve this lock: its coordinator is gone.
    std::thread::sleep(Duration::from_millis(EXPIRING_LOCK_TTL_MS * 3));

    // Pre-fix this read failed with a terminal KeyError carrying TxnNotFound,
    // and would have failed identically for every reader from then on.
    let observed = read_back(&cluster.opener, &secondary);
    assert_eq!(
        observed, None,
        "the orphaned transaction never committed, so its secondary must read \
         as absent once the lock is rolled back"
    );

    // Readable is not enough: the key must be writable again too, which only
    // holds if the lock is really gone rather than merely stepped over.
    let writer = cluster
        .opener
        .begin()
        .expect("allocate a writer newer than the orphaned lock");
    assert!(writer.start_ts() > orphan_start_ts);
    let outcome = writer
        .commit(
            vec![BufferMutation::set_with_flags(
                secondary.clone(),
                b"after-orphan-recovery".to_vec(),
                true,
                tidb_txnkv::AssertionOp::AssertNotExist,
            )
            .unwrap()],
            &call(),
        )
        .expect("the writer reaches a terminal outcome");
    assert!(
        matches!(outcome, OptimisticCommitOutcome::Committed(_)),
        "an orphaned lock that was really resolved cannot block a later \
         writer: {outcome:?}"
    );
    assert_eq!(
        read_back(&cluster.opener, &secondary),
        Some(b"after-orphan-recovery".to_vec())
    );

    // The recovery must not have invented a value for the primary either.
    assert_eq!(read_back(&cluster.opener, &primary), None);
}
