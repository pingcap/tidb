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

//! The cluster proof `TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY` is waiting on.
//!
//! `cde12e0033` let an optimistic Prewrite reach the pessimistic-lock resolver
//! and then switched the pessimistic half OFF, because resolving a lock
//! wrongly rolls back another transaction's work and the path had never run
//! against a real TiKV. Everything the scripted suite can prove about it is
//! already proved: the decode, the admission split, the gate's refusal string.
//! What is left needs TiKV to be the one answering, because the three answers
//! that matter -- "this lock is expired", "this lock is alive", and "this lock
//! is now rolled back" -- are TiKV's, not ours.
//!
//! Three claims, and the third one is why the gate exists:
//!
//! 1. **Recovery.** An EXPIRED pessimistic lock left on a key is resolved by an
//!    optimistic Prewrite that needs the key, and the writer commits. This is
//!    the availability gap the gate is holding shut: a Go tidb-server sharing
//!    the cluster leaves exactly this lock whenever `session.retry` replays an
//!    autocommit DML, since `decideTxnMode` is unconditionally pessimistic
//!    while retrying (`pkg/session/session.go:4921-4923`).
//! 2. **Safety.** A LIVE pessimistic lock is NOT rolled back. The Prewrite must
//!    fail, and the lock's owner must still be able to commit its own value
//!    afterwards. A resolver that cleaned this lock would silently destroy a
//!    committed transaction, which is strictly worse than the refusal the gate
//!    currently gives, and no in-process test can distinguish the two: both
//!    "resolved" outcomes look identical from this side of the RPC.
//! 3. **The gate itself.** With the variable unset, the same expired-lock
//!    fixture reproduces the exact refusal recorded before the wiring landed.
//!
//! `pessimistic_prewrite_recovery_enabled` reads the variable ONCE per process
//! (`std::sync::LazyLock`), so claims 1-2 and claim 3 cannot share a test
//! binary invocation. The runner therefore invokes `cargo test` twice; see
//! `scripts/run-realtikv-pessimistic-prewrite-recovery.sh`, which is the only
//! supported way to run this file.

use std::time::Duration;

use tidb_pd_client::PdClient;
use tidb_proto::{
    KvrpcAssertionLevel, KvrpcCommitRequest, KvrpcCommitRole, KvrpcContext, KvrpcMutation, KvrpcOp,
    KvrpcPeer, KvrpcPessimisticAction, KvrpcPessimisticLockRequest, KvrpcPrewriteRequest,
    KvrpcRegionEpoch, KvrpcRequestOrigin,
};
use tidb_txnkv::lock::pessimistic_prewrite_recovery_enabled;
use tidb_txnkv::region::RegionCache;
use tidb_txnkv::rpc::{TonicCoprocessorClient, UnaryCallContext};
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, OptimisticMutation, RealOptimisticTransactionOpener,
    TransactionCause,
};
use tidb_txnkv::{PdRegionLoader, SharedReadAuthority, SharedReadRuntime};

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
const GATED_LOCK_KEY: &[u8] = b"pessimistic-prewrite-recovery-gated";

type RealRuntime = SharedReadRuntime<TonicCoprocessorClient, PdRegionLoader>;

/// Everything one claim needs: the PD authority that hands out timestamps, the
/// opener the transaction under test uses, and a second session the fixture
/// speaks raw kvproto through.
struct Cluster {
    pd: PdClient,
    opener: RealOptimisticTransactionOpener,
    fixture: RealRuntime,
}

fn call() -> UnaryCallContext {
    UnaryCallContext::with_timeout(RPC_TIMEOUT)
}

fn connect() -> Cluster {
    let pd_address = std::env::var("PESSIMISTIC_PREWRITE_RECOVERY_PD_ADDR")
        .expect("runner must provide PESSIMISTIC_PREWRITE_RECOVERY_PD_ADDR");
    let pd = PdClient::connect_seeds([pd_address], Duration::from_secs(10))
        .expect("start sole real PD authority");
    assert_ne!(pd.cluster_id(), 0);
    let transport = TonicCoprocessorClient::new().expect("start sole real BatchCommands authority");
    let shared =
        SharedReadAuthority::start_with_store_liveness(transport, RegionCache::new(
            PdRegionLoader::from_client(pd.clone()),
        ))
        .expect("start sole real RegionCache authority");
    assert_eq!(shared.cluster_id(), pd.cluster_id());
    let read_opener = shared.opener();
    let opener = RealOptimisticTransactionOpener::from_process_capabilities(
        read_opener.clone(),
        pd.clone(),
        RPC_TIMEOUT,
    )
    .expect("derive transaction opener without starting another authority");
    let fixture = read_opener
        .open_session()
        .expect("open the fixture session from the same authority");
    Cluster {
        pd,
        opener,
        fixture,
    }
}

/// Routes one key to its leader, so the fixture's raw requests go where a real
/// client's would.
fn route(runtime: &RealRuntime, key: &[u8]) -> (String, KvrpcContext) {
    let location = runtime
        .locate_key(key)
        .expect("RegionCache lifecycle remains live")
        .expect("locate real fixture key");
    let leader_id = location.leader_peer_id.expect("real region has a leader");
    let leader = location
        .peers
        .iter()
        .find(|peer| peer.id == leader_id)
        .expect("leader peer is in region metadata");
    let store = location
        .stores
        .iter()
        .find(|store| store.id == leader.store_id)
        .expect("leader store is hydrated");
    (
        store.address.clone(),
        KvrpcContext {
            region_id: location.region.id,
            region_epoch: Some(KvrpcRegionEpoch {
                conf_ver: location.region.epoch.conf_ver,
                version: location.region.epoch.version,
            }),
            peer: Some(KvrpcPeer {
                id: leader.id,
                store_id: leader.store_id,
                role: leader.role.as_i32(),
                is_witness: leader.is_witness,
            }),
            request_source: "pessimistic_prewrite_recovery_fixture".to_owned(),
            request_origin: KvrpcRequestOrigin::TiDb as i32,
            cluster_id: runtime.cluster_id(),
            ..KvrpcContext::default()
        },
    )
}

/// Leaves a real pessimistic lock on `key`, the way a Go tidb-server's
/// pessimistic DML does: `Op::PessimisticLock`, the key as its own primary, and
/// a TTL the caller chooses so expiry is a decision rather than a race.
fn hold_real_pessimistic_lock(runtime: &RealRuntime, key: &[u8], start_ts: u64, ttl_ms: u64) {
    let (address, context) = route(runtime, key);
    let request = KvrpcPessimisticLockRequest {
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
    let call = call();
    let mut pending = runtime
        .client()
        .try_borrow_mut()
        .expect("fixture client is not borrowed")
        .begin_transaction_pessimistic_lock(&address, None, &request, &context, &call)
        .expect("publish the real pessimistic lock fixture");
    let response = pending
        .complete(&call)
        .expect("complete the real pessimistic lock fixture")
        .expect("decode the real pessimistic lock fixture response");
    assert!(response.response.region_error.is_none());
    assert!(
        response.response.errors.is_empty(),
        "the fixture key must be lockable: {:?}",
        response.response.errors
    );
}

/// Completes the fixture transaction the way its owner would: a Prewrite that
/// tells TiKV the pessimistic lock is already held, then a Commit.
///
/// Used only by the LIVE-lock claim, and it is the whole assertion there: it
/// can only succeed if the lock this test's Prewrite met was left alone.
fn commit_real_pessimistic_txn(
    runtime: &RealRuntime,
    key: &[u8],
    value: &[u8],
    start_ts: u64,
    commit_ts: u64,
) {
    let (address, context) = route(runtime, key);
    let prewrite = KvrpcPrewriteRequest {
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
    let call = call();
    let mut pending = runtime
        .client()
        .try_borrow_mut()
        .expect("fixture client is not borrowed")
        .begin_transaction_prewrite(&address, None, &prewrite, &context, &call)
        .expect("publish the fixture owner's Prewrite");
    let response = pending
        .complete(&call)
        .expect("complete the fixture owner's Prewrite")
        .expect("decode the fixture owner's Prewrite response");
    assert!(response.response.region_error.is_none());
    assert!(
        response.response.errors.is_empty(),
        "the live lock's owner must still be able to prewrite on it -- a \
         PessimisticLockNotFound here IS the disaster this claim exists to \
         rule out: {:?}",
        response.response.errors
    );

    let commit = KvrpcCommitRequest {
        start_version: start_ts,
        keys: vec![key.to_vec()],
        commit_version: commit_ts,
        commit_role: KvrpcCommitRole::Primary as i32,
        primary_key: key.to_vec(),
        ..KvrpcCommitRequest::default()
    };
    let mut pending = runtime
        .client()
        .try_borrow_mut()
        .expect("fixture client is not borrowed")
        .begin_transaction_commit(&address, None, &commit, &context, &call)
        .expect("publish the fixture owner's Commit");
    let response = pending
        .complete(&call)
        .expect("complete the fixture owner's Commit")
        .expect("decode the fixture owner's Commit response");
    assert!(response.response.region_error.is_none());
    assert!(response.response.error.is_none());
}

/// Reads a key back through a transaction newer than every commit so far, so
/// the value observed is what TiKV durably holds.
fn read_back(opener: &RealOptimisticTransactionOpener, key: &[u8]) -> Option<Vec<u8>> {
    let mut transaction = opener.begin(1, 128).expect("allocate a readback snapshot");
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
///
/// Requires `TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY` to be SET.
#[test]
#[ignore = "requires run-realtikv-pessimistic-prewrite-recovery.sh (gate ON pass)"]
fn an_expired_pessimistic_lock_is_resolved_and_the_writer_commits() {
    assert!(
        pessimistic_prewrite_recovery_enabled(),
        "this pass must run with TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY set"
    );
    let cluster = connect();

    let lock_start_ts = cluster
        .pd
        .get_timestamp()
        .expect("allocate the abandoned lock's start timestamp");
    hold_real_pessimistic_lock(
        &cluster.fixture,
        EXPIRED_LOCK_KEY,
        lock_start_ts,
        EXPIRING_LOCK_TTL_MS,
    );
    println!(
        "pessimistic_prewrite_recovery phase=locked key=expired lock_start_ts={lock_start_ts}"
    );

    // The owner is now gone: nothing refreshes this lock, so TiKV will report
    // it expired to the first CheckTxnStatus that asks after the TTL.
    std::thread::sleep(Duration::from_millis(EXPIRING_LOCK_TTL_MS * 3));

    let writer = cluster
        .opener
        .begin(1, 128)
        .expect("allocate a writer newer than the abandoned lock");
    assert!(writer.start_ts() > lock_start_ts);
    let writer_start_ts = writer.start_ts();
    let outcome = writer
        .commit(
            vec![
                OptimisticMutation::insert(EXPIRED_LOCK_KEY.to_vec(), b"resolved-writer".to_vec())
                    .unwrap(),
            ],
            &call(),
        )
        .expect("the Prewrite must reach a verdict, not a transport failure");
    let OptimisticCommitOutcome::Committed(committed) = outcome else {
        panic!(
            "an expired pessimistic lock must not stop an optimistic writer: {outcome:?}"
        );
    };
    assert_eq!(
        read_back(&cluster.opener, EXPIRED_LOCK_KEY).as_deref(),
        Some(b"resolved-writer".as_slice()),
        "the writer's value must be what TiKV durably holds"
    );
    println!(
        "pessimistic_prewrite_recovery status=passed claim=expired_resolved \
         cluster_id={} lock_start_ts={lock_start_ts} writer_start_ts={writer_start_ts} \
         commit_ts={}",
        cluster.pd.cluster_id(),
        committed.receipt.commit_ts,
    );
}

/// Claim 2, the safety half: a LIVE pessimistic lock is refused, not resolved,
/// and its owner still commits its own value afterwards.
///
/// Requires `TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY` to be SET. This is the
/// claim that cannot be faked in process: a resolver that wrongly rolled the
/// lock back would look exactly like a correct one from the caller's side, and
/// only the owner's later Prewrite -- which would come back
/// `PessimisticLockNotFound` -- tells the two apart.
#[test]
#[ignore = "requires run-realtikv-pessimistic-prewrite-recovery.sh (gate ON pass)"]
fn a_live_pessimistic_lock_survives_the_prewrite_and_still_commits() {
    assert!(
        pessimistic_prewrite_recovery_enabled(),
        "this pass must run with TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY set"
    );
    let cluster = connect();

    let lock_start_ts = cluster
        .pd
        .get_timestamp()
        .expect("allocate the live lock's start timestamp");
    hold_real_pessimistic_lock(
        &cluster.fixture,
        LIVE_LOCK_KEY,
        lock_start_ts,
        LIVE_LOCK_TTL_MS,
    );
    println!("pessimistic_prewrite_recovery phase=locked key=live lock_start_ts={lock_start_ts}");

    let writer = cluster
        .opener
        .begin(1, 128)
        .expect("allocate a writer newer than the live lock");
    assert!(writer.start_ts() > lock_start_ts);
    let writer_start_ts = writer.start_ts();
    let outcome = writer
        .commit(
            vec![
                OptimisticMutation::insert(LIVE_LOCK_KEY.to_vec(), b"must-not-win".to_vec())
                    .unwrap(),
            ],
            &call(),
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
    println!(
        "pessimistic_prewrite_recovery phase=refused key=live writer_start_ts={writer_start_ts}"
    );

    // The owner finishes its transaction. This is the assertion: it can only
    // succeed if its pessimistic lock was still there.
    let commit_ts = cluster
        .pd
        .get_timestamp()
        .expect("allocate the live lock owner's commit timestamp");
    assert!(commit_ts > lock_start_ts);
    commit_real_pessimistic_txn(
        &cluster.fixture,
        LIVE_LOCK_KEY,
        b"live-holder",
        lock_start_ts,
        commit_ts,
    );
    assert_eq!(
        read_back(&cluster.opener, LIVE_LOCK_KEY).as_deref(),
        Some(b"live-holder".as_slice()),
        "the live owner's value must survive: anything else means its work was \
         destroyed by the resolver"
    );
    println!(
        "pessimistic_prewrite_recovery status=passed claim=live_lock_survived \
         cluster_id={} lock_start_ts={lock_start_ts} writer_start_ts={writer_start_ts} \
         holder_commit_ts={commit_ts}",
        cluster.pd.cluster_id(),
    );
}

/// Claim 3: with the gate OFF the same fixture reproduces the recorded
/// refusal, so turning the variable off really does restore the previous
/// behaviour rather than some third thing.
///
/// Requires `TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY` to be UNSET, which is
/// why this is a separate `cargo test` invocation: the gate is read once per
/// process.
#[test]
#[ignore = "requires run-realtikv-pessimistic-prewrite-recovery.sh (gate OFF pass)"]
fn the_gate_off_run_reproduces_the_recorded_refusal() {
    assert!(
        !pessimistic_prewrite_recovery_enabled(),
        "this pass must run with TIDB_RUST_PESSIMISTIC_PREWRITE_RECOVERY unset"
    );
    let cluster = connect();

    let lock_start_ts = cluster
        .pd
        .get_timestamp()
        .expect("allocate the gated fixture's start timestamp");
    hold_real_pessimistic_lock(
        &cluster.fixture,
        GATED_LOCK_KEY,
        lock_start_ts,
        EXPIRING_LOCK_TTL_MS,
    );
    std::thread::sleep(Duration::from_millis(EXPIRING_LOCK_TTL_MS * 3));

    let writer = cluster
        .opener
        .begin(1, 128)
        .expect("allocate a writer newer than the gated lock");
    let writer_start_ts = writer.start_ts();
    let outcome = writer
        .commit(
            vec![
                OptimisticMutation::insert(GATED_LOCK_KEY.to_vec(), b"must-not-win".to_vec())
                    .unwrap(),
            ],
            &call(),
        )
        .expect("the Prewrite must reach a verdict, not a transport failure");
    let OptimisticCommitOutcome::RolledBack(refused) = outcome else {
        panic!("the gate is off, so the writer must be refused: {outcome:?}");
    };
    let TransactionCause::InvalidResponse { detail } = &refused.cause else {
        panic!("the gated refusal is an InvalidResponse: {:?}", refused.cause);
    };
    assert!(
        detail.contains("pessimistic lock type") && detail.contains("outside bounded recovery"),
        "the gated refusal must be the recorded one, not a new message: {detail}"
    );
    assert_eq!(
        read_back(&cluster.opener, GATED_LOCK_KEY),
        None,
        "a refused writer must have written nothing"
    );
    println!(
        "pessimistic_prewrite_recovery status=passed claim=gate_off_refusal \
         cluster_id={} lock_start_ts={lock_start_ts} writer_start_ts={writer_start_ts} \
         detail={detail}",
        cluster.pd.cluster_id(),
    );
}
