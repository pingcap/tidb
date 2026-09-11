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

//! Real-PD/TiKV proof that `@@tidb_pessimistic_txn_fair_locking` reaches the
//! cluster-session transaction's lock requests.
//!
//! Go arms fair locking per pessimistic statement
//! (`basePessimisticTxnContextProvider.OnPessimisticStmtStart` ->
//! `KVTxn.StartFairLocking`), and `KVTxn.LockKeys` then sends a single-key
//! statement in `WakeUpModeForceLock`, so a contended key never costs the
//! PessimisticRollback + fresh timestamp + second lock round that a Normal-mode
//! write conflict does. The txnkv contract (single key => ForceLock) is pinned
//! by `pessimistic_lock_source.rs`; what had no coverage was the seam above it:
//! [`SessionTransaction`] carries the switch from `BEGIN` and must apply it when
//! its lazy pessimistic state is promoted by the first locking statement. A
//! TPC-C Payment run on this node issued 0.76 pessimistic rollbacks per
//! transaction against Go's none precisely because that seam dropped the flag.
//!
//! The playground lifecycle is the runner's; nothing here is mocked.

use std::sync::Arc;
use std::time::Duration;

use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_exec::cluster_table_storage::{LockKeysOutcome, SessionTransaction};
use tidb_exec::real_tikv_read::ProductionReadProcessAuthority;
use tidb_exec::session_commit_protocol::session_commit_protocol;
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};

const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const TABLE_ID: i64 = 528_492;
const ID_COLUMN: i64 = 1;
const BALANCE_COLUMN: i64 = 2;

fn configured_table() -> ConfiguredTable {
    ConfiguredTable::new(
        "campaign28",
        "fair_locking",
        TABLE_ID,
        [
            ConfiguredColumn::clustered_primary_key("id", ID_COLUMN),
            ConfiguredColumn::stored_not_null("balance", BALANCE_COLUMN),
        ],
    )
}

fn pd_address() -> String {
    std::env::var("FAIR_LOCKING_PD_ADDR").expect("runner must provide FAIR_LOCKING_PD_ADDR")
}

fn row_key(handle: i64) -> Vec<u8> {
    encode_row_key_with_handle(TABLE_ID, &RecordHandle::Int(handle))
}

#[test]
#[ignore = "requires a live cluster: FAIR_LOCKING_PD_ADDR=127.0.0.1:2379"]
fn a_promoted_session_transaction_locks_fairly_when_the_switch_is_on() {
    let authority =
        ProductionReadProcessAuthority::connect([pd_address()], RPC_TIMEOUT, configured_table())
            .expect("connect one real read/write authority");
    let opener = Arc::new(authority.transaction_opener());

    let mut transaction =
        SessionTransaction::begin_pessimistic(opener, RPC_TIMEOUT, session_commit_protocol())
            .expect("a pessimistic transaction begins");
    transaction.set_fair_locking(true);
    assert!(
        !transaction.is_in_fair_locking_mode(),
        "before the first locking statement there is no locking transaction to arm"
    );

    let outcome = transaction
        .lock_keys(vec![row_key(1)])
        .expect("the first locking statement promotes the lazy pessimistic state");
    assert!(
        matches!(outcome, LockKeysOutcome::Locked { .. }),
        "an uncontended key is granted outright: {outcome:?}"
    );
    assert!(
        transaction.is_in_fair_locking_mode(),
        "the switch taken at BEGIN must reach the promoted locking transaction"
    );
    transaction.rollback().expect("the lock is released");
}

#[test]
#[ignore = "requires a live cluster: FAIR_LOCKING_PD_ADDR=127.0.0.1:2379"]
fn a_promoted_session_transaction_stays_in_normal_mode_when_the_switch_is_off() {
    let authority =
        ProductionReadProcessAuthority::connect([pd_address()], RPC_TIMEOUT, configured_table())
            .expect("connect one real read/write authority");
    let opener = Arc::new(authority.transaction_opener());

    let transaction =
        SessionTransaction::begin_pessimistic(opener, RPC_TIMEOUT, session_commit_protocol())
            .expect("a pessimistic transaction begins");
    let outcome = transaction
        .lock_keys(vec![row_key(2)])
        .expect("the first locking statement promotes the lazy pessimistic state");
    assert!(matches!(outcome, LockKeysOutcome::Locked { .. }));
    assert!(
        !transaction.is_in_fair_locking_mode(),
        "`@@tidb_pessimistic_txn_fair_locking = OFF` locks in Normal mode"
    );
    transaction.rollback().expect("the lock is released");
}
