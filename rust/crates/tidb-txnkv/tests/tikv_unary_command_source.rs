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

#![allow(missing_docs)]

use prost::Message;
use tidb_proto::{KvrpcCheckTxnStatusRequest, KvrpcContext, KvrpcResolveLockRequest, KvrpcTxnInfo};
use tidb_txnkv::{UnaryCallContext, UnaryCancellation};

#[test]
fn pinned_transaction_commands_preserve_exact_fields() {
    let context = KvrpcContext {
        region_id: 7,
        cluster_id: 11,
        ..KvrpcContext::default()
    };
    let check = KvrpcCheckTxnStatusRequest {
        context: Some(context.clone()),
        primary_key: b"primary".to_vec(),
        lock_ts: 13,
        caller_start_ts: 17,
        current_ts: 19,
        rollback_if_not_exist: true,
        force_sync_commit: false,
        resolving_pessimistic_lock: false,
        verify_is_primary: true,
        is_txn_file: false,
    };
    let check_wire = check.encode_to_vec();
    assert!(
        check_wire.windows(2).any(|field| field == [0x48, 0x01]),
        "verify_is_primary must be bool field 9"
    );
    assert_eq!(
        KvrpcCheckTxnStatusRequest::decode(check_wire.as_slice()).unwrap(),
        check
    );

    let resolve = KvrpcResolveLockRequest {
        context: Some(context),
        start_version: 23,
        commit_version: 29,
        txn_infos: vec![KvrpcTxnInfo {
            txn: 31,
            status: 37,
            is_txn_file: false,
        }],
        keys: vec![b"secondary".to_vec()],
        is_async: false,
        is_txn_file: false,
    };
    let resolve_wire = resolve.encode_to_vec();
    assert!(resolve_wire.contains(&0x2a), "keys must be field 5");
    assert_eq!(
        KvrpcResolveLockRequest::decode(resolve_wire.as_slice()).unwrap(),
        resolve
    );
}

#[test]
fn command_adapters_share_one_raw_unary_authority_and_exact_paths() {
    let adapter = include_str!("../src/rpc/tonic_coprocessor.rs");
    assert!(adapter.contains("/tikvpb.Tikv/Coprocessor"));
    assert!(adapter.contains("/tikvpb.Tikv/KvCheckTxnStatus"));
    assert!(adapter.contains("/tikvpb.Tikv/KvResolveLock"));
    assert_eq!(adapter.matches("unary: RawUnaryClient").count(), 2);
    assert!(!adapter.contains("ChannelPool::new()"));

    let core = include_str!("../src/rpc/unary.rs");
    assert_eq!(core.matches("ChannelPool::new()").count(), 1);
    assert!(core.contains("tokio::select!"));
}

#[test]
fn cancellation_carrier_is_monotonic_and_bound_to_call_context() {
    let cancellation = UnaryCancellation::new();
    let call = UnaryCallContext::new(std::time::Duration::from_millis(250), cancellation.clone());
    let lock_call = call.clone();
    assert!(!call.cancellation().is_cancelled());
    assert!(call
        .cancellation()
        .shares_state_with(lock_call.cancellation()));
    cancellation.cancel();
    assert!(call.cancellation().is_cancelled());
    assert!(lock_call.cancellation().is_cancelled());
    assert_eq!(call.timeout(), std::time::Duration::from_millis(250));
    assert_eq!(lock_call.timeout(), call.timeout());
}
