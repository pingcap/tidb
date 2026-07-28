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

use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

use prost::Message;
use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_distsql::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport, DirectUnaryRequest,
    DirectUnaryResponse, DirectUnaryRuntimeConfig, FixedTimestampSource, InjectedQueryRuntime,
    KvRequestMetadata, LockRecoveryClient, QueryResultContext, RequestKeyRange, RequestKeyRanges,
    RequestType, SelectInput, StoreType, TransportRequest, UnaryCallContext, WarningCollector,
};
use tidb_proto::tipb::{DagRequest, EncodeType, EngineType, ExecType, Executor, TableScan};
use tidb_proto::{
    KvrpcCheckTxnStatusRequest, KvrpcCheckTxnStatusResponse, KvrpcContext, KvrpcResolveLockRequest,
    KvrpcResolveLockResponse,
};
use tidb_txnkv::region::{RegionCache, StoreLiveness};
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::PdRegionLoader;

#[derive(Debug, Default)]
struct Evidence {
    cop_attempts: usize,
    cop_addresses: Vec<String>,
    checks: Vec<KvrpcCheckTxnStatusRequest>,
    check_addresses: Vec<String>,
    check_commit_ts: u64,
    resolves: Vec<KvrpcResolveLockRequest>,
    resolve_addresses: Vec<String>,
}

struct RecordingClient {
    inner: TonicCoprocessorClient,
    evidence: Rc<RefCell<Evidence>>,
}

impl DirectUnaryClient for RecordingClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let mut evidence = self.evidence.borrow_mut();
        evidence.cop_attempts += 1;
        evidence.cop_addresses.push(address.to_owned());
        drop(evidence);
        self.inner.send_request(address, request, timeout)
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let mut evidence = self.evidence.borrow_mut();
        evidence.cop_attempts += 1;
        evidence.cop_addresses.push(address.to_owned());
        drop(evidence);
        self.inner.send_request_with_context(address, request, call)
    }

    fn close_address(&mut self, address: &str) -> Result<(), DirectUnaryClientError> {
        self.inner.close_address(address)
    }

    fn close_address_version(
        &mut self,
        address: &str,
        version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        self.inner.close_address_version(address, version)
    }

    fn liveness(
        &self,
        address: &str,
        timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        self.inner.liveness(address, timeout)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        self.inner.close()
    }
}

impl LockRecoveryClient for RecordingClient {
    fn check_txn_status_for_lock(
        &mut self,
        address: &str,
        request: &KvrpcCheckTxnStatusRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        let mut evidence = self.evidence.borrow_mut();
        evidence.checks.push(request.clone());
        evidence.check_addresses.push(address.to_owned());
        drop(evidence);
        let response = self
            .inner
            .check_txn_status(address, request, context, call)?;
        self.evidence.borrow_mut().check_commit_ts = response.commit_version;
        Ok(response)
    }


    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        panic!("this realtikv test does not clean pessimistic locks")
    }
    fn resolve_lock_for_read(
        &mut self,
        address: &str,
        request: &KvrpcResolveLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<KvrpcResolveLockResponse, DirectUnaryClientError> {
        let mut evidence = self.evidence.borrow_mut();
        evidence.resolves.push(request.clone());
        evidence.resolve_addresses.push(address.to_owned());
        drop(evidence);
        self.inner.resolve_lock(address, request, context, call)
    }
}

#[test]
#[ignore = "requires the cleanup-safe committed-primary locked-secondary runner"]
fn committed_primary_resolves_secondary_then_publishes_one_cop_response() {
    let pd_address = std::env::var("LOCK_RECOVERY_PD_ADDR").expect("runner must provide PD address");
    let table_id: i64 = std::env::var("LOCK_RECOVERY_LOCK_TABLE_ID")
        .expect("runner must provide table id")
        .parse()
        .expect("table id must be i64");
    let current_ts: u64 = std::env::var("LOCK_RECOVERY_CURRENT_TS")
        .expect("runner must provide exact TSO")
        .parse()
        .expect("current TSO must be u64");
    let secondary_key = encode_row_key_with_handle(table_id, &RecordHandle::Int(2));
    let expected_secondary_key = secondary_key.clone();
    let end_key = prefix_next(secondary_key.clone());
    let dag = DagRequest {
        executors: vec![Executor {
            tp: Some(ExecType::TypeTableScan as i32),
            tbl_scan: Some(TableScan {
                table_id: Some(table_id),
                columns: Vec::new(),
                desc: Some(false),
                next_read_engine: Some(EngineType::Local as i32),
                keep_order: Some(true),
                ..TableScan::default()
            }),
            executor_id: Some(String::new()),
            ..Executor::default()
        }],
        encode_type: Some(EncodeType::TypeDefault as i32),
        ..DagRequest::default()
    };

    let loader = PdRegionLoader::connect(pd_address, Duration::from_secs(5))
        .expect("connect sole PD-backed loader");
    let evidence = Rc::new(RefCell::new(Evidence::default()));
    let client = RecordingClient {
        inner: TonicCoprocessorClient::new().expect("construct sole unary client"),
        evidence: Rc::clone(&evidence),
    };
    let transport = DirectUnaryQueryTransport::new_injected(
        client,
        RegionCache::new(loader),
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(5),
            ..DirectUnaryRuntimeConfig::default()
        },
        FixedTimestampSource::new(current_ts),
    )
    .expect("install lock recovery over sole runtime");
    let mut runtime = InjectedQueryRuntime::new(transport);
    let metadata = KvRequestMetadata::from_request(tidb_txnkv::Request {
        request_type: RequestType::Dag,
        data: Some(dag.encode_to_vec()),
        key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
            RequestKeyRange {
                start_key: secondary_key.into(),
                end_key: end_key.into(),
            },
        ])),
        keep_order: true,
        store_type: StoreType::TiKv,
        start_ts: current_ts,
        read_replica_scope: "global".to_owned(),
        txn_scope: "global".to_owned(),
        ..tidb_txnkv::Request::default()
    });
    let mut result = runtime
        .select_with_runtime_stats(
            &TransportRequest::new(
                metadata,
                std::sync::Arc::new(tidb_distsql::CancelHandle::default()),
            ),
            SelectInput::default(),
            QueryResultContext::new(Vec::new(), WarningCollector::new()),
            Vec::new(),
            0,
            false,
        )
        .expect("bind live locked-secondary read");
    let published = result
        .next_raw()
        .expect("resolved retry must return usable response");
    assert!(published.is_some(), "retry must publish one response");
    assert_eq!(
        result.next_raw().unwrap(),
        None,
        "only one response may publish"
    );

    let evidence = evidence.borrow();
    assert_eq!(evidence.cop_attempts, 2, "locked response plus exact retry");
    assert_eq!(evidence.cop_addresses.len(), 2);
    assert_eq!(evidence.checks.len(), 1);
    assert_eq!(evidence.check_addresses.len(), 1);
    assert!(evidence.check_commit_ts > 0, "primary must be committed");
    assert!(
        evidence.checks[0].lock_ts > 0,
        "lock start TS must be observed"
    );
    assert!(
        evidence.checks[0].lock_ts < current_ts,
        "the blocking transaction must precede the read"
    );
    assert!(
        !evidence.checks[0].primary_key.is_empty(),
        "the locked response must provide the primary key"
    );
    assert_eq!(evidence.checks[0].caller_start_ts, current_ts);
    assert_eq!(evidence.checks[0].current_ts, current_ts);
    assert_eq!(evidence.resolves.len(), 1);
    assert_eq!(evidence.resolve_addresses.len(), 1);
    assert_eq!(
        evidence.resolves[0].keys,
        vec![expected_secondary_key.clone()]
    );
    assert_eq!(
        evidence.resolves[0].commit_version,
        evidence.check_commit_ts
    );
    println!(
        "campaign13_lock_recovery status=committed lock_start_ts={} caller_start_ts={} locked_key_hex={} primary_key_hex={} primary_route={} commit_ts={} resolve_route={} resolve_key_hex={} cop_route={} cop_attempts=2 publications=1",
        evidence.checks[0].lock_ts,
        current_ts,
        hex(&expected_secondary_key),
        hex(&evidence.checks[0].primary_key),
        evidence.check_addresses[0],
        evidence.check_commit_ts,
        evidence.resolve_addresses[0],
        hex(&expected_secondary_key),
        evidence.cop_addresses[0],
    );
}

fn prefix_next(mut key: Vec<u8>) -> Vec<u8> {
    for byte in key.iter_mut().rev() {
        if *byte != u8::MAX {
            *byte += 1;
            return key;
        }
        *byte = 0;
    }
    key.push(0);
    key
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
