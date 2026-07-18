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

use std::cell::Cell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

use prost::Message;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_distsql::region::{
    Peer, PeerRole, RegionCache, RegionLoadError, RegionLoader, RegionLocation, RegionMetadata,
    RegionRecoveryLoader, RegionVerId, Store, StoreLiveness,
};
use tidb_distsql::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryQueryTransport, DirectUnaryRequest,
    DirectUnaryResponse, DirectUnaryRuntimeConfig, KvRequestMetadata, RequestKeyRange,
    RequestKeyRanges, RequestType, StoreType, TransportRequest,
};
use tidb_exec::storage_reader::{ReaderKind, ReaderPlan, ReaderState, TableIndexReader};
use tidb_proto::tipb::{Chunk, SelectResponse};
use tidb_proto::CoprocessorResponse;

struct ReaderUnaryClient {
    sends: Rc<Cell<usize>>,
    responses: VecDeque<Vec<u8>>,
}

struct ReaderLoader {
    region: RegionLocation,
}

impl RegionLoader for ReaderLoader {
    fn cluster_id(&self) -> u64 {
        9001
    }

    fn load_region(&mut self, _key: &[u8]) -> Result<RegionLocation, RegionLoadError> {
        Ok(self.region.clone())
    }
}

impl RegionRecoveryLoader for ReaderLoader {
    fn hydrate_region(
        &mut self,
        metadata: &RegionMetadata,
        leader_store_id: u64,
    ) -> Result<RegionLocation, RegionLoadError> {
        if metadata.region != self.region.region {
            return Err(RegionLoadError::new(
                "reader-loader-region-mismatch",
                "recovery metadata does not match the scripted region",
            ));
        }
        let mut location = self.region.clone();
        location.leader_peer_id = location
            .peers
            .iter()
            .find(|peer| peer.store_id == leader_store_id)
            .map(|peer| peer.id)
            .or(location.leader_peer_id);
        Ok(location)
    }
}

impl DirectUnaryClient for ReaderUnaryClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        assert_eq!(address, "tikv-1:20160");
        let wire =
            tidb_proto::CoprocessorRequest::decode(request.encoded_request.as_slice()).unwrap();
        assert!(wire.context.is_none());
        assert_eq!(request.context.region_id, 1);
        assert!(timeout <= Duration::from_secs(9));
        assert!(timeout > Duration::from_secs(8));
        self.sends.set(self.sends.get() + 1);
        Ok(DirectUnaryResponse {
            encoded_response: self.responses.pop_front().expect("one unary response"),
        })
    }

    fn send_request_with_context(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        call: &tidb_distsql::UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        if call.cancellation().is_cancelled() {
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        let result = self.send_request(address, request, call.timeout());
        if call.cancellation().is_cancelled() {
            Err(DirectUnaryClientError::CallerCancelled)
        } else {
            result
        }
    }

    fn close_address(&mut self, _address: &str) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn close_address_version(
        &mut self,
        _address: &str,
        _version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<StoreLiveness, DirectUnaryClientError> {
        Ok(StoreLiveness::Reachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

impl tidb_distsql::LockRecoveryClient for ReaderUnaryClient {
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_distsql::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        panic!("reader test does not return locks")
    }

    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        _request: &tidb_proto::KvrpcResolveLockRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &tidb_distsql::UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        panic!("reader test does not return locks")
    }
}

fn request(cancel: std::sync::Arc<tidb_distsql::CancelHandle>) -> TransportRequest {
    TransportRequest::new(
        KvRequestMetadata {
            request_type: RequestType::Dag,
            data: Some(b"table-scan-dag".to_vec()),
            key_ranges: Some(RequestKeyRanges::new_non_partitioned(vec![
                RequestKeyRange {
                    start_key: b"a".to_vec(),
                    end_key: b"z".to_vec(),
                },
            ])),
            keep_order: true,
            store_type: StoreType::TiKv,
            start_ts: 42,
            read_replica_scope: "global".to_owned(),
            txn_scope: "global".to_owned(),
            ..KvRequestMetadata::default()
        },
        cancel,
    )
}

fn transport(sends: Rc<Cell<usize>>) -> DirectUnaryQueryTransport<ReaderUnaryClient, ReaderLoader> {
    transport_with_rows(sends, &[&[1, 2, 3]])
}

fn transport_with_rows(
    sends: Rc<Cell<usize>>,
    responses: &[&[i64]],
) -> DirectUnaryQueryTransport<ReaderUnaryClient, ReaderLoader> {
    let responses = responses
        .iter()
        .map(|values| {
            CoprocessorResponse {
                data: encoded_rows(values),
                ..CoprocessorResponse::default()
            }
            .encode_to_vec()
        })
        .collect();
    DirectUnaryQueryTransport::new(
        ReaderUnaryClient { sends, responses },
        RegionCache::new(ReaderLoader {
            region: RegionLocation {
                region: RegionVerId::new(1, 1, 1),
                start_key: b"a".to_vec(),
                end_key: b"z".to_vec(),
                peers: vec![Peer {
                    id: 2,
                    store_id: 3,
                    role: PeerRole::Voter,
                    is_witness: false,
                    store_epoch: 4,
                }],
                leader_peer_id: Some(2),
                stores: vec![Store {
                    id: 3,
                    address: "tikv-1:20160".to_owned(),
                    epoch: 4,
                }],
                ..RegionLocation::default()
            },
        }),
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(9),
            ..DirectUnaryRuntimeConfig::default()
        },
        tidb_distsql::FixedTimestampSource::new(1 << 18),
    )
    .unwrap()
}

#[test]
fn exhausting_first_serial_request_does_not_cancel_its_sibling() {
    let execution = tidb_distsql::ExecutionState::new();
    let sends = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(
        ReaderKind::Table,
        vec![
            request(std::sync::Arc::clone(&execution.cancel)),
            request(std::sync::Arc::clone(&execution.cancel)),
        ],
        vec![FieldType::new(FieldTypeCode::Long)],
    );
    let mut reader =
        TableIndexReader::new(plan, transport_with_rows(Rc::clone(&sends), &[&[1], &[2]]));

    reader.open().unwrap();
    assert_eq!(ints(reader.next(1).unwrap()), [1]);
    assert_eq!(ints(reader.next(1).unwrap()), [2]);
    assert_eq!(sends.get(), 2);
    assert!(!execution.cancel.is_cancelled());
}

fn encoded_rows(values: &[i64]) -> Vec<u8> {
    let mut rows_data = Vec::new();
    for value in values {
        rows_data.extend_from_slice(&[8, u8::try_from(value * 2).unwrap()]);
    }
    SelectResponse {
        chunks: vec![Chunk {
            rows_data: Some(rows_data),
            rows_meta: Vec::new(),
        }],
        ..SelectResponse::default()
    }
    .encode_to_vec()
}

fn ints(rows: Vec<Vec<Datum>>) -> Vec<i64> {
    rows.into_iter()
        .map(|row| match row.as_slice() {
            [Datum::Int(value)] => *value,
            other => panic!("unexpected row {other:?}"),
        })
        .collect()
}

#[test]
fn table_and_index_readers_lazily_cross_the_unary_boundary_and_keep_row_budgets() {
    // pkg/executor/table_readers_required_rows_test.go:172 TestTableReaderRequiredRows
    // pkg/executor/table_readers_required_rows_test.go:225 TestIndexReaderRequiredRows
    for kind in [ReaderKind::Table, ReaderKind::Index] {
        let execution = tidb_distsql::ExecutionState::new();
        let sends = Rc::new(Cell::new(0));
        let plan = ReaderPlan::new(
            kind,
            vec![request(std::sync::Arc::clone(&execution.cancel))],
            vec![FieldType::new(FieldTypeCode::Long)],
        );
        let mut reader = TableIndexReader::new(plan, transport(Rc::clone(&sends)));

        reader.open().unwrap();
        assert_eq!(sends.get(), 0, "open only transfers the lazy owner");
        assert_eq!(ints(reader.next(1).unwrap()), [1]);
        assert_eq!(sends.get(), 1);
        // The decoder buffers the unused rows from the one raw TiKV response;
        // the second caller budget is honored without a speculative RPC.
        assert_eq!(ints(reader.next(2).unwrap()), [2, 3]);
        assert_eq!(sends.get(), 1);
        assert!(reader.next(1).unwrap().is_empty());
        reader.close();
        reader.close();
        assert_eq!(reader.state(), ReaderState::Closed);
        assert_eq!(sends.get(), 1);
    }
}

#[test]
fn close_after_open_discards_the_unpulled_unary_response() {
    let execution = tidb_distsql::ExecutionState::new();
    let sends = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(
        ReaderKind::Table,
        vec![request(std::sync::Arc::clone(&execution.cancel))],
        vec![FieldType::new(FieldTypeCode::Long)],
    );
    let mut reader = TableIndexReader::new(plan, transport(Rc::clone(&sends)));
    reader.open().unwrap();
    reader.close();
    assert_eq!(sends.get(), 0);
}

#[test]
fn table_index_reader_preserves_typed_execution_cancellation_without_dispatch() {
    // pkg/executor/adapter_test.go:93 TestContextCancelWhenReadFromCopIterator
    let execution = tidb_distsql::ExecutionState::new();
    let sends = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(
        ReaderKind::Table,
        vec![request(std::sync::Arc::clone(&execution.cancel))],
        vec![FieldType::new(FieldTypeCode::Long)],
    );
    let mut reader = TableIndexReader::new(plan, transport(Rc::clone(&sends)));
    reader.open().unwrap();

    execution.cancel.cancel();
    assert_eq!(
        reader.next(1),
        Err(tidb_exec::storage_reader::StorageReaderError::Cancelled)
    );
    assert_eq!(sends.get(), 0, "cancellation must win before TiKV dispatch");
    reader.close();
    assert_eq!(reader.state(), ReaderState::Closed);
}
