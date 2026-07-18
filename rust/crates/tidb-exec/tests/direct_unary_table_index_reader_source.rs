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
use tidb_distsql::{
    DirectUnaryClient, DirectUnaryQueryTransport, DirectUnaryRequest, DirectUnaryResponse,
    DirectUnaryRuntimeConfig, KvRequestMetadata, RegionTaskEpoch, RegionTaskPeer,
    RegionTaskTopology, RequestKeyRange, RequestKeyRanges, RequestType, ResolvedRegionRoute,
    StoreType, TransportRequest,
};
use tidb_exec::storage_reader::{ReaderKind, ReaderPlan, ReaderState, TableIndexReader};
use tidb_proto::tipb::{Chunk, SelectResponse};
use tidb_proto::CoprocessorResponse;

struct ReaderUnaryClient {
    sends: Rc<Cell<usize>>,
    responses: VecDeque<Vec<u8>>,
}

impl DirectUnaryClient for ReaderUnaryClient {
    fn send_request(
        &mut self,
        address: &str,
        request: &DirectUnaryRequest,
        timeout: Duration,
    ) -> Result<DirectUnaryResponse, String> {
        assert_eq!(address, "tikv-1:20160");
        let wire =
            tidb_proto::CoprocessorRequest::decode(request.encoded_request.as_slice()).unwrap();
        let context = tidb_proto::KvrpcContext::decode(wire.context.unwrap().as_slice()).unwrap();
        assert_eq!(context.region_id, 1);
        assert_eq!(timeout, Duration::from_secs(9));
        self.sends.set(self.sends.get() + 1);
        Ok(DirectUnaryResponse {
            encoded_response: self.responses.pop_front().expect("one unary response"),
        })
    }
}

fn request() -> TransportRequest {
    TransportRequest::new(KvRequestMetadata {
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
    })
}

fn transport(sends: Rc<Cell<usize>>) -> DirectUnaryQueryTransport<ReaderUnaryClient> {
    let response = CoprocessorResponse {
        data: encoded_rows(&[1, 2, 3]),
        ..CoprocessorResponse::default()
    }
    .encode_to_vec();
    DirectUnaryQueryTransport::new(
        ReaderUnaryClient {
            sends,
            responses: [response].into_iter().collect(),
        },
        [ResolvedRegionRoute {
            topology: RegionTaskTopology {
                region_id: 1,
                region_epoch: Some(RegionTaskEpoch {
                    conf_ver: 1,
                    version: 1,
                }),
                peer: Some(RegionTaskPeer {
                    id: 2,
                    store_id: 3,
                    role: 0,
                    is_witness: false,
                }),
                start_key: b"a".to_vec(),
                end_key: b"z".to_vec(),
                ..RegionTaskTopology::default()
            },
            address: "tikv-1:20160".to_owned(),
        }],
        DirectUnaryRuntimeConfig {
            default_timeout: Duration::from_secs(9),
            ..DirectUnaryRuntimeConfig::default()
        },
    )
    .unwrap()
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
        let sends = Rc::new(Cell::new(0));
        let plan = ReaderPlan::new(
            kind,
            vec![request()],
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
    let sends = Rc::new(Cell::new(0));
    let plan = ReaderPlan::new(
        ReaderKind::Table,
        vec![request()],
        vec![FieldType::new(FieldTypeCode::Long)],
    );
    let mut reader = TableIndexReader::new(plan, transport(Rc::clone(&sends)));
    reader.open().unwrap();
    reader.close();
    assert_eq!(sends.get(), 0);
}
