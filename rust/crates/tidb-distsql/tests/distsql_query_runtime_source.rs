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

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_distsql::{InjectedQueryRuntime, KvRequestBuilder, QueryDispatch, QueryResultContext, QueryTransport, ResponseChannel, SelectInput};
use tidb_txnkv::TransportRequest;
use tidb_util::context::WarningCollector;
use tidb_txnkv::StoreType;

struct OneResponse(Option<ResponseChannel<prost::bytes::Bytes>>);

impl QueryTransport for OneResponse {
    type Response = ResponseChannel<prost::bytes::Bytes>;

    fn send(
        &mut self,
        _request: &TransportRequest,
        _dispatch: &QueryDispatch,
    ) -> Result<Option<Self::Response>, String> {
        Ok(self.0.take())
    }
}

#[test]
fn injected_query_runtime_feeds_the_existing_recordset_consumer() {
    let rows_data = [8, 2, 8, 4];
    let mut chunk = vec![0x1a, rows_data.len() as u8];
    chunk.extend_from_slice(&rows_data);
    let mut response = vec![0x1a, chunk.len() as u8];
    response.extend_from_slice(&chunk);
    let mut source = ResponseChannel::new();
    source.push_result(response.into()).unwrap();
    source.finish().unwrap();

    let mut builder = KvRequestBuilder::new();
    let request = TransportRequest::new(
        builder.build().unwrap(),
        std::sync::Arc::new(tidb_txnkv::CancelHandle::default()),
    );
    let mut runtime = InjectedQueryRuntime::new(OneResponse(Some(source)));
    let result = runtime
        .select(
            &request,
            SelectInput {
                store_type: StoreType::TiKv,
                row_len: 1,
                ..SelectInput::default()
            },
            QueryResultContext::new(
                vec![FieldType::new(FieldTypeCode::Long)],
                WarningCollector::new(),
            ),
        )
        .unwrap();
    let iter = result.into_select_iter(Vec::new());
    let mut iter = iter;
    let mut rows = Vec::new();
    while let Some(row) = iter.next_row().unwrap() {
        rows.push(row.row);
    }
    assert_eq!(
        rows,
        vec![
            vec![tidb_datatype::Datum::Int(1)],
            vec![tidb_datatype::Datum::Int(2)]
        ]
    );
    iter.close();
}
