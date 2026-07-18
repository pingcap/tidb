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
use tidb_distsql::{
    InjectedQueryRuntime, KvRequestBuilder, QueryDispatch, QueryResultContext, QueryTransport,
    ResponseChannel, SelectInput, StoreType, TransportRequest, WarningCollector,
};
use tidb_exec::distsql_recordset::DistSqlRecordSet;

struct OneResponse(Option<ResponseChannel<Vec<u8>>>);

impl QueryTransport for OneResponse {
    type Response = ResponseChannel<Vec<u8>>;

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
    source.push_result(response).unwrap();
    source.finish().unwrap();

    let mut builder = KvRequestBuilder::new();
    let request = TransportRequest::new(builder.build().unwrap());
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
    let mut recordset = DistSqlRecordSet::new(iter, Vec::new());
    assert_eq!(
        recordset.next_batch(32).unwrap(),
        vec![
            vec![tidb_datatype::Datum::Int(1)],
            vec![tidb_datatype::Datum::Int(2)]
        ]
    );
    assert!(recordset.next_batch(32).unwrap().is_empty());
    recordset.close().unwrap();
}
