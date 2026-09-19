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
use tidb_proto::tipb;

#[test]
fn full_sampling_request_preserves_go_wire_contract() {
    // tipb@fed7bc47c39d: AnalyzeReq.tp=1, col_req=6;
    // AnalyzeColumnsReq.sample_size=2, sample_rate=11.
    let wire = [
        0x08, 0x05, 0x32, 0x0b, 0x10, 0x00, 0x59, 0, 0, 0, 0, 0, 0, 0xe0, 0x3f,
    ];
    let request = tipb::AnalyzeReq::decode(wire.as_slice()).unwrap();
    assert_eq!(request.tp, Some(tipb::AnalyzeType::TypeFullSampling as i32));
    let columns = request.col_req.as_ref().unwrap();
    assert_eq!(columns.sample_size, Some(0));
    assert_eq!(columns.sample_rate, Some(0.5));
    assert_eq!(request.encode_to_vec(), wire);
}

#[test]
fn full_sampling_response_preserves_counts_and_sample_weights() {
    // AnalyzeColumnsResp.row_collector=3; count includes unsampled rows.
    let wire = [
        0x1a, 0x0e, 0x0a, 0x07, 0x0a, 0x03, 0x01, 0x02, 0x03, 0x10, 0x07, 0x10, 0x02, 0x18, 0xe8,
        0x07,
    ];
    let response = tipb::AnalyzeColumnsResp::decode(wire.as_slice()).unwrap();
    let collector = response.row_collector.unwrap();
    assert_eq!(collector.count, Some(1000));
    assert_eq!(collector.null_counts, vec![2]);
    assert_eq!(collector.samples[0].weight, Some(7));
    assert_eq!(collector.samples[0].row, vec![vec![1, 2, 3]]);
}
