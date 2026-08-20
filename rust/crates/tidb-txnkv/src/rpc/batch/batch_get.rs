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

use prost::Message;
use tidb_proto::{KvrpcBatchGetRequest, KvrpcBatchGetResponse, KvrpcContext};

use super::{BatchCommandEntry, BatchCommandTag};
use crate::rpc::TransactionBatchPending;

pub(in crate::rpc) fn entry(
    request: &KvrpcBatchGetRequest,
    context: &KvrpcContext,
    forwarded_host: Option<&str>,
) -> (
    BatchCommandEntry,
    TransactionBatchPending<KvrpcBatchGetResponse>,
) {
    let mut request = request.clone();
    request.context = Some(context.clone());
    TransactionBatchPending::entry(
        BatchCommandTag::BatchGet,
        request.encode_to_vec(),
        forwarded_host,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_region_key_set_is_one_batch_get_command() {
        let request = KvrpcBatchGetRequest {
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
            version: 42,
            need_commit_ts: true,
            ..KvrpcBatchGetRequest::default()
        };
        let context = KvrpcContext {
            region_id: 7,
            ..KvrpcContext::default()
        };

        let (entry, _pending) = entry(&request, &context, None);

        assert_eq!(entry.payload().tag(), BatchCommandTag::BatchGet);
        let decoded = KvrpcBatchGetRequest::decode(entry.payload().body()).unwrap();
        assert_eq!(decoded.keys, request.keys);
        assert_eq!(decoded.version, 42);
        assert!(decoded.need_commit_ts);
        assert_eq!(decoded.context, Some(context));
    }
}
