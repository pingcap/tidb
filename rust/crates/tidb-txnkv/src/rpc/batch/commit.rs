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
use tidb_proto::{KvrpcCommitRequest, KvrpcCommitResponse, KvrpcContext};

use super::{BatchCommandEntry, BatchCommandTag};
use crate::rpc::TransactionBatchPending;

pub(in crate::rpc) fn entry(
    request: &KvrpcCommitRequest,
    context: &KvrpcContext,
    forwarded_host: Option<&str>,
) -> (
    BatchCommandEntry,
    TransactionBatchPending<KvrpcCommitResponse>,
) {
    let mut request = request.clone();
    request.context = Some(context.clone());
    TransactionBatchPending::entry(
        BatchCommandTag::Commit,
        request.encode_to_vec(),
        forwarded_host,
    )
}
