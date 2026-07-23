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

//! Direct DistSQL consumer obligations for `pkg/util/tiflash`.

use tidb_distsql::{
    DistSqlContext, KvRequestMetadata, ReadRequestMetadata, TiFlashReplicaRead,
};

#[test]
fn existing_request_projection_consumes_the_canonical_policy() {
    let mut context = DistSqlContext::default();
    context.request.tiflash_replica_read = TiFlashReplicaRead::ClosestReplicas;

    let session = ReadRequestMetadata::from_context(&context);
    assert_eq!(
        session.tiflash_replica_read,
        TiFlashReplicaRead::ClosestReplicas
    );
    assert_eq!(
        KvRequestMetadata::from_context(&context).tiflash_replica_read,
        TiFlashReplicaRead::ClosestReplicas
    );
}
