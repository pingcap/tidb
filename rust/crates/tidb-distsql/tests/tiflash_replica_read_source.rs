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

//! Complete portable obligations from `pkg/util/tiflash/tiflash_replica_read.go`.

use tidb_distsql::{
    DistSqlContext, KvRequestMetadata, ReadRequestMetadata, TiFlashReplicaRead, ALL_REPLICAS,
    CLOSEST_ADAPTIVE, CLOSEST_REPLICAS, MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS,
};

#[test]
fn every_source_policy_discriminant_predicate_and_string_is_exact() {
    let cases = [
        (TiFlashReplicaRead::AllReplicas, 0, ALL_REPLICAS),
        (TiFlashReplicaRead::ClosestAdaptive, 1, CLOSEST_ADAPTIVE),
        (TiFlashReplicaRead::ClosestReplicas, 2, CLOSEST_REPLICAS),
    ];

    for (policy, discriminant, spelling) in cases {
        assert_eq!(policy as isize, discriminant);
        assert_eq!(policy.as_str(), spelling);
        assert_eq!(TiFlashReplicaRead::from_source_str(spelling), policy);
        assert_eq!(
            TiFlashReplicaRead::source_str_from_raw(discriminant),
            spelling
        );
    }
    assert!(TiFlashReplicaRead::AllReplicas.is_all_replicas());
    assert!(!TiFlashReplicaRead::ClosestAdaptive.is_all_replicas());
    assert!(TiFlashReplicaRead::ClosestReplicas.is_closest_replicas());
    assert!(!TiFlashReplicaRead::ClosestAdaptive.is_closest_replicas());
}

#[test]
fn unknown_values_use_the_source_all_replicas_fallback() {
    for raw in [-2, -1, 3, isize::MAX] {
        assert_eq!(TiFlashReplicaRead::source_str_from_raw(raw), ALL_REPLICAS);
    }
    for value in ["", "ALL_REPLICAS", "closest", "unknown"] {
        assert_eq!(
            TiFlashReplicaRead::from_source_str(value),
            TiFlashReplicaRead::AllReplicas
        );
    }
    assert_eq!(
        TiFlashReplicaRead::default(),
        TiFlashReplicaRead::AllReplicas
    );
    assert_eq!(MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS, 3);
}

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
        KvRequestMetadata::from_context(&context)
            .session
            .tiflash_replica_read,
        TiFlashReplicaRead::ClosestReplicas
    );
}
