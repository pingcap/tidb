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

//! Source-complete tests for the testless `pkg/util/tiflash` package.

use tidb_txnkv::{
    get_tiflash_replica_read, get_tiflash_replica_read_by_str, ClientSendOption, ReplicaRead,
    TiFlashReplicaRead, ALL_REPLICAS, CLOSEST_ADAPTIVE, CLOSEST_REPLICAS,
    MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS,
};

#[test]
fn every_source_policy_discriminant_predicate_and_string_is_exact() {
    let cases = [
        (ReplicaRead::AllReplicas, 0, ALL_REPLICAS),
        (ReplicaRead::ClosestAdaptive, 1, CLOSEST_ADAPTIVE),
        (ReplicaRead::ClosestReplicas, 2, CLOSEST_REPLICAS),
    ];

    for (policy, discriminant, spelling) in cases {
        assert_eq!(policy.raw(), discriminant);
        assert_eq!(policy.as_str(), spelling);
        assert_eq!(get_tiflash_replica_read(policy), spelling);
        assert_eq!(ReplicaRead::from_source_str(spelling), policy);
        assert_eq!(get_tiflash_replica_read_by_str(spelling), policy);
        assert_eq!(ReplicaRead::source_str_from_raw(discriminant), spelling);
    }
    assert!(ReplicaRead::AllReplicas.is_all_replicas());
    assert!(!ReplicaRead::ClosestAdaptive.is_all_replicas());
    assert!(ReplicaRead::ClosestReplicas.is_closest_replicas());
    assert!(!ReplicaRead::ClosestAdaptive.is_closest_replicas());
}

#[test]
fn unknown_source_int_values_remain_observable_and_fall_back() {
    for raw in [isize::MIN, -2, -1, 3, isize::MAX] {
        let policy = ReplicaRead::from_raw(raw);
        assert_eq!(policy.raw(), raw);
        assert_eq!(get_tiflash_replica_read(policy), ALL_REPLICAS);
        assert!(!policy.is_all_replicas());
        assert!(!policy.is_closest_replicas());
    }
    for value in ["", "ALL_REPLICAS", "closest", "unknown"] {
        assert_eq!(
            get_tiflash_replica_read_by_str(value),
            ReplicaRead::AllReplicas
        );
    }
}

#[test]
fn source_width_default_constant_and_client_consumer_are_exact() {
    assert_eq!(
        std::mem::size_of::<ReplicaRead>(),
        std::mem::size_of::<isize>()
    );
    assert_eq!(ReplicaRead::default(), ReplicaRead::AllReplicas);
    assert_eq!(MAX_REMOTE_READ_COUNT_PER_NODE_FOR_CLOSEST_REPLICAS, 3);

    let option = ClientSendOption::<()> {
        tiflash_replica_read: TiFlashReplicaRead::ClosestReplicas,
        ..ClientSendOption::default()
    };
    assert_eq!(
        option.tiflash_replica_read,
        ReplicaRead::ClosestReplicas
    );
}
