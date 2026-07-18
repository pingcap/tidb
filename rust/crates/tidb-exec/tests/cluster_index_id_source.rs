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

//! Source-backed tests for clustered-index ID selection.

use tidb_exec::cluster_index_id::{cluster_index_id, ClusterIndexTableInfo, IndexInfo};

#[test]
fn cluster_index_id_matches_source_table_matrix() {
    // Source: pkg/executor/internal/exec/indexusage.go:130-148.
    // Direct Go coverage: pkg/executor/internal/exec/indexusage_test.go:447
    // (TestIndexUsageReporterWithClusterIndex), t0/t1/t2/t3 metadata cases.
    let integer_primary = ClusterIndexTableInfo {
        pk_is_handle: true,
        is_common_handle: false,
        indices: vec![IndexInfo {
            id: 11,
            primary: true,
        }],
    };
    assert_eq!(cluster_index_id(&integer_primary), Some(0));

    let common_primary = ClusterIndexTableInfo {
        pk_is_handle: false,
        is_common_handle: true,
        indices: vec![
            IndexInfo {
                id: 12,
                primary: false,
            },
            IndexInfo {
                id: 13,
                primary: true,
            },
        ],
    };
    assert_eq!(cluster_index_id(&common_primary), Some(13));

    let non_clustered_primary = ClusterIndexTableInfo {
        pk_is_handle: false,
        is_common_handle: false,
        indices: vec![IndexInfo {
            id: 14,
            primary: true,
        }],
    };
    assert_eq!(cluster_index_id(&non_clustered_primary), None);
}

#[test]
fn common_handle_without_primary_preserves_source_zero_default() {
    // Source: pkg/executor/internal/exec/indexusage.go:132-142.
    // Direct Go coverage: pkg/executor/internal/exec/indexusage_test.go:447
    // (TestIndexUsageReporterWithClusterIndex), common-handle branch.
    let common_without_primary = ClusterIndexTableInfo {
        pk_is_handle: false,
        is_common_handle: true,
        indices: vec![IndexInfo {
            id: 99,
            primary: false,
        }],
    };
    assert_eq!(cluster_index_id(&common_without_primary), Some(0));
}
