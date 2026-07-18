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

//! Source-backed tests for DDL job comment formatting.

use tidb_exec::ddl_job_comments::{
    show_job_comments, show_subjob_comments, AnalyzeState, DdlJobKind, DdlReorgMetadata, ReorgType,
};

fn ingest_metadata() -> DdlReorgMetadata {
    DdlReorgMetadata {
        reorg_type: ReorgType::Ingest,
        is_dist_reorg: true,
        use_cloud_storage: true,
        may_need_reorg: true,
        ..DdlReorgMetadata::default()
    }
}

#[test]
fn show_job_comments_match_source_vectors() {
    // Source: pkg/executor/show_ddl_jobs.go:302-355.
    // Direct Go coverage: pkg/executor/show_ddl_jobs_test.go:32
    // (TestShowCommentsFromJob).
    assert_eq!(show_job_comments(DdlJobKind::Other, None, false), "");

    let txn = DdlReorgMetadata {
        reorg_type: ReorgType::Txn,
        may_need_reorg: true,
        ..DdlReorgMetadata::default()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&txn), false),
        "txn"
    );

    let txn_dxf = DdlReorgMetadata {
        reorg_type: ReorgType::Txn,
        is_dist_reorg: true,
        may_need_reorg: true,
        ..DdlReorgMetadata::default()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&txn_dxf), false),
        "txn"
    );

    let txn_merge = DdlReorgMetadata {
        reorg_type: ReorgType::TxnMerge,
        is_dist_reorg: true,
        may_need_reorg: true,
        ..DdlReorgMetadata::default()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&txn_merge), false),
        "txn-merge"
    );

    let ingest = ingest_metadata();
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&ingest), false),
        "ingest, DXF, cloud"
    );

    let ingest_with_nodes = DdlReorgMetadata {
        max_node_count: 5,
        ..ingest.clone()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&ingest_with_nodes), false),
        "ingest, DXF, cloud, max_node_count=5"
    );

    let ingest_with_tunables = DdlReorgMetadata {
        concurrency: 8,
        batch_size: 1024,
        max_write_speed: 1024 * 1024,
        ..ingest.clone()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&ingest_with_tunables), false),
        "ingest, DXF, cloud, thread=8, batch_size=1024, max_write_speed=1048576"
    );

    let ingest_defaults = DdlReorgMetadata {
        concurrency: 4,
        batch_size: 256,
        max_write_speed: 0,
        ..ingest.clone()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&ingest_defaults), false),
        "ingest, DXF, cloud"
    );

    let ingest_scoped = DdlReorgMetadata {
        target_scope: "background".to_owned(),
        ..ingest_defaults
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&ingest_scoped), false),
        "ingest, DXF, cloud, service_scope=background"
    );
}

#[test]
fn show_job_comments_preserves_analyze_and_next_gen_boundaries() {
    let analyzing = DdlReorgMetadata {
        analyze_state: AnalyzeState::Running,
        reorg_type: ReorgType::Ingest,
        is_dist_reorg: true,
        use_cloud_storage: true,
        may_need_reorg: true,
        concurrency: 8,
        ..DdlReorgMetadata::default()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&analyzing), false),
        "analyzing, ingest, DXF, cloud, thread=8"
    );
    assert_eq!(
        show_job_comments(DdlJobKind::AddIndex, Some(&analyzing), true),
        "analyzing"
    );

    let failed = DdlReorgMetadata {
        analyze_state: AnalyzeState::Failed,
        ..DdlReorgMetadata::default()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::Other, Some(&failed), false),
        "analyze_failed"
    );
    let timeout = DdlReorgMetadata {
        analyze_state: AnalyzeState::Timeout,
        ..DdlReorgMetadata::default()
    };
    assert_eq!(
        show_job_comments(DdlJobKind::Other, Some(&timeout), false),
        "analyze_timeout"
    );
}

#[test]
fn show_subjob_comments_match_source_vectors() {
    // Source: pkg/executor/show_ddl_jobs.go:357-370.
    // Direct Go coverage: pkg/executor/show_ddl_jobs_test.go:123
    // (TestShowCommentsFromSubJob).
    assert_eq!(
        show_subjob_comments(ReorgType::None, false, false, false),
        ""
    );
    assert_eq!(
        show_subjob_comments(ReorgType::Ingest, false, false, false),
        "ingest"
    );
    assert_eq!(
        show_subjob_comments(ReorgType::Ingest, true, false, false),
        "ingest, DXF"
    );
    assert_eq!(
        show_subjob_comments(ReorgType::Ingest, true, true, false),
        "ingest, DXF, cloud"
    );
    assert_eq!(
        show_subjob_comments(ReorgType::Ingest, false, true, false),
        "ingest"
    );
    assert_eq!(
        show_subjob_comments(ReorgType::Ingest, true, true, true),
        ""
    );
}
