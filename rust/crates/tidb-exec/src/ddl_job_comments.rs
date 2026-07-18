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

//! DDL job comment formatting from `pkg/executor/show_ddl_jobs.go`.
//!
//! This is the dependency-closed metadata formatter behind TiDB's `SHOW DDL
//! JOBS` comments column. The live DDL job model, kernel-mode selection, and
//! SQL result rows remain outside this leaf; callers provide those facts as
//! typed inputs.

/// The analyze phase recorded in a DDL reorganization metadata object.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AnalyzeState {
    /// No analyze status label is emitted.
    #[default]
    None,
    /// Statistics analysis is running.
    Running,
    /// Statistics analysis failed.
    Failed,
    /// Statistics analysis timed out.
    Timeout,
}

/// The reorganization implementation selected for a DDL job.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReorgType {
    /// No reorganization implementation is present.
    #[default]
    None,
    /// Transactional reorganization.
    Txn,
    /// Ingest reorganization.
    Ingest,
    /// Transactional merge reorganization.
    TxnMerge,
}

impl ReorgType {
    fn label(self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Txn => Some("txn"),
            Self::Ingest => Some("ingest"),
            Self::TxnMerge => Some("txn-merge"),
        }
    }
}

/// The DDL action kind needed by the source formatter.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DdlJobKind {
    /// Any action other than adding an index or primary key.
    #[default]
    Other,
    /// `ALTER TABLE ... ADD INDEX`.
    AddIndex,
    /// `ALTER TABLE ... ADD PRIMARY KEY`.
    AddPrimaryKey,
}

impl DdlJobKind {
    fn adds_index(self) -> bool {
        matches!(self, Self::AddIndex | Self::AddPrimaryKey)
    }
}

/// Reorganization metadata consumed by the `SHOW DDL JOBS` formatter.
///
/// The defaults match TiDB's `DDLReorgMeta` getters used by the Go source:
/// four workers, a batch size of 256, and no write-speed cap. `may_need_reorg`
/// is supplied by the caller because that predicate belongs to the live DDL
/// job model rather than this formatting leaf.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DdlReorgMetadata {
    /// Analyze phase label to append before reorganization labels.
    pub analyze_state: AnalyzeState,
    /// Reorganization implementation selected for the job.
    pub reorg_type: ReorgType,
    /// Whether distributed execution (DXF) is enabled.
    pub is_dist_reorg: bool,
    /// Whether cloud storage is used by ingest execution.
    pub use_cloud_storage: bool,
    /// Requested reorganization worker count.
    pub concurrency: i64,
    /// Requested reorganization batch size.
    pub batch_size: i64,
    /// Maximum write speed, or zero when unlimited.
    pub max_write_speed: i64,
    /// Target placement scope for reorganization.
    pub target_scope: String,
    /// Maximum node count for placement metadata.
    pub max_node_count: i64,
    /// Whether the source job requires reorganization metadata formatting.
    pub may_need_reorg: bool,
}

impl Default for DdlReorgMetadata {
    fn default() -> Self {
        Self {
            analyze_state: AnalyzeState::None,
            reorg_type: ReorgType::None,
            is_dist_reorg: false,
            use_cloud_storage: false,
            concurrency: 4,
            batch_size: 256,
            max_write_speed: 0,
            target_scope: String::new(),
            max_node_count: 0,
            may_need_reorg: false,
        }
    }
}

fn analyze_label(state: AnalyzeState) -> Option<&'static str> {
    match state {
        AnalyzeState::Running => Some("analyzing"),
        AnalyzeState::Failed => Some("analyze_failed"),
        AnalyzeState::Timeout => Some("analyze_timeout"),
        AnalyzeState::None => None,
    }
}

/// Formats the comment suffix for one DDL job.
///
/// Labels intentionally follow the source order: analyze state, reorg mode,
/// DXF/cloud flags, then non-default worker settings and placement metadata.
/// In next-gen mode add-index jobs return after the analyze label because the
/// parameters are selected automatically.
#[must_use]
pub fn show_job_comments(
    job_kind: DdlJobKind,
    reorg: Option<&DdlReorgMetadata>,
    next_gen: bool,
) -> String {
    let Some(reorg) = reorg else {
        return String::new();
    };

    let mut labels = Vec::new();
    if let Some(label) = analyze_label(reorg.analyze_state) {
        labels.push(label.to_owned());
    }

    if job_kind.adds_index() && next_gen {
        return labels.join(", ");
    }

    if job_kind.adds_index() {
        match reorg.reorg_type {
            ReorgType::Txn | ReorgType::TxnMerge => {
                if let Some(label) = reorg.reorg_type.label() {
                    labels.push(label.to_owned());
                }
            }
            ReorgType::Ingest => {
                labels.push("ingest".to_owned());
                if reorg.is_dist_reorg {
                    labels.push("DXF".to_owned());
                }
                if reorg.use_cloud_storage {
                    labels.push("cloud".to_owned());
                }
            }
            ReorgType::None => {}
        }
    }

    if reorg.may_need_reorg {
        if reorg.concurrency != 4 {
            labels.push(format!("thread={}", reorg.concurrency));
        }
        if reorg.batch_size != 256 {
            labels.push(format!("batch_size={}", reorg.batch_size));
        }
        if reorg.max_write_speed != 0 {
            labels.push(format!("max_write_speed={}", reorg.max_write_speed));
        }
        if !reorg.target_scope.is_empty() {
            labels.push(format!("service_scope={}", reorg.target_scope));
        }
        if reorg.max_node_count != 0 {
            labels.push(format!("max_node_count={}", reorg.max_node_count));
        }
    }

    labels.join(", ")
}

/// Formats the comment suffix for one subjob.
///
/// Cloud storage is only shown when DXF is also enabled, matching the source's
/// `if useDXF { ... if useDXF && useCloud { ... } }` nesting.
#[must_use]
pub fn show_subjob_comments(
    reorg_type: ReorgType,
    use_dxf: bool,
    use_cloud: bool,
    next_gen: bool,
) -> String {
    if next_gen {
        return String::new();
    }
    let Some(label) = reorg_type.label() else {
        return String::new();
    };

    let mut labels = vec![label.to_owned()];
    if use_dxf {
        labels.push("DXF".to_owned());
        if use_cloud {
            labels.push("cloud".to_owned());
        }
    }
    labels.join(", ")
}
