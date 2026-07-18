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

//! Dependency-closed request-envelope policy from
//! `pkg/distsql/request_builder.go`.
//!
//! This module models the DAG shape and concurrency choices made before a
//! request reaches `kv.Request`. It intentionally carries no protobuf bytes,
//! key material, region routing, or TiKV transport state. Those boundaries
//! remain separate leaves until their source consumers are ported.

/// The source threshold below which a scan limit can use minimal
/// concurrency. This is `estimatedRegionRowCount` in the Go builder.
pub const ESTIMATED_REGION_ROW_COUNT: u64 = 100_000;

/// Executor kinds relevant to the request-builder concurrency policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorKind {
    /// A table scan executor.
    TableScan,
    /// An index scan executor.
    IndexScan,
    /// A partitioned table scan executor.
    PartitionTableScan,
    /// An index-lookup executor used by push-down lookup plans.
    IndexLookup,
    /// A limit executor.
    Limit,
    /// Any executor not involved in the source policy.
    Other,
}

impl ExecutorKind {
    const fn is_simple_scan(self) -> bool {
        matches!(
            self,
            Self::TableScan | Self::IndexScan | Self::PartitionTableScan
        )
    }
}

/// The dependency-closed shape of one DAG executor.
///
/// `limit` is populated only for a limit executor. `parent_index` preserves
/// the source `ParentIdx` relation used to recognize an index-lookup
/// push-down scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutorShape {
    /// Executor kind copied from the source DAG enum.
    pub kind: ExecutorKind,
    /// Limit value, when this is a limit executor.
    pub limit: Option<u64>,
    /// Parent executor index, when supplied by the source DAG.
    pub parent_index: Option<usize>,
}

impl ExecutorShape {
    /// Creates a non-limit executor shape.
    #[must_use]
    pub const fn new(kind: ExecutorKind) -> Self {
        Self {
            kind,
            limit: None,
            parent_index: None,
        }
    }

    /// Creates a source-shaped limit executor.
    #[must_use]
    pub const fn limit(value: u64, parent_index: Option<usize>) -> Self {
        Self {
            kind: ExecutorKind::Limit,
            limit: Some(value),
            parent_index,
        }
    }
}

/// Request fields needed by `RequestBuilder.SetDAGRequest` and `Build`.
///
/// `partition_count` is the already-built key-range partition count. A zero
/// value means the source builder has no wrapped ranges and therefore uses a
/// single request partition. The envelope is not a KV request and cannot be
/// sent to a store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestEnvelope {
    /// Whether the result order must be preserved.
    pub keep_order: bool,
    /// Current request concurrency before the final `Build` policy.
    pub concurrency: u64,
    /// Number of key-range partitions, or zero when ranges are unset.
    pub partition_count: usize,
    /// Ordered DAG executor shapes.
    pub executors: Vec<ExecutorShape>,
}

impl RequestEnvelope {
    /// Creates an envelope with source zero-value fields.
    #[must_use]
    pub fn new(executors: Vec<ExecutorShape>) -> Self {
        Self {
            keep_order: false,
            concurrency: 0,
            partition_count: 0,
            executors,
        }
    }

    /// Compatibility projection for shape-only callers.
    ///
    /// The canonical [`crate::RequestBuilder`] preserves source mutation order
    /// by applying the small-limit rule in `set_dag_request` and the ordered
    /// scan rule in `build`. This helper retains the older combined projection
    /// for callers that do not own a builder.
    ///
    /// The projection has two source rules:
    ///
    /// * a one-executor ordered scan at the default session concurrency is
    ///   reduced to two workers;
    /// * a small limit on a plain scan or push-down index lookup uses one
    ///   worker per key-range partition (or one when ranges are unset).
    ///
    /// Explicit session concurrency is preserved except where the source DAG
    /// policy itself has already selected the minimal small-limit value.
    #[must_use]
    pub fn effective_concurrency(&self, default_concurrency: u64) -> u64 {
        // Shape-only callers have no session-projection step, so zero uses the
        // supplied session default here. RequestBuilder itself does not do
        // this defaulting in Build.
        let mut concurrency = if self.concurrency == 0 {
            default_concurrency
        } else {
            self.concurrency
        };

        if self.executors.len() == 1
            && self.keep_order
            && concurrency == default_concurrency
            && self.executors[0].kind.is_simple_scan()
        {
            concurrency = 2;
        }

        if self.executors.len() >= 2 {
            let second = self.executors[1];
            let small_limit = second
                .limit
                .is_some_and(|limit| limit < ESTIMATED_REGION_ROW_COUNT);
            let index_lookup_pushdown = second
                .parent_index
                .filter(|index| *index > 0)
                .and_then(|index| self.executors.get(index))
                .is_some_and(|executor| executor.kind == ExecutorKind::IndexLookup);
            let minimal_concurrency =
                small_limit && (self.executors.len() == 2 || index_lookup_pushdown);
            if minimal_concurrency {
                // `SetDAGRequest` chooses the partition count before
                // `SetFromSessionVars` applies the session upper bound. The
                // final value therefore cannot exceed that bound.
                concurrency = self.partition_count.max(1) as u64;
                if default_concurrency != 0 {
                    concurrency = concurrency.min(default_concurrency);
                }
            }
        }

        concurrency
    }

    /// Returns the concurrency selected immediately by Go `SetDAGRequest`.
    ///
    /// This is intentionally separate from final `Build` policy because
    /// `SetFromSessionVars` may run after this setter and cap the selected
    /// partition count. Deferring the rule to Build loses builder call order.
    #[must_use]
    pub fn small_limit_concurrency(&self) -> Option<u64> {
        if self.executors.len() < 2 {
            return None;
        }
        let second = self.executors[1];
        let small_limit = second
            .limit
            .is_some_and(|limit| limit < ESTIMATED_REGION_ROW_COUNT);
        let index_lookup_pushdown = second
            .parent_index
            .filter(|index| *index > 0)
            .and_then(|index| self.executors.get(index))
            .is_some_and(|executor| executor.kind == ExecutorKind::IndexLookup);
        (small_limit && (self.executors.len() == 2 || index_lookup_pushdown))
            .then_some(self.partition_count.max(1) as u64)
    }

    /// Applies the only DAG rule owned by Go `Build` itself.
    #[must_use]
    pub fn build_concurrency(&self, current: u64, default_concurrency: u64) -> u64 {
        if self.executors.len() == 1
            && self.keep_order
            && current == default_concurrency
            && self.executors[0].kind.is_simple_scan()
        {
            2
        } else {
            current
        }
    }

    /// Returns the source `LimitSize` field from the terminal executor.
    #[must_use]
    pub fn limit_size(&self) -> Option<u64> {
        self.executors.last().and_then(|executor| executor.limit)
    }
}
