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

//! Two configured real-TiKV reads under one statement snapshot.
//!
//! TiDB's executor builder resolves a statement snapshot once and gives it to
//! every table reader. This authority follows the same order: lower both
//! source scans, reject local contradictions, acquire one timestamp, then call
//! the existing single-reader supplied-snapshot seam on two connection-local
//! transports opened by [`RealTiKvReadSessionOpener::open_multi_session`]. No
//! row is materialized here and repeated statements reuse those transports.

use std::sync::Arc;

use tidb_distsql::{CancelHandle, QueryTransport, TimestampSource};
use tidb_planner::read_only_scan::{ReadOnlyScanError, ReadOnlyScanPlan};

pub use crate::real_tikv_read::RealTiKvMultiReadSession;
use crate::real_tikv_read::{RealTiKvQuery, RealTiKvReadError};

/// A source-identified failure while opening two physical table reads.
#[derive(Debug)]
pub enum RealTiKvMultiReadError {
    /// One input failed read-only planning before runtime side effects.
    Plan {
        /// Zero-based source relation (`0` is left, `1` is right).
        relation: usize,
        /// Existing planner error.
        source: ReadOnlyScanError,
    },
    /// A contradiction would not publish the two physical scans required by
    /// the bounded Campaign 25 join runtime.
    Contradiction {
        /// Zero-based contradictory relation.
        relation: usize,
    },
    /// The statement timestamp source failed.
    Timestamp(String),
    /// The timestamp source returned an invalid zero timestamp.
    ZeroTimestamp,
    /// One supplied-snapshot physical read failed.
    Read {
        /// Zero-based source relation.
        relation: usize,
        /// Existing reader failure.
        source: RealTiKvReadError,
    },
}

impl std::fmt::Display for RealTiKvMultiReadError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plan { relation, source } => {
                write!(formatter, "relation {relation} planning failed: {source}")
            }
            Self::Contradiction { relation } => {
                write!(formatter, "relation {relation} is locally contradictory")
            }
            Self::Timestamp(message) => write!(formatter, "PD timestamp failed: {message}"),
            Self::ZeroTimestamp => formatter.write_str("PD returned a zero snapshot timestamp"),
            Self::Read { relation, source } => {
                write!(formatter, "relation {relation} read failed: {source}")
            }
        }
    }
}

impl std::error::Error for RealTiKvMultiReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Plan { source, .. } => Some(source),
            Self::Read { source, .. } => Some(source),
            Self::Contradiction { .. } | Self::Timestamp(_) | Self::ZeroTimestamp => None,
        }
    }
}

impl<T, S> RealTiKvMultiReadSession<T, S>
where
    T: QueryTransport,
    T::Response: 'static,
    S: TimestampSource,
{
    /// Returns configured inputs in stable left-then-right order.
    #[must_use]
    pub fn configured_tables(&self) -> [&tidb_planner::read_only_scan::ConfiguredTable; 2] {
        [
            self.readers[0].configured_table(),
            self.readers[1].configured_table(),
        ]
    }

    /// Lowers and starts two physical scans with a fresh cancellation owner.
    pub fn execute(
        &mut self,
        relation_sql: [&str; 2],
    ) -> Result<RealTiKvMultiQuery, RealTiKvMultiReadError> {
        self.execute_with_cancellation(relation_sql, Arc::new(CancelHandle::default()))
    }

    /// Lowers and starts two physical scans under one caller cancellation.
    pub fn execute_with_cancellation(
        &mut self,
        relation_sql: [&str; 2],
        cancellation: Arc<CancelHandle>,
    ) -> Result<RealTiKvMultiQuery, RealTiKvMultiReadError> {
        let left_plan = self.lower_plan(0, relation_sql[0])?;
        let right_plan = self.lower_plan(1, relation_sql[1])?;
        if left_plan.is_contradiction() {
            return Err(RealTiKvMultiReadError::Contradiction { relation: 0 });
        }
        if right_plan.is_contradiction() {
            return Err(RealTiKvMultiReadError::Contradiction { relation: 1 });
        }

        let snapshot_ts = self
            .timestamp_source()
            .current_ts()
            .map_err(RealTiKvMultiReadError::Timestamp)?;
        if snapshot_ts == 0 {
            return Err(RealTiKvMultiReadError::ZeroTimestamp);
        }

        let [left_reader, right_reader] = &mut self.readers;
        let left = left_reader
            .execute_plan_at_snapshot(left_plan, snapshot_ts, Arc::clone(&cancellation))
            .map_err(|source| RealTiKvMultiReadError::Read {
                relation: 0,
                source,
            })?;
        let right = match right_reader.execute_plan_at_snapshot(
            right_plan,
            snapshot_ts,
            Arc::clone(&cancellation),
        ) {
            Ok(query) => query,
            Err(source) => {
                cancellation.cancel();
                close_partial_query(left);
                return Err(RealTiKvMultiReadError::Read {
                    relation: 1,
                    source,
                });
            }
        };

        Ok(RealTiKvMultiQuery {
            relations: [left, right],
            snapshot_ts,
        })
    }

    fn lower_plan(
        &self,
        relation: usize,
        sql: &str,
    ) -> Result<ReadOnlyScanPlan, RealTiKvMultiReadError> {
        ReadOnlyScanPlan::lower(sql, self.readers[relation].configured_table())
            .map_err(|source| RealTiKvMultiReadError::Plan { relation, source })
    }
}

/// Two lazy response owners from the connection-local table readers.
pub struct RealTiKvMultiQuery {
    relations: [RealTiKvQuery; 2],
    snapshot_ts: u64,
}

impl RealTiKvMultiQuery {
    /// Returns the single timestamp placed in both requests.
    #[must_use]
    pub const fn snapshot_ts(&self) -> u64 {
        self.snapshot_ts
    }

    /// Returns immutable per-relation query and plan evidence in source order.
    #[must_use]
    pub const fn relations(&self) -> &[RealTiKvQuery; 2] {
        &self.relations
    }

    /// Transfers both lazy response owners to the join runtime.
    #[must_use]
    pub fn into_relations(self) -> [RealTiKvQuery; 2] {
        self.relations
    }
}

fn close_partial_query(query: RealTiKvQuery) {
    let mut record_set = query.into_record_set();
    let _ = record_set.close();
}
