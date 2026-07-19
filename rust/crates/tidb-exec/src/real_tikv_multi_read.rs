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
//! source scans, reject local contradictions, acquire one timestamp, open two
//! transport instances from the same process factory, then call the existing
//! single-reader supplied-snapshot seam. No row is materialized here.

use std::sync::Arc;

use tidb_distsql::{CancelHandle, QueryTransport, TimestampSource};
use tidb_planner::read_only_scan::{ConfiguredTable, ReadOnlyScanError, ReadOnlyScanPlan};

use crate::real_tikv_read::{
    RealTiKvQuery, RealTiKvReadError, RealTiKvReadSession, RealTiKvSessionTransportFactory,
};

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
    /// One query-local transport could not be opened from the shared factory.
    Transport {
        /// Zero-based source relation.
        relation: usize,
        /// Factory failure.
        message: String,
    },
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
            Self::Transport { relation, message } => {
                write!(formatter, "relation {relation} transport failed: {message}")
            }
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
            Self::Contradiction { .. }
            | Self::Timestamp(_)
            | Self::ZeroTimestamp
            | Self::Transport { .. } => None,
        }
    }
}

/// Reusable statement authority over one process transport factory.
pub struct RealTiKvMultiReadSession<F, S> {
    tables: [ConfiguredTable; 2],
    transport_factory: F,
    timestamp_source: S,
}

impl<F, S> RealTiKvMultiReadSession<F, S> {
    /// Retains exactly two configured tables in source order.
    #[must_use]
    pub const fn new(
        tables: [ConfiguredTable; 2],
        transport_factory: F,
        timestamp_source: S,
    ) -> Self {
        Self {
            tables,
            transport_factory,
            timestamp_source,
        }
    }

    /// Returns configured inputs in stable left-then-right order.
    #[must_use]
    pub const fn configured_tables(&self) -> &[ConfiguredTable; 2] {
        &self.tables
    }
}

impl<F, S> RealTiKvMultiReadSession<F, S>
where
    F: RealTiKvSessionTransportFactory,
    F::Transport: QueryTransport,
    <F::Transport as QueryTransport>::Response: 'static,
    S: TimestampSource + Clone,
{
    /// Lowers and opens two physical scans with a fresh cancellation owner.
    pub fn execute(
        &self,
        relation_sql: [&str; 2],
    ) -> Result<RealTiKvMultiQuery<F::Transport, S>, RealTiKvMultiReadError> {
        self.execute_with_cancellation(relation_sql, Arc::new(CancelHandle::default()))
    }

    /// Lowers and opens two physical scans under one caller cancellation.
    pub fn execute_with_cancellation(
        &self,
        relation_sql: [&str; 2],
        cancellation: Arc<CancelHandle>,
    ) -> Result<RealTiKvMultiQuery<F::Transport, S>, RealTiKvMultiReadError> {
        let left_plan = self.lower_plan(0, relation_sql[0])?;
        let right_plan = self.lower_plan(1, relation_sql[1])?;
        if left_plan.is_contradiction() {
            return Err(RealTiKvMultiReadError::Contradiction { relation: 0 });
        }
        if right_plan.is_contradiction() {
            return Err(RealTiKvMultiReadError::Contradiction { relation: 1 });
        }

        let snapshot_ts = self
            .timestamp_source
            .current_ts()
            .map_err(RealTiKvMultiReadError::Timestamp)?;
        if snapshot_ts == 0 {
            return Err(RealTiKvMultiReadError::ZeroTimestamp);
        }

        let left_transport =
            self.transport_factory
                .open_session_transport()
                .map_err(|message| RealTiKvMultiReadError::Transport {
                    relation: 0,
                    message,
                })?;
        let right_transport =
            self.transport_factory
                .open_session_transport()
                .map_err(|message| RealTiKvMultiReadError::Transport {
                    relation: 1,
                    message,
                })?;
        let mut left_session = RealTiKvReadSession::new(
            self.tables[0].clone(),
            left_transport,
            self.timestamp_source.clone(),
        );
        let mut right_session = RealTiKvReadSession::new(
            self.tables[1].clone(),
            right_transport,
            self.timestamp_source.clone(),
        );
        let left = left_session
            .execute_plan_at_snapshot(left_plan, snapshot_ts, Arc::clone(&cancellation))
            .map_err(|source| RealTiKvMultiReadError::Read {
                relation: 0,
                source,
            })?;
        let right = match right_session.execute_plan_at_snapshot(
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
            sessions: [left_session, right_session],
            snapshot_ts,
        })
    }

    fn lower_plan(
        &self,
        relation: usize,
        sql: &str,
    ) -> Result<ReadOnlyScanPlan, RealTiKvMultiReadError> {
        ReadOnlyScanPlan::lower(sql, &self.tables[relation])
            .map_err(|source| RealTiKvMultiReadError::Plan { relation, source })
    }
}

/// Two lazy response owners plus their distinct query-local transports.
pub struct RealTiKvMultiQuery<T, S> {
    relations: [RealTiKvQuery; 2],
    sessions: [RealTiKvReadSession<T, S>; 2],
    snapshot_ts: u64,
}

impl<T, S> RealTiKvMultiQuery<T, S> {
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

    /// Returns the distinct table-bound sessions retaining transport evidence.
    #[must_use]
    pub const fn sessions(&self) -> &[RealTiKvReadSession<T, S>; 2] {
        &self.sessions
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
