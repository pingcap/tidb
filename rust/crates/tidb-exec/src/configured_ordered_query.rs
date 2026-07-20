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

//! Terminal ORDER BY/LIMIT adapter for the configured two-relation join.
//!
//! The planner resolves order keys against `ConfiguredJoinPlan::full_schema`,
//! whereas the existing join record set exposes only query projections. This
//! adapter consumes its private full-row stream, applies the typed LIMIT or
//! bounded TopN, and projects just before handing rows to the connection. It
//! owns no read, timestamp, join, or cancellation resource of its own.

use std::{collections::VecDeque, error::Error, fmt};

use tidb_datatype::Datum;
use tidb_planner::{
    configured_join_plan::ConfiguredJoinPlan, configured_order_limit::ConfiguredOrderLimit,
    read_only_scan::ConfiguredTable,
};
use tidb_protocol::ColumnInfo;

use crate::{
    configured_inner_join::{
        configured_join_columns, ConfiguredInnerJoinError, ConfiguredInnerJoinRecordSet,
    },
    configured_topn::{
        ConfiguredLimitEvidence, ConfiguredLimitStream, ConfiguredRowSource, ConfiguredTopN,
        ConfiguredTopNError, ConfiguredTopNEvidence,
    },
    recordset_lifecycle::RecordSetLifecycle,
    Row,
};

const TOPN_PULL_ROWS: usize = 128;

/// Failure while applying a configured terminal ORDER BY/LIMIT operator.
#[derive(Debug)]
pub enum ConfiguredOrderedQueryError {
    /// The one underlying configured join failed or was cancelled.
    Join(ConfiguredInnerJoinError),
    /// The typed bounded TopN rejected its capacity or a physical full row.
    TopN(ConfiguredTopNError),
    /// A prepared terminal tail was attached to a different physical schema.
    PreparedTailSchemaWidth {
        /// FullSchema width checked before the join was opened.
        expected: usize,
        /// FullSchema width exposed by the supplied opened join.
        actual: usize,
    },
}

impl fmt::Display for ConfiguredOrderedQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Join(error) => error.fmt(formatter),
            Self::TopN(error) => error.fmt(formatter),
            Self::PreparedTailSchemaWidth { expected, actual } => write!(
                formatter,
                "prepared configured ordered tail expects FullSchema width {expected}, got {actual}"
            ),
        }
    }
}

impl Error for ConfiguredOrderedQueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Join(error) => Some(error),
            Self::TopN(error) => Some(error),
            Self::PreparedTailSchemaWidth { .. } => None,
        }
    }
}

impl From<ConfiguredInnerJoinError> for ConfiguredOrderedQueryError {
    fn from(error: ConfiguredInnerJoinError) -> Self {
        Self::Join(error)
    }
}

impl From<ConfiguredTopNError> for ConfiguredOrderedQueryError {
    fn from(error: ConfiguredTopNError) -> Self {
        Self::TopN(error)
    }
}

/// Immutable terminal accounting available after the tail has completed.
///
/// A bounded TopN is complete after it has consumed the joined stream and
/// finalized its heap. A streaming LIMIT is complete once it has closed its
/// exact upstream, either at the typed window boundary or during record-set
/// finish/close.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredOrderedQueryEvidence {
    /// Bounded TopN capacity, high-water mark, consumed, and emitted rows.
    TopN(ConfiguredTopNEvidence),
    /// Streaming LIMIT consumed, emitted, and upstream-close accounting.
    Limit(ConfiguredLimitEvidence),
}

enum PreparedTailState {
    Limit(ConfiguredLimitStream),
    TopN(ConfiguredTopN),
}

/// A terminal tail fully validated before a configured join is opened.
///
/// Preparing a TopN checks both its capacity admission and every resolved
/// FullSchema key offset without contacting PD/TiKV. The state is then moved
/// into exactly one opened join, so server routing has no second validation or
/// allocation step hidden after reader creation.
pub struct PreparedConfiguredOrderedQueryTail {
    full_schema_width: usize,
    state: PreparedTailState,
}

impl PreparedConfiguredOrderedQueryTail {
    /// Validates and prepares one typed terminal tail before any join opens.
    pub fn prepare(
        tail: ConfiguredOrderLimit,
        full_schema_width: usize,
        topn_capacity: usize,
    ) -> Result<Self, ConfiguredOrderedQueryError> {
        let state = match tail {
            ConfiguredOrderLimit::Limit(limit) => {
                PreparedTailState::Limit(ConfiguredLimitStream::new(limit))
            }
            ConfiguredOrderLimit::TopN(spec) => PreparedTailState::TopN(ConfiguredTopN::new(
                spec,
                full_schema_width,
                topn_capacity,
            )?),
        };
        Ok(Self {
            full_schema_width,
            state,
        })
    }

    /// Attaches this once-only prepared tail to its matching opened join.
    pub fn attach(
        self,
        mut inner: ConfiguredInnerJoinRecordSet,
    ) -> Result<ConfiguredOrderedQueryRecordSet, ConfiguredOrderedQueryError> {
        let actual = inner.full_schema_width();
        if actual != self.full_schema_width {
            inner.cancel();
            let _ = inner.close();
            return Err(ConfiguredOrderedQueryError::PreparedTailSchemaWidth {
                expected: self.full_schema_width,
                actual,
            });
        }
        let tail = match self.state {
            PreparedTailState::Limit(limit) => OrderedTail::Limit(limit),
            PreparedTailState::TopN(state) => OrderedTail::TopN {
                state: Some(state),
                rows: VecDeque::new(),
                evidence: None,
            },
        };
        Ok(ConfiguredOrderedQueryRecordSet {
            source: OrderedQuerySource::Joined(Box::new(inner)),
            tail,
        })
    }
}

enum OrderedTail {
    Empty,
    Limit(ConfiguredLimitStream),
    TopN {
        state: Option<ConfiguredTopN>,
        rows: VecDeque<Row>,
        evidence: Option<ConfiguredTopNEvidence>,
    },
}

enum OrderedQuerySource {
    Joined(Box<ConfiguredInnerJoinRecordSet>),
    LocalEmpty {
        columns: Vec<ColumnInfo>,
        lifecycle: RecordSetLifecycle,
    },
}

/// One lazy result-set authority for a planner-bound terminal ORDER BY/LIMIT.
///
/// LIMIT-only calls preserve the join's caller-sized pull boundary and close
/// upstream at the exact typed window. TopN consumes the full joined stream
/// once into its bounded heap because no candidate can be omitted before all
/// configured keys have been seen.
pub struct ConfiguredOrderedQueryRecordSet {
    source: OrderedQuerySource,
    tail: OrderedTail,
}

impl ConfiguredOrderedQueryRecordSet {
    /// Attaches one typed terminal tail to an already-open configured join.
    ///
    /// `topn_capacity` is a caller-owned admission bound for the bounded heap;
    /// it is not a fallback, spill, or full-sort quota.
    pub fn new(
        inner: ConfiguredInnerJoinRecordSet,
        tail: ConfiguredOrderLimit,
        topn_capacity: usize,
    ) -> Result<Self, ConfiguredOrderedQueryError> {
        let prepared = PreparedConfiguredOrderedQueryTail::prepare(
            tail,
            inner.full_schema_width(),
            topn_capacity,
        )?;
        prepared.attach(inner)
    }

    /// Returns an already-empty result with exactly normal join metadata.
    ///
    /// This is for planner-known local emptiness such as `LIMIT 0`; it does
    /// not create a timestamp, reader, transport, or cancellation authority.
    pub fn local_empty(
        plan: &ConfiguredJoinPlan,
        tables: [&ConfiguredTable; 2],
    ) -> Result<Self, ConfiguredOrderedQueryError> {
        Ok(Self {
            source: OrderedQuerySource::LocalEmpty {
                columns: configured_join_columns(plan, tables)?,
                lifecycle: RecordSetLifecycle::default(),
            },
            tail: OrderedTail::Empty,
        })
    }

    /// Returns MySQL metadata in the configured query projection order.
    #[must_use]
    pub fn columns(&self) -> &[tidb_protocol::ColumnInfo] {
        match &self.source {
            OrderedQuerySource::Joined(inner) => inner.columns(),
            OrderedQuerySource::LocalEmpty { columns, .. } => columns,
        }
    }

    /// Returns the one snapshot shared by both physical join inputs.
    #[must_use]
    pub const fn snapshot_ts(&self) -> Option<u64> {
        match &self.source {
            OrderedQuerySource::Joined(inner) => inner.snapshot_ts(),
            OrderedQuerySource::LocalEmpty { .. } => None,
        }
    }

    /// Returns whether the single underlying statement cancellation fired.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        match &self.source {
            OrderedQuerySource::Joined(inner) => inner.is_cancelled(),
            OrderedQuerySource::LocalEmpty { .. } => false,
        }
    }

    /// Cancels the one underlying configured join and both physical inputs.
    pub fn cancel(&self) {
        if let OrderedQuerySource::Joined(inner) = &self.source {
            inner.cancel();
        }
    }

    /// Pulls up to `required_rows` query-projected rows.
    pub fn next_batch(
        &mut self,
        required_rows: usize,
    ) -> Result<Vec<Vec<Datum>>, ConfiguredOrderedQueryError> {
        if required_rows == 0 {
            return Ok(Vec::new());
        }
        if let OrderedQuerySource::LocalEmpty { lifecycle, .. } = &mut self.source {
            lifecycle.mark_advanced();
            return Ok(Vec::new());
        }
        if matches!(&self.tail, OrderedTail::Empty) {
            return Ok(Vec::new());
        }
        if matches!(&self.tail, OrderedTail::Limit(_)) {
            let mut output = Vec::with_capacity(required_rows);
            while output.len() < required_rows {
                let next = {
                    let (OrderedQuerySource::Joined(inner), OrderedTail::Limit(limit)) =
                        (&mut self.source, &mut self.tail)
                    else {
                        unreachable!("LIMIT tail retains one join")
                    };
                    let mut source = InnerFullRowSource { inner };
                    limit.next(&mut source)
                };
                match next? {
                    Some(row) => output.push(row),
                    None => break,
                }
            }
            return Ok(output
                .iter()
                .map(|row| self.project_full_row(row))
                .collect());
        }

        self.drain_topn_once()?;
        let OrderedTail::TopN { rows, .. } = &mut self.tail else {
            unreachable!("only TopN remains after nonempty/non-LIMIT tails")
        };
        let output = rows
            .drain(..required_rows.min(rows.len()))
            .collect::<Vec<_>>();
        Ok(output
            .iter()
            .map(|row| self.project_full_row(row))
            .collect())
    }

    /// Returns terminal accounting after the tail has genuinely completed.
    #[must_use]
    pub fn completed_evidence(&self) -> Option<ConfiguredOrderedQueryEvidence> {
        match &self.tail {
            OrderedTail::Empty => None,
            OrderedTail::Limit(limit) => limit
                .evidence()
                .source_closed()
                .then(|| ConfiguredOrderedQueryEvidence::Limit(limit.evidence())),
            OrderedTail::TopN { evidence, .. } => {
                evidence.map(ConfiguredOrderedQueryEvidence::TopN)
            }
        }
    }

    /// Finishes the exact one underlying join record set.
    pub fn finish(&mut self) -> Result<(), ConfiguredOrderedQueryError> {
        self.close_limit_source();
        match &mut self.source {
            OrderedQuerySource::Joined(inner) => inner.finish().map_err(Into::into),
            OrderedQuerySource::LocalEmpty { lifecycle, .. } => {
                lifecycle.begin_finish();
                Ok(())
            }
        }
    }

    /// Closes the exact one underlying join record set.
    pub fn close(&mut self) -> Result<(), ConfiguredOrderedQueryError> {
        self.close_limit_source();
        match &mut self.source {
            OrderedQuerySource::Joined(inner) => inner.close().map_err(Into::into),
            OrderedQuerySource::LocalEmpty { lifecycle, .. } => {
                if lifecycle.begin_close() {
                    lifecycle.begin_finish();
                }
                Ok(())
            }
        }
    }

    /// Exposes the underlying once-only lifecycle for connection adapters.
    #[must_use]
    pub const fn lifecycle(&self) -> &RecordSetLifecycle {
        match &self.source {
            OrderedQuerySource::Joined(inner) => inner.lifecycle(),
            OrderedQuerySource::LocalEmpty { lifecycle, .. } => lifecycle,
        }
    }

    fn drain_topn_once(&mut self) -> Result<(), ConfiguredOrderedQueryError> {
        let Some(mut topn) = (match &mut self.tail {
            OrderedTail::TopN { state, .. } => state.take(),
            OrderedTail::Empty | OrderedTail::Limit(_) => None,
        }) else {
            return Ok(());
        };
        if topn.is_empty() {
            let result = topn.finish();
            let OrderedTail::TopN { evidence, .. } = &mut self.tail else {
                unreachable!("TopN state did not change while draining")
            };
            *evidence = Some(result.evidence);
            self.close()?;
            return Ok(());
        }

        loop {
            let source_rows = self.join_mut().next_full_batch(TOPN_PULL_ROWS)?;
            if source_rows.is_empty() {
                break;
            }
            for row in source_rows {
                if let Err(error) = topn.push(row) {
                    self.join_mut().cancel();
                    let _ = self.join_mut().close();
                    return Err(error.into());
                }
            }
        }
        let result = topn.finish();
        let OrderedTail::TopN { rows, evidence, .. } = &mut self.tail else {
            unreachable!("TopN state did not change while draining")
        };
        *rows = result.rows.into();
        *evidence = Some(result.evidence);
        Ok(())
    }

    fn join_mut(&mut self) -> &mut ConfiguredInnerJoinRecordSet {
        match &mut self.source {
            OrderedQuerySource::Joined(inner) => inner,
            OrderedQuerySource::LocalEmpty { .. } => {
                unreachable!("only a joined query drains a terminal tail")
            }
        }
    }

    fn project_full_row(&self, row: &[Datum]) -> Vec<Datum> {
        match &self.source {
            OrderedQuerySource::Joined(inner) => inner.project_full_row(row),
            OrderedQuerySource::LocalEmpty { .. } => {
                unreachable!("local empty result cannot project a physical row")
            }
        }
    }

    fn close_limit_source(&mut self) {
        let (OrderedQuerySource::Joined(inner), OrderedTail::Limit(limit)) =
            (&mut self.source, &mut self.tail)
        else {
            return;
        };
        let mut source = InnerFullRowSource { inner };
        limit.close(&mut source);
    }
}

impl Drop for ConfiguredOrderedQueryRecordSet {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

struct InnerFullRowSource<'a> {
    inner: &'a mut ConfiguredInnerJoinRecordSet,
}

impl ConfiguredRowSource for InnerFullRowSource<'_> {
    type Error = ConfiguredInnerJoinError;

    fn next_row(&mut self) -> Result<Option<Row>, Self::Error> {
        let mut rows = self.inner.next_full_batch(1)?;
        Ok(rows.pop())
    }

    fn close(&mut self) {
        let _ = self.inner.close();
    }
}
