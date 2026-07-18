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

//! Direct lifecycle translation of Go TableReaderExecutor and
//! IndexReaderExecutor after request construction.

use tidb_datatype::{Datum, FieldType};
use tidb_distsql::{
    InjectedQueryRuntime, QueryResultContext, QueryRuntimeError, QueryTransport, SelectInput,
    SelectResponseIter, SelectResultError, SelectResultRow, SelectResultSource,
    SerialSelectResults, TransportRequest, WarningCollector,
};

/// The physical reader family whose lifecycle is being executed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderKind {
    /// Go's `TableReaderExecutor`.
    Table,
    /// Go's `IndexReaderExecutor`.
    Index,
}

/// Immutable post-lowering input for one reader executor.
#[derive(Clone, Debug)]
pub struct ReaderPlan {
    kind: ReaderKind,
    requests: Vec<TransportRequest>,
    final_field_types: Vec<FieldType>,
    intermediate_output_types: Vec<Vec<FieldType>>,
    warnings: WarningCollector,
    cop_plan_ids: Vec<isize>,
    root_plan_id: isize,
    runtime_stats_collector_enabled: bool,
    dummy: bool,
}

impl ReaderPlan {
    /// Creates a non-dummy reader over already-built requests in consumption
    /// order.
    #[must_use]
    pub fn new(
        kind: ReaderKind,
        requests: Vec<TransportRequest>,
        final_field_types: Vec<FieldType>,
    ) -> Self {
        Self {
            kind,
            requests,
            final_field_types,
            intermediate_output_types: Vec::new(),
            warnings: WarningCollector::new(),
            cop_plan_ids: Vec::new(),
            root_plan_id: 0,
            runtime_stats_collector_enabled: false,
            dummy: false,
        }
    }

    /// Creates a temporary/cached-table reader that must never call transport.
    #[must_use]
    pub fn dummy(kind: ReaderKind, final_field_types: Vec<FieldType>) -> Self {
        let mut plan = Self::new(kind, Vec::new(), final_field_types);
        plan.dummy = true;
        plan
    }

    /// Replaces the intermediate response-channel layouts.
    #[must_use]
    pub fn with_intermediate_output_types(
        mut self,
        intermediate_output_types: Vec<Vec<FieldType>>,
    ) -> Self {
        self.intermediate_output_types = intermediate_output_types;
        self
    }

    /// Uses the shared statement warning collector for every request result.
    #[must_use]
    pub fn with_warnings(mut self, warnings: WarningCollector) -> Self {
        self.warnings = warnings;
        self
    }

    /// Binds the cop and root plan identifiers used by runtime statistics.
    #[must_use]
    pub fn with_runtime_stats(
        mut self,
        cop_plan_ids: Vec<isize>,
        root_plan_id: isize,
        collector_enabled: bool,
    ) -> Self {
        self.cop_plan_ids = cop_plan_ids;
        self.root_plan_id = root_plan_id;
        self.runtime_stats_collector_enabled = collector_enabled;
        self
    }

    /// Returns the reader family retained from the physical executor.
    #[must_use]
    pub const fn kind(&self) -> ReaderKind {
        self.kind
    }

    /// Returns whether this reader is the no-send temporary/cache-table form.
    #[must_use]
    pub const fn is_dummy(&self) -> bool {
        self.dummy
    }
}

/// Observable reader lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReaderState {
    /// Constructed but not opened.
    Created,
    /// Opened and available for row pulls.
    Open,
    /// Closed explicitly or after an open failure.
    Closed,
}

/// Failure returned by the storage-reader lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageReaderError {
    /// `open` was called after this reader left the created state.
    AlreadyOpened(ReaderState),
    /// A non-dummy reader had no built request to dispatch.
    MissingRequest,
    /// `next` was called before `open` or after `close`.
    NotOpen(ReaderState),
    /// Request dispatch failed before producing a response iterator.
    Query(QueryRuntimeError),
    /// Pulling or decoding a response failed.
    Response(String),
}

impl std::fmt::Display for StorageReaderError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyOpened(state) => {
                write!(formatter, "storage reader cannot open from state {state:?}")
            }
            Self::MissingRequest => {
                formatter.write_str("non-dummy storage reader has no built request")
            }
            Self::NotOpen(state) => {
                write!(formatter, "storage reader cannot read from state {state:?}")
            }
            Self::Query(error) => write!(formatter, "storage reader dispatch failed: {error}"),
            Self::Response(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for StorageReaderError {}

impl From<QueryRuntimeError> for StorageReaderError {
    fn from(error: QueryRuntimeError) -> Self {
        Self::Query(error)
    }
}

/// Executor-owned TableReader/IndexReader response lifecycle.
pub struct TableIndexReader<T: QueryTransport>
where
    T::Response: 'static,
{
    plan: ReaderPlan,
    runtime: InjectedQueryRuntime<T>,
    results: Option<SerialSelectResults<DecodedSelectSource>>,
    state: ReaderState,
}

impl<T: QueryTransport> TableIndexReader<T>
where
    T::Response: 'static,
{
    /// Binds a post-lowering reader plan to its transport capability.
    #[must_use]
    pub const fn new(plan: ReaderPlan, transport: T) -> Self {
        Self {
            plan,
            runtime: InjectedQueryRuntime::new(transport),
            results: None,
            state: ReaderState::Created,
        }
    }

    /// Returns the physical reader family.
    #[must_use]
    pub const fn kind(&self) -> ReaderKind {
        self.plan.kind()
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> ReaderState {
        self.state
    }

    /// Opens every request in source order and transfers each response owner
    /// exactly once into its decoded iterator.
    ///
    /// If a later send fails, all earlier response owners are closed before
    /// the error is returned. A dummy reader never enters the transport path.
    pub fn open(&mut self) -> Result<(), StorageReaderError> {
        if self.state != ReaderState::Created {
            return Err(StorageReaderError::AlreadyOpened(self.state));
        }
        if self.plan.is_dummy() {
            self.results = Some(SerialSelectResults::new(Vec::new()));
            self.state = ReaderState::Open;
            return Ok(());
        }
        if self.plan.requests.is_empty() {
            return Err(StorageReaderError::MissingRequest);
        }

        let mut opened = Vec::with_capacity(self.plan.requests.len());
        for request in &self.plan.requests {
            let result = match self.runtime.select_with_runtime_stats(
                request,
                SelectInput::default(),
                QueryResultContext::new(
                    self.plan.final_field_types.clone(),
                    self.plan.warnings.clone(),
                ),
                self.plan.cop_plan_ids.clone(),
                self.plan.root_plan_id,
                self.plan.runtime_stats_collector_enabled,
            ) {
                Ok(result) => result,
                Err(error) => {
                    close_opened(&mut opened);
                    self.state = ReaderState::Closed;
                    return Err(error.into());
                }
            };
            opened.push(DecodedSelectSource::new(
                result.into_select_iter(self.plan.intermediate_output_types.clone()),
            ));
        }
        self.results = Some(SerialSelectResults::new(opened));
        self.state = ReaderState::Open;
        Ok(())
    }

    /// Pulls at most `required_rows` rows, preserving request order without
    /// probing one row beyond the caller's bound.
    pub fn next(&mut self, required_rows: usize) -> Result<Vec<Vec<Datum>>, StorageReaderError> {
        if self.state != ReaderState::Open {
            return Err(StorageReaderError::NotOpen(self.state));
        }
        if required_rows == 0 {
            return Ok(Vec::new());
        }
        let results = self
            .results
            .as_mut()
            .expect("an open reader owns its serial result set");
        let mut rows = Vec::with_capacity(required_rows);
        while rows.len() < required_rows {
            let Some(row) = results
                .next_row_with_required_rows(required_rows - rows.len())
                .map_err(|error| StorageReaderError::Response(error.to_string()))?
            else {
                break;
            };
            rows.push(row.row);
        }
        Ok(rows)
    }

    /// Closes every opened response. Repeated close calls are no-ops.
    pub fn close(&mut self) {
        if self.state == ReaderState::Closed {
            return;
        }
        if let Some(results) = &mut self.results {
            let _ = results.close();
        }
        self.results = None;
        self.state = ReaderState::Closed;
    }
}

impl<T: QueryTransport> Drop for TableIndexReader<T>
where
    T::Response: 'static,
{
    fn drop(&mut self) {
        self.close();
    }
}

fn close_opened(opened: &mut [DecodedSelectSource]) {
    for source in opened {
        let _ = source.close();
    }
}

struct DecodedSelectSource {
    iter: SelectResponseIter,
}

impl DecodedSelectSource {
    const fn new(iter: SelectResponseIter) -> Self {
        Self { iter }
    }
}

impl SelectResultSource for DecodedSelectSource {
    type Row = Vec<Datum>;

    fn next_row(&mut self) -> Result<Option<SelectResultRow<Self::Row>>, SelectResultError> {
        self.iter
            .next_row()
            .map_err(|error| SelectResultError::source(error.to_string()))
    }

    fn next_row_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<SelectResultRow<Self::Row>>, SelectResultError> {
        self.iter
            .next_row_with_required_rows(required_rows)
            .map_err(|error| SelectResultError::source(error.to_string()))
    }

    fn close(&mut self) -> Result<(), SelectResultError> {
        self.iter.close();
        Ok(())
    }
}
