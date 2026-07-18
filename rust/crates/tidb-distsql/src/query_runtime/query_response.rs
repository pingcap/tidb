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

//! Pull-based ownership boundary corresponding to Go's `kv.Response` and
//! `selectResult`.

use tidb_datatype::FieldType;

use crate::distsql_runtime::SelectResultMetadata;
use crate::response_channel::{ResponseRuntimeStats, SelectResponseIter};
use crate::warning::WarningCollector;

/// One response subset returned by a single pull from the transport response.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryResultSubset {
    /// Raw protobuf response bytes. Decoding belongs to the select consumer.
    pub data: Vec<u8>,
    /// Runtime data returned by the same response pull, when present.
    pub runtime: Option<ResponseRuntimeStats>,
}

/// Error returned while pulling an already-created response owner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryResponseError {
    /// The deterministic adapter is still open but has no subset available.
    Pending,
    /// The response source returned its first terminal error.
    Source(String),
}

impl std::fmt::Display for QueryResponseError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => formatter.write_str("DistSQL query response is still pending"),
            Self::Source(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for QueryResponseError {}

/// Pull-based raw response owner corresponding to Go's `kv.Response`.
pub trait QueryResponse {
    /// Pulls one raw result subset, or `None` after natural exhaustion.
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError>;

    /// Pulls one raw result subset while forwarding the caller's remaining row
    /// budget to response owners that can use it.
    ///
    /// Existing transports remain source-compatible: the default preserves the
    /// original unbudgeted pull. Bounded adapters may override this method to
    /// avoid materializing rows that the executor did not request.
    fn next_with_required_rows(
        &mut self,
        _required_rows: usize,
    ) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        self.next()
    }

    /// Closes the response owner and releases any unconsumed subsets.
    fn close(&mut self);
}

/// Go-shaped DistSQL result that owns one raw response until conversion or
/// close.
pub struct QuerySelectResult<R: QueryResponse> {
    response: Option<R>,
    metadata: SelectResultMetadata,
    final_field_types: Vec<FieldType>,
    warnings: WarningCollector,
    runtime_stats_collector_enabled: Option<bool>,
    closed: bool,
}

impl<R: QueryResponse> QuerySelectResult<R> {
    pub(super) fn new(
        response: R,
        metadata: SelectResultMetadata,
        final_field_types: Vec<FieldType>,
        warnings: WarningCollector,
        runtime_stats_collector_enabled: Option<bool>,
    ) -> Self {
        Self {
            response: Some(response),
            metadata,
            final_field_types,
            warnings,
            runtime_stats_collector_enabled,
            closed: false,
        }
    }

    /// Borrows the result metadata fixed before the transport send.
    #[must_use]
    pub const fn result_metadata(&self) -> &SelectResultMetadata {
        &self.metadata
    }

    /// Pulls raw response bytes without decoding them.
    ///
    /// This is the path used by Analyze and Checksum. Runtime metadata stays
    /// attached to the subset for Select consumption and is deliberately not
    /// reinterpreted by raw callers.
    pub fn next_raw(&mut self) -> Result<Option<Vec<u8>>, QueryResponseError> {
        if self.closed {
            return Ok(None);
        }
        let response = self
            .response
            .as_mut()
            .expect("an open query result owns its response");
        match response.next() {
            Ok(Some(subset)) => Ok(Some(subset.data)),
            Ok(None) => {
                self.close();
                Ok(None)
            }
            Err(error) => {
                self.close();
                Err(error)
            }
        }
    }

    /// Transfers the sole raw response owner into the existing lazy decoder.
    ///
    /// The conversion consumes `self`, so the same subset cannot be read raw
    /// and decoded, or decoded twice.
    #[must_use]
    pub fn into_select_iter(
        mut self,
        intermediate_output_types: Vec<Vec<FieldType>>,
    ) -> SelectResponseIter
    where
        R: 'static,
    {
        let response = self
            .response
            .take()
            .expect("an open query result owns its response");
        self.closed = true;
        SelectResponseIter::from_query_response(
            Box::new(response),
            self.final_field_types.clone(),
            intermediate_output_types,
            self.warnings.clone(),
            self.metadata.clone(),
            self.runtime_stats_collector_enabled,
        )
    }

    /// Closes the raw response owner. Closing is idempotent.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        if let Some(response) = &mut self.response {
            response.close();
        }
        self.closed = true;
    }

    /// Reports whether this result has closed or transferred its response.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }
}

impl<R: QueryResponse> Drop for QuerySelectResult<R> {
    fn drop(&mut self) {
        self.close();
    }
}
