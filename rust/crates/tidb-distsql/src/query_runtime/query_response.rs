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

use tidb_datatype::{FieldType, SessionTimeZone};

use crate::distsql_runtime::SelectResultMetadata;
use crate::response_channel::SelectResponseIter;
pub use tidb_txnkv::{QueryResponseError, QueryResultSubset};
use crate::WarningCollector;

/// Pull-based raw response owner: Go's `kv.Response` (`tidb_txnkv::Response`)
/// with what DistSQL's `selectResult` reads off the coprocessor iterator
/// behind it.
pub trait QueryResponse: tidb_txnkv::Response {}

/// Every `kv.Response`: the optional hooks distsql reads (Go's type
/// assertions on `kv.Response`, e.g. `copr.CopInfo`) are the kv trait's own.
impl<R: tidb_txnkv::Response + ?Sized> QueryResponse for R {}

pub use tidb_txnkv::CopInfo;

/// Go-shaped DistSQL result that owns one raw response until conversion or
/// close.
pub struct QuerySelectResult<R: QueryResponse> {
    response: Option<R>,
    metadata: SelectResultMetadata,
    final_field_types: Vec<FieldType>,
    time_zone: SessionTimeZone,
    warnings: WarningCollector,
    runtime_stats_collector_enabled: Option<bool>,
    closed: bool,
}

impl<R: QueryResponse> QuerySelectResult<R> {
    pub(super) fn new(
        response: R,
        metadata: SelectResultMetadata,
        final_field_types: Vec<FieldType>,
        time_zone: SessionTimeZone,
        warnings: WarningCollector,
        runtime_stats_collector_enabled: Option<bool>,
    ) -> Self {
        Self {
            response: Some(response),
            metadata,
            final_field_types,
            time_zone,
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
            // Raw consumers own their copy; the shared buffer stays with
            // the select path, which decodes from it without copying.
            Ok(Some(subset)) => Ok(Some(subset.data.to_vec())),
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
        R: Send + 'static,
    {
        let response = self
            .response
            .take()
            .expect("an open query result owns its response");
        self.closed = true;
        let limiter_wait = response.limiter_wait_stats();
        let mut iter = SelectResponseIter::from_query_response(
            Box::new(response),
            self.final_field_types.clone(),
            intermediate_output_types,
            self.time_zone.clone(),
            self.warnings.clone(),
            self.metadata.clone(),
            self.runtime_stats_collector_enabled,
        );
        iter.record_limiter_wait(limiter_wait);
        iter
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
