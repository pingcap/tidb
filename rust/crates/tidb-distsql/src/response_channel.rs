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

//! Owned response-channel lifecycle for the DistSQL result path.
//!
//! Go's `selectResult.fetchRespWithIntermediateResults` receives one owned
//! response at a time, appends response warnings, returns a source error, and
//! closes the underlying response after consumption.  This leaf preserves the
//! lifecycle across the checked-in protobuf and chunk decoders: results and
//! warnings are ordered events, errors are terminal state, and
//! finishing/closing is idempotent. A transport owner still supplies the raw
//! response bytes; this module never fabricates a TiKV RPC resource.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;

use tidb_codec::ColumnLayout;
use tidb_datatype::{Datum, FieldType};
use tidb_proto::{Chunk, EncodeType, ExecutorExecutionSummary, SelectResponse};

use super::channel_iter::{ChannelIter, ChannelIterError};
use super::chunk_decode::{decode_chunk, decode_select_response, ChunkDecodeError};
use super::distsql_runtime::{SelectResultMetadata, SelectResultRuntimeStats};
use super::query_runtime::{QueryResponse, QueryResponseError, QueryResultSubset};
use super::select_iter::SelectResultRow;
use super::warning::{Warning, WarningCollector, WarningLevel};

/// One already-owned event from a response stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResponseChannelEvent<T> {
    /// One already-decoded result payload.
    Result(T),
    /// One result accompanied by the CopRuntimeStats presence and subset data
    /// returned by the same transport call.
    ResultWithRuntime {
        /// Owned result payload.
        result: T,
        /// Runtime sample from that exact response subset.
        runtime_stats: ResponseRuntimeStats,
    },
    /// A warning attached to the response that produced the result.
    Warning(Warning),
    /// A terminal source error, emitted once before [`Self::Closed`].
    Error(String),
    /// The response source reached its end or was explicitly closed.
    Closed,
}

/// Non-protobuf CopRuntimeStats data attached to one response subset.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResponseRuntimeStats {
    /// TiKV callee address, which may be empty when request RPC stats exist.
    pub callee_address: String,
    /// Whether request stats reported a nonzero RPCStatsCount, which satisfies
    /// the source empty-callee gate.
    pub request_rpc_stats_present: bool,
    /// Backoff sleep samples owned by this response subset.
    pub backoff_sleep_ns: Vec<(String, u64)>,
}

/// Lifecycle state of an owned response channel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseChannelState {
    /// The producer may append results and warnings.
    Open,
    /// A source error was recorded and will terminate the stream.
    Failed,
    /// The stream has finished or has been explicitly closed.
    Closed,
}

impl fmt::Display for ResponseChannelState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Open => "open",
            Self::Failed => "failed",
            Self::Closed => "closed",
        };
        f.write_str(name)
    }
}

/// Capabilities that remain owned by future response/transport layers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseChannelUnsupported {
    /// Legacy marker for a caller that requests decoding without using the
    /// typed `ResponseChannel<Vec<u8>>::into_select_iter` entry point.
    RawTipbResponse,
    /// Receiving from TiKV's transport-backed response channel is outside
    /// this leaf.
    TiKvResponseChannel,
    /// A select iterator already owns a pull-based query response and cannot
    /// be mutated through the deterministic channel adapter.
    TransportOwnedResponseMutation,
}

impl fmt::Display for ResponseChannelUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::RawTipbResponse => "raw tipb response",
            Self::TiKvResponseChannel => "TiKV response channel",
            Self::TransportOwnedResponseMutation => "transport-owned query response mutation",
        };
        f.write_str(name)
    }
}

/// Errors raised when a caller crosses an owned response-channel boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResponseChannelError {
    /// The requested producer operation is invalid in the current state.
    InvalidState {
        /// State in which the operation was attempted.
        state: ResponseChannelState,
        /// Name of the attempted operation.
        operation: &'static str,
    },
    /// The selected legacy or transport capability has no bound owner.
    Unsupported(ResponseChannelUnsupported),
    /// Raw bytes were not a valid checked-in `tipb.SelectResponse`.
    Decode(String),
    /// The response carried a TiKV error.
    SelectResponse {
        /// TiKV error code.
        code: i32,
        /// TiKV error message.
        message: String,
    },
    /// The caller's intermediate schemas do not match the response layout.
    IntermediateOutputCount {
        /// Number of intermediate schemas supplied by the caller.
        expected: usize,
        /// Number of intermediate outputs in the response.
        actual: usize,
    },
    /// A checked row decoder rejected the response payload.
    RowDecode(String),
    /// An owned response source reported a terminal error.
    Source(String),
    /// The owned source is still open but has no response available yet.
    Pending,
}

impl ResponseChannelError {
    /// Creates the legacy error for callers outside the typed raw-byte entry.
    #[must_use]
    pub const fn unsupported_raw_tipb_response() -> Self {
        Self::Unsupported(ResponseChannelUnsupported::RawTipbResponse)
    }

    /// Creates the explicit TiKV response-channel boundary error.
    #[must_use]
    pub const fn unsupported_tikv_response_channel() -> Self {
        Self::Unsupported(ResponseChannelUnsupported::TiKvResponseChannel)
    }
}

impl fmt::Display for ResponseChannelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidState { state, operation } => {
                write!(f, "cannot {operation} response channel in {state} state")
            }
            Self::Unsupported(capability) => {
                write!(f, "unsupported DistSQL response capability: {capability}")
            }
            Self::Decode(message) => write!(f, "invalid tipb SelectResponse: {message}"),
            Self::SelectResponse { code, message } => {
                write!(f, "TiKV select response error {code}: {message}")
            }
            Self::IntermediateOutputCount { expected, actual } => write!(
                f,
                "the length of intermediate output types {expected} mismatches the length of got intermediate outputs {actual}"
            ),
            Self::RowDecode(message) => write!(f, "failed to decode select response row: {message}"),
            Self::Source(message) => f.write_str(message),
            Self::Pending => f.write_str("DistSQL response source is still open and pending"),
        }
    }
}

impl Error for ResponseChannelError {}

/// Owned, deterministic response-channel state machine.
///
/// An open channel may contain result and warning events.  [`Self::fail`]
/// records one source error and drops no already-owned events; the error is
/// emitted after those events, then the stream emits [`ResponseChannelEvent::Closed`].
/// [`Self::finish`] has the same close marker without an error.  Explicit
/// [`Self::close`] drops pending events immediately, matching a consumer that
/// abandons a Go `SelectResultIter`.
#[derive(Debug)]
pub struct ResponseChannel<T> {
    events: VecDeque<ResponseChannelEvent<T>>,
    state: ResponseChannelState,
    terminal_error: Option<String>,
    close_emitted: bool,
}

impl<T> Default for ResponseChannel<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ResponseChannel<T> {
    /// Creates an empty open channel.
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            state: ResponseChannelState::Open,
            terminal_error: None,
            close_emitted: false,
        }
    }

    /// Creates an open channel from already-owned events.
    #[must_use]
    pub fn from_events(events: impl IntoIterator<Item = ResponseChannelEvent<T>>) -> Self {
        Self {
            events: events.into_iter().collect(),
            ..Self::new()
        }
    }

    /// Returns the current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> ResponseChannelState {
        self.state
    }

    /// Returns whether the channel has emitted its terminal close marker.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.close_emitted
    }

    /// Appends an already-owned result to an open channel.
    pub fn push_result(&mut self, result: T) -> Result<(), ResponseChannelError> {
        self.ensure_open("append result")?;
        self.events.push_back(ResponseChannelEvent::Result(result));
        Ok(())
    }

    /// Appends a result and the CopRuntimeStats sample returned beside it.
    pub fn push_result_with_runtime(
        &mut self,
        result: T,
        runtime_stats: ResponseRuntimeStats,
    ) -> Result<(), ResponseChannelError> {
        self.ensure_open("append result with runtime stats")?;
        self.events
            .push_back(ResponseChannelEvent::ResultWithRuntime {
                result,
                runtime_stats,
            });
        Ok(())
    }

    /// Appends an already-owned warning to an open channel.
    pub fn push_warning(&mut self, warning: Warning) -> Result<(), ResponseChannelError> {
        self.ensure_open("append warning")?;
        self.events
            .push_back(ResponseChannelEvent::Warning(warning));
        Ok(())
    }

    /// Appends a regular warning while preserving TiDB's warning level.
    pub fn push_warning_message(
        &mut self,
        level: WarningLevel,
        message: impl Into<String>,
    ) -> Result<(), ResponseChannelError> {
        self.push_warning(Warning {
            level,
            class: super::warning::WarningClass::Statement,
            code: None,
            message: message.into(),
        })
    }

    /// Records a terminal source error.
    ///
    /// The error is emitted after events already owned by this channel.  A
    /// second failure is rejected rather than replacing the first error.
    pub fn fail(&mut self, message: impl Into<String>) -> Result<(), ResponseChannelError> {
        self.ensure_open("fail")?;
        self.state = ResponseChannelState::Failed;
        self.terminal_error = Some(message.into());
        Ok(())
    }

    /// Marks an open channel as successfully finished.
    ///
    /// The marker is emitted once all already-owned events have been drained.
    /// Calling `finish` again after completion is harmless.
    pub fn finish(&mut self) -> Result<(), ResponseChannelError> {
        match self.state {
            ResponseChannelState::Open => {
                self.state = ResponseChannelState::Closed;
                Ok(())
            }
            ResponseChannelState::Closed => Ok(()),
            ResponseChannelState::Failed => Err(ResponseChannelError::InvalidState {
                state: self.state,
                operation: "finish",
            }),
        }
    }

    /// Explicitly closes the channel and drops pending owned events.
    ///
    /// Closing is idempotent and does not claim to close an external TiKV
    /// resource.  That resource remains an explicit unsupported boundary.
    pub fn close(&mut self) {
        self.events.clear();
        self.terminal_error = None;
        self.state = ResponseChannelState::Closed;
        self.close_emitted = true;
    }

    /// Returns the next owned lifecycle event.
    ///
    /// An open channel with no queued event returns `None` because a
    /// future response may still arrive.  Once finished, the method emits one
    /// [`ResponseChannelEvent::Closed`] marker and then remains drained.
    pub fn next_event(&mut self) -> Option<ResponseChannelEvent<T>> {
        if let Some(event) = self.events.pop_front() {
            return Some(event);
        }

        if self.state == ResponseChannelState::Failed {
            self.state = ResponseChannelState::Closed;
            return self.terminal_error.take().map(ResponseChannelEvent::Error);
        }

        if self.state == ResponseChannelState::Closed && !self.close_emitted {
            self.close_emitted = true;
            return Some(ResponseChannelEvent::Closed);
        }

        None
    }

    fn ensure_open(&self, operation: &'static str) -> Result<(), ResponseChannelError> {
        if self.state == ResponseChannelState::Open {
            Ok(())
        } else {
            Err(ResponseChannelError::InvalidState {
                state: self.state,
                operation,
            })
        }
    }
}

impl QueryResponse for ResponseChannel<Vec<u8>> {
    fn next(&mut self) -> Result<Option<QueryResultSubset>, QueryResponseError> {
        match self.next_event() {
            Some(ResponseChannelEvent::Result(data)) => Ok(Some(QueryResultSubset {
                data,
                runtime: None,
            })),
            Some(ResponseChannelEvent::ResultWithRuntime {
                result,
                runtime_stats,
            }) => Ok(Some(QueryResultSubset {
                data: result,
                runtime: Some(runtime_stats),
            })),
            Some(ResponseChannelEvent::Error(message)) => Err(QueryResponseError::Source(message)),
            Some(ResponseChannelEvent::Closed) => Ok(None),
            Some(ResponseChannelEvent::Warning(_)) => Err(QueryResponseError::Source(
                "standalone response-channel warnings are not part of the raw kv.Response contract"
                    .to_owned(),
            )),
            None => Err(QueryResponseError::Pending),
        }
    }

    fn close(&mut self) {
        ResponseChannel::close(self);
    }
}

enum SelectResponseSource {
    Channel(ResponseChannel<Vec<u8>>),
    Query(Box<dyn QueryResponse>),
}

impl SelectResponseSource {
    fn next_event(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<ResponseChannelEvent<Vec<u8>>>, ResponseChannelError> {
        match self {
            Self::Channel(source) => Ok(source.next_event()),
            Self::Query(source) => match source.next_with_required_rows(required_rows) {
                Ok(Some(subset)) => Ok(Some(match subset.runtime {
                    Some(runtime_stats) => ResponseChannelEvent::ResultWithRuntime {
                        result: subset.data,
                        runtime_stats,
                    },
                    None => ResponseChannelEvent::Result(subset.data),
                })),
                Ok(None) => {
                    source.close();
                    Ok(Some(ResponseChannelEvent::Closed))
                }
                Err(QueryResponseError::Pending) => Err(ResponseChannelError::Pending),
                Err(QueryResponseError::Source(message)) => {
                    Err(ResponseChannelError::Source(message))
                }
            },
        }
    }

    fn close(&mut self) {
        match self {
            Self::Channel(source) => source.close(),
            Self::Query(source) => source.close(),
        }
    }

    fn push_result(&mut self, bytes: Vec<u8>) -> Result<(), ResponseChannelError> {
        match self {
            Self::Channel(source) => source.push_result(bytes),
            Self::Query(_) => Err(ResponseChannelError::Unsupported(
                ResponseChannelUnsupported::TransportOwnedResponseMutation,
            )),
        }
    }

    fn push_result_with_runtime(
        &mut self,
        bytes: Vec<u8>,
        runtime_stats: ResponseRuntimeStats,
    ) -> Result<(), ResponseChannelError> {
        match self {
            Self::Channel(source) => source.push_result_with_runtime(bytes, runtime_stats),
            Self::Query(_) => Err(ResponseChannelError::Unsupported(
                ResponseChannelUnsupported::TransportOwnedResponseMutation,
            )),
        }
    }

    fn finish(&mut self) -> Result<(), ResponseChannelError> {
        match self {
            Self::Channel(source) => source.finish(),
            Self::Query(_) => Err(ResponseChannelError::Unsupported(
                ResponseChannelUnsupported::TransportOwnedResponseMutation,
            )),
        }
    }
}

impl ResponseChannel<Vec<u8>> {
    /// Converts this raw response source into the sole decoded row iterator.
    ///
    /// Consuming `self` makes the Go `selectResult` invalidation rule a Rust
    /// ownership invariant: raw reads, a second conversion, and direct close
    /// cannot race the returned iterator. The iterator itself owns final close.
    pub fn into_select_iter(
        self,
        final_field_types: Vec<FieldType>,
        intermediate_output_types: Vec<Vec<FieldType>>,
        warnings: WarningCollector,
    ) -> SelectResponseIter {
        SelectResponseIter {
            source: SelectResponseSource::Channel(self),
            final_field_types,
            intermediate_output_types,
            warnings,
            channels: Vec::new(),
            runtime_stats: SelectResultRuntimeStats::default(),
            runtime_stats_binding: None,
            result_metadata: None,
            closed: false,
        }
    }
}

/// Connected decoder and reverse-priority iterator for raw select responses.
///
/// Each response is decoded once, then represented by the existing
/// [`ChannelIter`] authority. The final channel is consumed first, followed by
/// intermediate channels from highest to lowest index, exactly like Go's
/// `selectResultIter.Next`.
pub struct SelectResponseIter {
    source: SelectResponseSource,
    final_field_types: Vec<FieldType>,
    intermediate_output_types: Vec<Vec<FieldType>>,
    warnings: WarningCollector,
    channels: Vec<SelectResponseChannel>,
    runtime_stats: SelectResultRuntimeStats,
    runtime_stats_binding: Option<RuntimeStatsBinding>,
    result_metadata: Option<SelectResultMetadata>,
    closed: bool,
}

#[derive(Clone, Debug)]
struct RuntimeStatsBinding {
    metadata: SelectResultMetadata,
    collector_enabled: bool,
}

#[derive(Debug)]
struct SelectResponseChannel {
    channel_index: usize,
    raw_encode_type: Option<i32>,
    chunks: Vec<Chunk>,
    field_types: Vec<FieldType>,
    next_chunk_index: usize,
    decoded: Option<ChannelIter<Vec<Datum>>>,
}

impl SelectResponseChannel {
    fn next_row(
        &mut self,
    ) -> Result<Option<super::channel_iter::ChannelRow<Vec<Datum>>>, ResponseChannelError> {
        loop {
            if let Some(decoded) = &mut self.decoded {
                if let Some(row) = decoded.next_row().map_err(map_channel_error)? {
                    return Ok(Some(row));
                }
                self.decoded = None;
            }
            let Some(chunk) = self.chunks.get(self.next_chunk_index) else {
                return Ok(None);
            };
            let decoded = decode_channel(
                self.channel_index,
                self.raw_encode_type,
                std::slice::from_ref(chunk),
                &self.field_types,
            )?;
            self.next_chunk_index += 1;
            self.decoded = Some(decoded);
        }
    }

    fn close(&mut self) {
        if let Some(decoded) = &mut self.decoded {
            decoded.close();
        }
        self.chunks.clear();
    }
}

impl SelectResponseIter {
    pub(crate) fn from_query_response(
        response: Box<dyn QueryResponse>,
        final_field_types: Vec<FieldType>,
        intermediate_output_types: Vec<Vec<FieldType>>,
        warnings: WarningCollector,
        result_metadata: SelectResultMetadata,
        runtime_stats_collector_enabled: Option<bool>,
    ) -> Self {
        let runtime_stats_binding =
            runtime_stats_collector_enabled.map(|collector_enabled| RuntimeStatsBinding {
                metadata: result_metadata.clone(),
                collector_enabled,
            });
        Self {
            source: SelectResponseSource::Query(response),
            final_field_types,
            intermediate_output_types,
            warnings,
            channels: Vec::new(),
            runtime_stats: SelectResultRuntimeStats::default(),
            runtime_stats_binding,
            result_metadata: Some(result_metadata),
            closed: false,
        }
    }

    /// Returns the next decoded row or `None` after the response source closes.
    pub fn next_row(
        &mut self,
    ) -> Result<Option<SelectResultRow<Vec<Datum>>>, ResponseChannelError> {
        self.next_row_with_required_rows(1)
    }

    /// Returns the next decoded row while forwarding the caller's remaining
    /// row budget to the raw response owner when another response must be
    /// pulled. A zero budget consumes nothing.
    pub fn next_row_with_required_rows(
        &mut self,
        required_rows: usize,
    ) -> Result<Option<SelectResultRow<Vec<Datum>>>, ResponseChannelError> {
        if required_rows == 0 {
            return Ok(None);
        }
        if self.closed {
            return Ok(None);
        }
        loop {
            while let Some(channel) = self.channels.last_mut() {
                match channel.next_row()? {
                    Some(row) => return Ok(Some(SelectResultRow::new(row.channel_index, row.row))),
                    None => {
                        self.channels.pop();
                    }
                }
            }

            let event = match self.source.next_event(required_rows) {
                Ok(event) => event,
                Err(ResponseChannelError::Pending) => return Err(ResponseChannelError::Pending),
                Err(error) => {
                    self.close();
                    return Err(error);
                }
            };
            match event {
                Some(ResponseChannelEvent::Result(bytes)) => {
                    if let Err(error) = self.install_encoded_response(&bytes, None) {
                        self.close();
                        return Err(error);
                    }
                }
                Some(ResponseChannelEvent::ResultWithRuntime {
                    result,
                    runtime_stats,
                }) => {
                    if let Err(error) = self.install_encoded_response(&result, Some(&runtime_stats))
                    {
                        self.close();
                        return Err(error);
                    }
                }
                Some(ResponseChannelEvent::Warning(warning)) => {
                    append_warning(&self.warnings, warning);
                }
                Some(ResponseChannelEvent::Error(message)) => {
                    let error = ResponseChannelError::Source(message);
                    self.close();
                    return Err(error);
                }
                Some(ResponseChannelEvent::Closed) => {
                    self.closed = true;
                    return Ok(None);
                }
                None => return Err(ResponseChannelError::Pending),
            }
        }
    }

    /// Closes the underlying response source and drops pending decoded rows.
    pub fn close(&mut self) {
        if self.closed {
            return;
        }
        for channel in &mut self.channels {
            channel.close();
        }
        self.channels.clear();
        self.source.close();
        self.closed = true;
    }

    /// Returns whether close or natural source exhaustion has occurred.
    #[must_use]
    pub const fn is_closed(&self) -> bool {
        self.closed
    }

    /// Merges one response subset's runtime sample into this consumer.
    ///
    /// The caller supplies the non-protobuf coprocessor detail carried beside
    /// the raw response; the checked execution summaries remain authoritative
    /// tipb values. This delegates to the single runtime-stat accumulator.
    pub fn update_runtime_stats(
        &mut self,
        metadata: &SelectResultMetadata,
        collector_enabled: bool,
        callee_address: &str,
        request_rpc_stats_present: bool,
        backoff_sleep_ns: impl IntoIterator<Item = (String, u64)>,
        execution_summaries: &[ExecutorExecutionSummary],
    ) {
        self.runtime_stats.update(
            metadata,
            collector_enabled,
            callee_address,
            request_rpc_stats_present,
            backoff_sleep_ns,
            execution_summaries,
        );
    }

    /// Borrows the runtime statistics accumulated by this consumer.
    #[must_use]
    pub const fn runtime_stats(&self) -> &SelectResultRuntimeStats {
        &self.runtime_stats
    }

    /// Borrows source result metadata retained with this live iterator.
    #[must_use]
    pub const fn result_metadata(&self) -> Option<&SelectResultMetadata> {
        self.result_metadata.as_ref()
    }

    /// Attaches the metadata created by the source `selectResult`
    /// constructor to the iterator that owns the returned response lifecycle.
    #[must_use]
    pub fn with_result_metadata(mut self, metadata: SelectResultMetadata) -> Self {
        self.result_metadata = Some(metadata);
        self
    }

    /// Binds plan metadata used to merge each consumed response's execution
    /// summaries automatically.
    #[must_use]
    pub fn with_runtime_stats(
        mut self,
        metadata: SelectResultMetadata,
        collector_enabled: bool,
    ) -> Self {
        self.runtime_stats_binding = Some(RuntimeStatsBinding {
            metadata,
            collector_enabled,
        });
        self
    }

    /// Appends a response to the still-open owned source.
    ///
    /// This is the synchronous producer seam until the concrete blocking TiKV
    /// transport owns this iterator. A pending read never closes this seam.
    pub fn push_response(&mut self, bytes: Vec<u8>) -> Result<(), ResponseChannelError> {
        self.source.push_result(bytes)
    }

    /// Appends a response with CopRuntimeStats from the same producer call.
    pub fn push_response_with_runtime(
        &mut self,
        bytes: Vec<u8>,
        runtime_stats: ResponseRuntimeStats,
    ) -> Result<(), ResponseChannelError> {
        self.source.push_result_with_runtime(bytes, runtime_stats)
    }

    /// Marks the owned producer seam complete.
    pub fn finish_source(&mut self) -> Result<(), ResponseChannelError> {
        self.source.finish()
    }

    fn install_encoded_response(
        &mut self,
        bytes: &[u8],
        runtime_stats: Option<&ResponseRuntimeStats>,
    ) -> Result<(), ResponseChannelError> {
        let response = decode_select_response(bytes)
            .map_err(|error| ResponseChannelError::Decode(error.to_string()))?;
        self.install_response(response, runtime_stats)
    }

    fn install_response(
        &mut self,
        response: SelectResponse,
        response_runtime_stats: Option<&ResponseRuntimeStats>,
    ) -> Result<(), ResponseChannelError> {
        if let Some(error) = response.error.as_ref() {
            return Err(ResponseChannelError::SelectResponse {
                code: error.code.unwrap_or_default(),
                message: error.msg.clone().unwrap_or_default(),
            });
        }
        if response.intermediate_outputs.len() != self.intermediate_output_types.len() {
            return Err(ResponseChannelError::IntermediateOutputCount {
                expected: self.intermediate_output_types.len(),
                actual: response.intermediate_outputs.len(),
            });
        }
        for warning in &response.warnings {
            self.warnings.append_tikv_warning(
                warning.code.unwrap_or_default(),
                warning.msg.clone().unwrap_or_default(),
            );
        }
        if let (Some(binding), Some(sample)) = (&self.runtime_stats_binding, response_runtime_stats)
        {
            self.runtime_stats.update(
                &binding.metadata,
                binding.collector_enabled,
                &sample.callee_address,
                sample.request_rpc_stats_present,
                sample.backoff_sleep_ns.clone(),
                &response.execution_summaries,
            );
        }

        self.channels.clear();
        for (channel_index, (output, field_types)) in response
            .intermediate_outputs
            .into_iter()
            .zip(&self.intermediate_output_types)
            .enumerate()
        {
            self.channels.push(SelectResponseChannel {
                channel_index,
                raw_encode_type: output.encode_type,
                chunks: output.chunks,
                field_types: field_types.clone(),
                next_chunk_index: 0,
                decoded: None,
            });
        }
        self.channels.push(SelectResponseChannel {
            channel_index: self.intermediate_output_types.len(),
            raw_encode_type: response.encode_type,
            chunks: response.chunks,
            field_types: self.final_field_types.clone(),
            next_chunk_index: 0,
            decoded: None,
        });
        Ok(())
    }
}

fn decode_channel(
    channel_index: usize,
    raw_encode_type: Option<i32>,
    chunks: &[Chunk],
    field_types: &[FieldType],
) -> Result<ChannelIter<Vec<Datum>>, ResponseChannelError> {
    let raw_encode_type = raw_encode_type.unwrap_or(EncodeType::TypeDefault as i32);
    let encode_type = EncodeType::try_from(raw_encode_type).map_err(|_| {
        ResponseChannelError::RowDecode(format!("invalid tipb encode type {raw_encode_type}"))
    })?;
    let mut decoded_chunks = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        if chunk.rows_data.as_deref().unwrap_or_default().is_empty() {
            decoded_chunks.push(Vec::new());
            continue;
        }
        let raw = decode_chunk(chunk, encode_type).map_err(map_chunk_error)?;
        let rows = match encode_type {
            EncodeType::TypeDefault => raw
                .decode_default_datums(field_types.len())
                .map_err(map_chunk_error)?,
            EncodeType::TypeChunk => {
                let layouts: Vec<_> = field_types
                    .iter()
                    .map(ColumnLayout::for_field_type)
                    .collect();
                let columnar = raw.decode_columnar(&layouts).map_err(map_chunk_error)?;
                if !columnar.remainder.is_empty() {
                    return Err(ResponseChannelError::RowDecode(format!(
                        "TypeChunk channel has {} trailing bytes",
                        columnar.remainder.len()
                    )));
                }
                let typed = columnar
                    .decode_datums(field_types)
                    .map_err(map_chunk_error)?;
                transpose_columns(typed.columns)?
            }
            EncodeType::TypeChBlock => {
                return Err(ResponseChannelError::RowDecode(
                    "TypeCHBlock row materialization is not implemented".to_owned(),
                ));
            }
        };
        decoded_chunks.push(rows);
    }
    Ok(ChannelIter::new(channel_index, decoded_chunks))
}

fn transpose_columns(columns: Vec<Vec<Datum>>) -> Result<Vec<Vec<Datum>>, ResponseChannelError> {
    let row_count = columns.first().map_or(0, Vec::len);
    if columns.iter().any(|column| column.len() != row_count) {
        return Err(ResponseChannelError::RowDecode(
            "TypeChunk columns have different row counts".to_owned(),
        ));
    }
    let mut rows = vec![Vec::with_capacity(columns.len()); row_count];
    for column in columns {
        for (row, value) in rows.iter_mut().zip(column) {
            row.push(value);
        }
    }
    Ok(rows)
}

fn append_warning(collector: &WarningCollector, warning: Warning) {
    collector.append_owned_warning(warning);
}

fn map_channel_error(error: ChannelIterError) -> ResponseChannelError {
    ResponseChannelError::RowDecode(error.to_string())
}

fn map_chunk_error(error: ChunkDecodeError) -> ResponseChannelError {
    ResponseChannelError::RowDecode(error.to_string())
}

/// Returns the legacy boundary error for callers outside the typed raw-byte
/// select iterator entry point.
#[must_use]
pub const fn unsupported_raw_tipb_response() -> ResponseChannelError {
    ResponseChannelError::unsupported_raw_tipb_response()
}

/// Returns the explicit boundary error for a TiKV response channel.
#[must_use]
pub const fn unsupported_tikv_response_channel() -> ResponseChannelError {
    ResponseChannelError::unsupported_tikv_response_channel()
}
