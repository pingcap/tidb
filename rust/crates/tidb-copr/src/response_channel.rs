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


use tidb_txnkv::QueryResponseError;
use tidb_txnkv::QueryResultSubset;
use tidb_txnkv::ResponseRuntimeStats;
use tidb_context::Warning;
use tidb_context::WarningLevel;

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

/// Operations incompatible with transport-owned responses.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseChannelUnsupported {
    /// A select iterator already owns a pull-based query response and cannot
    /// be mutated through the deterministic channel adapter.
    TransportOwnedResponseMutation,
}

impl fmt::Display for ResponseChannelUnsupported {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
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
    /// The operation is incompatible with the response owner.
    Unsupported(ResponseChannelUnsupported),
    /// Raw bytes were not a valid checked-in `tipb.SelectResponse`.
    Decode(String),
    /// The response carried a TiKV or registered storage/SQL error.
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
    /// The canonical query cancellation interrupted the source.
    Cancelled,
    /// The owned source is still open but has no response available yet.
    Pending,
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
            Self::Cancelled => f.write_str("query cancelled by caller"),
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
            class: tidb_context::WarningClass::Statement,
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

impl tidb_txnkv::Response for ResponseChannel<prost::bytes::Bytes> {
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
