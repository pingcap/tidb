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

//! Go `copr.CopClient.Send`'s send step over one worker-local transport:
//! the built request is bound once (under the operation's request-source
//! override), the query's cancellation is checked before and after, and the
//! transport's raw response is returned for the reader to decode.

use tidb_trxevents::EventCallback;
use tidb_txnkv::{RequestSource, RequestType, TransportBinding, TransportRequest, TransportRequestError};

/// Source operation sent through [`QueryTransport`]: what the transport
/// checks the request type against.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryOperation {
    /// `Select` without runtime-plan binding.
    Select,
    /// `SelectWithRuntimeStats`.
    SelectWithRuntimeStats,
    /// Statistics analyze request.
    Analyze,
    /// Checksum request.
    Checksum,
}

impl QueryOperation {
    /// The operation a built request's type is sent as; `None` for a type
    /// the coprocessor client does not send.
    #[must_use]
    pub fn for_request_type(request_type: RequestType) -> Option<Self> {
        match request_type {
            RequestType::Dag => Some(Self::Select),
            RequestType::Analyze => Some(Self::Analyze),
            RequestType::Checksum => Some(Self::Checksum),
            _ => None,
        }
    }
}

/// Caller-supplied owner of the `kv.Client.Send` capability.
pub trait QueryTransport {
    /// Raw response owner returned by this transport.
    type Response: tidb_txnkv::Response;

    /// Installs the callback from this send's client options.
    ///
    /// Transports without transaction events keep the default no-op. A
    /// concrete transport must copy the callback into its response owner
    /// during `send`; the runtime clears this transient slot immediately
    /// afterward.
    fn set_event_callback(&mut self, _callback: Option<EventCallback>) {}

    /// Sends one already-built request and returns its raw response owner.
    ///
    /// `Ok(None)` is kept distinct because Go treats a nil response as an
    /// explicit error rather than an empty result.
    fn send(
        &mut self,
        request: &TransportRequest,
        operation: QueryOperation,
    ) -> Result<Option<Self::Response>, String>;
}

impl<T: QueryTransport + ?Sized> QueryTransport for &mut T {
    type Response = T::Response;

    fn set_event_callback(&mut self, callback: Option<EventCallback>) {
        (**self).set_event_callback(callback);
    }

    fn send(
        &mut self,
        request: &TransportRequest,
        operation: QueryOperation,
    ) -> Result<Option<Self::Response>, String> {
        (**self).send(request, operation)
    }
}

/// Errors returned by the injected query runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryRuntimeError {
    /// The immutable request could not be exclusively bound to this transport.
    Request(TransportRequestError),
    /// The injected transport returned Go's nil-response case.
    NilResponse,
    /// The injected transport failed before yielding a response iterator.
    Transport(String),
    /// The canonical query cancellation won before a response was returned.
    Cancelled,
}

impl std::fmt::Display for QueryRuntimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Request(error) => write!(formatter, "DistSQL request is not sendable: {error:?}"),
            Self::NilResponse => formatter.write_str("client returns nil response"),
            Self::Transport(message) => formatter.write_str(message),
            Self::Cancelled => formatter.write_str("query cancelled by caller"),
        }
    }
}

impl std::error::Error for QueryRuntimeError {}

/// The `kv.Client.Send` boundary over one injected transport: claims the
/// built request for this send (minting its request-local cancellation),
/// sends it, and returns the raw response owner.
pub fn send_request<T: QueryTransport>(
    transport: &mut T,
    request: &TransportRequest,
    operation: QueryOperation,
    request_source_override: Option<RequestSource>,
    event_callback: Option<EventCallback>,
) -> Result<T::Response, QueryRuntimeError> {
    let bound = match request_source_override {
        Some(request_source) => request
            .bind_with_request_source(TransportBinding::new(), request_source)
            .map_err(QueryRuntimeError::Request)?,
        None => request
            .bind(TransportBinding::new())
            .map_err(QueryRuntimeError::Request)?,
    };
    if bound
        .request_cancellation()
        .map_err(QueryRuntimeError::Request)?
        .is_cancelled()
    {
        return Err(QueryRuntimeError::Cancelled);
    }
    transport.set_event_callback(event_callback);
    let response = transport.send(&bound, operation);
    transport.set_event_callback(None);
    // The canonical carrier has Go's post-Send ctx.Err precedence over a
    // simultaneous transport failure or response.
    if bound
        .request_cancellation()
        .map_err(QueryRuntimeError::Request)?
        .is_cancelled()
    {
        return Err(QueryRuntimeError::Cancelled);
    }
    response
        .map_err(QueryRuntimeError::Transport)?
        .ok_or(QueryRuntimeError::NilResponse)
}
