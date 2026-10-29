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

//! Go `copr.CopClient` (`pkg/store/copr/coprocessor.go`): the node's
//! `kv.Client`, which `kv.Storage.GetClient()` hands every reader. Each send
//! opens a worker-local transport over the node's process authority and
//! sends the already-built request through it.

use std::fmt;
use std::sync::Arc;

use tidb_distsql::{QueryDispatch, QueryTransport};
use tidb_txnkv::TransportRequest;
use tidb_txnkv::RequestTypeSupportedChecker;


/// Builds one worker-local query transport from process-owned shared handles.
///
/// An implementation may retain the unique process lifecycle owners together
/// with their cloneable request/cache handles, but it must give a session only
/// the handles. In particular, opening a session must not connect another PD
/// client, create a region cache, start a maintenance worker, or start a tonic
/// runtime. The returned transport deliberately has no `Send` bound because
/// the fixed server worker creates and consumes it on one thread.
///
/// The production implementation reads through the node's client-rust store
/// (`NativeCopTransport` over `TikvStore`).
pub trait RealTiKvSessionTransportFactory: Send + Sync {
    /// Worker-local transport type created for one admitted connection.
    type Transport: tidb_distsql::QueryTransport;

    /// Creates query-local state over the already-running process authority.
    fn open_session_transport(&self) -> Result<Self::Transport, String>;
}

/// Go `copr.CopClient` over the node's transport factory.
pub struct CopClient<F> {
    factory: Arc<F>,
}

impl<F> CopClient<F> {
    /// The client over an already-running transport factory.
    #[must_use]
    pub fn new(factory: Arc<F>) -> Self {
        Self { factory }
    }
}

impl<F> fmt::Debug for CopClient<F> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("CopClient").finish_non_exhaustive()
    }
}

impl<F> tidb_txnkv::Client for CopClient<F>
where
    F: RealTiKvSessionTransportFactory + 'static,
    <F::Transport as QueryTransport>::Response: Send + 'static,
{
    fn send(
        &self,
        request: &TransportRequest,
    ) -> Result<Box<dyn tidb_txnkv::Response + Send>, String> {
        let dispatch = QueryDispatch::for_request(request).ok_or_else(|| {
            format!(
                "unsupported coprocessor request type {:?}",
                request.metadata().request_type
            )
        })?;
        let mut transport = self.factory.open_session_transport()?;
        let response = tidb_distsql::send_request(&mut transport, request, &dispatch, None)
            .map_err(|error| error.to_string())?;
        Ok(Box::new(response))
    }

    fn is_request_type_supported(&self, request_type: i64, sub_type: i64) -> bool {
        RequestTypeSupportedChecker.is_request_type_supported(request_type, sub_type)
    }
}
