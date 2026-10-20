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

//! Go `pkg/store/mockstore/redirector.go`: `clientRedirector`, the mock
//! store's client wrapper. The embedded store serves no TiDB RPC service, so
//! TiDB requests -- memory-table reads and `KILL` sent to a peer's status
//! address -- go over a real RPC client, and every other request to the
//! embedded store's [`RpcClient`].
//!
//! # Narrowings, by name
//!
//! * Go creates the RPC client on the first TiDB request (`sync.Once`); here
//!   it is built with the redirector. Building a connector dials nothing,
//!   so the difference is unobservable.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tikv_client::tikv::{
    Client, ClientEventListener, EndpointType, KvConnect, KvRpcClient, Request, TikvConnect,
};
use tikv_client::Result;

use crate::rpc::{RpcClient, RpcConnect};

/// Go `clientRedirector`: the embedded store's connector, with TiDB
/// requests sent over `rpc_client`.
#[derive(Clone)]
pub struct ClientRedirector {
    mock_client: RpcConnect,
    rpc_client: TikvConnect,
}

impl std::fmt::Debug for ClientRedirector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientRedirector")
            .field("mock_client", &self.mock_client)
            .finish_non_exhaustive()
    }
}

impl ClientRedirector {
    /// Go `newClientRedirector(mockClient)`, with the RPC client Go builds
    /// from the global config's cluster security: `config`'s TLS settings.
    pub fn new(mock_client: RpcConnect, config: &tikv_client::Config) -> Result<Self> {
        Ok(Self {
            mock_client,
            rpc_client: TikvConnect::new_rpc_client(config)?,
        })
    }
}

#[async_trait]
impl KvConnect for ClientRedirector {
    type KvClient = RedirectedClient;

    async fn connect(&self, address: &str) -> Result<RedirectedClient> {
        self.mock_client
            .connect(address)
            .await
            .map(RedirectedClient::Mock)
    }

    /// Go `clientRedirector.SendRequest`: `req.StoreTp == tikvrpc.TiDB` goes
    /// to the RPC client.
    async fn connect_with_store_type(
        &self,
        address: &str,
        store_type: EndpointType,
    ) -> Result<RedirectedClient> {
        if store_type == EndpointType::TiDb {
            return self
                .rpc_client
                .connect_with_store_type(address, store_type)
                .await
                .map(RedirectedClient::Rpc);
        }
        self.connect(address).await
    }
}

/// The client [`ClientRedirector`] hands out for one address: the embedded
/// store's, or a real RPC client's for a TiDB peer.
#[derive(Clone)]
pub enum RedirectedClient {
    /// Go `clientRedirector.mockClient`.
    Mock(RpcClient),
    /// Go `clientRedirector.rpcClient`.
    Rpc(KvRpcClient),
}

#[async_trait]
impl Client for RedirectedClient {
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        match self {
            Self::Mock(client) => client.dispatch(req).await,
            Self::Rpc(client) => client.dispatch(req).await,
        }
    }

    async fn dispatch_with_timeout(
        &self,
        req: &dyn Request,
        timeout: Option<Duration>,
    ) -> Result<Box<dyn Any>> {
        match self {
            Self::Mock(client) => client.dispatch_with_timeout(req, timeout).await,
            Self::Rpc(client) => client.dispatch_with_timeout(req, timeout).await,
        }
    }

    async fn dispatch_with_forwarded_host(
        &self,
        req: &dyn Request,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        match self {
            Self::Mock(client) => {
                client
                    .dispatch_with_forwarded_host(req, forwarded_host)
                    .await
            }
            Self::Rpc(client) => {
                client
                    .dispatch_with_forwarded_host(req, forwarded_host)
                    .await
            }
        }
    }

    async fn dispatch_with_timeout_and_forwarded_host(
        &self,
        req: &dyn Request,
        timeout: Option<Duration>,
        forwarded_host: &str,
    ) -> Result<Box<dyn Any>> {
        match self {
            Self::Mock(client) => {
                client
                    .dispatch_with_timeout_and_forwarded_host(req, timeout, forwarded_host)
                    .await
            }
            Self::Rpc(client) => {
                client
                    .dispatch_with_timeout_and_forwarded_host(req, timeout, forwarded_host)
                    .await
            }
        }
    }

    /// Go `clientRedirector.SetEventListener`: the mock client's only.
    fn set_event_listener(&self, listener: Arc<dyn ClientEventListener>) {
        if let Self::Mock(client) = self {
            client.set_event_listener(listener);
        }
    }

    fn with_connection_info(self, address: String, version: u64) -> Self {
        match self {
            Self::Mock(client) => Self::Mock(client.with_connection_info(address, version)),
            Self::Rpc(client) => Self::Rpc(client.with_connection_info(address, version)),
        }
    }

    /// Go `clientRedirector.CloseAddr`: closes whichever client serves the
    /// address.
    fn close(&self) {
        match self {
            Self::Mock(client) => Client::close(client),
            Self::Rpc(client) => client.close(),
        }
    }

    fn is_idle(&self) -> bool {
        match self {
            Self::Mock(client) => client.is_idle(),
            Self::Rpc(client) => client.is_idle(),
        }
    }
}
