// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `testutil.go`: helpers that are only for tests, including the mock
//! service discovery of PD's `servicediscovery.NewMockServiceDiscovery`.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use reqwest::header::HeaderMap;

use super::client::{Client, ClientOption, HttpClient, HttpRequest, HttpResponse, RoundTripper};
use super::service_discovery::{ServiceClient, ServiceDiscovery};
use crate::Result;

// Go `requestChecker`: checks the HTTP request sent by the client.
struct RequestChecker {
    checker: Box<dyn Fn(&HttpRequest) -> Result<()> + Send + Sync>,
}

#[async_trait]
impl RoundTripper for RequestChecker {
    // Go `RoundTrip`: a checked request gets a 200 response with body `{}`.
    // Go leaves `Status` empty, so the metrics label is empty too.
    async fn round_trip(&self, req: HttpRequest) -> Result<HttpResponse> {
        (self.checker)(&req)?;
        Ok(HttpResponse {
            status_code: 200,
            status: String::new(),
            headers: HeaderMap::new(),
            body: b"{}".to_vec(),
            request: req,
        })
    }
}

/// Go `NewHTTPClientWithRequestChecker`: an HTTP client whose transport only
/// runs the checker and answers `200 {}`.
pub fn new_http_client_with_request_checker(
    checker: impl Fn(&HttpRequest) -> Result<()> + Send + Sync + 'static,
) -> HttpClient {
    HttpClient::with_transport(Arc::new(RequestChecker {
        checker: Box::new(checker),
    }))
}

/// Go `servicediscovery.mockServiceDiscovery`: every URL is a client, none of
/// them connected to the leader, and membership checks do nothing.
pub(crate) struct MockServiceDiscovery {
    clients: Mutex<Vec<ServiceClient>>,
}

impl MockServiceDiscovery {
    /// Go `NewMockServiceDiscovery` followed by `Init`.
    pub(crate) fn new(urls: &[String]) -> MockServiceDiscovery {
        MockServiceDiscovery {
            clients: Mutex::new(
                urls.iter()
                    .map(|url| ServiceClient::new(url.clone(), false))
                    .collect(),
            ),
        }
    }
}

#[async_trait]
impl ServiceDiscovery for MockServiceDiscovery {
    async fn get_all_service_clients(&self) -> Vec<ServiceClient> {
        self.clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn schedule_check_member_changed(&self) {}

    // Go `Close` clears the service clients.
    async fn close(&self) {
        self.clients
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// Go `newClientWithMockServiceDiscovery`: a PD HTTP client over a mock
/// service discovery, which the client does not own.
pub(crate) fn new_client_with_mock_service_discovery(
    source: &str,
    pd_addrs: &[String],
    opts: Vec<ClientOption>,
) -> Result<Client> {
    let sd = Arc::new(MockServiceDiscovery::new(pd_addrs));
    super::client::new_client_with_service_discovery(source, sd, opts)
}
