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

//! Localhost TLS handshake proof for the shared `secure_endpoint` helper.
//!
//! A tonic gRPC server is started on loopback with the throwaway server cert
//! from `testdata/tls`. The client builds its channel through the same
//! `secure_endpoint` every PD/TiKV transport uses, with the throwaway CA as
//! its only trust root, and completes the TLS + HTTP/2 handshake. This is the
//! strongest offline proof that the credential construction is correct: a
//! live TLS playground would only additionally prove PD/TiKV advertise the
//! `https://` URLs and accept these credentials.

use std::time::Duration;

use tidb_pd_client::{secure_endpoint, ClusterSecurity};
use tidb_proto::etcdserverpb::kv_server::{Kv, KvServer};
use tidb_proto::etcdserverpb::{PutRequest, PutResponse, RangeRequest, RangeResponse};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

fn testdata(name: &str) -> String {
    format!("{}/testdata/tls/{name}", env!("CARGO_MANIFEST_DIR"))
}

/// A trivial KV service: enough for tonic to route a request over the secured
/// channel. The handshake, not the reply, is what this test proves.
#[derive(Default)]
struct EchoKv;

#[tonic::async_trait]
impl Kv for EchoKv {
    async fn range(
        &self,
        _request: Request<RangeRequest>,
    ) -> Result<Response<RangeResponse>, Status> {
        Ok(Response::new(RangeResponse::default()))
    }

    async fn put(&self, _request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        Ok(Response::new(PutResponse::default()))
    }
}

fn server_tls_config() -> ServerTlsConfig {
    let cert = std::fs::read(testdata("server.crt")).unwrap();
    let key = std::fs::read(testdata("server.key")).unwrap();
    ServerTlsConfig::new().identity(Identity::from_pem(cert, key))
}

/// A client that trusts the throwaway CA completes the handshake and one RPC.
#[test]
fn secure_endpoint_completes_a_localhost_tls_handshake() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let incoming = TcpListenerStream::new(listener);

        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls_config())
                .unwrap()
                .add_service(KvServer::new(EchoKv))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        let security =
            ClusterSecurity::new(testdata("ca.crt"), String::new(), String::new(), vec![]);
        let endpoint = secure_endpoint(&format!("http://127.0.0.1:{port}"), &security)
            .unwrap()
            .connect_timeout(Duration::from_secs(5));

        // Retry briefly while the server binds; the assertion is that the TLS
        // handshake against the CA-signed server cert eventually succeeds.
        let mut connected = None;
        for _ in 0..50 {
            match endpoint.connect().await {
                Ok(channel) => {
                    connected = Some(channel);
                    break;
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
        let channel = connected.expect("TLS handshake through secure_endpoint must succeed");

        let mut client = tidb_proto::etcdserverpb::kv_client::KvClient::new(channel);
        let response = client.range(RangeRequest::default()).await;
        assert!(
            response.is_ok(),
            "secured RPC over the TLS channel must return: {response:?}"
        );

        server.abort();
    });
}

/// A client that does not trust the server's CA fails the handshake: proof the
/// helper actually verifies the server certificate rather than accepting any
/// peer.
#[test]
fn secure_endpoint_rejects_an_untrusted_server_cert() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let incoming = TcpListenerStream::new(listener);

        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(server_tls_config())
                .unwrap()
                .add_service(KvServer::new(EchoKv))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        // Trust the *client* cert as the CA: it never signed the server cert,
        // so verification must fail.
        let security =
            ClusterSecurity::new(testdata("client.crt"), String::new(), String::new(), vec![]);
        let endpoint = secure_endpoint(&format!("http://127.0.0.1:{port}"), &security)
            .unwrap()
            .connect_timeout(Duration::from_secs(2));

        let mut last_ok = false;
        for _ in 0..20 {
            if endpoint.connect().await.is_ok() {
                last_ok = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            !last_ok,
            "handshake must fail when the server cert is signed by an untrusted CA"
        );

        server.abort();
    });
}
