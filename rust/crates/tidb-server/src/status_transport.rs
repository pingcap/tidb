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

//! Owned HTTP/gRPC transport and Go status-listener cluster TLS policy.
use rustls::{
    pki_types::{CertificateDer, UnixTime},
    server::danger::{ClientCertVerified, ClientCertVerifier},
    DigitallySignedStruct, DistinguishedName, SignatureScheme,
};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
pub(crate) struct Connection(Box<dyn Socket>);
impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_read(cx, buf)
    }
}
impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.0).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.0).poll_shutdown(cx)
    }
}

pub(crate) async fn serve(
    listener: tokio::net::TcpListener,
    tls: Option<tokio_rustls::TlsAcceptor>,
    router: axum::Router,
    config: tidb_config::config_tree::big_sections::Status,
) -> io::Result<()> {
    use futures::StreamExt;
    let mut incoming = std::pin::pin!(incoming(listener, tls));
    let mut connections = tokio::task::JoinSet::new();
    let mut builder =
        hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());
    // net/http serves a complete request even if its client closes the write half.
    builder.http1().half_close(true).max_buf_size(1 << 20);
    builder
        .http2()
        .timer(hyper_util::rt::TokioTimer::new())
        .keep_alive_interval(std::time::Duration::from_secs(
            config.grpc_keep_alive_time as u64,
        ))
        .keep_alive_timeout(std::time::Duration::from_secs(
            config.grpc_keep_alive_timeout as u64,
        ))
        .max_concurrent_streams(config.grpc_concurrent_streams as u32)
        .initial_stream_window_size(config.grpc_initial_window_size as u32);
    loop {
        tokio::select! {
            next = incoming.next() => {
                let Some(socket) = next else { return Ok(()); };
                let socket = socket?;
                let builder = builder.clone();
                let service = hyper_util::service::TowerToHyperService::new(router.clone());
                connections.spawn(async move {
                    let _ = builder.serve_connection(hyper_util::rt::TokioIo::new(socket), service).await;
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
        }
    }
}

pub(crate) fn incoming(
    listener: tokio::net::TcpListener,
    tls: Option<tokio_rustls::TlsAcceptor>,
) -> impl futures::Stream<Item = io::Result<Connection>> {
    async_stream::stream! {
        let mut handshakes = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    match accepted {
                        Ok((socket, _)) => {
                            if let Some(tls) = tls.clone() {
                                handshakes.spawn(async move { tls.accept(socket).await.map(|socket| Connection(Box::new(socket))) });
                            } else { yield Ok(Connection(Box::new(socket))); }
                        }
                        Err(error) => { yield Err(error); break; }
                    }
                }
                Some(done) = handshakes.join_next(), if !handshakes.is_empty() => {
                    // A bad TLS client cannot terminate the listener or block other handshakes.
                    if let Ok(Ok(socket)) = done { yield Ok(socket); }
                }
            }
        }
    }
}

pub(crate) fn tls_acceptor(
    security: &tidb_pd_client::ClusterSecurity,
) -> io::Result<Option<tokio_rustls::TlsAcceptor>> {
    if !security.is_tls_enabled() {
        return Ok(None);
    }
    let certs = |path: &str| -> io::Result<Vec<CertificateDer<'static>>> {
        rustls_pemfile::certs(&mut io::BufReader::new(std::fs::File::open(path)?)).collect()
    };
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs(security.ca_path())? {
        roots.add(cert).map_err(io::Error::other)?;
    }
    if roots.is_empty() {
        return Err(io::Error::other("empty cluster CA certificates"));
    }
    let builder = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(io::Error::other)?;
    // Go ToTLSConfig has NoClientCert unless SetCNChecker installs an allowlist.
    let builder = if security.verify_cn().is_empty() {
        builder.with_no_client_auth()
    } else {
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .map_err(io::Error::other)?;
        builder.with_client_cert_verifier(Arc::new(CommonNames {
            verifier,
            names: security
                .verify_cn()
                .iter()
                .map(|s| s.trim().to_owned())
                .collect(),
        }))
    };
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(std::fs::File::open(
        security.key_path(),
    )?))?
    .ok_or_else(|| io::Error::other("missing cluster private key"))?;
    let mut config = builder
        .with_single_cert(certs(security.cert_path())?, key)
        .map_err(io::Error::other)?;
    config.alpn_protocols = vec![b"http/1.1".to_vec(), b"h2".to_vec()];
    Ok(Some(tokio_rustls::TlsAcceptor::from(Arc::new(config))))
}

#[derive(Debug)]
struct CommonNames {
    verifier: Arc<dyn ClientCertVerifier>,
    names: Vec<String>,
}
impl ClientCertVerifier for CommonNames {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.verifier.root_hint_subjects()
    }
    fn verify_client_cert(
        &self,
        end: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self.verifier.verify_client_cert(end, intermediates, now)?;
        let (_, cert) = x509_parser::parse_x509_certificate(end.as_ref())
            .map_err(|_| rustls::Error::General("invalid client certificate".into()))?;
        if cert
            .subject()
            .iter_common_name()
            .filter_map(|cn| cn.as_str().ok())
            .any(|cn| self.names.iter().any(|allowed| allowed == cn))
        {
            Ok(verified)
        } else {
            Err(rustls::Error::General(
                "client certificate common name is not in cluster-verify-cn".into(),
            ))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.verifier
            .verify_tls12_signature(message, cert, signature)
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.verifier
            .verify_tls13_signature(message, cert, signature)
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.verifier.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpStream,
        path::Path,
        time::Duration,
    };
    use tidb_pd_client::ClusterSecurity;

    #[test]
    fn peer_host_batch_cluster_tls_http_grpc_cn_and_shutdown() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../tidb-pd-client/testdata/tls");
        let pem = |name: &str| std::fs::read(dir.join(name)).unwrap();
        let certs = |name: &str| {
            rustls_pemfile::certs(&mut pem(name).as_slice())
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        let key = |name: &str| {
            rustls_pemfile::private_key(&mut pem(name).as_slice())
                .unwrap()
                .unwrap()
        };
        let mut roots = rustls::RootCertStore::empty();
        for cert in certs("ca.crt") {
            roots.add(cert).unwrap();
        }
        for (names, client_identity, allowed) in [
            (vec![], false, true),
            (vec![" tidb-test-client ".to_owned()], true, true),
            (vec!["tidb-test-client".to_owned()], false, false),
            (vec!["other-client".to_owned()], true, false),
        ] {
            let security = ClusterSecurity::new(
                dir.join("ca.crt").display().to_string(),
                dir.join("server.crt").display().to_string(),
                dir.join("server.key").display().to_string(),
                names,
            );
            let server = crate::http_status::start_status_listener_with_routes(
                "127.0.0.1",
                0,
                Arc::new(crate::sql_node::ConnectionTracker::default()),
                "test".into(),
                "test".into(),
                crate::http_status::StatusRoutes {
                    security,
                    peer: Some(crate::peer_rpc::PeerService::new(
                        Default::default(),
                        Default::default(),
                        None,
                    )),
                    ..Default::default()
                },
            )
            .unwrap();
            let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots.clone());
            let mut config = if client_identity {
                builder
                    .with_client_auth_cert(certs("client.crt"), key("client.key"))
                    .unwrap()
            } else {
                builder.with_no_client_auth()
            };
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let conn =
                rustls::ClientConnection::new(Arc::new(config), "localhost".try_into().unwrap())
                    .unwrap();
            let socket = TcpStream::connect(server.local_addr()).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut stream = rustls::StreamOwned::new(conn, socket);
            let response = (|| -> io::Result<String> {
                stream.write_all(
                    b"GET /status HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                )?;
                let mut response = String::new();
                // Rustls may report missing close_notify after the complete HTTP response.
                let result = stream.read_to_string(&mut response);
                if !response.starts_with("HTTP/1.1 200") {
                    result?;
                }
                Ok(response)
            })();
            assert_eq!(
                response.is_ok_and(|r| r.starts_with("HTTP/1.1 200")),
                allowed
            );
            if allowed {
                let runtime = tokio::runtime::Runtime::new().unwrap();
                runtime.block_on(async {
                    let mut tls = tonic::transport::ClientTlsConfig::new()
                        .ca_certificate(tonic::transport::Certificate::from_pem(pem("ca.crt")))
                        .domain_name("localhost");
                    if client_identity {
                        tls = tls.identity(tonic::transport::Identity::from_pem(
                            pem("client.crt"),
                            pem("client.key"),
                        ));
                    }
                    let channel = tonic::transport::Endpoint::from_shared(format!(
                        "https://{}",
                        server.local_addr()
                    ))
                    .unwrap()
                    .tls_config(tls)
                    .unwrap()
                    .connect()
                    .await
                    .unwrap();
                    let response = tidb_proto::tikvpb::tikv_client::TikvClient::new(channel)
                        .coprocessor(tidb_proto::coprocessor::Request::default())
                        .await
                        .unwrap()
                        .into_inner();
                    assert!(response.other_error.contains("unsupported request type"));
                });
            }
            let address = server.local_addr();
            // An unfinished handshake is owned by the listener and must not stall drop.
            let _unfinished = TcpStream::connect(address).unwrap();
            drop(stream);
            drop(server);
            assert!(std::net::TcpListener::bind(address).is_ok());
        }
    }
}
