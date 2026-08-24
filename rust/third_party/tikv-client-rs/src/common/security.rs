// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use log::info;
use regex::Regex;
use tonic::transport::Channel;
use tonic::transport::ClientTlsConfig;
use tonic::transport::Identity;
use tonic::transport::{Certificate, Endpoint};

use crate::internal_err;
use crate::Result;

lazy_static::lazy_static! {
    static ref SCHEME_REG: Regex = Regex::new(r"^\s*(https?://)").unwrap();
}

fn check_pem_file(tag: &str, path: &Path) -> Result<File> {
    File::open(path)
        .map_err(|e| internal_err!("failed to open {} to load {}: {:?}", path.display(), tag, e))
}

fn load_pem_file(tag: &str, path: &Path) -> Result<Vec<u8>> {
    let mut file = check_pem_file(tag, path)?;
    let mut key = vec![];
    file.read_to_end(&mut key)
        .map_err(|e| {
            internal_err!(
                "failed to load {} from path {}: {:?}",
                tag,
                path.display(),
                e
            )
        })
        .map(|_| key)
}

/// Manages the TLS protocol
#[derive(Debug, Default)]
pub struct SecurityManager {
    /// The path to the PEM encoding of the server’s CA certificates.
    ca_path: Option<PathBuf>,
    /// The path to the PEM encoding of the server’s certificate chain.
    cert_path: Option<PathBuf>,
    /// The path to the file that contains the PEM encoding of the server’s private key.
    key_path: Option<PathBuf>,
}

impl SecurityManager {
    /// Load a CA-only TLS configuration from a file.
    pub fn load_ca(ca_path: impl AsRef<Path>) -> Result<SecurityManager> {
        let ca_path = ca_path.as_ref().to_path_buf();
        check_pem_file("ca", &ca_path)?;
        Ok(SecurityManager {
            ca_path: Some(ca_path),
            cert_path: None,
            key_path: None,
        })
    }

    /// Load TLS configuration from files.
    pub fn load(
        ca_path: impl AsRef<Path>,
        cert_path: impl AsRef<Path>,
        key_path: impl Into<PathBuf>,
    ) -> Result<SecurityManager> {
        let ca_path = ca_path.as_ref().to_path_buf();
        let cert_path = cert_path.as_ref().to_path_buf();
        let key_path = key_path.into();
        check_pem_file("ca", &ca_path)?;
        check_pem_file("certificate", &cert_path)?;
        check_pem_file("private key", &key_path)?;
        Ok(SecurityManager {
            ca_path: Some(ca_path),
            cert_path: Some(cert_path),
            key_path: Some(key_path),
        })
    }

    pub(crate) fn tls_configured(&self) -> bool {
        self.ca_path.is_some()
    }

    /// Connect to gRPC server using TLS connection. If TLS is not configured, use normal connection.
    pub async fn connect<Factory, Client>(
        &self,
        // env: Arc<Environment>,
        addr: &str,
        factory: Factory,
    ) -> Result<Client>
    where
        Factory: FnOnce(Channel) -> Client,
    {
        self.connect_with_keepalive(
            addr,
            Duration::from_secs(10),
            Duration::from_secs(3),
            factory,
        )
        .await
    }

    /// Connect using explicit gRPC TCP keepalive settings.
    pub async fn connect_with_keepalive<Factory, Client>(
        &self,
        addr: &str,
        keepalive_time: Duration,
        keepalive_timeout: Duration,
        factory: Factory,
    ) -> Result<Client>
    where
        Factory: FnOnce(Channel) -> Client,
    {
        self.connect_with_http2_settings(
            addr,
            keepalive_time,
            keepalive_timeout,
            None,
            None,
            factory,
        )
        .await
    }

    /// Connect using explicit HTTP/2 keepalive and flow-control settings.
    pub async fn connect_with_http2_settings<Factory, Client>(
        &self,
        addr: &str,
        keepalive_time: Duration,
        keepalive_timeout: Duration,
        initial_stream_window_size: Option<u32>,
        initial_connection_window_size: Option<u32>,
        factory: Factory,
    ) -> Result<Client>
    where
        Factory: FnOnce(Channel) -> Client,
    {
        info!("connect to rpc server at endpoint: {:?}", addr);
        let channel = if self.tls_configured() {
            self.tls_channel(
                addr,
                keepalive_time,
                keepalive_timeout,
                initial_stream_window_size,
                initial_connection_window_size,
            )
            .await?
        } else {
            self.default_channel(
                addr,
                keepalive_time,
                keepalive_timeout,
                initial_stream_window_size,
                initial_connection_window_size,
            )
            .await?
        };
        let ch = channel.connect().await?;

        Ok(factory(ch))
    }

    async fn tls_channel(
        &self,
        addr: &str,
        keepalive_time: Duration,
        keepalive_timeout: Duration,
        initial_stream_window_size: Option<u32>,
        initial_connection_window_size: Option<u32>,
    ) -> Result<Endpoint> {
        let (ca, identity) = self.load_tls_materials().await?;
        let addr = "https://".to_string() + &SCHEME_REG.replace(addr, "");
        let builder = self.endpoint(
            addr.to_string(),
            keepalive_time,
            keepalive_timeout,
            initial_stream_window_size,
            initial_connection_window_size,
        )?;
        let mut tls = ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca));
        if let Some((cert, key)) = identity {
            tls = tls.identity(Identity::from_pem(cert, key));
        }
        let builder = builder.tls_config(tls)?;
        Ok(builder)
    }

    async fn load_tls_materials(&self) -> Result<(Vec<u8>, Option<(Vec<u8>, Vec<u8>)>)> {
        let ca_path = self
            .ca_path
            .clone()
            .ok_or_else(|| internal_err!("TLS is not configured"))?;
        let cert_path = self.cert_path.clone();
        let key_path = self.key_path.clone();

        let materials = tokio::task::spawn_blocking(move || -> Result<_> {
            let identity = match (cert_path, key_path) {
                (Some(cert), Some(key)) => Some((
                    load_pem_file("certificate", &cert)?,
                    load_pem_file("private key", &key)?,
                )),
                _ => None,
            };
            Ok((load_pem_file("ca", &ca_path)?, identity))
        })
        .await??;
        Ok(materials)
    }

    async fn default_channel(
        &self,
        addr: &str,
        keepalive_time: Duration,
        keepalive_timeout: Duration,
        initial_stream_window_size: Option<u32>,
        initial_connection_window_size: Option<u32>,
    ) -> Result<Endpoint> {
        let addr = "http://".to_string() + &SCHEME_REG.replace(addr, "");
        self.endpoint(
            addr,
            keepalive_time,
            keepalive_timeout,
            initial_stream_window_size,
            initial_connection_window_size,
        )
    }

    fn endpoint(
        &self,
        addr: String,
        keepalive_time: Duration,
        keepalive_timeout: Duration,
        initial_stream_window_size: Option<u32>,
        initial_connection_window_size: Option<u32>,
    ) -> Result<Endpoint> {
        let endpoint = Channel::from_shared(addr)?
            .http2_keep_alive_interval(keepalive_time)
            .keep_alive_timeout(keepalive_timeout)
            .initial_stream_window_size(initial_stream_window_size)
            .initial_connection_window_size(initial_connection_window_size);
        Ok(endpoint)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write;
    use std::path::PathBuf;

    use tempfile;

    use super::*;

    #[tokio::test]
    async fn test_security() {
        let temp = tempfile::tempdir().unwrap();
        let example_ca = temp.path().join("ca");
        let example_cert = temp.path().join("cert");
        let example_pem = temp.path().join("key");
        for (id, f) in [&example_ca, &example_cert, &example_pem]
            .iter()
            .enumerate()
        {
            File::create(f).unwrap().write_all(&[id as u8]).unwrap();
        }
        let cert_path: PathBuf = format!("{}", example_cert.display()).into();
        let key_path: PathBuf = format!("{}", example_pem.display()).into();
        let ca_path: PathBuf = format!("{}", example_ca.display()).into();
        let mgr = SecurityManager::load(ca_path, cert_path, &key_path).unwrap();
        assert!(mgr.tls_configured());
        let (ca, identity) = mgr.load_tls_materials().await.unwrap();
        let (cert, key) = identity.unwrap();
        assert_eq!(ca, vec![0]);
        assert_eq!(cert, vec![1]);
        assert_eq!(key, vec![2]);
    }

    #[tokio::test]
    async fn test_security_reload() {
        let temp = tempfile::tempdir().unwrap();
        let example_ca = temp.path().join("ca");
        let example_cert = temp.path().join("cert");
        let example_pem = temp.path().join("key");
        for (id, f) in [&example_ca, &example_cert, &example_pem]
            .iter()
            .enumerate()
        {
            File::create(f).unwrap().write_all(&[id as u8]).unwrap();
        }

        let mgr = SecurityManager::load(&example_ca, &example_cert, &example_pem).unwrap();
        let first = mgr.load_tls_materials().await.unwrap();

        File::create(&example_ca)
            .unwrap()
            .write_all(&[9, 9])
            .unwrap();
        File::create(&example_cert)
            .unwrap()
            .write_all(&[8, 8, 8])
            .unwrap();
        File::create(&example_pem)
            .unwrap()
            .write_all(&[7, 7, 7, 7])
            .unwrap();

        let second = mgr.load_tls_materials().await.unwrap();
        assert_ne!(first, second);
        assert_eq!(second.0, vec![9, 9]);
        assert_eq!(second.1.as_ref().unwrap().0, vec![8, 8, 8]);
        assert_eq!(second.1.as_ref().unwrap().1, vec![7, 7, 7, 7]);
    }
}
