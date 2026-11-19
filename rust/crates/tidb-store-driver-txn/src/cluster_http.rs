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

//! Go `util.InternalHTTPClient()`: the process's one HTTP client for TiDB's
//! internal requests -- status ports of TiDB, TiKV and TiFlash -- with the
//! cluster TLS settings. PD's HTTP API goes through the store's PD HTTP
//! client instead (Go `store.GetPDHTTPClient()`), driven by
//! [`block_on_pd_http`].

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Go `util.InternalHTTPClient()`: built once (`internalClientInit`) and
/// shared. Go builds it from the global config's cluster security; a process
/// here can hold more than one security configuration (tests), so the one
/// client is kept per configuration.
pub fn internal_http_client(
    security: &tidb_pd_client::ClusterSecurity,
) -> Result<reqwest::blocking::Client, String> {
    static CLIENTS: OnceLock<Mutex<HashMap<(String, String, String), reqwest::blocking::Client>>> =
        OnceLock::new();
    let key = (
        security.ca_path().to_owned(),
        security.cert_path().to_owned(),
        security.key_path().to_owned(),
    );
    let clients = CLIENTS.get_or_init(Default::default);
    let mut clients = clients
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(client) = clients.get(&key) {
        return Ok(client.clone());
    }
    let client = init_internal_client(security)?;
    clients.insert(key, client.clone());
    Ok(client)
}

/// A client over Go's `http.DefaultTransport`, whose `Proxy` is
/// `ProxyFromEnvironment`: `HTTP_PROXY` / `HTTPS_PROXY` (upper case first)
/// minus `NO_PROXY`, never for a loopback host -- and never the operating
/// system's proxy settings, which reqwest's `system-proxy` feature (enabled
/// in this build through other crates) would otherwise read.
pub fn default_transport_client_builder() -> Result<reqwest::blocking::ClientBuilder, String> {
    proxy_from_environment(|key| std::env::var(key).ok())
}

/// [`default_transport_client_builder`] over the environment `getenv` reads.
fn proxy_from_environment(
    getenv: impl Fn(&str) -> Option<String>,
) -> Result<reqwest::blocking::ClientBuilder, String> {
    let env = |upper: &str, lower: &str| {
        getenv(upper)
            .filter(|value| !value.is_empty())
            .or_else(|| getenv(lower).filter(|value| !value.is_empty()))
    };
    let no_proxy = env("NO_PROXY", "no_proxy").unwrap_or_default();
    let mut builder = reqwest::blocking::Client::builder().no_proxy();
    for (scheme, proxy) in [
        ("http", env("HTTP_PROXY", "http_proxy")),
        ("https", env("HTTPS_PROXY", "https_proxy")),
    ] {
        let Some(proxy) = proxy else { continue };
        let proxy = reqwest::Url::parse(&proxy)
            .or_else(|_| reqwest::Url::parse(&format!("http://{proxy}")))
            .map_err(|_| "invalid proxy URL".to_owned())?;
        let custom = reqwest::Proxy::custom(move |url| {
            let loopback = match url.host_str() {
                None => true,
                Some(host) => {
                    host.eq_ignore_ascii_case("localhost")
                        || host
                            .trim_start_matches('[')
                            .trim_end_matches(']')
                            .parse::<std::net::IpAddr>()
                            .is_ok_and(|ip| ip.is_loopback())
                }
            };
            (url.scheme() == scheme && !loopback).then(|| proxy.clone())
        });
        builder = builder.proxy(custom.no_proxy(reqwest::NoProxy::from_string(&no_proxy)));
    }
    Ok(builder)
}

/// Go `initInternalClient`: `http.DefaultTransport` without cluster TLS, a
/// bare `http.Transport` (no proxy at all) with it.
fn init_internal_client(
    security: &tidb_pd_client::ClusterSecurity,
) -> Result<reqwest::blocking::Client, String> {
    let builder = if security.is_tls_enabled() {
        reqwest::blocking::Client::builder().no_proxy()
    } else {
        default_transport_client_builder()?
    };
    let mut builder = builder.timeout(std::time::Duration::from_secs(5 * 60));
    if security.is_tls_enabled() {
        let ca = std::fs::read(security.ca_path()).map_err(|error| error.to_string())?;
        let roots =
            reqwest::Certificate::from_pem_bundle(&ca).map_err(|error| error.to_string())?;
        if roots.is_empty() {
            return Err("cluster CA contains no certificates".to_owned());
        }
        builder = builder.tls_certs_only(roots);
        if !security.cert_path().is_empty() && !security.key_path().is_empty() {
            let mut identity =
                std::fs::read(security.cert_path()).map_err(|error| error.to_string())?;
            identity.push(b'\n');
            identity.extend(std::fs::read(security.key_path()).map_err(|error| error.to_string())?);
            builder = builder.identity(
                reqwest::Identity::from_pem(&identity).map_err(|error| error.to_string())?,
            );
        }
    }
    builder.build().map_err(|error| error.to_string())
}

/// Runs one request of the node's PD HTTP client (Go `pdhttp.Client`, which
/// `KVStore.GetPDHTTPClient` hands out) from synchronous code, on the store
/// runtime its connections live on.
pub fn block_on_pd_http<T, F>(request: F) -> Result<T, String>
where
    F: std::future::Future<Output = tikv_client::Result<T>> + Send,
    T: Send,
{
    tidb_txnkv::driver::tikv_store::block_on_store_runtime(request)?
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    /// Go `ProxyFromEnvironment` never proxies a loopback host, and a
    /// non-loopback request goes to `HTTP_PROXY` (here a listener answering
    /// as the proxy), not to the operating system's proxy.
    #[test]
    fn the_internal_client_proxies_from_the_environment_only() {
        use std::io::{Read, Write};
        let serve = |answer: &'static str| {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = listener.local_addr().unwrap().port();
            std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0u8; 4096];
                let read = stream.read(&mut request).unwrap();
                let first = String::from_utf8_lossy(&request[..read]).lines().next().unwrap().to_owned();
                let body = format!("{answer} {first}");
                write!(stream, "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).unwrap();
            });
            port
        };
        let proxy = serve("proxy");
        let target = serve("direct");
        let proxy_url = format!("http://127.0.0.1:{proxy}");
        let client = super::proxy_from_environment(|key| (key == "HTTP_PROXY").then(|| proxy_url.clone()))
            .unwrap()
            .build()
            .unwrap();
        let direct = client.get(format!("http://127.0.0.1:{target}/status")).send().unwrap().text().unwrap();
        assert_eq!(direct, "direct GET /status HTTP/1.1");
        let proxied = client.get("http://10.255.255.1:20180/config").send().unwrap().text().unwrap();
        assert_eq!(proxied, "proxy GET http://10.255.255.1:20180/config HTTP/1.1");
    }
}
