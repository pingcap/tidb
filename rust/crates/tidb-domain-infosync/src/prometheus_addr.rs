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

//! Go `pkg/domain/infosync/info.go`'s `GetPrometheusAddr`: the address of the
//! cluster's metric storage, which the `metrics_schema` tables query.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tidb_pd_client::ClusterSecurity;

/// Go `TopologyPrometheus`: the etcd key of Prometheus's address.
pub const TOPOLOGY_PROMETHEUS: &str = "/topology/prometheus";
/// Go `TablePrometheusCacheExpiry`: how long a read address is reused.
pub const TABLE_PROMETHEUS_CACHE_EXPIRY: Duration = Duration::from_secs(10);
/// Go `serverinfo.KeyOpDefaultTimeout`.
const KEY_OP_DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
/// Go `pdhttp.Config`.
const PD_HTTP_CONFIG: &str = "/pd/api/v1/config";

/// What `GetPrometheusAddr` answers when it has no address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrometheusAddrError {
    /// Go `ErrPrometheusAddrIsNotSet`: neither PD nor etcd names one.
    NotSet,
    /// Every other failure, by its text.
    Other(String),
}

impl std::fmt::Display for PrometheusAddrError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotSet => write!(
                formatter,
                "{}",
                *crate::infosync_errors::ERR_PROMETHEUS_ADDR_IS_NOT_SET
            ),
            Self::Other(message) => formatter.write_str(message),
        }
    }
}

/// The `InfoSyncer`'s `etcdCli`, as `getPrometheusAddr` uses it.
pub trait PrometheusEtcd: Send + Sync {
    /// Go `clientv3.Client.Endpoints`.
    fn endpoints(&self) -> Vec<String>;
    /// Go `etcdCli.Get(ctx, k)`'s first value, with `timeout` as its context.
    fn get(&self, key: &str, timeout: Duration) -> Result<Option<Vec<u8>>, String>;
}

impl PrometheusEtcd for tidb_pd_client::EtcdClient {
    fn endpoints(&self) -> Vec<String> {
        tidb_pd_client::EtcdClient::endpoints(self).to_vec()
    }

    fn get(&self, key: &str, timeout: Duration) -> Result<Option<Vec<u8>>, String> {
        self.get_with_timeout(key.as_bytes(), timeout)
            .map_err(|error| error.to_string())
    }
}

/// The `InfoSyncer` fields `GetPrometheusAddr` reads and writes.
#[derive(Default)]
struct PrometheusState {
    /// Go `etcdCli`; `None` on a node without PD.
    etcd: Option<Arc<dyn PrometheusEtcd>>,
    /// The cluster security Go's `util.InternalHTTPClient` is built from.
    security: ClusterSecurity,
    /// Go `prometheusAddr`.
    prometheus_addr: String,
    /// Go `modifyTime`; `None` is Go's zero time.
    modify_time: Option<Instant>,
}

static PROMETHEUS_STATE: Mutex<Option<PrometheusState>> = Mutex::new(None);

/// Go `GlobalInfoSyncerInit`'s `etcdCli`: the client `GetPrometheusAddr`
/// reads PD's endpoints and the topology key through.
pub fn init_prometheus_addr(etcd: Option<Arc<dyn PrometheusEtcd>>, security: &ClusterSecurity) {
    *PROMETHEUS_STATE
        .lock()
        .unwrap_or_else(PoisonError::into_inner) = Some(PrometheusState {
        etcd,
        security: security.clone(),
        prometheus_addr: String::new(),
        modify_time: None,
    });
}

/// Go `GetPrometheusAddr`: the cached address while it is younger than
/// [`TABLE_PROMETHEUS_CACHE_EXPIRY`], else a fresh read.
pub fn get_prometheus_addr() -> Result<String, PrometheusAddrError> {
    let mut state = PROMETHEUS_STATE
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let state = state.get_or_insert_with(PrometheusState::default);
    // if the cache of prometheusAddr is over 10s, update the prometheusAddr
    if state
        .modify_time
        .is_some_and(|modify_time| modify_time.elapsed() < TABLE_PROMETHEUS_CACHE_EXPIRY)
    {
        return Ok(state.prometheus_addr.clone());
    }
    state.read_prometheus_addr()
}

impl PrometheusState {
    /// Go `InfoSyncer.getPrometheusAddr`.
    fn read_prometheus_addr(&mut self) -> Result<String, PrometheusAddrError> {
        // Get PD servers info.
        let pd_addrs = self.etcd.as_ref().map(|etcd| etcd.endpoints()).unwrap_or_default();
        let (Some(etcd), Some(pd_addr)) = (&self.etcd, pd_addrs.first()) else {
            return Err(PrometheusAddrError::Other("pd unavailable".to_owned()));
        };
        // Get prometheus address from pdhttp.
        let url = compose_url(pd_addr, PD_HTTP_CONFIG, &self.security);
        let other = |error: String| PrometheusAddrError::Other(error);
        let body = tidb_store_driver_txn::cluster_http::internal_http_client(&self.security)
            .map_err(other)?
            .get(&url)
            .send()
            .and_then(|response| response.bytes())
            .map_err(|error| other(error.to_string()))?;
        let metric_storage: serde_json::Value =
            serde_json::from_slice(&body).map_err(|error| other(error.to_string()))?;
        let mut res = metric_storage["pd-server"]["metric-storage"]
            .as_str()
            .unwrap_or_default()
            .to_owned();

        // Get prometheus address from etcdApi.
        if res.is_empty() {
            let values = etcd
                .get(TOPOLOGY_PROMETHEUS, KEY_OP_DEFAULT_TIMEOUT)
                .map_err(other)?
                .unwrap_or_default();
            if values.is_empty() {
                return Err(PrometheusAddrError::NotSet);
            }
            let prometheus: serde_json::Value =
                serde_json::from_slice(&values).map_err(|error| other(error.to_string()))?;
            res = format!(
                "http://{}",
                join_host_port(
                    prometheus["ip"].as_str().unwrap_or_default(),
                    prometheus["port"].as_i64().unwrap_or_default()
                )
            );
        }
        self.prometheus_addr.clone_from(&res);
        self.modify_time = Some(Instant::now());
        Ok(res)
    }
}

/// Go `util.ComposeURL`: `address` keeps its own scheme, else takes the
/// internal HTTP client's.
fn compose_url(address: &str, path: &str, security: &ClusterSecurity) -> String {
    if address.starts_with("http://") || address.starts_with("https://") {
        return format!("{address}{path}");
    }
    let schema = if security.is_tls_enabled() { "https" } else { "http" };
    format!("{schema}://{address}{path}")
}

/// Go `net.JoinHostPort(host, strconv.Itoa(port))`.
fn join_host_port(host: &str, port: i64) -> String {
    if host.contains(':') || host.contains('%') {
        return format!("[{host}]:{port}");
    }
    format!("{host}:{port}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Etcd {
        endpoints: Vec<String>,
        value: Option<Vec<u8>>,
        gets: AtomicUsize,
    }

    impl PrometheusEtcd for Etcd {
        fn endpoints(&self) -> Vec<String> {
            self.endpoints.clone()
        }

        fn get(&self, key: &str, timeout: Duration) -> Result<Option<Vec<u8>>, String> {
            assert_eq!((key, timeout), ("/topology/prometheus", Duration::from_secs(1)));
            self.gets.fetch_add(1, Ordering::Relaxed);
            Ok(self.value.clone())
        }
    }

    /// A PD whose `/pd/api/v1/config` answers `body`, `requests` times.
    fn pd(body: &'static str, requests: usize) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            for _ in 0..requests {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0_u8; 4096];
                let read = stream.read(&mut request).unwrap();
                assert!(request[..read].starts_with(b"GET /pd/api/v1/config HTTP/1.1\r\n"));
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        address
    }

    fn state(endpoints: Vec<String>, value: Option<&str>) -> (PrometheusState, Arc<Etcd>) {
        let etcd = Arc::new(Etcd {
            endpoints,
            value: value.map(|value| value.as_bytes().to_vec()),
            gets: AtomicUsize::new(0),
        });
        let state = PrometheusState {
            etcd: Some(Arc::clone(&etcd) as Arc<dyn PrometheusEtcd>),
            ..PrometheusState::default()
        };
        (state, etcd)
    }

    #[test]
    fn a_node_without_pd_has_no_prometheus_like_go() {
        let unavailable = Err(PrometheusAddrError::Other("pd unavailable".to_owned()));
        assert_eq!(PrometheusState::default().read_prometheus_addr(), unavailable);
        assert_eq!(state(Vec::new(), None).0.read_prometheus_addr(), unavailable);
    }

    #[test]
    fn pds_metric_storage_is_the_address_and_etcd_is_not_read_like_go() {
        let address = pd(r#"{"pd-server":{"metric-storage":"http://10.0.1.2:9090"}}"#, 1);
        let (mut state, etcd) = state(vec![address], Some(r#"{"ip":"1.1.1.1","port":1}"#));
        assert_eq!(state.read_prometheus_addr().unwrap(), "http://10.0.1.2:9090");
        assert_eq!(etcd.gets.load(Ordering::Relaxed), 0);
        assert_eq!(state.prometheus_addr, "http://10.0.1.2:9090");
        assert!(state.modify_time.is_some());
    }

    #[test]
    fn the_topology_key_is_the_address_when_pd_names_none_like_go() {
        let address = pd(r#"{"pd-server":{"metric-storage":""}}"#, 2);
        let (mut state, _) = state(
            vec![format!("http://{address}")],
            Some(r#"{"ip":"10.0.1.3","binary_path":"/bin/prometheus","port":9091}"#),
        );
        assert_eq!(state.read_prometheus_addr().unwrap(), "http://10.0.1.3:9091");
        let (mut v6, _) = super::tests::state(vec![address], Some(r#"{"ip":"::1","port":9091}"#));
        assert_eq!(v6.read_prometheus_addr().unwrap(), "http://[::1]:9091");
    }

    #[test]
    fn an_absent_topology_key_is_prometheus_addr_is_not_set_like_go() {
        let address = pd(r#"{"pd-server":{}}"#, 1);
        let (mut state, etcd) = state(vec![address], None);
        let error = state.read_prometheus_addr().unwrap_err();
        assert_eq!(error, PrometheusAddrError::NotSet);
        assert_eq!(
            error.to_string(),
            "[domain:9009]Prometheus address is not set in PD and etcd"
        );
        assert_eq!(etcd.gets.load(Ordering::Relaxed), 1);
        // A failed read leaves the cache as it was.
        assert!(state.modify_time.is_none());
    }
}
