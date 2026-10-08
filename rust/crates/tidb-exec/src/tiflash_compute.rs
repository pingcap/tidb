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

//! Go `pkg/util/tiflashcompute`: process topology, autoscaler protocols and
//! dispatch policy. The HTTP client and timestamped cache belong to the fetcher,
//! never to a SQL session. See the package inventory in the batch receipt.

use reqwest::blocking::Client;
use std::sync::{Arc, OnceLock, RwLock};
use tidb_config::tiflash::{get_auto_scaler_type, AutoScalerType};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchPolicy {
    RoundRobin,
    ConsistentHash,
    Invalid,
}
impl DispatchPolicy {
    pub const fn valid_names() -> [&'static str; 2] {
        ["consistent_hash", "round_robin"]
    }
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "consistent_hash" => Ok(Self::ConsistentHash),
            "round_robin" => Ok(Self::RoundRobin),
            _ => Err(format!("unexpected tiflash_compute dispatch policy, expect [consistent_hash round_robin], got {value}")),
        }
    }
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RoundRobin => "round_robin",
            Self::ConsistentHash => "consistent_hash",
            Self::Invalid => "invalid",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryType(pub u32);
impl RecoveryType {
    pub const NULL: Self = Self(0);
    pub const MEM_LIMIT: Self = Self(1);
    pub fn as_str(self) -> Result<&'static str, String> {
        match self {
            Self::NULL => Ok("Null"),
            Self::MEM_LIMIT => Ok("MemLimit"),
            _ => Err("unsupported recovery type for topo_fetcher".into()),
        }
    }
}

/// Preserve Go's internal-error code across the topology/SQL boundary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyError {
    pub code: u16,
    pub message: String,
}
impl TopologyError {
    fn internal(message: &str) -> Self {
        Self {
            code: 1815,
            message: format!("Internal : {message}"),
        }
    }
}
impl From<String> for TopologyError {
    fn from(message: String) -> Self {
        Self {
            code: 1105,
            message,
        }
    }
}
impl From<&str> for TopologyError {
    fn from(message: &str) -> Self {
        message.to_owned().into()
    }
}
impl std::fmt::Display for TopologyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.code == 1815 {
            write!(f, "[util:1815]{}", self.message)
        } else {
            f.write_str(&self.message)
        }
    }
}
impl std::error::Error for TopologyError {}
impl PartialEq<&str> for TopologyError {
    fn eq(&self, other: &&str) -> bool {
        self.message == *other
    }
}

pub trait TopoFetcher: Send + Sync {
    fn fetch_and_get_topo(&self) -> Result<Vec<String>, TopologyError>;
    fn recovery_and_get_topo(
        &self,
        recovery: RecoveryType,
        original_count: i64,
    ) -> Result<Vec<String>, TopologyError>;
}

fn global() -> &'static RwLock<Option<Arc<dyn TopoFetcher>>> {
    static GLOBAL: OnceLock<RwLock<Option<Arc<dyn TopoFetcher>>>> = OnceLock::new();
    GLOBAL.get_or_init(Default::default)
}
pub fn global_topo_fetcher() -> Option<Arc<dyn TopoFetcher>> {
    global()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}
pub fn init_global_topo_fetcher(
    typ: &str,
    address: &str,
    cluster: &str,
    fixed: bool,
) -> Result<(), String> {
    log(
        "init globalTopoFetcher",
        &format!("type={typ} addr={address} clusterID={cluster} isFixedPool={fixed}"),
    );
    if cluster.is_empty() || address.is_empty() {
        return Err(format!(
            "ClusterID({cluster}) or AutoScaler({address}) addr is empty"
        ));
    }
    let fetcher: Option<Arc<dyn TopoFetcher>> = match get_auto_scaler_type(typ) {
        AutoScalerType::Mock => Some(Arc::new(MockTopoFetcher::new(address)?)),
        AutoScalerType::Aws => Some(Arc::new(AwsTopoFetcher::new(address, cluster, fixed)?)),
        AutoScalerType::Test => Some(Arc::new(TestTopoFetcher)),
        AutoScalerType::Gcp => return Err(format!("topo fetch not implemented yet({typ})")),
        AutoScalerType::Invalid => None,
    };
    let valid = fetcher.is_some();
    *global()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = fetcher;
    if valid {
        Ok(())
    } else {
        Err(format!(
            "unexpected topo fetch type. expect: mock or aws or gcp, got {typ}"
        ))
    }
}

fn log(message: &str, detail: &str) {
    tidb_log::info(
        message,
        &[tidb_log::Field::new(
            "topology",
            tidb_log::Value::Str(detail.to_owned()),
        )],
    );
}
fn http_error() -> TopologyError {
    TopologyError::internal("get tiflash_compute topology failed")
}
fn client() -> Result<Client, String> {
    // Go http.DefaultTransport's dial timeout; neither http.Get nor this
    // client imposes a whole-response timeout.
    Client::builder()
        .timeout(None)
        .connect_timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|_| http_error().to_string())
}
fn get(client: &Client, url: reqwest::Url) -> Result<Vec<u8>, TopologyError> {
    log("fetchTopo", url.as_str());
    let result = (|| {
        let response = client.get(url).send().map_err(|error| error.to_string())?;
        let status = response.status();
        let bytes = response.bytes().map_err(|error| error.to_string())?;
        if status.as_u16() != 200 {
            return Err(format!(
                "http get AutoScaler failed: {status}: {}",
                String::from_utf8_lossy(&bytes)
            ));
        }
        Ok(bytes.to_vec())
    })();
    result.map_err(|error: String| {
        tidb_log::error(&error, &[]);
        http_error()
    })
}
fn url(address: &str, path: &str) -> Result<reqwest::Url, TopologyError> {
    reqwest::Url::parse(&format!("http://{address}/{path}")).map_err(|_| http_error())
}

pub struct MockTopoFetcher {
    address: String,
    client: Client,
    topology: RwLock<Vec<String>>,
}
impl MockTopoFetcher {
    pub fn new(address: &str) -> Result<Self, String> {
        Ok(Self {
            address: address.into(),
            client: client()?,
            topology: RwLock::default(),
        })
    }
}
impl TopoFetcher for MockTopoFetcher {
    fn fetch_and_get_topo(&self) -> Result<Vec<String>, TopologyError> {
        let bytes = get(&self.client, url(&self.address, "fetch_topo")?)?;
        if bytes.is_empty() {
            return Err("topo list is empty".into());
        }
        let topology = String::from_utf8_lossy(&bytes)
            .split(';')
            .map(str::to_owned)
            .collect();
        *self
            .topology
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = topology;
        let current = self
            .topology
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        log("FetchAndGetTopo", &format!("{current:?}"));
        Ok(current)
    }
    fn recovery_and_get_topo(&self, _: RecoveryType, _: i64) -> Result<Vec<String>, TopologyError> {
        Err("RecoveryAndGetTopo not implemented".into())
    }
}

#[derive(Default, Debug)]
struct TopologyResponse {
    has_error: Option<i64>,
    error_info: Option<String>,
    state: Option<String>,
    topology: Option<Vec<Option<String>>>,
    timestamp: Option<String>,
}
// encoding/json matches field names case-insensitively and leaves scalar
// values unchanged for null, including duplicate fields. Preserve map order.
impl<'de> serde::Deserialize<'de> for TopologyResponse {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = TopologyResponse;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an autoscaler topology object")
            }
            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Self::Value, A::Error> {
                let mut result = TopologyResponse::default();
                while let Some(key) = map.next_key::<String>()? {
                    match key.to_ascii_lowercase().as_str() {
                        "haserror" => {
                            if let Some(value) = map.next_value::<Option<i64>>()? {
                                result.has_error = Some(value);
                            }
                        }
                        "errorinfo" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                result.error_info = Some(value);
                            }
                        }
                        "state" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                result.state = Some(value);
                            }
                        }
                        "timestamp" => {
                            if let Some(value) = map.next_value::<Option<String>>()? {
                                result.timestamp = Some(value);
                            }
                        }
                        "topology" => result.topology = map.next_value()?,
                        _ => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(result)
            }
        }
        deserializer.deserialize_map(Visitor)
    }
}
struct CachedTopology {
    nodes: Vec<String>,
    timestamp: i64,
}
pub struct AwsTopoFetcher {
    address: String,
    cluster: String,
    fixed: bool,
    client: Client,
    topology: RwLock<CachedTopology>,
}
impl AwsTopoFetcher {
    pub fn new(address: &str, cluster: &str, fixed: bool) -> Result<Self, String> {
        Ok(Self {
            address: address.into(),
            cluster: cluster.into(),
            fixed,
            client: client()?,
            topology: RwLock::new(CachedTopology {
                nodes: Vec::new(),
                timestamp: -1,
            }),
        })
    }
    fn cached(&self) -> Vec<String> {
        self.topology
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .nodes
            .clone()
    }
    fn update(&self, response: TopologyResponse) -> Result<bool, TopologyError> {
        let timestamp = response
            .timestamp
            .as_deref()
            .unwrap_or_default()
            .parse::<i64>()
            .map_err(|_| {
                TopologyError::internal("parse timestamp of tiflash_compute topology failed")
            })?;
        if self
            .topology
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .timestamp
            >= timestamp
        {
            return Ok(false);
        }
        let mut cache = self
            .topology
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Go rechecks strictly greater after acquiring the write lock.
        if cache.timestamp > timestamp {
            return Ok(false);
        }
        cache.timestamp = timestamp;
        cache.nodes = response
            .topology
            .unwrap_or_default()
            .into_iter()
            .map(Option::unwrap_or_default)
            .collect();
        log(
            "try update topo",
            &format!("timestamp={timestamp} topology={:?}", cache.nodes),
        );
        Ok(true)
    }
    fn fetch(
        &self,
        recovery: RecoveryType,
        original_count: i64,
    ) -> Result<Vec<String>, TopologyError> {
        if recovery != RecoveryType::NULL && recovery != RecoveryType::MEM_LIMIT {
            return Err(format!("topo_fetcher cannot handle error: {}", recovery.0).into());
        }
        if recovery == RecoveryType::MEM_LIMIT && original_count == 0 {
            return Err("ori CN count should not be zero".into());
        }
        if self.fixed {
            let cached = self.cached();
            if !cached.is_empty() {
                return Ok(cached);
            }
        }
        let mut url = url(
            &self.address,
            if self.fixed {
                "sharedfixedpool"
            } else {
                "resume-and-get-topology"
            },
        )?;
        if !self.fixed {
            let mut query = url.query_pairs_mut();
            if recovery == RecoveryType::MEM_LIMIT {
                query.append_pair("cn_cnt", &original_count.to_string());
                query.append_pair("recovery", recovery.as_str()?);
            }
            query.append_pair("tidbclusterid", &self.cluster);
        }
        let bytes = get(&self.client, url)?;
        let response: TopologyResponse = serde_json::from_str(&String::from_utf8_lossy(&bytes))
            .map_err(|error| {
                tidb_log::error(&error.to_string(), &[]);
                http_error()
            })?;
        log("awsHTTPGetAndParseResp succeed", &format!("{response:?}"));
        // hasError/errorInfo/state are decoded by Go but do not reject topology.
        let _ = (&response.has_error, &response.error_info, &response.state);
        self.update(response)?;
        let cached = self.cached();
        log(
            "AWSTopoFetcher FetchAndGetTopo done",
            &format!("{cached:?}"),
        );
        Ok(cached)
    }
}
impl TopoFetcher for AwsTopoFetcher {
    fn fetch_and_get_topo(&self) -> Result<Vec<String>, TopologyError> {
        self.fetch(RecoveryType::NULL, 0)
    }
    fn recovery_and_get_topo(
        &self,
        recovery: RecoveryType,
        original_count: i64,
    ) -> Result<Vec<String>, TopologyError> {
        self.fetch(recovery, original_count)
    }
}
pub struct TestTopoFetcher;
impl TopoFetcher for TestTopoFetcher {
    fn fetch_and_get_topo(&self) -> Result<Vec<String>, TopologyError> {
        Ok(Vec::new())
    }
    fn recovery_and_get_topo(&self, _: RecoveryType, _: i64) -> Result<Vec<String>, TopologyError> {
        Err("RecoveryAndGetTopo not implemented".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    // Actual HTTP requests exercise query encoding, status/body handling and
    // cache state together; no alternate production fetcher is injected.
    fn server(
        responses: Vec<(u16, &'static str)>,
    ) -> (String, std::thread::JoinHandle<Vec<String>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let thread = std::thread::spawn(move || {
            let mut paths = Vec::new();
            for (status, body) in responses {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).unwrap();
                    bytes.push(byte[0]);
                }
                paths.push(
                    String::from_utf8(bytes)
                        .unwrap()
                        .lines()
                        .next()
                        .unwrap()
                        .to_owned(),
                );
                write!(socket, "HTTP/1.1 {status} status\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
            paths
        });
        (address, thread)
    }

    #[test]
    fn compute_topology_batch_aws_cache_recovery_and_errors() {
        let (address, thread) = server(vec![
            (
                200,
                r#"{"timestamp":"10","topology":["cn-a","cn-b"],"hasError":1,"errorInfo":"ignored","state":"resuming"}"#,
            ),
            (200, r#"{"timestamp":"9","topology":["stale"]}"#),
            (200, r#"{"timestamp":"11","topology":["scaled"]}"#),
            (200, r#"{"timestamp":"broken","topology":["bad"]}"#),
            (503, "unavailable"),
            (200, "bad json"),
            (200, r#"{"timestamp":"12","topology":null}"#),
        ]);
        let fetcher = AwsTopoFetcher::new(&address, "a b&c", false).unwrap();
        assert_eq!(fetcher.fetch_and_get_topo().unwrap(), ["cn-a", "cn-b"]);
        assert_eq!(fetcher.fetch_and_get_topo().unwrap(), ["cn-a", "cn-b"]);
        assert_eq!(
            fetcher
                .recovery_and_get_topo(RecoveryType::MEM_LIMIT, 2)
                .unwrap(),
            ["scaled"]
        );
        assert!(fetcher
            .fetch_and_get_topo()
            .unwrap_err()
            .message
            .contains("parse timestamp"));
        assert_eq!(fetcher.cached(), ["scaled"]);
        for _ in 0..2 {
            assert_eq!(fetcher.fetch_and_get_topo().unwrap_err(), http_error());
        }
        assert_eq!(fetcher.cached(), ["scaled"]);
        assert!(fetcher.fetch_and_get_topo().unwrap().is_empty());
        let paths = thread.join().unwrap();
        assert_eq!(
            paths[0],
            "GET /resume-and-get-topology?tidbclusterid=a+b%26c HTTP/1.1"
        );
        assert_eq!(paths[2], "GET /resume-and-get-topology?cn_cnt=2&recovery=MemLimit&tidbclusterid=a+b%26c HTTP/1.1");
    }

    #[test]
    fn compute_topology_batch_fixed_pool_and_recovery_validation() {
        let (address, thread) = server(vec![
            (200, r#"{"timestamp":"1","topology":[]}"#),
            (200, r#"{"timestamp":"2","topology":["fixed"]}"#),
        ]);
        let fetcher = AwsTopoFetcher::new(&address, "cluster", true).unwrap();
        assert!(fetcher.fetch_and_get_topo().unwrap().is_empty());
        assert_eq!(fetcher.fetch_and_get_topo().unwrap(), ["fixed"]);
        assert_eq!(
            thread.join().unwrap(),
            vec!["GET /sharedfixedpool HTTP/1.1"; 2]
        );
        // Listener is gone: cached nonempty fixed pools perform no HTTP.
        assert_eq!(fetcher.fetch_and_get_topo().unwrap(), ["fixed"]);
        assert_eq!(
            fetcher
                .recovery_and_get_topo(RecoveryType::MEM_LIMIT, -1)
                .unwrap(),
            ["fixed"]
        );
        assert_eq!(
            fetcher
                .recovery_and_get_topo(RecoveryType::MEM_LIMIT, 0)
                .unwrap_err(),
            "ori CN count should not be zero"
        );
        assert_eq!(
            fetcher
                .recovery_and_get_topo(RecoveryType(9), 1)
                .unwrap_err(),
            "topo_fetcher cannot handle error: 9"
        );
        assert!(RecoveryType(9).as_str().is_err());
    }

    #[test]
    fn compute_topology_batch_mock_and_global_publication() {
        let (address, thread) = server(vec![(200, "one;; two;"), (200, ""), (500, "failure")]);
        let fetcher = MockTopoFetcher::new(&address).unwrap();
        assert_eq!(
            fetcher.fetch_and_get_topo().unwrap(),
            ["one", "", " two", ""]
        );
        assert_eq!(
            fetcher.fetch_and_get_topo().unwrap_err(),
            "topo list is empty"
        );
        assert_eq!(fetcher.fetch_and_get_topo().unwrap_err(), http_error());
        assert!(fetcher
            .recovery_and_get_topo(RecoveryType::NULL, 0)
            .is_err());
        assert_eq!(thread.join().unwrap(), vec!["GET /fetch_topo HTTP/1.1"; 3]);
        init_global_topo_fetcher("test", "unused", "cluster", false).unwrap();
        let previous = global_topo_fetcher().unwrap();
        assert!(previous.fetch_and_get_topo().unwrap().is_empty());
        assert!(previous
            .recovery_and_get_topo(RecoveryType::MEM_LIMIT, 1)
            .is_err());
        assert!(init_global_topo_fetcher("aws", "", "cluster", false).is_err());
        assert!(init_global_topo_fetcher("gcp", "unused", "cluster", false).is_err());
        assert!(Arc::ptr_eq(&previous, &global_topo_fetcher().unwrap()));
        assert!(init_global_topo_fetcher("bogus", "unused", "cluster", false).is_err());
        assert!(global_topo_fetcher().is_none());
    }

    #[test]
    fn compute_topology_batch_concurrent_timestamp_publication() {
        let fetcher = Arc::new(AwsTopoFetcher::new("unused", "cluster", false).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(16));
        std::thread::scope(|scope| {
            for timestamp in 0..16 {
                let fetcher = Arc::clone(&fetcher);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    fetcher
                        .update(TopologyResponse {
                            timestamp: Some(timestamp.to_string()),
                            topology: Some(vec![Some(timestamp.to_string())]),
                            ..Default::default()
                        })
                        .unwrap();
                });
            }
        });
        assert_eq!(fetcher.cached(), ["15"]);
        assert!(!fetcher
            .update(TopologyResponse {
                timestamp: Some("15".into()),
                topology: Some(vec![Some("equal".into())]),
                ..Default::default()
            })
            .unwrap());
        assert_eq!(fetcher.cached(), ["15"]);
        let response: TopologyResponse = serde_json::from_str(
            r#"{"TiMeStAmP":"16","timestamp":null,"Topology":[null,"cn"],"unknown":{"value":1}}"#,
        )
        .unwrap();
        fetcher.update(response).unwrap();
        assert_eq!(fetcher.cached(), ["", "cn"]);
        assert_eq!(http_error().code, 1815);
        assert_eq!(
            http_error().to_string(),
            "[util:1815]Internal : get tiflash_compute topology failed"
        );
    }

    #[test]
    fn compute_topology_batch_dispatch_policy() {
        for name in DispatchPolicy::valid_names() {
            assert_eq!(DispatchPolicy::parse(name).unwrap().as_str(), name);
        }
        assert!(DispatchPolicy::parse("ROUND_ROBIN").is_err());
        assert_eq!(DispatchPolicy::Invalid.as_str(), "invalid");
    }
}
