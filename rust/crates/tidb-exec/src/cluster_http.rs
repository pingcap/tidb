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
pub(crate) fn internal_http_client(
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

/// Go `initInternalClient`.
fn init_internal_client(
    security: &tidb_pd_client::ClusterSecurity,
) -> Result<reqwest::blocking::Client, String> {
    let mut builder =
        reqwest::blocking::Client::builder().timeout(std::time::Duration::from_secs(5 * 60));
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
pub(crate) fn block_on_pd_http<T, F>(request: F) -> Result<T, String>
where
    F: std::future::Future<Output = tikv_client::Result<T>> + Send,
    T: Send,
{
    tidb_txnkv::driver::tikv_store::block_on_store_runtime(request)?
        .map_err(|error| error.to_string())
}

/// Go infosync `GlobalInfoSyncerInit`'s client: the store's PD HTTP client
/// `WithCallerID("tidb-info-syncer").WithRespHandler(pdResponseHandler)`,
/// which the placement, label-rule, TiFlash replica and affinity managers
/// share.
pub(crate) fn info_syncer_pd_http_client(
    pd_http_client: &tikv_client::pd_http::Client,
) -> tikv_client::pd_http::Client {
    let handler: tikv_client::pd_http::RespHandleFunc = std::sync::Arc::new(pd_response_handler);
    pd_http_client
        .with_caller_id("tidb-info-syncer")
        .with_resp_handler(handler)
}

/// Go infosync `pdResponseHandler`: a 200 answer decodes into the result; a
/// 404 or 412 answer is no error and leaves the result's zero value; any
/// other status is `ErrHTTPServiceError` carrying the body, without the
/// status.
fn pd_response_handler(
    resp: tikv_client::pd_http::HttpResponse,
    res: Option<&mut dyn tikv_client::pd_http::ResponseTarget>,
) -> tikv_client::Result<()> {
    if resp.status_code == 200 {
        return match res {
            Some(res) => res.unmarshal(&resp.body),
            None => Ok(()),
        };
    }
    tidb_util::logutil::bg_logger().warn(
        "response not 200",
        &[
            tidb_log::Field::new(
                "method",
                tidb_log::Value::Str(resp.request.method.as_str().to_owned()),
            ),
            tidb_log::Field::new("url", tidb_log::Value::Str(resp.request.url.clone())),
            tidb_log::Field::new(
                "http status",
                tidb_log::Value::U64(u64::from(resp.status_code)),
            ),
        ],
    );
    if resp.status_code != 404 && resp.status_code != 412 {
        return Err(tikv_client::Error::StringError(format!(
            "{}{}",
            http_service_error_prefix(),
            String::from_utf8_lossy(&resp.body)
        )));
    }
    Ok(())
}

// Go `ErrHTTPServiceError.FastGen("%s", body)`'s text before the body.
fn http_service_error_prefix() -> String {
    format!(
        "[domain:{}]",
        tidb_error::tidb::errcode::ErrHTTPServiceError
    )
}

/// Whether `message` is the info syncer's `ErrHTTPServiceError` (see
/// [`info_syncer_pd_http_client`]), which Go keeps as a coded TiDB error.
pub(crate) fn is_http_service_error(message: &str) -> bool {
    message.starts_with(&http_service_error_prefix())
}
