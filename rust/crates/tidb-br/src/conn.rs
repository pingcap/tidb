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

//! Go `br/pkg/conn` and `br/pkg/conn/util`: BR's connection to the
//! cluster (`Mgr`): PD, the TiKV stores, the GC manager and TiDB's domain.
//!
//! Go's `NewMgr` dials a PD client and opens a `kv.Storage` through the
//! glue; inside TiDB the glue answers TiDB's own store, so here the
//! storage is opened first and the PD controller is built over the store's
//! PD clients. The log backup client (`GetLogBackupClient`) is not
//! reachable from SQL and is not ported.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::metapb;

use crate::config::KvConfig;
use crate::errors::{self, Result, ERR_PD_INVALID_RESPONSE};
use crate::gc;
use crate::glue::{Domain, Glue, Storage};

use crate::pdclient::{PdClient, TikvPdClient};
use crate::pdutil::PdController;
use crate::utils::backoff::new_aggressive_pd_backoff_strategy;
use crate::utils::retry::with_retry;
use crate::utils::store_manager::{Keepalive, StoreManager};
use crate::version;

/// Go `DefaultMergeRegionSizeBytes`.
pub const DEFAULT_MERGE_REGION_SIZE_BYTES: u64 = 96 * 1024 * 1024;
/// Go `DefaultMergeRegionKeyCount`.
pub const DEFAULT_MERGE_REGION_KEY_COUNT: u64 = 960_000;
/// Go `DefaultImportNumGoroutines`.
pub const DEFAULT_IMPORT_NUM_GOROUTINES: u64 = 36;
/// Go `minRestoreConcurrencyOverImportThreads`.
const MIN_RESTORE_CONCURRENCY_OVER_IMPORT_THREADS: u64 = 4;

/// Go `util.StoreBehavior`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreBehavior {
    /// Go `ErrorOnTiFlash`.
    ErrorOnTiFlash,
    /// Go `SkipTiFlash`.
    SkipTiFlash,
    /// Go `TiFlashOnly`.
    TiFlashOnly,
}

/// Go `VersionCheckerType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VersionCheckerType {
    /// Go `NormalVersionChecker`.
    Normal,
    /// Go `StreamVersionChecker`.
    Stream,
    /// Go `NoVersionChecker`.
    None,
}

/// Go `util.GetAllTiKVStores`.
///
/// # Errors
/// PD's error, or an active TiFlash store under
/// [`StoreBehavior::ErrorOnTiFlash`].
pub fn get_all_tikv_stores(
    ctx: &Context,
    pd_client: &dyn PdClient,
    behavior: StoreBehavior,
) -> Result<Vec<metapb::Store>> {
    let stores = pd_client.get_all_stores(ctx, true)?;
    let mut out = Vec::with_capacity(stores.len());
    for store in stores {
        let tiflash = version::is_tiflash(&store);
        if tiflash {
            match behavior {
                StoreBehavior::SkipTiFlash => continue,
                StoreBehavior::ErrorOnTiFlash => {
                    return Err(errors::annotatef(
                        &ERR_PD_INVALID_RESPONSE,
                        format!(
                            "cannot restore to a cluster with active TiFlash stores (store {} at {})",
                            store.id, store.address
                        ),
                    ));
                }
                StoreBehavior::TiFlashOnly => {}
            }
        }
        if !tiflash && behavior == StoreBehavior::TiFlashOnly {
            continue;
        }
        out.push(store);
    }
    Ok(out)
}

/// Go `GetAllTiKVStoresWithRetry` (`conn` and `conn/util`), with `conn`'s
/// test failpoints.
///
/// # Errors
/// The last attempts' errors.
pub fn get_all_tikv_stores_with_retry(
    ctx: &Context,
    pd_client: &dyn PdClient,
    behavior: StoreBehavior,
) -> Result<Vec<metapb::Store>> {
    let mut stores = Vec::new();
    with_retry(
        ctx,
        || {
            const FP: &str = "github.com/pingcap/tidb/br/pkg/conn/";
            if tidb_failpoint::eval(&format!("{FP}hint-GetAllTiKVStores-error")).is_some_and(|v| v.bool()) {
                return Err(errors::grpc(tonic::Status::unknown("Retryable error")));
            }
            if tidb_failpoint::eval(&format!("{FP}hint-GetAllTiKVStores-grpc-cancel")).is_some_and(|v| v.bool()) {
                return Err(errors::grpc(tonic::Status::cancelled("Cancel Retry")));
            }
            if tidb_failpoint::eval(&format!("{FP}hint-GetAllTiKVStores-ctx-cancel")).is_some_and(|v| v.bool()) {
                return Err(Arc::new(tidb_gocontext::CtxError::Canceled));
            }
            stores = get_all_tikv_stores(ctx, pd_client, behavior)?;
            Ok(())
        },
        &mut new_aggressive_pd_backoff_strategy(),
    )?;
    Ok(stores)
}

/// Go `util.GetCurrentTsFromPD`.
///
/// # Errors
/// PD's error.
pub fn get_current_ts_from_pd(ctx: &Context, pd_client: &dyn PdClient) -> Result<u64> {
    let (p, l) = pd_client.get_ts(ctx)?;
    Ok(crate::pdclient::compose_ts(p, l))
}

/// Go `util.GetCurrentTsFromPDWithRetry`.
///
/// # Errors
/// The attempts' errors.
pub fn get_current_ts_from_pd_with_retry(ctx: &Context, pd_client: &dyn PdClient) -> Result<u64> {
    let mut ts = 0;
    let mut retry = 0_u64;
    let res = with_retry(
        ctx,
        || {
            retry += 1;
            match get_current_ts_from_pd(ctx, pd_client) {
                Ok(v) => {
                    ts = v;
                    Ok(())
                }
                Err(err) => {
                    tidb_log::warn(
                        "failed to get current TS from PD, retry it",
                        &[Field::new("retry time", Value::U64(retry)), Field::new("error", Value::Str(err.to_string()))],
                    );
                    Err(err)
                }
            }
        },
        &mut new_aggressive_pd_backoff_strategy(),
    );
    if let Err(err) = &res {
        tidb_log::error("failed to get current TS from PD", &[Field::new("error", Value::Str(err.to_string()))]);
    }
    res.map(|()| ts)
}

/// Go `util.HandleTiKVAddress`: the store's status address, with the
/// store address's host when the two hosts differ.
///
/// # Errors
/// No status address, or an unparsable address.
pub fn handle_tikv_address(store: &metapb::Store, http_prefix: &str) -> Result<String> {
    if store.status_address.is_empty() {
        return Err(errors::errorf(format!("TiKV store {} does not have status address", store.id)));
    }
    let mut status_addr = store.status_address.clone();
    let mut node_addr = store.address.clone();
    if !status_addr.starts_with("http") {
        status_addr = format!("{http_prefix}{status_addr}");
    }
    if !node_addr.starts_with("http") {
        node_addr = format!("{http_prefix}{node_addr}");
    }
    let parse = |s: &str| tidb_go_url::parse(s.as_bytes()).map_err(|e| errors::errorf(e.to_string()));
    let mut status_url = parse(&status_addr)?;
    let node_url = parse(&node_addr)?;
    if status_url.hostname() != node_url.hostname() {
        let host = join_host_port(&String::from_utf8_lossy(node_url.hostname()), &String::from_utf8_lossy(status_url.port()));
        status_url.host = host.into_bytes();
        tidb_log::warn(
            "store address and status address mismatch the host, we will use the store address as hostname",
            &[
                Field::new("store", Value::U64(store.id)),
                Field::new("status address", Value::Str(status_addr)),
                Field::new("node address", Value::Str(node_addr)),
            ],
        );
    }
    Ok(String::from_utf8_lossy(&status_url.to_bytes()).into_owned())
}

use tidb_go_net::join_host_port;

/// Go `httputil.NewClient(tlsConf)`: a client for TiKV's status ports.
///
/// # Errors
/// The TLS files cannot be read.
pub fn new_http_client(security: &tidb_pd_client::ClusterSecurity) -> Result<reqwest::blocking::Client> {
    let mut builder = reqwest::blocking::Client::builder().no_proxy();
    if security.is_tls_enabled() {
        let ca = std::fs::read(security.ca_path()).map_err(|e| errors::errorf(e.to_string()))?;
        let cert = reqwest::Certificate::from_pem(&ca).map_err(|e| errors::errorf(e.to_string()))?;
        builder = builder.add_root_certificate(cert);
        if !security.cert_path().is_empty() {
            let mut pem = std::fs::read(security.cert_path()).map_err(|e| errors::errorf(e.to_string()))?;
            pem.extend(std::fs::read(security.key_path()).map_err(|e| errors::errorf(e.to_string()))?);
            let identity = reqwest::Identity::from_pem(&pem).map_err(|e| errors::errorf(e.to_string()))?;
            builder = builder.identity(identity);
        }
    }
    builder.build().map_err(|e| errors::errorf(e.to_string()))
}

/// Go `util.GetConfigFromTiKVStores`: `f` over each up store's `/config`
/// response, retried with the aggressive PD strategy.
///
/// # Errors
/// An address, request or `f` error.
pub fn get_config_from_tikv_stores(
    ctx: &Context,
    stores: &[metapb::Store],
    cli: &reqwest::blocking::Client,
    http_prefix: &str,
    f: &mut dyn FnMut(reqwest::blocking::Response) -> Result<()>,
) -> Result<()> {
    for store in stores {
        if store.state != metapb::StoreState::Up as i32 {
            continue;
        }
        let addr = handle_tikv_address(store, http_prefix)?;
        let config_addr = format!("{}/config", addr.trim_end_matches('/'));
        with_retry(
            ctx,
            || {
                let resp = cli.get(&config_addr).send().map_err(|e| errors::errorf(e.to_string()))?;
                f(resp)
            },
            &mut new_aggressive_pd_backoff_strategy(),
        )?;
    }
    Ok(())
}

/// Go `util.GetConfigBytesFromTiKVStores`.
///
/// # Errors
/// A non-200 response, or [`get_config_from_tikv_stores`]'s errors.
pub fn get_config_bytes_from_tikv_stores(
    ctx: &Context,
    stores: &[metapb::Store],
    cli: &reqwest::blocking::Client,
    http_prefix: &str,
    collect: &mut dyn FnMut(Vec<u8>) -> Result<()>,
) -> Result<()> {
    get_config_from_tikv_stores(ctx, stores, cli, http_prefix, &mut |resp| {
        if resp.status() != reqwest::StatusCode::OK {
            return Err(errors::errorf(format!(
                "request {} failed: {}",
                resp.url(),
                go_http_status(resp.status())
            )));
        }
        let bytes = resp.bytes().map_err(|e| errors::errorf(e.to_string()))?;
        collect(bytes.to_vec())
    })
}

/// Go `http.Response.Status`: `"404 Not Found"`.
fn go_http_status(s: reqwest::StatusCode) -> String {
    format!("{} {}", s.as_u16(), s.canonical_reason().unwrap_or(""))
}

/// Go `checkStoresAlive`: logs how many stores are up.
fn check_stores_alive(ctx: &Context, pd_client: &dyn PdClient, behavior: StoreBehavior) -> Result<()> {
    let stores = get_all_tikv_stores(ctx, pd_client, behavior).inspect_err(|err| {
        tidb_log::error("failed to get store", &[Field::new("error", Value::Str(err.to_string()))]);
    })?;
    let live = stores.iter().filter(|s| s.state == metapb::StoreState::Up as i32).count();
    tidb_log::info(
        "checked alive KV stores",
        &[Field::new("aliveStores", Value::I64(live as i64)), Field::new("totalStores", Value::I64(stores.len() as i64))],
    );
    Ok(())
}

/// Go `Mgr`.
pub struct Mgr {
    /// Go's embedded `*pdutil.PdController`.
    pub pd_controller: Arc<PdController>,
    dom: Option<Arc<dyn Domain>>,
    storage: Arc<dyn Storage>,
    owns_storage: bool,
    gc_manager: Arc<dyn gc::Manager>,
    /// Go's embedded `*utils.StoreManager`.
    pub store_manager: StoreManager,
}

/// Go `NewMgr`'s arguments.
pub struct MgrOptions<'a> {
    /// Go `keyspaceName`.
    pub keyspace_name: &'a str,
    /// Go `pdAddrs`, logged only: the node's PD client is used.
    pub pd_addrs: &'a [String],
    /// Go `tlsConf` / `securityOption`.
    pub security: tidb_pd_client::ClusterSecurity,
    /// Go `keepalive`.
    pub keepalive: Keepalive,
    /// Go `storeBehavior`.
    pub store_behavior: StoreBehavior,
    /// Go `checkRequirements`.
    pub check_requirements: bool,
    /// Go `needDomain`.
    pub need_domain: bool,
    /// Go `versionCheckerType`.
    pub version_checker: VersionCheckerType,
}

impl Mgr {
    /// Go `NewMgr`.
    ///
    /// # Errors
    /// The storage, PD, version, store or domain error.
    pub fn new(ctx: &Context, g: &dyn Glue, opts: MgrOptions<'_>) -> Result<Self> {
        tidb_log::info("new mgr", &[Field::new("pdAddrs", Value::Str(format!("{:?}", opts.pd_addrs)))]);
        let storage = g.open()?;
        let pd_client: Arc<dyn PdClient> = Arc::new(TikvPdClient::new(storage.tikv().clone()));
        let pd_http = storage
            .tikv()
            .pd_http_client()
            .ok_or_else(|| errors::new(&errors::ERR_KV_NOT_TIKV))?;
        let runtime = Arc::clone(storage.tikv().runtime());
        let controller = PdController::new(Arc::clone(&pd_client), pd_http, Arc::clone(&runtime)).inspect_err(|err| {
            tidb_log::error("failed to create pd controller", &[Field::new("error", Value::Str(err.to_string()))]);
        })?;
        if opts.check_requirements {
            let res = match opts.version_checker {
                VersionCheckerType::Normal => {
                    version::check_cluster_version(ctx, pd_client.as_ref(), &version::check_version_for_br)
                }
                VersionCheckerType::Stream => {
                    version::check_cluster_version(ctx, pd_client.as_ref(), &version::check_version_for_br_pitr)
                }
                VersionCheckerType::None => Ok(()),
            };
            res.map_err(|err| {
                errors::annotate(
                    err,
                    "running BR in incompatible version of cluster, if you believe it's OK, use --check-requirements=false to skip.",
                )
            })?;
        }
        check_stores_alive(ctx, pd_client.as_ref(), opts.store_behavior)?;
        let dom = if opts.need_domain {
            let dom = g.get_domain()?;
            version::check_cluster_version(ctx, pd_client.as_ref(), &version::check_version_for_ddl)
                .map_err(|err| errors::annotate(err, "unable to check cluster version for ddl"))?;
            Some(dom)
        } else {
            None
        };
        let gc_manager = gc::new_manager(Arc::clone(&pd_client), storage.keyspace_id());
        let store_manager = StoreManager::new(Arc::clone(&pd_client), opts.keepalive, opts.security, runtime);
        Ok(Self {
            pd_controller: Arc::new(controller),
            dom,
            storage,
            owns_storage: g.owns_storage(),
            gc_manager,
            store_manager,
        })
    }

    /// Go `GetBackupClient`.
    ///
    /// # Errors
    /// The connection error.
    pub fn get_backup_client(
        &self,
        ctx: &Context,
        store_id: u64,
    ) -> Result<tidb_proto::backup::backup_client::BackupClient<tonic::transport::Channel>> {
        self.store_manager.with_conn(ctx, store_id, crate::utils::store_manager::new_backup_client)
    }

    /// Go `GetPDClient`.
    #[must_use]
    pub fn get_pd_client(&self) -> &Arc<dyn PdClient> {
        self.pd_controller.get_pd_client()
    }

    /// Go `GetStorage` and `GetStore`.
    #[must_use]
    pub fn get_storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }

    /// Go `GetGCManager`.
    #[must_use]
    pub fn get_gc_manager(&self) -> &Arc<dyn gc::Manager> {
        &self.gc_manager
    }

    /// Go `SetGcManager`.
    pub fn set_gc_manager(&mut self, mgr: Arc<dyn gc::Manager>) {
        self.gc_manager = mgr;
    }

    /// Go `GetDomain`.
    #[must_use]
    pub fn get_domain(&self) -> Option<&Arc<dyn Domain>> {
        self.dom.as_ref()
    }

    /// Go `GetTLSConfig() != nil`.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.store_manager.tls_enabled()
    }

    /// Go `Close`. The storage and domain are TiDB's own unless the glue
    /// owns them; inside TiDB it never does.
    pub fn close(&self) {
        self.store_manager.close();
        let _ = self.owns_storage;
        self.pd_controller.close();
    }

    /// Go `GetCurrentTsFromPD`.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_current_ts_from_pd(&self, ctx: &Context) -> Result<u64> {
        get_current_ts_from_pd(ctx, self.get_pd_client().as_ref())
    }

    fn http_prefix(&self) -> &'static str {
        if self.tls_enabled() {
            "https://"
        } else {
            "http://"
        }
    }

    /// Go `ProcessTiKVConfigs`: the region merge size and import threads
    /// from TiKV's config, for the settings the user did not set.
    pub fn process_tikv_configs(&self, ctx: &Context, cfg: &mut KvConfig, client: &reqwest::blocking::Client) {
        let mut merge_size = cfg.merge_region_size;
        let mut merge_keys = cfg.merge_region_key_count;
        let mut import = cfg.import_goroutines;
        if merge_size.modified && merge_keys.modified && import.modified {
            tidb_log::info("no need to retrieve the config from tikv if user has set the config", &[]);
            return;
        }
        let res = self.get_config_from_tikv(ctx, client, &mut |resp| {
            let bytes = resp.bytes().map_err(|e| errors::errorf(e.to_string()))?;
            if !merge_size.modified || !merge_keys.modified {
                let (size, keys) = crate::config::parse_merge_region_size_from_config(&bytes).map_err(|e| {
                    tidb_log::warn("Failed to parse region split size and keys from config", &[Field::new("error", Value::Str(e.clone()))]);
                    errors::errorf(e)
                })?;
                if merge_keys.value == DEFAULT_MERGE_REGION_KEY_COUNT || keys < merge_keys.value {
                    merge_size.value = size;
                    merge_keys.value = keys;
                }
            }
            if !import.modified {
                let threads = crate::config::parse_import_threads_from_config(&bytes).map_err(|e| {
                    tidb_log::warn("Failed to parse import num-threads from config", &[Field::new("error", Value::Str(e.clone()))]);
                    errors::errorf(e)
                })?;
                if threads > 0 {
                    import.value = import.value.max(threads + MIN_RESTORE_CONCURRENCY_OVER_IMPORT_THREADS);
                }
            }
            cfg.merge_region_size = merge_size;
            cfg.merge_region_key_count = merge_keys;
            cfg.import_goroutines = import;
            Ok(())
        });
        if let Err(err) = res {
            tidb_log::warn("Failed to get config from TiKV; using default", &[Field::new("error", Value::Str(err.to_string()))]);
        }
    }

    /// Go `IsLogBackupEnabled`: whether every TiKV enables log backup.
    ///
    /// # Errors
    /// A request or parse error.
    pub fn is_log_backup_enabled(&self, ctx: &Context, client: &reqwest::blocking::Client) -> Result<bool> {
        let mut enabled = true;
        self.get_config_from_tikv(ctx, client, &mut |resp| {
            let bytes = resp.bytes().map_err(|e| errors::errorf(e.to_string()))?;
            let enable = crate::config::parse_log_backup_enable_from_config(&bytes).map_err(|e| {
                tidb_log::warn("Failed to parse log-backup enable from config", &[Field::new("error", Value::Str(e.clone()))]);
                errors::errorf(e)
            })?;
            enabled = enabled && enable;
            Ok(())
        })?;
        Ok(enabled)
    }

    /// Go `Mgr.GetConfigFromTiKV`.
    ///
    /// # Errors
    /// The stores or a request error.
    pub fn get_config_from_tikv(
        &self,
        ctx: &Context,
        client: &reqwest::blocking::Client,
        f: &mut dyn FnMut(reqwest::blocking::Response) -> Result<()>,
    ) -> Result<()> {
        let stores = get_all_tikv_stores_with_retry(ctx, self.get_pd_client().as_ref(), StoreBehavior::SkipTiFlash)?;
        get_config_from_tikv_stores(ctx, &stores, client, self.http_prefix(), f)
    }

    /// Go `GetConfigBytesFromTiKV`.
    ///
    /// # Errors
    /// The stores or a request error.
    pub fn get_config_bytes_from_tikv(
        &self,
        ctx: &Context,
        client: &reqwest::blocking::Client,
        collect: &mut dyn FnMut(Vec<u8>) -> Result<()>,
    ) -> Result<()> {
        let stores = get_all_tikv_stores_with_retry(ctx, self.get_pd_client().as_ref(), StoreBehavior::SkipTiFlash)?;
        get_config_bytes_from_tikv_stores(ctx, &stores, client, self.http_prefix(), collect)
    }
}

#[cfg(test)]
mod tests;
