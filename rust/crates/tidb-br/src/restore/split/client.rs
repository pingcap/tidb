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

//! Go `br/pkg/restore/split/client.go`: `SplitClient` over PD, with
//! TiKV's `SplitRegion` RPC for the split itself.
//!
//! Keyspace-aware (`CodecPDClient`) splitting and raw KV splitting have no
//! SQL caller here and are not ported: keys are memcomparable-encoded as a
//! classic cluster's PD stores them.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tidb_proto::{kvrpcpb, metapb, pdpb};
use tikv_client::pd_http;

use super::region::RegionInfo;
use super::split::{
    get_split_keys_of_regions, paginate_scan_region, BackoffMayNotCountBackoffer, ERR_BACKOFF,
    ERR_BACKOFF_AND_DONT_COUNT, SCAN_REGION_PAGINATION_LIMIT, SPLIT_MAX_RETRY_INTERVAL,
    SPLIT_RETRY_INTERVAL, SPLIT_RETRY_TIMES,
};
use crate::errors::{self, Error, Result};
use crate::pdclient::PdClient;
use crate::utils::backoff::{
    new_backoff_retry_all_error_strategy, new_backoff_retry_all_except_strategy, BackoffStrategy,
};
use crate::utils::retry::{multierr_append, with_retry, with_retry_return_last_err};

/// Go `splitRegionMaxRetryTime`.
const SPLIT_REGION_MAX_RETRY_TIME: usize = 4;
/// Go `maxBatchSplitSize`.
pub(crate) static MAX_BATCH_SPLIT_SIZE: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(6 << 20);

/// Go `SplitClient`, the operations snapshot restore uses.
pub trait SplitClient: Send + Sync {
    /// Go `GetStore`.
    ///
    /// # Errors
    /// PD's error.
    fn get_store(&self, ctx: &Context, store_id: u64) -> Result<metapb::Store>;
    /// Go `GetRegion`.
    ///
    /// # Errors
    /// PD's error.
    fn get_region(&self, ctx: &Context, key: &[u8]) -> Result<Option<RegionInfo>>;
    /// Go `GetRegionByID`.
    ///
    /// # Errors
    /// PD's error.
    fn get_region_by_id(&self, ctx: &Context, region_id: u64) -> Result<Option<RegionInfo>>;
    /// Go `SplitKeysAndScatter`: the new regions to wait for.
    ///
    /// # Errors
    /// The split's error after its retries.
    fn split_keys_and_scatter(
        &self,
        ctx: &Context,
        sorted_split_keys: &[Vec<u8>],
    ) -> Result<Vec<RegionInfo>>;
    /// Go `SplitKeys`.
    ///
    /// # Errors
    /// The split's error after its retries.
    fn split_keys(&self, ctx: &Context, sorted_split_keys: &[Vec<u8>]) -> Result<Vec<RegionInfo>>;
    /// Go `SplitWaitAndScatter`.
    ///
    /// # Errors
    /// The split's error.
    fn split_wait_and_scatter(
        &self,
        ctx: &Context,
        region: &RegionInfo,
        keys: &[Vec<u8>],
    ) -> Result<Vec<RegionInfo>>;
    /// Go `GetOperator`.
    ///
    /// # Errors
    /// PD's error.
    fn get_operator(&self, ctx: &Context, region_id: u64) -> Result<pdpb::GetOperatorResponse>;
    /// Go `ScanRegions`.
    ///
    /// # Errors
    /// PD's error.
    fn scan_regions(
        &self,
        ctx: &Context,
        key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<RegionInfo>>;
    /// Go `GetPlacementRule`.
    ///
    /// # Errors
    /// PD's error.
    fn get_placement_rule(
        &self,
        ctx: &Context,
        group_id: &str,
        rule_id: &str,
    ) -> Result<pd_http::Rule>;
    /// Go `SetPlacementRule`.
    ///
    /// # Errors
    /// PD's error.
    fn set_placement_rule(&self, ctx: &Context, rule: &pd_http::Rule) -> Result<()>;
    /// Go `DeletePlacementRule`.
    ///
    /// # Errors
    /// PD's error.
    fn delete_placement_rule(&self, ctx: &Context, group_id: &str, rule_id: &str) -> Result<()>;
    /// Go `SetStoresLabel`.
    ///
    /// # Errors
    /// PD's error.
    fn set_stores_label(
        &self,
        ctx: &Context,
        stores: &[u64],
        label_key: &str,
        label_value: &str,
    ) -> Result<()>;
    /// Go `WaitRegionsScattered`: how many regions are still not scattered.
    ///
    /// # Errors
    /// A non-retryable PD error, or `ctx` done.
    fn wait_regions_scattered(&self, ctx: &Context, regions: &[RegionInfo]) -> (usize, Result<()>);
}

/// Go `WithOnSplit`'s callback: the keys of each split batch.
pub type OnSplit = Box<dyn Fn(&[Vec<u8>]) + Send + Sync>;

/// Go `pdClient`.
pub struct PdSplitClient {
    client: Arc<dyn PdClient>,
    http_cli: Option<pd_http::Client>,
    security: tidb_pd_client::ClusterSecurity,
    runtime: Arc<tokio::runtime::Runtime>,
    store_cache: Mutex<HashMap<u64, metapb::Store>>,
    pub(crate) need_scatter_val: OnceLock<bool>,
    on_split: Option<OnSplit>,
    split_concurrency: usize,
    split_batch_key_cnt: usize,
    /// Go's `intest` hook: `MockPDClientForSplit.SplitRegion` answers the
    /// split instead of TiKV.
    #[cfg(test)]
    pub(crate) mock_split: Option<MockSplit>,
}

/// A test stand-in for TiKV's split: retry, response or error.
#[cfg(test)]
pub(crate) type MockSplit = Box<
    dyn Fn(&RegionInfo, &[Vec<u8>]) -> (bool, Result<Option<kvrpcpb::SplitRegionResponse>>)
        + Send
        + Sync,
>;

/// Go `NewClient(client, httpCli, tlsConf, splitBatchKeyCnt,
/// splitConcurrency, WithOnSplit(onSplit))`.
#[must_use]
pub fn new_client(
    client: Arc<dyn PdClient>,
    http_cli: Option<pd_http::Client>,
    security: tidb_pd_client::ClusterSecurity,
    runtime: Arc<tokio::runtime::Runtime>,
    split_batch_key_cnt: usize,
    split_concurrency: usize,
    on_split: Option<OnSplit>,
) -> PdSplitClient {
    PdSplitClient {
        client,
        http_cli,
        security,
        runtime,
        store_cache: Mutex::new(HashMap::new()),
        need_scatter_val: OnceLock::new(),
        on_split,
        split_concurrency,
        split_batch_key_cnt,
        #[cfg(test)]
        mock_split: None,
    }
}

fn status_of(err: &Error) -> Option<&tonic::Status> {
    errors::cause(err.as_ref())
        .downcast_ref::<errors::GrpcStatus>()
        .map(|s| &s.0)
}

/// Go `PdErrorCanRetry`.
#[must_use]
pub fn pd_error_can_retry(err: &Error) -> bool {
    let msg = status_of(err).map_or_else(|| err.to_string(), |s| s.message().to_owned());
    msg.contains("is not fully replicated")
        || msg.contains("has no leader")
        || msg.contains("cannot add an operator to the execute queue")
        || msg.contains("failed to create scatter region operator")
}

/// Go `isUnsupportedError`.
fn is_unsupported_error(err: &Error) -> bool {
    status_of(err).is_some_and(|s| {
        s.code() == tonic::Code::Unimplemented || s.message().contains("region 0 not found")
    })
}

/// Go lightning `common.IsRetryableError`, over the gRPC status a BR
/// error carries.
pub(crate) fn is_retryable_error(err: &Error) -> bool {
    match status_of(err) {
        Some(status) => tidb_lightning_common::is_retryable_error(status),
        None => tidb_lightning_common::is_retryable_error(err.as_ref()),
    }
}

/// Go `CheckRegionEpoch`.
#[must_use]
pub fn check_region_epoch(new: &RegionInfo, old: &RegionInfo) -> bool {
    let (n, o) = (
        new.region.region_epoch.unwrap_or_default(),
        old.region.region_epoch.unwrap_or_default(),
    );
    new.region.id == old.region.id && n.version == o.version && n.conf_ver == o.conf_ver
}

/// Go `isScatterRegionFinished`: done, needs a rescatter, or an error.
///
/// # Errors
/// PD answered an error other than a missing region.
pub fn is_scatter_region_finished(resp: &pdpb::GetOperatorResponse) -> Result<(bool, bool)> {
    if let Some(err) = resp.header.as_ref().and_then(|h| h.error.as_ref()) {
        if err.r#type == pdpb::ErrorType::RegionNotFound as i32 {
            return Ok((true, false));
        }
        let ty = pdpb::ErrorType::try_from(err.r#type)
            .map_or_else(|_| err.r#type.to_string(), |t| t.as_str_name().to_owned());
        return Err(errors::annotatef(
            &errors::ERR_PD_INVALID_RESPONSE,
            format!("get operator error: {ty}, error message: {}", err.message),
        ));
    }
    if resp.desc != b"scatter-region" {
        return Ok((true, false));
    }
    match pdpb::OperatorStatus::try_from(resp.status) {
        Ok(pdpb::OperatorStatus::Success) => Ok((true, false)),
        Ok(pdpb::OperatorStatus::Running) => Ok((false, false)),
        _ => Ok((false, true)),
    }
}

fn info_fields(region: &metapb::Region) -> Field {
    Field::new("region", Value::Str(format!("{{id: {}}}", region.id)))
}

impl PdSplitClient {
    fn http(&self) -> Result<&pd_http::Client> {
        self.http_cli
            .as_ref()
            .ok_or_else(|| errors::new(&errors::ERR_KV_NOT_TIKV))
    }

    fn block_on<T>(
        &self,
        f: impl std::future::Future<Output = tikv_client::Result<T>>,
    ) -> Result<T> {
        self.runtime
            .block_on(f)
            .map_err(crate::pdclient::from_client_error)
    }

    /// Go `needScatter`: checked once; an error scatters.
    pub(crate) fn need_scatter(&self, ctx: &Context) -> bool {
        *self.need_scatter_val.get_or_init(|| match self.check_need_scatter(ctx) {
            Ok(v) => {
                if !v {
                    tidb_log::info("skipping scatter because the replica number isn't less than store count.", &[]);
                }
                v
            }
            Err(err) => {
                tidb_log::warn(
                    "failed to check whether need to scatter, use permissive strategy: always scatter",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                true
            }
        })
    }

    /// Go `checkNeedScatter`.
    fn check_need_scatter(&self, ctx: &Context) -> Result<bool> {
        let store_count = crate::conn::get_all_tikv_stores(
            ctx,
            self.client.as_ref(),
            crate::conn::StoreBehavior::SkipTiFlash,
        )?
        .len();
        let http = self.http()?.clone();
        let resp = self.block_on(async move { http.get_replicate_config().await })?;
        let max_replica = resp
            .get("max-replicas")
            .and_then(serde_json::Value::as_f64)
            .ok_or_else(|| {
                errors::errorf(format!("key max-replicas not found in response {resp:?}"))
            })? as usize;
        tidb_log::info(
            "checking whether need to scatter",
            &[
                Field::new("store", Value::I64(store_count as i64)),
                Field::new("max-replica", Value::I64(max_replica as i64)),
            ],
        );
        Ok(store_count >= max_replica && store_count > 1)
    }

    /// Go `scatterRegions`: batch scatter with retries, falling back to one
    /// region at a time; regions PD keeps failing are left.
    pub(crate) fn scatter_regions(&self, ctx: &Context, new_regions: &[RegionInfo]) -> Result<()> {
        tidb_log::info(
            "scatter regions",
            &[Field::new("regions", Value::I64(new_regions.len() as i64))],
        );
        let mut regions = new_regions.to_vec();
        let res = with_retry(
            ctx,
            || match self.try_scatter_regions(ctx, &regions) {
                Err(err)
                    if is_unsupported_error(&err)
                        || errors::is(err.as_ref(), &errors::ERR_PD_REGIONS_NOT_FULLY_SCATTER) =>
                {
                    tidb_log::warn(
                        "failed to batch scatter regions, rollback to sequentially scatter",
                        &[Field::new("error", Value::Str(err.to_string()))],
                    );
                    self.scatter_regions_sequentially(
                        ctx,
                        &regions,
                        &mut new_backoff_retry_all_error_strategy(
                            1800,
                            Duration::from_millis(100),
                            Duration::from_secs(2),
                        ),
                    );
                    Ok(())
                }
                Err(err) => Err(err),
                Ok(failed) if !failed.is_empty() => {
                    regions.retain(|r| failed.contains(&r.region.id));
                    Err(errors::annotatef(
                            &errors::ERR_PD_NOT_FULLY_SCATTER,
                            format!("pd returns error during batch scattering: {} regions failed to scatter", failed.len()),
                        ))
                }
                Ok(_) => Ok(()),
            },
            &mut new_backoff_retry_all_error_strategy(
                1800,
                Duration::from_millis(500),
                Duration::from_secs(2),
            ),
        );
        match res {
            Err(err) if errors::is(err.as_ref(), &errors::ERR_PD_NOT_FULLY_SCATTER) => {
                tidb_log::warn(
                    "some regions haven't been scattered",
                    &[Field::new("error", Value::Str(err.to_string()))],
                );
                Ok(())
            }
            other => other,
        }
    }

    /// Go `tryScatterRegions`: the regions PD failed to scatter.
    fn try_scatter_regions(&self, ctx: &Context, regions: &[RegionInfo]) -> Result<HashSet<u64>> {
        let ids: Vec<u64> = regions.iter().map(|r| r.region.id).collect();
        let resp = self.client.scatter_regions(ctx, &ids)?;
        if let Some(err) = resp.header.as_ref().and_then(|h| h.error.as_ref()) {
            if err.r#type != pdpb::ErrorType::Ok as i32 {
                return Err(errors::annotatef(
                    &errors::ERR_PD_INVALID_RESPONSE,
                    format!("pd returns error during batch scattering: {err:?}"),
                ));
            }
        }
        if !resp.failed_regions_id.is_empty() {
            return Ok(resp.failed_regions_id.into_iter().collect());
        }
        if resp.finished_percentage < 100 {
            return Err(errors::annotatef(
                &errors::ERR_PD_REGIONS_NOT_FULLY_SCATTER,
                format!(
                    "scatter finished percentage {} less than 100",
                    resp.finished_percentage
                ),
            ));
        }
        Ok(HashSet::new())
    }

    /// Go `scatterRegion`.
    fn scatter_region(&self, ctx: &Context, region: &RegionInfo) -> Result<()> {
        if !self.need_scatter(ctx) {
            return Ok(());
        }
        self.client.scatter_region(ctx, region.region.id)
    }

    /// Go `scatterRegionsSequentially`.
    pub(crate) fn scatter_regions_sequentially(
        &self,
        ctx: &Context,
        new_regions: &[RegionInfo],
        bo: &mut dyn BackoffStrategy,
    ) {
        let mut set: HashMap<u64, RegionInfo> = new_regions
            .iter()
            .map(|r| (r.region.id, r.clone()))
            .collect();
        let res = with_retry(
            ctx,
            || {
                tidb_log::info(
                    "trying to scatter regions...",
                    &[Field::new("remain", Value::I64(set.len() as i64))],
                );
                let mut errs: Option<Error> = None;
                let regions: Vec<RegionInfo> = set.values().cloned().collect();
                for region in regions {
                    match self.scatter_region(ctx, &region) {
                        Ok(()) => {
                            set.remove(&region.region.id);
                        }
                        Err(err) => {
                            if !pd_error_can_retry(&err) {
                                tidb_log::warn(
                                    "scatter meet error cannot be retried, skipping",
                                    &[
                                        Field::new("error", Value::Str(err.to_string())),
                                        info_fields(&region.region),
                                    ],
                                );
                                set.remove(&region.region.id);
                            }
                            errs = Some(multierr_append(errs, err));
                        }
                    }
                }
                errs.map_or(Ok(()), Err)
            },
            bo,
        );
        if let Err(err) = res {
            let mut ids: Vec<String> = set.keys().map(ToString::to_string).collect();
            ids.sort();
            tidb_log::warn(
                "Some regions haven't been scattered because errors.",
                &[
                    Field::new("count", Value::I64(set.len() as i64)),
                    Field::new("error", Value::Str(err.to_string())),
                    Field::new("failed-regions", Value::Str(ids.join(","))),
                ],
            );
        }
    }

    /// Go `sendSplitRegionRequest`: the split RPC to the region's leader,
    /// retried on a new leader or a busy store.
    ///
    /// A leader named by a not-leader error is written into `region`, as
    /// Go's write through the shared `*RegionInfo`.
    fn send_split_region_request(
        &self,
        ctx: &Context,
        region: &mut RegionInfo,
        keys: &[Vec<u8>],
    ) -> Result<kvrpcpb::SplitRegionResponse> {
        let mut split_errors: Option<Error> = None;
        for retry in 0..SPLIT_REGION_MAX_RETRY_TIME {
            let (again, result) =
                self.send_split_region_request_once(ctx, region, keys, &mut split_errors, retry);
            if again {
                continue;
            }
            return match result {
                Err(err) => Err(match split_errors {
                    Some(all) => multierr_append(Some(all), err),
                    None => err,
                }),
                Ok(Some(resp)) => Ok(resp),
                Ok(None) => {
                    Err(split_errors.unwrap_or_else(|| errors::errorf("split region failed")))
                }
            };
        }
        Err(split_errors.unwrap_or_else(|| errors::errorf("split region failed")))
    }

    /// Go's package-level `sendSplitRegionRequest`: retry, response or
    /// error of one attempt.
    fn send_split_region_request_once(
        &self,
        ctx: &Context,
        region: &mut RegionInfo,
        keys: &[Vec<u8>],
        split_errors: &mut Option<Error>,
        retry: usize,
    ) -> (bool, Result<Option<kvrpcpb::SplitRegionResponse>>) {
        #[cfg(test)]
        if let Some(mock) = &self.mock_split {
            return mock(region, keys);
        }
        let peer = match &region.leader {
            Some(l) if l.id != 0 => *l,
            _ => match region.region.peers.first() {
                Some(p) => *p,
                None => {
                    return (
                        false,
                        Err(errors::annotatef(
                            &errors::ERR_RESTORE_NO_PEER,
                            format!("region[{}] doesn't have any peer", region.region.id),
                        )),
                    )
                }
            },
        };
        let store = match self.get_store(ctx, peer.store_id) {
            Ok(s) => s,
            Err(e) => return (false, Err(e)),
        };
        let resp = match self.split_region_with_failpoint(region, &peer, &store.address, keys) {
            Ok(r) => r,
            Err(e) => return (false, Err(e)),
        };
        let Some(region_err) = &resp.region_error else {
            return (false, Ok(Some(resp)));
        };
        tidb_log::warn(
            "fail to split region",
            &[
                info_fields(&region.region),
                Field::new("regionErr", Value::Str(format!("{region_err:?}"))),
            ],
        );
        *split_errors = Some(multierr_append(
            split_errors.take(),
            errors::annotatef(
                &errors::ERR_RESTORE_SPLIT_FAILED,
                format!("split region failed: err={region_err:?}"),
            ),
        ));
        if let Some(nl) = &region_err.not_leader {
            let logged = if let Some(leader) = &nl.leader {
                region.leader = Some(*leader);
                region.clone()
            } else {
                let new_info = match self.get_region_by_id(ctx, nl.region_id) {
                    Ok(Some(info)) => info,
                    Ok(None) => return (false, Err(errors::new(&errors::ERR_KV_EPOCH_NOT_MATCH))),
                    Err(e) => return (false, Err(e)),
                };
                if !check_region_epoch(&new_info, region) {
                    return (false, Err(errors::new(&errors::ERR_KV_EPOCH_NOT_MATCH)));
                }
                let Some(leader) = &new_info.leader else {
                    return (false, Err(errors::new(&errors::ERR_PD_LEADER_NOT_FOUND)));
                };
                tidb_log::info(
                    "find new leader",
                    &[Field::new("new leader", Value::U64(leader.id))],
                );
                // Go rebinds its local `regionInfo` here: the next attempt
                // still sends to the caller's region.
                new_info
            };
            tidb_log::info(
                "split region meet not leader error, retrying",
                &[
                    Field::new("retry times", Value::I64(retry as i64)),
                    Field::new("regionID", Value::U64(logged.region.id)),
                    Field::new("new leader", Value::Str(format!("{:?}", logged.leader))),
                ],
            );
            return (true, Ok(None));
        }
        if region_err.server_is_busy.is_some() || region_err.stale_command.is_some() {
            tidb_log::warn(
                "a error occurs on split region",
                &[
                    Field::new("retry times", Value::I64(retry as i64)),
                    Field::new("regionID", Value::U64(region.region.id)),
                    Field::new("error", Value::Str(region_err.message.clone())),
                ],
            );
            return (true, Ok(None));
        }
        (false, Ok(None))
    }

    /// Go `splitRegionWithFailpoint`: TiKV's `SplitRegion` on `peer`'s store.
    fn split_region_with_failpoint(
        &self,
        region: &RegionInfo,
        peer: &metapb::Peer,
        address: &str,
        keys: &[Vec<u8>],
    ) -> Result<kvrpcpb::SplitRegionResponse> {
        if let Some(v) =
            tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/restore/split/not-leader-error")
        {
            tidb_log::debug("failpoint not-leader-error injected.", &[]);
            return Ok(kvrpcpb::SplitRegionResponse {
                region_error: Some(Box::new(tidb_proto::errorpb::Error {
                    not_leader: Some(tidb_proto::errorpb::NotLeader {
                        region_id: region.region.id,
                        leader: if v.bool() { region.leader } else { None },
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        if tidb_failpoint::eval(
            "github.com/pingcap/tidb/br/pkg/restore/split/somewhat-retryable-error",
        )
        .is_some()
        {
            tidb_log::debug("failpoint somewhat-retryable-error injected.", &[]);
            return Ok(kvrpcpb::SplitRegionResponse {
                region_error: Some(Box::new(tidb_proto::errorpb::Error {
                    server_is_busy: Some(tidb_proto::errorpb::ServerIsBusy::default()),
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        let req = kvrpcpb::SplitRegionRequest {
            context: Some(kvrpcpb::Context {
                region_id: region.region.id,
                region_epoch: region.region.region_epoch,
                peer: Some(*peer),
                ..Default::default()
            }),
            split_keys: keys.to_vec(),
            is_raw_kv: false,
            ..Default::default()
        };
        let endpoint = tidb_pd_client::secure_endpoint(address, &self.security)
            .map_err(|e| errors::errorf(e.to_string()))?
            // Go lightning `config.DefaultGrpcKeepaliveParams`.
            .http2_keep_alive_interval(Duration::from_secs(60))
            .keep_alive_timeout(Duration::from_secs(120));
        self.runtime
            .block_on(async move {
                let channel = endpoint
                    .connect()
                    .await
                    .map_err(|e| errors::errorf(e.to_string()))?;
                let mut cli = tikv_client::proto::tikvpb::tikv_client::TikvClient::new(channel);
                cli.split_region(req).await.map_err(errors::grpc)
            })
            .map(tonic::Response::into_inner)
    }

    /// Go `batchSplitRegionsWithOrigin`: the region after the split and the
    /// new regions, each with its peer on the old leader's store.
    fn batch_split_regions_with_origin(
        &self,
        ctx: &Context,
        region: &mut RegionInfo,
        keys: &[Vec<u8>],
    ) -> Result<(Option<RegionInfo>, Vec<RegionInfo>)> {
        let resp = self.send_split_region_request(ctx, region, keys)?;
        let mut origin = None;
        let mut new_regions = Vec::with_capacity(resp.regions.len());
        for r in resp.regions {
            let leader = region
                .leader
                .as_ref()
                .and_then(|l| r.peers.iter().find(|p| p.store_id == l.store_id).cloned());
            let info = RegionInfo {
                region: r,
                leader,
                ..Default::default()
            };
            if info.region.id == region.region.id {
                origin = Some(info);
            } else {
                new_regions.push(info);
            }
        }
        Ok((origin, new_regions))
    }

    /// Go `waitRegionsSplit`: until PD reports each new region without
    /// pending peers.
    fn wait_regions_split(&self, ctx: &Context, new_regions: &[RegionInfo]) -> Result<()> {
        let mut regions = new_regions.to_vec();
        with_retry_return_last_err(
            ctx,
            || {
                let mut need_recheck = Vec::new();
                for r in &regions {
                    match self.has_healthy_region(ctx, r.region.id) {
                        Ok(true) => {}
                        res => {
                            if let Err(err) = res {
                                tidb_log::warn(
                                    "wait for split failed",
                                    &[
                                        Field::new("regionID", Value::U64(r.region.id)),
                                        Field::new("error", Value::Str(err.to_string())),
                                    ],
                                );
                            }
                            need_recheck.push(r.clone());
                        }
                    }
                }
                if need_recheck.is_empty() {
                    return Ok(());
                }
                let backoff_err = if need_recheck.len() < regions.len() {
                    &*ERR_BACKOFF_AND_DONT_COUNT
                } else {
                    &*ERR_BACKOFF
                };
                let first = need_recheck[0].region.id;
                let n = need_recheck.len();
                regions = need_recheck;
                Err(errors::annotate(
                    Arc::clone(backoff_err),
                    format!("WaitRegionsSplit not finished, needRecheck: {n}, the first unfinished region: {{id:{first}}}"),
                ))
            },
            &mut BackoffMayNotCountBackoffer::new(),
        )
    }

    /// Go `hasHealthyRegion`.
    fn has_healthy_region(&self, ctx: &Context, region_id: u64) -> Result<bool> {
        let Some(info) = self.get_region_by_id(ctx, region_id)? else {
            return Ok(false);
        };
        Ok(info.pending_peers.is_empty())
    }

    /// Go `getEncodedKeys`.
    fn get_encoded_keys(start: &[u8], end: &[u8]) -> (Vec<u8>, Vec<u8>) {
        let enc = |k: &[u8]| {
            let mut b = Vec::new();
            tidb_codec::encode_bytes(&mut b, k);
            b
        };
        (enc(start), enc(end))
    }

    /// Go `splitKeys`: the keys grouped by the regions they fall in, each
    /// region split (and scattered) by a worker, the keys of failed regions
    /// retried.
    fn split_keys_impl(
        &self,
        ctx: &Context,
        sorted_split_keys: &[Vec<u8>],
        scatter: bool,
    ) -> Result<Vec<RegionInfo>> {
        if sorted_split_keys.is_empty() {
            return Ok(Vec::new());
        }
        let key_next = |k: &[u8]| {
            let mut n = k.to_vec();
            n.push(0);
            n
        };
        let last = sorted_split_keys.last().unwrap();
        let last_key = if last.is_empty() {
            Vec::new()
        } else {
            key_next(last)
        };
        let (mut scan_start, mut scan_end) =
            Self::get_encoded_keys(&sorted_split_keys[0], &last_key);
        let ret: Mutex<Vec<RegionInfo>> = Mutex::new(Vec::new());
        let retry_split_keys: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
        let last_split_err: Mutex<Option<Error>> = Mutex::new(None);
        let res = with_retry_return_last_err(
            ctx,
            || {
                ret.lock().unwrap().clear();
                let retry_keys: Vec<Vec<u8>> =
                    std::mem::take(&mut *retry_split_keys.lock().unwrap());
                if !retry_keys.is_empty() {
                    (scan_start, scan_end) = Self::get_encoded_keys(
                        &retry_keys[0],
                        &key_next(retry_keys.last().unwrap()),
                    );
                }
                let regions = paginate_scan_region(
                    ctx,
                    self,
                    &scan_start,
                    &scan_end,
                    SCAN_REGION_PAGINATION_LIMIT,
                )?;
                tidb_log::info(
                    "paginate scan regions",
                    &[Field::new("count", Value::I64(regions.len() as i64))],
                );
                let all_split_keys: &[Vec<u8>] = if retry_keys.is_empty() {
                    sorted_split_keys
                } else {
                    &retry_keys
                };
                let split_key_map = get_split_keys_of_regions(all_split_keys, &regions);
                let queue = Mutex::new(split_key_map);
                let (ectx, ecancel) = Context::with_cancel(ctx);
                let first: Mutex<Option<Error>> = Mutex::new(None);
                std::thread::scope(|scope| {
                    for _ in 0..self.split_concurrency.max(1) {
                        scope.spawn(|| loop {
                            let Some((region, keys)) = queue.lock().unwrap().pop() else {
                                return;
                            };
                            if ectx.err().is_some() {
                                return;
                            }
                            match self.split_wait_and_maybe_scatter(&ectx, &region, &keys, scatter)
                            {
                                Err(err) => {
                                    if crate::errors::is_context_canceled(err.as_ref()) {
                                        first.lock().unwrap().get_or_insert(err);
                                        ecancel.cancel();
                                        return;
                                    }
                                    tidb_log::warn(
                                        "split and scatter region meet error, will retry",
                                        &[
                                            Field::new("region_id", Value::U64(region.region.id)),
                                            Field::new("error", Value::Str(err.to_string())),
                                        ],
                                    );
                                    retry_split_keys.lock().unwrap().extend(keys);
                                    *last_split_err.lock().unwrap() = Some(err);
                                }
                                Ok(new_regions) => {
                                    if scatter && new_regions.len() != keys.len() {
                                        tidb_log::warn(
                                            "split key count and new region count mismatch",
                                            &[
                                                Field::new(
                                                    "new region count",
                                                    Value::I64(new_regions.len() as i64),
                                                ),
                                                Field::new(
                                                    "split key count",
                                                    Value::I64(keys.len() as i64),
                                                ),
                                            ],
                                        );
                                    }
                                    if scatter {
                                        ret.lock().unwrap().extend(new_regions);
                                    }
                                }
                            }
                        });
                    }
                });
                if let Some(err) = first.into_inner().unwrap() {
                    return Err(err);
                }
                let mut retry_keys = retry_split_keys.lock().unwrap();
                if retry_keys.is_empty() {
                    return Ok(());
                }
                retry_keys.sort();
                Err(last_split_err
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| errors::errorf("split failed")))
            },
            &mut new_backoff_retry_all_except_strategy(
                SPLIT_RETRY_TIMES.load(std::sync::atomic::Ordering::Relaxed),
                SPLIT_RETRY_INTERVAL,
                SPLIT_MAX_RETRY_INTERVAL,
                Arc::new(|e: &(dyn std::error::Error + 'static)| {
                    errors::is(e, &errors::ERR_INVALID_RANGE)
                }),
            ),
        );
        res.map(|()| ret.into_inner().unwrap())
    }

    /// Go `splitWaitAndMaybeScatter`: the keys split in batches of at most
    /// `maxBatchSplitSize` bytes and `splitBatchKeyCnt` keys, each batch
    /// waited for and maybe scattered.
    fn split_wait_and_maybe_scatter(
        &self,
        ctx: &Context,
        region: &RegionInfo,
        keys: &[Vec<u8>],
        scatter: bool,
    ) -> Result<Vec<RegionInfo>> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/restore/split/failToSplit")
            .is_some()
        {
            return Err(errors::errorf("retryable error"));
        }
        if keys.is_empty() {
            return Ok(vec![region.clone()]);
        }
        let max_batch = MAX_BATCH_SPLIT_SIZE.load(std::sync::atomic::Ordering::Relaxed);
        let mut region = region.clone();
        let (mut start, mut end, mut batch_size) = (0, 0, 0);
        let mut new_regions = Vec::with_capacity(keys.len());
        while end <= keys.len() {
            if end == keys.len()
                || batch_size + keys[end].len() > max_batch
                || end - start >= self.split_batch_key_cnt
            {
                let (origin, batch) =
                    self.batch_split_regions_with_origin(ctx, &mut region, &keys[start..end])?;
                if let Err(err) = self.wait_regions_split(ctx, &batch) {
                    tidb_log::warn(
                        "wait regions split failed, will continue anyway",
                        &[Field::new("error", Value::Str(err.to_string()))],
                    );
                }
                if let Some(err) = ctx.err() {
                    return Err(Arc::new(err));
                }
                if scatter {
                    if let Err(err) = self.scatter_regions(ctx, &batch) {
                        tidb_log::warn(
                            "scatter regions failed, will continue anyway",
                            &[Field::new("error", Value::Str(err.to_string()))],
                        );
                    }
                }
                if let Some(f) = &self.on_split {
                    f(&keys[start..end]);
                }
                let origin = origin
                    .ok_or_else(|| errors::errorf("the split response has no origin region"))?;
                let last_new = batch
                    .last()
                    .cloned()
                    .ok_or_else(|| errors::errorf("the split response has no new region"))?;
                region = if origin.region.start_key < last_new.region.start_key {
                    last_new
                } else {
                    origin
                };
                new_regions.extend(batch);
                batch_size = 0;
                start = end;
            }
            if end < keys.len() {
                batch_size += keys[end].len();
            }
            end += 1;
        }
        ctx.err().map_or(Ok(new_regions), |e| Err(Arc::new(e)))
    }
}

impl SplitClient for PdSplitClient {
    fn get_store(&self, ctx: &Context, store_id: u64) -> Result<metapb::Store> {
        let mut cache = self.store_cache.lock().unwrap();
        if let Some(s) = cache.get(&store_id) {
            return Ok(s.clone());
        }
        let store = self.client.get_store(ctx, store_id)?;
        cache.insert(store_id, store.clone());
        Ok(store)
    }

    fn get_region(&self, ctx: &Context, key: &[u8]) -> Result<Option<RegionInfo>> {
        Ok(self.client.get_region(ctx, key)?.map(|r| RegionInfo {
            region: r.meta,
            leader: r.leader,
            ..Default::default()
        }))
    }

    fn get_region_by_id(&self, ctx: &Context, region_id: u64) -> Result<Option<RegionInfo>> {
        Ok(self
            .client
            .get_region_by_id(ctx, region_id)?
            .map(RegionInfo::from))
    }

    fn split_keys_and_scatter(
        &self,
        ctx: &Context,
        sorted_split_keys: &[Vec<u8>],
    ) -> Result<Vec<RegionInfo>> {
        self.split_keys_impl(ctx, sorted_split_keys, true)
    }

    fn split_keys(&self, ctx: &Context, sorted_split_keys: &[Vec<u8>]) -> Result<Vec<RegionInfo>> {
        self.split_keys_impl(ctx, sorted_split_keys, false)
    }

    fn split_wait_and_scatter(
        &self,
        ctx: &Context,
        region: &RegionInfo,
        keys: &[Vec<u8>],
    ) -> Result<Vec<RegionInfo>> {
        self.split_wait_and_maybe_scatter(ctx, region, keys, true)
    }

    fn get_operator(&self, ctx: &Context, region_id: u64) -> Result<pdpb::GetOperatorResponse> {
        self.client.get_operator(ctx, region_id)
    }

    fn scan_regions(
        &self,
        ctx: &Context,
        key: &[u8],
        end_key: &[u8],
        limit: usize,
    ) -> Result<Vec<RegionInfo>> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/restore/split/no-leader-error")
            .is_some()
        {
            tidb_log::debug("failpoint no-leader-error injected.", &[]);
            return Err(errors::grpc(tonic::Status::unavailable("not leader")));
        }
        Ok(self
            .client
            .scan_regions(ctx, key, end_key, limit)?
            .into_iter()
            .map(|r| RegionInfo {
                region: r.meta,
                leader: r.leader,
                ..Default::default()
            })
            .collect())
    }

    fn get_placement_rule(
        &self,
        _: &Context,
        group_id: &str,
        rule_id: &str,
    ) -> Result<pd_http::Rule> {
        let http = self.http()?.clone();
        let (g, r) = (group_id.to_owned(), rule_id.to_owned());
        self.block_on(async move { http.get_placement_rule(&g, &r).await })
    }

    fn set_placement_rule(&self, _: &Context, rule: &pd_http::Rule) -> Result<()> {
        let http = self.http()?.clone();
        let rule = rule.clone();
        self.block_on(async move { http.set_placement_rule(&rule).await })
    }

    fn delete_placement_rule(&self, _: &Context, group_id: &str, rule_id: &str) -> Result<()> {
        let http = self.http()?.clone();
        let (g, r) = (group_id.to_owned(), rule_id.to_owned());
        self.block_on(async move { http.delete_placement_rule(&g, &r).await })
    }

    fn set_stores_label(
        &self,
        _: &Context,
        stores: &[u64],
        label_key: &str,
        label_value: &str,
    ) -> Result<()> {
        let http = self.http()?.clone();
        for &id in stores {
            let http = http.clone();
            let labels: std::collections::HashMap<String, String> =
                [(label_key.to_owned(), label_value.to_owned())].into();
            self.block_on(async move { http.set_store_labels(id as i64, &labels).await })?;
        }
        Ok(())
    }

    fn wait_regions_scattered(&self, ctx: &Context, regions: &[RegionInfo]) -> (usize, Result<()>) {
        let mut regions = regions.to_vec();
        let mut retry_cnt: i64 = -1;
        let mut need_recheck: Vec<RegionInfo> = Vec::new();
        let res = with_retry_return_last_err(
            ctx,
            || {
                retry_cnt += 1;
                let mut logged = false;
                need_recheck.clear();
                let mut need_rescatter = Vec::new();
                for (i, region) in regions.iter().enumerate() {
                    let region_id = region.region.id;
                    if retry_cnt > 10 && !logged {
                        logged = true;
                        let resp = self.get_operator(ctx, region_id);
                        tidb_log::info(
                            "retried many times to wait for scattering regions, checking operator",
                            &[
                                Field::new("retryCnt", Value::I64(retry_cnt)),
                                Field::new("firstRegionID", Value::U64(region_id)),
                                Field::new("response", Value::Str(format!("{resp:?}"))),
                            ],
                        );
                    }
                    let finished = match self.get_operator(ctx, region_id) {
                        Err(err) if is_retryable_error(&err) => Ok((false, false)),
                        Err(err) => Err(err),
                        Ok(resp) => is_scatter_region_finished(&resp),
                    };
                    match finished {
                        Err(err) => {
                            if !is_retryable_error(&err) {
                                tidb_log::warn(
                                    "wait for scatter region encountered non-retryable error",
                                    &[
                                        info_fields(&region.region),
                                        Field::new("error", Value::Str(err.to_string())),
                                    ],
                                );
                                need_recheck.extend(regions[i..].iter().cloned());
                                return Err(err);
                            }
                            tidb_log::warn(
                                "wait for scatter region encountered error, will retry again",
                                &[
                                    info_fields(&region.region),
                                    Field::new("error", Value::Str(err.to_string())),
                                ],
                            );
                            need_recheck.push(region.clone());
                        }
                        Ok((true, _)) => {}
                        Ok((false, rescatter)) => {
                            need_recheck.push(region.clone());
                            if rescatter {
                                need_rescatter.push(region.clone());
                            }
                        }
                    }
                }
                if need_recheck.is_empty() {
                    return Ok(());
                }
                let backoff_err = if need_recheck.len() < regions.len() {
                    &*ERR_BACKOFF_AND_DONT_COUNT
                } else {
                    &*ERR_BACKOFF
                };
                regions = need_recheck.clone();
                if !need_rescatter.is_empty() {
                    if let Err(scatter_err) = self.scatter_regions(ctx, &need_rescatter) {
                        if !is_retryable_error(&scatter_err) {
                            return Err(scatter_err);
                        }
                        return Err(errors::annotate(
                            Arc::clone(backoff_err),
                            scatter_err.to_string(),
                        ));
                    }
                }
                Err(errors::annotate(
                    Arc::clone(backoff_err),
                    format!(
                        "scatter region not finished, retryCnt: {retry_cnt}, needRecheck: {}, needRescatter: {}, the first unfinished region: {{id:{}}}",
                        need_recheck.len(),
                        need_rescatter.len(),
                        need_recheck[0].region.id
                    ),
                ))
            },
            &mut BackoffMayNotCountBackoffer::new(),
        );
        (need_recheck.len(), res)
    }
}
