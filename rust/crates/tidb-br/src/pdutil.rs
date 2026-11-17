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

//! Go `br/pkg/pdutil` (`pd.go`, `utils.go`): pausing PD's schedulers and
//! schedule limits while a backup or restore runs, and the PD reads BR
//! makes over PD's HTTP API.
//!
//! Go's `NewPdController` dials its own PD client; inside TiDB this one is
//! built over the node's PD client and PD HTTP client (the same cluster).
//! `RecoverBaseAllocID`, `ResetTS` and the recovering marks (EBS snapshot
//! restore), `SetFollowerHandle` (log restore), and `GetPlacementRules` /
//! `SearchPlacementRule` (no caller) are not reachable from SQL and are not
//! ported. [`pause_schedulers_by_key_range`] (IMPORT INTO's ingest) and
//! [`PdController::remove_schedulers_on_region`] share Go's one
//! `pauseSchedulerByKeyRangeWithTTL`, over the PD HTTP calls it makes
//! ([`LabelRuleClient`]).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value as Json;
use tidb_go_semver::Version;
use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tikv_client::pd_http;

use crate::errors::{self, Result, ERR_PD_UPDATE_FAILED};
use crate::pdclient::PdClient;

/// Go `pauseTimeout`.
pub const PAUSE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
/// Go `PDRequestRetryTime`.
pub const PD_REQUEST_RETRY_TIME: i64 = 120;
/// Go `maxPendingPeerUnlimited`.
const MAX_PENDING_PEER_UNLIMITED: u64 = i32::MAX as u64;

/// Go `pauseConfigVersion`.
static PAUSE_CONFIG_VERSION: Version = Version { major: 4, minor: 0, patch: 8, pre_release: String::new(), metadata: String::new() };
/// Go `minVersionForRegionLabelTTL`.
static MIN_VERSION_FOR_REGION_LABEL_TTL: Version =
    Version { major: 6, minor: 1, patch: 0, pre_release: String::new(), metadata: String::new() };

/// Go `Schedulers`: the schedulers BR pauses.
pub const SCHEDULERS: &[&str] = &[
    "balance-leader-scheduler",
    "balance-hot-region-scheduler",
    "balance-region-scheduler",
    "shuffle-leader-scheduler",
    "shuffle-region-scheduler",
    "shuffle-hot-region-scheduler",
    "evict-slow-store-scheduler",
];

/// Go `pauseConfigGenerator`: the paused value of a schedule config, from
/// the store count and the config's current value.
pub type PauseConfigGenerator = Arc<dyn Fn(usize, &Json) -> Json + Send + Sync>;

/// A Go `float64` as Go's `encoding/json` writes it: whole values without a
/// fraction.
fn go_float(f: f64) -> Json {
    if f.fract() == 0.0 && f.abs() < 1e15 {
        Json::from(f as i64)
    } else {
        Json::from(f)
    }
}

/// Go `zeroPauseConfig`.
fn zero_pause_config() -> PauseConfigGenerator {
    Arc::new(|_, _| Json::from(0))
}

/// Go `pauseConfigMulStores`: `min(40, value * stores)`.
fn pause_config_mul_stores() -> PauseConfigGenerator {
    Arc::new(|stores, raw| go_float(f64::min(40.0, raw.as_f64().unwrap_or(0.0) * stores as f64)))
}

/// Go `pauseConfigFalse`.
fn pause_config_false() -> PauseConfigGenerator {
    Arc::new(|_, _| Json::from("false"))
}

/// Go `constConfigGeneratorBuilder`.
fn const_config_generator(val: Json) -> PauseConfigGenerator {
    Arc::new(move |_, _| val.clone())
}

/// Go `expectPDCfgGenerators` (`DefaultExpectPDCfgGenerators`).
#[must_use]
pub fn default_expect_pd_cfg_generators() -> BTreeMap<String, PauseConfigGenerator> {
    BTreeMap::from([
        ("merge-schedule-limit".to_owned(), zero_pause_config()),
        ("leader-schedule-limit".to_owned(), pause_config_mul_stores()),
        ("region-schedule-limit".to_owned(), pause_config_mul_stores()),
        ("max-snapshot-count".to_owned(), pause_config_mul_stores()),
        ("enable-location-replacement".to_owned(), pause_config_false()),
        ("max-pending-peer-count".to_owned(), const_config_generator(Json::from(MAX_PENDING_PEER_UNLIMITED))),
    ])
}

/// Go `defaultPDCfg`.
fn default_pd_cfg() -> BTreeMap<String, Json> {
    BTreeMap::from([
        ("merge-schedule-limit".to_owned(), Json::from(8)),
        ("leader-schedule-limit".to_owned(), Json::from(4)),
        ("region-schedule-limit".to_owned(), Json::from(2048)),
        ("enable-location-replacement".to_owned(), Json::from("true")),
    ])
}

/// Go `ClusterConfig`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ClusterConfig {
    /// Go `Schedulers`.
    #[serde(rename = "schedulers")]
    pub schedulers: Option<Vec<String>>,
    /// Go `ScheduleCfg`.
    #[serde(rename = "schedule_cfg")]
    pub schedule_cfg: Option<BTreeMap<String, Json>>,
    /// Go `RuleID`.
    #[serde(rename = "rule_id")]
    pub rule_id: String,
}

/// Go `UndoFunc`.
pub type UndoFunc = Box<dyn FnOnce(&Context) -> Result<()> + Send>;

/// Go `Nop`.
pub fn nop() -> UndoFunc {
    Box::new(|_| Ok(()))
}

/// Go `PdController`.
pub struct PdController {
    pd_client: Arc<dyn PdClient>,
    pd_http: pd_http::Client,
    version: Version,
    runtime: Arc<tokio::runtime::Runtime>,
    scheduler_pause: Mutex<Option<crossbeam_channel::Sender<()>>>,
    /// Go `SchedulerPauseTTL`; zero is [`PAUSE_TIMEOUT`].
    pub scheduler_pause_ttl: Duration,
}

fn pd_err(err: tikv_client::Error) -> errors::Error {
    crate::pdclient::from_client_error(err)
}

/// Go `parseVersion`: v0.0.0 for an unparsable version.
#[must_use]
pub fn parse_version(version_str: &str) -> Version {
    let v = version_str.trim().trim_matches('"');
    let v = v.strip_prefix('v').unwrap_or(v);
    let mut version = tidb_go_semver::new_version(v).unwrap_or_else(|err| {
        tidb_log::warn(
            "fail back to v0.0.0 version",
            &[Field::new("version", Value::Str(version_str.into())), Field::new("error", Value::Str(err))],
        );
        Version::default()
    });
    if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/pdutil/PDEnabledPauseConfig").is_some_and(|v| v.bool()) {
        version = Version { major: 5, ..Version::default() };
    }
    version
}

impl PdController {
    /// Go `NewPdController` over the node's PD clients: reads PD's version
    /// first. Go's PD HTTP client retries every request for up to
    /// [`PD_REQUEST_RETRY_TIME`] seconds.
    ///
    /// # Errors
    /// PD's version cannot be read.
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        pd_http: pd_http::Client,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Result<Self> {
        let second = 1_000_000_000;
        let pd_http = pd_http.with_backoffer(tikv_client::pd_backoff::Backoffer::new(
            second,
            second,
            PD_REQUEST_RETRY_TIME * second,
        ));
        let http = pd_http.clone();
        let version_str = runtime.block_on(async move { http.get_pd_version().await }).map_err(pd_err)?;
        Ok(Self::with_pd_client(pd_client, pd_http, parse_version(&version_str), runtime))
    }

    /// Go `NewPdControllerWithPDClient`.
    #[must_use]
    pub fn with_pd_client(
        pd_client: Arc<dyn PdClient>,
        pd_http: pd_http::Client,
        version: Version,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self { pd_client, pd_http, version, runtime, scheduler_pause: Mutex::new(None), scheduler_pause_ttl: Duration::ZERO }
    }

    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.runtime.block_on(f)
    }

    /// Go `isPauseConfigEnabled`.
    fn is_pause_config_enabled(&self) -> bool {
        self.version.compare(&PAUSE_CONFIG_VERSION) >= 0
    }

    /// Go `GetPDClient`.
    #[must_use]
    pub fn get_pd_client(&self) -> &Arc<dyn PdClient> {
        &self.pd_client
    }

    /// Go `GetPDHTTPClient`.
    #[must_use]
    pub fn get_pd_http_client(&self) -> &pd_http::Client {
        &self.pd_http
    }

    /// Go `GetClusterVersion`.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_cluster_version(&self, _: &Context) -> Result<String> {
        self.block_on(self.pd_http.get_cluster_version()).map_err(pd_err)
    }

    /// Go `GetRegionCount`: the regions in `[start_key, end_key)`; an empty
    /// end key is the end of the key space.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_region_count(&self, _: &Context, start_key: &[u8], end_key: &[u8]) -> Result<i64> {
        let mut start = Vec::new();
        tidb_codec::encode_bytes(&mut start, start_key);
        let mut end = Vec::new();
        if !end_key.is_empty() {
            tidb_codec::encode_bytes(&mut end, end_key);
        }
        let range = pd_http::new_key_range(start, end);
        let status = self.block_on(self.pd_http.get_region_status_by_key_range(&range, true)).map_err(pd_err)?;
        Ok(status.count)
    }

    /// Go `GetStoreInfo`.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_store_info(&self, _: &Context, store_id: u64) -> Result<pd_http::StoreInfo> {
        self.block_on(self.pd_http.get_store(store_id)).map_err(pd_err)
    }

    /// Go `ttlOfPausing`.
    fn ttl_of_pausing(&self) -> Duration {
        if self.scheduler_pause_ttl > Duration::ZERO {
            self.scheduler_pause_ttl
        } else {
            PAUSE_TIMEOUT
        }
    }

    /// Go `doPauseSchedulers`: the schedulers paused before the first
    /// failure.
    fn do_pause_schedulers(&self, schedulers: &[String]) -> (Vec<String>, Option<errors::Error>) {
        let delay = self.ttl_of_pausing().as_secs() as i64;
        let mut removed = Vec::with_capacity(schedulers.len());
        for scheduler in schedulers {
            if let Err(err) = self.block_on(self.pd_http.set_scheduler_delay(scheduler, delay)) {
                return (removed, Some(pd_err(err)));
            }
            removed.push(scheduler.clone());
        }
        (removed, None)
    }

    /// Go `pauseSchedulersAndConfigWith`: pauses the schedulers and configs,
    /// then keeps them paused every third of the pause TTL until `ctx` is
    /// done or [`Self::resume_schedulers`] runs.
    fn pause_schedulers_and_config_with(
        self: &Arc<Self>,
        ctx: &Context,
        schedulers: Vec<String>,
        scheduler_cfg: Option<BTreeMap<String, Json>>,
    ) -> Result<Vec<String>> {
        let (removed, err) = self.do_pause_schedulers(&schedulers);
        if let Some(err) = err {
            tidb_log::error(
                "failed to pause scheduler at beginning",
                &[Field::new("name", Value::Str(format!("{schedulers:?}"))), Field::new("error", Value::Str(err.to_string()))],
            );
            return Err(err);
        }
        tidb_log::info("pause scheduler successful at beginning", &[Field::new("name", Value::Str(format!("{schedulers:?}")))]);
        if let Some(cfg) = &scheduler_cfg {
            if let Err(err) = self.do_pause_configs(cfg) {
                tidb_log::error(
                    "failed to pause config at beginning",
                    &[Field::new("cfg", Value::Str(json_text(cfg))), Field::new("error", Value::Str(err.to_string()))],
                );
                return Err(err);
            }
            tidb_log::info("pause configs successful at beginning", &[Field::new("cfg", Value::Str(json_text(cfg)))]);
        }
        let (tx, rx) = crossbeam_channel::bounded(1);
        *self.scheduler_pause.lock().unwrap() = Some(tx);
        let this = Arc::clone(self);
        let ctx = ctx.clone();
        let removed_names = removed.clone();
        std::thread::Builder::new()
            .name("br-pause-schedulers".into())
            .spawn(move || {
                let tick = crossbeam_channel::tick(this.ttl_of_pausing() / 3);
                loop {
                    crossbeam_channel::select! {
                        recv(ctx.done()) -> _ => return,
                        recv(tick) -> _ => {
                            if let (_, Some(err)) = this.do_pause_schedulers(&schedulers) {
                                tidb_log::warn("pause scheduler failed, ignore it and wait next time pause", &[Field::new("error", Value::Str(err.to_string()))]);
                            }
                            if let Some(cfg) = &scheduler_cfg {
                                if let Err(err) = this.do_pause_configs(cfg) {
                                    tidb_log::warn("pause configs failed, ignore it and wait next time pause", &[Field::new("error", Value::Str(err.to_string()))]);
                                }
                            }
                            tidb_log::info(
                                "pause scheduler(configs)",
                                &[
                                    Field::new("name", Value::Str(format!("{removed_names:?}"))),
                                    Field::new("cfg", Value::Str(scheduler_cfg.as_ref().map(json_text).unwrap_or_else(|| "null".into()))),
                                ],
                            );
                        }
                        recv(rx) -> _ => {
                            tidb_log::info("exit pause scheduler and configs successful", &[]);
                            return;
                        }
                    }
                }
            })
            .map_err(|e| errors::errorf(e.to_string()))?;
        Ok(removed)
    }

    /// Go `ResumeSchedulers`.
    ///
    /// # Errors
    /// None today: a scheduler PD does not resume is logged.
    pub fn resume_schedulers(&self, _: &Context, schedulers: &[String]) -> Result<()> {
        self.resume_scheduler_with(schedulers)
    }

    /// Go `ResumeRegionLabelRule`: deletes the label rule BR set.
    pub fn resume_region_label_rule(&self, _: &Context, rule_id: &str) {
        if rule_id.is_empty() {
            return;
        }
        let rules = self.block_on(self.pd_http.get_region_label_rules_by_ids(&[rule_id.to_owned()]));
        let Ok(rules) = rules.map_err(|_| ()).and_then(|r| if r.is_empty() { Err(()) } else { Ok(r) }) else {
            tidb_log::warn(
                "failed to get the region label rule, the rule may have been removed",
                &[Field::new("rule-id", Value::Str(rule_id.into()))],
            );
            return;
        };
        let patch = pd_http::LabelRulePatch { set_rules: Vec::new(), delete_rules: vec![rule_id.to_owned()] };
        if let Err(err) = self.block_on(self.pd_http.patch_region_label_rules(&patch)) {
            tidb_log::warn(
                "failed to delete region label rule, the rule will be removed after ttl expires",
                &[Field::new("rule-id", Value::Str(rules[0].id.clone())), Field::new("error", Value::Str(err.to_string()))],
            );
        }
    }

    /// Go `resumeSchedulerWith`.
    fn resume_scheduler_with(&self, schedulers: &[String]) -> Result<()> {
        if schedulers.is_empty() {
            return Ok(());
        }
        tidb_log::info("resume scheduler", &[Field::new("schedulers", Value::Str(format!("{schedulers:?}")))]);
        if let Some(tx) = self.scheduler_pause.lock().unwrap().as_ref() {
            let _ = tx.try_send(());
        }
        for scheduler in schedulers {
            match self.block_on(self.pd_http.set_scheduler_delay(scheduler, 0)) {
                Err(_) => tidb_log::error(
                    "failed to resume scheduler after retry, you may reset this scheduler manuallyor just wait this scheduler pause timeout",
                    &[Field::new("scheduler", Value::Str(scheduler.clone()))],
                ),
                Ok(()) => tidb_log::info("resume scheduler successful", &[Field::new("scheduler", Value::Str(scheduler.clone()))]),
            }
        }
        Ok(())
    }

    /// Go `ListSchedulers`.
    ///
    /// # Errors
    /// PD's error.
    pub fn list_schedulers(&self, _: &Context) -> Result<Vec<String>> {
        self.block_on(self.pd_http.get_schedulers()).map_err(pd_err)
    }

    /// Go `GetPDScheduleConfig`.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_pd_schedule_config(&self, _: &Context) -> Result<BTreeMap<String, Json>> {
        self.block_on(self.pd_http.get_schedule_config()).map(|m| m.into_iter().collect()).map_err(pd_err)
    }

    /// Go `UpdatePDScheduleConfig`: PD's default schedule limits.
    ///
    /// # Errors
    /// [`ERR_PD_UPDATE_FAILED`].
    pub fn update_pd_schedule_config(&self, _: &Context) -> Result<()> {
        let cfg = default_pd_cfg();
        tidb_log::info("update pd with default config", &[Field::new("cfg", Value::Str(json_text(&cfg)))]);
        self.do_update_pd_schedule_config(&cfg, None)
    }

    /// Go `doUpdatePDScheduleConfig`: `schedule.<key>` for every key.
    fn do_update_pd_schedule_config(&self, cfg: &BTreeMap<String, Json>, ttl_seconds: Option<f64>) -> Result<()> {
        let new_cfg: std::collections::HashMap<String, Json> =
            cfg.iter().map(|(k, v)| (format!("schedule.{k}"), v.clone())).collect();
        self.block_on(self.pd_http.set_config(&new_cfg, ttl_seconds)).map_err(|err| {
            errors::annotatef(&ERR_PD_UPDATE_FAILED, format!("failed to update PD schedule config: {err}"))
        })
    }

    /// Go `doPauseConfigs`.
    fn do_pause_configs(&self, cfg: &BTreeMap<String, Json>) -> Result<()> {
        self.do_update_pd_schedule_config(cfg, Some(self.ttl_of_pausing().as_secs_f64()))
    }

    /// Go `MakeUndoFunctionByConfig`.
    pub fn make_undo_function_by_config(self: &Arc<Self>, config: ClusterConfig) -> UndoFunc {
        self.gen_restore_scheduler_func(config, default_expect_pd_cfg_generators())
    }

    /// Go `MakeFineGrainedUndoFunction`.
    pub fn make_fine_grained_undo_function(
        self: &Arc<Self>,
        config: ClusterConfig,
        undo: Box<dyn FnOnce() + Send>,
    ) -> UndoFunc {
        let this = Arc::clone(self);
        Box::new(move |ctx| {
            undo();
            restore_schedulers(ctx, &this, &config, &default_expect_pd_cfg_generators())
        })
    }

    /// Go `GenRestoreSchedulerFunc`.
    pub fn gen_restore_scheduler_func(
        self: &Arc<Self>,
        config: ClusterConfig,
        configs_need_restore: BTreeMap<String, PauseConfigGenerator>,
    ) -> UndoFunc {
        let this = Arc::clone(self);
        Box::new(move |ctx| restore_schedulers(ctx, &this, &config, &configs_need_restore))
    }

    /// Go `RemoveSchedulers`.
    ///
    /// # Errors
    /// PD's error.
    pub fn remove_schedulers(self: &Arc<Self>, ctx: &Context) -> (UndoFunc, Result<()>) {
        match self.remove_schedulers_with_origin(ctx) {
            Err(err) => (nop(), Err(err)),
            Ok((origin, _)) => (
                self.make_undo_function_by_config(ClusterConfig {
                    schedulers: origin.schedulers,
                    schedule_cfg: origin.schedule_cfg,
                    rule_id: String::new(),
                }),
                Ok(()),
            ),
        }
    }

    /// Go `RemoveSchedulersWithConfig`.
    ///
    /// # Errors
    /// PD's error.
    pub fn remove_schedulers_with_config(self: &Arc<Self>, ctx: &Context) -> (UndoFunc, Option<ClusterConfig>, Result<()>) {
        match self.remove_schedulers_with_origin(ctx) {
            Err(err) => (nop(), None, Err(err)),
            Ok((origin, _)) => {
                let undo = self.make_undo_function_by_config(ClusterConfig {
                    schedulers: origin.schedulers.clone(),
                    schedule_cfg: origin.schedule_cfg.clone(),
                    rule_id: String::new(),
                });
                (undo, Some(origin), Ok(()))
            }
        }
    }

    /// Go `RemoveAllPDSchedulers`: also zeroes every schedule limit and
    /// stops TiKV's own region splits.
    ///
    /// # Errors
    /// PD's error.
    pub fn remove_all_pd_schedulers(self: &Arc<Self>, ctx: &Context) -> (UndoFunc, Result<()>) {
        const ENABLE_TIKV_SPLIT_REGION: &str = "enable-tikv-split-region";
        let mut generators = default_expect_pd_cfg_generators();
        for param in [
            "hot-region-schedule-limit",
            "leader-schedule-limit",
            "merge-schedule-limit",
            "region-schedule-limit",
            "replica-schedule-limit",
            ENABLE_TIKV_SPLIT_REGION,
        ] {
            let g: PauseConfigGenerator = if param == ENABLE_TIKV_SPLIT_REGION {
                Arc::new(|_, _| Json::Bool(false))
            } else {
                Arc::new(|_, _| Json::from(0))
            };
            generators.insert(param.to_owned(), g);
        }
        match self.remove_schedulers_with_config_generator(ctx, &generators) {
            Err(err) => (nop(), Err(err)),
            Ok((old, _)) => (self.gen_restore_scheduler_func(old, generators), Ok(())),
        }
    }

    /// Go `RemoveSchedulersWithOrigin`.
    ///
    /// # Errors
    /// PD's error.
    pub fn remove_schedulers_with_origin(self: &Arc<Self>, ctx: &Context) -> Result<(ClusterConfig, ClusterConfig)> {
        self.remove_schedulers_with_config_generator(ctx, &default_expect_pd_cfg_generators())
    }

    /// Go `RemoveSchedulersWithConfigGenerator`: the original and the paused
    /// schedulers and configs.
    ///
    /// # Errors
    /// PD's error.
    pub fn remove_schedulers_with_config_generator(
        self: &Arc<Self>,
        ctx: &Context,
        generators: &BTreeMap<String, PauseConfigGenerator>,
    ) -> Result<(ClusterConfig, ClusterConfig)> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/br/pkg/pdutil/removeSchedulersError").is_some() {
            return Err(errors::errorf("injected scheduler removal failure"));
        }
        let mut origin = ClusterConfig::default();
        let mut removed = ClusterConfig::default();
        let stores = self.pd_client.get_all_stores(ctx, false)?;
        let schedule_cfg = self.get_pd_schedule_config(ctx)?;
        let mut disable = BTreeMap::new();
        let mut original = BTreeMap::new();
        for (key, gen) in generators {
            let Some(value) = schedule_cfg.get(key) else { continue };
            disable.insert(key.clone(), gen(stores.len(), value));
            original.insert(key.clone(), value.clone());
        }
        origin.schedule_cfg = Some(original);
        removed.schedule_cfg = Some(disable.clone());
        tidb_log::debug("saved PD config", &[Field::new("config", Value::Str(json_text(&schedule_cfg)))]);
        let exist = self.list_schedulers(ctx)?;
        let need: Vec<String> = exist.into_iter().filter(|s| SCHEDULERS.contains(&s.as_str())).collect();
        let removed_schedulers = self.do_remove_schedulers_with(ctx, need, Some(disable))?;
        origin.schedulers = Some(removed_schedulers.clone());
        removed.schedulers = Some(removed_schedulers);
        Ok((origin, removed))
    }

    /// Go `GetOriginPDConfig`.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_origin_pd_config(&self, ctx: &Context) -> Result<ClusterConfig> {
        let cfg = self.get_pd_schedule_config(ctx)?;
        tidb_log::debug("saved PD config", &[Field::new("config", Value::Str(json_text(&cfg)))]);
        Ok(ClusterConfig { schedule_cfg: Some(cfg), ..ClusterConfig::default() })
    }

    /// Go `RemoveSchedulersOnRegion`: pauses PD's schedulers on the key
    /// ranges; answers the label rule's ID and a function that stops the
    /// pause and waits for the rule's deletion.
    ///
    /// # Errors
    /// PD refused the first label rule.
    pub fn remove_schedulers_on_region(
        &self,
        ctx: &Context,
        key_ranges: &[(Vec<u8>, Vec<u8>)],
    ) -> (String, Box<dyn FnOnce() + Send>, Result<()>) {
        tidb_log::info("removing scheduler on region", &[]);
        let (scheduler_ctx, cancel) = Context::with_cancel(ctx);
        let res = pause_scheduler_by_key_range_with_ttl(
            &scheduler_ctx,
            Arc::new(self.pd_http.clone()),
            key_ranges,
            PAUSE_TIMEOUT,
        )
        .map_err(errors::errorf);
        std::thread::sleep(Duration::from_millis(20));
        match res {
            Ok((done, rule_id)) => (
                rule_id,
                Box::new(move || {
                    let Some(done) = done else { return };
                    cancel.cancel();
                    let _ = done.join();
                    tidb_log::info("scheduler pause cancelled and cleaned up", &[]);
                }),
                Ok(()),
            ),
            Err(err) => (String::new(), Box::new(|| {}), Err(err)),
        }
    }

    /// Go `RemoveSchedulersWithCfg`.
    ///
    /// # Errors
    /// PD's error.
    pub fn remove_schedulers_with_cfg(self: &Arc<Self>, ctx: &Context, cfg: ClusterConfig) -> Result<()> {
        self.do_remove_schedulers_with(ctx, cfg.schedulers.unwrap_or_default(), cfg.schedule_cfg).map(|_| ())
    }

    /// Go `doRemoveSchedulersWith`.
    fn do_remove_schedulers_with(
        self: &Arc<Self>,
        ctx: &Context,
        need: Vec<String>,
        disable: Option<BTreeMap<String, Json>>,
    ) -> Result<Vec<String>> {
        if !self.is_pause_config_enabled() {
            return Err(errors::errorf(format!(
                "pd version {} not support pause config, please upgrade",
                self.version
            )));
        }
        self.pause_schedulers_and_config_with(ctx, need, disable)
    }

    /// Go `GetMinResolvedTS`.
    ///
    /// # Errors
    /// PD's error.
    pub fn get_min_resolved_ts(&self, _: &Context) -> Result<u64> {
        self.block_on(self.pd_http.get_min_resolved_ts_by_stores_ids(&[])).map(|(ts, _)| ts).map_err(pd_err)
    }

    /// Go `ResetTS`: PD's TSO moved to at least `ts`, past the default
    /// 24-hour bound (`force-use-larger`). A `Forbidden` answer means PD's
    /// TSO is already larger and is ignored.
    ///
    /// # Errors
    /// PD's error.
    pub fn reset_ts(&self, _: &Context, ts: u64) -> Result<()> {
        match self.block_on(self.pd_http.reset_ts(ts, true)) {
            Ok(()) => Ok(()),
            Err(err) if err.to_string().contains("Forbidden") => {
                tidb_log::info("reset-ts returns with status forbidden, ignore", &[]);
                Ok(())
            }
            Err(err) => Err(pd_err(err)),
        }
    }

    /// Go `CanPauseSchedulerByKeyRange`.
    #[must_use]
    pub fn can_pause_scheduler_by_key_range(&self) -> bool {
        self.version.compare(&MIN_VERSION_FOR_REGION_LABEL_TTL) >= 0
    }

    /// Go `Close`: stops the pause loop. The PD clients are the node's.
    pub fn close(&self) {
        self.scheduler_pause.lock().unwrap().take();
    }
}

/// `%v`-like text of a config map for logs.
fn json_text(m: &BTreeMap<String, Json>) -> String {
    serde_json::to_string(m).unwrap_or_default()
}

/// Go `restoreSchedulers`: resumes the schedulers, deletes the label rule
/// and puts the paused configs back.
fn restore_schedulers(
    ctx: &Context,
    pd: &PdController,
    cfg: &ClusterConfig,
    configs_need_restore: &BTreeMap<String, PauseConfigGenerator>,
) -> Result<()> {
    pd.resume_schedulers(ctx, cfg.schedulers.as_deref().unwrap_or_default())
        .map_err(|e| errors::annotate(e, "fail to add PD schedulers"))?;
    pd.resume_region_label_rule(ctx, &cfg.rule_id);
    let schedule = cfg.schedule_cfg.clone().unwrap_or_default();
    tidb_log::info("restoring config", &[Field::new("config", Value::Str(json_text(&schedule)))]);
    let merge: BTreeMap<String, Json> = configs_need_restore
        .keys()
        .filter_map(|k| schedule.get(k).filter(|v| !v.is_null()).map(|v| (k.clone(), v.clone())))
        .collect();
    let ttl = pd.is_pause_config_enabled().then_some(0.0);
    pd.do_update_pd_schedule_config(&merge, ttl).map_err(|e| errors::annotate(e, "fail to update PD merge config"))
}

/// Go `KeyRangeRule`.
#[derive(serde::Serialize)]
struct KeyRangeRule {
    start_key: String,
    end_key: String,
}

/// The calls Go's `pauseSchedulerByKeyRangeWithTTL` makes on its
/// `pdhttp.Client`.
pub trait LabelRuleClient: Send + Sync {
    /// Go `pdhttp.Client.SetRegionLabelRule`.
    ///
    /// # Errors
    /// PD's error.
    fn set_region_label_rule(&self, rule: &pd_http::LabelRule) -> std::result::Result<(), String>;
    /// Go `pdhttp.Client.PatchRegionLabelRules` under a ctx that times out
    /// after `timeout`, when one is given.
    ///
    /// # Errors
    /// PD's error, or the timeout.
    fn patch_region_label_rules(
        &self,
        patch: &pd_http::LabelRulePatch,
        timeout: Option<Duration>,
    ) -> std::result::Result<(), String>;
}

impl LabelRuleClient for pd_http::Client {
    fn set_region_label_rule(&self, rule: &pd_http::LabelRule) -> std::result::Result<(), String> {
        tidb_store_driver_txn::cluster_http::block_on_pd_http(pd_http::Client::set_region_label_rule(self, rule))
    }

    fn patch_region_label_rules(
        &self,
        patch: &pd_http::LabelRulePatch,
        timeout: Option<Duration>,
    ) -> std::result::Result<(), String> {
        let patch_rules = pd_http::Client::patch_region_label_rules(self, patch);
        let Some(timeout) = timeout else {
            return tidb_store_driver_txn::cluster_http::block_on_pd_http(patch_rules);
        };
        tidb_store_driver_txn::cluster_http::block_on_pd_http(async move {
            tokio::time::timeout(timeout, patch_rules)
                .await
                .unwrap_or_else(|_| Err(tikv_client::Error::StringError("context deadline exceeded".to_owned())))
        })
    }
}

/// Go `PauseSchedulersByKeyRange`'s `done` channel together with the
/// caller's `cancel`: dropping it ends the pause and waits for the rule's
/// deletion (Go `cancel(); <-done`).
pub struct PausedSchedulers {
    cancel: Option<tidb_gocontext::CancelFunc>,
    done: Option<std::thread::JoinHandle<()>>,
}

impl Drop for PausedSchedulers {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            cancel.cancel();
        }
        if let Some(done) = self.done.take() {
            let _ = done.join();
        }
    }
}

/// Go `PauseSchedulersByKeyRange`: pauses the schedulers of the regions in
/// the key range until the returned handle drops.
///
/// # Errors
/// PD refused the label rule.
pub fn pause_schedulers_by_key_range(
    pd_http_cli: Arc<dyn LabelRuleClient>,
    start_key: &[u8],
    end_key: &[u8],
) -> std::result::Result<PausedSchedulers, String> {
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    let paused = pause_scheduler_by_key_range_with_ttl(
        &ctx,
        pd_http_cli,
        &[(start_key.to_vec(), end_key.to_vec())],
        PAUSE_TIMEOUT,
    );
    // Wait for the rule to take effect because the PD operator is processed
    // asynchronously (pingcap/tidb#49477): two times the default
    // `patrol-region-interval`.
    std::thread::sleep(Duration::from_millis(20));
    let (done, _) = paused?;
    Ok(PausedSchedulers { cancel: Some(cancel), done })
}

/// Go `pauseSchedulerByKeyRangeWithTTL`: a `schedule=deny` label rule over
/// the ranges, refreshed every third of `ttl` until `ctx` is done and then
/// deleted. No rule without a range.
fn pause_scheduler_by_key_range_with_ttl(
    ctx: &Context,
    pd_http_cli: Arc<dyn LabelRuleClient>,
    key_ranges: &[(Vec<u8>, Vec<u8>)],
    ttl: Duration,
) -> std::result::Result<(Option<std::thread::JoinHandle<()>>, String), String> {
    if key_ranges.is_empty() || key_ranges[0].0.is_empty() {
        return Ok((None, String::new()));
    }
    let data: Vec<KeyRangeRule> =
        key_ranges.iter().map(|(s, e)| KeyRangeRule { start_key: hex::encode(s), end_key: hex::encode(e) }).collect();
    let rule = pd_http::LabelRule {
        id: uuid::Uuid::new_v4().to_string(),
        index: 0,
        labels: vec![pd_http::RegionLabel {
            key: "schedule".into(),
            value: "deny".into(),
            ttl: tidb_model::go_duration::format_go_duration(ttl.as_nanos() as i64),
            start_at: String::new(),
        }],
        rule_type: "key-range".into(),
        data: serde_json::to_value(data).unwrap_or_default(),
    };
    pd_http_cli.set_region_label_rule(&rule)?;
    let id = rule.id.clone();
    let ctx = ctx.clone();
    let handle = std::thread::Builder::new()
        .name("br-pause-key-range".into())
        .spawn(move || {
            while !ctx.sleep(ttl / 3) {
                if let Err(err) = pd_http_cli.set_region_label_rule(&rule) {
                    tidb_log::warn(
                        "pause scheduler by key range failed, ignore it and wait next time pause",
                        &[Field::new("error", Value::Str(err))],
                    );
                }
            }
            let patch = pd_http::LabelRulePatch { set_rules: Vec::new(), delete_rules: vec![rule.id.clone()] };
            if pd_http_cli.patch_region_label_rules(&patch, Some(Duration::from_secs(5))).is_err() {
                tidb_log::warn(
                    "failed to delete region label rule, the rule will be removed after ttl expires",
                    &[
                        Field::new("rule-id", Value::Str(rule.id.clone())),
                        Field::new("ttl", Value::Duration(ttl.as_nanos() as i64)),
                    ],
                );
            }
        })
        .map_err(|e| e.to_string())?;
    Ok((Some(handle), id))
}

#[cfg(test)]
mod tests;
