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

//! Go `pkg/ttl/ttlworker/timer_sync.go`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tidb_ast::CiString;
use tidb_logutil::bg_logger;
use tidb_metrics::server::TIDB_SERVER_TTL_EVENT_COUNT;
use tidb_model::{GoShared, PartitionDefinition, TableInfo, TTLInfo};
use tidb_timer::client::{
    with_key_prefix, with_set_enable, with_set_sched_expr, with_set_tags, TimerClient,
};
use tidb_timer::error::TimerError;
use tidb_timer::go_time::{GoTime, SECOND};
use tidb_timer::timer::{SchedPolicyType, TimerRecord, TimerSpec};
use tidb_ttl::cache::table::PhysicalTable;
use tidb_ttl::cache::ttlstatus::{row_to_table_status, select_from_ttl_table_status_with_id, TableStatus};
use tidb_ttl_client::context::Context;

use crate::session::{with_session, SessionPool};
use crate::{field, mysql_time_to_go, str_arg, InfoSchema, Result, Session};

/// Go `timerKeyPrefix`.
pub const TIMER_KEY_PREFIX: &str = "/tidb/ttl/physical_table/";
/// Go `timerHookClass`.
pub const TIMER_HOOK_CLASS: &str = "tidb.ttl";
/// Go `fullRefreshTimersCacheInterval`.
pub const FULL_REFRESH_TIMERS_CACHE_INTERVAL: Duration = Duration::from_secs(10 * 60);
/// Go `timerDelayDeleteInterval`.
pub const TIMER_DELAY_DELETE_INTERVAL: Duration = Duration::from_secs(10 * 60);

/// Go `TTLTimerData`: the data stored in each TTL timer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlTimerData {
    /// Go `TableID`.
    #[serde(default)]
    pub table_id: i64,
    /// Go `PhysicalID`.
    #[serde(default)]
    pub physical_id: i64,
}

fn timer_ctx() -> tidb_timer::store::Context {
    tidb_timer::store::Context::background()
}

/// Go `metrics.TTLSyncTimerCounter.Inc()`.
fn inc_sync_timer_counter() {
    TIDB_SERVER_TTL_EVENT_COUNT.with_label_values(&["sync_one_timer"]).inc();
}

/// The job a manually triggered timer ran, once known: Go's returned
/// `func() (string, bool, error)`.
pub type ManualTriggerWait = Box<dyn FnMut() -> std::result::Result<(String, bool), String> + Send>;

/// Go `TTLTimersSyncer`: keeps one timer per TTL physical table.
pub struct TtlTimersSyncer {
    pool: Arc<dyn SessionPool>,
    cli: Arc<dyn TimerClient>,
    key2_timers: HashMap<String, TimerRecord>,
    last_pull_timers: GoTime,
    delay_delete: Duration,
    last_sync_time: GoTime,
    last_sync_ver: i64,
    now_func: fn() -> GoTime,
}

/// Go `NewTTLTimerSyncer`.
pub fn new_ttl_timer_syncer(pool: Arc<dyn SessionPool>, cli: Arc<dyn TimerClient>) -> TtlTimersSyncer {
    TtlTimersSyncer {
        pool,
        cli,
        key2_timers: HashMap::new(),
        last_pull_timers: GoTime::zero(),
        delay_delete: TIMER_DELAY_DELETE_INTERVAL,
        last_sync_time: GoTime::zero(),
        last_sync_ver: 0,
        now_func: GoTime::now,
    }
}

fn since(time: &GoTime) -> Duration {
    let elapsed = GoTime::now().sub(time);
    Duration::from_nanos(elapsed.max(0) as u64)
}

impl TtlTimersSyncer {
    /// Go `SetDelayDeleteInterval`: it's better not to delete a timer at once
    /// when its table is gone, as the info schema syncs asynchronously and a
    /// new table may not have reached this node yet.
    pub fn set_delay_delete_interval(&mut self, interval: Duration) {
        self.delay_delete = interval;
    }

    /// Go `ManualTriggerTTLTimer`: triggers a job for a physical table and
    /// returns a function answering the job id once it has started.
    pub fn manual_trigger_ttl_timer(&mut self, ctx: &Context, tbl: &PhysicalTable) -> Result<ManualTriggerWait> {
        let mut timer_id = String::new();
        let mut req_id = String::new();
        let pool = Arc::clone(&self.pool);
        with_session(pool.as_ref(), &mut |se| {
            let timer = self.sync_one_timer(
                ctx,
                se,
                &tbl.schema,
                &tbl.table_info,
                tbl.partition_def.as_ref(),
                true,
            )?;
            timer_id = timer.id.clone();
            req_id = self
                .cli
                .manual_trigger_event(&timer_ctx(), &timer.id)
                .map_err(|err| err.to_string())?;
            Ok(())
        })?;

        let ctx = ctx.clone();
        let cli = Arc::clone(&self.cli);
        Ok(Box::new(move || {
            if let Some(err) = ctx.err() {
                return Err(err.to_string());
            }

            let timer = cli.get_timer_by_id(&timer_ctx(), &timer_id).map_err(|err| err.to_string())?;

            if timer.manual_request.manual_request_id != req_id {
                return Err("manual request failed to trigger, request not found".to_owned());
            }

            if timer.is_manual_requesting() {
                let timeout = timer.manual_request.manual_timeout;
                if timeout > 0
                    && GoTime::now().sub(&timer.manual_request.manual_request_time) > timeout + 5 * SECOND
                {
                    return Err("manual request timeout".to_owned());
                }
                return Ok((String::new(), false));
            }

            if timer.manual_request.manual_event_id.is_empty() {
                return Err("manual request failed to trigger, request cancelled".to_owned());
            }

            let job_id = timer.manual_request.manual_event_id.clone();
            let mut found = false;
            with_session(pool.as_ref(), &mut |se| {
                let rows = se
                    .execute_sql(&ctx, "select 1 from mysql.tidb_ttl_job_history where job_id=%?", &[str_arg(&job_id)])
                    .map_err(|err| err.to_string())?;
                found = !rows.is_empty();
                Ok(())
            })?;

            if !found {
                return Ok((String::new(), false));
            }

            Ok((job_id, true))
        }))
    }

    /// Go `Reset`.
    pub fn reset(&mut self) {
        self.last_pull_timers = GoTime::zero();
        self.last_sync_time = GoTime::zero();
        self.last_sync_ver = 0;
        self.key2_timers.clear();
    }

    /// Go `GetLastSyncInfo`.
    pub fn get_last_sync_info(&self) -> (GoTime, i64) {
        (self.last_sync_time.clone(), self.last_sync_ver)
    }

    /// Go `GetCachedTimerRecord`.
    pub fn get_cached_timer_record(&self, key: &str) -> Option<&TimerRecord> {
        self.key2_timers.get(key)
    }

    /// Go `SyncTimers`: one timer per TTL physical table, disabling and
    /// later deleting the timers of tables that went away.
    pub fn sync_timers(&mut self, ctx: &Context, is: &dyn InfoSchema) {
        self.last_sync_time = (self.now_func)();
        self.last_sync_ver = is.schema_meta_version();
        if since(&self.last_pull_timers) > FULL_REFRESH_TIMERS_CACHE_INTERVAL {
            TIDB_SERVER_TTL_EVENT_COUNT.with_label_values(&["full_refresh_timers"]).inc();
            let timers = match self.cli.get_timers(&timer_ctx(), &[with_key_prefix(TIMER_KEY_PREFIX)]) {
                Ok(timers) => timers,
                Err(err) => {
                    bg_logger().warn("failed to pull timers", &[field("error", err.to_string())]);
                    return;
                }
            };
            self.key2_timers = timers.into_iter().map(|timer| (timer.spec.key.clone(), timer)).collect();
            self.last_pull_timers = (self.now_func)();
        }

        let mut current_timer_keys = HashSet::new();
        let pool = Arc::clone(&self.pool);
        let result = with_session(pool.as_ref(), &mut |se| {
            for (db_name, tables) in is.list_ttl_tables() {
                for tbl_info in tables {
                    for key in self.sync_timers_for_table(ctx, se, &db_name, &tbl_info) {
                        current_timer_keys.insert(key);
                    }
                }
            }
            Ok(())
        });

        if let Err(err) = result {
            bg_logger().error("failed to sync TTL timers", &[field("error", err)]);
            return;
        }

        let keys: Vec<String> = self.key2_timers.keys().cloned().collect();
        for key in keys {
            if current_timer_keys.contains(&key) {
                continue;
            }

            let timer = self.key2_timers[&key].clone();
            let timer_id = timer.id.clone();
            if since(&timer.create_time) > self.delay_delete {
                inc_sync_timer_counter();
                match self.cli.delete_timer(&timer_ctx(), &timer_id) {
                    Err(err) => bg_logger().error(
                        "failed to delete timer",
                        &[field("error", err.to_string()), field("timerID", timer_id.clone())],
                    ),
                    Ok(_) => {
                        self.key2_timers.remove(&key);
                    }
                }
            } else if timer.spec.enable {
                inc_sync_timer_counter();
                if let Err(err) = self.cli.update_timer(&timer_ctx(), &timer_id, &[with_set_enable(false)]) {
                    bg_logger().error(
                        "failed to disable timer",
                        &[field("error", err.to_string()), field("timerID", timer_id.clone())],
                    );
                }

                match self.cli.get_timer_by_id(&timer_ctx(), &timer_id) {
                    Err(TimerError::TimerNotExist) => {
                        self.key2_timers.remove(&key);
                    }
                    Err(err) => bg_logger().error(
                        "failed to get timer",
                        &[field("error", err.to_string()), field("timerID", timer_id.clone())],
                    ),
                    Ok(timer) => {
                        self.key2_timers.insert(key, timer);
                    }
                }
            }
        }
    }

    /// Go `syncTimersForTable`.
    fn sync_timers_for_table(&mut self, ctx: &Context, se: &Session, schema: &CiString, tbl_info: &GoShared<TableInfo>) -> Vec<String> {
        let definitions = tbl_info.read().partition.as_ref().map(|partition| partition.read().definitions.snapshot());
        let Some(defs) = definitions else {
            let key = build_timer_key(&tbl_info.read(), None);
            if let Err(err) = self.sync_one_timer(ctx, se, schema, tbl_info, None, false) {
                bg_logger().error("failed to syncOneTimer", &[field("error", err), field("key", key.clone())]);
            }
            return vec![key];
        };

        let mut keys = Vec::with_capacity(defs.len());
        for partition in &defs {
            let key = build_timer_key(&tbl_info.read(), Some(partition));
            keys.push(key.clone());
            if let Err(err) = self.sync_one_timer(ctx, se, schema, tbl_info, Some(partition), false) {
                bg_logger().error("failed to syncOneTimer", &[field("error", err), field("key", key)]);
            }
        }
        keys
    }

    /// Go `shouldSyncTimer`.
    fn should_sync_timer(
        &self,
        timer: Option<&TimerRecord>,
        schema: &CiString,
        tbl_info: &TableInfo,
        partition: Option<&PartitionDefinition>,
    ) -> bool {
        let Some(timer) = timer else {
            return true;
        };

        let tags = get_timer_tags(schema, tbl_info, partition);
        let ttl_info = ttl_info_of(tbl_info);
        let (policy_type, policy_expr) = get_ttl_schedule_policy(&ttl_info);
        timer.spec.tags != tags
            || timer.spec.enable != ttl_info.enable
            || timer.spec.sched_policy_type != policy_type
            || timer.spec.sched_policy_expr != policy_expr
    }

    /// Go `syncOneTimer`.
    fn sync_one_timer(
        &mut self,
        ctx: &Context,
        se: &Session,
        schema: &CiString,
        tbl_info: &GoShared<TableInfo>,
        partition: Option<&PartitionDefinition>,
        skip_cache: bool,
    ) -> Result<TimerRecord> {
        let info = tbl_info.read().clone();
        let key = build_timer_key(&info, partition);
        let tags = get_timer_tags(schema, &info, partition);
        let ttl_info = ttl_info_of(&info);
        if !skip_cache {
            let cached = self.key2_timers.get(&key);
            if cached.is_some() && !self.should_sync_timer(cached, schema, &info, partition) {
                return Ok(cached.cloned().expect("checked above"));
            }
        }

        inc_sync_timer_counter();
        let timer = match self.cli.get_timer_by_key(&timer_ctx(), &key) {
            Ok(timer) => timer,
            Err(TimerError::TimerNotExist) => {
                self.key2_timers.remove(&key);
                let mut watermark = GoTime::zero();
                match get_ttl_table_status(ctx, se, &info, partition) {
                    Err(err) => bg_logger().warn(
                        "failed to get TTL table status",
                        &[field("error", err), field("key", key.clone())],
                    ),
                    Ok(Some(ttl_table_status)) => {
                        let location = se.get_session_vars().location();
                        let at = |time: &Option<tidb_datatype::Time>| {
                            time.map_or_else(GoTime::zero, |time| mysql_time_to_go(time, &location))
                        };
                        watermark = if !ttl_table_status.current_job_id.is_empty() {
                            at(&ttl_table_status.current_job_start_time)
                        } else {
                            at(&ttl_table_status.last_job_start_time)
                        };
                    }
                    Ok(None) => {}
                }

                let data_obj = TtlTimerData {
                    table_id: info.id,
                    physical_id: partition.map_or(info.id, |partition| partition.id),
                };
                let data = serde_json::to_vec(&data_obj).map_err(|err| err.to_string())?;
                let (policy_type, policy_expr) = get_ttl_schedule_policy(&ttl_info);
                let timer = self
                    .cli
                    .create_timer(
                        &timer_ctx(),
                        TimerSpec {
                            key: key.clone(),
                            tags,
                            data,
                            sched_policy_type: policy_type,
                            sched_policy_expr: policy_expr,
                            hook_class: TIMER_HOOK_CLASS.to_owned(),
                            watermark,
                            enable: ttl_info.enable,
                            ..TimerSpec::default()
                        },
                    )
                    .map_err(|err| err.to_string())?;
                self.key2_timers.insert(key, timer.clone());
                return Ok(timer);
            }
            Err(err) => return Err(err.to_string()),
        };

        self.key2_timers.insert(key.clone(), timer.clone());
        if !self.should_sync_timer(Some(&timer), schema, &info, partition) {
            return Ok(timer);
        }

        let (policy_type, policy_expr) = get_ttl_schedule_policy(&ttl_info);
        if let Err(err) = self.cli.update_timer(
            &timer_ctx(),
            &timer.id,
            &[
                with_set_tags(tags.clone()),
                with_set_sched_expr(policy_type, &policy_expr),
                with_set_enable(ttl_info.enable),
            ],
        ) {
            bg_logger().error(
                "failed to update timer",
                &[
                    field("error", err.to_string()),
                    field("timerID", timer.id.clone()),
                    field("key", key),
                    field("tags", tags.join(",")),
                ],
            );
            return Err(err.to_string());
        }

        let timer = self.cli.get_timer_by_id(&timer_ctx(), &timer.id).map_err(|err| err.to_string())?;
        self.key2_timers.insert(timer.spec.key.clone(), timer.clone());
        Ok(timer)
    }
}

fn ttl_info_of(tbl_info: &TableInfo) -> TTLInfo {
    tbl_info.ttl_info.as_ref().map(|ttl| ttl.read().clone()).unwrap_or_default()
}

/// Go `getTimerTags`.
pub fn get_timer_tags(schema: &CiString, tbl_info: &TableInfo, partition: Option<&PartitionDefinition>) -> Vec<String> {
    let db_tag = format!("db={}", schema.original());
    let tbl_tag = format!("table={}", tbl_info.name.original());
    match partition {
        Some(partition) => vec![db_tag, tbl_tag, format!("partition={}", partition.name.original())],
        None => vec![db_tag, tbl_tag],
    }
}

/// Go `buildTimerKey`.
pub fn build_timer_key(tbl_info: &TableInfo, partition: Option<&PartitionDefinition>) -> String {
    let physical_id = partition.map_or(tbl_info.id, |partition| partition.id);
    build_timer_key_with_id(tbl_info.id, physical_id)
}

/// Go `buildTimerKeyWithID`.
pub fn build_timer_key_with_id(tbl_id: i64, physical_id: i64) -> String {
    format!("{TIMER_KEY_PREFIX}{tbl_id}/{physical_id}")
}

/// Go `getTTLTableStatus`.
fn get_ttl_table_status(
    ctx: &Context,
    se: &Session,
    tbl_info: &TableInfo,
    partition: Option<&PartitionDefinition>,
) -> Result<Option<TableStatus>> {
    let pid = partition.map_or(tbl_info.id, |partition| partition.id);
    let (sql, args) = select_from_ttl_table_status_with_id(pid);
    let args: Vec<_> = args.into_iter().map(tidb_datatype::Datum::new_int).collect();
    let rows = se.execute_sql(ctx, &sql, &args).map_err(|err| err.to_string())?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    row_to_table_status(row).map(Some).map_err(|err| err.to_string())
}

/// Go `getTTLSchedulePolicy`: the timer's schedule policy and expression.
pub fn get_ttl_schedule_policy(info: &TTLInfo) -> (SchedPolicyType, String) {
    let mut interval = info.job_interval.clone();
    if interval.is_empty() {
        // This only happens when the table is created from 6.5 in which the `tidb_job_interval` is not introduced yet.
        // We use `OldDefaultTTLJobInterval` as the return value to ensure a consistent behavior for the
        // upgrades: v6.5 -> v8.5(or previous version) -> newer version than v8.5.
        interval = tidb_model::OLD_DEFAULT_TTL_JOB_INTERVAL.to_owned();
    }
    (SchedPolicyType::interval(), interval)
}
