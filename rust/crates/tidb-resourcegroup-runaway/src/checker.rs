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

//! Go `checker.go`: one statement's runaway checker.

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Local, SecondsFormat};
use tidb_proto::kvrpcpb;
use tidb_proto::resource_manager as rmpb;
use tidb_terror::TerrorError;
use tikv_client::RuDetails;

use crate::manager::Manager;

/// Go `checker`: is the query runaway?
pub struct Checker {
    manager: Arc<Manager>,
    resource_group_name: String,
    original_sql: String,
    sql_digest: String,
    plan_digest: String,

    // threshold for resource usage
    deadline: Option<DateTime<Local>>,
    ru_threshold: i64,
    processed_keys_threshold: i64,
    /// From the group runaway settings, applied when a query lacks a
    /// specified watch rule.
    settings: Option<rmpb::RunawaySettings>,

    /// The specified watch action for the runaway query; the action in
    /// `settings` applies when it is not given.
    watch_action: AtomicI32,

    // mutable fields below
    /// The total processed keys of all coprocessor tasks.
    total_processed_keys: AtomicI64,
    /// Set when the query matches the group runaway settings.
    marked_by_identify_in_runaway_settings: AtomicBool,
    /// Set when the query matches the specified watch rules.
    marked_by_query_watch_rule: AtomicBool,
}

impl Checker {
    /// Go `NewChecker`.
    pub fn new(
        manager: Arc<Manager>,
        resource_group_name: &str,
        settings: Option<rmpb::RunawaySettings>,
        original_sql: &str,
        sql_digest: &str,
        plan_digest: &str,
        start_time: DateTime<Local>,
    ) -> Self {
        let mut checker = Self {
            manager,
            resource_group_name: resource_group_name.to_owned(),
            original_sql: original_sql.to_owned(),
            sql_digest: sql_digest.to_owned(),
            plan_digest: plan_digest.to_owned(),
            deadline: None,
            ru_threshold: 0,
            processed_keys_threshold: 0,
            settings,
            watch_action: AtomicI32::new(rmpb::RunawayAction::NoneAction as i32),
            total_processed_keys: AtomicI64::new(0),
            marked_by_identify_in_runaway_settings: AtomicBool::new(false),
            marked_by_query_watch_rule: AtomicBool::new(false),
        };
        if let Some(rule) = checker.settings.as_ref().map(|settings| settings.rule.clone().unwrap_or_default()) {
            // avoid setting deadline if the threshold is 0
            if rule.exec_elapsed_time_ms != 0 {
                checker.deadline =
                    Some(start_time + chrono::Duration::milliseconds(rule.exec_elapsed_time_ms as i64));
            }
            checker.ru_threshold = rule.request_unit;
            checker.processed_keys_threshold = rule.processed_keys;
        }
        checker
    }

    fn settings_action(&self) -> rmpb::RunawayAction {
        self.settings.as_ref().map_or(rmpb::RunawayAction::NoneAction, |settings| {
            rmpb::RunawayAction::try_from(settings.action).unwrap_or(rmpb::RunawayAction::NoneAction)
        })
    }

    fn settings_switch_group_name(&self) -> String {
        self.settings.as_ref().map(|settings| settings.switch_group_name.clone()).unwrap_or_default()
    }

    fn watch_action(&self) -> rmpb::RunawayAction {
        rmpb::RunawayAction::try_from(self.watch_action.load(Ordering::SeqCst))
            .unwrap_or(rmpb::RunawayAction::NoneAction)
    }

    fn is_marked_by_identify_in_runaway_settings(&self) -> bool {
        self.marked_by_identify_in_runaway_settings.load(Ordering::SeqCst)
    }

    /// Go `BeforeExecutor`: whether the query is in the watch list before
    /// executing and after compiling; a switch group name to run it in.
    ///
    /// # Errors
    /// Go `ErrResourceGroupQueryRunawayQuarantine` for a `KILL` watch.
    pub fn before_executor(&self) -> Result<String, TerrorError> {
        // Check if the query matches any specified watch rules.
        for convict in self.get_convict_identifiers() {
            let Some((mut action, mut switch_group_name, exceed_cause)) =
                self.manager.examine_watch_list(&self.resource_group_name, convict)
            else {
                continue;
            };
            // Use the group runaway settings if none are provided.
            if action == rmpb::RunawayAction::NoneAction && self.settings.is_some() {
                action = self.settings_action();
                switch_group_name = self.settings_switch_group_name();
            }
            // Mark it if this is the first time being watched.
            self.mark_runaway_by_query_watch_rule(action, &switch_group_name, &exceed_cause);
            // Take action if needed.
            match action {
                // Return an error to interrupt the query.
                rmpb::RunawayAction::Kill => {
                    return Err(tidb_exeerrors::ERR_RESOURCE_GROUP_QUERY_RUNAWAY_QUARANTINE.clone())
                }
                // Handled in `BeforeCopRequest`.
                rmpb::RunawayAction::CoolDown => return Ok(String::new()),
                rmpb::RunawayAction::DryRun => return Ok(String::new()),
                // Switch the resource group before executing.
                rmpb::RunawayAction::SwitchGroup => return Ok(self.check_switch_group_name(&switch_group_name)),
                // Continue to examine other convicts.
                rmpb::RunawayAction::NoneAction => {}
            }
        }
        Ok(String::new())
    }

    fn check_switch_group_name(&self, group_name: &str) -> String {
        if group_name.is_empty() {
            return String::new();
        }
        match self.manager.resource_group_ctl.get_resource_group(group_name) {
            Ok(Some(_)) => group_name.to_owned(),
            Ok(None) | Err(_) => String::new(),
        }
    }

    /// Go `BeforeCopRequest`: checks runaway and modifies the request before
    /// it is sent.
    ///
    /// # Errors
    /// Go `ErrResourceGroupQueryRunawayInterrupted` for a `KILL` group.
    pub fn before_cop_request(&self, req: &mut kvrpcpb::Context) -> Result<(), TerrorError> {
        // A watch-marked cooldown overrides the priority; other watch actions
        // were handled in `BeforeExecutor`.
        if self.marked_by_query_watch_rule.load(Ordering::SeqCst)
            && self.watch_action() == rmpb::RunawayAction::CoolDown
        {
            resource_control_context(req).override_priority = 1; // set priority to lowest
        }
        let Some(settings) = &self.settings else {
            return Ok(());
        };
        let now = Local::now();
        // Check and ensure the deadline exists.
        let exceed_cause = self.exceeds_thresholds(Some(now), None, 0);
        // Only set timeout when the query has not been marked as runaway yet.
        if !self.is_marked_by_identify_in_runaway_settings() && exceed_cause.is_empty() {
            if self.settings_action() == rmpb::RunawayAction::Kill {
                if let Some(deadline) = self.deadline {
                    let until = deadline - now;
                    // if the execution time is close to the threshold, set a timeout
                    if until < chrono::Duration::from_std(tikv_client::tikv::READ_TIMEOUT_MEDIUM).expect("fits") {
                        req.max_execution_duration_ms = until.num_milliseconds() as u64;
                    }
                }
            }
            return Ok(());
        }
        // Safe to call concurrently: `CheckThresholds` may have marked it.
        self.mark_runaway_by_identify_in_runaway_settings(now, &exceed_cause);
        match self.settings_action() {
            rmpb::RunawayAction::Kill => Err(tidb_exeerrors::ERR_RESOURCE_GROUP_QUERY_RUNAWAY_INTERRUPTED
                .fast_gen_by_args(&[exceed_cause.into()])),
            rmpb::RunawayAction::CoolDown => {
                resource_control_context(req).override_priority = 1; // set priority to lowest
                Ok(())
            }
            rmpb::RunawayAction::SwitchGroup => {
                let switch_group_name = self.check_switch_group_name(&settings.switch_group_name);
                if !switch_group_name.is_empty() {
                    resource_control_context(req).resource_group_name = switch_group_name;
                }
                Ok(())
            }
            rmpb::RunawayAction::NoneAction | rmpb::RunawayAction::DryRun => Ok(()),
        }
    }

    /// Go `CheckAction`: the query's current action; safe concurrently.
    pub fn check_action(&self) -> rmpb::RunawayAction {
        if self.marked_by_query_watch_rule.load(Ordering::SeqCst) {
            return self.watch_action();
        }
        if self.is_marked_by_identify_in_runaway_settings() {
            return self.settings_action();
        }
        rmpb::RunawayAction::NoneAction
    }

    /// Go `CheckRuleKillAction`: whether the group settings kill the query.
    pub fn check_rule_kill_action(&self) -> (String, bool) {
        // Neither group settings nor a watch mark: nothing to check.
        if self.settings.is_none() && !self.marked_by_query_watch_rule.load(Ordering::SeqCst) {
            return (String::new(), false);
        }
        // Group settings not yet matched by rule: check the execution time.
        if self.settings.is_some() && !self.is_marked_by_identify_in_runaway_settings() {
            let now = Local::now();
            let exceed_cause = self.exceeds_thresholds(Some(now), None, 0);
            if exceed_cause.is_empty() {
                return (String::new(), false);
            }
            self.mark_runaway_by_identify_in_runaway_settings(now, &exceed_cause);
            return (exceed_cause, self.settings_action() == rmpb::RunawayAction::Kill);
        }
        (String::new(), false)
    }

    fn mark_quarantine(&self, now: DateTime<Local>, exceed_cause: &str) {
        let Some(settings) = &self.settings else {
            return;
        };
        let Some(watch) = &settings.watch else {
            return;
        };
        // The latest group settings changed: do not mark quarantine.
        match self.manager.resource_group_ctl.get_resource_group(&self.resource_group_name) {
            Ok(Some(group)) if group.runaway_settings.as_ref() == Some(settings) => {}
            _ => return,
        }
        let ttl = std::time::Duration::from_millis(u64::try_from(watch.lasting_duration_ms).unwrap_or(0));
        self.manager.mark_quarantine(
            &self.resource_group_name,
            &self.get_setting_convict_identifier(),
            rmpb::RunawayWatchType::try_from(watch.r#type).unwrap_or(rmpb::RunawayWatchType::NoneWatch),
            self.settings_action(),
            &settings.switch_group_name,
            ttl,
            now,
            exceed_cause,
        );
    }

    fn mark_runaway_by_identify_in_runaway_settings(&self, now: DateTime<Local>, exceed_cause: &str) {
        let swapped = self
            .marked_by_identify_in_runaway_settings
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
        if swapped {
            self.mark_runaway("identify", self.settings_action(), &self.settings_switch_group_name(), now, exceed_cause);
            if !self.marked_by_query_watch_rule.load(Ordering::SeqCst) {
                self.mark_quarantine(now, exceed_cause);
            }
        }
    }

    fn mark_runaway_by_query_watch_rule(&self, action: rmpb::RunawayAction, switch_group_name: &str, exceed_cause: &str) {
        self.marked_by_query_watch_rule.store(true, Ordering::SeqCst);
        self.watch_action.store(action as i32, Ordering::SeqCst);
        self.mark_runaway("watch", action, switch_group_name, Local::now(), exceed_cause);
    }

    fn mark_runaway(
        &self,
        match_type: &str,
        action: rmpb::RunawayAction,
        switch_group_name: &str,
        now: DateTime<Local>,
        exceed_cause: &str,
    ) {
        let action_str = match action {
            rmpb::RunawayAction::SwitchGroup => format!("{}({switch_group_name})", action.as_str_name()),
            action => action.as_str_name().to_owned(),
        }
        .to_lowercase();
        tidb_metrics::server::TIDB_SERVER_QUERY_RUNAWAY_CHECK
            .with_label_values(&[&self.resource_group_name, match_type, &action_str])
            .inc();
        self.manager.mark_runaway(self, &action_str, match_type, now, exceed_cause);
    }

    fn get_setting_convict_identifier(&self) -> String {
        let Some(watch) = self.settings.as_ref().and_then(|settings| settings.watch.as_ref()) else {
            return String::new();
        };
        match rmpb::RunawayWatchType::try_from(watch.r#type) {
            Ok(rmpb::RunawayWatchType::Plan) => self.plan_digest.clone(),
            Ok(rmpb::RunawayWatchType::Similar) => self.sql_digest.clone(),
            Ok(rmpb::RunawayWatchType::Exact) => self.original_sql.clone(),
            _ => String::new(),
        }
    }

    fn get_convict_identifiers(&self) -> [&str; 3] {
        [&self.original_sql, &self.sql_digest, &self.plan_digest]
    }

    pub(crate) fn resource_group_name(&self) -> &str {
        &self.resource_group_name
    }

    pub(crate) fn original_sql(&self) -> &str {
        &self.original_sql
    }

    pub(crate) fn sql_digest(&self) -> &str {
        &self.sql_digest
    }

    pub(crate) fn plan_digest(&self) -> &str {
        &self.plan_digest
    }

    /// Go `CheckThresholds`: checks the coprocessor response's error and the
    /// accumulated usage. `err` is the response's error text; the answer is
    /// the error that replaces it, `None` to keep it.
    pub fn check_thresholds(&self, ru_detail: Option<&RuDetails>, process_keys: i64, err: Option<&str>) -> Option<TerrorError> {
        self.settings.as_ref()?;
        let now = Local::now();
        // decide whether to check the time.
        let check_time = err
            .is_some_and(|err| err.starts_with("Coprocessor task terminated due to exceeding the deadline"))
            .then_some(now);
        // add the processed keys to the total processed keys.
        let total_processed_keys = self.total_processed_keys.fetch_add(process_keys, Ordering::SeqCst) + process_keys;
        let exceed_cause = self.exceeds_thresholds(check_time, ru_detail, total_processed_keys);
        // No need to mark as runaway if the query is not exceeded any threshold.
        if exceed_cause.is_empty() {
            return None;
        }
        self.mark_runaway_by_identify_in_runaway_settings(now, &exceed_cause);
        // Other actions are handled in `BeforeCopRequest`.
        (self.settings_action() == rmpb::RunawayAction::Kill).then(|| {
            tidb_exeerrors::ERR_RESOURCE_GROUP_QUERY_RUNAWAY_INTERRUPTED.fast_gen_by_args(&[exceed_cause.into()])
        })
    }

    /// Go `exceedsThresholds`; `now` is `None` for Go's `NullTime`, which no
    /// deadline is before.
    fn exceeds_thresholds(&self, now: Option<DateTime<Local>>, ru: Option<&RuDetails>, processed_keys: i64) -> String {
        if let (Some(deadline), Some(now)) = (self.deadline, now) {
            if deadline <= now {
                // Go `exceedCause{cause: exceedCauseTime}.String()`.
                return format!(
                    "ElapsedTime = {}({})",
                    now.to_rfc3339_opts(SecondsFormat::Secs, true),
                    deadline.to_rfc3339_opts(SecondsFormat::Secs, true)
                );
            }
        }
        if let Some(ru) = ru {
            if self.ru_threshold != 0 && (ru.write_ru() + ru.read_ru()) as i64 >= self.ru_threshold {
                return format!("RequestUnit = {ru}({})", self.ru_threshold);
            }
        }
        if processed_keys != 0 && self.processed_keys_threshold != 0 && processed_keys >= self.processed_keys_threshold {
            return format!("ProcessedKeys = {processed_keys}({})", self.processed_keys_threshold);
        }
        String::new()
    }

    /// Go `ResetTotalProcessedKeys`.
    pub fn reset_total_processed_keys(&self) {
        self.total_processed_keys.store(0, Ordering::SeqCst);
    }
}

/// Go `req.ResourceControlContext`, created when absent.
fn resource_control_context(req: &mut kvrpcpb::Context) -> &mut kvrpcpb::ResourceControlContext {
    req.resource_control_context.get_or_insert_with(Default::default)
}

impl tidb_txnkv::RunawayChecker for Checker {
    fn before_executor(&self) -> Result<String, TerrorError> {
        Checker::before_executor(self)
    }

    fn before_cop_request(&self, req: &mut kvrpcpb::Context) -> Result<(), TerrorError> {
        Checker::before_cop_request(self, req)
    }

    fn check_thresholds(&self, ru_detail: Option<&RuDetails>, process_keys: i64, err: Option<&str>) -> Option<TerrorError> {
        Checker::check_thresholds(self, ru_detail, process_keys, err)
    }

    fn reset_total_processed_keys(&self) {
        Checker::reset_total_processed_keys(self);
    }

    fn check_action(&self) -> rmpb::RunawayAction {
        Checker::check_action(self)
    }

    fn check_rule_kill_action(&self) -> (String, bool) {
        Checker::check_rule_kill_action(self)
    }
}
