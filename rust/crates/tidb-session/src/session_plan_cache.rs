// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Session physical cache lifetime and Go's ADMIN FLUSH PLAN_CACHE policy.

use crate::{DriverError, Session};
use tidb_ast::AdminPlanCacheScope;

impl Session {
    /// Installs the hosting domain's plan-cache invalidation owner. It must
    /// outlive individual catalog images and be shared by all its sessions.
    pub fn set_plan_cache_invalidation(
        &mut self,
        owner: std::sync::Arc<tidb_executor::PlanCacheInvalidation>,
    ) {
        self.physical_plan_cache.clear();
        self.plan_cache_invalidation = owner;
    }

    /// Install the hosting cluster's persisted InfoSchema version on the
    /// shared instance key; locally rebuilt catalog image IDs remain local.
    pub fn set_plan_cache_schema_version(&self, version: u64) {
        self.physical_plan_cache
            .set_instance_schema_version(version);
    }

    /// Close a binary-protocol prepared definition under the same policy as
    /// SQL DEALLOCATE. The caller releases the definition after this call.
    pub fn close_prepared(&self, prepared: &crate::PreparedAst) {
        self.release_prepared_plans(
            prepared.select_plan().as_deref(),
            prepared.dml_plan().as_deref(),
        );
    }

    pub(crate) fn release_prepared_plans(
        &self,
        select: Option<&tidb_executor::PreparedSelectPlan>,
        dml: Option<&tidb_executor::PreparedDmlPlan>,
    ) {
        if !self.vars.prepared_plan_cache_enabled()
            || self.session_bool("tidb_ignore_prepared_cache_close_stmt", false)
        {
            return;
        }
        self.configure_session_plan_cache();
        let Some(environment) = self.prepared_plan_cache_environment_for_binding(None) else {
            return;
        };
        let Ok(catalog) = self.lock_catalog() else {
            return;
        };
        if let Some(plan) = select {
            plan.discard_cached_plan(&self.physical_plan_cache, &catalog, &environment);
        }
        if let Some(plan) = dml {
            plan.discard_cached_plan(&self.physical_plan_cache, &catalog, &environment);
        }
    }

    pub(crate) fn configure_session_plan_cache(&self) {
        if !self.vars.prepared_plan_cache_enabled()
            && !self.session_bool("tidb_enable_non_prepared_plan_cache", false)
        {
            return;
        }
        let enabled = self
            .vars
            .get_global("tidb_enable_instance_plan_cache")
            .is_ok_and(|value| value == "ON" || value == "1");
        self.physical_plan_cache
            .select_instance(&self.plan_cache_invalidation, enabled);
        let epoch = self.plan_cache_invalidation.epoch();
        let monitor = self.session_bool("tidb_enable_prepared_plan_cache_memory_monitor", true);
        let guard = self
            .vars
            .system_value("tidb_prepared_plan_cache_memory_guard_ratio")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_PREP_PLAN_CACHE_MEMORY_GUARD_RATIO);
        self.physical_plan_cache.configure(
            self.session_plan_cache_capacity(),
            monitor,
            epoch,
            guard,
        );
    }

    pub(crate) fn flush_session_plan_cache(
        &mut self,
        scope: AdminPlanCacheScope,
    ) -> Result<(), DriverError> {
        if scope == AdminPlanCacheScope::Global {
            return Err(DriverError::unsupported(
                "Do not support the 'admin flush global scope.'",
            ));
        }
        if !self.vars.prepared_plan_cache_enabled() {
            self.append_routed_warning(
                1105,
                "The plan cache is disable. So there no need to flush the plan cache".to_owned(),
            );
            return Ok(());
        }
        self.configure_session_plan_cache();
        self.physical_plan_cache.clear();
        if scope == AdminPlanCacheScope::Instance {
            self.plan_cache_invalidation.expire();
        }
        Ok(())
    }
}
