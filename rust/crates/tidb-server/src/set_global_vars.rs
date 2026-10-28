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

//! The reachable slice of Go `setGlobalVars` (`cmd/tidb-server/main.go`):
//! the startup push of config-derived values into the sysvar registry, which
//! is what makes `@@port`, `@@socket` and `@@tidb_isolation_read_engines`
//! report THIS node's configuration instead of the registry defaults.
//!
//! Go's `variable.SetSysVar` replaces a registry default in place; here the
//! same write lands in [`GlobalSysvars::set_startup`]'s node tier, which
//! every `@@` read without a session copy falls through to.
//!
//! Dispositions of the rest of Go's function, by name:
//! * `hostname` — already live through `tidb_util::sem`: startup calls
//!   `sem::disable()`/`enable()`, whose hostname default is what
//!   `effective_sysvar_default` answers. Not repeated here.
//! * `version` and `version_comment` — initialized from the same process-wide
//!   package variables Go updates before this function runs.
//! * `tidb_slow_query_file` — the registry's effective default reads the
//!   same config field (`effective_default`).
//! * Plan-cache sizing (`PreparedPlanCacheMaxMemory`) — no prepared-plan
//!   cache memory bound exists in this port to receive it.

use tidb_session::GlobalSysvars;

use tidb_session::domain::node_config::NodeConfig;

/// Applies the ported `setGlobalVars` writes to the node's sysvar registry.
pub fn set_global_vars(config: &NodeConfig, globals: &GlobalSysvars) {
    globals.set_startup("version", tidb_mysql::runtime_versions().server_version);
    globals.set_startup(
        "version_comment",
        format!(
            "TiDB Server (Apache License 2.0) {} Edition, MySQL 8.0 compatible",
            tidb_util::versioninfo::tidb_edition()
        ),
    );
    // Go's `setInstanceVar` block: the `[instance]` values this instance
    // keeps over the `mysql.global_variables` rows.
    let instance = &config.global_config.instance;
    for (name, value) in [
        (
            tidb_vardef::tidb_vars::TIDB_STMT_SUMMARY_MAX_STMT_COUNT,
            instance.stmt_summary_max_stmt_count.to_string(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT,
            instance.server_memory_limit.clone(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_MODE,
            instance.mem_arbitrator_mode.clone(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_MEM_ARBITRATOR_SOFT_LIMIT,
            instance.mem_arbitrator_soft_limit.clone(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_SERVER_MEMORY_LIMIT_GC_TRIGGER,
            instance.server_memory_limit_gc_trigger.clone(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE,
            instance.instance_plan_cache_max_mem_size.clone(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_STATS_CACHE_MEM_QUOTA,
            instance.stats_cache_mem_quota.to_string(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_BINDING_CACHE,
            instance.mem_quota_binding_cache.to_string(),
        ),
        (
            tidb_vardef::tidb_vars::TIDB_SCHEMA_CACHE_SIZE,
            instance.schema_cache_size.clone(),
        ),
    ] {
        globals.set_instance_var(name, value);
    }
    let source = &config.global_config;
    let on_off = |enabled: bool| if enabled { "ON" } else { "OFF" }.to_owned();
    // `variable.SetSysVar(vardef.TiDBForcePriority,
    // mysql.Priority2Str[mysql.Str2Priority(cfg.Instance.ForcePriority)])`.
    globals.set_startup(
        tidb_vardef::tidb_vars::TIDB_FORCE_PRIORITY,
        tidb_mysql::priority_from_str(&instance.force_priority)
            .as_name()
            .to_owned(),
    );
    globals.set_startup(
        tidb_vardef::tidb_vars::TIDB_OPT_DISTINCT_AGG_PUSH_DOWN,
        on_off(source.performance.distinct_agg_push_down),
    );
    globals.set_startup(
        tidb_vardef::tidb_vars::TIDB_OPT_PROJECTION_PUSH_DOWN,
        on_off(source.performance.projection_push_down),
    );
    // `variable.SetSysVar(vardef.Port, fmt.Sprintf("%d", cfg.Port))`.
    globals.set_startup("port", config.port.to_string());
    // `cfg.Socket` arrives with `{Port}` already substituted (the parse did
    // Go's one `strings.Replace`).
    globals.set_startup("socket", config.socket.clone());
    globals.set_startup("datadir", source.path.clone());
    // `strings.Join(cfg.IsolationRead.Engines, ",")`.
    globals.set_startup(
        "tidb_isolation_read_engines",
        config.isolation_read_engines.join(","),
    );
    globals.set_startup(
        tidb_vardef::tidb_vars::TIDB_ENFORCE_MPP_EXECUTION,
        on_off(source.performance.enforce_mpp),
    );
    globals.set_startup(
        tidb_vardef::tidb_vars::TIDB_CONSTRAINT_CHECK_IN_PLACE_PESSIMISTIC,
        on_off(source.pessimistic_txn.constraint_check_in_place_pessimistic),
    );
    // Go binds `max_connections` to the LIVE instance config, in the sysvar
    // definition itself rather than in `setGlobalVars`:
    //
    //     {Scope: ScopeInstance, Name: MaxConnections,
    //      Value: strconv.FormatUint(
    //          uint64(config.GetGlobalConfig().Instance.MaxConnections), 10),
    //      ... GetGlobal: ... return ...Instance.MaxConnections }
    //
    // so `SELECT @@max_connections` answers the limit the node is actually
    // enforcing. The static catalog here carried the registry default `0`
    // instead, and 0 MEANS UNLIMITED -- so a node booted with
    // `--max-connections 5` refused the sixth connection while telling every
    // client that asked that it had no limit at all. Connection poolers and
    // monitoring read this variable to size themselves.
    globals.set_startup("max_connections", config.max_connections.to_string());
    // `vardef.MemoryUsageAlarmRatio.Store(cfg.Instance.MemoryUsageAlarmRatio)`.
    // Keep this after the startup registry writes: publishing a registry
    // value refreshes the typed process globals, while Go's direct atomic
    // store is the final value for this setting during startup.
    tidb_vardef::set_memory_usage_alarm_ratio(
        config.global_config.instance.memory_usage_alarm_ratio,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_session::domain::node_config::NodeOptions;

    struct RestoreMemoryUsageAlarmRatio(f64);

    impl Drop for RestoreMemoryUsageAlarmRatio {
        fn drop(&mut self) {
            tidb_vardef::set_memory_usage_alarm_ratio(self.0);
        }
    }

    /// `TestSetGlobalVars` (`cmd/tidb-server/main_test.go:97`), transcreated
    /// over the ported slice. Legs kept, in the source's order: the
    /// isolation-read-engines default and its config override, the
    /// mem-quota-query default, the socket value, and the hostname.
    #[test]
    fn set_global_vars_matches_gos_table() {
        let _restore_alarm_ratio =
            RestoreMemoryUsageAlarmRatio(tidb_vardef::memory_usage_alarm_ratio());
        // Registry defaults before any startup push (Go's first asserts).
        assert_eq!(
            tidb_session::sysvar::get_sys_var("tidb_isolation_read_engines")
                .expect("registered")
                .value,
            "tikv,tiflash,tidb"
        );
        assert_eq!(
            tidb_session::sysvar::get_sys_var("tidb_mem_quota_query")
                .expect("registered")
                .value,
            "1073741824"
        );

        // The config override: engines `tikv,tidb`, an explicit socket, and
        // the port. The projection carries Go's `{Port}` substitution.
        let config = NodeConfig::with_defaults(
            |config| {
                config.store = tidb_config::store::StoreType("unistore".to_owned());
                config.port = 4157;
            },
            NodeOptions {
                auth_file: Some("/dev/null".to_owned()),
                ..NodeOptions::default()
            },
        )
        .expect("parses");
        assert_eq!(config.socket, "/tmp/tidb-4157.sock");

        let globals = GlobalSysvars::new();
        let mut config = config;
        assert_eq!(config.global_config.instance.memory_usage_alarm_ratio, 0.8);
        config.isolation_read_engines = vec!["tikv".to_owned(), "tidb".to_owned()];
        set_global_vars(&config, &globals);
        assert_eq!(tidb_vardef::memory_usage_alarm_ratio(), 0.8);

        assert_eq!(
            globals
                .get("tidb_isolation_read_engines")
                .expect("readable"),
            "tikv,tidb"
        );
        assert_eq!(globals.get("port").expect("readable"), "4157");
        // The SHOW GLOBAL VARIABLES / `@@global.x` read path answers a
        // NONE-scope variable from the same node tier (probe: SHOW GLOBAL
        // VARIABLES LIKE 'socket' reported the stale registry default while
        // `@@socket` reported the configured path).
        let mut vars = tidb_session::SessionVars::default();
        let _ = vars.swap_globals(Some(std::sync::Arc::new(globals.clone())));
        assert_eq!(
            vars.get_global("socket").expect("readable"),
            "/tmp/tidb-4157.sock"
        );
        assert_eq!(vars.get_global("port").expect("readable"), "4157");
        assert_eq!(
            globals.get("socket").expect("readable"),
            "/tmp/tidb-4157.sock"
        );

        // Go's `setInstanceVar` leg: an `[instance]` value replaces the
        // registry value, and the instance keeps it over a persisted row
        // (`IsInitedFromConfig`), while a variable left unset still reads its
        // row.
        let globals = GlobalSysvars::new();
        config.global_config.instance.instance_plan_cache_max_mem_size = "444".to_owned();
        config.global_config.instance.force_priority = "high_priority".to_owned();
        config.global_config.performance.distinct_agg_push_down = true;
        config.global_config.performance.enforce_mpp = true;
        config.global_config.path = "/data/tidb".to_owned();
        set_global_vars(&config, &globals);
        // The rest of Go's `SetSysVar` writes follow their config fields.
        assert_eq!(globals.get("tidb_force_priority").expect("readable"), "HIGH_PRIORITY");
        // Go `vardef.ForcePriority`, which every statement then runs at.
        assert_eq!(
            tidb_vardef::FORCE_PRIORITY.swap(0, std::sync::atomic::Ordering::SeqCst),
            tidb_mysql::Priority::High.as_enum()
        );
        assert_eq!(globals.get("tidb_opt_distinct_agg_push_down").expect("readable"), "ON");
        // Go's config default `ProjectionPushDown: true`.
        assert_eq!(globals.get("tidb_opt_projection_push_down").expect("readable"), "ON");
        assert_eq!(globals.get("tidb_enforce_mpp").expect("readable"), "ON");
        assert_eq!(globals.get("datadir").expect("readable"), "/data/tidb");
        let plan_cache = tidb_vardef::tidb_vars::TIDB_INSTANCE_PLAN_CACHE_MAX_MEM_SIZE;
        let binding_cache = tidb_vardef::tidb_vars::TIDB_MEM_QUOTA_BINDING_CACHE;
        assert_eq!(globals.get(plan_cache).expect("readable"), "444");
        globals.replace_from(&GlobalSysvars::from_cluster_rows([
            (plan_cache.to_owned(), "209715200".to_owned()),
            (binding_cache.to_owned(), "1048576".to_owned()),
        ]));
        assert_eq!(globals.get(plan_cache).expect("readable"), "444");
        assert_eq!(globals.get(binding_cache).expect("readable"), "1048576");

        // Go's hostname leg: `os.Hostname()` when it resolves. The sem tier
        // owns that default here; `disable()` is the restore path startup
        // takes without SEM.
        tidb_util::sem::disable();
        let hostname = tidb_util::sem::effective_sysvar_default("hostname").expect("answered");
        assert_ne!(hostname, "");
    }
}
