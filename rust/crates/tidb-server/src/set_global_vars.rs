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
//! * `version` — carried by `VersionInfo` (`--server-version`), which
//!   `SessionVars::get_system` answers directly, including Go's
//!   keep-when-empty behavior.
//! * The `setInstanceVar` block — the registry's scope table is a static
//!   catalog in this port; Go MUTATES `SysVar.Scope` at startup to grant
//!   instance scope. Unported with that mutation, alongside the config
//!   fields it reads (`cfg.Instance.*`).
//! * `tidb_force_priority`, distinct/projection push-down switches,
//!   `datadir`, slow-query file, MPP enforcement, memory alarms, plan-cache
//!   sizing — each reads a `NodeConfig` surface this port does not carry
//!   yet; they follow their config fields.

use tidb_session::GlobalSysvars;

use crate::node_config::NodeConfig;

/// Applies the ported `setGlobalVars` writes to the node's sysvar registry.
pub(crate) fn set_global_vars(config: &NodeConfig, globals: &GlobalSysvars) {
    // `variable.SetSysVar(vardef.Port, fmt.Sprintf("%d", cfg.Port))`.
    globals.set_startup("port", config.port.to_string());
    // `cfg.Socket` arrives with `{Port}` already substituted (the parse did
    // Go's one `strings.Replace`).
    globals.set_startup("socket", config.socket.clone());
    // `strings.Join(cfg.IsolationRead.Engines, ",")`.
    globals.set_startup(
        "tidb_isolation_read_engines",
        config.isolation_read_engines.join(","),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `TestSetGlobalVars` (`cmd/tidb-server/main_test.go:97`), transcreated
    /// over the ported slice. Legs kept, in the source's order: the
    /// isolation-read-engines default and its config override, the
    /// mem-quota-query default, the socket value, and the hostname. The
    /// version legs ride `VersionInfo` (see the module header), and the
    /// instance-scope promotion leg is unported with `setInstanceVar`.
    #[test]
    fn set_global_vars_matches_gos_table() {
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
        // the port. Parse carries Go's `{Port}` substitution.
        let config = NodeConfig::parse(
            [
                "tidb-server",
                "--store",
                "unistore",
                "--cluster-session",
                "--port",
                "4157",
                "--auth-file",
                "/dev/null",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>(),
        )
        .expect("parses");
        assert_eq!(config.socket, "/tmp/tidb-4157.sock");

        let globals = GlobalSysvars::new();
        let mut config = config;
        config.isolation_read_engines = vec!["tikv".to_owned(), "tidb".to_owned()];
        set_global_vars(&config, &globals);

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
        let _ = vars.swap_globals(globals.clone());
        assert_eq!(
            vars.get_global("socket").expect("readable"),
            "/tmp/tidb-4157.sock"
        );
        assert_eq!(vars.get_global("port").expect("readable"), "4157");
        assert_eq!(
            globals.get("socket").expect("readable"),
            "/tmp/tidb-4157.sock"
        );

        // Go's hostname leg: `os.Hostname()` when it resolves. The sem tier
        // owns that default here; `disable()` is the restore path startup
        // takes without SEM.
        tidb_util::sem::disable();
        let hostname = tidb_util::sem::effective_sysvar_default("hostname").expect("answered");
        assert_ne!(hostname, "");
    }
}
