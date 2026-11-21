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

//! Go `doDMLWorks`' inputs: the facts the bootstrap reads from its
//! environment, and every GLOBAL variable's initial value. The rows
//! themselves are written by Go's statements
//! ([`crate::bootstrap::do_dml_works`]).

use tidb_datatype::Time;

/// The facts about *this* cluster and host that Go's `doDMLWorks` reads out of
/// its environment rather than out of any constant.
///
/// They are inputs, not defaults: a real TiDB reads `system_tz` and
/// `new_collation_enabled` back on every start and refuses to run without them,
/// so a bootstrap that cannot state them is a bootstrap that produces a cluster
/// no TiDB can open.
#[derive(Clone, Debug)]
pub struct BootstrapEnvironment {
    /// Go `timeutil.InferSystemTZ()`: the host's own zone name.
    pub system_tz: String,
    /// Go `config.NewCollationsEnabledOnFirstBootstrap`, frozen for the life of
    /// the cluster by this very row.
    pub new_collation_enabled: bool,
    /// The PD cluster ID this keyspace belongs to.
    pub cluster_id: u64,
    /// What `CURRENT_TIMESTAMP` evaluates to for this bootstrap, in UTC.
    ///
    /// Go's seed rows get theirs from the `INSERT` that writes them; this one
    /// is stated so a bootstrap plan stays a pure function of its inputs.
    pub current_timestamp: Time,
    /// Go `Mutator.GetDDLTableVersion` before `InitDDLTables` advances it.
    /// The bootstrap planner creates every newer version group and records the
    /// resulting version in both metadata and `mysql.tidb`, as Go does.
    pub ddl_table_version: i64,
}

/// Go `doDMLWorks`' `mysql.global_variables` seeding: every variable with a
/// global scope, at `GlobalSystemVariableInitialValue(v.Name, v.Value)` --
/// which, in a test build (`intest`), seeds `tidb_mem_oom_action = LOG` and
/// `tidb_enable_auto_analyze = OFF` as Go's testkit stores do.
pub(crate) fn global_variables() -> Vec<(String, String)> {
    tidb_variable::sysvar::sys_vars()
        .iter()
        .filter(|definition| definition.has_global_scope())
        .map(|definition| {
            (
                definition.name.to_owned(),
                tidb_variable::vars::global_system_variable_initial_value(definition),
            )
        })
        .collect()
}

/// The `mysql.global_variables` rows a bootstrapped cluster holds, as
/// `(name, value)`: what a test node that never ran the bootstrap loads.
#[cfg(test)]
pub(crate) fn bootstrapped_global_variables() -> Vec<(String, String)> {
    global_variables()
}
