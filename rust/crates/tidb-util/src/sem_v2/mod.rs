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

//! Go `pkg/util/sem/v2` lands as a complete package: the SEM v2 policy object
//! and its process-wide switch (`sem.go`), the JSON configuration and its
//! validation (`config.go`), the restricted-SQL rules (`sql_rule.go`), the
//! restricted optimizer hints (`restricted_hint.go`), and the test helpers
//! (`testhelper.go`), with all six of the package's test functions.
//!
//! # Relationship to `crate::sem`
//!
//! `crate::sem` is Go `pkg/util/sem` — SEM v1, whose restricted set is
//! hard-coded in the source. This module is the independent Go package
//! `pkg/util/sem/v2`, whose restricted set is *configured* from a JSON file:
//! databases, tables, system and status variables, privileges, SQL statements
//! and rules, and optimizer hints. The two coexist in Go under the same
//! package name in different directories; neither imports the other, and this
//! module does not modify or reuse `crate::sem`. Where v2 deliberately mirrors
//! v1's behavior it says so in the source (`isInvisibleTable` falls back to the
//! schema check "to be compatible with SEM v1"; `isInvisibleStatusVar` exists
//! only for that compatibility).
//!
//! # Narrowings and boundaries
//!
//! `tidb-util` is the workspace's bottom crate, so every dependency that lives
//! above it is recovered locally rather than dropped:
//!
//! - **`pkg/parser/ast`.** The restricted-SQL rules read a handful of AST
//!   shapes: a `CREATE TABLE`'s table options, an `ALTER TABLE`'s specs, a
//!   `SELECT`'s `INTO OUTFILE` clause, and the path/source of `IMPORT INTO` and
//!   `LOAD DATA`. Those become [`StmtView`] and its enums in [`sql_rule`], a
//!   narrowed statement view the parser crate can build. `ast.StmtNode`'s
//!   `SEMCommand()` becomes [`StmtView::sem_command`].
//! - **`pkg/sessionctx/variable`'s sysvar registry.** `GetSysVar`/`SetSysVar`
//!   are narrowed to the [`SysVarRegistry`] trait, installed once with
//!   [`set_sys_var_registry`]. Without a registry every variable reads as
//!   unknown, which is exactly Go's `nil` `SysVar` branch.
//! - **`pkg/sessionctx/vardef`.** Only five constants are referenced; their
//!   string values are inlined here (verified against
//!   `pkg/sessionctx/vardef`), the same treatment `crate::sem` gives them.
//! - **`pkg/parser/mysql.TiDBReleaseVersion`** is a Go package *variable* the
//!   tests assign. [`tidb_mysql::TIDB_RELEASE_VERSION`] is a constant, so
//!   [`set_tidb_release_version`] provides the same overridable value.
//! - **`github.com/coreos/go-semver/semver`** is not a workspace dependency and
//!   no `semver` crate is vendored, so [`config::SemVersion`] hand-rolls the
//!   parse and ordering `validateSEMConfig` needs.
//! - **`pkg/objstore`** is used for `objstore.IsLocal(u)` at two call sites and
//!   is inlined as [`sql_rule::is_local_url`], together with the scheme half of
//!   Go's `net/url.Parse` that feeds it.
//! - Go's `Enable` and `EnableBy` assert (`intest.Assert`) that SEM is not
//!   already enabled; that assertion is preserved through `crate::intest`.

mod config;
mod restricted_hint;
mod sql_rule;
mod testhelper;

pub use config::{
    validate_sem_config, ColumnRestriction, Config, SQLRestriction, SemVersion, TableRestriction,
    VariableRestriction,
};
pub use restricted_hint::{is_restricted_hint, HINT_GUARD_VARS};
pub use sql_rule::{
    alter_table_attributes_rule, import_from_local_rule, import_with_external_id_rule,
    is_local_url, select_into_file_rule, sql_rule_by_name, time_to_live_sql_rule, url_scheme,
    AlterTableSpec, AlterTableType, SQLRule, StmtKind, StmtView, TableOptionType, SQL_RULE_NAMES,
};
pub use testhelper::{
    add_restricted_privileges_for_test, enable_from_path_for_test,
    remove_restricted_privileges_for_test,
};

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use tidb_log::Value;

use crate::logutil;

// Go `vardef` constants, inlined (see the module boundaries).
/// Go `vardef.TiDBEnableEnhancedSecurity`.
pub const TIDB_ENABLE_ENHANCED_SECURITY: &str = "tidb_enable_enhanced_security";
/// Go `vardef.On`.
pub const ON: &str = "ON";
/// Go `vardef.Off`.
pub const OFF: &str = "OFF";
/// The value `EnableBy` stores to mark SEM as configured by a config file.
pub const CONFIG: &str = "CONFIG";
/// The `RESTRICTED_` privilege prefix.
const RESTRICTED_PRIV_PREFIX: &str = "RESTRICTED_";

/// Go `vardef.SysVar.Scope`, narrowed to the distinction SEM makes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SysVarScope {
    /// Go `vardef.ScopeNone`: the variable is read-only.
    #[default]
    None,
    /// Go `vardef.ScopeGlobal`.
    Global,
    /// Go `vardef.ScopeSession`.
    Session,
    /// Go `vardef.ScopeInstance`.
    Instance,
    /// Any other scope combination; distinct from `None`, which is all SEM
    /// checks.
    Other,
}

/// Go `variable.SysVar`, narrowed to the two fields SEM reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SysVar {
    /// Go `SysVar.Scope`.
    pub scope: SysVarScope,
    /// Go `SysVar.Value`.
    pub value: String,
}

// boundary: `pkg/sessionctx/variable`'s process-wide system variable registry.
/// The registry `variable.GetSysVar` / `variable.SetSysVar` reach.
pub trait SysVarRegistry: Send + Sync {
    /// Go `variable.GetSysVar`.
    fn get_sys_var(&self, name: &str) -> Option<SysVar>;
    /// Go `variable.SetSysVar`.
    fn set_sys_var(&self, name: &str, value: &str);
}

static SYS_VAR_REGISTRY: RwLock<Option<Arc<dyn SysVarRegistry>>> = RwLock::new(None);

/// Installs the registry SEM reads and writes system variables through.
pub fn set_sys_var_registry(registry: Option<Arc<dyn SysVarRegistry>>) {
    *SYS_VAR_REGISTRY
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = registry;
}

/// Go `variable.GetSysVar`. Without a registry every variable is unknown,
/// which is Go's `nil` `SysVar` branch.
#[must_use]
pub fn get_sys_var(name: &str) -> Option<SysVar> {
    let registry = SYS_VAR_REGISTRY
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    registry.and_then(|registry| registry.get_sys_var(name))
}

/// Go `variable.SetSysVar`.
pub fn set_sys_var(name: &str, value: &str) {
    let registry = SYS_VAR_REGISTRY
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some(registry) = registry {
        registry.set_sys_var(name, value);
    }
}

/// The current TiDB release version, overriding [`tidb_mysql::TIDB_RELEASE_VERSION`].
static TIDB_RELEASE_VERSION_OVERRIDE: RwLock<Option<String>> = RwLock::new(None);

/// Sets the value Go's `mysql.TiDBReleaseVersion` package variable would hold.
/// `None` restores the compiled-in [`tidb_mysql::TIDB_RELEASE_VERSION`].
pub fn set_tidb_release_version(version: Option<String>) {
    *TIDB_RELEASE_VERSION_OVERRIDE
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = version;
}

/// Go `mysql.TiDBReleaseVersion`.
#[must_use]
pub fn tidb_release_version() -> String {
    TIDB_RELEASE_VERSION_OVERRIDE
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .unwrap_or_else(|| tidb_mysql::TIDB_RELEASE_VERSION.to_owned())
}

/// Go `globalSem`. Go uses an `atomic.Pointer` because the tests swap it
/// repeatedly; the normal path only writes it at startup.
static GLOBAL_SEM: RwLock<Option<Arc<SemImpl>>> = RwLock::new(None);

fn load_global_sem() -> Option<Arc<SemImpl>> {
    GLOBAL_SEM
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

fn store_global_sem(sem: Option<Arc<SemImpl>>) {
    *GLOBAL_SEM
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = sem;
}

/// Go `restrictedVariableAttr`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct RestrictedVariableAttr {
    hidden: bool,
    readonly: bool,
    value: String,
}

/// Go `restrictedTableAttr`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct RestrictedTableAttr {
    hidden: bool,
}

/// Go `buildSEMSqlValidateFunction`'s return type.
type SqlValidator = Box<dyn Fn(&StmtView) -> bool + Send + Sync>;

/// Go `semImpl`: the compiled SEM policy.
pub struct SemImpl {
    restricted_databases: HashSet<String>,
    restricted_tables: HashMap<String, HashMap<String, RestrictedTableAttr>>,
    restricted_variables: HashMap<String, RestrictedVariableAttr>,
    /// Go mutates this map in place from `testhelper.go`, so it is behind a
    /// lock here; Go documents the same operation as goroutine-unsafe.
    restricted_privileges: RwLock<HashSet<String>>,
    restricted_status_variables: HashSet<String>,
    restricted_sql: Option<SqlValidator>,
    /// The set of lower-case hint names to strip.
    restricted_hints: HashSet<String>,
}

impl std::fmt::Debug for SemImpl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SemImpl")
            .field("restricted_databases", &self.restricted_databases)
            .field("restricted_tables", &self.restricted_tables)
            .field("restricted_variables", &self.restricted_variables)
            .field("restricted_privileges", &self.restricted_privileges)
            .field(
                "restricted_status_variables",
                &self.restricted_status_variables,
            )
            .field("has_restricted_sql", &self.restricted_sql.is_some())
            .field("restricted_hints", &self.restricted_hints)
            .finish()
    }
}

impl SemImpl {
    /// Go `semImpl.isInvisibleSchema`.
    #[must_use]
    pub fn is_invisible_schema(&self, db_name: &str) -> bool {
        self.restricted_databases.contains(&db_name.to_lowercase())
    }

    /// Go `semImpl.isInvisibleTable`.
    #[must_use]
    pub fn is_invisible_table(&self, db_lower_name: &str, tbl_lower_name: &str) -> bool {
        // to be compatible with SEM v1, we need to check the invisible schema.
        if self.is_invisible_schema(db_lower_name) {
            return true;
        }
        match self.restricted_tables.get(db_lower_name) {
            None => false,
            Some(tables) => tables.get(tbl_lower_name).is_some_and(|tbl| tbl.hidden),
        }
    }

    /// Go `semImpl.isRestrictedPrivilege`.
    #[must_use]
    pub fn is_restricted_privilege(&self, privilege: &str) -> bool {
        // All privileges starting with "RESTRICTED_" are considered restricted.
        if privilege.starts_with(RESTRICTED_PRIV_PREFIX) {
            return true;
        }
        self.restricted_privileges
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(privilege)
    }

    /// Go `semImpl.isInvisibleSysVar`.
    #[must_use]
    pub fn is_invisible_sys_var(&self, var_name: &str) -> bool {
        self.restricted_variables
            .get(var_name)
            .is_some_and(|attr| attr.hidden)
    }

    /// Go `semImpl.isInvisibleStatusVar`. SEM v2 does not support restricted
    /// status variables; this is kept for compatibility with SEM v1.
    #[must_use]
    pub fn is_invisible_status_var(&self, var_name: &str) -> bool {
        self.restricted_status_variables.contains(var_name)
    }

    /// Go `semImpl.isReadOnlyVariable`.
    #[must_use]
    pub fn is_read_only_variable(&self, var_name: &str) -> bool {
        self.restricted_variables
            .get(var_name)
            .is_some_and(|attr| attr.readonly)
    }

    /// Go `semImpl.isRestrictedSQL`.
    #[must_use]
    pub fn is_restricted_sql(&self, stmt: &StmtView) -> bool {
        match self.restricted_sql.as_ref() {
            None => false,
            Some(validate) => validate(stmt),
        }
    }

    /// Go `semImpl.isRestrictedHint`.
    ///
    /// # Errors
    ///
    /// Returns Go's user-facing message when the hint is restricted.
    pub fn is_restricted_hint(&self, hint_name_lower: &str) -> Result<(), String> {
        restricted_hint::is_restricted_hint_impl(self, hint_name_lower)
    }

    /// Go `semImpl.overrideRestrictedVariable`.
    pub fn override_restricted_variable(&self) {
        for (name, attr) in &self.restricted_variables {
            if !attr.value.is_empty() {
                set_sys_var(name, &attr.value);
            }
        }
    }

    fn add_restricted_privilege(&self, privilege: String) {
        self.restricted_privileges
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(privilege);
    }

    fn remove_restricted_privilege(&self, privilege: &str) {
        self.restricted_privileges
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(privilege);
    }
}

/// Go `buildSEMSqlValidateFunction`.
fn build_sem_sql_validate_function(sql_restriction: &SQLRestriction) -> SqlValidator {
    let mut sql_rules: Vec<SQLRule> = Vec::with_capacity(sql_restriction.rule.len());
    for rule_name in &sql_restriction.rule {
        match sql_rule_by_name(rule_name) {
            Some(rule) => sql_rules.push(rule),
            None => {
                // should never happen
                logutil::bg_logger().warn(
                    "unknown SQL rule",
                    &[tidb_log::Field::new("rule", Value::Str(rule_name.clone()))],
                );
                crate::intest::assert_with_message(false, format!("unknown SQL rule: {rule_name}"));
            }
        }
    }

    let mut sql_commands = HashSet::with_capacity(sql_restriction.sql.len());
    for sql in &sql_restriction.sql {
        let sql = sql.trim().to_uppercase();
        if sql.is_empty() {
            continue;
        }
        sql_commands.insert(sql);
    }

    Box::new(move |stmt: &StmtView| {
        // check SQL commands
        if sql_commands.contains(stmt.sem_command()) {
            return true;
        }
        // check SQL rules
        sql_rules.iter().any(|rule| rule(stmt))
    })
}

/// Go `buildSEMFromConfig`.
#[must_use]
pub fn build_sem_from_config(cfg: &Config) -> SemImpl {
    let mut restricted_tables: HashMap<String, HashMap<String, RestrictedTableAttr>> =
        HashMap::new();
    for tbl in &cfg.restricted_tables {
        restricted_tables
            .entry(tbl.schema.clone())
            .or_default()
            .insert(tbl.name.clone(), RestrictedTableAttr { hidden: tbl.hidden });
    }

    SemImpl {
        restricted_databases: cfg.restricted_databases.iter().cloned().collect(),
        restricted_tables,
        restricted_variables: cfg
            .restricted_variables
            .iter()
            .map(|var| {
                (
                    var.name.clone(),
                    RestrictedVariableAttr {
                        hidden: var.hidden,
                        readonly: var.readonly,
                        value: var.value.clone(),
                    },
                )
            })
            .collect(),
        restricted_status_variables: cfg.restricted_status_var.iter().cloned().collect(),
        restricted_privileges: RwLock::new(
            cfg.restricted_privileges
                .iter()
                .map(|privilege| privilege.to_uppercase())
                .collect(),
        ),
        restricted_sql: Some(build_sem_sql_validate_function(&cfg.restricted_sql)),
        restricted_hints: cfg
            .restricted_hints
            .iter()
            .map(|hint| hint.to_lowercase())
            .collect(),
    }
}

/// Go `IsInvisibleSchema`.
#[must_use]
pub fn is_invisible_schema(db_name: &str) -> bool {
    load_global_sem().is_some_and(|sem| sem.is_invisible_schema(db_name))
}

/// Go `IsInvisibleTable`.
#[must_use]
pub fn is_invisible_table(db_lower_name: &str, tbl_lower_name: &str) -> bool {
    load_global_sem().is_some_and(|sem| sem.is_invisible_table(db_lower_name, tbl_lower_name))
}

/// Go `IsRestrictedPrivilege`.
#[must_use]
pub fn is_restricted_privilege(privilege: &str) -> bool {
    crate::intest::assert_with_message(
        privilege.to_uppercase() == privilege,
        "privilege name must be uppercase",
    );
    load_global_sem().is_some_and(|sem| sem.is_restricted_privilege(privilege))
}

/// Go `IsInvisibleSysVar`.
#[must_use]
pub fn is_invisible_sys_var(var_name: &str) -> bool {
    load_global_sem().is_some_and(|sem| sem.is_invisible_sys_var(var_name))
}

/// Go `IsReadOnlyVariable`.
#[must_use]
pub fn is_read_only_variable(var_name: &str) -> bool {
    load_global_sem().is_some_and(|sem| sem.is_read_only_variable(var_name))
}

/// Go `IsInvisibleStatusVar`.
#[must_use]
pub fn is_invisible_status_var(var_name: &str) -> bool {
    load_global_sem().is_some_and(|sem| sem.is_invisible_status_var(var_name))
}

/// Go `IsRestrictedSQL`.
#[must_use]
pub fn is_restricted_sql(stmt: &StmtView) -> bool {
    load_global_sem().is_some_and(|sem| sem.is_restricted_sql(stmt))
}

/// Go `Enable`: enables SEM from a configuration file.
///
/// # Errors
///
/// Propagates the parse and validation errors.
pub fn enable(config_path: &str) -> Result<(), String> {
    crate::intest::assert_with_message(load_global_sem().is_none(), "SEM is already enabled");
    let sem_config = config::parse_sem_config_from_file(config_path)?;
    enable_by(&sem_config)
}

/// Go `EnableBy`: enables SEM from an already-parsed configuration.
///
/// # Errors
///
/// Propagates the validation error.
pub fn enable_by(sem_config: &Config) -> Result<(), String> {
    crate::intest::assert_with_message(load_global_sem().is_none(), "SEM is already enabled");
    validate_sem_config(sem_config)?;

    let sem = Arc::new(build_sem_from_config(sem_config));
    sem.override_restricted_variable();
    store_global_sem(Some(sem));

    // set the system variable to indicate SEM is configured by the config file.
    set_sys_var(TIDB_ENABLE_ENHANCED_SECURITY, CONFIG);

    // write to log so users understand why some operations are weird.
    logutil::bg_logger().info(
        "tidb-server is operating with security enhanced mode (SEM) v2 enabled",
        &[],
    );

    Ok(())
}

/// Go `IsEnabled`.
#[must_use]
pub fn is_enabled() -> bool {
    load_global_sem().is_some()
}

/// Go `Disable`.
pub fn disable() {
    store_global_sem(None);
    set_sys_var(TIDB_ENABLE_ENHANCED_SECURITY, OFF);
}

/// The active policy, for the callers Go serves through `globalSem.Load()`.
#[must_use]
pub fn global_sem() -> Option<Arc<SemImpl>> {
    load_global_sem()
}

#[cfg(test)]
mod tests;
