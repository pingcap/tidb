# TiDB Design Documents

- Author(s): [morgo](http://github.com/morgo)
- Discussion PR: https://github.com/pingcap/tidb/pull/30558
- Tracking Issue: https://github.com/pingcap/tidb/issues/30366

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Behavior Changes](#behavior-changes)
* [Documentation Changes](#documentation-changes)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Currently, TiDB has two primary methods of configuration:

- The configuration file (in `.toml` format) ([Docs](https://docs.pingcap.com/tidb/stable/tidb-configuration-file))
- Using MySQL compatible system variables (`SET GLOBAL sysvar=x`, `SET SESSION sysvar=x`) ([Docs](https://docs.pingcap.com/tidb/stable/system-variables))

Both the **semantics** and **naming conventions** differ between these two methods:

1. Configuration file settings only apply to a single TiDB server instance. Making changes to settings managed by the configuration file requires restarting the TiDB server.
2. System variables _natively_ manage settings that have _session_ (connection) or _global_ (cluster-wide) scope. However, some _session_ scoped variables are used to allow instance configuration. This is not a feature that is natively supported, and the usage is often confusing.
3. The configuration file uses a hierarchy of sections such as `[performance]` or `[experimental]`.
4. System variables use a flat naming convention (but often use a prefix of `tidb_feature_XXX`). Thus mapping between the two is not straight forward.

This proposal introduces _native_ support for `INSTANCE` scoped variables, such that system variables offer the superset of functionality of configuration files. It does this using a much simplified implementation from earlier proposals: an individual system variable **must not** permit both `GLOBAL` and `INSTANCE` scope. Implementing this restriction is key to this proposal; since it avoids a confusing set of precedence rules which are hard to understand. For alternative proposals see "investigation and alternatives".

## Motivation or Background

The motivation for this proposal is ease of use and maintainability:

1. It's not clear if each setting should be a configuration file setting or a system variable (we usually choose system variables, but there is no clear rule). After this proposal is implemented, we can make every setting available as a system variable (even if read-only).
2. We are being asked to make additional configuration file settings dynamically configuration (via a system variable). When we do this, the naming is inconsistent. After this proposal is implemented, we can allow the `.toml` file to support an `[instance]` section where the flat named for system variables can be used. This makes it identical to how system variables are configured in MySQL.
3. The current system is hard for users. It's hard to explain the two systems, and hard to explain the differences between them. Often "INSTANCE" scoped system variables are incorrectly documented as `SESSION`, since it's not a native behavior. Explaining how `SESSION` scope is hijacked is difficult for MySQL users to understand, because usually changes in a different session should not affect your session.

## Detailed Design

### Stage 1: Initial Implementation

From a user-oriented point of view, setting an instance scoped sysvar is the same as setting a `GLOBAL` variable:

```sql
SET GLOBAL max_connections=1234;
```

Because `INSTANCE` and `GLOBAL` are mutually exclusive, the only semantic difference is that changes to `INSTANCE` scoped variables are not persisted and are not propagated to other TiDB servers.

From a sysvar framework perspective the changes required are quite minimal. A new scope is added, and validating if setting permits `GLOBAL` scope also permits `INSTANCE` scope:

```
+++ b/sessionctx/variable/sysvar.go
@@ -56,6 +56,8 @@ const (
        ScopeGlobal ScopeFlag = 1 << 0
        // ScopeSession means the system variable can only be changed in current session.
        ScopeSession ScopeFlag = 1 << 1
+       // ScopeInstance means it is similar to global but doesn't propagate to other TiDB servers.
+       ScopeInstance ScopeFlag = 1 << 2
 
        // TypeStr is the default
        TypeStr TypeFlag = 0
@@ -257,6 +259,11 @@ func (sv *SysVar) HasGlobalScope() bool {
        return sv.Scope&ScopeGlobal != 0
 }
 
+// HasInstanceScope returns true if the scope for the sysVar includes global or instance
+func (sv *SysVar) HasInstanceScope() bool {
+       return sv.Scope&ScopeInstance != 0
+}
+
 // Validate checks if system variable satisfies specific restriction.
 func (sv *SysVar) Validate(vars *SessionVars, value string, scope ScopeFlag) (string, error) {
        // Check that the scope is correct first.
@@ -313,7 +320,7 @@ func (sv *SysVar) validateScope(scope ScopeFlag) error {
        if sv.ReadOnly || sv.Scope == ScopeNone {
                return ErrIncorrectScope.FastGenByArgs(sv.Name, "read only")
        }
-       if scope == ScopeGlobal && !sv.HasGlobalScope() {
+       if scope == ScopeGlobal && !(sv.HasGlobalScope() || sv.HasInstanceScope()) {
```

The session package handles loading/saving `GLOBAL` variable values. It will need minor changes to make `INSTANCE` Scope a noop operation (since persistence is not supported, and in the initial implementation a `GetGlobal()` function will be used to retrieve the instance value:

```
+++ b/session/session.go
@@ -1103,6 +1103,10 @@ func (s *session) GetGlobalSysVar(name string) (string, error) {
                return "", variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
        }
 
+       if sv.HasInstanceScope() { // has INSTANCE scope only, not pure global
+               return "", errors.New("variable has only instance scope and no GetGlobal func. Not sure how to handle yet.")
+       }
+
        sysVar, err := domain.GetDomain(s).GetGlobalVar(name)
        if err != nil {
                // The sysvar exists, but there is no cache entry yet.
@@ -1121,6 +1125,7 @@ func (s *session) GetGlobalSysVar(name string) (string, error) {
 }
 
 // SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
+// it is used for setting instance scope as well.
 func (s *session) SetGlobalSysVar(name, value string) (err error) {
        sv := variable.GetSysVar(name)
        if sv == nil {
@@ -1132,6 +1137,9 @@ func (s *session) SetGlobalSysVar(name, value string) (err error) {
        if err = sv.SetGlobalFromHook(s.sessionVars, value, false); err != nil {
                return err
        }
+       if sv.HasInstanceScope() { // skip for INSTANCE scope
+               return nil
+       }
        if sv.GlobalConfigName != "" {
                domain.GetDomain(s).NotifyGlobalConfigChange(sv.GlobalConfigName, variable.OnOffToTrueFalse(value))
        }
@@ -1148,6 +1156,9 @@ func (s *session) SetGlobalSysVarOnly(name, value string) (err error) {
        if err = sv.SetGlobalFromHook(s.sessionVars, value, true); err != nil {
                return err
        }
+       if !sv.HasInstanceScope() { // skip for INSTANCE scope
+               return nil
+       }
        return s.replaceGlobalVariablesTableValue(context.TODO(), sv.Name, value)
 }
```

A "non-native" `INSTANCE` variable can be changed to a native one as follows:

```
+       {Scope: ScopeInstance, Name: TiDBGeneralLog, Value: BoolToOnOff(DefTiDBGeneralLog), Type: TypeBool, skipInit: true, SetGlobal: func(s *SessionVars, val string) error {
                ProcessGeneralLog.Store(TiDBOptOn(val))
                return nil
-       }, GetSession: func(s *SessionVars) (string, error) {
+       }, GetGlobal: func(s *SessionVars) (string, error) {
                return BoolToOnOff(ProcessGeneralLog.Load()), nil
        }},
```

This introduces a compatibility issue, since users who have previously configured instance scope with `SET [SESSION] tidb_general_log = x` will receive an error because the scope is wrong. This can be fixed by adding a legacy mode to the sysvar framework which allows `INSTANCE` scoped variables to be set with either `SET GLOBAL` or `SET SESSION`:

```sql
SET GLOBAL tidb_enable_legacy_instance_scope = 1;
```

We can default to `TRUE` for now, but should revisit this in the future.

### Stage 2: Mapping All Configuration File Settings to System Variables

The second stage is to map all configuration file settings to system variable names. In MySQL any configuration file setting is also available as a system variable (even if it is read only), which makes it possible to get the configuration of the cluster with `SHOW GLOBAL VARIABLES`. Once this step is complete, it should also be possible to offer an `[instance]` section of the configuration file which accepts the system variable names (and finally consistent naming between the two systems). We can then modify the example configuration files to be based on the flat `[instance]` configuration:

```
# tidb.toml
[instance]
max_connections = 1234
tidb_general_log = "/path/to/file"
```

The default will be that we map variables as read-only, so we don't need to think about the specific cases (i.e. you can't easily change the socket or port). This helps us achieve completeness first, and then we can then evaluate which ones can be made dynamic.

The mapping system should consider all current instance variable mappings such as `log.enable-slow-log` to `tidb_enable_slow_log`. The earlier instance scoped variables proposal has an [example mapping table](https://docs.google.com/document/d/1RuajYAFVsjJCwCBpIYF--9jtgxDZZcz3cbP-iVuLxJI/edit?n=2020-09-15_Design_Doc_for_Instance_Variables#) which can be used as a guideline for how to create new system variable names.

Because variables can be configured through either an "sysvar name" (under `[instance]`) or a hierarchical name, we will need to decide which is the alias versus the actual name (the actual name appears in results like `SELECT * FROM INFORMATION_SCHEMA.CLUSTER_CONFIG`). I propose for this stage the original name is still the source of truth, but in refactoring (stage 3) we switch it.

### Stage 3: Refactoring

The use of a `GetGlobal()` and `SetGlobal()` func for each instance scoped system variable is not ideal. It is possible to refactor the system variable framework so that instance scope is stored in a map, and the values are updated automatically by `SET GLOBAL` on an instance scoped variable. On startup, as the configuration file is parsed it will update the values in the map. This seems like a better approach than the current use of Setters/Getters, and because there is a prescribed way of doing it we can correctly handle the data races that are common with our current incorrect usage of calling `config.GetGlobalConfig()`.

Thus, the source of truth for instance scoped variables moves from the `config` package to another part of the server (likely `domain`). See [issue #30366](https://github.com/pingcap/tidb/issues/30366).

Because at this stage the source of truth is now no longer the `config` package, we will also need to decide how to handle features like `INFORMATION_SCHEMA.CLUSTER_CONFIG`. If it refers to the config file, it will not necessarily reflect the current configuration of the cluster. Because every instance setting will now have a system variable name (which becomes the unified name), I recommend that we deprecate `CLUSTER_CONFIG` for TiDB. We can change `CLUSTER_CONFIG` to read from the new source of truth and maintain both for some versions to support upgrades.

## Behavior Changes

There are **very few** known scenarios where a system variable is *currently both* instance and global scoped, but each of these will require behavior changes. Here is a script to detect them:

```
package main

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func main() {
	for _, v := range variable.GetSysVars() {
		if v.HasGlobalScope() && v.HasSessionScope() && (v.GetSession != nil || v.SetSession != nil) && (v.GetGlobal != nil || v.SetGlobal != nil) {
			fmt.Printf("%s\n", v.Name)
		}
	}
}
```

**Output:**

```
tidb_store_limit
tidb_stmt_summary_internal_query
tidb_enable_stmt_summary
tidb_stmt_summary_max_sql_length
tidb_stmt_summary_max_stmt_count
tidb_capture_plan_baselines
tidb_stmt_summary_refresh_interval
tidb_stmt_summary_history_size
```

Changes can be grouped into the following:

1. `tidb_store_limit`: This has now been converted to global-only, see [issue #30515 (merged)](https://github.com/pingcap/tidb/issues/30515).
2. `tidb_stmt_summary_XXX`, `tidb_enable_stmt_summary` and `tidb_capture_plan_baselines` (features work together): The recommendation discussed with the feature maintainers is to convert to global only.

The change to `tidb_store_limit` is unlikely to affect users, since the feature was not working correctly. However, the change to statement summary and capture plan baselines is a behavior change which might affect some users.

## Documentation Changes

For the purposes of user-communication we don't need to use the terminology `INSTANCE`. We will remove more of the confusion by instead referring to the difference between these as whether it "persists to [the] cluster". This also aligns closer to the syntax that users will be using. Consider the following example changes which are easier to read (the current text also doesn't actually explain that to set an `INSTANCE` variable you must use `SET SESSION`, so it actually communicates a lot more in fewer words):

```
+++ b/system-variables.md
@@ -6,13 +6,11 @@ aliases: ['/tidb/dev/tidb-specific-system-variables','/docs/dev/system-variables
 
 # System Variables
 
-TiDB system variables behave similar to MySQL with some differences, in that settings might apply on a `SESSION`, `INSTANCE`, or `GLOBAL` scope, or on a scope that combines `SESSION`, `INSTANCE`, or `GLOBAL`.
+TiDB system variables behave similar to MySQL, in that settings apply on a `SESSION` or `GLOBAL` scope:
 
-- Changes to `GLOBAL` scoped variables **only apply to new connection sessions with TiDB**. Currently active connection sessions are not affected. These changes are persisted and valid after restarts.
-- Changes to `INSTANCE` scoped variables apply to all active or new connection sessions with the current TiDB instance immediately after the changes are made. Other TiDB instances are not affected. These changes are not persisted and become invalid after TiDB restarts.
-- Variables can also have `NONE` scope. These variables are read-only, and are typically used to convey static information that will not change after a TiDB server has started.
-
-Variables can be set with the [`SET` statement](/sql-statements/sql-statement-set-variable.md) on a per-session, instance or global basis:
+- Changes on a `SESSION` scope will only affect the current session.
+- Changes on a `GLOBAL` scope apply immediately, provided that the variable is not also `SESSION` scoped. In which case all sessions (including your session) will continue to use their current session value.
+- Changes are made using the [`SET` statement](/sql-statements/sql-statement-set-variable.md):
 
 ```sql
 # These two identical statements change a session variable
@@ -26,9 +24,9 @@ SET  GLOBAL tidb_distsql_scan_concurrency = 10;
 
 > **Note:**
 >
-> Executing `SET GLOBAL` applies immediately on the TiDB server where the statement was issued. A notification is then sent to all TiDB servers to refresh their system variable cache, which will start immediately as a background operation. Because there is a risk that some TiDB servers might miss the notification, the system variable cache is also refreshed automatically every 30 seconds. This helps ensure that all servers are operating with the same configuration.
+> Several `GLOBAL` variables persist to the TiDB cluster. For variables that specify `Persists to Cluster: Yes` a notification is sent to all TiDB servers to refresh their system variable cache when the global variable is changed. Adding additional TiDB servers (or restarting existing TiDB servers) will automatically use the persisted configuration value. For variables that specify `Persists to Cluster: No` any changes only apply to the local TiDB instance that you are connected to. In order to retain any values set, you will need to specify them in your `tidb.toml` configuration file.
 >
-> TiDB differs from MySQL in that `GLOBAL` scoped variables **persist** through TiDB server restarts. Additionally, TiDB presents several MySQL variables as both readable and settable. This is required for compatibility, because it is common for both applications and connectors to read MySQL variables. For example, JDBC connectors both read and set query cache settings, despite not relying on the behavior.
+> Additionally, TiDB presents several MySQL variables as both readable and settable. This is required for compatibility, because it is common for both applications and connectors to read MySQL variables. For example, JDBC connectors both read and set query cache settings, despite not relying on the behavior.
 
 > **Note:**
 >
@@ -47,6 +45,7 @@ SET  GLOBAL tidb_distsql_scan_concurrency = 10;
 ### allow_auto_random_explicit_insert <span class="version-mark">New in v4.0.3</span>
 
 - Scope: SESSION | GLOBAL
+- Persists to cluster: Yes
 - Default value: `OFF`
 - Determines whether to allow explicitly specifying the values of the column with the `AUTO_RANDOM` attribute in the `INSERT` statement.
 
@@ -166,7 +165,8 @@ mysql> SELECT * FROM t1;
 
 ### ddl_slow_threshold
 
-- Scope: INSTANCE
+- Scope: GLOBAL
+- Persists to cluster: No
 - Default value: `300`
 - Unit: Milliseconds
 - Log DDL operations whose execution time exceeds the threshold value.
```

What this does mean is that each variable that includes `GLOBAL` or `INSTANCE` scope needs a new line added in Docs. But this can be [auto-generated](https://github.com/pingcap/docs/pull/5720) from the sysvar source code.

## Impacts & Risks

The biggest risk is that we can not agree on a reduced scope of implementation and the project extends to cover increased scope. Configuration management is a classic example of a [bikeshed problem](https://en.wikipedia.org/wiki/Law_of_triviality), and we will likely need to make some tradeoffs to get anywhere.

Many of the alternatives are difficult to implement because they break compatibility (we need to live with `GLOBAL` means cluster `GLOBAL`, and there is no `SET CLUSTER`) or they break rolling upgrade scenarios because we currently have no specific rules of the minimum version of TiDB which can be upgraded from.

## Investigation & Alternatives

- There is no equivalent functionality to reference in MySQL since it does not have the native concept of a cluster _in the context of configuration_.
- An [earlier proposal](https://docs.google.com/document/d/1RuajYAFVsjJCwCBpIYF--9jtgxDZZcz3cbP-iVuLxJI/edit) for `INSTANCE` scope, with rules for precedence (same author)
- CockroachDB has cluster level settings and node-level settings. Most settings are cluster level, and [node-level](https://www.cockroachlabs.com/docs/v21.2/cockroach-start) needs to be parsed as arguments when starting the server.

### SET INSTANCE syntax

A small behavior change (still with the restriction that `INSTANCE` and `GLOBAL` are mutually exclusive) proposes adding `SET INSTANCE` as explicit syntax to set an instance-scoped variable. This would require the following modifications to be complete:
- `SHOW INSTANCE VARIABLES` will need to return variables that have `NONE` or `INSTANCE` scope (and `SHOW VARIABLES` continues to return all variables).
- The parser will need to support `SET INSTANCE` syntax and the `@@instance.variable` syntax
- Various parser ast code will also need modifying because a boolean can no longer reflect the scope.

The advantage of this proposal is that the documentation and usage is clearer. The disadvantage is that because MySQL does not have an instance scope (global scope in MySQL is instance-like) it could create strange behaviors with compatibility sysvars which are implemented. This **only** affects `INSTANCE` variables which are *not already* `ScopeNone` (so Port, Socket, GrantTables, LogBin are not affected). The only currently known example that is affected is `max_connections`, which is currently a noop in system variables, but available in the configuration file as `max-server-connections` (it is presumed it would be [mapped](https://docs.google.com/document/d/1RuajYAFVsjJCwCBpIYF--9jtgxDZZcz3cbP-iVuLxJI/edit?n=2020-09-15_Design_Doc_for_Instance_Variables#) to `max_connections` in Stage 2).

This means that `SET GLOBAL max_connections` would need to return an error in TiDB, because the correct syntax is `SET INSTANCE max_connections`. There is a known failover use-case to execute `SET GLOBAL max_connections = 1` + `SET GLOBAL [super_]read_only = 1` to disable writes. However, this already does not work in TiDB as there is no support for `SET GLOBAL [super_]read_only=1`. Thus, removing this compatibility does not appear to be a blocking issue.

However, the IBG support team rejects this proposal based on the expectation that there will be further tool compatibility issues not yet discovered. As MySQL does not have `SET INSTANCE` scope, users will also not be used to using this syntax.

## Unresolved Questions

- None