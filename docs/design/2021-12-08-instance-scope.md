# TiDB Design Documents

- Author(s): [morgo](http://github.com/morgo)
- Discussion PR: https://github.com/pingcap/tidb/pull/30558
- Tracking Issue: https://github.com/pingcap/tidb/issues/30366

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

Currently, TiDB has two primary methods of configuration:

- The configuration file (in `.toml` format) ([Docs](https://docs.pingcap.com/tidb/stable/tidb-configuration-file))
- Using MySQL compatible system variables (`SET GLOBAL sysvar=x`, `SET SESSION sysvar=x`) ([Docs](https://docs.pingcap.com/tidb/stable/system-variables))

Both the **semantics** and **naming conventions** differ between these two methods:

1. Configuration file settings only apply to a single TiDB server instance. Making changes to settings managed by the configuration file requires restarting TiDB server.
2. System variables _natively_ manage settings that have _session_ (connection) or _global_ (cluster-wide) scope. However, some _session_ scoped variables are used to allow instance configuration. This is not a feature that is natively supported, and the usage is often confusing.
3. The configuration file uses a heirachy of sections such as `[performance]` or `[experimental]`.
4. System variables use are flat naming convention (but often use a prefix of `tidb_feature_XXX`). Thus mapping between the two is not straight forward.

This proposal introduces _native_ support for `INSTANCE` scoped variables, such that system variables offer the superset of functionality of configuration files. It does this using a much simplified implementation from earlier proposals: an individual system variable **must not** permit both `GLOBAL` and `INSTANCE` scope. Implementing with this restriction is key to this proposal; since it avoids a confusing set of precedence rules which are hard to understand. For alternative proposals see "investigation and alternatives".

## Motivation or Background

The motivation for this proposal is ease of use and maintainability:

1. It's not clear if each setting should be a configuration file setting or a system variable (we usually chose system variable, but there is no clear rule). After this proposal is implemented, we can make every setting available as a system variable (even if read-only).
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

The session package handles loading/saving GLOBAL variable values. It will need minor changes to make Instance Scope a noop operation (since persistence is not supported, and in the initial implementation a `GetGlobal()` function will be used to retrieve the instance value:

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

### Stage 3: Refactoring

The use of a `GetGlobal()` and `SetGlobal()` func for each instance scoped system variable is not ideal. It is possible to refactor the system variable framework so that instance scope is stored in a map, and the values are updated automatically by `SET GLOBAL` on an instance scoped variable. On startup, as the configuration file is parsed it will update the values in the map. This seems like a better approach than the current use of Setters/Getters, and because there is a prescribed way of doing it we can correctly handle the data races that are common with our current incorrect usage of calling `config.GetGlobalConfig()`.

Thus, the source of truth for instance scoped variables moves from the `config` package to another part of the server (likely `domain`). See [issue #30366](https://github.com/pingcap/tidb/issues/30366).

## Impacts & Risks

Biggest risk is that we can not agree on a reduced scope of implementation and the project extends to cover increased scope. Configuration management is a classic example of a [bikeshed problem](https://en.wikipedia.org/wiki/Law_of_triviality), and we will likely need to make some tradeoffs to get anywhere.

Many of the alternatives are difficult to implement because they break compatibility (we need to live with `GLOBAL` means cluster `GLOBAL`, and there is no `SET CLUSTER`) or they break rolling upgrade scenarios because we currently have no specific rules of the minimum version of TiDB which can be upgraded from.

## Investigation & Alternatives

- There is no equiverlant functionality to reference in MySQL since it does not have the native concept of a cluster.
- An [earlier proposal](https://docs.google.com/document/d/1RuajYAFVsjJCwCBpIYF--9jtgxDZZcz3cbP-iVuLxJI/edit) for `INSTANCE` scope, with rules for precedence (same author)
- CockroachDB has cluster level settings and node-level settings. Most settings are cluster level, and [node-level](https://www.cockroachlabs.com/docs/v21.2/cockroach-start) needs to be parsed as arguments when starting the server.

## Unresolved Questions

There are **very few** known scenarios where a system variable is *currently both* instance and global scoped, but these individually would need to be handled:

```
package main

import (
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
)

func main() {
	for _, v := range variable.GetSysVars() {
		if v.HasGlobalScope() && v.HasSessionScope() && (v.GetSession != nil || v.SetSession != nil) && (v.SetGlobal != nil || v.SetGlobal != nil) {
			fmt.Printf("%s\n", v.Name)
		}
	}
}
```

Output:

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

The following suggestions are provided (to be discussed):

- `tidb_store_limit`: Suggestion is to convert to global only ([issue #30515](https://github.com/pingcap/tidb/issues/30515))
- `tidb_stmt_summary_XXX`: Suggestion is to convert to global only.
- `tidb_enable_stmt_summary`: Convert to instance scope only.
- `tidb_capture_plan_baselines`: Convert to instance scope only. 

Thus, turning on statement summary is a per-server decision, but the configuration for statement summary is a per-cluster decision.
