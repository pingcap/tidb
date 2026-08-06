# The session/variable layer against Go: divergence inventory

A file-by-file semantic comparison of

| Rust | Go |
| --- | --- |
| `rust/crates/tidb-session/src/sysvar.rs` + `sysvar/catalog/*.rs` (948 entries) | `pkg/sessionctx/variable/sysvar.go` (`defaultSysVars`) + `noop.go` (`noopSysVars`) |
| `rust/crates/tidb-session/src/vars.rs` (`GlobalSysvars`, `SessionVars`) | `pkg/sessionctx/variable/session.go`, `variable.go`, `varsutil.go` |
| `rust/crates/tidb-session/src/variables.rs` (`SET` dispatch) | `pkg/executor/set.go` `setSysVariable` |
| `rust/crates/tidb-session/src/stmt_ctx.rs` | `pkg/sessionctx/stmtctx/stmtctx.go`, `pkg/executor/select.go` `ResetContextOfStmt` |
| `rust/crates/tidb-vardef/src/*` | `pkg/sessionctx/vardef/*` |

Method: the Go registry was re-parsed from source (both slices, resolving
`vardef`/`mysql` constant references) and diffed field-by-field against the
Rust catalog parsed from its own source. Constants that Go computes at init
(`strconv.Itoa(config...)`, `GetDefault*()`) are marked below as unverifiable
from source alone.

The behavioral census is independently executable:

```sh
LC_ALL=en_US.UTF-8 LANG=en_US.UTF-8 \
  ruby -EUTF-8:UTF-8 rust/difftests/tools/sysvar-census.rb
```

The script parses every `SysVarDef`, locates direct `get_system`/`get_global`
consumers, checks source evidence for dynamic helper and special-hook readers,
and then classifies the remainder. It deliberately does not count
`SELECT @@x` or `SHOW VARIABLES`: those generic paths can echo every stored
entry but do not make a variable affect SQL, planning, execution, protocol, or
SET semantics.

---

## Counts

| | |
| --- | --- |
| Variables in Go's registry | 948 (515 `defaultSysVars` written as literals + 10 built by `newExecConcurrencySysVar` + 423 `noopSysVars`) |
| Variables in the Rust catalog | 948 |
| Name-set difference | **0** — every name matches exactly, and every Rust name is lowercase |
| Declarative fields compared (scope, type, default, read-only, min, max, allow-auto, auto-convert-negative-bool) across the 938 source-resolvable entries | **0 divergences** |
| `PossibleValues` divergences | **2** (both fixed in this change) |
| Runtime-behavior consumers | **42** |
| SET/validation-only consumers | **16** |
| Behaviorally unread entries | **890** |
| Writable but behaviorally unread entries | **730** |
| Read-only or scope-none and behaviorally unread | **160** |
| Go `Validation: func` occurrences | **95**; Rust models **15** unique variables |
| Go `SetSession: func` occurrences | **278**; a small subset modelled |

The quoted census output is:

```text
census: declared=948 runtime_behavior=42 set_or_validation_only=16 behaviorally_unread=890 sum=948
writability: writable_declared=785 writable_behaviorally_unread=730 read_only_or_scope_none_unread=160
```

The raw Go hook counts come from source occurrence counts:

```text
$ rg -o 'Validation:\s*func' pkg/sessionctx/variable/{sysvar.go,noop.go} | wc -l
95
$ rg -o 'SetSession:\s*func' pkg/sessionctx/variable/{sysvar.go,noop.go} | wc -l
278
```

The old statement "901 of 948 stored-never-read, 34 wired" cannot be
reproduced from this repository's history or current source. It also mixed
three different states: a variable that changes runtime behavior, a variable
read only while validating another `SET`, and a variable that is merely
accepted and echoed. The three-way census above replaces it.

The 42 runtime names are printed by the script. The 16 SET/validation-only
names are `offline_mode`, `read_only`, `super_read_only`,
`tidb_capture_plan_baselines`, `tidb_enable_fast_analyze`,
`tidb_enable_legacy_instance_scope`, `tidb_enable_list_partition`,
`tidb_enable_table_partition`, `tidb_prepared_plan_cache_size`,
`tidb_session_alias`, `tidb_session_plan_cache_size`,
`tidb_skip_isolation_level_check`, `transaction_isolation`,
`transaction_read_only`, `tx_isolation`, and `tx_read_only`.

---

## Highest-value oracle blind spots

1. **Error code and text are never compared when both engines reject.**
   `mysqltest_script::Stmt` stores only `expect_error: bool`; the parser drops
   the code after `--error`. Then `integration_diff::compare_output` takes
   `(Err(_), true)`, records `BothRejected`, and discards both Rust's error and
   TiDB's recorded `Error ...` line. The current replay prints:

   ```text
   integrationtest replay over 110 topics: 8234 of 11465 statements compared
   skips by class: {"BothRejected": 715, "OutOfDomain": 2386, ...}
   ```

   All 715 are therefore agreement only on rejection. A wrong errno, SQLSTATE,
   error argument, or message still passes. The next harness investment should
   preserve the `--error` tokens, parse the recorded error line, and compare at
   least errno before making message comparison a second ratchet.

2. **Warnings are compared on only 62 statements.** The current named gate
   prints:

   ```text
   warning gate reaches 62 of 11465 statements across 110 topics
   ```

   Statements outside those 62 can add, lose, reorder, or change warnings
   without affecting replay. The recordings do not contain those warnings,
   so closing this needs a live-server/wire oracle or new recordings rather
   than a more permissive reader.

3. **730 writable variables can be set and read back without affecting
   behavior.** The highest-value client-facing entries are
   `character_set_client`, `max_execution_time`, `transaction_isolation` /
   `tx_isolation` (their refusal/warning contract is wired, execution is not),
   `tidb_replica_read`, `tidb_retry_limit`,
   `tidb_disable_txn_auto_retry`, `tidb_max_chunk_size`,
   `tidb_init_chunk_size`, `tidb_request_source_type`, and
   `tidb_scatter_region`. This is a product gap and an oracle gap together:
   generic `SELECT @@x` coverage proves storage while masking the absent
   consumer.

---

## Ranked findings

### Rank 1 — a switch that is never read, or a value the engine cannot reach

#### F1. All 28 `ScopeInstance` variables were unsettable and unreadable — **FIXED**

* Go: `pkg/sessionctx/variable/variable.go:265` — `validateScope` accepts
  `ScopeGlobal` when `sv.HasGlobalScope() || sv.HasInstanceScope()`;
  `pkg/executor/set.go:153` converts an *unqualified* `SET` on an
  instance-scoped variable into an instance set (with warning
  `ErrInstanceScope`, `pkg/errno/errname.go:1058`, "modifying %s will require
  SET GLOBAL in a future version of TiDB") because
  `DefEnableLegacyInstanceScope = true` (`pkg/sessionctx/vardef/tidb_vars.go:1650`).
* Rust: `rust/crates/tidb-session/src/vars.rs:120` (`GlobalSysvars::set`) gates
  on `has_global_scope()` alone; `vars.rs:334` (`SessionVars::set_system`) gates
  on `has_session_scope()` alone; `vars.rs:312` (`get_global`) gates on
  `has_global_scope()`. `SysVarDef::has_instance_scope` (`sysvar.rs:126`) has
  **no caller anywhere in the workspace**.
* Distinguishing case:
  ```sql
  SET GLOBAL tidb_general_log = 1;
  ```
  Go: succeeds, general log turns on. Rust: `ErrLocalVariable` (1228)
  "Variable 'tidb_general_log' is a SESSION variable and can't be used with SET
  GLOBAL" — the exact opposite of the truth.
  ```sql
  SET tidb_general_log = 1;
  ```
  Go: succeeds with warning 8154. Rust: `ErrGlobalVariable` (1229).
  `SELECT @@global.tidb_general_log` likewise errors in Rust.
* Affected (28): `ddl_slow_threshold`, `max_connections`,
  `plugin_audit_log_buffer_size`, `plugin_audit_log_flush_interval`,
  `plugin_dir`, `plugin_load`, `tidb_check_mb4_value_in_utf8`, `tidb_config`,
  `tidb_enable_collect_execution_info`, `tidb_enable_ddl`,
  `tidb_enable_slow_log`, `tidb_enable_stats_owner`,
  `tidb_expensive_query_time_threshold`, `tidb_expensive_txn_time_threshold`,
  `tidb_force_priority`, `tidb_general_log`, `tidb_log_file_max_days`,
  `tidb_pprof_sql_cpu`, `tidb_rc_read_check_ts`, `tidb_record_plan_in_slow_log`,
  `tidb_service_scope`, `tidb_slow_log_threshold`,
  `tidb_stmt_summary_enable_persistent`, `tidb_stmt_summary_file_max_backups`,
  `tidb_stmt_summary_file_max_days`, `tidb_stmt_summary_file_max_size`,
  `tidb_stmt_summary_filename`, `tidb_trace_event`.
* Note that `max_connections` being in this set means a client cannot even read
  `@@global.max_connections`, which some drivers do at connect.
* **FIXED.** The audit's warning was the design constraint: relaxing the
  set-guard alone would have stored a value `get_system` never consults, which
  is a silent no-op — worse than the loud wrong error. So the TIER came first:
  `GlobalSysvars` now holds a second map (`instances`) beside `values`, and one
  private selector `GlobalSysvars::store(def)` picks between them for every
  read AND every write, so a value cannot be written where no reader looks.
  * Which map: a variable with GLOBAL scope at all is cluster state (the six
    `ScopeGlobal|ScopeInstance` entries stay in `values`); the 28 instance-only
    ones are node-local. `overrides()` — which feeds cluster persistence and
    the connect-time session seed — still returns `values` alone, so an
    instance value is never offered to the cluster writer.
  * Guards relaxed afterwards: `set` admits `has_global_scope() ||
    has_instance_scope()` (Go `validateScope`), `get_system` falls through to
    the node-wide tier when `!has_session_scope() && (global || instance)`, and
    `get_global` answers for an instance-scoped variable because Go's read path
    does not run `validateScope` (`SELECT @@global.max_connections` works).
  * Legacy routing: `variables.rs::routes_to_instance_tier` reproduces
    `set.go:152` — a SESSION/unqualified `SET` on an instance-scoped variable
    is rewritten to an instance set and warned with `ErrInstanceScope`, whose
    code is **8142** (`pkg/errno/errcode.go:1063`), not 8154. With
    `tidb_enable_legacy_instance_scope = OFF` the rewrite is skipped and the
    write falls through to `set_system`'s `!has_session_scope()` guard, which
    is Go's 1229. `SET INSTANCE` is accepted directly and warns about nothing.
  * The warning goes through `Session::append_warning`, the single door that
    feeds both `SHOW WARNINGS` and the OK packet's `wire_warning_count`.
  * Pinned in `tests_global_vars.rs`, including the mutation probe that a
    genuinely SESSION-only variable still refuses `SET GLOBAL` with 1228.

#### F2. `tidb_enable_clustered_index` — **FIXED**, now a DDL behavior reader

* Go: `pkg/sessionctx/variable/sysvar.go:2782`, `TypeEnum`,
  `PossibleValues {OFF, ON, INT_ONLY}`, default `ON`, plus a `SetSession`
  closure writing `SessionVars.EnableClusteredIndex`, read by the DDL table
  builder to decide whether a table's PK becomes the row handle.
* Rust now reads the enum in `Session::clustered_index_mode`
  (`tidb-session/src/stmt_ctx.rs`) and passes the resulting
  `ClusteredIndexDefMode` through DDL table construction. `OFF`, `ON`, and
  `INT_ONLY` keep their three-way Go meaning; this is one of the 42 runtime
  consumers in the fresh census.
* Distinguishing case:
  ```sql
  SET @@tidb_enable_clustered_index = 'OFF';
  CREATE TABLE t (a VARCHAR(10) PRIMARY KEY, b INT);
  SHOW CREATE TABLE t;
  ```
  Go and Rust now both print
  `PRIMARY KEY (a) /*T![clustered_index] NONCLUSTERED */`, and use
  `_tidb_rowid` as the handle.
* Remaining gap: Go's deprecation warning around `INT_ONLY` is still one of
  the unmodelled `Validation` closures.

#### F3. `max_allowed_packet` — SET and expression sizing fixed; wire limit remains node config

* Go: `pkg/sessionctx/variable/sysvar.go:2193`. Its `Validation` closure defines
  the two behaviors Rust originally lacked:
  1. `SET SESSION max_allowed_packet = ...` is **refused**:
     `ErrReadOnly.GenWithStackByArgs("SESSION", "max_allowed_packet", "GLOBAL")`
     (1621, "SESSION variable 'max_allowed_packet' is read-only. Use SET GLOBAL
     to assign the value").
  2. The accepted global value is truncated **down to a multiple of 1024** with
     `ErrTruncatedWrongValue`.
* Rust originally accepted the session write and used the 64 MiB default for
  all result-sizing builtins.
* Distinguishing case:
  ```sql
  SET SESSION max_allowed_packet = 1048576;
  ```
  Go and Rust now both answer `ERROR 1621 (HY000)`.
  ```sql
  SET GLOBAL max_allowed_packet = 1025; SELECT @@global.max_allowed_packet;
  ```
  Go and Rust now both store `1024` and append warning 1292.
* **Both SET behaviours FIXED.** The 1024-rounding is in
  `SysVarDef::run_validation`, where the existing `truncated` flag already
  carries `ErrTruncatedWrongValue` (1292) naming the value as TYPED; the
  SESSION refusal is `variables.rs::check_max_allowed_packet_scope` →
  `VarErrorKind::SessionScopeIsReadOnly` → 1621. The refusal cannot be a scope
  bit: the variable still HAS session scope for READING, and only the write is
  refused.
* **The SQL behavior reader is now fixed.** `Session::statement_context` reads
  the session copy and passes it through `StmtContext::with_max_allowed_packet`,
  which `SPACE()` and the other result-sizing builtins consume. A global write
  affects a newly seeded session, not the current session, matching Go.
* **The network packet limit is still node configuration, not the sysvar.**
  `PacketReader` already accepts a per-connection limit, and `tidb-server`
  supplies `NodeConfig::max_allowed_packet`; a later `SET GLOBAL` does not
  reconfigure an existing connection's packet reader or long-data buffer.
* Go's third refusal, `errSetGlobalMaxAllowedPacket` in starter deployments
  (`deploymode.IsStarter()`), has no deployment mode to key off in this tier
  and is not modelled.

#### F4. Transaction-retry variables: accepted, stored, never read — and one is stored wrong

* Go: `tidb_retry_limit` (`sysvar.go:2581`, `TypeInt`, default `10`, min `-1`)
  and `tidb_disable_txn_auto_retry` (`sysvar.go:2585`, `TypeBool`, default `ON`).
* Rust: `tidb-vardef/src/tidb_vars.rs:136` and `:140` define the names; **no
  consumer** in any crate.
* `tidb_disable_txn_auto_retry` additionally has a validation contract Rust
  inverts. Go's closure (`sysvar.go:2586-2591`) **always returns `vardef.On`**,
  warning `errWarnDeprecatedSyntax` when the assignment was `OFF`:
  ```sql
  SET @@tidb_disable_txn_auto_retry = OFF; SELECT @@tidb_disable_txn_auto_retry;
  ```
  Go: `ON`, with a deprecation warning. Rust: `OFF`, no warning.
  This is the same shape as `tidb_enable_table_partition`, which *is* modelled
  in `sysvar.rs:334` — the pattern exists, this variable was missed.

#### F5. `tidb_max_chunk_size` / `tidb_init_chunk_size` — accepted, stored, never read

* Go: `sysvar.go:2504` and `:2520`; `SetSession` writes
  `SessionVars.MaxChunkSize` / `InitChunkSize`, which every executor's chunk
  allocator reads.
* Rust: names defined at `tidb-vardef/src/tidb_vars.rs:473` and `:540`; the only
  non-catalog occurrences are membership entries in
  `rust/crates/tidb-exec/src/hint_updatable_vars.rs:90,97` (a list of names
  `SET_VAR` may target), which does not make anything read the value.
* Distinguishing case: `SET tidb_max_chunk_size = 32; SELECT * FROM big_table;`
  — Go returns rows in 32-row chunks (observable through
  `EXPLAIN ANALYZE` execution info and through memory tracking); Rust's chunk
  size is unchanged.

#### F6. Isolation level: SET validation fixed, transaction behavior still unread

* Go: `pkg/sessionctx/variable/varsutil.go:116` `checkIsolationLevel` — both
  values raise `ErrUnsupportedIsolationLevel` (8048) **unless**
  `tidb_skip_isolation_level_check` is ON, in which case they are accepted with
  that error as a *warning*. Wired to both spellings at `sysvar.go:2100`
  (`tx_isolation`) and `:2106` (`transaction_isolation`).
* Rust now validates both spellings in `variables.rs::check_isolation_level`,
  including the `tidb_skip_isolation_level_check` warning downgrade.
* Distinguishing case:
  ```sql
  SET SESSION transaction_isolation = 'SERIALIZABLE';
  ```
  Go and Rust now both raise 8048 unless the skip switch is on.
* **FIXED** in `variables.rs::check_isolation_level`, which is where both
  halves of Go's closure can reach what they need: the skip switch is read
  from the session and the downgraded warning is appended to it. The refusal
  is `VarErrorKind::UnsupportedIsolationLevel` (8048). Go tests the
  *normalized* value, so the ordinal and lower-case spellings are refused with
  it; the two accepted levels store and read back unchanged, on both spellings
  of the variable name. Pinned in `tests_global_vars.rs`.
* Still open, and deliberately not faked: nothing downstream reads the stored
  level. `tidb-exec/src/isolation_state.rs` is now reachable from a correct
  SET, but the executor does not consult it — a `READ-COMMITTED` session still
  runs at the tier's one isolation. That is why both spellings are classified
  as SET/validation-only rather than runtime behavior.

### Rank 2 — validation contract inverted or absent

#### F7. `tidb_request_source_type` enum lost its zeroth value (ordinals shifted)  — **FIXED**

* Go: `sysvar.go:465`, `TypeEnum`, `PossibleValues: tikvcliutil.ExplicitTypeList`
  = `["", "lightning", "br", "dumpling", "background", "ddl", "stats", "import"]`
  (`client-go@v2.0.8-0.20260708122311.../util/request_source.go:58`), default `""`.
* Rust before this change:
  `rust/crates/tidb-session/src/sysvar/catalog/connections.rs:408` began at
  `"lightning"` — 7 values, no empty.
* Go's `checkEnumSystemVar` (`variable.go:393`) matches **by ordinal position as
  well as by name**, so dropping index 0 shifts every ordinal:
  ```sql
  SET @@tidb_request_source_type = 0;  -- Go: ''         Rust(before): 'lightning'
  SET @@tidb_request_source_type = 1;  -- Go: 'lightning' Rust(before): 'br'
  SET @@tidb_request_source_type = ''; -- Go: OK          Rust(before): ERROR 1231
  ```
* Fixed by prepending `""`.

#### F8. `tidb_scatter_region`: `ScatterOff` missing, and the lower-casing refusal is absent

* Go: `sysvar.go:915`. `PossibleValues {ScatterOff /* = "" */, "table",
  "global"}`, `TypeStr`, plus a `Validation` closure that **lower-cases** the
  value and **refuses** anything outside the set with a bare `fmt.Errorf`
  (reported as 1105).
* Rust: `sysvar/catalog/distsql_storage.rs:456` listed only `["table","global"]`
  (fixed here to include `""`); the refusal/lower-casing closure is still
  absent.
* Distinguishing case:
  ```sql
  SET @@tidb_scatter_region = 'TABLE'; SELECT @@tidb_scatter_region;
  ```
  Go: `table`. Rust: `TABLE`.
  ```sql
  SET @@tidb_scatter_region = 'bogus';
  ```
  Go: `ERROR 1105 (HY000): invalid value for 'bogus', it should be either '',
  'table' or 'global'`. Rust: accepted.

#### F9. Rust models 15 variables with Go `Validation` closures out of 95

`sysvar.rs::run_validation` and the SET dispatch model 15 unique variables:
`timestamp`, `time_zone`,
`tidb_enable_table_partition`, `tidb_enable_list_partition`,
`tidb_session_alias`, `max_allowed_packet`, `sql_mode`,
`tidb_enable_fast_analyze`, both isolation spellings, and the five
read-only-noop variables. The unmodelled remainder falls into three contract
classes, and the difference between them is exactly the "clamp vs refuse"
distinction:

* **REFUSE-WITH-ERROR** (Rust currently accepts): `secure_auth`,
  `tidb_scatter_region`, `character_set_*` and `collation_*`
  (`checkCharacterSet` / `checkCollation` → 1115 "Unknown character set"),
  `tidb_partition_prune_mode`, `tidb_isolation_read_engines`,
  `tidb_read_consistency`, `tidb_replica_read`, `require_secure_transport`,
  `tidb_super_read_only`,
  `validate_password.*`, `tidb_analyze_version`, `tidb_dml_type`.
  The `sysvar.rs` test `per_variable_validation_closures_are_not_modelled`
  already pins this for `secure_auth`; that pin covers one variable, not the
  class.
* **REWRITE-THE-VALUE** (Rust stores what was typed):
  `tidb_disable_txn_auto_retry` (F4) and `tidb_scatter_region` lower-casing
  (F8).
* **WARN-ONLY** (Rust is silent): `tidb_enable_clustered_index` `INT_ONLY`
  deprecation warning (`sysvar.go:2784`), the 10 `newExecConcurrencySysVar`
  entries' `appendDeprecationWarning(..., tidb_executor_concurrency)`
  (`sysvar.go:85-105`), `foreign_key_checks`, `group_concat_max_len`.
* Character-set validation is the one with the broadest blast radius:
  ```sql
  SET NAMES bogus;
  ```
  Go: `ERROR 1115 (42000): Unknown character set: 'bogus'`. Rust:
  `SessionVars::set_names` (`vars.rs:409`) calls `set_system` three times, each
  of which sees `VarType::Str` and accepts anything.

#### F10. `InternalSessionVariable` is not represented

* Go: `variable.go:277-279` — a `ScopeSession` write to a variable marked
  `InternalSessionVariable` returns `errUnknownSystemVariable`. Exactly one
  entry carries it: `tidb_redact_log` (`sysvar.go:2827`).
* Rust: the flag is absent from `SysVarDef` (`sysvar.rs:76-97`).
* Distinguishing case:
  ```sql
  SET SESSION tidb_redact_log = 'ON';
  ```
  Go: `ERROR 1193 (HY000): Unknown system variable 'tidb_redact_log'`.
  Rust: accepted.

#### F11. `SysVar.IsNoop` is not represented

* Go: `pkg/executor/set.go:146` warns `ErrSettingNoopVariable` ("setting %s has
  no effect in TiDB", `pkg/errno/errname.go:1060`) on a write to any of the 423
  noop variables when `tidb_enable_noop_variables` is OFF; `pkg/executor/show.go:963,988`
  **hides** them from `SHOW VARIABLES` under the same condition.
* Rust: `SysVarDef` has no `is_noop` field, so neither behaviour can happen.
* Distinguishing case:
  ```sql
  SET GLOBAL tidb_enable_noop_variables = OFF;
  SET @@ndb_index_stat_option = 'x'; SHOW WARNINGS;
  SHOW VARIABLES LIKE 'ndb_index_stat_option';
  ```
  Go: one warning, and the `SHOW` returns no row. Rust: no warning, and the row
  is returned.
* Default is ON (`DefTiDBEnableNoopVariables = true`), so this is only visible
  after the operator opts in — which is why it ranks here and not higher.

### Rank 3 — missing variables

**None.** The name set is identical in both directions (948 = 948). This was
the single most likely place for a silent feature-switch hole, and it is clean.

### Rank 4 — message and scope differences

#### F12. Read-only refusal uses the wrong error

* Go: `variable.go:266` — `ErrIncorrectScope.FastGenByArgs(sv.Name, "read only")`
  → 1238 "Variable '%s' is a %s variable".
* Rust: `vars.rs:187` names `VarError::ReadOnlyVariable` after
  `ErrIncorrectGlobalLocalVar`. Codes coincide at 1238, but the argument list
  differs (Go passes the literal string `"read only"` as the second `%s`); the
  rendered text should be checked against a live server.

#### F13. `SET GLOBAL` privilege check placement

* Go: `pkg/executor/set.go:124-137` runs `sysVar.RequireDynamicPrivileges`
  **before** validation, so a privilege failure wins over a bad value.
* Rust: `variables.rs:220` `require_set_global_privilege()` also runs before
  validation. **Matches.** Recorded so nobody re-checks it.

#### F14. `tidb_txn_mode` reads the session copy directly rather than through Go's mode decision

* Go: `sysvar.go:2600`, `SetSession` does `s.TxnMode = strings.ToUpper(val)`,
  and `decideTxnMode` treats anything that is not `PESSIMISTIC` as optimistic —
  which is what makes the `AllowEmptyAll` empty value meaningful.
* Rust: `rust/crates/tidb-session/src/txn.rs:184` reads
  `get_system("tidb_txn_mode")` at use. `ALLOW_EMPTY_ALL_VARS` (`sysvar.rs:187`)
  correctly includes it, so `SET tidb_txn_mode = ''` is accepted in both. The
  stored *case* differs (Go upper-cases into `TxnMode`; Rust stores the enum's
  canonical lower-case `pessimistic`), which is invisible to `SELECT @@` because
  Go's read comes from `systems`, not `TxnMode`. **Effectively matches**; noted
  because the case difference would bite a byte-comparing reader.

---

## Behavioral census and priority unread variables

The census categories are mutually exclusive:

* **Runtime behavior** means the value changes parsing, planning, execution,
  DDL, protocol result encoding, or a session state machine. Forty-two entries are
  in this class. Newly landed readers explicitly included here are
  `tidb_enable_clustered_index`, `max_allowed_packet`,
  `tidb_opt_join_reorder_through_proj`, and `tidb_partition_prune_mode`.
* **SET/validation-only** means Rust reads or special-cases the variable only
  while validating, warning about, routing, or aliasing a SET. Sixteen entries
  are in this class. `tidb_skip_isolation_level_check` and
  `tidb_enable_legacy_instance_scope` are here: both have real consumers, but
  neither changes ordinary statement execution.
* **Behaviorally unread** means no source-backed consumer in either class.
  Generic storage, `SELECT @@x`, and `SHOW VARIABLES` still work. There are
  890 such entries; 730 are writable.

The highest-priority unread variables are:

| Variable | Why it matters |
| --- | --- |
| `character_set_client` | Common connector/session setup; the parser does not consume the stored client charset. |
| `max_execution_time` | Common ORM/driver timeout; accepted without enforcing a statement deadline. |
| `transaction_isolation` / `tx_isolation` | SET refusal/warning behavior is wired, but accepted levels do not select a transaction isolation implementation. |
| `tidb_replica_read` | Operational read-routing control; no request-routing consumer. |
| `tidb_retry_limit` / `tidb_disable_txn_auto_retry` | Retry policy is client-visible under conflicts; neither reaches a retry loop, and the latter's deprecated-value rewrite is also missing. |
| `tidb_max_chunk_size` / `tidb_init_chunk_size` | Common mysql-tester DSN settings and executor tuning knobs; stored but not read by chunk allocation. |
| `tidb_request_source_type` | Real clients and tools label requests for admission/observability; no request field consumes it. |
| `tidb_scatter_region` | DDL placement behavior and its validation/lower-casing are both absent. |

This table replaces the old nine-name sample. That sample correctly found
several high-value gaps, but it was not a census and became stale as clustered
index and result-size readers landed.

---

## Verified equal — where not to look

These were compared and agree; re-auditing them is wasted effort.

1. **The catalog's declarative surface.** For all 938 source-resolvable entries:
   name, scope bitset, type, default value, `ReadOnly`, `MinValue`, `MaxValue`,
   `AllowAutoValue`, `AutoConvertNegativeBool` — **zero divergences**. Defaults
   that Go computes at init (config-derived, `GetDefault*()`) could not be
   checked from source, but the Rust table was captured from a running registry,
   which is the stronger source for exactly those.
2. **The name set.** 948 in both directions, no extras either way, all lowercase
   in the Rust table, and `get_sys_var` (`sysvar.rs:149`) lower-cases the lookup
   key — so the un-lowercased-name class of bug is closed here.
3. **`sql_mode`.** Default string is byte-identical to
   `pkg/parser/mysql/const.go:373`
   (`ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION`).
   Its `Validation` (format → expand combination modes → reject unknown token,
   naming the *token*) is modelled at `sysvar.rs:409-427`. The mode flags are
   read per statement at `stmt_ctx.rs:206-305`: `NO_ZERO_DATE`,
   `NO_ZERO_IN_DATE`, `ALLOW_INVALID_DATES` on **both** the query and DML
   branches, `ONLY_FULL_GROUP_BY` on both, `ERROR_FOR_DIVISION_BY_ZERO` and
   `STRICT_TRANS_TABLES || STRICT_ALL_TABLES` on the DML branch,
   `NO_AUTO_VALUE_ON_ZERO` on the DML branch, and the six scanner-visible modes
   (`REAL_AS_FLOAT`, `NO_BACKSLASH_ESCAPES`, `ANSI_QUOTES`,
   `HIGH_NOT_PRECEDENCE`, `IGNORE_SPACE`, `PIPES_AS_CONCAT`) via
   `scanner_sql_mode_of`.
4. **The type-directed validation contracts** — this is the "clamp vs refuse"
   question the task asks about, and Rust reproduces Go exactly:

   | Type | Go (`variable.go`) | Rust (`sysvar.rs`) |
   | --- | --- | --- |
   | `Unsigned` | unparseable → refuse 1232; negative → clamp to `MinValue` + warn 1292; out of range → clamp + warn; `AllowAutoValue` lets `-1` through untouched | `check_uint64` (`:437`), identical including the `-1` escape and the `uint64(MinValue)` sign-cast |
   | `Int` | unparseable → refuse 1232; out of range → clamp + warn | `check_int64` (`:477`), identical |
   | `Bool` | `ON`/`OFF` case-insensitive; `0`/`1`; `AutoConvertNegativeBool` maps any negative to `ON`; everything else → refuse 1231 | `check_bool` (`:503`), identical |
   | `Enum` | name case-insensitive **or ordinal position**; else refuse 1231 | `check_enum` (`:551`), identical |
   | `Float` | empty or unparseable → refuse 1232; out of range → clamp to the *integer* bound + warn | `check_float` (`:566`), identical including the integer-formatted bound |
   | `Str` | pass through | pass through |
   | `Time`, `Duration` | parse and clamp | **not ported** — value passes through unchanged; documented at `sysvar.rs:317` |

   The `AllowEmpty` / `AllowEmptyAll` escape hatch (`variable.go:241`) is
   reproduced at `sysvar.rs:430` with the same scope rule, over hand-maintained
   name lists (`sysvar.rs:164`, `:187`) that match `sysvar.go`.
5. **Aliases.** Go's three reciprocal pairs — `tx_isolation` ↔
   `transaction_isolation`, `tx_read_only` ↔ `transaction_read_only`,
   `tidb_prepared_plan_cache_size` ↔ `tidb_session_plan_cache_size` — are all
   present at `sysvar.rs:203`, in both directions, and are applied *after*
   validation with the alias's own validation skipped, matching Go's
   `SetSessionFromHook`.
6. **Session/global read fall-through.** `get_system` (`vars.rs:265`) falls
   through to the global table exactly when `!HasSessionScope()`, matching Go's
   `GetSessionOrGlobalSystemVar`; `get_global` (`vars.rs:312`) never reads the
   session copy. `seed_from_globals` (`vars.rs:242`) copies at connect only and
   guards on `HasSessionScope`, matching `NewSessionVars`.
7. **`autocommit`.** Default `ON`, `TypeBool`, `Global|Session`, no `Validation`.
   Go's `SetSession` closure (`sysvar.go:2117`) ends an ongoing transaction on
   the OFF→ON *transition* only; Rust reproduces this at `variables.rs:274-283`,
   including the "already ON leaves an explicit BEGIN running" case.
8. **`time_zone`.** Default `SYSTEM`, `TypeStr`, `Global|Session`. The
   canonicalisation rule (only `SYSTEM` is upper-cased; every other name is
   stored as typed) is modelled at `sysvar.rs:346`, and the reader
   (`stmt_ctx.rs:37`) handles named zones and `±HH:MM` with Go's
   `[-12:59, +14:00]` bound. What is *not* checked at SET time is whether a
   named zone exists — an unknown name silently falls back to the host zone
   instead of Go's 1298; that is stated in the code comment at `stmt_ctx.rs:33`.
9. **`tidb_enable_check_constraint`.** `ScopeGlobal` only, `TypeBool`, default
   `OFF` — matched. Correctly read through `get_global`, not `get_system`
   (`stmt_ctx.rs:346`), which is the right call for a global-only variable, and
   consumed at `dispatch.rs:530`.
10. **`tidb_mem_quota_query`.** `TypeInt`, min `-1`, max `MaxInt64`, default
    matched. Read per statement at `stmt_ctx.rs:242` with the OOM action taken
    from `tidb_mem_oom_action` via `get_global` (correctly, since that one is
    global-only).
11. **`foreign_key_checks`**, **`tidb_allow_remove_auto_inc`**,
    **`auto_increment_increment`/`_offset`**, **`cte_max_recursion_depth`**,
    **`default_week_format`**, **`div_precision_increment`**, **`timestamp`**,
    **`sql_select_limit`** — all read per statement from the session table with
    Go's fallbacks (`stmt_ctx.rs:214-360`, `variables.rs:656`).
12. **Scope-error selection for the two scopes Rust does implement.**
    `SET SESSION` on a global-only variable → 1229; `SET GLOBAL` on a
    session-only variable → 1228; read-only → 1238. Matches `validateScope`
    apart from F10 (internal); F1's instance tier is now modelled.

---

## StmtContext (item 5)

Rust's statement context is **built fresh per statement** by
`Session::statement_context` (`stmt_ctx.rs:~190`) rather than reset in place,
which structurally eliminates Go's stale-field class — Go's `ResetContextOfStmt`
(`pkg/executor/select.go:923`) must explicitly clear ~40 fields on a reused
`StatementContext`, and `StatementContext.Reset` (`stmtctx.go:591`) exists only
because the object is pooled. Building fresh is the better shape and no
divergence follows from it.

What crosses the statement boundary in Rust is exactly two values, promoted in
`publish_statement_status` (`stmt_ctx.rs:~370`):

* `last_insert_id` — promoted even when the statement **failed**, matching Go's
  `StmtCtx.LastInsertID` (the code comment records the captured case
  `SELECT LAST_INSERT_ID(17), bad()`).
* `prev_row_count` — `-1` for `SELECT`, affected rows for DML (0 on failure),
  0 otherwise, matching Go's `ROW_COUNT()` contract.

Go additionally carries per-statement flags Rust's context does not model:
`InRestrictedSQL`, `InLoadDataStmt`, `InExplainStmt`, and the
`AlternativeLogicalPlan*` planner signals (`stmtctx.go:273-294`, reset at
`:686`). `InInsertStmt`/`InUpdateStmt`/`InDeleteStmt`/`InSelectStmt` are covered
in Rust by the `for_query()` / `for_dml()` split plus `StatementKind`, which is
coarser: Go distinguishes INSERT from UPDATE/DELETE when deriving type flags
(`stmtctx.go:1255-1306`, notably `WithAllowNegativeToUnsigned(!sc.InInsertStmt)`).
**That INSERT-only distinction is a real gap** — an UPDATE writing a negative
value into an UNSIGNED column follows a different path in Go than an INSERT
does — but confirming its user-visible shape needs execution.

**Retry inheritance:** Go re-runs `ResetContextOfStmt` on each retry attempt,
so per-statement flags are rebuilt while `SessionVars.RetryInfo` (auto-increment
ids, `TxnCtx`) survives. Rust has the same split — `retry_auto_ids` is an `Rc`
handed to each fresh context (`stmt_ctx.rs`, `with_retry_auto_ids`) while
everything else is rebuilt. Consistent. Since F4 shows the retry *variables*
are unread, whether the retry loop itself runs at all is out of this audit's
scope.

---

## Verification and remaining limits

The 2026-08-05 re-census executed the checked-in script and the integration
replay/warning gates quoted above. The allow-empty registry fix was also pinned
by `sysvar::tests::allow_empty_tables_name_live_registry_entries`: the test
failed before the fix on the stale `enable_resource_metering` name and passed
after `tidb_enable_top_sql` plus plural `tidb_capture_plan_baselines` replaced
the stale entries.

Still not verified by this audit:

* No live Go server was started for every ranked sysvar case. Go behavior in
  those cases remains source-derived or inherited from the captures already
  documented beside the implementation.
* Go defaults computed at init were not re-derived from source: entries whose
  `Value` is `strconv.Itoa(config.GetGlobalConfig()...)`, `GetDefault*()`, or a
  `versioninfo` concatenation. The Rust catalog was generated from a running Go
  registry, which remains the stronger source for those values.
* The census proves whether a source consumer exists, not that every consumer
  matches all Go semantics. The ranked findings above remain the semantic
  review of the highest-risk readers and missing hooks.
