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

**Nothing in this document was executed.** See "What is unverified" at the end.

---

## Counts

| | |
| --- | --- |
| Variables in Go's registry | 948 (515 `defaultSysVars` written as literals + 10 built by `newExecConcurrencySysVar` + 423 `noopSysVars`) |
| Variables in the Rust catalog | 948 |
| Name-set difference | **0** — every name matches exactly, and every Rust name is lowercase |
| Declarative fields compared (scope, type, default, read-only, min, max, allow-auto, auto-convert-negative-bool) across the 938 source-resolvable entries | **0 divergences** |
| `PossibleValues` divergences | **2** (both fixed in this change) |
| Ranked behavioural findings | **14** |
| Go `Validation` closures in the registry | 94; **87 unmodelled** in Rust |
| Go `SetSession` closures in the registry | 279; a handful modelled |
| "Present but unwired" (accepted, stored, never read) | **9** named below |

---

## Ranked findings

### Rank 1 — a switch that is never read, or a value the engine cannot reach

#### F1. All 28 `ScopeInstance` variables are unsettable and unreadable

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

#### F2. `tidb_enable_clustered_index` — accepted, stored, never read

* Go: `pkg/sessionctx/variable/sysvar.go:2782`, `TypeEnum`,
  `PossibleValues {OFF, ON, INT_ONLY}`, default `ON`, plus a `SetSession`
  closure writing `SessionVars.EnableClusteredIndex`, read by the DDL table
  builder to decide whether a table's PK becomes the row handle.
* Rust: the catalog entry exists and is correct
  (`sysvar/catalog/ddl_schema.rs:208`, and `tidb-vardef/src/tidb_vars.rs:787`
  defines the name constant) — but the *name constant has no consumer*, and
  the only non-catalog mention in the workspace is a comment at
  `rust/crates/tidb-exec/src/cluster_ddl.rs:244`. No code path calls
  `get_system("tidb_enable_clustered_index")`.
* Distinguishing case:
  ```sql
  SET @@tidb_enable_clustered_index = 'OFF';
  CREATE TABLE t (a VARCHAR(10) PRIMARY KEY, b INT);
  SHOW CREATE TABLE t;
  ```
  Go: `PRIMARY KEY (a) /*T![clustered_index] NONCLUSTERED */`, and the row key
  is `_tidb_rowid`. Rust: unaffected by the SET — whatever the DDL path
  hardcodes.
* The `INT_ONLY` third value makes this worse than a boolean: any reader added
  later that treats it as ON/OFF will get `INT_ONLY` wrong.
* The clustered-index DDL path is owned by another unit in this session; this
  is reported, not touched.

#### F3. `max_allowed_packet` is a compile-time constant, and `SET SESSION` on it is not refused

* Go: `pkg/sessionctx/variable/sysvar.go:2193`. Its `Validation` closure does
  two things Rust does neither of:
  1. `SET SESSION max_allowed_packet = ...` is **refused**:
     `ErrReadOnly.GenWithStackByArgs("SESSION", "max_allowed_packet", "GLOBAL")`
     (1621, "SESSION variable 'max_allowed_packet' is read-only. Use SET GLOBAL
     to assign the value").
  2. The accepted global value is truncated **down to a multiple of 1024** with
     `ErrTruncatedWrongValue`.
* Rust: `vars.rs:334` accepts the session write with only the `TypeUnsigned`
  range check (`MinValue 1024`). And the wire layer never reads the variable at
  all: `rust/crates/tidb-protocol/src/packet.rs:24` hardcodes
  `DEFAULT_MAX_ALLOWED_PACKET: usize = 64 << 20`, used at `packet.rs:151`.
* Distinguishing case:
  ```sql
  SET SESSION max_allowed_packet = 1048576;
  ```
  Go: `ERROR 1621 (HY000)`. Rust: `Query OK`.
  ```sql
  SET GLOBAL max_allowed_packet = 1025; SELECT @@global.max_allowed_packet;
  ```
  Go: `1024` plus warning 1292. Rust: `1025`, no warning.
* **Both SET behaviours FIXED.** The 1024-rounding is in
  `SysVarDef::run_validation`, where the existing `truncated` flag already
  carries `ErrTruncatedWrongValue` (1292) naming the value as TYPED; the
  SESSION refusal is `variables.rs::check_max_allowed_packet_scope` →
  `VarErrorKind::SessionScopeIsReadOnly` → 1621. The refusal cannot be a scope
  bit: the variable still HAS session scope for READING, and only the write is
  refused.
* **The wire READER is NOT fixed, and was misdiagnosed above.**
  `packet.rs`'s `DEFAULT_MAX_ALLOWED_PACKET` is a correctly-named *default*:
  `PacketReader` already carries a per-connection `max_allowed_packet` field
  with `with_max_allowed_packet`/`set_max_allowed_packet`. What is missing is
  the caller — `tidb-server` (`sql_node.rs:1561`, `node_config.rs:383`,
  and the long-data buffer bound at `mysql_connection.rs:282`) passes the
  constant and never re-reads the variable after a `SET GLOBAL`. That is a
  `tidb-server` change, not a `tidb-protocol` one; recorded here rather than
  faked.
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

#### F6. Isolation level: `SERIALIZABLE` and `READ-UNCOMMITTED` are accepted

* Go: `pkg/sessionctx/variable/varsutil.go:116` `checkIsolationLevel` — both
  values raise `ErrUnsupportedIsolationLevel` (8048) **unless**
  `tidb_skip_isolation_level_check` is ON, in which case they are accepted with
  that error as a *warning*. Wired to both spellings at `sysvar.go:2100`
  (`tx_isolation`) and `:2106` (`transaction_isolation`).
* Rust: neither spelling has a `Validation` in `sysvar.rs::run_validation`. The
  machinery exists but is dead: `rust/crates/tidb-exec/src/isolation_state.rs`
  and `SysVarErrorCode::UNSUPPORTED_ISOLATION_LEVEL`
  (`rust/crates/tidb-exec/src/sysvar_error.rs:53`, code 8048) are referenced
  **only by their own source tests**.
* Distinguishing case:
  ```sql
  SET SESSION transaction_isolation = 'SERIALIZABLE';
  ```
  Go: `ERROR 8048 (HY000): The isolation level 'SERIALIZABLE' is not supported.
  Set tidb_skip_isolation_level_check=1 to skip this error`. Rust: accepted and
  stored — and since nothing downstream reads it, the session silently keeps
  running at snapshot isolation while reporting SERIALIZABLE.
* **FIXED** in `variables.rs::check_isolation_level`, which is where both
  halves of Go's closure can reach what they need: the skip switch is read
  from the session and the downgraded warning is appended to it. The refusal
  is `VarErrorKind::UnsupportedIsolationLevel` (8048). Go tests the
  *normalized* value, so the ordinal and lower-case spellings are refused with
  it; the two accepted levels store and read back unchanged, on both spellings
  of the variable name. Pinned in `tests_global_vars.rs`.
* Still open, and deliberately not faked: nothing downstream READS the stored
  level. `tidb-exec/src/isolation_state.rs` is now reachable from a correct
  SET, but the executor does not consult it — a `READ-COMMITTED` session still
  runs at the tier's one isolation. That reader belongs to the transaction
  seam, not to the variable layer.

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

#### F9. 87 of Go's 94 `Validation` closures are unmodelled

`sysvar.rs::run_validation` models 6 (`timestamp`, `time_zone`,
`tidb_enable_table_partition`, `tidb_enable_list_partition`,
`tidb_session_alias`, `sql_mode`), plus `tidb_enable_fast_analyze` in the
dispatch layer. The remaining 87 fall into three contract classes, and the
difference between them is exactly the "clamp vs refuse" distinction:

* **REFUSE-WITH-ERROR** (Rust currently accepts): `secure_auth`,
  `max_allowed_packet` (session scope), `tx_isolation`/`transaction_isolation`,
  `tidb_scatter_region`, `character_set_*` and `collation_*`
  (`checkCharacterSet` / `checkCollation` → 1115 "Unknown character set"),
  `tidb_partition_prune_mode`, `tidb_isolation_read_engines`,
  `tidb_read_consistency`, `tidb_replica_read`, `require_secure_transport`,
  `read_only` / `super_read_only` / `offline_mode` / `tidb_super_read_only`,
  `validate_password.*`, `tidb_analyze_version`, `tidb_dml_type`.
  The `sysvar.rs` test `per_variable_validation_closures_are_not_modelled`
  already pins this for `secure_auth`; that pin covers one variable, not the
  class.
* **REWRITE-THE-VALUE** (Rust stores what was typed): `tidb_disable_txn_auto_retry`
  (F4), `tidb_scatter_region` lower-casing (F8), `max_allowed_packet`
  1024-rounding (F3).
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

## "Present but unwired": accepted and stored, never read

Nine, each verified by grepping every crate for the literal name and for the
`tidb-vardef` constant, excluding the catalog files and tests:

| Variable | Name constant | Consumers found |
| --- | --- | --- |
| `tidb_enable_clustered_index` | `tidb-vardef/src/tidb_vars.rs:787` | none (one comment) |
| `tidb_retry_limit` | `tidb_vars.rs:136` | none |
| `tidb_disable_txn_auto_retry` | `tidb_vars.rs:140` | none |
| `tidb_max_chunk_size` | `tidb_vars.rs:473` | name-list membership only |
| `tidb_init_chunk_size` | `tidb_vars.rs:540` | name-list membership only |
| `max_allowed_packet` | — | none; wire uses a `const` |
| `transaction_isolation` / `tx_isolation` | — | none outside the alias pair |
| `tidb_request_source_type` | — | none |
| `tidb_scatter_region` | — | none |

Dispositions after this change:

| Variable | Disposition |
| --- | --- |
| `transaction_isolation` / `tx_isolation` | SET contract wired (8048 + skip-switch warning). The READER is the transaction seam's; not faked. |
| `max_allowed_packet` | SET contract wired (1621 + 1024-rounding). The wire reader needs a `tidb-server` change (see F3); the `tidb-protocol` side already takes a per-connection limit. |
| `tidb_max_chunk_size` / `tidb_init_chunk_size` | The clamp fires; the missing reader is the chunk allocator in `tidb-exec`/`tidb-executor`, outside this seam. Threading it is a one-line read at the allocator once that unit lands — the variable layer has nothing left to add. |
| `tidb_request_source_type` | No consumer exists in this tier at all: there is no request-source field on the coprocessor request this node builds. Documented, not faked. |
| `tidb_scatter_region` | No region-scatter path exists (DDL does not pre-split). Its `Validation` (lower-case + refuse outside `''`/`table`/`global`) is still unmodelled: `ValidationError::Refused` carries a `&'static str`, and Go's message interpolates the offending value, so wiring it needs that variant to carry an owned string. Left for the F9 sweep. |
| `tidb_retry_limit` / `tidb_disable_txn_auto_retry` | Untouched: both need the retry seam (F4), including the inverted `always returns ON` contract. |

Every `tidb-vardef` name constant in the workspace was checked; those five are
the ones that are *defined and never referenced*, which is a cheap standing
detector for this class.

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
    apart from F1 (instance) and F10 (internal).

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

## What is unverified because nothing can execute here

`syspolicyd` on this machine wedges every freshly built binary at `_dyld_start`.
Therefore:

* **No test was run.** `cargo check` and `cargo clippy` are the only gates
  applied to the change in this document.
* **No SQL was executed.** Every "distinguishing case" above is derived from
  reading both implementations; none was confirmed against a live `tidb-server`
  or through `gorun`/`goeval`.
* **Go defaults computed at init were not verified from source**: entries whose
  `Value` is `strconv.Itoa(config.GetGlobalConfig()...)`, `GetDefault*()`, or a
  `versioninfo` concatenation. The Rust table's own provenance (captured from a
  running `GetSysVars()`) is the better evidence for those, and it is the reason
  this audit reports zero default divergences with confidence for the resolvable
  938 and *inherited* confidence for the rest.
* **The two fixes in this change are unverified by test.** They are pure data
  edits to `possible_values` with `cargo check`/`cargo clippy` clean; the
  registry's `the_registry_is_complete_and_sorted` test is unaffected (neither
  edit changes a name, a count, or the ordering).
