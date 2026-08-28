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

//! Gap tests for Go `pkg/executor/set_test.go` (items 577-586). All ten
//! tests exercise the session/system-variable machinery — SET/SELECT of
//! variables, their validation, warning emission, and global/session scoping
//! (`pkg/executor/set.go:57 SetExecutor.Next` over
//! `pkg/sessionctx/variable`). This crate owns no variable surface; the
//! session tier that would own it has not transcreated these behaviors.

/// Go `pkg/executor/set_test.go:45::TestSetVar` (~950 lines): SET syntax
/// matrix (`@a`, `@@global.`, `@@session.`, `@@instance.`, multi-assign,
/// `SET ... = DEFAULT`), unknown-variable/unknown-collation/unknown-charset
/// errors, `ddl_slow_threshold` instance vs global writes,
/// `SET TRANSACTION ISOLATION LEVEL` mapping to `tx_isolation` with
/// `tidb_skip_isolation_level_check` warning 8048 escalation, dozens of
/// tidb_ variables' parsing/clamping warnings (1292), read-only scoped
/// variables (`plugin_dir`), `validate_password.*` composites, and
/// `tidb_dml_batch_size` redaction. Needs SetExecutor + the variable
/// registry.
#[test]
#[ignore = "go-parity-gap: SET statement execution (pkg/executor/set.go:57) and the session variable registry (pkg/sessionctx/variable) are unported on this tier"]
fn set_var_assigns_validates_and_warns_across_scopes() {}

/// Go `pkg/executor/set_test.go:993::TestSetCollationAndCharset`: `set
/// character_set_connection` also sets `collation_connection` to the
/// charset's default collation (and the database/server pairs likewise);
/// setting the collation side sets the charset side to the collation's
/// charset — read back through `GetSystemVar`. Needs the charset/collation
/// variable pairing hooks.
#[test]
#[ignore = "go-parity-gap: charset/collation variable co-assignment (pkg/executor/set.go:57, pkg/sessionctx/variable) is unported"]
fn setting_a_charset_or_collation_sets_its_pair() {}

/// Go `pkg/executor/set_test.go:1037::TestValidateSetVar`: per-variable
/// validation — `'fff'` for concurrency variables errors
/// `ErrWrongTypeForVar`, `-2` truncates with warning 1292,
/// `tidb_batch_delete` accepts only boolean forms (`'ok'` errors),
/// `group_concat_max_len` clamps to 4 and rejects overflow values,
/// `default_week_format` clamps to [0,7], read-only variables (`error_count`,
/// `warning_count`) refuse assignment with `ErrIncorrectScope`. Needs the
/// `ValidateSetVar` hook chain (pkg/sessionctx/variable/varsutil.go).
#[test]
#[ignore = "go-parity-gap: variable validation/truncation-warning machinery (pkg/sessionctx/variable/varsutil.go, ErrWrongTypeForVar paths) is unported"]
fn validate_set_var_rejects_and_truncates_per_variable() {}

/// Go `pkg/executor/set_test.go:1434::TestSetConcurrency`: deprecated
/// concurrency variables (`tidb_index_lookup_concurrency`,
/// `tidb_hash_join_concurrency`, ...) default to `ConcurrencyUnset`, report
/// the `tidb_executor_concurrency` default when unset, emit warning 1287
/// when set, and fall back to the executor default when set to -1;
/// `tidb_executor_concurrency = -1` clamps to 1 with warning 1292. Needs
/// the concurrency-unset indirection in the variable registry.
#[test]
#[ignore = "go-parity-gap: deprecated concurrency-variable unset/default fallback (vardef.ConcurrencyUnset, pkg/sessionctx/variable/session.go) is unported"]
fn deprecated_concurrency_variables_follow_executor_concurrency() {}

/// Go `pkg/executor/set_test.go:1530::TestEnableNoopFunctionsVar`:
/// `tidb_enable_noop_functions` is session-settable to ON and back to OFF
/// (global stays OFF), rejects `'abc'`/`11`, and once ON gates the
/// now-supported READ ONLY variables (`tx_read_only`, `read_only`,
/// `super_read_only`, `offline_mode`) whose GLOBAL forms need the flag.
/// Needs the variable plus its validator/hook wiring.
#[test]
#[ignore = "go-parity-gap: tidb_enable_noop_functions gating and its dependent read-only variables (pkg/sessionctx/variable) are unported"]
fn enabling_noop_functions_gates_read_only_variables() {}

/// Go `pkg/executor/set_test.go:1635::TestSetClusterConfig`: `set config
/// <type|instance> <key>=<value>` routes to the right server class —
/// unknown type errors, tidb refuses online config changes, tso/scheduling
/// refuse outright, tikv/tiflash fan out over `infoschema.ServerInfo`
/// addresses via HTTP POST /config (injected per-test through
/// `TestSetConfigHTTPHandlerKey`), `raftstore-proxy.` prefixes are stripped,
/// failures become warning 1105 per instance. Needs the config HTTP client
/// and the test-injection seams.
#[test]
#[ignore = "go-parity-gap: SET CONFIG fan-out over cluster ServerInfo (pkg/executor/set_config.go) with injected HTTP handlers is unported"]
fn set_config_routes_by_server_type_and_fans_out() {}

/// Go `pkg/executor/set_test.go:1709::TestSetClusterConfigJSONData`:
/// `executor.ConvertConfigItem2JSON(ctx, "k", expr)` renders constant
/// expressions as a JSON object body — bool-typed ints as true/false, ints,
/// floats, strings, decimals (`123.456`), string-typed JSON arrays as quoted
/// strings — and errors for NULL constants, JSON-typed constants, and nil
/// expressions (pkg/executor/set_config.go:187).
#[test]
#[ignore = "go-parity-gap: ConvertConfigItem2JSON (pkg/executor/set_config.go:187) has no Rust counterpart"]
fn convert_config_item_to_json_renders_constant_types() {}

/// Go `pkg/executor/set_test.go:1742::TestSetTopSQLVariables`: the Top SQL
/// variables round-trip through the global state —
/// `tidb_enable_top_sql` 'On'/'off', `tidb_top_sql_max_time_series_count`
/// rejecting 'abc' (1232), clamping -1 to 1 and 5001 to 5000 with warning
/// 1292, `tidb_top_sql_max_meta_count` likewise to [1,10000] — each value
/// observable in `topsqlstate.GlobalState`. Needs the topsql state package
/// and variable registry.
#[test]
#[ignore = "go-parity-gap: Top SQL global state (pkg/util/topsql/state) and its variable setters are unported"]
fn top_sql_variables_clamp_into_their_ranges() {}

/// Go `pkg/executor/set_test.go:1791::TestDivPrecisionIncrement`:
/// `div_precision_increment` defaults to 4, is session-settable in
/// [0,30] (-1 clamps to 0, 31 clamps to 30), and accepts a global set.
/// Needs the variable registry's min/max validation.
#[test]
#[ignore = "go-parity-gap: div_precision_increment range validation (pkg/sessionctx/variable) is unported"]
fn div_precision_increment_clamps_to_zero_thirty() {}

/// Go `pkg/executor/set_test.go:1822::TestSetTiDBServiceScopeCaseInsensitive`:
/// `set global tidb_service_scope=BACKground` / `TiDB` normalizes to
/// `background`/`tidb` in `vardef.ServiceScope`, rejects unknown scopes,
/// and the config round-trips. Needs the service-scope global and its
/// validator.
#[test]
#[ignore = "go-parity-gap: tidb_service_scope normalization against vardef.ServiceScope (pkg/sessionctx/variable) is unported"]
fn service_scope_assignment_is_case_insensitive() {}
