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

//! Port ledger for `pkg/planner/core/integration_test.go` TiFlash
//! function/window push-down EXPLAIN items (`pkg/planner.part11`, Go items
//! 603–616, 627, 628, 630, 638 on `origin/master`).
//!
//! Family contract: each test runs under
//! `testkit.RunTestUnderCascadesWithDomain` (`WithDomain` grants the domain
//! used to hack `tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{
//! Count: 1, Available: true }`, which is what makes the optimizer consider
//! `mpp[tiflash]`), sets `@@tidb_isolation_read_engines = 'tiflash'` plus
//! `@@tidb_allow_mpp = @@tidb_enforce_mpp = ON`, then pins the
//! `explain format='plan_tree'` text: the scalar function becomes a root-side
//! Projection *above* `ExchangeSender mpp[tiflash]` (proving push-down),
//! or window operators render with `stream_count:` after fine-grained-shuffle
//! exchange placement.
//!
//! All eighteen items are honest gap ports: this crate has no SQL optimize
//! entry point, no explain renderer, no TiFlash replica meta injection and no
//! session-variable surface, so none of these plan-tree goldens has an honest
//! carrier here. Nothing was approximated to simulate Go behavior.

/// GO PORT of `pkg/planner/core/integration_test.go:974
/// TestRepeatPushDownToTiFlash`.
///
/// Re-derived contract: over t(a int, b int) rows (2147483647, 2) / (12, 2)
/// with a hacked TiFlash replica, `explain format='plan_tree' select
/// repeat(a,b) from t` must show the projection pushed below the root:
/// TableReader(root, MppVersion data:ExchangeSender) → ExchangeSender
/// mpp[tiflash] PassThrough → Projection mpp[tiflash]
/// `repeat(cast(test.t.a, var_string(20)), test.t.b)` → TableFullScan
/// mpp[tiflash] (:995-1000, checked at :932-999 via CheckAt columns 0/1/3).
/// Pins that REPEAT() with its implicit var_string cast of the int argument
/// is TiFlash-pushable.
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn repeat_pushdown_to_tiflash_keeps_var_string_cast() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1002
/// TestGetFormatPushDownToTiFlash`.
///
/// Re-derived contract: over t(location) rows USA/JIS/ISO/EUR/INTERNAL,
/// `explain format='plan_tree' select GET_FORMAT(DATE, location) from t`
/// renders Projection mpp[tiflash] `get_format(DATE, test.t.location)`
/// under the root TableReader/ExchangeSender pair (:1020-1028).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn get_format_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1023
/// TestAggWithJsonPushDownToTiFlash`.
///
/// Re-derived contract: over t(a json) with one NULL row,
/// tidb_allow_tiflash_cop=ON: `avg(a)`/:`sum(a)` plans keep HashAgg at ROOT
/// fed by a ROOT Projection casting `test.t.a` to `double BINARY`, while only
/// the TableFullScan sits `cop[tiflash]`; `group_concat(a)` likewise casts to
/// `var_string(4294967295)` first (:1059-1067 avg, :1069-1077 sum,
/// :1084-1092 group_concat with hash_agg hint). Pins that JSON aggregates are
/// NOT themselves pushed to TiFlash — only their scan is.
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn json_agg_stays_root_while_scan_reads_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1068
/// TestLeftShiftPushDownToTiFlash`.
///
/// Re-derived contract: over t(a int, b int), `explain format='plan_tree'
/// select a << b from t` shows Projection mpp[tiflash]
/// `leftshift(test.t.a, test.t.b)` between ExchangeSender and TableFullScan
/// (:1090-1095).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn left_shift_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1097
/// TestHexIntOrStrPushDownToTiFlash`.
///
/// Re-derived contract: both arms of HEX push down — `hex(a)` (int) renders
/// `hex(test.t.a)` and `hex(b)` (varchar) renders `hex(test.t.b)`, each as
/// Projection mpp[tiflash]; note the int arm carries NO extra cast here,
/// unlike unhex/repeat (:1121-1126, :1136-1141).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn hex_int_and_str_arms_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1129
/// TestBinPushDownToTiFlash`.
///
/// Re-derived contract: over t(a int), `explain format='plan_tree' select
/// bin(a) from t` shows Projection mpp[tiflash] `bin(test.t.a)` below the
/// root ExchangeSender (:1150-1153 area golden at :1147-1152).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn bin_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1153
/// TestEltPushDownToTiFlash`.
///
/// Re-derived contract: over t(a int, b varchar(20)), `select elt(a, b)`
/// pushes as Projection mpp[tiflash] `elt(test.t.a, test.t.b)` (:1177-1182).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn elt_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1182
/// TestRegexpInstrPushDownToTiFlash`.
///
/// Re-derived contract: regexp family with user-supplied pos/occur/ret_op/
/// match_type column arguments stays TiFlash-pushable: Projection mpp[tiflash]
/// `regexp_instr(test.t.expr, test.t.pattern, 1, 1, 0, test.t.match_type)`
/// (:1206-1210); fixture rows include case-insensitive ('i') and multiline
/// ('m') matches (:1190).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn regexp_instr_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1210
/// TestRegexpSubstrPushDownToTiFlash`.
///
/// Re-derived contract: Projection mpp[tiflash]
/// `regexp_substr(test.t.expr, test.t.pattern, 1, 1, test.t.match_type)`
/// over the same i/m fixture shape (:1233-1237).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn regexp_substr_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1237
/// TestRegexpReplacePushDownToTiFlash`.
///
/// Re-derived contract: Projection mpp[tiflash] `regexp_replace(test.t.expr,
/// test.t.pattern, test.t.repl, 1, 1, test.t.match_type)` (:1261-1265).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn regexp_replace_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1265
/// TestCastTimeAsDurationToTiFlash`.
///
/// Re-derived contract: over t(a date, b datetime(4)) with sub-second
/// fixtures up to .999999, `explain format='plan_tree' select cast(a as
/// time), cast(b as time) from t` shows BOTH casts pushed into ONE Projection
/// mpp[tiflash]: `cast(test.t.a, time BINARY)->Column, cast(test.t.b, time
/// BINARY)->Column` (:1293-1297). Pins DATE→TIME and DATETIME→TIME casts as
/// TiFlash-pushable in plan building.
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn cast_time_as_duration_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1298
/// TestUnhexPushDownToTiFlash`.
///
/// Re-derived contract: asymmetric arms — `unhex(a)` over an INT wraps its
/// argument in an implicit cast first: `unhex(cast(test.t.a, var_string(20)))`,
/// while `unhex(b)` over VARCHAR renders direct `unhex(test.t.b)`; both are
/// Projection mpp[tiflash] (:1318-1323, :1325-1330).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn unhex_int_wraps_var_string_cast_str_direct_on_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:1330
/// TestLeastGretestStringPushDownToTiFlash`.
///
/// Re-derived contract: LEAST and GREATEST over two varchar columns both push
/// down: Projections mpp[tiflash] `least(test.t.a, test.t.b)` and
/// `greatest(test.t.a, test.t.b)` (:1350-1355, :1357-1361).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn least_greatest_string_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:904
/// TestTiFlashFineGrainedShuffleWithMaxTiFlashThreads`.
///
/// Re-derived contract: window query `row_number() over w1 partition by c1`
/// with isolation=tiflash/enforce_mpp must derive Window-exchange
/// `stream_count` from the session variables (:934-969): when
/// tiflash_fine_grained_shuffle_stream_count=0 it mirrors
/// @@tidb_max_tiflash_threads (=10 → "stream_count: 10", :939); when
/// max_tiflash_threads is -1 OR 0 it falls back to vardef default
/// DefStreamCountWhenMaxThreadsNotSet=8 (pkg/sessionctx/vardef/tidb_vars.go:
/// 1748; asserts :947, :955); shuffle=-1 disables streaming entirely so NO
/// row contains stream_count (:958-962); an explicit positive value wins
/// (=16 → "stream_count: 16", :965-969). Extraction helper regexes
/// `stream_count: ([0-9]+)` per explain row (:918-932).
#[test]
#[ignore = "go-parity-gap: needs domain-scoped tiflash replica hack + window fine-grained-shuffle planner + explain renderer"]
fn fine_grained_shuffle_stream_count_tracks_max_tiflash_threads() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2082
/// TestIsIPv4ToTiFlash`.
///
/// Re-derived contract: over v4/v6 address fixtures (valid IPv4, all-zero
/// IPv4/IPv6, loopback, compressed IPv6), `explain format='plan_tree' select
/// is_ipv4(v4) from t` pushes as Projection mpp[tiflash]
/// `is_ipv4(test.t.v4)` (:2105-2110).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn is_ipv4_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2113
/// TestIsIPv6ToTiFlash`.
///
/// Re-derived contract: same fixture table; `select is_ipv6(v6)` pushes as
/// Projection mpp[tiflash] `is_ipv6(test.t.v6)` (:2136-2141).
#[test]
#[ignore = "go-parity-gap: needs mock-store session + tiflash replica injection + explain plan_tree renderer"]
fn is_ipv6_pushdown_to_tiflash() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2255
/// TestWindowRangeFramePushDownTiflash`.
///
/// Re-derived contract: RANGE-frame windows push to TiFlash as
/// Window+Sort+ExchangeReceiver+HashPartition-ExchangeSender stacks, with
/// frame bounds rendered from the SQL (:2277-2322): integer literal
/// `range between 3 preceding and 0 following`; float-literal bound keeps
/// fractional rendering `3 preceding and 2.9 following` even though the SQL
/// spells 2.9E0 (:2283); decimal order-by column range `2.3 preceding … 0
/// following` over decimal(17,1); datetime order key renders interval units
/// QUOTED as identifiers — `interval 1 \"DAY\" preceding and interval 1
/// \"DAY\" following` (:2305); and a TIME order key is NOT TiFlash-window-
/// pushable: it falls back to a root Shuffle operator stack (concurrency:5,
/// ShuffleReceiver) above the mpp read (:2310-2321). All with
/// @@tidb_max_tiflash_threads=20 driving stream_count: 20.
#[test]
#[ignore = "go-parity-gap: needs range-frame window planning + mpp exchange placement + explain renderer"]
fn window_range_frame_pushdown_variants_and_time_fallback() {}

/// GO PORT of `pkg/planner/core/integration_test.go:2528
/// TestAggregationInWindowFunctionPushDownToTiFlash`.
///
/// Re-derived contract: aggregate-in-over (`sum/count/avg/min/max(v) over
/// w` partition by p order by o, implicit RANGE UNBOUNDED PRECEDING..CURRENT
/// ROW) pushes the whole Window (with five aggregated functions after
/// decimal(10,0) casts for sum/avg) below the root exchange as
/// mpp[tiflash], rendering stream_count: 8 throughout Window/Sort/Exchange
/// operators (:2553-2566). Pins that aggregates nested in window specs do
/// not block TiFlash push-down.
#[test]
#[ignore = "go-parity-gap: needs aggregation-in-window planning + tiflash replica injection + explain renderer"]
fn aggregation_in_window_function_pushdown_to_tiflash() {}
