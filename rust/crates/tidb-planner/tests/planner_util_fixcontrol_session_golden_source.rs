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

//! Port ledger for `pkg/planner/util/fixcontrol/fixcontrol_test.go`
//! (`pkg/planner.part22` items 1269-1270 on `origin/master`).
//!
//! Item 1270 (`TestParseToMapEmptyValue`, :92-96) already has complete,
//! in-gate coverage: `src/fix_control.rs` `tests::empty_value_is_a_present_empty_string`
//! pins `parse_to_map("123:")` -> exactly one entry `123 -> ""`, no warnings.
//!
//! THIS item (1269, `TestFixControl`, :46-90) stays documentary: it drives the
//! 13-case golden fixture pkg/planner/util/fixcontrol/testdata/
//! fix_control_suite_in.json through a live SQL session — `set
//! @@tidb_opt_fix_control = ...` per case (:66), then re-reads the SESSION's
//! map (`s.GetSessionVars().OptimizerFixControl`) and re-runs all four typed
//! getters over every surviving key via `getTestResultForSingleFix` (:30-44),
//! pinning error text, sorted SHOW WARNINGS rows and `select
//! @@tidb_opt_fix_control` echoes against fix_control_suite_out.json. The
//! crate owns the parser and getters (`OptimizerFixControl::parse`,
//! `get_{str,bool,int,float}_with_default`) but not the session-variable
//! assignment/validation pipeline or the testdata harness they interleave with.

/// GO PORT of `pkg/planner/util/fixcontrol/fixcontrol_test.go:46 TestFixControl`.
///
/// Re-derived contract from the production pieces the session exercises:
/// - `ParseToMap` grammar (set.go:26-88): `key:value[,key:value...]`,
///   quoted values keep inner separators, repeated keys warn only when the
///   value CHANGES; malformed colon/quote/key produce errors;
/// - typed getters (get.go:86-181): absent key returns the default; GetBool
///   accepts case-insensitive `ON` / exact `1`; GetInt/GetFloat delegate to
///   Go ParseInt/ParseFloat semantics and fall back to the default on parse
///   failure.
#[test]
#[ignore = "go-parity-gap: needs live session @@tidb_opt_fix_control assignment plus the testdata BookKeeper golden harness"]
fn fix_control_session_round_trips_the_thirteen_case_golden_suite() {}
