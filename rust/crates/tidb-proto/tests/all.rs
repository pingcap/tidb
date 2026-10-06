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

//! Shared integration-test harness for module-safe `tidb-proto` suites.
//! Register new module-safe suites here; isolated suites belong in Cargo.toml.

mod analyze_wire_source;
mod batch_commands_wire_source;
mod coprocessor_mpp_wire_source;
mod pd_wire_source;
mod region_error_wire_source;
mod tipb_selection_expression_source;
mod tipb_spfresh_source;
mod transaction_wire_source;
