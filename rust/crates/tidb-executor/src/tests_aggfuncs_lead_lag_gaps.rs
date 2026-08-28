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

//! Gap tests for Go `pkg/executor/aggfuncs/func_lead_lag_test.go`. The
//! LEAD/LAG window state is transcreated in the sibling crate `tidb-exec`
//! (`lead_lag.rs`), which this crate does not depend on.

/// Go `pkg/executor/aggfuncs/func_lead_lag_test.go:27::TestLeadLag` over 3
/// rows (values 0,1,2): LAG(field, 0) = own row, offsets past either end
/// yield NULL or the DEFAULT when provided -- constant default (1000000)
/// fills out-of-range rows, while a `field0` default evaluates the DEFAULT
/// expression per row (so LAG(f, 3, f) reproduces 0,1,2); LEAD mirrors it
/// forward (LEAD(f,1) = 1,2,NULL; LEAD(f,3,million) = million x3; LEAD(f,3,f)
/// = 0,1,2). Offsets arrive as constants of TypeTiny/TypeLong.
#[test]
#[ignore = "go-parity-gap: LEAD/LAG offset/default evaluation lives in tidb-exec::lead_lag (sibling crate); the windowTest expression harness has no counterpart here"]
fn lead_lag_offsets_default_to_null_constant_or_expression() {}

/// Go `pkg/executor/aggfuncs/func_lead_lag_test.go:119::TestMemLeadLag`: the
/// LEAD/LAG partial result charges a fixed size once and then only per-row
/// DEFAULT-expression evaluation deltas, mirroring the offset matrix above.
#[test]
#[ignore = "go-parity-gap: window memory-tracker harness not modeled; state lives in tidb-exec::lead_lag (sibling crate)"]
fn mem_lead_lag_charges_fixed_size_plus_row_deltas() {}
