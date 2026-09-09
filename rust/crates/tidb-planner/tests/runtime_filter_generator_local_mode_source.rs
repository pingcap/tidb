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

//! Documentary gap port for `pkg/planner/core/runtime_filter_generator_test.go`
//! (`pkg/planner.part15` item 866 on `origin/master`).
//!
//! | Go function (`runtime_filter_generator_test.go`) | Rust test |
//! | --- | --- |
//! | `:34 TestRuntimeFilterGenerator` | [`runtime_filter_generator_local_mode_plan_goldens`] |

/// GO PORT of `pkg/planner/core/runtime_filter_generator_test.go:34
/// TestRuntimeFilterGenerator`.
///
/// Re-derived contract: live session over mock store; analyzed tables
/// `t1/t2` given available TiFlash replicas (:45-68); join_reorder added to
/// the opt-rule blacklist (:51-52); `set tidb_runtime_filter_mode=LOCAL`
/// (:80); failpoint `mockPreferredBuildIndex return(0)` pins the build side
/// (:81-84). Each input from the `runtime_filter_generator_suite` BookKeeper
/// data is explained as `explain format='plan_tree'` and compared against
/// recorded goldens (:85-91) — the generator must inject runtime-filter
/// operators into hash-join probes exactly where the goldens show them.
#[test]
#[ignore = "go-parity-gap: needs TiFlash-replica MPP planning, runtime-filter operators and plan_tree golden rendering"]
fn runtime_filter_generator_local_mode_plan_goldens() {}
