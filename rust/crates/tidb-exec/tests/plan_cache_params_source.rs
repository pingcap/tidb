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

//! Source-backed tests for plan-cache parameter-list behavior.

use tidb_datatype::Datum;
use tidb_exec::plan_cache_params::PlanCacheParamList;

#[test]
fn plan_cache_params_preserve_values_across_snapshot_and_reset() {
    // Source: pkg/sessionctx/variable/session.go:2223-2270 and
    // pkg/expression/exprstatic/evalctx_test.go:418-439 (TestParamList).
    let mut params = PlanCacheParamList::new();
    params.append([Datum::new_int(1), Datum::new_int(2), Datum::new_int(3)]);
    assert_eq!(params.all_param_values().len(), 3);
    assert_eq!(params.get_param_value(0).as_int(), Some(1));
    assert_eq!(params.get_param_value(2).as_int(), Some(3));

    // EvalContext copies the source list; resetting the producer must not
    // mutate the copied snapshot used by an in-flight statement.
    let snapshot = params.clone();
    params.reset();
    params.push(Datum::new_int(4));
    assert_eq!(params.all_param_values().len(), 1);
    assert_eq!(params.get_param_value(0).as_int(), Some(4));
    assert_eq!(snapshot.all_param_values().len(), 3);
    assert_eq!(snapshot.get_param_value(1).as_int(), Some(2));
}

#[test]
fn plan_cache_params_keep_non_prepared_privacy_bit_separate() {
    // Source: pkg/sessionctx/variable/session.go:2247-2251. The privacy bit
    // is metadata only; String rendering remains an external owner.
    let mut params = PlanCacheParamList::new();
    assert!(!params.for_non_prep_cache());
    params.set_for_non_prep_cache(true);
    assert!(params.for_non_prep_cache());
    params.reset();
    assert!(!params.for_non_prep_cache());
}
