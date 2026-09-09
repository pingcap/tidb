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

//! Running port of Go
//! `pkg/executor/aggfuncs/aggfunc_test.go:1331::TestAggApproxCountDistinctPushDown`.
//! The aggregate push-down decision is transcreated in `tidb-expr`
//! (`aggregation/mod.rs`: Go `aggregation.CheckAggPushDown`,
//! `pkg/expression/aggregation/aggregation.go:223`), which this crate
//! depends on, so the store matrix below executes for real.

use std::collections::HashMap;

use tidb_datatype::{FieldType, FieldTypeCode};
use tidb_expr::aggregation::{check_agg_push_down, names, AggFuncDesc};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::infer_pushdown::PushDownStore;
use tidb_expr::NoColumns;

/// Go `pkg/executor/aggfuncs/aggfunc_test.go:1331::TestAggApproxCountDistinctPushDown`:
/// `approx_count_distinct` over a single `TypeLonglong` column can only be
/// pushed to TiFlash -- true for `kv.TiFlash`, false for `kv.TiKV`,
/// `kv.TiDB` and `kv.UnSpecified`, with an empty
/// `expr_pushdown_blacklist`.
#[test]
fn agg_approx_count_distinct_push_down_is_tiflash_only() {
    let arg = Expression::Column(Column::new(1, FieldType::new(FieldTypeCode::LongLong)));
    let agg = AggFuncDesc::new(&NoColumns, names::APPROX_COUNT_DISTINCT, vec![arg], false)
        .expect("Go aggregation.NewAggFuncDesc builds the descriptor");

    let blacklist: HashMap<String, u32> = HashMap::new();
    assert!(check_agg_push_down(
        &agg,
        PushDownStore::TiFlash,
        &blacklist
    ));
    assert!(!check_agg_push_down(&agg, PushDownStore::TiKv, &blacklist));
    assert!(!check_agg_push_down(&agg, PushDownStore::TiDb, &blacklist));
    assert!(!check_agg_push_down(
        &agg,
        PushDownStore::Unspecified,
        &blacklist
    ));
}
