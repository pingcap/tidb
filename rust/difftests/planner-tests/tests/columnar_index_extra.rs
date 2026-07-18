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

//! Public-routing regression for `pkg/planner/core/columnar_index_utils.go`.

use tidb_planner::columnar_index_extra::{
    build_vector_index_extra, AnnQueryType, ColumnIdentity, DistanceMetric, IndexIdentity,
};

#[test]
fn source_vector_index_metadata_is_reachable_through_planner_crate() {
    let extra = build_vector_index_extra(
        IndexIdentity::new(100, "vector_index"),
        AnnQueryType::new(1),
        DistanceMetric::new(2),
        10,
        "vec",
        vec![1, 2, 3, 4],
        ColumnIdentity::new(3),
    );
    assert!(extra.is_vector_index());
    assert_eq!(extra.index().id(), 100);
    assert_eq!(extra.index().name(), "vector_index");
    assert_eq!(extra.query().index_id(), 100);
    assert_eq!(extra.query().query_type().raw(), 1);
    assert_eq!(extra.query().distance_metric().raw(), 2);
    assert_eq!(extra.query().top_k(), 10);
    assert_eq!(extra.query().column_name(), "vec");
    assert_eq!(extra.query().column().id(), 3);
    assert_eq!(extra.query().ref_vec_f32(), &[1, 2, 3, 4]);
}
