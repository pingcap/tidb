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

//! Behavioral tests retained from the Go source inventory.
//! Removed empty entries and their original contracts are indexed in
//! rust/docs/parity/current-audit/empty-test-cleanup-obligations.json.

use tidb_planner::logical::projection::LogicalProjection;
use tidb_planner::logical::{schema_producer, BaseLogicalPlan};

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:308
/// TestLogicalProjectionHash64Equals`.
///
/// Ports onto the REAL merged operator `logical::projection::LogicalProjection`
/// (`hash64(schema)` / `equals(...)`), the crate's home for the generated
/// projection body (`hash64_equals_generated.go:633-681`). Sequence: equal
/// expr lists + equal schemas match (:320-330); EMPTY exprs break both
/// (:331-337) — Go then sets nil separately (:338-343), which the same
/// `Vec::new()` represents here because every list shape still differs from a
/// one-element list; `CalculateNoDelay` flips both halves (:344-355);
/// `Proj4Expand` flips them (:356-361) and resetting it restores parity
/// (:362-367). The operator feeds the producer schema as a parameter, so
/// the schema stays covered implicitly.
#[test]
fn projection_hash64_equals_tracks_exprs_flags_and_schema() {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::expression::Expression;
    use tidb_expr::schema::Schema;

    let column = |unique_id: i64| Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    let output = Schema::new(vec![column(1)]);
    let projection = |expr_unique_id: i64| {
        LogicalProjection::new(BaseLogicalPlan::default(), vec![Expression::Column(column(expr_unique_id))])
    };

    let p1 = projection(2);
    let mut p2 = projection(2);
    assert_eq!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(p1.equals(Some(&output), &p2, Some(&output)));

    p2 = LogicalProjection::new(BaseLogicalPlan::default(), Vec::new());
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2 = LogicalProjection::new(BaseLogicalPlan::default(), Vec::new());
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2 = projection(2);
    p2.calculate_no_delay = true;
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2.calculate_no_delay = false;
    p2.proj4_expand = true;
    assert_ne!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(!p1.equals(Some(&output), &p2, Some(&output)));

    p2.proj4_expand = false;
    assert_eq!(p1.hash64(Some(&output)), p2.hash64(Some(&output)));
    assert!(p1.equals(Some(&output), &p2, Some(&output)));

    // (extra guard for the schema half of the identity: same exprs under a
    // different producer schema are a different operator)
    let other_output = Schema::new(vec![column(9)]);
    assert_ne!(p1.hash64(Some(&output)), p1.hash64(Some(&other_output)));
}

/// GO PORT of
/// `pkg/planner/core/operator/logicalop/logicalop_test/hash64_equals_test.go:479
/// TestLogicalSchemaProducerHash64Equals`.
///
/// Go builds two DataSources whose only populated identity field is the
/// embedded LogicalSchemaProducer schema, shows they match over `[col1]`
/// (:491-501) and differ over `[col2]` (:502-507). The DataSource operator
/// itself is not yet hashed in Rust, but the exact sub-surface under test —
/// `LogicalSchemaProducer.Hash64/Equals`
/// (`logical_schema_producer.go:36`,`:51`, ported as
/// `logical::schema_producer::schema_hash64/schema_equals` over the REAL
/// `tidb_expr::Schema`) — is, so the contract is pinned at that layer.
#[test]
fn schema_producer_hash64_equals_over_real_schema_columns() {
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::schema::Schema;

    let column = |unique_id: i64| Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    let d1_left = Schema::new(vec![column(1)]);
    let d1_right = Schema::new(vec![column(1)]);
    assert_eq!(
        schema_producer::schema_hash64(Some(&d1_left)),
        schema_producer::schema_hash64(Some(&d1_right))
    );
    assert!(schema_producer::schema_equals(Some(&d1_left), Some(&d1_right)));

    let d2 = Schema::new(vec![column(2)]);
    assert_ne!(
        schema_producer::schema_hash64(Some(&d1_left)),
        schema_producer::schema_hash64(Some(&d2))
    );
    assert!(!schema_producer::schema_equals(Some(&d1_left), Some(&d2)));
}
