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

//! Documentary gap ports for `pkg/planner/core/casetest/vectorsearch/`
//! (`pkg/planner.part10` items 541-549 on `origin/master`): `main_test.go`
//! TestMain and `vector_index_test.go`. Every body needs a mock-store session
//! with injected virtual TiFlash replicas (`testkit.SetTiFlashReplica`), the
//! HNSW vector-index DDL failpoint `MockCheckColumnarIndexProcess`, and
//! plan_tree golden books (`ann_index_suite`) — none of which exist in this
//! workspace yet.

/// GO PORT of `pkg/planner/core/casetest/vectorsearch/main_test.go:30
/// TestMain`.
///
/// Bootstrap only: loads the `ann_index_suite` book (:34), zeroes the async
/// commit safety windows and enables the stats memory quota (:35-39), and
/// wraps the suite in goleak — no assertions of its own.
#[test]
#[ignore = "go-parity-gap: bootstrap harness (suite book loading + goleak), no Rust twin by design"]
fn main_loads_ann_index_suite_book() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:50
/// TestVectorIndexProtobufMatch`.
///
/// Contract (:51): the tipb enum's wire string equals the model constant —
/// `tipb.VectorDistanceMetric_INNER_PRODUCT.String()` ==
/// `model.DistanceMetricInnerProduct` (Go source: `pkg/meta/model/index.go:44`
/// pins `"INNER_PRODUCT"`; the name map lives in go-tipb's generated
/// `executor.pb.go`). The Rust workspace carries only the model half today:
/// `tidb-model/src/index.rs::distance_metric::INNER_PRODUCT`; the tipb
/// executor proto leaf is not generated into `tidb-proto`, so the coupling is
/// recorded rather than approximated.
#[test]
#[ignore = "go-parity-gap: tidb-proto does not generate tipb VectorDistanceMetric, so the enum-string == model-constant pin cannot run"]
fn vector_index_protobuf_metric_string_matches_model_constant() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:54
/// TestTiFlashANNIndex`.
///
/// Contract (:54-118): t1 over vector(3)/int columns with a cosine HNSW index
/// and 64 seeded rows analyzed (:65-82); a virtual TiFlash replica plus
/// isolation-read-engine narrowing to tiflash (:86,:93); every golden query's
/// plan_tree rows and warning strings replay exactly from `ann_index_suite`
/// (:94-117) — pinning ANN-aware physical shapes end-to-end.
#[test]
#[ignore = "go-parity-gap: mock TiFlash replicas, HNSW DDL failpoint and plan_tree goldens need the unported session stack"]
fn tiflash_ann_index_plans_match_golden_book() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:119
/// TestANNIndexNormalizedPlan`.
///
/// Contract (:137-217): the normalized plan and digest produced by
/// `NormalizePlan` must EQUAL those of `FlattenPhysicalPlan`+`NormalizeFlatPlan`
/// (:140-146); TopN-over-cosine-limit plans normalize stably regardless of the
/// literal argument vector (:168-177); flipping the ORDER of arguments changes
/// the projection so the digest changes (:179-181); making the TiFlash replica
/// unavailable replays the same shape (:186-199) and restoring it restores the
/// original digest (:200-203).
#[test]
#[ignore = "go-parity-gap: normalized-plan/digest renderers over executed plans are outside this crate"]
fn ann_index_normalized_plan_digests_stable_across_literals_and_replica_state() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:220
/// TestANNInexWithSimpleCBO`.
///
/// Contract (:220-232): under simple CBO, `select * from t1 order by
/// vec_cosine_distance(vec, '[1,1,1]') limit 1` MUST use the vector_index
/// hint target — the index-based ANN path wins on cost.
#[test]
#[ignore = "go-parity-gap: cost-based ANN path selection needs TiFlash replica metadata plus vector costing"]
fn ann_index_with_simple_cbo_prefers_vector_index() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:253
/// TestANNIndexWithNonIntClusteredPk`.
///
/// Contract (:253-316): a composite non-int clustered PK table with a cosine
/// HNSW index, planned via Preprocess+Optimize, yields a PhysicalTableReader
/// whose TableScan carries exactly one UsedColumnarIndex of type
/// ColumnarIndexType_TypeVector, one range `[-inf,+inf]` whose bounds are
/// KindMinNotNull / KindMaxValue — full scan + ANN payload coexistence.
#[test]
#[ignore = "go-parity-gap: physical tree inspection of columnar index payloads needs the optimize pipeline"]
fn ann_index_non_int_clustered_pk_keeps_full_range_and_columnar_payload() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:318
/// TestVectorSearchWithPKAuto`.
///
/// Contract (:318-377): mixed workloads over a 6000-row vector table with a
/// cos-play VECTOR INDEX and a plain doc table auto-select between ANN index,
/// TiFlash and TiKV paths per query; every plan_tree row and warning replays
/// from `ann_index_suite`.
#[test]
#[ignore = "go-parity-gap: auto engine selection over vector tables needs session + TiFlash plumbing"]
fn vector_search_pk_auto_engine_selection_matches_golden() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:378
/// TestVectorSearchWithPKForceTiKV`.
///
/// Same fixture family as :318 but forced onto TiKV read paths (:378-438);
/// goldens pin that vector distance functions still plan (without ANN payload)
/// when TiKV is the isolation-read engine.
#[test]
#[ignore = "go-parity-gap: forced-TiKV vector planning needs session + engine isolation controls"]
fn vector_search_pk_force_tikv_matches_golden() {}

/// GO PORT of
/// `pkg/planner/core/casetest/vectorsearch/vector_index_test.go:440
/// TestVectorSearchHeavyFunction`.
///
/// Contract (:440-502): joins between a heavy 6000-row vector table and a doc
/// table with TiFlash replica; golden queries mix set/UPDATE statements
/// (:477-480 bypassing MustQuery) and check both plans and warnings.
#[test]
#[ignore = "go-parity-gap: join-heavy vector golden suite needs executor-backed sessions"]
fn vector_search_heavy_function_goldens_replay() {}
