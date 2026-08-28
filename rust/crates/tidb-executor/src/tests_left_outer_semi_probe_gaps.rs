// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap inventory for Go `pkg/executor/join/left_outer_semi_join_probe_test.go`.
//! The source reference `genLeftOuterSemiOrSemiJoinOrLeftOuterAntiSemiResultImpl`
//! at line 47 defines the flag-column and NULL-condition behavior; execution
//! is delegated to `buildHashJoinV2Exec` at `hash_join_test_util.go:53`.

/// Go `pkg/executor/join/left_outer_semi_join_probe_test.go:477::TestLeftOuterSemiJoinProbeBasic`.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2 and MockDataSource probe fixture layer is unported"]
fn left_outer_semi_probe_basic_emits_the_match_flag() {}

/// Go `pkg/executor/join/left_outer_semi_join_probe_test.go:481::TestLeftOuterSemiJoinProbeAllJoinKeys`; the shared key matrix is `left_outer_semi_join_probe_test.go:241::testLeftOuterSemiJoinProbeAllJoinKeys`.
#[test]
#[ignore = "go-parity-gap: all-key-type probe execution requires Go-only chunk and planner fixtures"]
fn left_outer_semi_probe_covers_all_join_key_layouts() {}

/// Go `pkg/executor/join/left_outer_semi_join_probe_test.go:485::TestLeftOuterSemiJoinProbeOtherCondition`; NULL condition outcomes become a NULL flag through the helper at `left_outer_semi_join_probe_test.go:104`.
#[test]
#[ignore = "go-parity-gap: condition evaluation and HashJoinV2 probe fixtures are unported"]
fn left_outer_semi_probe_tracks_other_condition_outcomes() {}

/// Go `pkg/executor/join/left_outer_semi_join_probe_test.go:489::TestLeftOuterSemiJoinProbeWithSel`; selected chunks are exercised by `left_outer_semi_join_probe_test.go:414::testLeftOuterSemiJoinProbeWithSel`.
#[test]
#[ignore = "go-parity-gap: selected-chunk HashJoinV2 fixtures are not exposed by tidb-executor"]
fn left_outer_semi_probe_honors_selection_vectors() {}

/// Go `pkg/executor/join/left_outer_semi_join_probe_test.go:493::TestLeftOuterSemiJoinBuildResultFastPath`; the source fast path is `left_outer_semi_join_probe_test.go:497`.
#[test]
#[ignore = "go-parity-gap: build-result fast-path and InOperand fixtures are unported"]
fn left_outer_semi_build_result_fast_path_matches_slow_path() {}

/// Go `pkg/executor/join/left_outer_semi_join_probe_test.go:560::TestLeftOuterSemiJoinSpill`; spill execution is defined by `left_outer_semi_join_probe_test.go:564`.
#[test]
#[ignore = "go-parity-gap: spill storage, worker failpoints, and leak-file checks are unported"]
fn left_outer_semi_spill_preserves_results() {}
