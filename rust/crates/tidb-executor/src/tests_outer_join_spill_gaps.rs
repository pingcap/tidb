// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap inventory for Go `pkg/executor/join/outer_join_spill_test.go`.
//! The source builds a v2 join with `buildHashJoinV2Exec` from
//! `pkg/executor/join/hash_join_test_util.go:53`; spill accounting is driven by
//! `testSpill` in `hash_join_spill_helper.go` and uses Go failpoints.

/// Go `pkg/executor/join/outer_join_spill_test.go:146::TestOuterJoinSpillBasic1`; the six `spillTestParam` cases assert exact spill byte counts.
#[test]
#[ignore = "go-parity-gap: outer-join spill drivers and exact byte accounting are unported"]
fn outer_join_spill_basic_left_preserves_results() {}

/// Go `pkg/executor/join/outer_join_spill_test.go:198::TestOuterJoinSpillBasic2`; the right-outer matrix starts at `outer_join_spill_test.go:198`.
#[test]
#[ignore = "go-parity-gap: right-outer spill fixtures and exact byte accounting are unported"]
fn outer_join_spill_basic_right_preserves_results() {}

/// Go `pkg/executor/join/outer_join_spill_test.go:250::TestOuterJoinSpillWithSel`; selected data sources are constructed at `outer_join_spill_test.go:261`.
#[test]
#[ignore = "go-parity-gap: selected-chunk spill fixtures are unported"]
fn outer_join_spill_honors_selection_vectors() {}

/// Go `pkg/executor/join/outer_join_spill_test.go:301::TestOuterJoinSpillWithOtherCondition`; the source adds a `GT` condition at `outer_join_spill_test.go:330`.
#[test]
#[ignore = "go-parity-gap: condition-aware spill execution and worker fixtures are unported"]
fn outer_join_spill_applies_other_conditions() {}

/// Go `pkg/executor/join/outer_join_spill_test.go:361::TestOuterJoinUnderApplyExec`; repeated open/close is driven by `testUnderApplyExec` at `outer_join_spill_test.go:405`.
#[test]
#[ignore = "go-parity-gap: Apply reopen harness and physical-plan fixtures are unported"]
fn outer_join_spill_reopens_under_apply() {}

/// Go `pkg/executor/join/outer_join_spill_test.go:410::TestFallBackAction`; the source checks `newRootExceedAction.GetTriggeredNum()` at `outer_join_spill_test.go:423`.
#[test]
#[ignore = "go-parity-gap: memory tracker fallback-action observation is not exposed by tidb-executor"]
fn outer_join_spill_triggers_the_fallback_action() {}

/// Go `pkg/executor/join/outer_join_spill_test.go:427::TestIssue59377`; the `Issue59377` failpoint and cleanup assertion are at `outer_join_spill_test.go:443`.
#[test]
#[ignore = "go-parity-gap: failpoint-driven mid-stream spill errors and cleanup are unported"]
fn outer_join_spill_cleans_up_after_mid_stream_error() {}

/// Go `pkg/executor/join/outer_join_spill_test.go:452::TestHashJoinRandomFail`; random worker failure is injected at `outer_join_spill_test.go:483`.
#[test]
#[ignore = "go-parity-gap: random worker failpoints and distributed close behavior are unported"]
fn outer_join_spill_surfaces_random_worker_failures() {}
