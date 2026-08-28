// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap inventory for Go `pkg/executor/join/right_outer_join_probe_test.go`.
//! The nested-loop contract is `genRightOuterJoinResult` at line 33; the Go
//! tests execute the v2 implementation through `buildHashJoinV2Exec` at
//! `pkg/executor/join/hash_join_test_util.go:53`.

/// Go `pkg/executor/join/right_outer_join_probe_test.go:109::TestRightOuterJoinProbeBasic`; the right side is preserved and left columns are NULL-padded by `right_outer_join_probe_test.go:33`.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2 MockDataSource fixture matrix is unported"]
fn right_outer_join_probe_basic_preserves_unmatched_right_rows() {}

/// Go `pkg/executor/join/right_outer_join_probe_test.go:166::TestRightOuterJoinProbeAllJoinKeys`; scalar and composite key cases are enumerated at `right_outer_join_probe_test.go:166`.
#[test]
#[ignore = "go-parity-gap: all-key-type probe execution requires Go-only planner and chunk fixtures"]
fn right_outer_join_probe_covers_all_join_key_layouts() {}

/// Go `pkg/executor/join/right_outer_join_probe_test.go:254::TestRightOuterJoinProbeOtherCondition`; condition evaluation is part of `genRightOuterJoinResult` at `right_outer_join_probe_test.go:83`.
#[test]
#[ignore = "go-parity-gap: other-condition HashJoinV2 probe fixtures are unported"]
fn right_outer_join_probe_filters_key_matches_by_other_condition() {}

/// Go `pkg/executor/join/right_outer_join_probe_test.go:294::TestRightOuterJoinProbeWithSel`; selected input is passed to `testJoinProbe` at `right_outer_join_probe_test.go:314`.
#[test]
#[ignore = "go-parity-gap: selection-vector probe fixtures are not available in tidb-executor"]
fn right_outer_join_probe_honors_selection_vectors() {}
