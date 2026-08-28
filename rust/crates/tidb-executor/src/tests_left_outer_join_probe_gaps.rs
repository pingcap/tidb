// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap inventory for Go `pkg/executor/join/left_outer_join_probe_test.go`.
//! Each test drives `buildHashJoinV2Exec` from
//! `pkg/executor/join/hash_join_test_util.go:53` and compares with the nested
//! loop reference `genLeftOuterJoinResult` at `left_outer_join_probe_test.go:16`.

/// Go `pkg/executor/join/left_outer_join_probe_test.go:109::TestLeftOuterJoinProbeBasic`; `testJoinProbe` is defined at `pkg/executor/join/inner_join_probe_test.go:228`.
#[test]
#[ignore = "go-parity-gap: the HashJoinV2 MockDataSource fixture matrix is unported"]
fn left_outer_join_probe_basic_preserves_unmatched_left_rows() {}

/// Go `pkg/executor/join/left_outer_join_probe_test.go:166::TestLeftOuterJoinProbeAllJoinKeys`; the source covers scalar and composite key layouts through `pkg/executor/join/left_outer_join_probe_test.go:166`.
#[test]
#[ignore = "go-parity-gap: all-key-type probe execution requires Go-only planner and chunk fixtures"]
fn left_outer_join_probe_covers_all_join_key_layouts() {}

/// Go `pkg/executor/join/left_outer_join_probe_test.go:255::TestLeftOuterJoinProbeOtherCondition`; `genLeftOuterJoinResult` evaluates conditions at `left_outer_join_probe_test.go:72`.
#[test]
#[ignore = "go-parity-gap: other-condition HashJoinV2 probe fixtures are unported"]
fn left_outer_join_probe_filters_key_matches_by_other_condition() {}

/// Go `pkg/executor/join/left_outer_join_probe_test.go:296::TestLeftOuterJoinProbeWithSel`; selected input handling is implemented by the source reference at `left_outer_join_probe_test.go:50`.
#[test]
#[ignore = "go-parity-gap: selection-vector probe fixtures are not available in tidb-executor"]
fn left_outer_join_probe_honors_selection_vectors() {}
