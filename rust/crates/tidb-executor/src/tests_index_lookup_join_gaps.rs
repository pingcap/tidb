// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap inventory for Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go`.
//! Every case is a session testkit driver that asserts an index-join plan,
//! session variables, storage behavior, or failpoint-driven worker cleanup.
//! The current executor test boundary has no equivalent complete seam. In
//! particular, Go's NULL-safe lookup branch is in
//! `pkg/executor/join/index_lookup_join.go:654-676`.

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:31::TestIndexLookupJoinHang`; the three hinted plans and repeated `Next` calls are defined at lines 44-68.
#[test]
#[ignore = "go-parity-gap: hint-driven session plans and mid-stream worker errors are unported"]
fn index_lookup_join_closes_after_inner_expression_error() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:74::TestIndexJoinNullEQ`; NULL probes require `constructDatumLookupKey` at `pkg/executor/join/index_lookup_join.go:654-676`.
#[test]
#[ignore = "go-parity-gap: the HashIsNullEQ lookup-key branch and plan assertions are unported"]
fn index_join_null_safe_equal_matches_null_keys() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:102::TestIndexJoinNullEQMultiKey`; composite NULL-safe key behavior is asserted at lines 119-130.
#[test]
#[ignore = "go-parity-gap: composite NULL-safe index lookup is not exposed by tidb-executor"]
fn index_join_null_safe_equal_matches_composite_null_keys() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:134::TestIndexJoinNullEQUniqueKey`; the unique-key NULL case is asserted at lines 151-159.
#[test]
#[ignore = "go-parity-gap: unique-index NULL-safe lookup and testkit plans are unported"]
fn index_join_null_safe_equal_matches_one_unique_null_row() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:162::TestIndexJoinNullEQOuterJoin`; left/right preserved-side results are asserted at lines 181-207.
#[test]
#[ignore = "go-parity-gap: NULL-safe index lookup plus outer-join plan execution is unported"]
fn index_join_null_safe_equal_preserves_outer_rows() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:210::TestIssue16887`; the 70-row result and warnings check are at `index_lookup_join_test.go:217-223`.
#[test]
#[ignore = "go-parity-gap: index-merge-join hint plans and SHOW WARNINGS are unported"]
fn index_join_issue_16887_returns_all_rows() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:226::TestPartitionTableIndexJoinAndIndexReader`; dynamic pruning and 512 randomized comparisons are driven at lines 230-255.
#[test]
#[ignore = "go-parity-gap: partitioned testkit storage, dynamic pruning, and TIDB_INLJ plans are unported"]
fn partitioned_index_join_matches_unpartitioned_reader() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:257::TestIssue45716`; the `inlNewInnerPanic` failpoint is enabled at lines 270-273.
#[test]
#[ignore = "go-parity-gap: failpoint injection into the index-join inner build is unported"]
fn index_join_issue_45716_surfaces_inner_build_error() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:276::TestIssue54688`; cancellation is injected by `joinMatchedInnerRow2Chunk` at lines 299-305.
#[test]
#[ignore = "go-parity-gap: cancellation, GOMAXPROCS, and repeated index-join worker cleanup are unported"]
fn index_join_issue_54688_repeated_cancellation_closes_cleanly() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:314::TestIssue54055`; count-scheduled failpoints are enabled at lines 369-375.
#[test]
#[ignore = "go-parity-gap: the two count-scheduled index-join failpoint seams are unported"]
fn index_join_issue_54055_surfaces_ordered_execution_error() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_join_test.go:341::TestIndexJoinInnerCTEStorageConcurrentBuild`; CTE and index-join plan assertions are at lines 374-389.
#[test]
#[ignore = "go-parity-gap: concurrent CTE inner storage, session variables, and index-join plans are unported"]
fn index_join_inner_cte_storage_builds_concurrently() {}
