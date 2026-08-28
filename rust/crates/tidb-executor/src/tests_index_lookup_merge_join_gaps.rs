// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache 2.0 license (see the License file at the crate root).

//! Gap inventory for Go `pkg/executor/join/test/indexjoin/index_lookup_merge_join_test.go`.
//! These tests require session testkit hint plans and Go failpoints.

/// Go `pkg/executor/join/test/indexjoin/index_lookup_merge_join_test.go:25::TestIssue18068`; repeated executions and the `testIssue18068` failpoint are set at lines 31-49.
#[test]
#[ignore = "go-parity-gap: hinted index-merge-join execution and repeated worker lifecycle are unported"]
fn index_lookup_merge_join_issue_18068_does_not_hang_on_repeated_execution() {}

/// Go `pkg/executor/join/test/indexjoin/index_lookup_merge_join_test.go:49::TestIssue54064`; result rows and the negative `IndexMergeJoin` plan assertion are at lines 69-88.
#[test]
#[ignore = "go-parity-gap: session hint plans and explain-format plan assertions are unported"]
fn index_lookup_merge_join_issue_54064_avoids_wrong_plan() {}
