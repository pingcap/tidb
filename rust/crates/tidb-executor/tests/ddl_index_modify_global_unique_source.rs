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

//! Port ledger for `pkg/ddl/index_modify_test.go:1972
//! TestInsertDuplicateBeforeIndexMerge` (pkg/ddl.part7 item 361 of the local
//! enumeration).

/// GO PORT of `pkg/ddl/index_modify_test.go:1972
/// TestInsertDuplicateBeforeIndexMerge`.
///
/// Re-derived contract (regression for issue #57414): during the MERGE phase
/// of an ingest add-index on a hash-partitioned table, a duplicate-key DML
/// admitted at `beforeBackfillMerge` (`insert ignore ... on duplicate key
/// update`) must not desync the new index — both shapes (existing GLOBAL
/// unique index `i1(col2)` then adding `i2(col1, col2)`; and existing local
/// unique `i1(col1, col2)` then adding GLOBAL unique `i2(col2)`) complete
/// with `admin check table` clean. Go skips it on nextgen kernels where
/// add-index always runs via DXF ingest.
#[test]
#[ignore = "go-parity-gap: needs the ingest backfill merge phase and admin check table over partitioned global indexes, none transcreated"]
fn insert_duplicate_before_index_merge_keeps_the_new_index_consistent() {}
