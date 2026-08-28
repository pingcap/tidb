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

//! Port ledger for `pkg/ddl/index_cop_test.go:35
//! TestAddIndexFetchRowsFromCoprocessor` (`pkg/ddl.part6` batch b105, item
//! 335 of the pkg/ddl enumeration).
//!
//! The Go test drives `NewReorgCopContext` + `FetchChunk4Test` +
//! `ConvertRowToHandleAndDatum` -- the coprocessor row-fetch half of index
//! backfill -- over three handle encodings and checks the (handle, index
//! datum) pairs the fetch produces. That backfill-fetch machinery is not
//! transcreated in this tier.

/// GO PORT of `pkg/ddl/index_cop_test.go:35
/// TestAddIndexFetchRowsFromCoprocessor`.
///
/// Re-derived contract (index_cop_test.go:35-120): the reorg cop context for
/// a single-index backfill is a `*copr.CopContextSingleIndex`; fetching the
/// table's record-prefix range yields one (handle, index-value) pair per
/// row, where the handle is
/// * the IMPLICIT `_tidb_rowid` (i+1) for a nonclustered table
///   `t (a bigint, b int, index idx (b))`,
/// * the PK value `a` itself for a `pk_is_handle` table
///   (`a bigint primary key`), and
/// * the `{a, c}` common-handle string for a clustered common-handle table
///   (`primary key (a, c) clustered`),
/// and the index datum is always the `b` value.
#[test]
#[ignore = "go-parity-gap: NewReorgCopContext/FetchChunk4Test/ConvertRowToHandleAndIndexDatum -- the coprocessor backfill fetch -- are not transcreated"]
fn add_index_fetch_rows_from_coprocessor_across_handle_kinds() {}
