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

//! Documentary gap ports for `pkg/planner/core/tests/extractor`
//! (`pkg/planner.part15` items 877–882 on `origin/master`).
//!
//! `main_test.go:25 TestMain` only bootstraps the testkit book (skipped-
//! reason in the batch receipt). The five real tests all run
//! `select * from information_schema.<memtable> where <conds>` through a live
//! session — exercising the memtable infoschema EXTRACTORS' predicate
//! pushdown/lookups — so each is recorded as a gap whose contract is quoted
//! from the Go bodies.
//!
//! | Go function | Rust test |
//! | --- | --- |
//! | `extractor/main_test.go:25 TestMain` | — skipped-reason |
//! | `memtable_infoschema_extractor_test.go:426 TestMemtableInfoschemaExtractorPart1` | [`memtable_infoschema_extractor_part1_tables_views_indexes`] |
//! | `memtable_infoschema_extractor_test.go:447 TestMemtableInfoschemaExtractorPart2` | [`memtable_infoschema_extractor_part2_key_column_constraints_partitions`] |
//! | `memtable_infoschema_extractor_test.go:471 TestMemtableInfoschemaExtractorPart3` | [`memtable_infoschema_extractor_part3_statistics_schemata_check_constraints`] |
//! | `memtable_infoschema_extractor_test.go:492 TestMemtableInfoschemaExtractorPart4` | [`memtable_infoschema_extractor_part4_sequences_index_usage_tidb_checks`] |
//! | `memtable_infoschema_extractor_test.go:514 TestInfoSchemaTableNameLikeEscape` | [`infoschema_table_name_like_custom_escape`] |

/// GO PORT of `pkg/planner/core/tests/extractor/
/// memtable_infoschema_extractor_test.go:426 TestMemtableInfoschemaExtractorPart1`.
///
/// Re-derived contract via `testMemtableInfoschemaExtractor` (:401-424):
/// create index/view fixture data (`prepareDataTiDBIndexes/Tables/Views`),
/// then for EVERY condition produced by the case builders run
/// `select * from information_schema.(tidb_indexes|tables|views) where …`
/// through a live session requiring no error — pinning that each extractor's
/// pushed-down lookups return consistent result sets instead of diverging.
#[test]
#[ignore = "go-parity-gap: needs live memtable query execution and extractor plumbing"]
fn memtable_infoschema_extractor_part1_tables_views_indexes() {}

/// GO PORT of `…/memtable_infoschema_extractor_test.go:447
/// TestMemtableInfoschemaExtractorPart2`.
///
/// Same harness over `key_column_usage`, `table_constraints`, and
/// `partitions` using REPRESENTATIVE conditions (`buildRepresentativeConditions`,
/// covering one equality per extractor-supported column) rather than full
/// cartesian conditions (:452-469).
#[test]
#[ignore = "go-parity-gap: needs live memtable query execution and extractor plumbing"]
fn memtable_infoschema_extractor_part2_key_column_constraints_partitions() {}

/// GO PORT of `…/memtable_infoschema_extractor_test.go:471
/// TestMemtableInfoschemaExtractorPart3`.
///
/// Full-cartesian conditions over `statistics`, `schemata`, and
/// `check_constraints` (:475-490).
#[test]
#[ignore = "go-parity-gap: needs live memtable query execution and extractor plumbing"]
fn memtable_infoschema_extractor_part3_statistics_schemata_check_constraints() {}

/// GO PORT of `…/memtable_infoschema_extractor_test.go:492
/// TestMemtableInfoschemaExtractorPart4`.
///
/// Representative conditions over `tidb_check_constraints`, `sequences`, and
/// `tidb_index_usage` (the last reusing the statistics fixtures) (:497-512).
#[test]
#[ignore = "go-parity-gap: needs live memtable query execution and extractor plumbing"]
fn memtable_infoschema_extractor_part4_sequences_index_usage_tidb_checks() {}

/// GO PORT of `…/memtable_infoschema_extractor_test.go:514
/// TestInfoSchemaTableNameLikeEscape` (#69653 regression).
///
/// Re-derived contract: database `like_escape` holds ``abc_def`` and
/// ``abc#x``; the query
/// `… where table_schema='like_escape' and table_name like '%#_%'
/// escape '#'` must return EXACTLY the row `"abc_def 1"` (:531-537): with a
/// custom ESCAPE character, `\#_` means literal underscore, so only
/// ``abc_def`` matches AND the self-evaluated LIKE projection is TRUE — i.e.
/// the extractor compiled the pattern with '#' as escape rather than falling
/// back to '\'.
#[test]
#[ignore = "go-parity-gap: LIKE-ESCAPE compilation inside memTableExtractor unported; needs query execution"]
fn infoschema_table_name_like_custom_escape() {}
