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

//! Port of Go `pkg/ddl/column_test.go` (part2 slice:
//! `TestModifyColumnWithIndex`, line 920 — the file's last test; the rest
//! belongs to part1).
//!
//! Go counts the index records the modify-column reorg backfills through the
//! `addIndexTxnMsgBackfillData` failpoint (`cnt += idxRecordNum`), with
//! `tidb_ddl_reorg_worker_cnt = 1`.

/// `pkg/ddl/column_test.go::TestModifyColumnWithIndex` (line 920): a table
/// with nine indexes on `a` and `b`; `modify column a char(4)` (varchar→char,
/// lossless) backfills 6 index records per worker pass, `modify column b
/// bigint` backfills 0 (the int→bigint index encoding is unchanged so no
/// index entries are rewritten), `modify column b int UNSIGNED` backfills 6
/// (signedness changes the encoding), and the combined `modify column a
/// varchar(2), modify column b int` backfills 18.
// go-parity-gap: the pinned observable is the reorg backfill record count;
// this crate applies modify-column metadata without a backfill phase (and
// refuses the lossy indexed type changes instead).
#[test]
#[ignore = "go-parity-gap: modify-column index backfill counting needs the reorg phase"]
fn modify_column_with_index_backfill_counts() {}
