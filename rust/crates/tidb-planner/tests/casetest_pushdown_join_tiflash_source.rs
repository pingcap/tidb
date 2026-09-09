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

//! Documentary gap port for `pkg/planner/core/casetest/pushdown/push_down_test.go::TestJoinNotSupportedByTiFlash`
//! (`pkg/planner.part9` item 481 on `origin/master`; earlier entries of this
//! file belong to an already-covered batch slice).
//!
//! The Go test needs a domain-scoped testkit session with an injected virtual
//! TiFlash replica plus the MPP-aware physical planner — none of which exist
//! in the Rust workspace yet.

/// GO PORT of
/// `pkg/planner/core/casetest/pushdown/push_down_test.go:216
/// TestJoinNotSupportedByTiFlash`.
///
/// Re-derived contract: table_1(id, bit(2), datetime) has composite index
/// idx(id, bit_col, datetime_col) and two seeded rows (:222-226);
/// `dayofmonth` goes into mysql.expr_pushdown_blacklist for tiflash followed
/// by `admin reload expr_pushdown_blacklist` (:232-233); a virtual TiFlash
/// replica marks the table (`testkit.SetTiFlashReplica`, :237) and the session
/// reads only from tiflash engines with mpp allowed (:238-239). Every
/// integration-suite query must explain exactly per its recorded plan_tree
/// golden (:244-251); then broadcast join thresholds drop to 1 (:252-253) and
/// the SAME book replays with re-recorded goldens (:254-260), pinning that
/// joins unsupported by TiFlash shift shape rather than fail once broadcast
/// is effectively disabled.
#[test]
#[ignore = "go-parity-gap: TiFlash replica metadata, MPP planning decision and explain rendering are unported"]
fn join_not_supported_by_tiflash_golden_under_broadcast_thresholds() {}
