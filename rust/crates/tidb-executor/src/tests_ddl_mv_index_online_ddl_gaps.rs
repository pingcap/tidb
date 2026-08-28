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

//! Documented go-parity-gap port of `pkg/ddl/mv_index_test.go` (master
//! snapshot). The test pins multi-valued JSON indexes through online DDL:
//! concurrent DML interleaved via failpoint during the index backfill, and
//! the signed/unsigned array cast semantics (duplicate detection over
//! multi-valued entries, negative values overflowing the unsigned cast).

/// Go `mv_index_test.go:28::TestMultiValuedIndexOnlineDDL`. During
/// `add index idx((cast(a as signed array)))` over a 32-partition hash
/// table, a failpoint interleaves insert/delete/update of multi-valued rows
/// before every job step and `admin check table t` afterwards confirms the
/// backfill caught them all; separately, unique multi-valued indexes detect
/// the duplicate entry across `[-4,5,6]`-style arrays (1062) and
/// `cast(a as unsigned array)` answers
/// `[types:1690]constant -4 overflows bigint`.
// go-parity-gap: multi-valued (JSON array) indexes and their cast-based
// backfill are not built in this tier.
#[test]
#[ignore]
fn multi_valued_index_online_ddl_backfills_and_detects_duplicates() {
}
