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

//! Port ledger for `pkg/ddl/ingest/mem_root_test.go` (pkg/ddl.part7 items
//! 394-395). Both Go tests are unit tests over the lightning-backfill
//! ingest environment; the crate has no ingest memory/disk carriers, so both
//! are documentary gap ports with the contracts re-derived from the Go
//! source.

/// GO PORT of `pkg/ddl/ingest/mem_root_test.go:24 TestMemoryRoot`.
///
/// Re-derived contract over `memRootImpl` (pkg/ddl/ingest/mem_root.go:59-146):
/// `NewMemRootImpl(1024)` starts at max quota 1024 and zero usage;
/// `CheckConsume(n)` is the TOCTOU probe `currUsage+n <= maxLimit` (:122-127)
/// — so 1023 and 1024 pass and 1025 fails at zero usage; `Consume`/`Release`
/// add and subtract raw amounts (:104-113) — 512 consumed leaves 512, then
/// 512 passes and 513 fails, `Release(10)` lands on 502, and shrinking the
/// quota with `SetMaxMemoryQuota(512)` makes even 20 fail (:88-93);
/// `ConsumeWithTag` adds to the shared counter AND to the per-tag map
/// (:114-121), `ReleaseWithTag` subtracts the tag's whole recorded amount and
/// deletes the tag (:132-138), so a double release is a no-op for usage and
/// tag and untagged usage mix freely in one counter (the test ends at 522 =
/// 512 after one tagged release + 10 untagged).
#[test]
#[ignore = "go-parity-gap: no Rust carrier for ingest MemRoot (pkg/ddl/ingest/mem_root.go:59-146); tidb-util memory::tracker is pkg/util/memory.Tracker, a different Go type"]
fn ingest_mem_root_quota_gates_check_consume_against_the_configured_limit() {}

/// GO PORT of `pkg/ddl/ingest/mem_root_test.go:62 TestRiskOfDiskFull`.
///
/// Re-derived contract (pkg/ddl/ingest/disk_root.go:51, :197-199):
/// `RiskOfDiskFull(available, capacity)` is
/// `available < (1-0.9)*capacity`, i.e. remaining space strictly under 10%
/// of capacity is risk — 11 of 100 and exactly 10 of 100 are false, 9 of 100
/// is true.
#[test]
#[ignore = "go-parity-gap: no Rust carrier for ingest RiskOfDiskFull (pkg/ddl/ingest/disk_root.go:197-199)"]
fn risk_of_disk_full_flags_only_strictly_below_ten_percent_remaining() {}
