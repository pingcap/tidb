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

//! `pkg/planner.part14` DOCUMENTED GAP ports for the two TOP-LEVEL plan
//! cache suites:
//!
//! * `pkg/planner/core/plan_cache_instance_test.go:47
//!   TestInstancePlanCacheSuite`
//! * `pkg/planner/core/plan_cache_lru_test.go:49 TestLRUPlanCacheSuite`
//!
//! Both are honest `#[ignore]` gap ports. The Go suites exercise
//! `core.LRUPlanCache` (`pkg/planner/core/plan_cache_lru.go:65`) and the
//! `sessionctx.InstancePlanCache` implementation over
//! `coretestsdk.MockContext()` sessions plus a Domain `StatsHandle`; this
//! crate has neither. The workspace's session-tier plan caches
//! (`tidb-session`'s `prepared_plan_cache.rs`/`non_prepared_plan_cache.rs`)
//! are `pub(crate)` with a different key design and are outside this batch's
//! gate crate. Nothing is approximated: each entry records the re-derived Go
//! contract.

/// GO PARITY GAP port of `pkg/planner/core/plan_cache_lru_test.go:49
/// TestLRUPlanCacheSuite`.
///
/// go-parity-gap: `NewLRUPlanCache` and its container/list + bucket internals
/// are unported. The suite subtests pin, over a `MockContext` session:
/// `Put` clamps a 0 capacity to 100 (:65), evicts the oldest same-key entry
/// (onEvict counted :89-93) keeping `size == capacity` (:85), a key maps to
/// multi values keyed by param types (:75-83); `Get` returns the LRU head
/// value and refreshes recency, misses on param-type mismatch; `Delete`
/// removes exactly one entry, `DeleteAll` clears every bucket; `SetCapacity`
/// shrinks/grows with eviction to the new bound; regression cases pin
/// memory-guard behavior (quota overridde eviction, stats-hash keying); and
/// `MemoryUsage` sums per-entry memory against the guard threshold.
#[test]
#[ignore = "go-parity-gap: LRUPlanCache (plan_cache_lru.go:65) container/bucket internals and MockContext session unported"]
fn lru_plan_cache_put_get_delete_capacity_guard_suite() {}

/// GO PARITY GAP port of `pkg/planner/core/plan_cache_instance_test.go:47
/// TestInstancePlanCacheSuite`.
///
/// go-parity-gap: the domain-scoped instance plan cache carrier and its
/// `sessionctx.InstancePlanCache` interface are unported (the crate's only
/// traces are the sysvar defaults recorded in tidb-session's optimizer
/// sysvar catalog). The suite subtests pin, over `MockContext` sessions:
/// basic put/get/hit/miss with memory accounting (:55-133); `Put` matching
/// on param types and refusing entries over the memory quota
/// (:135-197); `EvictAll` clearing plus restricted-key eviction (:199-218);
/// and concurrent reads (:220-255) and mixed concurrent write/read
/// (:257-end) without data races or lost entries.
#[test]
#[ignore = "go-parity-gap: sessionctx.InstancePlanCache implementation + Domain StatsHandle session stack unported"]
fn instance_plan_cache_basic_match_opts_evict_concurrency_suite() {}
