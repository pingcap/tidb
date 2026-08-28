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

//! Port of the `pkg/domain/crossks` tests (origin/master):
//! `cross_ks_internal_test.go`'s `TestAcquireRuntimeHandle` (:95),
//! `TestEvictRuntime` (:266),
//! `TestRuntimeHandleManagerCloseClosesAllEntriesRegardlessOfIdleTimeout`
//! (:347), `TestGCLoopExitsWhenContextCancelled` (:368), and
//! `cross_ks_test.go`'s `TestManagerInClassical` (:130), `TestManager`
//! (:140), `TestDomainAcquireKSRuntimeHandle` (:412),
//! `TestDomainAlterTableModeInKeyspaceSubmitOnly` (:486).
//!
//! The package — `Manager` (cross_ks.go:68), `Acquire` (cross_ks.go:130),
//! `sweepIdleRuntimes` (cross_ks.go:407), `RunSystemKSGCLoop`
//! (cross_ks.go:390), `Close` (cross_ks.go:438) — is the cross-keyspace
//! runtime handle registry, an etcd+session-pool composition with no Rust
// home yet. Every port below is a documentary ignored gap.

#![cfg(test)]

/// Go
/// `pkg/domain/crossks/cross_ks_internal_test.go:95::TestAcquireRuntimeHandle`:
/// `Acquire` rejects an empty holderID ("holderID"), rejects on the classic
/// kernel ("cross keyspace is not available in classic kernel or current
/// keyspace"), tracks concurrent holder ids per keyspace, and only releases
/// the runtime once the LAST holder lets go.
// go-parity-gap: pkg/domain/crossks is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/crossks is not transcreated"]
fn acquire_runtime_handle() {}

/// Go
/// `pkg/domain/crossks/cross_ks_internal_test.go:266::TestEvictRuntime`:
/// `sweepIdleRuntimes` skips keyspaces with active holders, evicts only
/// entries idle longer than `crossKSRuntimeIdleTimeout`, and closing an
/// evicted runtime closes the store and session pool exactly once.
// go-parity-gap: pkg/domain/crossks is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/crossks is not transcreated"]
fn evict_runtime() {}

/// Go
/// `pkg/domain/crossks/cross_ks_internal_test.go:347::TestRuntimeHandleManagerCloseClosesAllEntriesRegardlessOfIdleTimeout`:
/// `Manager.Close` closes every runtime's session pool and store exactly
/// once, even freshly-released entries, and empties the keyspace set.
// go-parity-gap: pkg/domain/crossks is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/crossks is not transcreated"]
fn runtime_handle_manager_close_closes_all_entries_regardless_of_idle_timeout() {}

/// Go
/// `pkg/domain/crossks/cross_ks_internal_test.go:368::TestGCLoopExitsWhenContextCancelled`:
/// `RunSystemKSGCLoop` returns promptly when its context is cancelled.
// go-parity-gap: pkg/domain/crossks is not transcreated.
#[test]
#[ignore = "go-parity-gap: pkg/domain/crossks is not transcreated"]
fn gc_loop_exits_when_context_cancelled() {}

/// Go `pkg/domain/crossks/cross_ks_test.go:130::TestManagerInClassical`:
/// on the classic kernel, `dom.GetKSStore("aaa")` errors with "cross
/// keyspace is not available in classic kernel or current keyspace".
// go-parity-gap: crossks Manager + Domain.GetKSStore are not transcreated.
#[test]
#[ignore = "go-parity-gap: crossks Manager + Domain.GetKSStore are not \
           transcreated"]
fn manager_in_classical() {}

/// Go `pkg/domain/crossks/cross_ks_test.go:140::TestManager`: over a 20-node
/// etcd cluster, concurrent `Acquire`/`Release` of runtime handles across
/// many keyspaces (next-gen kernel) holds invariants on store/session-pool
/// lifecycle.
// go-parity-gap: crossks Manager + etcd integration cluster are not
// transcreated.
#[test]
#[ignore = "go-parity-gap: crossks Manager is not transcreated"]
fn manager() {}

/// Go
/// `pkg/domain/crossks/cross_ks_test.go:412::TestDomainAcquireKSRuntimeHandle`:
/// the Domain-level `AcquireKSRuntimeHandle` hands out handles for a second
/// keyspace's runtime and releases them cleanly (next-gen kernel).
// go-parity-gap: Domain + crossks runtime handles are not transcreated.
#[test]
#[ignore = "go-parity-gap: Domain + crossks runtime handles are not \
           transcreated"]
fn domain_acquire_ks_runtime_handle() {}

/// Go
/// `pkg/domain/crossks/cross_ks_test.go:486::TestDomainAlterTableModeInKeyspaceSubmitOnly`:
/// an `ALTER TABLE ...` in a cross-keyspace store is SUBMITTED to the target
/// keyspace's DDL queue without waiting for completion (next-gen kernel).
// go-parity-gap: cross-keyspace DDL submission is not transcreated.
#[test]
#[ignore = "go-parity-gap: cross-keyspace DDL submission is not transcreated"]
fn domain_alter_table_mode_in_keyspace_submit_only() {}
