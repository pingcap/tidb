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

//! Ports of Go `pkg/ddl/session/session_pool_test.go` (master): the
//! `ddl/session` package's pooled-session wrapper (`TestSessionPool` `:29`,
//! `TestPessimisticTxn` `:72`, `TestSessionPoolDestroyResourcePool` `:121`,
//! `TestSessionPoolDestroyDestroyableSessionPool` `:169`). The package is not
//! transcreated in this tier, so each test is recorded as an explicit gap
//! with the contract re-derived from the Go source. Nothing is approximated.

/// Go `TestSessionPool` (`pkg/ddl/session/session_pool_test.go:29`): a
/// session drawn from the pool (`session.NewSessionPool` over a
/// `pools.ResourcePool` of testkit sessions) can `Begin`, `select 2` returns
/// one row of int64(2), and while the transaction is open its startTS shows
/// up in the session manager's `GetInternalSessionStartTSList()` -- and is
/// GONE from that list after `Commit` + `Put`.
// go-parity-gap: no ddl/session pool carrier, no session-manager internal
// startTS registry in this tier.
#[test]
#[ignore]
fn pooled_session_start_ts_leaves_the_registry_on_commit() {
}

/// Go `TestPessimisticTxn` (`pkg/ddl/session/session_pool_test.go:72`): two
/// pooled sessions run `BeginPessimistic` and update the same row; the
/// second session's update BLOCKS (its done channel stays empty after 100ms)
/// until the first commits -- pessimistic lock waits between internal
/// sessions.
// go-parity-gap: no ddl/session pool carrier and no pessimistic lock-wait
// executor in this tier.
#[test]
#[ignore]
fn pooled_pessimistic_updates_block_each_other() {
}

/// Go `TestSessionPoolDestroyResourcePool`
/// (`pkg/ddl/session/session_pool_test.go:121`): `pool.Destroy(sessCtx)` on a
/// plain `*pools.ResourcePool` closes the session and puts nil, so the next
/// `TryGet` returns a FRESH (non-nil, different) resource.
// go-parity-gap: no ddl/session pool carrier.
#[test]
#[ignore]
fn destroying_a_session_over_a_plain_resource_pool_recycles_it() {
}

/// Go `TestSessionPoolDestroyDestroyableSessionPool`
/// (`pkg/ddl/session/session_pool_test.go:169`): when the underlying pool
/// implements `session.DestroyablePool`, `Destroy` routes to
/// `DestroyablePool.Destroy` (the mock counts one destroy, zero extra puts)
/// rather than Close+Put(nil).
// go-parity-gap: no ddl/session pool carrier.
#[test]
#[ignore]
fn destroy_routes_to_a_destroyable_pool() {
}
