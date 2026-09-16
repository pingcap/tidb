// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Inc. 2.0 (the "License");
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

//! Hash join v2's probe fetcher must close the workers' input channels when
//! the probe side is drained.
//!
//! Go `hash_join_base.go` runs the probe-side fetcher goroutine through
//! `RunWithRecover(fetchProbeSideChunks, handleProbeSideFetcherPanic)`, and
//! the handler closes every worker's `probeResultCh` on EVERY exit -- normal
//! EOF, skip-probe, spill, error, or panic -- so each `ProbeWorkerV2`'s
//! `runJoinWorker` loop sees `ok = false` and finishes. The Rust stage hands
//! each worker one `bounded(1)` input channel and models that close as sender
//! drop; the pooled fetcher keeps clones of the senders for its batches, so
//! the clones must be dropped when the fetcher completes. If they survive, a
//! worker still waiting for its next chunk never observes the end of the
//! probe side, never reports `Done`, and the stage blocks in `next` forever:
//! any join whose probe source has fewer ready chunks than
//! `tidb_hash_join_concurrency` hangs (this fixture, and the convergence
//! ladder's join + GROUP BY step, both did before the fix).
//!
//! The aggregate rows are the Go-oracle answers of the same fixture: the
//! convergence run asserts `1 -> 820`, `2 -> 55` against a same-commit Go
//! TiDB.

#![cfg(test)]

use crate::tests_support::*;
use crate::*;

/// Five orders joined against three customers: the whole probe side fits in
/// one chunk, so with the default concurrency most workers never receive a
/// chunk before the source reaches EOF.
fn fixture() -> Session {
    let mut session = Session::new();
    session
        .run("create table orders (id bigint primary key, customer bigint not null, amount bigint not null)")
        .unwrap();
    session
        .run("create table customers (id bigint primary key, region bigint not null)")
        .unwrap();
    session
        .run("insert into orders values (1, 10, 100), (2, 10, 250), (3, 20, 70), (4, 20, 400), (5, 30, 55)")
        .unwrap();
    session
        .run("insert into customers values (10, 1), (20, 1), (30, 2)")
        .unwrap();
    session
}

/// The probe side drains while idle workers hold no input; the join must
/// still publish every worker's terminal event and answer.
#[test]
fn a_drained_probe_side_lets_idle_workers_finish_the_join() {
    let mut session = fixture();
    assert_eq!(
        row_text(session.run(
            "SELECT o.id, c.region FROM orders o JOIN customers c ON o.customer = c.id ORDER BY o.id",
        )),
        [
            ["1", "1"],
            ["2", "1"],
            ["3", "1"],
            ["4", "1"],
            ["5", "2"],
        ]
    );
}

/// The convergence ladder's stranding shape: the join feeds an aggregate, so
/// the hash join's `next` is re-entered after its last output chunk and must
/// observe the workers' completion instead of blocking forever.
#[test]
fn an_aggregate_over_a_drained_join_returns_the_go_answers() {
    let mut session = fixture();
    assert_eq!(
        row_text(session.run(
            "SELECT c.region, SUM(o.amount) FROM orders o JOIN customers c ON o.customer = c.id GROUP BY c.region ORDER BY c.region",
        )),
        [["1", "820"], ["2", "55"]]
    );
}
