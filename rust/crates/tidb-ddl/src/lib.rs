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

//! Go `pkg/ddl`, the worker side, below the session.
//!
//! Go's layering is `ddl` < `domain` < `session`: the DDL executor builds a
//! job, the owner's worker steps it through the schema states, and a session
//! (a unit test's `testkit` session included) runs on a store that already
//! hosts both. The Rust node grew the other way round, with the worker living
//! in `tidb-server` beside the connection layer, so no session below the
//! server could ever have one (`rust/docs/structural-alignment-execplan.md`,
//! Milestone 3b).
//!
//! * [`worker`] is `RealClusterDdl`: `DoDDLJob` for the submitting side, the
//!   owner-elected scheduler that steps persisted jobs, the schema-version
//!   syncer and the reorganization backfillers.
//! * [`schema_sync`] is the non-owner's MDL loop (`MDLCheckLoop`): it
//!   acknowledges each loaded schema version to the owner's
//!   `WaitVersionSynced`, held back by the node's [`schema_sync::SchemaPinRegistry`].
//! * [`etcd_ops`] binds the syncers' etcd surface to the real client.
//! * [`bootstrap_publish`] is `session.BootstrapSession`'s transaction.
//! * [`embedded_store`] is `mockstore.NewMockStore` over unistore: the store
//!   the bootstrap and every in-process test run the worker on.

pub mod bootstrap_publish;
pub mod embedded_store;
pub mod etcd_ops;
pub mod schema_sync;
pub mod worker;

pub use worker::{ClusterDdl, RealClusterDdl};

/// Tests of process-global sysvar hooks must not share those hooks with other
/// simulated nodes. Runs the original test body in its own harness process.
#[cfg(test)]
pub(crate) fn isolate_process_globals() -> bool {
    const MARKER: &str = "TIDB_DDL_ISOLATED_GLOBALS_TEST";
    let thread = std::thread::current();
    let name = thread.name().expect("named Rust test thread");
    if std::env::var(MARKER).as_deref() == Ok(name) {
        return false;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", name, "--nocapture"])
        .env(MARKER, name)
        .output()
        .expect("start isolated global-state test");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed; 0 failed"),
        "isolated test {name} failed or was not selected: {}\n{stdout}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    true
}
