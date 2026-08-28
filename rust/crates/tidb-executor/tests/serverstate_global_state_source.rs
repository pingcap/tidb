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

//! Ports of Go `pkg/ddl/serverstate/syncer_test.go::TestStateSyncerSimple`
//! (master, `:45`). The `pkg/ddl/serverstate` package (the cluster
//! upgrading-state syncer over etcd's `ServerGlobalState` key) is not
//! transcreated in this tier, so the test is recorded as an explicit gap
//! with the contract re-derived from the Go source. Nothing is approximated.

/// Go `TestStateSyncerSimple` (`pkg/ddl/serverstate/syncer_test.go:45`, an
/// etcd integration test): a `serverstate.NewEtcdSyncer` over a one-node etcd
/// cluster, after `Init`, reports `GetGlobalState` as
/// `StateNormalRunning` with `IsUpgradingState()` false on the empty store;
/// watching `util2.ServerGlobalState`, a published `StateUpgrading` StateInfo
/// arrives on `WatchChan()` with the exact key/value bytes, flips
/// `IsUpgradingState()` on for the upgrade value and off again when
/// `StateNormalRunning` is re-published, and `GetGlobalState` tracks the
/// watched value at each step.
// go-parity-gap: no `pkg/ddl/serverstate` carrier (StateInfo/StateSyncer and
// the ServerGlobalState watch loop are not transcreated; the schema-version
// syncer it twins with lives in tidb-schemaver, outside this gate crate).
#[test]
#[ignore]
fn server_state_syncer_watches_the_global_upgrade_state() {
}
