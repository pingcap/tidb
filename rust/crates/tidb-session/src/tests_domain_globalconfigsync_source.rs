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

//! Port of `pkg/domain/globalconfigsync/globalconfig_test.go`
//! (origin/master): `TestGlobalConfigSyncer` (:47) and
//! `TestStoreGlobalConfig` (:71). `TestMain` (:36) is the goleak harness and
//! is recorded as skipped-reason in the batch receipt.
//!
//! The package (`globalconfig.go`: `NewGlobalConfigSyncer` :32, `Notify`
//! :53, `StoreGlobalConfig` :40) is an etcd-backed PD global-config writer;
//! both tests need a real PD client (etcd integration cluster on the first)
//! and a bootstrapped session executing `set global` statements (on the
//! second).

#![cfg(test)]

/// Go
/// `pkg/domain/globalconfigsync/globalconfig_test.go:47::TestGlobalConfigSyncer`:
/// `Notify({a, b})` queues the item on `NotifyCh`;
/// `StoreGlobalConfig` writes it to `/global/config/a` with value `b`, as
/// re-read by `client.LoadGlobalConfig` with revision 0.
// go-parity-gap: globalconfigsync is an etcd/PD-client writer with no Rust
// home yet.
#[test]
#[ignore = "go-parity-gap: pkg/domain/globalconfigsync is not transcreated"]
fn global_config_syncer() {}

/// Go
/// `pkg/domain/globalconfigsync/globalconfig_test.go:71::TestStoreGlobalConfig`:
/// executing `set @@global.tidb_enable_top_sql=1` and
/// `set @@global.tidb_source_id=2` through a session ends up in PD as
/// `/global/config/enable_resource_metering` = `true` (the top-sql
/// translation) and `/global/config/source_id` = `2`, polled for up to 2s.
// go-parity-gap: needs the session bootstrap + globalconfigsync's PD
// etcd writer, both unported.
#[test]
#[ignore = "go-parity-gap: session bootstrap + globalconfigsync PD writer \
           are not transcreated"]
fn store_global_config() {}
