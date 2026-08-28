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

//! Documented go-parity-gap ports of `pkg/ddl/options_test.go` and
//! `pkg/ddl/owner_mgr_test.go` (master snapshot). Both test server-process
//! wiring this tier does not build: the `ddl.Options` struct that carries an
//! etcd client, lease, store and info cache into `ddl.NewDDL`, and the
//! etcd-backed owner manager lifecycle.

/// Go `options_test.go:28::TestOptions`. Applying `WithEtcdClient`,
/// `WithLease`, `WithStore` and `WithInfoCache` to an empty `ddl.Options`
/// records exactly the values passed (each field pointer-equal to the
/// argument).
// go-parity-gap: this tier's ddl module takes no Options struct -- there is
// no etcd client, lease, kv store or infoschema cache to wire.
#[test]
#[ignore]
fn ddl_options_record_every_with_call_value() {
}

/// Go `owner_mgr_test.go:30::TestOwnerManager`. `StartOwnerManager` over a
/// unistore-configured server creates a manager with NO etcd client, no
/// owner op and an empty id; over a TiKV-configured server it creates an
/// etcd client, a non-empty id and a real owner manager; `CloseOwnerManager`
/// tears either down.
// go-parity-gap: the etcd-backed owner manager (campaigning for leadership)
// is server machinery this tier does not build.
#[test]
#[ignore]
fn owner_manager_lifecycle_depends_on_store_kind() {
}
