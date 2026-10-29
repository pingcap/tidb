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

//! Go `Domain.initResourceGroupsController`: the node's PD resource-group
//! controller, started over the store's PD client and stopped before the
//! store closes.

use std::sync::Arc;
use std::time::Duration;

use tidb_store_driver_error::sql_query_error::SqlQueryError;

use crate::domain::real_tikv_node::RunConfiguredNodeError;

/// Go `runaway.MaxWaitDuration`: the longest a request waits for its
/// resource group's tokens.
const RESOURCE_GROUP_MAX_WAIT_DURATION: Duration = Duration::from_secs(30);

/// Go `Domain.initResourceGroupsController`: the node's resource-group
/// controller over the store's PD client (`rmclient.NewResourceGroupController`
/// with the server ID as its client ID and the store's keyspace -- none, so
/// `NullKeyspaceID`), started and installed as client-rust's resource-control
/// interceptor (`tikv.SetResourceControlInterceptor`).
pub fn start_resource_groups_controller(
    store: &tidb_txnkv::driver::tikv_store::TikvStore,
    server_id: u64,
) -> Result<
    Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
    RunConfiguredNodeError,
> {
    start_resource_groups_controller_over(store, server_id, store.store().pd_client())
}

/// [`start_resource_groups_controller`] over any store and resource-manager
/// provider: Go takes the provider from `store.GetPDClient()`, which for
/// unistore is its mock `pdClient`.
pub fn start_resource_groups_controller_over<PdC: tikv_client::PdClient>(
    store: &tidb_txnkv::driver::tikv_store::TikvStore<PdC>,
    server_id: u64,
    provider: Arc<dyn tikv_client::pd_resource_group::controller::ResourceGroupProvider>,
) -> Result<
    Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
    RunConfiguredNodeError,
> {
    use tikv_client::pd_resource_group::controller as rmclient;
    let controller = store
        .block_on(rmclient::new_resource_group_controller(
            server_id,
            provider,
            None,
            tikv_client::tikv::NULLSPACE_ID,
            vec![rmclient::with_max_wait_duration(
                RESOURCE_GROUP_MAX_WAIT_DURATION,
            )],
        ))
        .map_err(|error| {
            RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
        })?;
    {
        let _runtime = store.runtime().enter();
        controller.start();
    }
    tikv_client::set_resource_control_interceptor(controller.clone());
    // Go `SetupTopProfiling(..., dom)` binds the domain as the RU-version
    // provider; its `GetRUVersion` asks this controller.
    tidb_topsql_stmtstats::bind_ru_version_provider(Some(Arc::new(DomainRuVersion(
        Arc::clone(&controller),
    ))));
    Ok(controller)
}

/// Go `Domain.GetRUVersion`: the resource-groups controller's RU version.
struct DomainRuVersion(Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>);

impl tidb_topsql_stmtstats::RuVersionProvider for DomainRuVersion {
    fn get_ru_version(&self) -> tidb_pd_client::rm_client::RuVersion {
        tidb_pd_client::rm_client::RuVersion(self.0.get_ru_version())
    }
}

/// Go `Domain.Close`'s controller teardown: the interceptor is removed and
/// the controller's loop stopped.
pub fn stop_resource_groups_controller<PdC: tikv_client::PdClient>(
    store: &tidb_txnkv::driver::tikv_store::TikvStore<PdC>,
    controller: &Arc<tikv_client::pd_resource_group::controller::ResourceGroupsController>,
) {
    tikv_client::unset_resource_control_interceptor();
    // Go `topsql.Close`: `BindRUVersionProvider(nil)`.
    tidb_topsql_stmtstats::bind_ru_version_provider(None);
    // Go cancels the controller's context and logs nothing.
    let _ = store.block_on(controller.stop());
}
