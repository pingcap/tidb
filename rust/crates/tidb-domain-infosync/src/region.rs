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

//! Go `pkg/domain/infosync/region.go`: the region distribution and the
//! scheduler calls TiDB makes through the info syncer's PD HTTP client
//! (`is.pdHTTPCli`), which a test replaces (`SetPDHttpCliForTest`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use tikv_client::pd_http;

/// The info syncer's `pdHTTPCli` (Go `pd.Client`), for the calls
/// `region.go` makes.
pub trait PdHttpClient: Send + Sync {
    /// Go `GetRegionDistributionByKeyRange`.
    fn get_region_distribution_by_key_range(
        &self,
        key_range: &pd_http::KeyRange,
        engine: &str,
    ) -> Result<pd_http::RegionDistributions, String>;
    /// Go `GetSchedulerConfig`.
    fn get_scheduler_config(&self, scheduler_name: &str) -> Result<serde_json::Value, String>;
    /// Go `CreateSchedulerWithInput`.
    fn create_scheduler_with_input(
        &self,
        name: &str,
        input: &mut HashMap<String, serde_json::Value>,
    ) -> Result<(), String>;
    /// Go `CancelSchedulerJob`.
    fn cancel_scheduler_job(&self, scheduler_name: &str, job_id: u64) -> Result<(), String>;
}

impl PdHttpClient for pd_http::Client {
    fn get_region_distribution_by_key_range(
        &self,
        key_range: &pd_http::KeyRange,
        engine: &str,
    ) -> Result<pd_http::RegionDistributions, String> {
        tidb_store_driver_txn::cluster_http::block_on_pd_http(
            pd_http::Client::get_region_distribution_by_key_range(self, key_range, engine),
        )
    }

    fn get_scheduler_config(&self, scheduler_name: &str) -> Result<serde_json::Value, String> {
        tidb_store_driver_txn::cluster_http::block_on_pd_http(pd_http::Client::get_scheduler_config(
            self,
            scheduler_name,
        ))
    }

    fn create_scheduler_with_input(
        &self,
        name: &str,
        input: &mut HashMap<String, serde_json::Value>,
    ) -> Result<(), String> {
        tidb_store_driver_txn::cluster_http::block_on_pd_http(
            pd_http::Client::create_scheduler_with_input(self, name, input),
        )
    }

    fn cancel_scheduler_job(&self, scheduler_name: &str, job_id: u64) -> Result<(), String> {
        tidb_store_driver_txn::cluster_http::block_on_pd_http(pd_http::Client::cancel_scheduler_job(
            self,
            scheduler_name,
            job_id,
        ))
    }
}

/// The info syncer's `pdHTTPCli`; `None` on a node without PD.
static PD_HTTP_CLI: Mutex<Option<Arc<dyn PdHttpClient>>> = Mutex::new(None);

/// Go `GlobalInfoSyncerInit`'s `pdHTTPCli`: the store's PD HTTP client as the
/// info syncer derives it, or none.
pub fn init_pd_http_cli(cli: Option<&pd_http::Client>) {
    *PD_HTTP_CLI.lock().unwrap_or_else(PoisonError::into_inner) = cli.map(|cli| {
        Arc::new(crate::pd_http::info_syncer_pd_http_client(cli)) as Arc<dyn PdHttpClient>
    });
}

/// Go `SetPDHttpCliForTest`: installs `cli` and answers the function that
/// puts the previous client back.
pub fn set_pd_http_cli_for_test(cli: Arc<dyn PdHttpClient>) -> impl FnOnce() {
    let original = PD_HTTP_CLI
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .replace(cli);
    move || *PD_HTTP_CLI.lock().unwrap_or_else(PoisonError::into_inner) = original
}

fn pd_http_cli(name: &str) -> Result<Arc<dyn PdHttpClient>, String> {
    PD_HTTP_CLI
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone()
        // PD client `errs.ErrClientGetLeader.FastGenByArgs(name)`.
        .ok_or_else(|| format!("[PD:client:ErrClientGetLeader]get leader failed, {name}"))
}

/// Go `GetRegionDistributionByKeyRange`.
///
/// # Errors
/// No PD, or PD's.
pub fn get_region_distribution_by_key_range(
    start_key: &[u8],
    end_key: &[u8],
    engine: &str,
) -> Result<pd_http::RegionDistributions, String> {
    pd_http_cli("pd http cli is nil")?
        .get_region_distribution_by_key_range(&pd_http::KeyRange::new(start_key, end_key), engine)
}

/// Go `GetSchedulerConfig`.
///
/// # Errors
/// No PD, or PD's.
pub fn get_scheduler_config(scheduler_name: &str) -> Result<serde_json::Value, String> {
    pd_http_cli("pd http cli is nil")?.get_scheduler_config(scheduler_name)
}

/// Go `CreateSchedulerConfigWithInput`.
///
/// # Errors
/// No PD, or PD's.
pub fn create_scheduler_config_with_input(
    scheduler_name: &str,
    mut input: HashMap<String, serde_json::Value>,
) -> Result<(), String> {
    pd_http_cli(scheduler_name)?.create_scheduler_with_input(scheduler_name, &mut input)
}

/// Go `CancelSchedulerJob`.
///
/// # Errors
/// No PD, or PD's.
pub fn cancel_scheduler_job(scheduler_name: &str, job_id: u64) -> Result<(), String> {
    pd_http_cli(scheduler_name)?.cancel_scheduler_job(scheduler_name, job_id)
}
