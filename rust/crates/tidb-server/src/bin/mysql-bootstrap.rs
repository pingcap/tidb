// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bootstraps the `mysql` schema of a fresh keyspace against a real cluster.
//!
//! This is [`tidb_exec::mysql_bootstrap::bootstrap_mysql_schema`]'s production
//! caller: the plan is made and published on one real optimistic transaction,
//! so a fresh keyspace either gains the whole schema or none of it.
//!
//! ```text
//! mysql-bootstrap --pd <addr>
//! ```
//!
//! A keyspace that already carries any bootstrap object is refused, and nothing
//! is written.
//!
//! The bootstrap publishes a schema version like any other catalog change, so
//! it announces it on etcd afterwards exactly as a DDL commit does: a Go TiDB
//! already watching this fresh keyspace then reloads at once instead of
//! waiting out its lease. Failing to announce is a warning, because the
//! version is already durable and every node's tick still finds it.

use std::process::ExitCode;
use std::time::Duration;

use tidb_exec::cluster_catalog::configure_loaded_table;
use tidb_exec::real_tikv_catalog::load_catalog_from_cluster;
use tidb_exec::real_tikv_ddl::SchemaVersionNotifier;
use tidb_exec::real_tikv_read::ProductionReadProcessAuthority;
use tidb_pd_client::EtcdClient;
use tidb_server::bootstrap_publish::{notify_committed_bootstrap, publish_bootstrap};

const TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("mysql-bootstrap: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut pd = None;
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        match flag.as_str() {
            "--pd" => pd = Some(argv.next().ok_or("--pd needs an address")?),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let pd = pd.ok_or("--pd is required")?;
    // Connected before the authority so a bootstrap never fails for the sake
    // of a notification it is allowed to skip.
    let notifier = EtcdClient::connect([pd.as_str()], TIMEOUT).ok();

    // The bootstrap runs inside the catalog choice because a fresh keyspace has
    // no table to serve until this call has written one: the authority is
    // started once, bootstraps, and then configures itself from the schema it
    // just published -- which is also the first proof our own loader reads it.
    let mut authority =
        ProductionReadProcessAuthority::connect_with_catalog([pd], TIMEOUT, |opener| {
            let (outcome, schema_version) = publish_bootstrap(opener, TIMEOUT)?;
            let schema_version = notify_committed_bootstrap(
                &outcome,
                schema_version,
                notifier
                    .as_ref()
                    .map(|client| client as &dyn SchemaVersionNotifier),
            )?;
            println!("bootstrap committed at schema version {schema_version}: {outcome:?}");
            let catalog =
                load_catalog_from_cluster(opener, TIMEOUT).map_err(|error| error.to_string())?;
            println!("schema version {}", catalog.schema_version);
            catalog
                .databases
                .iter()
                .find_map(|database| {
                    database.tables.iter().find_map(|table| {
                        configure_loaded_table(database.info.name.original(), table).ok()
                    })
                })
                .ok_or_else(|| "the bootstrapped catalog configures no table".to_owned())
        })
        .map_err(|error| error.to_string())?;
    authority.shutdown().map_err(|error| error.to_string())
}
