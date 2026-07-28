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

use std::process::ExitCode;
use std::time::Duration;

use tidb_exec::cluster_catalog::configure_loaded_table;
use tidb_exec::mysql_bootstrap::{
    bootstrap_mysql_schema, read_ddl_table_version, utc_now_timestamp, BootstrapEnvironment,
};
use tidb_exec::real_tikv_catalog::{load_catalog_from_cluster, TransactionMetaSnapshot};
use tidb_exec::real_tikv_read::ProductionReadProcessAuthority;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{OptimisticCommitOutcome, RealOptimisticTransactionOpener};
use tidb_util::timeutil::infer_system_tz;

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

    // The bootstrap runs inside the catalog choice because a fresh keyspace has
    // no table to serve until this call has written one: the authority is
    // started once, bootstraps, and then configures itself from the schema it
    // just published -- which is also the first proof our own loader reads it.
    let mut authority =
        ProductionReadProcessAuthority::connect_with_catalog([pd], TIMEOUT, |opener| {
            let outcome = publish_bootstrap(opener)?;
            println!("bootstrap committed: {outcome:?}");
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

/// Plans and commits the bootstrap on one transaction.
///
/// A first read-only pass exists only to size the write budget the coordinator
/// insists on knowing before it spends a timestamp; the plan that is published
/// is re-made on the writing transaction itself, so the freshness check and the
/// commit share one `start_ts`.
fn publish_bootstrap(
    opener: &RealOptimisticTransactionOpener,
) -> Result<OptimisticCommitOutcome, String> {
    let call = UnaryCallContext::with_timeout(TIMEOUT);
    let mut sizing = opener
        .begin_read_only()
        .map_err(|error| error.to_string())?;
    let mut environment = BootstrapEnvironment {
        system_tz: infer_system_tz(),
        // Go's `new_collations_enabled_on_first_bootstrap` default, which this
        // row then freezes for the life of the cluster.
        new_collation_enabled: true,
        cluster_id: opener.cluster_id(),
        current_timestamp: utc_now_timestamp(),
        ddl_table_version: 0,
    };
    let planned = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut sizing, TIMEOUT);
        environment.ddl_table_version =
            read_ddl_table_version(&mut snapshot).map_err(|error| error.to_string())?;
        // `u64::MAX` is not a placeholder timestamp: the plan's size depends on
        // how wide `update_ts` prints in each table's JSON, so sizing at the
        // widest possible timestamp is what makes this budget a ceiling for
        // whatever timestamp PD hands the writing transaction.
        bootstrap_mysql_schema(&mut snapshot, u64::MAX, &environment)
            .map_err(|error| error.to_string())?
    };
    sizing
        .finish_without_writes()
        .map_err(|error| error.to_string())?;
    let bytes: usize = planned
        .mutations
        .iter()
        .map(|mutation| mutation.key().len() + mutation.value().len())
        .sum();

    let mut transaction = opener
        .begin(planned.mutations.len(), bytes)
        .map_err(|error| error.to_string())?;
    let start_ts = transaction.start_ts();
    let write = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, TIMEOUT);
        bootstrap_mysql_schema(&mut snapshot, start_ts, &environment)
            .map_err(|error| error.to_string())?
    };
    println!(
        "bootstrapping {} tables at schema version {} (start_ts {start_ts})",
        write.created_tables.len(),
        write.schema_version
    );
    transaction
        .commit(write.mutations, &call)
        .map_err(|error| error.to_string())
}
