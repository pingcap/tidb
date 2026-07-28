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

//! Runs one wide-SQL statement through the session driver against a real
//! cluster: the catalog comes from TiKV's meta namespace, the rows come from a
//! transactional snapshot, and the SQL is planned and executed by the same
//! driver the in-process tier uses.
//!
//! Usage:
//!
//! ```text
//! cluster-session-smoke --pd <addr> --schema <db> --sql "<statement>"
//! ```
//!
//! Every statement runs at its own snapshot. Writes are staged and, with
//! `--commit`, published as one optimistic transaction at the end.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use tidb_exec::cluster_catalog::{configure_loaded_table, ClusterCatalog};
use tidb_exec::cluster_table_storage::{commit_staged_buffer, statement_storage};
use tidb_exec::real_tikv_catalog::load_catalog_from_cluster;
use tidb_exec::real_tikv_read::ProductionReadProcessAuthority;
use tidb_executor::cluster_storage::MutationBuffer;
use tidb_server::cluster_session::session_with_cluster_storage;
use tidb_session::StmtResult;

const TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cluster-session-smoke: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Arguments {
    pd: String,
    schema: String,
    statements: Vec<String>,
    commit: bool,
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut pd = None;
    let mut schema = None;
    let mut statements = Vec::new();
    let mut commit = false;
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        match flag.as_str() {
            "--pd" => pd = Some(argv.next().ok_or("--pd needs an address")?),
            "--schema" => schema = Some(argv.next().ok_or("--schema needs a database name")?),
            "--sql" => statements.push(argv.next().ok_or("--sql needs a statement")?),
            "--commit" => commit = true,
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Arguments {
        pd: pd.ok_or("--pd is required")?,
        schema: schema.ok_or("--schema is required")?,
        statements: if statements.is_empty() {
            return Err("at least one --sql is required".to_owned());
        } else {
            statements
        },
        commit,
    })
}

fn run() -> Result<(), String> {
    let arguments = parse_arguments()?;
    let mut loaded: Option<ClusterCatalog> = None;
    // The authority insists on naming one served table because the bounded
    // read path is built around a single relation; this driver does not use it,
    // so the first admitted table satisfies the constructor and the whole
    // catalog is what the session runs on.
    let mut authority = ProductionReadProcessAuthority::connect_with_catalog(
        [arguments.pd.clone()],
        TIMEOUT,
        |opener| {
            let catalog =
                load_catalog_from_cluster(opener, TIMEOUT).map_err(|error| error.to_string())?;
            let configured = catalog
                .databases
                .iter()
                .find_map(|database| {
                    database.tables.iter().find_map(|table| {
                        configure_loaded_table(database.info.name.original(), table).ok()
                    })
                })
                .ok_or_else(|| "the cluster has no table this node can configure".to_owned())?;
            loaded = Some(catalog);
            Ok(configured)
        },
    )
    .map_err(|error| error.to_string())?;
    let catalog = loaded.ok_or("the catalog load produced nothing")?;
    println!("schema version {}", catalog.schema_version);

    let opener = Arc::new(authority.transaction_opener());
    let buffer = MutationBuffer::new();
    let mut failure = None;
    for sql in &arguments.statements {
        if let Err(error) = run_statement(&opener, &buffer, &catalog, &arguments.schema, sql) {
            failure = Some(error);
            break;
        }
    }
    if failure.is_none() && arguments.commit {
        match commit_staged_buffer(&opener, &buffer, TIMEOUT) {
            Ok(None) => println!("commit: nothing staged"),
            Ok(Some(outcome)) => println!("commit: {outcome:?}"),
            Err(error) => failure = Some(error.to_string()),
        }
    }
    // The opener clone holds PD request handles; the authority's shutdown
    // drains and refuses to stop while any are live (the drain footgun only a
    // real cluster exposes) -- release ours before asking it to stop.
    drop(buffer);
    drop(opener);
    let shutdown = authority.shutdown().map_err(|error| error.to_string());
    match failure {
        Some(error) => Err(error),
        None => shutdown,
    }
}

/// Runs one statement at its own snapshot, over the session's staged writes.
fn run_statement(
    opener: &Arc<tidb_exec::real_tikv_read::RealOptimisticTransactionOpener>,
    buffer: &MutationBuffer,
    catalog: &ClusterCatalog,
    schema: &str,
    sql: &str,
) -> Result<(), String> {
    let (storage, snapshot) = statement_storage(Arc::clone(opener), buffer.clone(), TIMEOUT)
        .map_err(|error| error.to_string())?;
    let start_ts = snapshot
        .lock()
        .map_err(|_| "the snapshot handle is poisoned".to_owned())?
        .start_ts();
    let (mut session, skipped) = session_with_cluster_storage(catalog, &storage);
    for table in &skipped {
        println!("skipped {}: {}", table.name, table.reason);
    }
    session
        .run(&format!("USE {schema}"))
        .map_err(|error| format!("{error:?}"))?;
    let outcome = session.run(sql).map_err(|error| format!("{error:?}"));
    // The read transaction ends whether the statement succeeded or not, so a
    // failure never leaves a lock behind.
    let finished = snapshot
        .lock()
        .map_err(|_| "the snapshot handle is poisoned".to_owned())
        .and_then(|mut snapshot| snapshot.finish().map_err(|error| error.to_string()));
    match outcome? {
        StmtResult::Rows(rows) => {
            println!("[start_ts {start_ts}] {sql}");
            for row in rows {
                let cells: Vec<String> = row.iter().map(|value| format!("{value:?}")).collect();
                println!("  {}", cells.join("\t"));
            }
        }
        other => println!("[start_ts {start_ts}] {sql} -> {other:?}"),
    }
    finished
}
