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

//! Prints one table's statistics as this node reads them out of a real
//! cluster's `mysql.stats_*`.
//!
//! The output is deliberately shaped to be diffed line-for-line against what
//! the Go node reports for the same table:
//!
//! ```text
//! bucket <is_index> <hist_id> <bucket_id> <count> <repeats> <ndv> <lower> <upper>
//! topn   <is_index> <hist_id> <hex value> <count>
//! ```
//!
//! `count` is printed *cumulative*, matching `SHOW STATS_BUCKETS`'s `Count`
//! column (Go prints the in-memory `Bucket.Count`, not the stored delta).
//! Bounds are printed hex-encoded so that an index bound — raw index-key bytes
//! that are not text at all — survives the comparison intact.
//!
//! Usage:
//!
//! ```text
//! cluster-stats-dump --pd <addr> --schema <db> --table <name>
//! ```

use std::process::ExitCode;
use std::time::Duration;

use tidb_exec::cluster_catalog::configure_loaded_table;
use tidb_exec::real_tikv_read::ProductionReadProcessAuthority;
use tidb_exec::real_tikv_stats::load_table_stats_from_cluster;

const TIMEOUT: Duration = Duration::from_secs(10);

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cluster-stats-dump: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Arguments {
    pd: String,
    schema: String,
    table: String,
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut pd = None;
    let mut schema = None;
    let mut table = None;
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        match flag.as_str() {
            "--pd" => pd = Some(argv.next().ok_or("--pd needs an address")?),
            "--schema" => schema = Some(argv.next().ok_or("--schema needs a database name")?),
            "--table" => table = Some(argv.next().ok_or("--table needs a table name")?),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    Ok(Arguments {
        pd: pd.ok_or("--pd is required")?,
        schema: schema.ok_or("--schema is required")?,
        table: table.ok_or("--table is required")?,
    })
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02X}")).collect()
}

fn run() -> Result<(), String> {
    let arguments = parse_arguments()?;
    let mut authority = ProductionReadProcessAuthority::connect_with_catalog(
        [arguments.pd.clone()],
        TIMEOUT,
        |opener| {
            let catalog = tidb_exec::real_tikv_catalog::load_catalog_from_cluster(opener, TIMEOUT)
                .map_err(|error| error.to_string())?;
            catalog
                .databases
                .iter()
                .find_map(|database| {
                    database.tables.iter().find_map(|table| {
                        configure_loaded_table(database.info.name.original(), table).ok()
                    })
                })
                .ok_or_else(|| "the cluster has no table this node can configure".to_owned())
        },
    )
    .map_err(|error| error.to_string())?;

    let result = {
        let opener = authority.transaction_opener();
        load_table_stats_from_cluster(&opener, TIMEOUT, &arguments.schema, &arguments.table)
            .map_err(|error| error.to_string())
    };
    // The opener holds PD request handles and the authority's shutdown drains
    // while any are live; ours is already dropped above.
    let shutdown = authority.shutdown().map_err(|error| error.to_string());
    let stats = result?;
    shutdown?;

    let Some(stats) = stats else {
        println!("meta none");
        return Ok(());
    };
    println!(
        "meta {} {} {} {}",
        stats.table_id, stats.version, stats.modify_count, stats.row_count
    );
    for item in stats.columns.iter().chain(stats.indexes.iter()) {
        let is_index = i32::from(item.is_index);
        println!(
            "hist {} {} ndv={} null={} totcolsize={} corr={} statsver={} flag={} cms={}",
            is_index,
            item.id,
            item.histogram.ndv,
            item.histogram.null_count,
            item.histogram.tot_col_size,
            item.histogram.correlation,
            item.stats_ver,
            item.flag,
            i32::from(item.cms.is_some()),
        );
        for (bucket_id, bucket) in item.histogram.buckets.iter().enumerate() {
            println!(
                "bucket {} {} {} {} {} {} {} {}",
                is_index,
                item.id,
                bucket_id,
                bucket.count,
                bucket.repeat,
                bucket.ndv,
                hex(&datum_bytes(&bucket.lower_bound)),
                hex(&datum_bytes(&bucket.upper_bound)),
            );
        }
        if let Some(topn) = &item.topn {
            for entry in topn.entries() {
                println!(
                    "topn {} {} {} {}",
                    is_index,
                    item.id,
                    hex(&entry.encoded),
                    entry.count
                );
                // A decoded CMSketch is only proven by being queried: the
                // TopN value's own encoding is the key the sketch was built
                // over, so its estimate must land on the count the TopN row
                // states.
                if let Some(cms) = &item.cms {
                    println!(
                        "cmsq {} {} {} {}",
                        is_index,
                        item.id,
                        hex(&entry.encoded),
                        cms.query_with_topn(Some(topn), &entry.encoded)
                    );
                }
            }
        }
    }
    Ok(())
}

/// The bytes a bound is compared against, in the domain it was decoded into.
///
/// A raw-byte bound (index key, or a string column's stored collation key)
/// prints its own bytes; a converted bound prints its SQL text, which is
/// exactly what `SHOW STATS_BUCKETS` shows for that column.
fn datum_bytes(datum: &tidb_datatype::Datum) -> Vec<u8> {
    match datum {
        tidb_datatype::Datum::Bytes(bytes) | tidb_datatype::Datum::Raw(bytes) => bytes.clone(),
        tidb_datatype::Datum::String(string) => string.bytes().to_vec(),
        other => other
            .sql_string()
            .unwrap_or_else(|_| format!("{other:?}"))
            .into_bytes(),
    }
}
