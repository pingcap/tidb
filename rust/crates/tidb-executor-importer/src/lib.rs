//! Go `pkg/executor/importer`.

pub mod import_into_options;

use tidb_driver_error::DriverError;
mod import;
pub use import::*;

mod file_match;
pub mod chunk_process;
pub mod engine_process;
pub mod job;
pub mod kv_encode;
pub mod precheck;
pub mod query_source;
pub mod sampler;
pub mod table_import;
#[cfg(test)]
mod chunk_process_tests;
#[cfg(test)]
mod conflict_records_tests;
#[cfg(test)]
mod plan_tests;
#[cfg(test)]
mod sampler_tests;
#[cfg(test)]
mod table_import_tests;
mod local;

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
#[cfg(test)]
tidb_planner_init::link!();
