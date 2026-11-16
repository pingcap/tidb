//! Go `pkg/executor/importer`.
mod import;
pub use import::*;

mod file_match;
pub mod chunk_process;
pub mod engine_process;
pub mod job;
pub mod kv_encode;
pub mod precheck;
pub mod table_import;
#[cfg(test)]
mod chunk_process_tests;
#[cfg(test)]
mod plan_tests;
#[cfg(test)]
mod table_import_tests;
mod local;
