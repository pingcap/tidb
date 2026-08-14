#![cfg(test)]

//! Core session behavior, split by the statement surface under test.

mod aggregates;
mod builtins;
mod ddl;
mod dml;
mod lifecycle;
mod numeric_domain;
mod session_state;
mod status_values;
mod temporal_types;
mod transactions;
