//! Executor-side unit tests that need no SQL stack: how a storage failure
//! keeps its SQL identity through the row read and write adapters.

mod dml;

use super::*;
