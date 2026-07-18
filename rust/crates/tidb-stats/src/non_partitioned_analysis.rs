// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Compatibility wrappers for canonical non-partitioned jobs.
pub const ANALYZE_TABLE: &str = "analyzeTable";
pub const ANALYZE_INDEX: &str = "analyzeIndex";
#[must_use]
pub const fn has_newly_added_index(count: usize) -> bool {
    count > 0
}
#[must_use]
pub const fn analyze_type(count: usize) -> &'static str {
    if count > 0 {
        ANALYZE_INDEX
    } else {
        ANALYZE_TABLE
    }
}
#[must_use]
pub fn gen_sql_for_analyze_table(schema: &str, table: &str) -> (&'static str, Vec<String>) {
    (
        "analyze table %n.%n",
        vec![schema.to_owned(), table.to_owned()],
    )
}
#[must_use]
pub fn gen_sql_for_analyze_index(
    schema: &str,
    table: &str,
    index: &str,
) -> (&'static str, Vec<String>) {
    (
        "analyze table %n.%n index %n",
        vec![schema.to_owned(), table.to_owned(), index.to_owned()],
    )
}
