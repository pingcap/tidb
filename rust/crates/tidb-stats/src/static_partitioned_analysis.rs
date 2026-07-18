// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0.

//! Compatibility wrappers for canonical static-partition jobs.
pub const ANALYZE_STATIC_PARTITION: &str = "analyzeStaticPartition";
pub const ANALYZE_STATIC_PARTITION_INDEX: &str = "analyzeStaticPartitionIndex";
#[must_use]
pub const fn static_partition_table_id(id: i64) -> i64 {
    id
}
#[must_use]
pub const fn has_newly_added_static_partition_index(count: usize) -> bool {
    count > 0
}
#[must_use]
pub const fn static_partition_analyze_type(count: usize) -> &'static str {
    if count > 0 {
        ANALYZE_STATIC_PARTITION_INDEX
    } else {
        ANALYZE_STATIC_PARTITION
    }
}
#[must_use]
pub fn gen_sql_for_analyze_static_partition(
    schema: &str,
    table: &str,
    partition: &str,
) -> (&'static str, Vec<String>) {
    (
        "analyze table %n.%n partition %n",
        vec![schema.to_owned(), table.to_owned(), partition.to_owned()],
    )
}
#[must_use]
pub fn gen_sql_for_analyze_static_partition_index(
    schema: &str,
    table: &str,
    partition: &str,
    index: &str,
) -> (&'static str, Vec<String>) {
    (
        "analyze table %n.%n partition %n index %n",
        vec![
            schema.to_owned(),
            table.to_owned(),
            partition.to_owned(),
            index.to_owned(),
        ],
    )
}
