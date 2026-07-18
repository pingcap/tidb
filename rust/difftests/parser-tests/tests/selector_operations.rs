//! Stable Cargo shard for independently owned operational parser selectors.

#[path = "selectors/operations/analyze_incremental_selector.rs"]
mod analyze_incremental;
#[path = "selectors/operations/analyze_table_selector.rs"]
mod analyze_table;
#[path = "selectors/operations/flush_selector.rs"]
mod flush;
#[path = "selectors/operations/import_into_selector.rs"]
mod import_into;
#[path = "selectors/operations/load_stats_selector.rs"]
mod load_stats;
#[path = "selectors/operations/lock_tables_selector.rs"]
mod lock_tables;
#[path = "selectors/operations/split_table_selector.rs"]
mod split_table;
#[path = "selectors/operations/stats_lock_selector.rs"]
mod stats_lock;
#[path = "selectors/operations/traffic_selector.rs"]
mod traffic;
