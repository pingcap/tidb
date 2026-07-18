//! Stable Cargo shard for independently owned DROP parser selectors.

#[path = "selectors/drop/drop_database_selector.rs"]
mod drop_database;
#[path = "selectors/drop/drop_hypo_index_selector.rs"]
mod drop_hypo_index;
#[path = "selectors/drop/drop_index_selector.rs"]
mod drop_index;
#[path = "selectors/drop/drop_placement_policy_selector.rs"]
mod drop_placement_policy;
#[path = "selectors/drop/drop_resource_group_selector.rs"]
mod drop_resource_group;
#[path = "selectors/drop/drop_stats_selector.rs"]
mod drop_stats;
#[path = "selectors/drop/drop_tables_selector.rs"]
mod drop_tables;
