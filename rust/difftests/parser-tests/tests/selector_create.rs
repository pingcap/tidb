//! Stable Cargo shard for independently owned CREATE parser selectors.

#[path = "selectors/create/create_column_check_selector.rs"]
mod create_column_check;
#[path = "selectors/create/create_column_options_selector.rs"]
mod create_column_options;
#[path = "selectors/create/create_database_options_selector.rs"]
mod create_database_options;
#[path = "selectors/create/create_global_temporary_table_selector.rs"]
mod create_global_temporary_table;
#[path = "selectors/create/create_index_selector.rs"]
mod create_index;
#[path = "selectors/create/create_placement_policy_selector.rs"]
mod create_placement_policy;
#[path = "selectors/create/create_resource_group_selector.rs"]
mod create_resource_group;
#[path = "selectors/create/create_role_selector.rs"]
mod create_role;
#[path = "selectors/create/create_schema_selector.rs"]
mod create_schema;
#[path = "selectors/create/create_table_affinity_selector.rs"]
mod create_table_affinity;
#[path = "selectors/create/create_table_binary_string_selector.rs"]
mod create_table_binary_string;
#[path = "selectors/create/create_table_charset_validation_selector.rs"]
mod create_table_charset_validation;
#[path = "selectors/create/create_table_check_time_selector.rs"]
mod create_table_check_time;
#[path = "selectors/create/create_table_compatibility_selector.rs"]
mod create_table_compatibility;
#[path = "selectors/create/create_table_like_selector.rs"]
mod create_table_like;
#[path = "selectors/create/create_table_merge_union_selector.rs"]
mod create_table_merge_union;
#[path = "selectors/create/create_table_nonreserved_column_selector.rs"]
mod create_table_nonreserved_column;
#[path = "selectors/create/create_table_partition_selector.rs"]
mod create_table_partition;
#[path = "selectors/create/create_table_planner_issue_selector.rs"]
mod create_table_planner_issue;
#[path = "selectors/create/create_table_qualified_column_selector.rs"]
mod create_table_qualified_column;
#[path = "selectors/create/create_user_policy_selector.rs"]
mod create_user_policy;
#[path = "selectors/create/create_view_core_selector.rs"]
mod create_view_core;
#[path = "selectors/create/create_view_definer_selector.rs"]
mod create_view_definer;
#[path = "selectors/create/create_view_definer_union_selector.rs"]
mod create_view_definer_union;
