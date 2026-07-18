//! Stable Cargo shard for independently owned ALTER parser selectors.

#[path = "selectors/alter/collation_validation_selector.rs"]
mod collation_validation;

#[path = "selectors/alter/alter_add_check_selector.rs"]
mod alter_add_check;
#[path = "selectors/alter/alter_add_check_current_user_selector.rs"]
mod alter_add_check_current_user;
#[path = "selectors/alter/alter_add_column_constraints_selector.rs"]
mod alter_add_column_constraints;
#[path = "selectors/alter/alter_add_column_literal_default_selector.rs"]
mod alter_add_column_literal_default;
#[path = "selectors/alter/alter_add_index_selector.rs"]
mod alter_add_index;
#[path = "selectors/alter/alter_add_partition_count_selector.rs"]
mod alter_add_partition_count;
#[path = "selectors/alter/alter_add_partition_definition_selector.rs"]
mod alter_add_partition_definition;
#[path = "selectors/alter/alter_add_partition_empty_selector.rs"]
mod alter_add_partition_empty;
#[path = "selectors/alter/alter_analyze_partition_selector.rs"]
mod alter_analyze_partition;
#[path = "selectors/alter/alter_auto_id_options_selector.rs"]
mod alter_auto_id_options;
#[path = "selectors/alter/alter_auto_increment_selector.rs"]
mod alter_auto_increment;
#[path = "selectors/alter/alter_check_selector.rs"]
mod alter_check;
#[path = "selectors/alter/alter_column_default_selector.rs"]
mod alter_column_default;
#[path = "selectors/alter/alter_database_selector.rs"]
mod alter_database;
#[path = "selectors/alter/alter_drop_check_selector.rs"]
mod alter_drop_check;
#[path = "selectors/alter/alter_drop_foreign_key_selector.rs"]
mod alter_drop_foreign_key;
#[path = "selectors/alter/alter_drop_partition_selector.rs"]
mod alter_drop_partition;
#[path = "selectors/alter/alter_drop_primary_key_selector.rs"]
mod alter_drop_primary_key;
#[path = "selectors/alter/alter_engine_attribute_selector.rs"]
mod alter_engine_attribute;
#[path = "selectors/alter/alter_enum_set_binary_selector.rs"]
mod alter_enum_set_binary;
#[path = "selectors/alter/alter_exchange_partition_selector.rs"]
mod alter_exchange_partition;
#[path = "selectors/alter/alter_index_visibility_selector.rs"]
mod alter_index_visibility;
#[path = "selectors/alter/alter_lock_selector.rs"]
mod alter_lock;
#[path = "selectors/alter/alter_order_qualified_modify_selector.rs"]
mod alter_order_qualified_modify;
#[path = "selectors/alter/alter_partition_attributes_selector.rs"]
mod alter_partition_attributes;
#[path = "selectors/alter/alter_partition_check_import_selector.rs"]
mod alter_partition_check_import;
#[path = "selectors/alter/alter_partition_discard_selector.rs"]
mod alter_partition_discard;
#[path = "selectors/alter/alter_partition_interval_selector.rs"]
mod alter_partition_interval;
#[path = "selectors/alter/alter_partition_maintenance_selector.rs"]
mod alter_partition_maintenance;
#[path = "selectors/alter/alter_partition_merge_first_selector.rs"]
mod alter_partition_merge_first;
#[path = "selectors/alter/alter_partition_placement_policy_selector.rs"]
mod alter_partition_placement_policy;
#[path = "selectors/alter/alter_partition_split_maxvalue_selector.rs"]
mod alter_partition_split_maxvalue;
#[path = "selectors/alter/alter_placement_policy_selector.rs"]
mod alter_placement_policy;
#[path = "selectors/alter/alter_rename_column_selector.rs"]
mod alter_rename_column;
#[path = "selectors/alter/alter_rename_index_selector.rs"]
mod alter_rename_index;
#[path = "selectors/alter/alter_repartition_selector.rs"]
mod alter_repartition;
#[path = "selectors/alter/alter_resource_group_selector.rs"]
mod alter_resource_group;
#[path = "selectors/alter/alter_shard_row_id_bits_selector.rs"]
mod alter_shard_row_id_bits;
#[path = "selectors/alter/alter_table_affinity_selector.rs"]
mod alter_table_affinity;
#[path = "selectors/alter/alter_table_attributes_selector.rs"]
mod alter_table_attributes;
#[path = "selectors/alter/alter_table_cache_selector.rs"]
mod alter_table_cache;
#[path = "selectors/alter/alter_table_charset_collation_selector.rs"]
mod alter_table_charset_collation;
#[path = "selectors/alter/alter_table_comment_selector.rs"]
mod alter_table_comment;
#[path = "selectors/alter/alter_table_engine_row_format_selector.rs"]
mod alter_table_engine_row_format;
#[path = "selectors/alter/alter_table_generic_options_selector.rs"]
mod alter_table_generic_options;
#[path = "selectors/alter/alter_table_multi_spec_selector.rs"]
mod alter_table_multi_spec;
#[path = "selectors/alter/alter_table_placement_policy_selector.rs"]
mod alter_table_placement_policy;
#[path = "selectors/alter/alter_table_ttl_selector.rs"]
mod alter_table_ttl;
#[path = "selectors/alter/alter_table_validation_selector.rs"]
mod alter_table_validation;
#[path = "selectors/alter/alter_tiflash_replica_compact_selector.rs"]
mod alter_tiflash_replica_compact;
#[path = "selectors/alter/alter_user_dual_password_selector.rs"]
mod alter_user_dual_password;
#[path = "selectors/alter/alter_user_password_expire_selector.rs"]
mod alter_user_password_expire;
#[path = "selectors/alter/alter_user_resource_group_selector.rs"]
mod alter_user_resource_group;
#[path = "selectors/alter/alter_vector_index_selector.rs"]
mod alter_vector_index;
