//! Stable Cargo shard for independently owned DML parser selectors.

#[path = "selectors/dml/batch_dml_selector.rs"]
mod batch_dml;
#[path = "selectors/dml/binding_dml_selector.rs"]
mod binding_dml;
#[path = "selectors/dml/create_binding_with_dml_selector.rs"]
mod create_binding_with_dml;
#[path = "selectors/dml/dml_header_hint_selector.rs"]
mod dml_header_hint;
#[path = "selectors/dml/dml_order_limit_selector.rs"]
mod dml_order_limit;
#[path = "selectors/dml/insert_binary_escape_selector.rs"]
mod insert_binary_escape;
#[path = "selectors/dml/insert_parenthesized_source_selector.rs"]
mod insert_parenthesized_source;
#[path = "selectors/dml/insert_set_qualified_column_selector.rs"]
mod insert_set_qualified_column;
#[path = "selectors/dml/insert_with_cte_selector.rs"]
mod insert_with_cte;
#[path = "selectors/dml/insert_with_table_for_update_selector.rs"]
mod insert_with_table_for_update;
#[path = "selectors/dml/joined_update_default_selector.rs"]
mod joined_update_default;
#[path = "selectors/dml/on_duplicate_default_selector.rs"]
mod on_duplicate_default;
#[path = "selectors/dml/update_default_selector.rs"]
mod update_default;
#[path = "selectors/dml/update_derived_target_selector.rs"]
mod update_derived_target;
#[path = "selectors/dml/with_dml_selector.rs"]
mod with_dml;
