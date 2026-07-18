//! Stable Cargo shard for independently owned schema/type parser selectors.

#[path = "selectors/schema/binary_varbinary_selector.rs"]
mod binary_varbinary;
#[path = "selectors/schema/blob_text_type_selector.rs"]
mod blob_text_type;
#[path = "selectors/schema/clustered_index_parts_selector.rs"]
mod clustered_index_parts;
#[path = "selectors/schema/enum_set_restore_selector.rs"]
mod enum_set_restore;
#[path = "selectors/schema/json_column_selector.rs"]
mod json_column;
#[path = "selectors/schema/table_statement_selector.rs"]
mod table_statement;
#[path = "selectors/schema/vector_column_selector.rs"]
mod vector_column;
#[path = "selectors/schema/vector_index_selector.rs"]
mod vector_index;
