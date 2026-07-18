//! Stable Cargo shard for independently owned query-form parser selectors.

#[path = "selectors/query/binding_stmt_selector.rs"]
mod binding_stmt;
#[path = "selectors/query/char_using_charset_selector.rs"]
mod char_using_charset;
#[path = "selectors/query/charset_introducer_selector.rs"]
mod charset_introducer;
#[path = "selectors/query/convert_using_charset_selector.rs"]
mod convert_using_charset;
#[path = "selectors/query/cte_scalar_union_selector.rs"]
mod cte_scalar_union;
#[path = "selectors/query/current_datetime_precision_selector.rs"]
mod current_datetime_precision;
#[path = "selectors/query/desc_describe_query_selector.rs"]
mod desc_describe_query;
#[path = "selectors/query/describe_explain_selector.rs"]
mod describe_explain;
#[path = "selectors/query/do_selector.rs"]
mod do_statement;
#[path = "selectors/query/execute_selector.rs"]
mod execute;
#[path = "selectors/query/exists_setopr_selector.rs"]
mod exists_setopr;
#[path = "selectors/query/explain_binary_charset_selector.rs"]
mod explain_binary_charset;
#[path = "selectors/query/explain_plan_tree_selector.rs"]
mod explain_plan_tree;
#[path = "selectors/query/explain_values_selector.rs"]
mod explain_values;
#[path = "selectors/query/index_level_hint_selector.rs"]
mod index_level_hint;
#[path = "selectors/query/json_memberof_restore_failure_selector.rs"]
mod json_memberof_restore_failure;
#[path = "selectors/query/limit_uint64_selector.rs"]
mod limit_uint64;
#[path = "selectors/query/negative_join_hint_selector.rs"]
mod negative_join_hint;
#[path = "selectors/query/nested_cte_selector.rs"]
mod nested_cte;
#[path = "selectors/query/parenthesized_derived_join_selector.rs"]
mod parenthesized_derived_join;
#[path = "selectors/query/parenthesized_setopr_selector.rs"]
mod parenthesized_setopr;
#[path = "selectors/query/plan_replayer_dump_explain_selector.rs"]
mod plan_replayer_dump_explain;
#[path = "selectors/query/qb_name_view_list_selector.rs"]
mod qb_name_view_list;
#[path = "selectors/query/recursive_lateral_cte_selector.rs"]
mod recursive_lateral_cte;
#[path = "selectors/query/string_literal_alias_selector.rs"]
mod string_literal_alias;
#[path = "selectors/query/values_statement_selector.rs"]
mod values_statement;
#[path = "selectors/query/with_parenthesized_selector.rs"]
mod with_parenthesized;
#[path = "selectors/query/with_setopr_selector.rs"]
mod with_setopr;
