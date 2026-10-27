// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Each helper renders exactly what the `DriverError` variant its raise sites
//! used to build renders: code, SQLSTATE, message, identity and the
//! evaluation-origin flag (`MysqlError`'s `PartialEq` covers all five).

use crate::{DriverError, ExecError, MysqlError};
use tidb_expr::EvalError;

use tidb_ddl_build::errors::*;

fn s(text: &str) -> String {
    text.to_owned()
}

#[test]
fn ddl_build_errors_match_the_driver_variants() {
    let cases: Vec<(DriverError, MysqlError)> = vec![
        (DriverError::unsupported("a reason"), unsupported("a reason")),
        (DriverError::from(tidb_util::misc::syntax_error(s("near 'x'"))), parse_error("near 'x'")),
        (
            DriverError::from((&tidb_util::dbterror::ERR_TOO_LONG_IDENT).generate_with_stack(s("m"))),
            MysqlError::terror(&tidb_util::dbterror::ERR_TOO_LONG_IDENT, "m"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE.generate_with_stack_by_args(&["placement".into()])), opt_on_temporary_table("placement")),
        (
            DriverError::from(tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE.generate_with_stack_by_args(&["pre split regions".into()])),
            opt_on_temporary_table("pre split regions"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE.generate_with_stack_by_args(&["set TiFlash replica".into()])),
            opt_on_temporary_table("set TiFlash replica"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_OPT_ON_TEMPORARY_TABLE.generate_with_stack_by_args(&["masking policy".into()])), opt_on_temporary_table("masking policy")),
        (DriverError::from(tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE.generate_with_stack_by_args(&["PLACEMENT".into()])), opt_on_temporary_table("PLACEMENT")),
        (DriverError::from(tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE.generate_with_stack_by_args(&["auto_random".into()])), opt_on_temporary_table("auto_random")),
        (
            DriverError::from(tidb_error::plannererrors::ERR_OPT_ON_TEMPORARY_TABLE.generate_with_stack_by_args(&["shard_row_id_bits".into()])),
            opt_on_temporary_table("shard_row_id_bits"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_ON_COMMIT_PRESERVE.generate_with_stack_by_args(&[])), unsupported_on_commit_preserve()),
        (DriverError::from(tidb_util::dbterror::ERR_PARTITION_NO_TEMPORARY.generate_with_stack_by_args(&[])), partition_no_temporary()),
        (DriverError::from(tidb_util::dbterror::ERR_OPT_ON_CACHE_TABLE.generate_with_stack_by_args(&["Drop Table".into()])), opt_on_cache_table("Drop Table")),
        (DriverError::from(tidb_util::dbterror::ERR_PARTITION_MGMT_ON_NONPARTITIONED.generate_with_stack_by_args(&[])), partition_mgmt_on_nonpartitioned()),
        (DriverError::from(tidb_util::dbterror::ERR_DROP_PARTITION_NON_EXISTENT.generate_with_stack_by_args(&["DROP".into()])), drop_partition_non_existent()),
        (DriverError::from(tidb_util::dbterror::ERR_DROP_LAST_PARTITION.generate_with_stack_by_args(&[])), drop_last_partition()),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT.generate_with_stack_by_args(&[(s("p")).into()])),
            foreign_key_cannot_open_parent("p"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE.generate_with_stack_by_args(&[(s("a")).into(), (s("fk")).into()])),
            foreign_key_column_cannot_change("a", "fk"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE_CHILD.generate_with_stack_by_args(&[(s("a")).into(), (s("fk")).into(), (s("d.c")).into()])),
            foreign_key_column_cannot_change_child("a", "fk", "d.c"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_WRONG_TABLE_NAME.generate_with_stack_by_args(&[(s("t ")).into()])),
            wrong_table_name("t "),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_SEQUENCE_INVALID_DATA.generate_with_stack_by_args(&["d".into(), "s".into()])),
            sequence_values_conflicting("d", "s"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_SEQUENCE_UNSUPPORTED_TABLE_OPTION.generate_with_stack_by_args(&[(s("x")).into()])),
            sequence_unsupported_table_option("x"),
        ),
        (
            DriverError::from(tidb_error::catalog::table::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(&[(s("p9")).into(), (s("t")).into()])),
            unknown_partition("p9", "t"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_DUP_KEY_NAME.generate_with_stack_by_args(&[(s("k")).into()])), duplicate_key_name("k")),
        (DriverError::from(tidb_util::dbterror::exeerrors::ERR_PASSWORD_FORMAT.generate_with_stack_by_args(&[])), tidb_privilege::errors::password_format()),
        (
            DriverError::from(tidb_util::dbterror::exeerrors::ERR_PLUGIN_IS_NOT_LOADED.generate_with_stack_by_args(&[(s("p")).into()])),
            tidb_privilege::errors::plugin_is_not_loaded("p"),
        ),
        (
            DriverError::from(tidb_util::dbterror::exeerrors::ERR_EXISTS_IN_HISTORY_PASSWORD.generate_with_stack_by_args(&[(s("u")).into(), (s("%")).into()])),
            tidb_privilege::errors::exists_in_history_password("u", "%"),
        ),
        (
            DriverError::from(tidb_error::catalog::kv::ERR_KEY_EXISTS.fast_generate_by_args(&[(s("v")).into(), (s("mysql.password_history.PRIMARY")).into()])),
            tidb_privilege::errors::duplicate_entry("v", "mysql.password_history.PRIMARY"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_ONLY_ON_RANGE_LIST_PARTITION.generate_with_stack_by_args(&["DROP".into()])), only_on_range_list_partition("DROP")),
        (DriverError::from(tidb_util::dbterror::ERR_COALESCE_ONLY_ON_HASH_PARTITION.generate_with_stack_by_args(&[])), coalesce_only_on_hash_partition()),
        (DriverError::from(tidb_error::catalog::ast::ERR_COALESCE_PARTITION_NO_PARTITION.generate_with_stack_by_args(&[])), coalesce_partition_no_partition()),
        (DriverError::from(tidb_error::catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS.generate_with_stack_by_args(&[(s("p")).into()])), placement_policy_not_exists("p")),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_WRONG_OBJECT.generate_with_stack_by_args(&["d".into(), "v".into(), "VIEW".into()])),
            wrong_object("d", "v", "VIEW"),
        ),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_TABLE_EXISTS.generate_with_stack_by_args(&[(s("d.t")).into()])),
            table_exists("d.t"),
        ),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_TABLE_NOT_EXISTS.generate_with_stack_by_args(&["d".into(), "t".into()])),
            table_not_exists("d", "t"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_ERROR_ON_RENAME.generate_with_stack_by_args(&[(s("d.t")).into(), (s("x.t")).into(), 168.into(), format!("Database `{}` doesn't exist", s("x")).into()])),
            error_on_rename_database_missing("d.t", "x.t", "x"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_WRONG_TABLE_NAME.generate_with_stack_by_args(&[(s("t2")).into()])),
            tidb_ddl_build::column_changes::check_column_qualifier(&[s("t2")], "d", "t")
                .unwrap_err(),
        ),
        (
            DriverError::from((&tidb_util::dbterror::ERR_WRONG_DB_NAME).generate_with_stack(s("Incorrect database name 'd2'"))),
            tidb_ddl_build::column_changes::check_column_qualifier(&[s("d2"), s("t")], "d", "t")
                .unwrap_err(),
        ),
        (
            DriverError::from(tidb_error::catalog::types::ERR_TRUNCATED_WRONG_VAL.generate_with_stack_by_args(&[(s("DOUBLE")).into(), (s("x")).into()])),
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::TruncatedIncorrectValue {
                    kind: "DOUBLE",
                    value: s("x"),
                },
            ),
        ),
        (
            DriverError::from(tidb_error::catalog::types::ERR_TRUNCATED.generate_with_stack(format!("Data truncated for column '{}', value is '{}'", s("c"), s("x")))),
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::DataTruncatedValue {
                    column: s("c"),
                    value: s("x"),
                },
            ),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_INVALID_USE_OF_NULL.generate_with_stack_by_args(&[])),
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::InvalidUseOfNull,
            ),
        ),
        (
            DriverError::from(tidb_error::catalog::kv::ERR_KEY_EXISTS.fast_generate_by_args(&[(s("1")).into(), (s("t.PRIMARY")).into()])),
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::DuplicateEntry {
                    value: s("1"),
                    key: s("t.PRIMARY"),
                },
            ),
        ),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_TABLE_LOCKED.generate_with_stack_by_args(&[(s("t")).into(), (tidb_ast::TableLockType::WRITE).sql().into(), (s("server: s, session: 7")).into()])),
            table_locked("t", tidb_ast::TableLockType::WRITE, "server: s, session: 7"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_TEMP_TABLE_NOT_ALLOWED_WITH_TTL.generate_with_stack_by_args(&[])), temp_table_not_allowed_with_ttl()),
        (DriverError::from(tidb_util::dbterror::ERR_BAD_FIELD.generate_with_stack_by_args(&[(s("c")).into(), "TTL config".into()])), unknown_column_in_ttl_config("c")),
        (DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_COLUMN_IN_TTL_CONFIG.generate_with_stack_by_args(&[(s("c")).into()])), unsupported_column_in_ttl_config("c")),
        (
            DriverError::from(tidb_util::dbterror::ERR_SET_TTL_OPTION_FOR_NON_TTL_TABLE.fast_generate_by_args(&[(s("TTL_ENABLE")).into()])),
            set_ttl_option_for_non_ttl_table("TTL_ENABLE"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_PRIMARY_KEY_TYPE_WITH_TTL.generate_with_stack_by_args(&[])), unsupported_primary_key_type_with_ttl()),
        (DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK.generate_with_stack_by_args(&[])), ttl_referenced_by_foreign_key()),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NOT_MATCH.generate_with_stack_by_args(&[(s("fk")).into(), (s("why")).into()])),
            wrong_fk_def("fk", "why"),
        ),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL.generate_with_stack_by_args(&[(s("c")).into(), (s("fk")).into()])),
            foreign_key_column_not_null("c", "fk"),
        ),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT.generate_with_stack_by_args(&[(s("fk")).into(), (s("t")).into()])),
            foreign_key_no_index_in_parent("fk", "t"),
        ),
        (DriverError::from(tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED.generate_with_stack_by_args(&[])), foreign_key_on_partitioned()),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN.generate_with_stack_by_args(&[(s("fk")).into(), (s("c")).into()])),
            foreign_key_uses_virtual_column("fk", "c"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_WRONG_FK_OPTION_FOR_GENERATED_COLUMN.generate_with_stack_by_args(&[(s("ON UPDATE SET NULL")).into()])),
            wrong_fk_option_for_generated_column("ON UPDATE SET NULL"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_FK_INCOMPATIBLE_COLUMNS.generate_with_stack_by_args(&[(s("a")).into(), (s("b")).into(), (s("fk")).into()])),
            fk_incompatible_columns("a", "b", "fk"),
        ),
        (
            DriverError::from(tidb_error::catalog::infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT.generate_with_stack_by_args(&[(s("c")).into(), (s("fk")).into(), (s("t")).into()])),
            foreign_key_referenced_column_missing("c", "fk", "t"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_KEY_COLUMN_DOES_NOT_EXITS.generate_with_stack_by_args(&[(s("c")).into()])), foreign_key_child_column_missing("c")),
        (DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_SHARD_ROW_ID_BITS.generate_with_stack_by_args(&[])), unsupported_shard_row_id_bits()),
        (DriverError::from(tidb_error::catalog::infoschema::ERR_COLUMN_EXISTS.generate_with_stack_by_args(&[(s("c")).into()])), duplicate_column_name("c")),
        (DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.generate_with_stack_by_args(&["why".into()])), unsupported_modify_column("why")),
        (
            DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_MODIFY_COLUMN.generate_with_stack_by_args(&[format!("change from original type {} to {} is currently unsupported yet", s("int"), s("vector")).into()])),
            unsupported_modify_column_type("int", "vector"),
        ),
        (DriverError::from(tidb_error::plannererrors::ERR_WINDOW_INVALID_WINDOW_FUNC_USE.generate_with_stack_by_args(&[(s("rank")).into()])), window_invalid_window_func_use("rank")),
        (DriverError::from(tidb_error::plannererrors::ERR_NOT_SUPPORTED_YET.generate_with_stack_by_args(&["x".into()])), not_supported_yet("x")),
        (DriverError::from(tidb_error::plannererrors::ERR_INVALID_GROUP_FUNC_USE.generate_with_stack_by_args(&[])), invalid_group_func_use()),
        (DriverError::from(tidb_util::dbterror::ERR_BLOB_KEY_WITHOUT_LENGTH.generate_with_stack_by_args(&[(s("c")).into()])), blob_key_without_length("c")),
        (DriverError::from(tidb_util::dbterror::ERR_INCORRECT_PREFIX_KEY.generate_with_stack_by_args(&[])), incorrect_prefix_key()),
        (DriverError::from(tidb_util::dbterror::ERR_KEY_PART0.generate_with_stack_by_args(&[(s("c")).into()])), key_part0("c")),
        (DriverError::from(tidb_util::dbterror::ERR_WRONG_KEY_COLUMN.generate_with_stack_by_args(&[(s("c")).into()])), wrong_key_column("c")),
        (DriverError::from(tidb_util::dbterror::ERR_TOO_LONG_KEY.generate_with_stack_by_args(&[4000.into(), 3072.into()])), too_long_key(4000, 3072)),
        (DriverError::from(tidb_util::dbterror::ERR_JSON_USED_AS_KEY.generate_with_stack_by_args(&[(s("c")).into()])), json_used_in_key("c")),
        (DriverError::from(tidb_util::dbterror::ERR_BLOB_CANT_HAVE_DEFAULT.generate_with_stack_by_args(&[(s("c")).into()])), blob_cant_have_default("c")),
        (
            DriverError::from(tidb_error::plannererrors::ERR_UNKNOWN_COLUMN.generate_with_stack_by_args(&[tidb_util::stringutil::go_to_lower(&s("ÀB")).into(), (s("expression")).into()])),
            unknown_column_in_clause("ÀB", "expression"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED.generate_with_stack_by_args(&[(s("c")).into()])),
            generated_column_function_not_allowed("c"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_WRONG_EXPR_IN_PARTITION_FUNC.generate_with_stack_by_args(&[])), partition_wrong_expr_in_func()),
        (DriverError::from(tidb_util::dbterror::ERR_PARTITION_FUNC_NOT_ALLOWED.generate_with_stack_by_args(&["PARTITION".into()])), partition_func_wrong_type()),
        (DriverError::from(tidb_util::dbterror::ERR_SAME_NAME_PARTITION_FIELD.generate_with_stack_by_args(&[(s("a")).into()])), partition_duplicate_field("a")),
        (DriverError::from(tidb_util::dbterror::ERR_WRONG_TYPE_COLUMN_VALUE.generate_with_stack_by_args(&[])), partition_column_value_wrong_type()),
        (DriverError::from(tidb_error::catalog::ast::ERR_PARTITION_COLUMN_LIST.generate_with_stack_by_args(&[])), partition_column_list()),
        (DriverError::from(tidb_util::dbterror::ERR_TOO_MANY_PARTITIONS.generate_with_stack_by_args(&[])), partition_too_many()),
        (DriverError::from(tidb_error::catalog::ast::ERR_SUBPARTITION.generate_with_stack_by_args(&[])), partition_subpartition()),
        (
            DriverError::from(tidb_util::dbterror::ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF.generate_with_stack_by_args(&[(s("PRIMARY KEY")).into()])),
            partition_unique_key_need_all_fields("PRIMARY KEY"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_TOO_LONG_TABLE_PARTITION_COMMENT.generate_with_stack_by_args(&[(s("p0")).into(), 1024.into()])),
            partition_comment_too_long("p0", 1024),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_PARTITION_CONST_DOMAIN.generate_with_stack_by_args(&[])), partition_const_domain()),
        (DriverError::from(tidb_error::catalog::table::ERR_UNKNOWN_PARTITION.generate_with_stack_by_args(&[])), partition_metadata_unknown()),
        (DriverError::from(tidb_error::mysql::MysqlError::unknown("Table partition metadata not correct, neither partition expression or list of partition columns")), partition_metadata_incomplete()),
        (DriverError::from(tidb_error::catalog::ast::ERR_NO_PARTS.generate_with_stack_by_args(&["partitions".into()])), partition_no_parts("partitions")),
        (DriverError::from(tidb_util::dbterror::ERR_SAME_NAME_PARTITION.generate_with_stack_by_args(&[(s("p0")).into()])), partition_same_name("p0")),
        (DriverError::from(tidb_util::dbterror::ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED.generate_with_stack_by_args(&[])), partition_function_not_allowed()),
        (DriverError::from(tidb_util::dbterror::ERR_NULL_IN_VALUES_LESS_THAN.generate_with_stack_by_args(&[])), partition_null_in_values_less_than()),
        (DriverError::from(tidb_util::dbterror::ERR_NOT_ALLOWED_TYPE_IN_PARTITION.generate_with_stack_by_args(&[(s("c")).into()])), partition_field_type_not_allowed("c")),
        (DriverError::from(tidb_util::dbterror::ERR_FIELD_NOT_FOUND_PART.generate_with_stack_by_args(&[])), partition_field_not_found()),
        (DriverError::from(tidb_util::dbterror::ERR_GLOBAL_INDEX_NOT_EXPLICITLY_SET.generate_with_stack_by_args(&[(s("ix")).into()])), partition_global_index_needed("ix")),
        (
            DriverError::from(tidb_error::catalog::ast::ERR_PARTITION_WRONG_VALUES.generate_with_stack_by_args(&["RANGE".into(), "LESS THAN".into()])),
            partition_wrong_values("RANGE", "LESS THAN"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_PARTITION_MAXVALUE.generate_with_stack_by_args(&[])), partition_max_value_not_last()),
        (DriverError::from(tidb_error::catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.generate_with_stack_by_args(&["RANGE".into()])), partitions_must_be_defined("RANGE")),
        (DriverError::from(tidb_util::dbterror::ERR_RANGE_NOT_INCREASING.generate_with_stack_by_args(&[])), partition_range_not_increasing()),
        (DriverError::from(tidb_util::dbterror::ERR_MULTIPLE_DEF_CONST_IN_LIST_PART.generate_with_stack_by_args(&[])), partition_duplicate_list_value()),
        (DriverError::from(tidb_util::dbterror::ERR_VALUES_IS_NOT_INT_TYPE.generate_with_stack_by_args(&[(s("p0")).into()])), partition_values_not_int("p0")),
        (
            DriverError::from(tidb_util::dbterror::ERR_WRONG_KEY_COLUMN_FUNCTIONAL_INDEX.generate_with_stack_by_args(&[(s("a + 1")).into()])),
            wrong_key_column_functional_index("a + 1"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION.generate_with_stack_by_args(&[])), functional_index_on_json()),
        (DriverError::from(tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_BLOB.generate_with_stack_by_args(&[])), functional_index_on_blob()),
        (DriverError::from(tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ON_FIELD.generate_with_stack_by_args(&[])), functional_index_on_field()),
        (
            DriverError::from(tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_FUNCTION_IS_NOT_ALLOWED.generate_with_stack_by_args(&[(s("ix")).into()])),
            functional_index_function_not_allowed("ix"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_FUNCTIONAL_INDEX_ROW_VALUE_IS_NOT_ALLOWED.generate_with_stack_by_args(&[(s("ix")).into()])), functional_index_row_value("ix")),
        (DriverError::from(tidb_util::dbterror::ERR_GENERATED_COLUMN_ROW_VALUE_IS_NOT_ALLOWED.generate_with_stack_by_args(&[(s("c")).into()])), generated_column_row_value("c")),
        (DriverError::from(tidb_util::dbterror::ERR_EXPRESSION_INDEX_CAN_NOT_REFER.generate_with_stack_by_args(&[(s("ix")).into()])), expression_index_can_not_refer("ix")),
        (DriverError::from(tidb_util::dbterror::ERR_UNSUPPORTED_EXPRESSION_INDEX.generate_with_stack_by_args(&[])), unsafe_function_in_expression_index()),
        (DriverError::from(tidb_error::catalog::expression::ERR_INCORRECT_PARAMETER_COUNT.generate_with_stack_by_args(&[(s("f")).into()])), wrong_param_count_to_native_fct("f")),
        (DriverError::from(tidb_util::dbterror::ERR_TOO_LONG_IDENT.generate_with_stack_by_args(&[(s("x")).into()])), too_long_ident("x")),
        (DriverError::from(tidb_util::dbterror::ERR_ADD_COLUMN_WITH_SEQUENCE_AS_DEFAULT.generate_with_stack_by_args(&[(s("c")).into()])), add_column_sequence_default("c")),
        (DriverError::from(tidb_util::dbterror::ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION.generate_with_stack_by_args(&[])), binlog_unsafe_system_function()),
        (DriverError::from(tidb_util::dbterror::ERR_INVALID_DEFAULT_VALUE.generate_with_stack_by_args(&[(s("c")).into()])), invalid_default("c")),
        (
            DriverError::from(tidb_util::dbterror::ERR_DEF_VAL_GENERATED_NAMED_FUNCTION_IS_NOT_ALLOWED.generate_with_stack_by_args(&[(s("c")).into(), (s("rand")).into()])),
            default_function_not_allowed("c", "rand"),
        ),
        (
            DriverError::from(tidb_util::dbterror::ERR_COLUMN_TYPE_UNSUPPORTED_NEXT_VALUE.generate_with_stack_by_args(&[(s("c")).into()])),
            unsupported_sequence_default_type("c"),
        ),
        (DriverError::from(tidb_util::dbterror::ERR_PRIMARY_CANT_HAVE_NULL.generate_with_stack_by_args(&[])), primary_cant_have_null()),
        (
            DriverError::from(tidb_error::catalog::types::ERR_OVERFLOW.generate_with_stack(format!("constant {} overflows {}", s("300"), s("tinyint")))),
            constant_overflows("300", "tinyint"),
        ),
        (DriverError::from(tidb_error::catalog::autoid::ERR_AUTOINC_READ_FAILED.generate_with_stack_by_args(&[])), autoinc_read_failed()),
        (DriverError::from(tidb_error::mysql::MysqlError::unknown(s("detail"))), auto_id_unavailable("detail")),
        (DriverError::from(tidb_util::dbterror::ERR_INVALID_AUTO_RANDOM.generate_with_stack_by_args(&[(s("why")).into()])), invalid_auto_random("why")),
    ];
    for (old, new) in cases {
        let label = format!("{old:?}");
        assert_eq!(old.to_mysql_error(), new, "{label}");
    }
}

/// A typed evaluation error the package used to wrap as
/// `DriverError::Exec(ExecError::Eval(_))` -- or to pass through `?` from an
/// `ExecError` -- now leaves as `EvalError::to_mysql_error`, the
/// statement-level `Unknown charset` build failure included.
#[test]
fn evaluation_errors_render_as_the_driver_did() {
    for error in [
        EvalError::IntOverflow,
        EvalError::DivisionByZero,
        EvalError::WrongParameterCount("now"),
        EvalError::Unsupported("a window frame in RANGE units"),
        EvalError::Unsupported("Unknown charset 'x'"),
    ] {
        let label = format!("{error:?}");
        assert_eq!(
            DriverError::Exec(ExecError::Eval(error.clone())).to_mysql_error(),
            error.to_mysql_error(),
            "{label}"
        );
        assert_eq!(
            DriverError::from(ExecError::Eval(error.clone())).to_mysql_error(),
            error.to_mysql_error(),
            "{label}"
        );
    }
}

/// Go's table-layer write errors (`KvTableError::into_mysql_error`) render
/// what the write path's `kv_write_error` and `index_backfill_error` render
/// through `DriverError`, for every variant shape.
#[test]
fn table_write_errors_match_the_driver_rendering() {
    use tidb_kv::storage::StorageError;
    use tidb_table::kv_table::KvTableError;
    let cases: Vec<fn() -> KvTableError> = vec![
        || KvTableError::Storage(StorageError::Sql(MysqlError::unknown("sql"))),
        || KvTableError::Storage(StorageError::Retryable(s("region"))),
        || KvTableError::Storage(StorageError::NotFound),
        || KvTableError::Storage(StorageError::Backend(s("backend"))),
        || KvTableError::ColumnCast(MysqlError::unknown("cast")),
        || KvTableError::DuplicateEntry {
            value: s("1"),
            key: s("t.PRIMARY"),
        },
        || KvTableError::Generation {
            column: s("g"),
            detail: s("d"),
            eval: Some(EvalError::DivisionByZero),
        },
        || KvTableError::Generation {
            column: s("g"),
            detail: s("d"),
            eval: None,
        },
        || KvTableError::CheckConstraint {
            name: s("c"),
            detail: s("d"),
            eval: Some(EvalError::IntOverflow),
        },
        || KvTableError::CheckConstraint {
            name: s("c"),
            detail: s("d"),
            eval: None,
        },
        || KvTableError::PartitionEval(EvalError::DivisionByZero),
        || KvTableError::CheckConstraintViolated(s("c")),
        || KvTableError::NoPartitionForValue(s("7")),
        || KvTableError::RowDoesNotMatchGivenPartitionSet,
        || KvTableError::PartitionValueOverflowsBigint(s("18446744073709551615")),
        || KvTableError::AutoIdExhausted,
        || KvTableError::Encode(s("e")),
        || KvTableError::Decode(s("d")),
        || KvTableError::InvalidUseOfNull,
        || KvTableError::CacheTableUnsupported("Alter Table"),
    ];
    for case in cases {
        let label = format!("{:?}", case());
        assert_eq!(
            crate::driver::kv_write_error(case()).to_mysql_error(),
            case().into_mysql_error(),
            "{label}"
        );
        assert_eq!(
            crate::ddl::index_backfill_error(case()).to_mysql_error(),
            tidb_ddl_build::table_backfill::index_backfill_error(case()),
            "{label}"
        );
    }
    assert_eq!(
        crate::ddl::index_backfill_error(KvTableError::DuplicateKeyName(s("k"))).to_mysql_error(),
        tidb_ddl_build::table_backfill::index_backfill_error(KvTableError::DuplicateKeyName(s("k"))),
    );
}
