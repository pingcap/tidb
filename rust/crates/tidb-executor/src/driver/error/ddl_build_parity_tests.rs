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
        (DriverError::Parse(s("near 'x'")), parse_error("near 'x'")),
        (
            DriverError::DdlCoded {
                proto: &tidb_util::dbterror::ERR_TOO_LONG_IDENT,
                message: s("m"),
            },
            MysqlError::terror(&tidb_util::dbterror::ERR_TOO_LONG_IDENT, "m"),
        ),
        (DriverError::OptOnTemporaryTable("placement"), opt_on_temporary_table("placement")),
        (
            DriverError::OptOnTemporaryTable("pre split regions"),
            opt_on_temporary_table("pre split regions"),
        ),
        (
            DriverError::OptOnTemporaryTable("set TiFlash replica"),
            opt_on_temporary_table("set TiFlash replica"),
        ),
        (DriverError::OptOnTemporaryTable("masking policy"), opt_on_temporary_table("masking policy")),
        (DriverError::OptOnTemporaryTable("PLACEMENT"), opt_on_temporary_table("PLACEMENT")),
        (DriverError::OptOnTemporaryTable("auto_random"), opt_on_temporary_table("auto_random")),
        (
            DriverError::OptOnTemporaryTable("shard_row_id_bits"),
            opt_on_temporary_table("shard_row_id_bits"),
        ),
        (DriverError::UnsupportedOnCommitPreserve, unsupported_on_commit_preserve()),
        (DriverError::PartitionNoTemporary, partition_no_temporary()),
        (DriverError::OperationOnCachedTable("Drop Table"), opt_on_cache_table("Drop Table")),
        (DriverError::PartitionManagementOnNonpartitioned, partition_mgmt_on_nonpartitioned()),
        (DriverError::PartitionDropNonexistent, drop_partition_non_existent()),
        (DriverError::PartitionDropLast, drop_last_partition()),
        (
            DriverError::ForeignKeyReferencedTableMissing(s("p")),
            foreign_key_cannot_open_parent("p"),
        ),
        (
            DriverError::ForeignKeyColumnCannotChange {
                column: s("a"),
                constraint: s("fk"),
            },
            foreign_key_column_cannot_change("a", "fk"),
        ),
        (
            DriverError::ForeignKeyColumnCannotChangeChild {
                column: s("a"),
                constraint: s("fk"),
                child_table: s("d.c"),
            },
            foreign_key_column_cannot_change_child("a", "fk", "d.c"),
        ),
        (
            DriverError::Schema(crate::SchemaErrorKind::WrongTableName(s("t "))),
            wrong_table_name("t "),
        ),
        (
            DriverError::Schema(crate::SchemaErrorKind::SequenceValuesConflicting(s("d.s"))),
            sequence_values_conflicting("d.s"),
        ),
        (
            DriverError::SequenceUnsupportedTableOption(s("x")),
            sequence_unsupported_table_option("x"),
        ),
        (
            DriverError::UnknownPartition {
                partition: s("p9"),
                table: s("t"),
            },
            unknown_partition("p9", "t"),
        ),
        (DriverError::DuplicateKeyName(s("k")), duplicate_key_name("k")),
        (DriverError::PasswordFormat, tidb_privilege::errors::password_format()),
        (
            DriverError::PluginIsNotLoaded { plugin: s("p") },
            tidb_privilege::errors::plugin_is_not_loaded("p"),
        ),
        (
            DriverError::PasswordInHistory {
                user: s("u"),
                host: s("%"),
            },
            tidb_privilege::errors::exists_in_history_password("u", "%"),
        ),
        (
            DriverError::DuplicateEntry {
                value: s("v"),
                key: s("mysql.password_history.PRIMARY"),
            },
            tidb_privilege::errors::duplicate_entry("v", "mysql.password_history.PRIMARY"),
        ),
        (DriverError::PartitionOnlyRangeList("DROP"), only_on_range_list_partition("DROP")),
        (DriverError::CoalesceOnlyOnHashPartition, coalesce_only_on_hash_partition()),
        (DriverError::CoalescePartitionNoPartition, coalesce_partition_no_partition()),
        (DriverError::PlacementPolicyNotExists(s("p")), placement_policy_not_exists("p")),
        (
            DriverError::Schema(crate::SchemaErrorKind::WrongObject {
                name: s("d.v"),
                expected: "VIEW",
            }),
            wrong_object("d.v", "VIEW"),
        ),
        (
            DriverError::Schema(crate::SchemaErrorKind::TableExists(s("d.t"))),
            table_exists("d.t"),
        ),
        (
            DriverError::Schema(crate::SchemaErrorKind::UnknownTable(s("d.t"))),
            table_not_exists("d.t"),
        ),
        (
            DriverError::Schema(crate::SchemaErrorKind::RenameTargetDatabaseMissing {
                from: s("d.t"),
                to: s("x.t"),
                database: s("x"),
            }),
            error_on_rename_database_missing("d.t", "x.t", "x"),
        ),
        (
            DriverError::Schema(crate::SchemaErrorKind::WrongTableName(s("t2"))),
            tidb_ddl_build::column_changes::check_column_qualifier(&[s("t2")], "d", "t")
                .unwrap_err(),
        ),
        (
            DriverError::DdlCoded {
                proto: &tidb_util::dbterror::ERR_WRONG_DB_NAME,
                message: s("Incorrect database name 'd2'"),
            },
            tidb_ddl_build::column_changes::check_column_qualifier(&[s("d2"), s("t")], "d", "t")
                .unwrap_err(),
        ),
        (
            DriverError::TruncatedIncorrectValue {
                kind: s("DOUBLE"),
                value: s("x"),
            },
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::TruncatedIncorrectValue {
                    kind: "DOUBLE",
                    value: s("x"),
                },
            ),
        ),
        (
            DriverError::DataTruncatedValue {
                column: s("c"),
                value: s("x"),
            },
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::DataTruncatedValue {
                    column: s("c"),
                    value: s("x"),
                },
            ),
        ),
        (
            DriverError::InvalidUseOfNull,
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::InvalidUseOfNull,
            ),
        ),
        (
            DriverError::DuplicateEntry {
                value: s("1"),
                key: s("t.PRIMARY"),
            },
            tidb_ddl_build::column_changes::modify_column_error(
                tidb_table::kv_table::KvTableError::DuplicateEntry {
                    value: s("1"),
                    key: s("t.PRIMARY"),
                },
            ),
        ),
        (
            DriverError::TableLocked {
                table: s("t"),
                lock_type: tidb_ast::TableLockType::WRITE,
                holder: s("server: s, session: 7"),
            },
            table_locked("t", tidb_ast::TableLockType::WRITE, "server: s, session: 7"),
        ),
        (DriverError::TempTableNotAllowedWithTTL, temp_table_not_allowed_with_ttl()),
        (DriverError::UnknownColumnInTtlConfig(s("c")), unknown_column_in_ttl_config("c")),
        (DriverError::UnsupportedColumnInTtlConfig(s("c")), unsupported_column_in_ttl_config("c")),
        (
            DriverError::SetTtlOptionForNonTtlTable(s("TTL_ENABLE")),
            set_ttl_option_for_non_ttl_table("TTL_ENABLE"),
        ),
        (DriverError::UnsupportedPrimaryKeyTypeWithTtl, unsupported_primary_key_type_with_ttl()),
        (DriverError::TtlReferencedByForeignKey, ttl_referenced_by_foreign_key()),
        (
            DriverError::WrongFkDef {
                name: s("fk"),
                reason: s("why"),
            },
            wrong_fk_def("fk", "why"),
        ),
        (
            DriverError::ForeignKeyColumnNotNull {
                column: s("c"),
                constraint: s("fk"),
            },
            foreign_key_column_not_null("c", "fk"),
        ),
        (
            DriverError::ForeignKeyNoIndexInParent {
                constraint: s("fk"),
                table: s("t"),
            },
            foreign_key_no_index_in_parent("fk", "t"),
        ),
        (DriverError::ForeignKeyOnPartitioned, foreign_key_on_partitioned()),
        (
            DriverError::ForeignKeyUsesVirtualColumn {
                foreign_key: s("fk"),
                column: s("c"),
            },
            foreign_key_uses_virtual_column("fk", "c"),
        ),
        (
            DriverError::WrongFkOptionForGeneratedColumn {
                clause: s("ON UPDATE SET NULL"),
            },
            wrong_fk_option_for_generated_column("ON UPDATE SET NULL"),
        ),
        (
            DriverError::FkIncompatibleColumns {
                referencing: s("a"),
                referenced: s("b"),
                constraint: s("fk"),
            },
            fk_incompatible_columns("a", "b", "fk"),
        ),
        (
            DriverError::ForeignKeyReferencedColumnMissing {
                column: s("c"),
                constraint: s("fk"),
                table: s("t"),
            },
            foreign_key_referenced_column_missing("c", "fk", "t"),
        ),
        (DriverError::ForeignKeyChildColumnMissing(s("c")), foreign_key_child_column_missing("c")),
        (DriverError::UnsupportedShardRowIdBits, unsupported_shard_row_id_bits()),
        (DriverError::DuplicateColumnName(s("c")), duplicate_column_name("c")),
        (DriverError::UnsupportedModifyColumn("why"), unsupported_modify_column("why")),
        (
            DriverError::UnsupportedModifyColumnType {
                from: s("int"),
                to: s("vector"),
            },
            unsupported_modify_column_type("int", "vector"),
        ),
        (DriverError::WindowInvalidWindowFuncUse(s("rank")), window_invalid_window_func_use("rank")),
        (DriverError::NotSupportedYet("x".into()), not_supported_yet("x")),
        (DriverError::InvalidGroupFuncUse, invalid_group_func_use()),
        (DriverError::BlobKeyWithoutLength(s("c")), blob_key_without_length("c")),
        (DriverError::IncorrectPrefixKey, incorrect_prefix_key()),
        (DriverError::KeyPart0(s("c")), key_part0("c")),
        (DriverError::WrongKeyColumn(s("c")), wrong_key_column("c")),
        (DriverError::TooLongKey { length: 4000, max: 3072 }, too_long_key(4000, 3072)),
        (DriverError::JsonUsedInKey(s("c")), json_used_in_key("c")),
        (DriverError::BlobCantHaveDefault(s("c")), blob_cant_have_default("c")),
        (
            DriverError::UnknownColumnInClause {
                column: s("ÀB"),
                clause: s("expression"),
            },
            unknown_column_in_clause("ÀB", "expression"),
        ),
        (
            DriverError::GeneratedColumnFunctionNotAllowed(s("c")),
            generated_column_function_not_allowed("c"),
        ),
        (DriverError::PartitionWrongExprInFunc, partition_wrong_expr_in_func()),
        (DriverError::PartitionFuncWrongType, partition_func_wrong_type()),
        (DriverError::PartitionDuplicateField(s("a")), partition_duplicate_field("a")),
        (DriverError::PartitionColumnValueWrongType, partition_column_value_wrong_type()),
        (DriverError::PartitionColumnList, partition_column_list()),
        (DriverError::PartitionTooMany, partition_too_many()),
        (DriverError::PartitionSubpartition, partition_subpartition()),
        (
            DriverError::PartitionUniqueKeyNeedAllFields(s("PRIMARY KEY")),
            partition_unique_key_need_all_fields("PRIMARY KEY"),
        ),
        (
            DriverError::PartitionCommentTooLong {
                name: s("p0"),
                limit: 1024,
            },
            partition_comment_too_long("p0", 1024),
        ),
        (DriverError::PartitionConstDomain, partition_const_domain()),
        (DriverError::PartitionMetadataUnknown, partition_metadata_unknown()),
        (DriverError::PartitionMetadataIncomplete, partition_metadata_incomplete()),
        (DriverError::PartitionNoParts("partitions"), partition_no_parts("partitions")),
        (DriverError::PartitionSameName(s("p0")), partition_same_name("p0")),
        (DriverError::PartitionFunctionNotAllowed, partition_function_not_allowed()),
        (DriverError::PartitionNullInValuesLessThan, partition_null_in_values_less_than()),
        (DriverError::PartitionFieldTypeNotAllowed(s("c")), partition_field_type_not_allowed("c")),
        (DriverError::PartitionFieldNotFound, partition_field_not_found()),
        (DriverError::PartitionGlobalIndexNeeded(s("ix")), partition_global_index_needed("ix")),
        (
            DriverError::PartitionWrongValues {
                method: "RANGE",
                clause: "VALUES LESS THAN",
            },
            partition_wrong_values("RANGE", "VALUES LESS THAN"),
        ),
        (DriverError::PartitionMaxValueNotLast, partition_max_value_not_last()),
        (DriverError::PartitionsMustBeDefined("RANGE"), partitions_must_be_defined("RANGE")),
        (DriverError::PartitionRangeNotIncreasing, partition_range_not_increasing()),
        (DriverError::PartitionDuplicateListValue, partition_duplicate_list_value()),
        (DriverError::PartitionValuesNotInt(s("p0")), partition_values_not_int("p0")),
        (
            DriverError::WrongKeyColumnFunctionalIndex(s("a + 1")),
            wrong_key_column_functional_index("a + 1"),
        ),
        (DriverError::FunctionalIndexOnJson, functional_index_on_json()),
        (DriverError::FunctionalIndexOnBlob, functional_index_on_blob()),
        (DriverError::FunctionalIndexOnField, functional_index_on_field()),
        (
            DriverError::FunctionalIndexFunctionNotAllowed(s("ix")),
            functional_index_function_not_allowed("ix"),
        ),
        (DriverError::FunctionalIndexRowValue(s("ix")), functional_index_row_value("ix")),
        (DriverError::GeneratedColumnRowValue(s("c")), generated_column_row_value("c")),
        (DriverError::ExpressionIndexCanNotRefer(s("ix")), expression_index_can_not_refer("ix")),
        (DriverError::UnsafeFunctionInExpressionIndex, unsafe_function_in_expression_index()),
        (DriverError::WrongParamCountToNativeFct(s("f")), wrong_param_count_to_native_fct("f")),
        (DriverError::TooLongIdent(s("x")), too_long_ident("x")),
        (DriverError::AddColumnSequenceDefault(s("c")), add_column_sequence_default("c")),
        (DriverError::BinlogUnsafeSystemFunction, binlog_unsafe_system_function()),
        (DriverError::InvalidDefault(s("c")), invalid_default("c")),
        (
            DriverError::DefaultFunctionNotAllowed(s("c"), s("rand")),
            default_function_not_allowed("c", "rand"),
        ),
        (
            DriverError::UnsupportedSequenceDefaultType(s("c")),
            unsupported_sequence_default_type("c"),
        ),
        (DriverError::PrimaryCantHaveNull, primary_cant_have_null()),
        (
            DriverError::ConstantOverflows {
                value: s("300"),
                type_name: s("tinyint"),
            },
            constant_overflows("300", "tinyint"),
        ),
        (DriverError::AutoincReadFailed, autoinc_read_failed()),
        (DriverError::AutoIdUnavailable(s("detail")), auto_id_unavailable("detail")),
        (DriverError::InvalidAutoRandom(s("why")), invalid_auto_random("why")),
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
