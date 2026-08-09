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

//! Lockdown gates for Go `pkg/meta/model/job_args.go` and its direct test
//! owner `job_args_test.go`.

use std::collections::{BTreeMap, BTreeSet};

use sha2::{Digest, Sha256};

use crate::job_args::{get_or_decode_args, get_or_decode_args_v2};
use crate::{
    fill_rollback_args_for_add_partition, get_alter_index_visibility_args,
    get_alter_table_mode_args, get_batch_create_table_args, get_create_schema_args,
    get_create_table_args, get_drop_foreign_key_args, get_drop_schema_args,
    get_exchange_table_partition_args, get_finished_drop_schema_args,
    get_finished_table_partition_args, get_finished_truncate_table_args, get_modify_schema_args,
    get_modify_table_auto_id_cache_args, get_modify_table_charset_and_collate_args,
    get_modify_table_comment_args, get_modify_table_engine_attribute_args, get_rebase_auto_id_args,
    get_refresh_meta_args, get_set_default_value_args, get_shard_row_id_args,
    get_table_partition_args, get_truncate_table_args, index_arg_columnar_index_type,
    rename_tables_args_from_v1, AlterIndexVisibilityArgs, AlterTableModeArgs, BatchCreateTableArgs,
    ColumnarIndexType, CreateSchemaArgs, CreateTableArgs, DropForeignKeyArgs, DropSchemaArgs,
    EmptyArgs, ExchangeTablePartitionArgs, FinishedJobArgs, GoAny, GoShared, GoSharedSlice,
    IndexOp, Job, JobArgs, ModifySchemaArgs, ModifyTableAutoIDCacheArgs,
    ModifyTableCharsetAndCollateArgs, ModifyTableCommentArgs, ModifyTableEngineAttributeArgs,
    RebaseAutoIDArgs, RefreshMetaArgs, RenameTableArgs, SetDefaultValueArgs, ShardRowIDArgs,
    TableIDIndexID, TablePartitionArgs, TruncateTableArgs,
};
use tidb_ast::CiString;

type RenameFromV1Fn =
    fn(&[i64], &[CiString], &[CiString], &[i64], &[CiString], &[i64]) -> Vec<RenameTableArgs>;

const GO_OWNER: &[u8] = include_bytes!("../../../../pkg/meta/model/job_args.go");
const GO_TEST_SUPPORT: &[u8] = include_bytes!("../../../../pkg/meta/model/job_args_test.go");
const INVENTORY: &str = include_str!("job_args_go_inventory.tsv");

const GO_OWNER_SHA256: &str = "da1b59226cb2d3a05bbb65c945709088a33fac605237b4e195377c10ebec27bf";
const GO_OWNER_BYTES: usize = 65_331;
const GO_OWNER_LINES: usize = 1_917;
const GO_TEST_SHA256: &str = "7647a990a36ff19f4bc1e56ed915466c6be7c87c32bd0c68a443e24378edb382";
const GO_TEST_BYTES: usize = 39_148;
const GO_TEST_LINES: usize = 1_242;
const ORDERED_AST_IDENTITY_SHA256: &str =
    "8fd015cea480707605e59092382e84518b084d03b5d85a9b13bf965cc2604d8c";

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[test]
fn pinned_go_owner_and_direct_test_support_have_not_drifted() {
    assert_eq!(GO_OWNER.len(), GO_OWNER_BYTES);
    assert_eq!(
        GO_OWNER.iter().filter(|byte| **byte == b'\n').count(),
        GO_OWNER_LINES
    );
    assert_eq!(sha256(GO_OWNER), GO_OWNER_SHA256);

    assert_eq!(GO_TEST_SUPPORT.len(), GO_TEST_BYTES);
    assert_eq!(
        GO_TEST_SUPPORT
            .iter()
            .filter(|byte| **byte == b'\n')
            .count(),
        GO_TEST_LINES
    );
    assert_eq!(sha256(GO_TEST_SUPPORT), GO_TEST_SHA256);
}

#[test]
fn every_ast_obligation_has_exactly_one_concrete_verdict() {
    let expected_categories = BTreeMap::from([
        ("branch", 186_usize),
        ("const", 3),
        ("declaration", 55),
        ("field", 177),
        ("function", 161),
        ("loop", 24),
        ("short_circuit", 18),
        ("switch_case", 13),
        ("test", 44),
        ("test_assertion", 240),
        ("test_branch", 32),
        ("test_helper", 2),
        ("test_helper_closure", 4),
        ("test_loop", 144),
        ("test_row", 509),
    ]);
    let mut categories = BTreeMap::<&str, usize>::new();
    let mut verdicts = BTreeMap::<&str, usize>::new();
    let mut ids = BTreeSet::new();
    let mut ordered_identity = Vec::new();

    for (line_index, line) in INVENTORY.lines().enumerate() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 9, "inventory line {}", line_index + 1);
        let [id, category, source, anchor, node_hash, owner, verdict, symbol, evidence] =
            <[&str; 9]>::try_from(fields).expect("nine inventory fields");
        assert!(ids.insert(id), "duplicate obligation {id}");
        assert!(matches!(verdict, "PORTED" | "DECLINED" | "UNREACHABLE"));
        assert!(!evidence.is_empty());
        assert!(!evidence.contains("TODO"));
        match verdict {
            "PORTED" => assert_ne!(symbol, "-", "PORTED obligation {id} has no symbol"),
            "DECLINED" => {
                assert_eq!(symbol, "-");
                assert!(evidence.starts_with("Measured boundary:"));
            }
            "UNREACHABLE" => {
                assert_eq!(symbol, "-");
                assert!(evidence.starts_with("Structural proof:"));
            }
            _ => unreachable!(),
        }
        *categories.entry(category).or_default() += 1;
        *verdicts.entry(verdict).or_default() += 1;
        ordered_identity.extend_from_slice(
            format!("{id}\t{category}\t{source}\t{anchor}\t{node_hash}\t{owner}\n").as_bytes(),
        );
    }

    assert_eq!(ids.len(), 1_612);
    assert_eq!(categories, expected_categories);
    assert_eq!(verdicts.get("PORTED"), Some(&589));
    assert_eq!(verdicts.get("DECLINED"), Some(&1_023));
    assert_eq!(verdicts.get("UNREACHABLE"), None);
    assert_eq!(sha256(&ordered_identity), ORDERED_AST_IDENTITY_SHA256);
}

#[test]
fn every_ported_symbol_remains_compile_anchored() {
    let _ = IndexOp::ADD_INDEX;
    let _ = IndexOp::DROP_INDEX;
    let _ = IndexOp::ROLLBACK_ADD_INDEX;
    let rename = RenameTableArgs::default();
    let _ = (
        rename.old_schema_id,
        rename.old_schema_name,
        rename.new_table_name,
        rename.old_table_name,
        rename.new_schema_id,
        rename.table_id,
        rename.old_schema_id_for_schema_diff,
    );
    let _: fn(ColumnarIndexType, bool) -> ColumnarIndexType = index_arg_columnar_index_type;
    let _: RenameFromV1Fn = rename_tables_args_from_v1;
    let _ = get_or_decode_args::<CreateSchemaArgs>;
    let _ = get_or_decode_args_v2::<CreateSchemaArgs>;
    let _ = get_create_schema_args;
    let _ = get_drop_schema_args;
    let _ = get_finished_drop_schema_args;
    let _ = get_modify_schema_args;
    let _ = get_create_table_args;
    let _ = get_batch_create_table_args;
    let _ = get_truncate_table_args;
    let _ = get_finished_truncate_table_args;
    let _ = get_table_partition_args;
    let _ = get_finished_table_partition_args;
    let _ = fill_rollback_args_for_add_partition;
    let _ = get_exchange_table_partition_args;
    let _ = get_rebase_auto_id_args;
    let _ = get_modify_table_comment_args;
    let _ = get_modify_table_charset_and_collate_args;
    let _ = get_alter_index_visibility_args;
    let _ = get_drop_foreign_key_args;
    let _ = get_modify_table_auto_id_cache_args;
    let _ = get_shard_row_id_args;
    let _ = get_set_default_value_args;
    let _ = get_refresh_meta_args;
    let _ = get_modify_table_engine_attribute_args;
    let _ = get_alter_table_mode_args;
    let _ = <EmptyArgs as JobArgs>::get_args_v1;
    let _ = <EmptyArgs as JobArgs>::decode_v1;
    let _ = <CreateSchemaArgs as JobArgs>::get_args_v1;
    let _ = <CreateSchemaArgs as JobArgs>::decode_v1;
    let _ = <DropSchemaArgs as JobArgs>::get_args_v1;
    let _ = <DropSchemaArgs as JobArgs>::decode_v1;
    let _ = <DropSchemaArgs as FinishedJobArgs>::get_finished_args_v1;
    let _ = <ModifySchemaArgs as JobArgs>::get_args_v1;
    let _ = <ModifySchemaArgs as JobArgs>::decode_v1;
    let _ = <CreateTableArgs as JobArgs>::get_args_v1;
    let _ = <CreateTableArgs as JobArgs>::decode_v1;
    let _ = <BatchCreateTableArgs as JobArgs>::get_args_v1;
    let _ = <BatchCreateTableArgs as JobArgs>::decode_v1;
    let _ = <TruncateTableArgs as JobArgs>::get_args_v1;
    let _ = <TruncateTableArgs as JobArgs>::decode_v1;
    let _ = <TruncateTableArgs as FinishedJobArgs>::get_finished_args_v1;
    let _ = <TablePartitionArgs as JobArgs>::get_args_v1;
    let _ = <TablePartitionArgs as JobArgs>::decode_v1;
    let _ = <TablePartitionArgs as FinishedJobArgs>::get_finished_args_v1;
    let _ = <ExchangeTablePartitionArgs as JobArgs>::get_args_v1;
    let _ = <ExchangeTablePartitionArgs as JobArgs>::decode_v1;
    let _ = <RebaseAutoIDArgs as JobArgs>::get_args_v1;
    let _ = <RebaseAutoIDArgs as JobArgs>::decode_v1;
    let _ = <ModifyTableCommentArgs as JobArgs>::get_args_v1;
    let _ = <ModifyTableCommentArgs as JobArgs>::decode_v1;
    let _ = <ModifyTableCharsetAndCollateArgs as JobArgs>::get_args_v1;
    let _ = <ModifyTableCharsetAndCollateArgs as JobArgs>::decode_v1;
    let _ = <AlterIndexVisibilityArgs as JobArgs>::get_args_v1;
    let _ = <AlterIndexVisibilityArgs as JobArgs>::decode_v1;
    let _ = <DropForeignKeyArgs as JobArgs>::get_args_v1;
    let _ = <DropForeignKeyArgs as JobArgs>::decode_v1;
    let _ = <ModifyTableAutoIDCacheArgs as JobArgs>::get_args_v1;
    let _ = <ModifyTableAutoIDCacheArgs as JobArgs>::decode_v1;
    let _ = <ShardRowIDArgs as JobArgs>::get_args_v1;
    let _ = <ShardRowIDArgs as JobArgs>::decode_v1;
    let _ = <SetDefaultValueArgs as JobArgs>::get_args_v1;
    let _ = <SetDefaultValueArgs as JobArgs>::decode_v1;
    let _ = <RefreshMetaArgs as JobArgs>::get_args_v1;
    let _ = <RefreshMetaArgs as JobArgs>::decode_v1;
    let _ = <ModifyTableEngineAttributeArgs as JobArgs>::get_args_v1;
    let _ = <ModifyTableEngineAttributeArgs as JobArgs>::decode_v1;
    let _ = <AlterTableModeArgs as JobArgs>::get_args_v1;
    let _ = <AlterTableModeArgs as JobArgs>::decode_v1;
    let _ = EmptyArgs::default();
    let _ = CreateSchemaArgs::default().db_info;
    let drop_schema = DropSchemaArgs::default();
    let _ = (drop_schema.fk_check, drop_schema.all_dropped_table_ids);
    let modify_schema = ModifySchemaArgs::default();
    let _ = (
        modify_schema.to_charset,
        modify_schema.to_collate,
        modify_schema.policy_ref,
    );
    let create_table = CreateTableArgs::default();
    let _ = (
        create_table.table_info,
        create_table.on_exist_replace,
        create_table.old_view_table_id,
        create_table.fk_check,
    );
    let _: fn(crate::JobVersion, crate::ActionType, GoShared<CreateSchemaArgs>) -> Job =
        crate::job_args::tests::encoded_job::<CreateSchemaArgs>;
    let _: fn(crate::JobVersion, crate::ActionType, GoShared<DropSchemaArgs>) -> Job =
        crate::job_args::tests::encoded_finished_job::<DropSchemaArgs>;
    let _: fn() = crate::job_args::tests::v2_getter_reuses_the_exact_typed_pointer;
    let _: fn() =
        crate::job_args::tests::first_source_getter_matrix_round_trips_values_in_both_versions;
    let _: fn() =
        crate::job_args::tests::batch_create_table_v1_shares_one_fk_flag_and_v2_keeps_each_value;
    let _: fn() =
        crate::job_args::tests::truncate_table_submission_and_finished_action_matrix_matches_source;
    let _: fn() = crate::job_args::tests::table_partition_args_match_source;
    let _: fn() =
        crate::job_args::tests::finished_table_partition_matrix_and_add_assertion_match_source;
    let table_partition = TablePartitionArgs::default();
    let _ = (
        table_partition.part_names,
        table_partition.part_info,
        table_partition.old_physical_table_ids,
        table_partition.old_global_indexes,
        table_partition.new_partition_ids,
    );
    let table_index = TableIDIndexID::default();
    let _ = (table_index.table_id, table_index.index_id);
    let exchange = ExchangeTablePartitionArgs::default();
    let _ = (
        exchange.partition_id,
        exchange.partitioned_table_schema_id,
        exchange.partitioned_table_id,
        exchange.partition_name,
        exchange.with_validation,
    );
    let _: fn() = crate::job_args::tests::exchange_table_partition_args_match_source;
    let rebase = RebaseAutoIDArgs::default();
    let _ = (rebase.new_base, rebase.force);
    let _ = ModifyTableCommentArgs::default().comment;
    let charset = ModifyTableCharsetAndCollateArgs::default();
    let _ = (
        charset.to_charset,
        charset.to_collate,
        charset.needs_overwrite_columns,
    );
    let _: fn() = crate::job_args::tests::rebase_auto_id_args_match_source_matrix;
    let _: fn() = crate::job_args::tests::modify_table_comment_args_match_source_matrix;
    let _: fn() = crate::job_args::tests::modify_table_charset_and_collate_args_pin_every_field;
    let _: fn() =
        crate::job_args::tests::scalar_and_existing_model_args_match_the_source_v1_v2_matrix;
    let _: fn() =
        crate::job_args::tests::new_native_args_preserve_go_null_duplicate_and_pointer_rules;
    let _: Option<GoSharedSlice<GoAny>> = None;

    let symbols: BTreeSet<_> = INVENTORY
        .lines()
        .filter(|line| !line.starts_with('#') && !line.is_empty())
        .filter_map(|line| {
            let fields: Vec<_> = line.split('\t').collect();
            (fields[6] == "PORTED").then_some(fields[7])
        })
        .collect();
    assert_eq!(
        symbols,
        BTreeSet::from([
            "job_args::AlterIndexVisibilityArgs",
            "job_args::AlterIndexVisibilityArgs::decode_v1",
            "job_args::AlterIndexVisibilityArgs::get_args_v1",
            "job_args::AlterTableModeArgs",
            "job_args::AlterTableModeArgs::decode_v1",
            "job_args::AlterTableModeArgs::get_args_v1",
            "job_args::BatchCreateTableArgs",
            "job_args::BatchCreateTableArgs::decode_v1",
            "job_args::BatchCreateTableArgs::get_args_v1",
            "job_args::CreateSchemaArgs",
            "job_args::CreateSchemaArgs::decode_v1",
            "job_args::CreateSchemaArgs::get_args_v1",
            "job_args::CreateTableArgs",
            "job_args::CreateTableArgs::decode_v1",
            "job_args::CreateTableArgs::get_args_v1",
            "job_args::DropForeignKeyArgs",
            "job_args::DropForeignKeyArgs::decode_v1",
            "job_args::DropForeignKeyArgs::get_args_v1",
            "job_args::DropSchemaArgs",
            "job_args::DropSchemaArgs::decode_v1",
            "job_args::DropSchemaArgs::get_args_v1",
            "job_args::DropSchemaArgs::get_finished_args_v1",
            "job_args::EmptyArgs",
            "job_args::EmptyArgs::decode_v1",
            "job_args::EmptyArgs::get_args_v1",
            "job_args::ExchangeTablePartitionArgs",
            "job_args::ExchangeTablePartitionArgs::decode_v1",
            "job_args::ExchangeTablePartitionArgs::get_args_v1",
            "job_args::FinishedJobArgs",
            "job_args::IndexOp",
            "job_args::JobArgs",
            "job_args::ModifySchemaArgs",
            "job_args::ModifySchemaArgs::decode_v1",
            "job_args::ModifySchemaArgs::get_args_v1",
            "job_args::ModifyTableAutoIDCacheArgs",
            "job_args::ModifyTableAutoIDCacheArgs::decode_v1",
            "job_args::ModifyTableAutoIDCacheArgs::get_args_v1",
            "job_args::ModifyTableCharsetAndCollateArgs",
            "job_args::ModifyTableCharsetAndCollateArgs::decode_v1",
            "job_args::ModifyTableCharsetAndCollateArgs::get_args_v1",
            "job_args::ModifyTableCommentArgs",
            "job_args::ModifyTableCommentArgs::decode_v1",
            "job_args::ModifyTableCommentArgs::get_args_v1",
            "job_args::ModifyTableEngineAttributeArgs",
            "job_args::ModifyTableEngineAttributeArgs::decode_v1",
            "job_args::ModifyTableEngineAttributeArgs::get_args_v1",
            "job_args::RebaseAutoIDArgs",
            "job_args::RebaseAutoIDArgs::decode_v1",
            "job_args::RebaseAutoIDArgs::get_args_v1",
            "job_args::RefreshMetaArgs",
            "job_args::RefreshMetaArgs::decode_v1",
            "job_args::RefreshMetaArgs::get_args_v1",
            "job_args::RenameTableArgs",
            "job_args::SetDefaultValueArgs",
            "job_args::SetDefaultValueArgs::decode_v1",
            "job_args::SetDefaultValueArgs::get_args_v1",
            "job_args::ShardRowIDArgs",
            "job_args::ShardRowIDArgs::decode_v1",
            "job_args::ShardRowIDArgs::get_args_v1",
            "job_args::TruncateTableArgs",
            "job_args::TruncateTableArgs::decode_v1",
            "job_args::TruncateTableArgs::get_args_v1",
            "job_args::TruncateTableArgs::get_finished_args_v1",
            "job_args::TableIDIndexID",
            "job_args::TablePartitionArgs",
            "job_args::TablePartitionArgs::decode_v1",
            "job_args::TablePartitionArgs::get_args_v1",
            "job_args::TablePartitionArgs::get_finished_args_v1",
            "job_args::fill_rollback_args_for_add_partition",
            "job_args::get_alter_index_visibility_args",
            "job_args::get_alter_table_mode_args",
            "job_args::get_batch_create_table_args",
            "job_args::get_create_schema_args",
            "job_args::get_create_table_args",
            "job_args::get_drop_foreign_key_args",
            "job_args::get_drop_schema_args",
            "job_args::get_exchange_table_partition_args",
            "job_args::get_finished_drop_schema_args",
            "job_args::get_modify_schema_args",
            "job_args::get_modify_table_auto_id_cache_args",
            "job_args::get_modify_table_charset_and_collate_args",
            "job_args::get_modify_table_comment_args",
            "job_args::get_modify_table_engine_attribute_args",
            "job_args::get_rebase_auto_id_args",
            "job_args::get_refresh_meta_args",
            "job_args::get_set_default_value_args",
            "job_args::get_shard_row_id_args",
            "job_args::get_finished_truncate_table_args",
            "job_args::get_finished_table_partition_args",
            "job_args::get_or_decode_args",
            "job_args::get_or_decode_args_v2",
            "job_args::get_truncate_table_args",
            "job_args::get_table_partition_args",
            "job_args::index_arg_columnar_index_type",
            "job_args::rename_tables_args_from_v1",
            "job_args::tests::batch_create_table_v1_shares_one_fk_flag_and_v2_keeps_each_value",
            "job_args::tests::encoded_finished_job",
            "job_args::tests::encoded_job",
            "job_args::tests::exchange_table_partition_args_match_source",
            "job_args::tests::first_source_getter_matrix_round_trips_values_in_both_versions",
            "job_args::tests::modify_table_comment_args_match_source_matrix",
            "job_args::tests::rebase_auto_id_args_match_source_matrix",
            "job_args::tests::scalar_and_existing_model_args_match_the_source_v1_v2_matrix",
            "job_args::tests::truncate_table_submission_and_finished_action_matrix_matches_source",
            "job_args::tests::table_partition_args_match_source",
            "job_args::tests::finished_table_partition_matrix_and_add_assertion_match_source",
            "job_args::tests::v2_getter_reuses_the_exact_typed_pointer",
        ])
    );
}

#[test]
fn declined_rows_pin_the_remaining_typed_job_args_boundary() {
    let rust_job = include_str!("job.rs");
    for native_generic_symbol in [
        "pub struct Job {",
        "pub raw_args: Option<PersistedRawJson>",
        "pub fn fill_raw_args(",
        "pub fn encode(",
        "pub fn decode(",
    ] {
        assert!(
            rust_job.contains(native_generic_symbol),
            "generic Job envelope disappeared: {native_generic_symbol}"
        );
    }
    let rust_job_args = include_str!("job_args.rs");
    for present_typed_symbol in [
        "pub trait JobArgs",
        "pub trait FinishedJobArgs",
        "pub struct CreateSchemaArgs",
        "pub fn get_create_schema_args",
        "pub struct TablePartitionArgs",
        "pub fn get_table_partition_args",
        "pub struct ExchangeTablePartitionArgs",
        "pub fn get_exchange_table_partition_args",
        "pub struct RebaseAutoIDArgs",
        "pub fn get_rebase_auto_id_args",
        "pub struct ModifyTableCommentArgs",
        "pub fn get_modify_table_comment_args",
        "pub struct ModifyTableCharsetAndCollateArgs",
        "pub fn get_modify_table_charset_and_collate_args",
        "pub struct AlterIndexVisibilityArgs",
        "pub fn get_alter_index_visibility_args",
        "pub struct DropForeignKeyArgs",
        "pub fn get_drop_foreign_key_args",
        "pub struct ModifyTableAutoIDCacheArgs",
        "pub fn get_modify_table_auto_id_cache_args",
        "pub struct ShardRowIDArgs",
        "pub fn get_shard_row_id_args",
        "pub struct SetDefaultValueArgs",
        "pub fn get_set_default_value_args",
        "pub struct RefreshMetaArgs",
        "pub fn get_refresh_meta_args",
        "pub struct ModifyTableEngineAttributeArgs",
        "pub fn get_modify_table_engine_attribute_args",
        "pub struct AlterTableModeArgs",
        "pub fn get_alter_table_mode_args",
    ] {
        assert!(
            rust_job_args.contains(present_typed_symbol),
            "typed job-argument surface disappeared: {present_typed_symbol}"
        );
    }
    let absent_typed_symbol = "pub struct DropTableArgs";
    assert!(
        !rust_job_args.contains(absent_typed_symbol),
        "remaining typed job-argument boundary changed: {absent_typed_symbol}"
    );

    const CURRENT_BOUNDARY: &str = "Measured boundary: the source-shaped JobArgs and FinishedJobArgs framework plus Empty, CreateSchema, DropSchema, ModifySchema, CreateTable, BatchCreateTable, TruncateTable, TablePartition, ExchangeTablePartition, RebaseAutoID, ModifyTableComment, ModifyTableCharsetAndCollate, AlterIndexVisibility, DropForeignKey, ModifyTableAutoIDCache, ShardRowID, SetDefaultValue, RefreshMeta, ModifyTableEngineAttribute, and AlterTableMode argument types are native; this obligation belongs to a later concrete argument type or its imported AST or PD representation, so no source-equivalent typed entry point exists yet.";
    assert_eq!(INVENTORY.matches(CURRENT_BOUNDARY).count(), 1_023);
    assert!(!INVENTORY.contains("explicitly defers Job RawArgs Encode Decode and FillArgs"));

    let go_tests = std::str::from_utf8(GO_TEST_SUPPORT).expect("Go test source is UTF-8");
    assert!(go_tests.contains("func getJobBytes("));
    assert!(go_tests.contains("func getFinishedJobBytes("));
    assert!(go_tests.contains("j.FillArgs(inArgs)"));
    assert!(go_tests.contains("j.FillFinishedArgs(inArgs)"));
    assert_eq!(go_tests.matches("func Test").count(), 44);
}
