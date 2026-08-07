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

//! Executable drift, classification, receipt, and compile-symbol gates for
//! the complete `pkg/meta/meta.go` inventory.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use super::*;

const INVENTORY: &str = include_str!("meta_go_inventory.tsv");
const META_GO_SHA256: &str = "d0e948b97582b9f1e43ed98f6e3c2664ab71a0051161b5152768c439b0129083";
const META_TEST_GO_SHA256: &str =
    "c306fd0d4af006551eded4552e707951780bdedf99cd03d6fc134cc0233a39da";
const MAIN_TEST_GO_SHA256: &str =
    "3140c9451d6bac5c74455f15cbe67530e3e2cb052ceacd0a762002e524d307e8";
const BUILD_BAZEL_SHA256: &str = "db0b61627145acc6abbcf0e084396d34abe1fc256a357351ce9c9d73944b1c9d";
const ORDERED_IDENTITY_SHA256: &str =
    "23c46a45e0076a6e01581ad461ab861aee494c19c49008ec447383699ec15133";

const PRODUCTION_COMPILE_ANCHORS: &[&str] = &[
    "element::COLUMN_ELEMENT_KEY",
    "element::ELEMENT_KEY_LEN",
    "element::Element",
    "element::Element::decode",
    "element::Element::encode",
    "element::Element::string_bytes",
    "element::ElementKeyType",
    "element::INDEX_ELEMENT_KEY",
    "error::MetaError::DatabaseExists",
    "error::MetaError::DatabaseNotExists",
    "error::MetaError::DdlReorgElementNotExist",
    "error::MetaError::InvalidFieldPrefix",
    "error::MetaError::MaskingPolicyExpressionInvalidColumn",
    "error::MetaError::MaskingPolicyIdExists",
    "error::MetaError::MaskingPolicyIdNotExists",
    "error::MetaError::PolicyExists",
    "error::MetaError::PolicyIdNotExists",
    "error::MetaError::ResourceGroupExists",
    "error::MetaError::ResourceGroupIdNotExists",
    "error::MetaError::TableExists",
    "error::MetaError::TableNotExists",
    "key::AUTO_INCREMENT_ID_PREFIX",
    "key::AUTO_RANDOM_ID_PREFIX",
    "key::AUTO_TABLE_ID_PREFIX",
    "key::BDR_ROLE",
    "key::BOOTSTRAP",
    "key::BOOT_TABLE_VERSION",
    "key::DBS",
    "key::DB_PREFIX",
    "key::DDL_JOB_HISTORY",
    "key::DDL_TABLE_VERSION",
    "key::DXF_SCHEDULE_TUNE",
    "key::INGEST_MAX_BATCH_SPLIT_RANGES",
    "key::INGEST_MAX_INFLIGHT",
    "key::INGEST_MAX_PER_SEC",
    "key::INGEST_MAX_SPLIT_RANGES_PER_SEC",
    "key::MASKING_POLICIES",
    "key::MASKING_POLICY_GLOBAL_ID",
    "key::MASKING_POLICY_PREFIX",
    "key::METADATA_LOCK",
    "key::NEXT_GLOBAL_ID",
    "key::POLICIES",
    "key::POLICY_GLOBAL_ID",
    "key::POLICY_PREFIX",
    "key::REQUEST_UNIT_STATS",
    "key::RESOURCE_GROUPS",
    "key::RESOURCE_GROUP_PREFIX",
    "key::SCHEMA_CACHE_SIZE",
    "key::SCHEMA_DIFF_PREFIX",
    "key::SCHEMA_VERSION",
    "key::SEQUENCE_CYCLE_PREFIX",
    "key::SEQUENCE_PREFIX",
    "key::TABLE_PREFIX",
    "key::auto_increment_id_key",
    "key::auto_random_table_id_key",
    "key::auto_table_id_key",
    "key::db_key",
    "key::is_auto_increment_id_key",
    "key::is_auto_random_table_id_key",
    "key::is_auto_table_id_key",
    "key::is_db_key",
    "key::is_sequence_key",
    "key::is_table_key",
    "key::parse_auto_increment_id_key",
    "key::parse_auto_random_table_id_key",
    "key::parse_auto_table_id_key",
    "key::parse_db_key",
    "key::parse_sequence_key",
    "key::parse_table_key",
    "key::sequence_key",
    "key::table_key",
    "tidb_codec::table_key::META_PREFIX",
    "transaction::DEFAULT_RESOURCE_GROUP_ID",
    "transaction::DailyRuStats",
    "transaction::DdlTableVersion",
    "transaction::DdlTableVersion::BACKFILL",
    "transaction::DdlTableVersion::BASE",
    "transaction::DdlTableVersion::DDL_NOTIFIER",
    "transaction::DdlTableVersion::INIT",
    "transaction::DdlTableVersion::MDL",
    "transaction::FOREIGN_KEY_ATTRIBUTES_NIL",
    "transaction::FOREIGN_KEY_ATTRIBUTES_ZERO",
    "transaction::GLOBAL_ID_MUTEX",
    "transaction::GroupRuStats",
    "transaction::HistoryDdlJobIterator",
    "transaction::HistoryDdlJobIterator::get_last_jobs",
    "transaction::JOB_EXTRACT_FIELDS",
    "transaction::LastJobIterator",
    "transaction::MASKING_POLICY_ID_MUTEX",
    "transaction::MustLoadFilterAttr",
    "transaction::Mutator",
    "transaction::Mutator::add_history_ddl_job",
    "transaction::Mutator::add_resource_group",
    "transaction::Mutator::advance_global_ids",
    "transaction::Mutator::all_name_to_id_and_must_loaded_table_info",
    "transaction::Mutator::auto_ids",
    "transaction::Mutator::auto_table_id_key_value",
    "transaction::Mutator::bdr_role",
    "transaction::Mutator::bootstrap_version",
    "transaction::Mutator::clear_bdr_role",
    "transaction::Mutator::create_database",
    "transaction::Mutator::create_masking_policy",
    "transaction::Mutator::create_mysql_database_if_not_exists",
    "transaction::Mutator::create_policy",
    "transaction::Mutator::create_sequence_and_set_value",
    "transaction::Mutator::create_sys_database_by_id",
    "transaction::Mutator::create_sys_database_by_id_if_not_exists",
    "transaction::Mutator::create_table_and_set_auto_id",
    "transaction::Mutator::create_table_or_view",
    "transaction::Mutator::database",
    "transaction::Mutator::database_exists",
    "transaction::Mutator::databases",
    "transaction::Mutator::ddl_job_history_key",
    "transaction::Mutator::ddl_table_version",
    "transaction::Mutator::drop_database",
    "transaction::Mutator::drop_masking_policy",
    "transaction::Mutator::drop_policy",
    "transaction::Mutator::drop_resource_group",
    "transaction::Mutator::drop_sequence",
    "transaction::Mutator::drop_table_or_view",
    "transaction::Mutator::dxf_schedule_tune_factors",
    "transaction::Mutator::encoded_schema_diff_key",
    "transaction::Mutator::finish_bootstrap",
    "transaction::Mutator::gen_global_id",
    "transaction::Mutator::gen_global_ids",
    "transaction::Mutator::gen_masking_policy_id",
    "transaction::Mutator::gen_placement_policy_id",
    "transaction::Mutator::gen_schema_version",
    "transaction::Mutator::gen_schema_versions",
    "transaction::Mutator::global_id",
    "transaction::Mutator::global_id_key",
    "transaction::Mutator::history_ddl_count",
    "transaction::Mutator::history_ddl_job",
    "transaction::Mutator::history_ddl_jobs",
    "transaction::Mutator::ingest_max_batch_split_ranges",
    "transaction::Mutator::ingest_max_inflight",
    "transaction::Mutator::ingest_max_per_sec",
    "transaction::Mutator::ingest_max_split_ranges_per_sec",
    "transaction::Mutator::iter_databases",
    "transaction::Mutator::iter_tables",
    "transaction::Mutator::last_history_ddl_jobs",
    "transaction::Mutator::last_history_ddl_jobs_with_filter",
    "transaction::Mutator::masking_policies",
    "transaction::Mutator::masking_policy",
    "transaction::Mutator::masking_policy_id",
    "transaction::Mutator::metadata_lock",
    "transaction::Mutator::metas_by_database_id",
    "transaction::Mutator::new_with_options",
    "transaction::Mutator::next_gen_boot_table_version",
    "transaction::Mutator::policies",
    "transaction::Mutator::policy",
    "transaction::Mutator::policy_id",
    "transaction::Mutator::resource_group",
    "transaction::Mutator::resource_groups",
    "transaction::Mutator::restart_sequence_value",
    "transaction::Mutator::ru_stats",
    "transaction::Mutator::schema_cache_size",
    "transaction::Mutator::schema_diff",
    "transaction::Mutator::schema_version",
    "transaction::Mutator::schema_version_with_non_empty_diff",
    "transaction::Mutator::set_bdr_role",
    "transaction::Mutator::set_ddl_table_version",
    "transaction::Mutator::set_dxf_schedule_tune_factors",
    "transaction::Mutator::set_ingest_max_batch_split_ranges",
    "transaction::Mutator::set_ingest_max_inflight",
    "transaction::Mutator::set_ingest_max_per_sec",
    "transaction::Mutator::set_ingest_max_split_ranges_per_sec",
    "transaction::Mutator::set_metadata_lock",
    "transaction::Mutator::set_next_gen_boot_table_version",
    "transaction::Mutator::set_ru_stats",
    "transaction::Mutator::set_schema_cache_size",
    "transaction::Mutator::set_schema_diff",
    "transaction::Mutator::simple_tables",
    "transaction::Mutator::system_database_id",
    "transaction::Mutator::table",
    "transaction::Mutator::table_exists",
    "transaction::Mutator::table_info_with_attributes",
    "transaction::Mutator::tables_with_cancel",
    "transaction::Mutator::update_database",
    "transaction::Mutator::update_masking_policy",
    "transaction::Mutator::update_policy",
    "transaction::Mutator::update_resource_group",
    "transaction::Mutator::update_table",
    "transaction::MutatorOption",
    "transaction::NAME_EXTRACT_REGEXP",
    "transaction::NextGenBootTableVersion",
    "transaction::NextGenBootTableVersion::BASE",
    "transaction::NextGenBootTableVersion::INIT",
    "transaction::NextGenBootTableVersion::MASKING_POLICY",
    "transaction::POLICY_ID_MUTEX",
    "transaction::RawTransaction",
    "transaction::RuStats",
    "transaction::TABLE_INFO_MUST_LOAD_FILTERS",
    "transaction::TABLE_NAME_INFO_FIELDS",
    "transaction::default_resource_group_for_test",
    "transaction::extract_schema_and_table_name_from_job",
    "transaction::fast_unmarshal_table_name_info",
    "transaction::iter_all_tables",
    "transaction::job_matches",
    "transaction::oldest_schema_version",
    "transaction::split_range_int64_max",
    "transaction::table_info_must_load",
    "transaction::table_info_must_load_with_filters",
    "transaction::unescape_name",
    "value::CURRENT_MAGIC_BYTE_VER",
    "value::MagicType::Json",
    "value::MagicType::Unknown",
    "value::attach_magic_byte",
    "value::detach_magic_byte",
    "value::encode_int_value",
    "value::which_magic_type",
];

fn sha256(data: &[u8]) -> String {
    Sha256::digest(data)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()
        .expect("repository root")
}

fn read_source(relative: &str) -> Vec<u8> {
    std::fs::read(repository_root().join(relative)).expect("pinned Go source input")
}

#[test]
fn source_inputs_match() {
    let meta = read_source("pkg/meta/meta.go");
    assert_eq!(meta.len(), 68_013);
    assert_eq!(meta.iter().filter(|byte| **byte == b'\n').count(), 2_219);
    assert_eq!(sha256(&meta), META_GO_SHA256);

    assert_eq!(
        sha256(&read_source("pkg/meta/meta_test.go")),
        META_TEST_GO_SHA256
    );
    assert_eq!(
        sha256(&read_source("pkg/meta/main_test.go")),
        MAIN_TEST_GO_SHA256
    );
    let build = read_source("pkg/meta/BUILD.bazel");
    assert_eq!(sha256(&build), BUILD_BAZEL_SHA256);
    let build = std::str::from_utf8(&build).expect("BUILD.bazel is UTF-8");
    for membership in ["\"meta.go\"", "\"main_test.go\"", "\"meta_test.go\""] {
        assert!(
            build.contains(membership),
            "missing Bazel membership {membership}"
        );
    }
}

#[test]
fn inventory_is_complete_unique_and_classified() {
    let mut counts = BTreeMap::<&str, usize>::new();
    let mut verdicts = BTreeMap::<&str, usize>::new();
    let mut identities = BTreeSet::new();
    let mut identity_bytes = Vec::new();

    for (index, line) in INVENTORY.lines().enumerate() {
        if line.starts_with('#')
            || line == "class\tsource_identity\tsource_line\tverdict\trust_symbol\tevidence"
        {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(
            fields.len(),
            6,
            "inventory line {} has six columns",
            index + 1
        );
        let [class, identity, source_line, verdict, symbol, evidence] =
            <[&str; 6]>::try_from(fields).expect("six inventory fields");
        assert!(
            [
                "PRODUCTION_DECLARATION",
                "PRODUCTION_FUNCTION",
                "PRODUCTION_BRANCH",
                "TEST_SUPPORT_FUNCTION",
                "TEST_SUPPORT_DECLARATION",
                "BUILD_SUPPORT"
            ]
            .contains(&class),
            "unknown inventory class {class}"
        );
        assert!(["PORTED", "DECLINED", "UNREACHABLE"].contains(&verdict));
        assert!(!identity.is_empty() && !source_line.is_empty() && !evidence.is_empty());
        let normalized = line.to_ascii_uppercase();
        for forbidden in ["TODO", "WIP", "UNTRIAGED", "PENDING"] {
            assert!(
                !normalized.contains(forbidden),
                "placeholder {forbidden} at line {}",
                index + 1
            );
        }
        assert!(
            identities.insert((class, identity)),
            "duplicate inventory identity {class}:{identity}"
        );
        if verdict == "PORTED" {
            assert_ne!(symbol, "-", "PORTED row lacks a Rust symbol");
        } else {
            assert_eq!(symbol, "-", "non-PORTED row falsely names a Rust symbol");
        }
        if class.starts_with("PRODUCTION_") {
            assert_eq!(
                verdict, "PORTED",
                "production row is not fully ported: {identity}"
            );
        }
        *counts.entry(class).or_default() += 1;
        *verdicts.entry(verdict).or_default() += 1;
        identity_bytes
            .extend_from_slice(format!("{class}\t{identity}\t{source_line}\n").as_bytes());
    }

    assert_eq!(counts["PRODUCTION_DECLARATION"], 85);
    assert_eq!(counts["PRODUCTION_FUNCTION"], 154);
    assert_eq!(counts["PRODUCTION_BRANCH"], 498);
    assert_eq!(counts["TEST_SUPPORT_FUNCTION"], 35);
    assert_eq!(counts["TEST_SUPPORT_DECLARATION"], 1);
    assert_eq!(counts["BUILD_SUPPORT"], 1);
    assert_eq!(counts.values().sum::<usize>(), 774);
    assert_eq!(verdicts["PORTED"], 761);
    assert_eq!(verdicts["DECLINED"], 13);
    assert_eq!(verdicts.get("UNREACHABLE").copied().unwrap_or_default(), 0);
    assert_eq!(sha256(&identity_bytes), ORDERED_IDENTITY_SHA256);
}

#[test]
fn every_ported_production_symbol_is_compile_anchored() {
    let anchored = PRODUCTION_COMPILE_ANCHORS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    for line in INVENTORY.lines() {
        if !line.starts_with("PRODUCTION_") {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(fields[3], "PORTED");
        assert!(
            anchored.contains(fields[4]),
            "unanchored PORTED symbol {}",
            fields[4]
        );
    }
}

#[test]
fn every_ported_test_receipt_still_compiles_as_a_test_target() {
    for line in INVENTORY.lines() {
        if !line.starts_with("TEST_SUPPORT_") || !line.contains("\tPORTED\t") {
            continue;
        }
        let fields = line.split('\t').collect::<Vec<_>>();
        let (target, function) = fields[4]
            .split_once("::")
            .expect("test target and function");
        let source = std::fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests")
                .join(format!("{target}.rs")),
        )
        .expect("ported Rust test target");
        assert!(
            source.contains(&format!("fn {function}(")),
            "ported Rust test symbol disappeared: {}",
            fields[4]
        );
    }
}

struct AnchorStore;
impl MetaSnapshotStore for AnchorStore {
    type Snapshot = MemoryTransaction;
    fn snapshot(&self, start_ts: u64) -> Self::Snapshot {
        MemoryTransaction::at_start_ts(start_ts)
    }
}

struct AnchorMvccReader;
impl MvccReader for AnchorMvccReader {
    fn mvcc_by_encoded_key(&mut self, _: &[u8], _: u64) -> Result<Option<MvccInfo>> {
        Ok(None)
    }
}

#[allow(dead_code, clippy::type_complexity)]
fn compile_anchor_every_production_symbol<
    T: RawTransaction,
    J: DdlJobCodec,
    G: ResourceGroupCodec,
>(
    meta: &Mutator<T>,
) {
    let _: Option<crate::element::ElementKeyType> = None;
    let _: Option<crate::element::Element> = None;
    let _: Option<DailyRuStats> = None;
    let _: Option<DdlTableVersion> = None;
    let _: Option<GroupRuStats> = None;
    let _: Option<HistoryDdlJobIterator<J>> = None;
    let _: Option<MustLoadFilterAttr<'static>> = None;
    let _: Option<Mutator<T>> = None;
    let _: Option<MutatorOption<T>> = None;
    let _: Option<NextGenBootTableVersion> = None;
    let _: Option<RuStats> = None;
    fn assert_raw<T: RawTransaction>() {}
    fn assert_last<J, I: LastJobIterator<J>>() {}
    let _ = assert_raw::<MemoryTransaction>;
    let _ = assert_last::<J, HistoryDdlJobIterator<J>>;
    let _ = crate::element::COLUMN_ELEMENT_KEY;
    let _ = crate::element::ELEMENT_KEY_LEN;
    let _ = crate::element::Element::decode;
    let _ = crate::element::Element::encode;
    let _ = crate::element::Element::string_bytes;
    let _ = crate::element::INDEX_ELEMENT_KEY;
    let _ = crate::error::MetaError::DatabaseExists;
    let _ = crate::error::MetaError::DatabaseNotExists;
    let _ = crate::error::MetaError::DdlReorgElementNotExist;
    let _ = crate::error::MetaError::InvalidFieldPrefix;
    let _ = crate::error::MetaError::MaskingPolicyExpressionInvalidColumn;
    let _ = crate::error::MetaError::MaskingPolicyIdExists;
    let _ = crate::error::MetaError::MaskingPolicyIdNotExists;
    let _ = crate::error::MetaError::PolicyExists;
    let _ = crate::error::MetaError::PolicyIdNotExists;
    let _ = crate::error::MetaError::ResourceGroupExists;
    let _ = crate::error::MetaError::ResourceGroupIdNotExists;
    let _ = crate::error::MetaError::TableExists;
    let _ = crate::error::MetaError::TableNotExists;
    let _ = crate::key::AUTO_INCREMENT_ID_PREFIX;
    let _ = crate::key::AUTO_RANDOM_ID_PREFIX;
    let _ = crate::key::AUTO_TABLE_ID_PREFIX;
    let _ = crate::key::BDR_ROLE;
    let _ = crate::key::BOOTSTRAP;
    let _ = crate::key::BOOT_TABLE_VERSION;
    let _ = crate::key::DBS;
    let _ = crate::key::DB_PREFIX;
    let _ = crate::key::DDL_JOB_HISTORY;
    let _ = crate::key::DDL_TABLE_VERSION;
    let _ = crate::key::DXF_SCHEDULE_TUNE;
    let _ = crate::key::INGEST_MAX_BATCH_SPLIT_RANGES;
    let _ = crate::key::INGEST_MAX_INFLIGHT;
    let _ = crate::key::INGEST_MAX_PER_SEC;
    let _ = crate::key::INGEST_MAX_SPLIT_RANGES_PER_SEC;
    let _ = crate::key::MASKING_POLICIES;
    let _ = crate::key::MASKING_POLICY_GLOBAL_ID;
    let _ = crate::key::MASKING_POLICY_PREFIX;
    let _ = crate::key::METADATA_LOCK;
    let _ = crate::key::NEXT_GLOBAL_ID;
    let _ = crate::key::POLICIES;
    let _ = crate::key::POLICY_GLOBAL_ID;
    let _ = crate::key::POLICY_PREFIX;
    let _ = crate::key::REQUEST_UNIT_STATS;
    let _ = crate::key::RESOURCE_GROUPS;
    let _ = crate::key::RESOURCE_GROUP_PREFIX;
    let _ = crate::key::SCHEMA_CACHE_SIZE;
    let _ = crate::key::SCHEMA_DIFF_PREFIX;
    let _ = crate::key::SCHEMA_VERSION;
    let _ = crate::key::SEQUENCE_CYCLE_PREFIX;
    let _ = crate::key::SEQUENCE_PREFIX;
    let _ = crate::key::TABLE_PREFIX;
    let _ = crate::key::auto_increment_id_key;
    let _ = crate::key::auto_random_table_id_key;
    let _ = crate::key::auto_table_id_key;
    let _ = crate::key::db_key;
    let _ = crate::key::is_auto_increment_id_key;
    let _ = crate::key::is_auto_random_table_id_key;
    let _ = crate::key::is_auto_table_id_key;
    let _ = crate::key::is_db_key;
    let _ = crate::key::is_sequence_key;
    let _ = crate::key::is_table_key;
    let _ = crate::key::parse_auto_increment_id_key;
    let _ = crate::key::parse_auto_random_table_id_key;
    let _ = crate::key::parse_auto_table_id_key;
    let _ = crate::key::parse_db_key;
    let _ = crate::key::parse_sequence_key;
    let _ = crate::key::parse_table_key;
    let _ = crate::key::sequence_key;
    let _ = crate::key::table_key;
    let _ = tidb_codec::table_key::META_PREFIX;
    let _ = super::DEFAULT_RESOURCE_GROUP_ID;
    let _ = super::DdlTableVersion::BACKFILL;
    let _ = super::DdlTableVersion::BASE;
    let _ = super::DdlTableVersion::DDL_NOTIFIER;
    let _ = super::DdlTableVersion::INIT;
    let _ = super::DdlTableVersion::MDL;
    let _ = super::FOREIGN_KEY_ATTRIBUTES_NIL;
    let _ = super::FOREIGN_KEY_ATTRIBUTES_ZERO;
    let _ = &super::GLOBAL_ID_MUTEX;
    let _ = HistoryDdlJobIterator::<J>::get_last_jobs;
    let _ = super::JOB_EXTRACT_FIELDS;
    let _ = &super::MASKING_POLICY_ID_MUTEX;
    let _ = Mutator::<T>::add_history_ddl_job::<J>;
    let _ = Mutator::<T>::add_resource_group::<G>;
    let _ = Mutator::<T>::advance_global_ids;
    let _ = Mutator::<T>::all_name_to_id_and_must_loaded_table_info;
    let _ = Mutator::<T>::auto_ids;
    let _ = Mutator::<T>::auto_table_id_key_value;
    let _ = Mutator::<T>::bdr_role;
    let _ = Mutator::<T>::bootstrap_version;
    let _ = Mutator::<T>::clear_bdr_role;
    let _ = Mutator::<T>::create_database;
    let _ = Mutator::<T>::create_masking_policy;
    let _ = Mutator::<T>::create_mysql_database_if_not_exists;
    let _ = Mutator::<T>::create_policy;
    let _ = Mutator::<T>::create_sequence_and_set_value;
    let _ = Mutator::<T>::create_sys_database_by_id;
    let _ = Mutator::<T>::create_sys_database_by_id_if_not_exists;
    let _ = Mutator::<T>::create_table_and_set_auto_id;
    let _ = Mutator::<T>::create_table_or_view;
    let _ = Mutator::<T>::database;
    let _ = Mutator::<T>::database_exists;
    let _ = Mutator::<T>::databases;
    let _ = Mutator::<T>::ddl_job_history_key;
    let _ = Mutator::<T>::ddl_table_version;
    let _ = Mutator::<T>::drop_database;
    let _ = Mutator::<T>::drop_masking_policy;
    let _ = Mutator::<T>::drop_policy;
    let _ = Mutator::<T>::drop_resource_group;
    let _ = Mutator::<T>::drop_sequence;
    let _ = Mutator::<T>::drop_table_or_view;
    let _ = Mutator::<T>::dxf_schedule_tune_factors;
    let _ = Mutator::<T>::encoded_schema_diff_key;
    let _ = Mutator::<T>::finish_bootstrap;
    let _ = Mutator::<T>::gen_global_id;
    let _ = Mutator::<T>::gen_global_ids;
    let _ = Mutator::<T>::gen_masking_policy_id;
    let _ = Mutator::<T>::gen_placement_policy_id;
    let _ = Mutator::<T>::gen_schema_version;
    let _ = Mutator::<T>::gen_schema_versions;
    let _ = Mutator::<T>::global_id;
    let _ = Mutator::<T>::global_id_key;
    let _ = Mutator::<T>::history_ddl_count;
    let _ = Mutator::<T>::history_ddl_job::<J>;
    let _ = Mutator::<T>::history_ddl_jobs::<J>;
    let _ = Mutator::<T>::ingest_max_batch_split_ranges;
    let _ = Mutator::<T>::ingest_max_inflight;
    let _ = Mutator::<T>::ingest_max_per_sec;
    let _ = Mutator::<T>::ingest_max_split_ranges_per_sec;
    let _ = meta.iter_databases(|_| Ok(()));
    let _ = meta.iter_tables(0, |_| Ok(()));
    let _ = Mutator::<T>::last_history_ddl_jobs::<J>;
    let _ = Mutator::<T>::last_history_ddl_jobs_with_filter::<J>;
    let _ = Mutator::<T>::masking_policies;
    let _ = Mutator::<T>::masking_policy;
    let _ = Mutator::<T>::masking_policy_id;
    let _ = Mutator::<T>::metadata_lock;
    let _ = Mutator::<T>::metas_by_database_id;
    let _ = Mutator::<T>::new_with_options;
    let _ = Mutator::<T>::next_gen_boot_table_version;
    let _ = Mutator::<T>::policies;
    let _ = Mutator::<T>::policy;
    let _ = Mutator::<T>::policy_id;
    let _ = Mutator::<T>::resource_group::<G>;
    let _ = Mutator::<T>::resource_groups::<G>;
    let _ = Mutator::<T>::restart_sequence_value;
    let _ = Mutator::<T>::ru_stats;
    let _ = Mutator::<T>::schema_cache_size;
    let _ = Mutator::<T>::schema_diff;
    let _ = Mutator::<T>::schema_version;
    let _ = Mutator::<T>::schema_version_with_non_empty_diff;
    let _ = Mutator::<T>::set_bdr_role;
    let _ = Mutator::<T>::set_ddl_table_version;
    let _ = Mutator::<T>::set_dxf_schedule_tune_factors;
    let _ = Mutator::<T>::set_ingest_max_batch_split_ranges;
    let _ = Mutator::<T>::set_ingest_max_inflight;
    let _ = Mutator::<T>::set_ingest_max_per_sec;
    let _ = Mutator::<T>::set_ingest_max_split_ranges_per_sec;
    let _ = Mutator::<T>::set_metadata_lock;
    let _ = Mutator::<T>::set_next_gen_boot_table_version;
    let _ = Mutator::<T>::set_ru_stats;
    let _ = Mutator::<T>::set_schema_cache_size;
    let _ = Mutator::<T>::set_schema_diff;
    let _ = Mutator::<T>::simple_tables;
    let _ = Mutator::<T>::system_database_id;
    let _ = Mutator::<T>::table;
    let _ = Mutator::<T>::table_exists;
    let _ = Mutator::<T>::table_info_with_attributes;
    let _ = meta.tables_with_cancel(0, || false);
    let _ = Mutator::<T>::update_database;
    let _ = Mutator::<T>::update_masking_policy;
    let _ = Mutator::<T>::update_policy;
    let _ = Mutator::<T>::update_resource_group::<G>;
    let _ = Mutator::<T>::update_table;
    let _ = super::NAME_EXTRACT_REGEXP;
    let _ = super::NextGenBootTableVersion::BASE;
    let _ = super::NextGenBootTableVersion::INIT;
    let _ = super::NextGenBootTableVersion::MASKING_POLICY;
    let _ = &super::POLICY_ID_MUTEX;
    let _ = super::TABLE_INFO_MUST_LOAD_FILTERS;
    let _ = super::TABLE_NAME_INFO_FIELDS;
    let _ = super::default_resource_group_for_test::<G>;
    let _ = super::extract_schema_and_table_name_from_job;
    let _ = super::fast_unmarshal_table_name_info;
    let _ = super::iter_all_tables::<AnchorStore, fn() -> bool, fn(&TableInfo) -> Result<()>>;
    let _ = super::job_matches;
    let mut reader = AnchorMvccReader;
    let _ = super::oldest_schema_version(&mut reader);
    let _ = super::split_range_int64_max;
    let _ = super::table_info_must_load;
    let _ = super::table_info_must_load_with_filters;
    let _ = super::unescape_name;
    let _ = crate::value::CURRENT_MAGIC_BYTE_VER;
    let _ = crate::value::MagicType::Json;
    let _ = crate::value::MagicType::Unknown;
    let _ = crate::value::attach_magic_byte;
    let _ = crate::value::detach_magic_byte;
    let _ = crate::value::encode_int_value;
    let _ = crate::value::which_magic_type;
}
