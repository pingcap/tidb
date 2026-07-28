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

//! `pkg/meta/meta.go`: the catalog's key names, laid over [`crate::structure`].
//!
//! ```text
//! NextGlobalID     -> int64
//! SchemaVersionKey -> int64
//! DBs              -> { DB:<dbID>    -> DBInfo JSON }
//! DB:<dbID>        -> { Table:<tblID> -> TableInfo JSON
//!                       TID:<tblID>   -> int64 (row-ID allocator)
//!                       IID:<tblID>   -> int64 (auto_increment allocator)
//!                       TARID:<tblID> -> int64 (auto_random allocator)
//!                       SID:<seqID>   -> int64 (sequence value)
//!                       SequenceCycle:<seqID> -> int64 }
//! ```

use crate::error::{MetaError, Result};
use crate::structure::{encode_hash_data_key, encode_hash_data_key_prefix, encode_string_data_key};

/// Go `mNextGlobalIDKey`. Holds the max *used* ID, not the next one.
pub const NEXT_GLOBAL_ID: &[u8] = b"NextGlobalID";
/// Go `mSchemaVersionKey`.
pub const SCHEMA_VERSION: &[u8] = b"SchemaVersionKey";
/// Go `mDBs`: the hash holding every `DBInfo`.
pub const DBS: &[u8] = b"DBs";
/// Go `mBootstrapKey`.
pub const BOOTSTRAP: &[u8] = b"BootstrapKey";
/// Go `mPolicies`.
pub const POLICIES: &[u8] = b"Policies";
/// Go `mMaskingPolicies`.
pub const MASKING_POLICIES: &[u8] = b"MaskingPolicies";
/// Go `mResourceGroups`.
pub const RESOURCE_GROUPS: &[u8] = b"ResourceGroups";
/// Go `mPolicyGlobalID`.
pub const POLICY_GLOBAL_ID: &[u8] = b"PolicyGlobalID";
/// Go `mMaskingPolicyGlobalID`.
pub const MASKING_POLICY_GLOBAL_ID: &[u8] = b"MaskingPolicyGlobalID";
/// Go `mDDLTableVersion`.
pub const DDL_TABLE_VERSION: &[u8] = b"DDLTableVersion";
/// Go `mBootTableVersion`.
pub const BOOT_TABLE_VERSION: &[u8] = b"BootTableVersion";
/// Go `mBDRRole`.
pub const BDR_ROLE: &[u8] = b"BDRRole";
/// Go `mMetaDataLock`.
pub const METADATA_LOCK: &[u8] = b"metadataLock";
/// Go `mSchemaCacheSize`.
pub const SCHEMA_CACHE_SIZE: &[u8] = b"SchemaCacheSize";
/// Go `mRequestUnitStats`.
pub const REQUEST_UNIT_STATS: &[u8] = b"RequestUnitStats";

/// Go `mDBPrefix`.
pub const DB_PREFIX: &str = "DB";
/// Go `mTablePrefix`.
pub const TABLE_PREFIX: &str = "Table";
/// Go `mSequencePrefix`.
pub const SEQUENCE_PREFIX: &str = "SID";
/// Go `mSeqCyclePrefix`.
pub const SEQUENCE_CYCLE_PREFIX: &str = "SequenceCycle";
/// Go `mTableIDPrefix`: the `_tidb_rowid` allocator.
pub const AUTO_TABLE_ID_PREFIX: &str = "TID";
/// Go `mIncIDPrefix`: the `auto_increment` allocator.
pub const AUTO_INCREMENT_ID_PREFIX: &str = "IID";
/// Go `mRandomIDPrefix`: the `auto_random` allocator.
pub const AUTO_RANDOM_ID_PREFIX: &str = "TARID";
/// Go `mSchemaDiffPrefix`.
pub const SCHEMA_DIFF_PREFIX: &str = "Diff";
/// Go `mPolicyPrefix`.
pub const POLICY_PREFIX: &str = "Policy";
/// Go `mMaskingPolicyPrefix`.
pub const MASKING_POLICY_PREFIX: &str = "MaskingPolicy";
/// Go `mResourceGroupPrefix`.
pub const RESOURCE_GROUP_PREFIX: &str = "RG";

/// Go `fmt.Appendf(nil, "%s:%d", prefix, id)`: every meta hash field and every
/// ID-carrying meta key name shares this one shape.
#[must_use]
pub fn prefixed_id(prefix: &str, id: i64) -> Vec<u8> {
    format!("{prefix}:{id}").into_bytes()
}

/// Inverse of [`prefixed_id`]. Go's `Parse*Key` family.
pub fn parse_prefixed_id(prefix: &str, field: &[u8]) -> Result<i64> {
    let text = std::str::from_utf8(field).map_err(|_| MetaError::InvalidFieldKey)?;
    text.strip_prefix(prefix)
        .and_then(|rest| rest.strip_prefix(':'))
        .ok_or(MetaError::InvalidFieldKey)?
        .parse()
        .map_err(|_| MetaError::InvalidFieldKey)
}

/// Go `IsDBkey` and friends: whether `field` carries this prefix.
#[must_use]
pub fn has_prefix(prefix: &str, field: &[u8]) -> bool {
    field.starts_with(prefix.as_bytes()) && field.get(prefix.len()) == Some(&b':')
}

/// Go `DBkey`.
#[must_use]
pub fn db_key(db_id: i64) -> Vec<u8> {
    prefixed_id(DB_PREFIX, db_id)
}

/// Go `ParseDBKey`.
pub fn parse_db_key(field: &[u8]) -> Result<i64> {
    parse_prefixed_id(DB_PREFIX, field)
}

/// Go `TableKey`.
#[must_use]
pub fn table_key(table_id: i64) -> Vec<u8> {
    prefixed_id(TABLE_PREFIX, table_id)
}

/// Go `ParseTableKey`.
pub fn parse_table_key(field: &[u8]) -> Result<i64> {
    parse_prefixed_id(TABLE_PREFIX, field)
}

/// Go `AutoTableIDKey`.
#[must_use]
pub fn auto_table_id_key(table_id: i64) -> Vec<u8> {
    prefixed_id(AUTO_TABLE_ID_PREFIX, table_id)
}

/// Go `AutoIncrementIDKey`.
#[must_use]
pub fn auto_increment_id_key(table_id: i64) -> Vec<u8> {
    prefixed_id(AUTO_INCREMENT_ID_PREFIX, table_id)
}

/// Go `AutoRandomTableIDKey`.
#[must_use]
pub fn auto_random_table_id_key(table_id: i64) -> Vec<u8> {
    prefixed_id(AUTO_RANDOM_ID_PREFIX, table_id)
}

/// Go `SequenceKey`.
#[must_use]
pub fn sequence_key(sequence_id: i64) -> Vec<u8> {
    prefixed_id(SEQUENCE_PREFIX, sequence_id)
}

/// Go `Mutator.sequenceCycleKey`.
#[must_use]
pub fn sequence_cycle_key(sequence_id: i64) -> Vec<u8> {
    prefixed_id(SEQUENCE_CYCLE_PREFIX, sequence_id)
}

/// Go `Mutator.schemaDiffKey`.
#[must_use]
pub fn schema_diff_key(schema_version: i64) -> Vec<u8> {
    prefixed_id(SCHEMA_DIFF_PREFIX, schema_version)
}

/// Go `Mutator.policyKey`.
#[must_use]
pub fn policy_key(policy_id: i64) -> Vec<u8> {
    prefixed_id(POLICY_PREFIX, policy_id)
}

/// Go `Mutator.maskingPolicyKey`.
#[must_use]
pub fn masking_policy_key(policy_id: i64) -> Vec<u8> {
    prefixed_id(MASKING_POLICY_PREFIX, policy_id)
}

/// Go `Mutator.resourceGroupKey`.
#[must_use]
pub fn resource_group_key(group_id: i64) -> Vec<u8> {
    prefixed_id(RESOURCE_GROUP_PREFIX, group_id)
}

/// The raw KV key holding `NextGlobalID`. Go `Mutator.GlobalIDKey`.
#[must_use]
pub fn next_global_id_kv_key() -> Vec<u8> {
    encode_string_data_key(NEXT_GLOBAL_ID)
}

/// The raw KV key holding the global schema version.
#[must_use]
pub fn schema_version_kv_key() -> Vec<u8> {
    encode_string_data_key(SCHEMA_VERSION)
}

/// The raw KV key holding the bootstrap version. Go `Mutator.GetBootstrapVersion`.
#[must_use]
pub fn bootstrap_kv_key() -> Vec<u8> {
    encode_string_data_key(BOOTSTRAP)
}

/// The raw KV key holding one schema diff. Go `Mutator.EncodeSchemaDiffKey`.
#[must_use]
pub fn schema_diff_kv_key(schema_version: i64) -> Vec<u8> {
    encode_string_data_key(&schema_diff_key(schema_version))
}

/// The raw KV key holding one database's `DBInfo`. Go `Mutator.GetDatabase`.
#[must_use]
pub fn database_kv_key(db_id: i64) -> Vec<u8> {
    encode_hash_data_key(DBS, &db_key(db_id))
}

/// The scan prefix covering every database. Go `Mutator.ListDatabases`.
#[must_use]
pub fn databases_kv_prefix() -> Vec<u8> {
    encode_hash_data_key_prefix(DBS)
}

/// The raw KV key holding one table's `TableInfo`. Go `Mutator.GetTable`.
#[must_use]
pub fn table_kv_key(db_id: i64, table_id: i64) -> Vec<u8> {
    encode_hash_data_key(&db_key(db_id), &table_key(table_id))
}

/// The scan prefix covering one database's whole hash: table metadata *and*
/// its allocator fields. Go `Mutator.GetMetasByDBID`, which filters by field
/// prefix afterwards.
#[must_use]
pub fn database_metas_kv_prefix(db_id: i64) -> Vec<u8> {
    encode_hash_data_key_prefix(&db_key(db_id))
}

/// The raw KV key holding one table's `_tidb_rowid` allocator value.
#[must_use]
pub fn auto_table_id_kv_key(db_id: i64, table_id: i64) -> Vec<u8> {
    encode_hash_data_key(&db_key(db_id), &auto_table_id_key(table_id))
}

/// The raw KV key holding one table's `auto_increment` allocator value.
#[must_use]
pub fn auto_increment_id_kv_key(db_id: i64, table_id: i64) -> Vec<u8> {
    encode_hash_data_key(&db_key(db_id), &auto_increment_id_key(table_id))
}

/// The raw KV key holding one table's `auto_random` allocator value.
#[must_use]
pub fn auto_random_table_id_kv_key(db_id: i64, table_id: i64) -> Vec<u8> {
    encode_hash_data_key(&db_key(db_id), &auto_random_table_id_key(table_id))
}

/// The raw KV key holding one placement policy. Go `Mutator.GetPolicy`.
#[must_use]
pub fn policy_kv_key(policy_id: i64) -> Vec<u8> {
    encode_hash_data_key(POLICIES, &policy_key(policy_id))
}

/// The scan prefix covering every placement policy.
#[must_use]
pub fn policies_kv_prefix() -> Vec<u8> {
    encode_hash_data_key_prefix(POLICIES)
}

/// The raw KV key holding one resource group.
#[must_use]
pub fn resource_group_kv_key(group_id: i64) -> Vec<u8> {
    encode_hash_data_key(RESOURCE_GROUPS, &resource_group_key(group_id))
}

/// The scan prefix covering every resource group.
#[must_use]
pub fn resource_groups_kv_prefix() -> Vec<u8> {
    encode_hash_data_key_prefix(RESOURCE_GROUPS)
}
