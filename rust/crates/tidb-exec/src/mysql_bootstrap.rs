// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Writing the `mysql` schema a TiDB cluster boots on: Go
//! `pkg/session/bootstrap.go`'s `doDDLWorks` + `doDMLWorks`, as ONE
//! transaction's worth of meta and row mutations.
//!
//! Three deliberate differences from Go, each stated rather than hidden:
//!
//! * **No DDL jobs, no per-table schema version.** Go creates the 7 DDL-owned
//!   tables directly and creates the other 52 `mysql.*` tables through either
//!   bootstrap DDL or its next-gen direct path. This node is the only writer,
//!   so all 59 tables plus their seed rows land in one transaction at one new
//!   schema version, described by a single `ActionCreateTables` diff. Go's
//!   direct paths likewise build the same reserved-ID table metadata without
//!   enqueuing DDL jobs.
//! * **Reserved IDs, not allocated ones.** Every `mysql.*` table has a fixed ID
//!   above [`tidb_metadef::MAX_USER_GLOBAL_ID`] (see
//!   [`tidb_metadef::BOOTSTRAP_TABLES`]), so a bootstrap never touches
//!   `NextGlobalID` and can never collide with a user table.
//! * **New row format.** Go's bootstrap DML runs before
//!   `tidb_row_format_version` takes effect, so a real cluster's seed rows are
//!   in the OLD (v1) format. This writes v2, which every reader — Go's own and
//!   [`crate::mysql_system_tables`] — already handles, because there is no
//!   reason to reproduce an accident of Go's startup ordering.
//!
//! **Idempotence is refusal, not merge.** If any bootstrap object is already
//! present, [`plan_mysql_bootstrap`] fails and writes nothing: deciding what to
//! do with a half-bootstrapped or already-bootstrapped cluster is
//! [`crate::cluster_privilege_load::read_bootstrap_state`]'s job, and it reads
//! exactly the `mysql.tidb` marker this writes.

use std::fmt;
use std::sync::OnceLock;

use tidb_ast::{CiString, DdlStmt, Stmt};
use tidb_codec::Encoder;
use tidb_datatype::{new_collation_enabled, Datum};
use tidb_meta::{key, value};
use tidb_metadef::system::SYSTEM_DATABASE_ID;
use tidb_metadef::{BootstrapTable, BOOTSTRAP_TABLES, DDL_TABLE_VERSION_TABLES};
use tidb_model::action_type::ActionType;
use tidb_model::db::DBInfo;
use tidb_model::schema_diff::{AffectedOption, SchemaDiff};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::{generate_index_key, TableInfo as CodecTableInfo};
use tidb_txnkv::transaction::{BufferMutation, MutationSetError};

use crate::cluster_catalog::{ClusterCatalogError, MetaSnapshot};
use crate::mysql_system_tables::SYSTEM_DB;
use crate::system_row_write::codec_table_info;
use crate::table_info_build::{
    build_table_info, ClusteredIndexDefMode, DdlAdmissionError, GENERIC_ERROR_CODE,
};

mod rows;

pub use rows::{
    local_now_datetime, local_now_datetime6, utc_now_timestamp, BootstrapEnvironment, SeedRow,
    SeedValue,
};

/// Go `mysql.DefaultCharset` / the collation TiDB's own `mysql` schema carries.
const SYSTEM_DB_CHARSET: &str = "utf8mb4";
/// The collation paired with [`SYSTEM_DB_CHARSET`].
const SYSTEM_DB_COLLATION: &str = "utf8mb4_bin";

/// Go `bootstrappedVar`.
pub const BOOTSTRAPPED_VAR: &str = "bootstrapped";
/// Go `tidbServerVersionVar`.
pub const TIDB_SERVER_VERSION_VAR: &str = "tidb_server_version";
/// Go `tidbSystemTZ`.
pub const SYSTEM_TZ_VAR: &str = "system_tz";
/// Go `TidbNewCollationEnabled`.
pub const NEW_COLLATION_ENABLED_VAR: &str = "new_collation_enabled";
/// Go `tidbDDLTableVersion`.
pub const DDL_TABLE_VERSION_VAR: &str = "ddl_table_version";

type AdvisoryLockKeyInfo = (CodecTableInfo, usize, i64);

fn advisory_lock_key_info() -> Result<&'static AdvisoryLockKeyInfo, BootstrapError> {
    static INFO: OnceLock<Result<AdvisoryLockKeyInfo, String>> = OnceLock::new();
    INFO.get_or_init(|| {
        let table = BOOTSTRAP_TABLES
            .iter()
            .find(|table| table.id == tidb_metadef::system::ADVISORY_LOCKS_TABLE_ID)
            .ok_or_else(|| "advisory_locks metadata is absent".to_owned())?;
        let info = build_bootstrap_table(table, 0).map_err(|error| error.to_string())?;
        let primary_position = info
            .indices
            .iter_deref()
            .position(|index| index.read().primary)
            .ok_or_else(|| "advisory_locks primary index is absent".to_owned())?;
        Ok((codec_table_info(&info), primary_position, info.id))
    })
    .as_ref()
    .map_err(|error| BootstrapError::Encode(error.clone()))
}

/// Encodes the unique-primary-index key Go's INSERT into
/// `mysql.advisory_locks` pessimistically locks.
pub fn advisory_lock_key(lock_name: &str) -> Result<Vec<u8>, BootstrapError> {
    let (codec_info, primary_position, table_id) = advisory_lock_key_info()?;
    let primary = &codec_info.indices[*primary_position];
    let mut values = vec![Datum::new_string(lock_name)];
    let (key, distinct) = generate_index_key(
        Encoder::new(new_collation_enabled()),
        None,
        codec_info,
        primary,
        *table_id,
        &mut values,
        None,
    )
    .map_err(encode_error)?;
    if !distinct {
        return Err(BootstrapError::Encode(
            "advisory_locks primary key encoded as non-distinct".to_owned(),
        ));
    }
    Ok(key)
}
/// Go `tidbClusterID`.
pub const CLUSTER_ID_VAR: &str = "cluster_id";
/// Go `varTrue`.
pub const VAR_TRUE: &str = "True";
/// Go `varFalse`.
pub const VAR_FALSE: &str = "False";

/// Why a bootstrap cannot be planned.
#[derive(Clone, Debug)]
pub enum BootstrapError {
    /// The snapshot read itself failed.
    Snapshot(ClusterCatalogError),
    /// A bootstrap object is already present, so this is not a fresh cluster.
    AlreadyPresent {
        /// What was found, named precisely.
        object: String,
    },
    /// A `mysql.*` `CREATE TABLE` did not lower.
    Admission {
        /// The table whose statement was refused.
        table: &'static str,
        /// The refusal.
        error: DdlAdmissionError,
    },
    /// A catalog object or seed row could not be encoded.
    Encode(String),
    /// The mutation set was rejected before it could be published.
    Mutations(MutationSetError),
}

impl fmt::Display for BootstrapError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(error) => write!(formatter, "{error}"),
            Self::AlreadyPresent { object } => write!(
                formatter,
                "this cluster already carries {object}, so it is not a fresh keyspace to bootstrap"
            ),
            Self::Admission { table, error } => {
                write!(formatter, "mysql.{table} could not be built: {error}")
            }
            Self::Encode(detail) => write!(formatter, "bootstrap encode failed: {detail}"),
            Self::Mutations(error) => write!(formatter, "bootstrap mutations: {error}"),
        }
    }
}

impl std::error::Error for BootstrapError {}

impl From<ClusterCatalogError> for BootstrapError {
    fn from(error: ClusterCatalogError) -> Self {
        Self::Snapshot(error)
    }
}

impl From<MutationSetError> for BootstrapError {
    fn from(error: MutationSetError) -> Self {
        Self::Mutations(error)
    }
}

/// Everything one bootstrap publishes, in a deterministic order.
#[derive(Clone, Debug)]
pub struct BootstrapWrite {
    /// Every meta-key and row mutation.
    pub mutations: Vec<BufferMutation>,
    /// The schema version this bootstrap produces.
    pub schema_version: i64,
    /// The diff stored under `Diff:<schema_version>`.
    pub diff: SchemaDiff,
    /// The `mysql.*` tables created, in the order they were written.
    pub created_tables: Vec<TableInfo>,
    /// The resource-manager write of `doDDLWorks`' `ALTER RESOURCE GROUP
    /// default BACKGROUND=(TASK_TYPES='stats')` (`onAlterResourceGroup`'s
    /// `infosync.ModifyResourceGroup`), which the committing caller makes
    /// before the meta record it pairs with is published.
    pub resource_group_update: crate::cluster_ddl::ResourceGroupUpdate,
}

/// Plans the whole `mysql` schema, its seed rows, and the bootstrap marker.
///
/// Everything is read at the one snapshot and published in the one transaction
/// that owns it, so a bootstrap either lands whole or not at all. The
/// `SchemaVersionKey` write makes a competing writer — this node's or a real
/// TiDB's — a definite write conflict rather than an interleaved half-cluster.
pub fn plan_mysql_bootstrap<S: MetaSnapshot>(
    snapshot: &mut S,
    start_ts: u64,
    environment: &BootstrapEnvironment,
) -> Result<BootstrapWrite, BootstrapError> {
    refuse_if_present(snapshot)?;

    let mut mutations = Vec::new();
    // Go `bootstrap`'s first step, `InitMDLVariableForBootstrap`:
    // `Mutator.SetMetadataLock(true)` before any bootstrap DDL runs.
    mutations.push(BufferMutation::set(
        key::metadata_lock_kv_key(),
        b"1".to_vec(),
    )?);
    let database = DBInfo {
        id: SYSTEM_DATABASE_ID,
        name: CiString::new(SYSTEM_DB),
        charset: SYSTEM_DB_CHARSET.to_owned(),
        collate: SYSTEM_DB_COLLATION.to_owned(),
        state: SchemaState::PUBLIC,
        ..DBInfo::default()
    };
    mutations.push(BufferMutation::set(
        key::database_kv_key(SYSTEM_DATABASE_ID),
        value::serialize_db_info(&database).map_err(encode_error)?,
    )?);

    let ddl_tables = DDL_TABLE_VERSION_TABLES
        .iter()
        .filter(|version| environment.ddl_table_version < version.version)
        .flat_map(|version| version.tables.iter());
    let tables = ddl_tables.chain(BOOTSTRAP_TABLES.iter());
    let table_count = DDL_TABLE_VERSION_TABLES
        .iter()
        .filter(|version| environment.ddl_table_version < version.version)
        .map(|version| version.tables.len())
        .sum::<usize>()
        + BOOTSTRAP_TABLES.len();
    let mut created_tables = Vec::with_capacity(table_count);
    let mut affected = Vec::with_capacity(table_count);
    for table in tables {
        let info = build_bootstrap_table(table, start_ts)?;
        mutations.push(BufferMutation::set(
            key::table_kv_key(SYSTEM_DATABASE_ID, info.id),
            value::serialize_table_info(&info).map_err(encode_error)?,
        )?);
        affected.push(AffectedOption {
            schema_id: SYSTEM_DATABASE_ID,
            table_id: info.id,
            ..AffectedOption::default()
        });
        created_tables.push(info);
    }

    let ddl_table_version = DDL_TABLE_VERSION_TABLES
        .last()
        .map_or(environment.ddl_table_version, |version| {
            environment.ddl_table_version.max(version.version)
        });
    if ddl_table_version > environment.ddl_table_version {
        mutations.push(BufferMutation::set(
            key::ddl_table_version_kv_key(),
            value::encode_int_value(ddl_table_version),
        )?);
    }
    let seed_environment = BootstrapEnvironment {
        ddl_table_version,
        ..environment.clone()
    };
    rows::seed(&created_tables, &seed_environment, &mut mutations)?;

    // Go `doDDLWorks`' ordinary DDL, each object's id drawn from
    // `NextGlobalID` exactly as `cluster_ddl`'s allocator does, so the first
    // user object continues after them instead of colliding: the `sys`
    // database's views (`CreateTiDBMDLView`, `CreateSchemaUnusedIndexesView`)
    // and then `CREATE DATABASE IF NOT EXISTS test` (`bootstrap.go:418`).
    let mut allocate_global_id = |mutations: &mut Vec<BufferMutation>,
                                  snapshot: &mut S,
                                  allocated_before: &mut Option<i64>|
     -> Result<i64, BootstrapError> {
        let current = match allocated_before {
            Some(current) => *current,
            None => match snapshot.get(&key::next_global_id_kv_key())? {
                Some(stored) => value::parse_int_value(&stored)
                    .map_err(|error| BootstrapError::Encode(format!("NextGlobalID: {error}")))?,
                // Go's `Inc` treats a missing key as zero.
                None => 0,
            },
        };
        let allocated = current + 1;
        *allocated_before = Some(allocated);
        mutations.push(BufferMutation::set(
            key::next_global_id_kv_key(),
            value::encode_int_value(allocated),
        )?);
        Ok(allocated)
    };
    let mut allocated_before = None;
    let sys_database = DBInfo {
        id: tidb_metadef::system::SYS_DATABASE_ID,
        name: CiString::new(SYS_DB),
        charset: SYSTEM_DB_CHARSET.to_owned(),
        collate: SYSTEM_DB_COLLATION.to_owned(),
        state: SchemaState::PUBLIC,
        ..DBInfo::default()
    };
    mutations.push(BufferMutation::set(
        key::database_kv_key(sys_database.id),
        value::serialize_db_info(&sys_database).map_err(encode_error)?,
    )?);
    for view in BOOTSTRAP_VIEWS {
        let id = allocate_global_id(&mut mutations, snapshot, &mut allocated_before)?;
        let info = build_bootstrap_view(view, id, start_ts);
        mutations.push(BufferMutation::set(
            key::table_kv_key(view.database_id, info.id),
            value::serialize_table_info(&info).map_err(encode_error)?,
        )?);
        affected.push(AffectedOption {
            schema_id: view.database_id,
            table_id: info.id,
            ..AffectedOption::default()
        });
    }
    let test_db_id = allocate_global_id(&mut mutations, snapshot, &mut allocated_before)?;
    let test_database = DBInfo {
        id: test_db_id,
        name: CiString::new("test"),
        charset: SYSTEM_DB_CHARSET.to_owned(),
        collate: SYSTEM_DB_COLLATION.to_owned(),
        state: SchemaState::PUBLIC,
        ..DBInfo::default()
    };
    mutations.push(BufferMutation::set(
        key::database_kv_key(test_db_id),
        value::serialize_db_info(&test_database).map_err(encode_error)?,
    )?);

    // Go `doDDLWorks` ends with `ALTER RESOURCE GROUP default
    // BACKGROUND=(TASK_TYPES='stats')`, whose `onAlterResourceGroup` stores
    // the group with `UpdateResourceGroup` (the unpersisted default needs no
    // existence check) and tells the resource manager.
    let (default_group, resource_group_update) = bootstrap_default_resource_group()?;
    mutations.push(BufferMutation::set(
        key::resource_group_kv_key(default_group.id),
        serde_json::to_vec(&default_group)
            .map(|json| value::attach_magic_byte(&json))
            .map_err(encode_error)?,
    )?);

    // Go `InitTiDBSchemaCacheSize`: publish the source default only when no
    // persisted setting exists.
    if read_schema_cache_size(snapshot)?.is_none() {
        let schema_cache_size = u64::try_from(tidb_vardef::defaults::DEF_TIDB_SCHEMA_CACHE_SIZE)
            .expect("the source schema-cache default is non-negative");
        mutations.push(BufferMutation::set(
            schema_cache_size_kv_key(),
            schema_cache_size.to_string().as_bytes(),
        )?);
    }

    // The version bump comes last, so the write set ends with the two keys that
    // make the whole schema observable at once.
    let schema_version = read_schema_version(snapshot)? + 1;
    mutations.push(BufferMutation::set(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    let diff = SchemaDiff {
        version: schema_version,
        action_type: ActionType::ACTION_CREATE_TABLES,
        schema_id: SYSTEM_DATABASE_ID,
        affected_options: affected.into(),
        ..SchemaDiff::default()
    };
    mutations.push(BufferMutation::set(
        key::schema_diff_kv_key(schema_version),
        value::serialize_schema_diff(&diff).map_err(encode_error)?,
    )?);

    Ok(BootstrapWrite {
        mutations,
        schema_version,
        diff,
        created_tables,
        resource_group_update,
    })
}

/// The `default` group `doDDLWorks`' `ALTER RESOURCE GROUP default
/// BACKGROUND=(TASK_TYPES='stats')` (`kv.InternalTxnStats`) leaves behind.
///
/// Go `executor.AlterResourceGroup` reads the group from the info schema --
/// meta's unpersisted `defaultRGroupMeta` on a fresh store -- and
/// `buildResourceGroup` gives it a fresh `Background` whose `JobTypes` are
/// `["stats"]`, then `Adjust`s; `onAlterResourceGroup` keeps the stored
/// group's ID, name and state and replaces only its settings. The proto is
/// `resourcegroup.NewGroupFromOptions` over those settings, which is what
/// the job sends through `infosync.ModifyResourceGroup`.
fn bootstrap_default_resource_group() -> Result<
    (
        tidb_model::ResourceGroupInfo,
        crate::cluster_ddl::ResourceGroupUpdate,
    ),
    BootstrapError,
> {
    let mut group = tidb_model::ResourceGroupInfo::default_group();
    let settings = group
        .settings
        .get_or_insert_with(|| Box::new(tidb_model::ResourceGroupSettings::new()));
    settings.background = Some(tidb_model::ResourceGroupShared::new(
        tidb_model::ResourceGroupBackgroundSettings {
            job_types: Some(vec![tidb_txnkv::INTERNAL_TXN_STATS.to_owned()]),
            resource_util_limit: 0,
        },
    ));
    settings.adjust();
    let proto = tidb_ddl_resourcegroup::new_group_from_options(
        group.name.lowercase(),
        group.settings.as_deref(),
    )
    .map_err(encode_error)?;
    Ok((
        group,
        crate::cluster_ddl::ResourceGroupUpdate::Modify(Box::new(proto)),
    ))
}

/// Plans the whole `mysql` schema — the named entry point for a caller that
/// just wants a fresh keyspace bootstrapped.
///
/// This is [`plan_mysql_bootstrap`] under the name the bootstrap contract is
/// stated in; it plans the mutations but does not commit them, because only the
/// caller owns the transaction they belong to.
pub fn bootstrap_mysql_schema<S: MetaSnapshot>(
    snapshot: &mut S,
    start_ts: u64,
    environment: &BootstrapEnvironment,
) -> Result<BootstrapWrite, BootstrapError> {
    plan_mysql_bootstrap(snapshot, start_ts, environment)
}

/// Reads Go `Mutator.GetDDLTableVersion` at this snapshot.
///
/// A keyspace no TiDB has touched has no such key, and Go's own reader treats
/// that absence as `InitDDLTableVersion` — zero.
pub fn read_ddl_table_version<S: MetaSnapshot>(snapshot: &mut S) -> Result<i64, BootstrapError> {
    match snapshot.get(&key::ddl_table_version_kv_key())? {
        Some(stored) => value::parse_int_value(&stored)
            .map_err(|error| BootstrapError::Encode(format!("DDLTableVersion: {error}"))),
        None => Ok(0),
    }
}

/// Reads Go `Mutator.GetSchemaCacheSize` at this snapshot.
///
/// `None` is Go's `isNull=true` result when a keyspace bootstrap has not yet
/// initialized it.
pub fn read_schema_cache_size<S: MetaSnapshot>(
    snapshot: &mut S,
) -> Result<Option<u64>, BootstrapError> {
    match snapshot.get(&schema_cache_size_kv_key())? {
        Some(stored) => std::str::from_utf8(&stored)
            .map_err(|error| BootstrapError::Encode(format!("SchemaCacheSize: {error}")))?
            .parse::<u64>()
            .map(Some)
            .map_err(|error| BootstrapError::Encode(format!("SchemaCacheSize: {error}"))),
        None => Ok(None),
    }
}

fn schema_cache_size_kv_key() -> Vec<u8> {
    tidb_meta::structure::encode_string_data_key(key::SCHEMA_CACHE_SIZE)
}

/// One view Go's bootstrap session creates with `CREATE OR REPLACE VIEW`
/// (`doDDLWorks`: `metadef.CreateTiDBMDLView`, then
/// `metadef.CreateSchemaUnusedIndexesView`). A view stores no column types --
/// Go's `buildViewInfo` leaves every `ColumnInfo.FieldType` zero and the
/// planner derives them when the view is used -- so this is the whole
/// `TableInfo` the oracle holds, captured with its restored SELECT text.
struct BootstrapView {
    database_id: i64,
    name: &'static str,
    columns: &'static [&'static str],
    /// `ViewInfo.Security`: 1 is `SQL SECURITY INVOKER`.
    security: i64,
    select_stmt: &'static str,
}

const BOOTSTRAP_VIEWS: &[BootstrapView] = &[
    BootstrapView {
        database_id: SYSTEM_DATABASE_ID,
        name: "tidb_mdl_view",
        columns: &[
            "job_id",
            "db_name",
            "table_name",
            "query",
            "session_id",
            "start_time",
            "SQL_DIGESTS",
        ],
        security: 1,
        select_stmt: r#"(SELECT `tidb_mdl_info`.`job_id` AS `job_id`,JSON_UNQUOTE(JSON_EXTRACT(CAST(CAST(`job_meta` AS CHAR) AS JSON), _UTF8MB4'$.schema_name')) AS `db_name`,JSON_UNQUOTE(JSON_EXTRACT(CAST(CAST(`job_meta` AS CHAR) AS JSON), _UTF8MB4'$.table_name')) AS `table_name`,JSON_UNQUOTE(JSON_EXTRACT(CAST(CAST(`job_meta` AS CHAR) AS JSON), _UTF8MB4'$.query')) AS `query`,`session_id` AS `session_id`,`cluster_tidb_trx`.`start_time` AS `start_time`,TIDB_DECODE_SQL_DIGESTS(`all_sql_digests`, 4096) AS `SQL_DIGESTS` FROM ((`mysql`.`tidb_ddl_job`) JOIN `mysql`.`tidb_mdl_info`) JOIN `information_schema`.`cluster_tidb_trx` WHERE `tidb_ddl_job`.`job_id`=`tidb_mdl_info`.`job_id` AND CONCAT(_UTF8MB4',', `tidb_mdl_info`.`table_ids`, _UTF8MB4',') REGEXP CONCAT(_UTF8MB4',(', REPLACE(`cluster_tidb_trx`.`related_table_ids`, _UTF8MB4',', _UTF8MB4'|'), _UTF8MB4'),')!=0)"#,
    },
    BootstrapView {
        database_id: tidb_metadef::system::SYS_DATABASE_ID,
        name: "schema_unused_indexes",
        columns: &["object_schema", "object_name", "index_name"],
        security: 0,
        select_stmt: r#"SELECT `table_schema` AS `object_schema`,`table_name` AS `object_name`,`index_name` AS `index_name` FROM `information_schema`.`cluster_tidb_index_usage` WHERE `table_schema` NOT IN (_UTF8MB4'sys',_UTF8MB4'mysql',_UTF8MB4'INFORMATION_SCHEMA',_UTF8MB4'PERFORMANCE_SCHEMA') AND `index_name`!=_UTF8MB4'PRIMARY' GROUP BY `table_schema`,`table_name`,`index_name` HAVING SUM(`information_schema`.`CLUSTER_TIDB_INDEX_USAGE`.`LAST_ACCESS_TIME`) IS NULL"#,
    },
];

/// How many views the fresh bootstrap creates beside its tables; each is named
/// in the bootstrap's one schema diff.
pub const BOOTSTRAP_VIEW_COUNT: usize = BOOTSTRAP_VIEWS.len();

/// Go `bootstrapSchemas`' `sys` database (`SysDatabaseID`), which holds the
/// views above.
const SYS_DB: &str = "sys";

fn build_bootstrap_view(
    view: &BootstrapView,
    table_id: i64,
    start_ts: u64,
) -> TableInfo {
    use tidb_model::table::ViewInfo;
    let columns = view
        .columns
        .iter()
        .enumerate()
        .map(|(offset, name)| tidb_model::ColumnInfo {
            id: i64::try_from(offset).expect("a column offset fits in i64") + 1,
            name: CiString::new(*name),
            offset: i64::try_from(offset).expect("a column offset fits in i64"),
            state: SchemaState::PUBLIC,
            ..tidb_model::ColumnInfo::default()
        })
        .collect::<Vec<_>>();
    TableInfo {
        id: table_id,
        name: CiString::new(view.name),
        charset: SYSTEM_DB_CHARSET.to_owned(),
        collate: SYSTEM_DB_COLLATION.to_owned(),
        max_column_id: i64::try_from(view.columns.len()).expect("a column count fits in i64"),
        columns: columns.into(),
        state: SchemaState::PUBLIC,
        version: tidb_model::table_info::TABLE_INFO_VERSION5,
        update_ts: start_ts,
        view: Some(tidb_model::GoShared::new(ViewInfo {
            algorithm: tidb_ast::ViewAlgorithm(0),
            definer: Some(Box::new(tidb_parser::auth::UserIdentity {
                current_user: true,
                ..tidb_parser::auth::UserIdentity::default()
            })),
            security: tidb_ast::ViewSecurity(view.security),
            select_stmt: view.select_stmt.to_owned(),
            check_option: tidb_ast::ViewCheckOption(1),
            ..ViewInfo::default()
        })),
        ..TableInfo::default()
    }
}

/// Builds one `mysql.*` table at its reserved ID.
fn build_bootstrap_table(
    table: &BootstrapTable,
    start_ts: u64,
) -> Result<TableInfo, BootstrapError> {
    let admission = |error| BootstrapError::Admission {
        table: table.name,
        error,
    };
    let parsed = tidb_parser::parse(table.create_sql).map_err(|error| {
        admission(DdlAdmissionError::plain(
            format!("its CREATE TABLE does not parse: {error:?}"),
        ))
    })?;
    let Stmt::Ddl(ddl) = &parsed else {
        return Err(admission(DdlAdmissionError::plain(
            "its statement is not a DDL",
        )));
    };
    let DdlStmt::CreateTable(create) = ddl.as_ref() else {
        return Err(admission(DdlAdmissionError::plain(
            "its statement is not a CREATE TABLE",
        )));
    };
    // Go's classic bootstrap session sets `ClusteredIndexDefModeIntOnly`
    // explicitly "for the bootstrap SQLs", so this is the mode a real
    // cluster's own `mysql.*` was built under — not the server default.
    let mut info = build_table_info(
        create,
        SYSTEM_DB_CHARSET,
        SYSTEM_DB_COLLATION,
        ClusteredIndexDefMode::IntOnly,
    )
    .map_err(admission)?;
    info.id = table.id;
    info.update_ts = start_ts;
    Ok(info)
}

/// Refuses a cluster that already carries any bootstrap object.
///
/// The `mysql` database key and every reserved table key are checked, not just
/// the marker row: a cluster whose bootstrap died halfway must be refused too,
/// because re-running would write a second copy of a table that already exists.
fn refuse_if_present<S: MetaSnapshot>(snapshot: &mut S) -> Result<(), BootstrapError> {
    if snapshot
        .get(&key::database_kv_key(SYSTEM_DATABASE_ID))?
        .is_some()
    {
        return Err(BootstrapError::AlreadyPresent {
            object: format!("the `{SYSTEM_DB}` database"),
        });
    }
    for table in DDL_TABLE_VERSION_TABLES
        .iter()
        .flat_map(|version| version.tables)
        .chain(BOOTSTRAP_TABLES)
    {
        if snapshot
            .get(&key::table_kv_key(SYSTEM_DATABASE_ID, table.id))?
            .is_some()
        {
            return Err(BootstrapError::AlreadyPresent {
                object: format!("`{SYSTEM_DB}`.`{}`", table.name),
            });
        }
    }
    Ok(())
}

fn read_schema_version<S: MetaSnapshot>(snapshot: &mut S) -> Result<i64, BootstrapError> {
    match snapshot.get(&key::schema_version_kv_key())? {
        Some(stored) => value::parse_int_value(&stored)
            .map_err(|error| BootstrapError::Encode(format!("SchemaVersionKey: {error}"))),
        // Go's `Inc` treats a missing key as zero, which is what a keyspace no
        // TiDB ever touched looks like.
        None => Ok(0),
    }
}

fn encode_error(error: impl fmt::Display) -> BootstrapError {
    BootstrapError::Encode(error.to_string())
}

/// One `Datum` for a seed row's character column.
fn text(value: &str) -> Datum {
    Datum::Bytes(value.as_bytes().to_vec())
}

#[cfg(test)]
mod source_tests {
    use std::collections::BTreeMap;

    use tidb_datatype::{Time, TimeType};

    use super::*;
    use crate::cluster_catalog::MetaPairs;

    #[derive(Default)]
    struct Snapshot {
        pairs: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl MetaSnapshot for Snapshot {
        fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
            Ok(self.pairs.get(raw_key).cloned())
        }

        fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
            Ok(self
                .pairs
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    // Go pkg/session/tidb_test.go::TestSchemaCacheSizeVar.
    #[test]
    fn test_schema_cache_size_var() {
        let mut snapshot = Snapshot::default();
        assert_eq!(read_schema_cache_size(&mut snapshot).unwrap(), None);

        let environment = BootstrapEnvironment {
            system_tz: "Asia/Shanghai".to_owned(),
            new_collation_enabled: true,
            cluster_id: 1,
            current_timestamp: Time::from_date_checked(
                2026,
                8,
                10,
                0,
                0,
                0,
                0,
                TimeType::Timestamp,
                0,
            )
            .unwrap(),
            ddl_table_version: 0,
        };
        let write = plan_mysql_bootstrap(&mut snapshot, 1, &environment).unwrap();
        let raw_key = schema_cache_size_kv_key();
        let matching = write
            .mutations
            .iter()
            .filter(|mutation| mutation.key() == raw_key);
        let stored = matching
            .map(|mutation| mutation.value().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(stored, vec![b"536870912".to_vec()]);

        snapshot.pairs.insert(raw_key, stored[0].clone());
        assert_eq!(
            read_schema_cache_size(&mut snapshot).unwrap(),
            Some(536_870_912)
        );
    }

    /// Go `bootstrap` persists `InitMDLVariableForBootstrap`'s metadata-lock
    /// flag and `doDDLWorks`' `ALTER RESOURCE GROUP default
    /// BACKGROUND=(TASK_TYPES='stats')`: the meta record its
    /// `onAlterResourceGroup` stores and the group it sends to the resource
    /// manager. The JSON is Go's `json.Marshal` of that group.
    #[test]
    fn bootstrap_persists_default_group_background_and_metadata_lock() {
        const GO_DEFAULT_GROUP: &str = r#"{"ru_per_sec":2147483647,"priority":8,"cpu_limit":"","io_read_bandwidth":"","io_write_bandwidth":"","burst_limit":-1,"runaway":null,"background":{"job_types":["stats"],"utilization_limit":0},"id":1,"name":{"O":"default","L":"default"},"state":5}"#;
        let mut snapshot = Snapshot::default();
        let environment = BootstrapEnvironment {
            system_tz: "UTC".to_owned(),
            new_collation_enabled: true,
            cluster_id: 1,
            current_timestamp: Time::from_date_checked(
                2026,
                10,
                20,
                0,
                0,
                0,
                0,
                TimeType::Timestamp,
                0,
            )
            .unwrap(),
            ddl_table_version: 0,
        };
        let write = plan_mysql_bootstrap(&mut snapshot, 1, &environment).unwrap();
        let stored = |raw_key: Vec<u8>| {
            write
                .mutations
                .iter()
                .filter(|mutation| mutation.key() == raw_key)
                .map(|mutation| mutation.value().to_vec())
                .collect::<Vec<_>>()
        };

        assert_eq!(stored(key::metadata_lock_kv_key()), vec![b"1".to_vec()]);
        assert_eq!(
            stored(key::resource_group_kv_key(1)),
            vec![value::attach_magic_byte(GO_DEFAULT_GROUP.as_bytes())]
        );
        let crate::cluster_ddl::ResourceGroupUpdate::Modify(proto) = &write.resource_group_update
        else {
            panic!("the bootstrap modifies the resource manager's default group");
        };
        assert_eq!(proto.name, "default");
        assert_eq!(proto.priority, 8);
        assert_eq!(
            proto
                .background_settings
                .as_ref()
                .map(|background| background.job_types.clone()),
            Some(vec!["stats".to_owned()])
        );
        let fill_rate = proto
            .r_u_settings
            .as_ref()
            .and_then(|settings| settings.r_u.as_ref())
            .and_then(|bucket| bucket.settings.as_ref())
            .map(|settings| (settings.fill_rate, settings.burst_limit));
        assert_eq!(fill_rate, Some((2_147_483_647, -1)));
    }

    #[test]
    fn advisory_lock_key_is_the_reserved_table_primary_index_key() {
        let table = BOOTSTRAP_TABLES
            .iter()
            .find(|table| table.id == tidb_metadef::system::ADVISORY_LOCKS_TABLE_ID)
            .expect("advisory-lock bootstrap metadata");
        let info = build_bootstrap_table(table, 0).unwrap();
        let primary_id = info
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                index.primary.then_some(index.id)
            })
            .expect("advisory-lock primary index");
        let prefix = tidb_codec::table_key::encode_index_seek_key(info.id, primary_id, &[]);
        let key = advisory_lock_key("rootlock").unwrap();

        assert!(key.starts_with(&prefix));
        assert_ne!(key, advisory_lock_key("another-lock").unwrap());
    }
}
