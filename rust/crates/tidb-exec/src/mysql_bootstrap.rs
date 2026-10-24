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
//! `pkg/session/bootstrap.go`'s `bootstrapSessionImpl` (`bootstrapSchemas`,
//! `InitDDLTables`), `doDDLWorks` and `doDMLWorks`, as ONE transaction's worth
//! of meta and row mutations.
//!
//! The layout follows the kernel type, as in Go ([`BootstrapLayout`]):
//!
//! * **Classic.** `mysql` takes the first global id, the DDL-owned tables sit
//!   at their reserved ids, and every other object is made by an ordinary DDL
//!   job: `sys`, the 53 `mysql.*` tables, the two views, `test` and the
//!   resource-group change. Ids, schema versions (one per job) and
//!   `tidb_ddl_history` rows are the DDL engine's own.
//! * **Next-gen.** `mysql`, `sys` and every `mysql.*` table sit at the
//!   reserved ids of [`tidb_metadef`], written straight into meta without a
//!   schema version; only the views, `test` and the resource-group change are
//!   DDL jobs.
//!
//! Both are planned by [`jobs`], which runs those jobs through the persisted
//! job planners against an overlay of the bootstrap's own writes. Two
//! deliberate differences from Go remain, stated rather than hidden:
//!
//! * **One transaction.** Go commits each job on its own; this node is the
//!   only writer, so the whole bootstrap and its seed rows land in one
//!   transaction. The committed bytes are the same.
//! * **New row format.** Go's bootstrap DML runs before
//!   `tidb_row_format_version` takes effect, so a real cluster's seed rows are
//!   in the OLD (v1) format. This writes v2, which every reader — Go's own and
//!   [`crate::mysql_system_tables`] — already handles, because there is no
//!   reason to reproduce an accident of Go's startup ordering.
//!
//! **Idempotence is refusal, not merge.** If the `mysql` database is already
//! present (found by name: no id is reserved for it under the classic kernel),
//! [`plan_mysql_bootstrap`] fails and writes nothing: deciding what to do with
//! a half-bootstrapped or already-bootstrapped cluster is
//! [`crate::cluster_privilege_load::read_bootstrap_state`]'s job, and it reads
//! exactly the `mysql.tidb` marker this writes.

use std::fmt;

use tidb_ast::{CiString, DdlStmt, Stmt};
use tidb_codec::Encoder;
use tidb_datatype::{new_collation_enabled, Datum};
use tidb_meta::{key, value};
use tidb_metadef::BootstrapTable;
use tidb_model::schema_diff::SchemaDiff;
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::{generate_index_key, TableInfo as CodecTableInfo};
use tidb_txnkv::transaction::{BufferMutation, MutationSetError};

use crate::cluster_catalog::{ClusterCatalogError, MetaSnapshot};
use crate::mysql_system_tables::SYSTEM_DB;
use crate::system_row_write::codec_table_info;
use crate::table_info_build::{
    build_table_info, ClusteredIndexDefMode, DdlAdmissionError,
};

mod jobs;
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

/// Reads one stored `mysql.<name>` table by name at one snapshot, the way Go's
/// restricted SQL resolves a system table through the info schema: the layout
/// a bootstrap chose (reserved ids under the next-gen kernel, ordinary ones
/// under the classic kernel) is not the reader's business. `None` when the
/// snapshot holds no such table.
pub fn find_system_table<S: MetaSnapshot>(
    snapshot: &mut S,
    table_name: &str,
) -> Result<Option<TableInfo>, BootstrapError> {
    for (raw_key, stored) in snapshot.scan_prefix(&key::databases_kv_prefix())? {
        let (_, field) = tidb_meta::structure::decode_hash_data_key(&raw_key)
            .map_err(|error| BootstrapError::Encode(format!("DBs hash key: {error}")))?;
        if !key::has_prefix(key::DB_PREFIX, &field) {
            continue;
        }
        let database = value::parse_db_info(&stored)
            .map_err(|error| BootstrapError::Encode(format!("DBInfo: {error}")))?;
        if database.name.lowercase() != SYSTEM_DB {
            continue;
        }
        return Ok(
            crate::cluster_catalog::load_database_tables(snapshot, database.id)?
                .into_iter()
                .find(|table| table.name.lowercase() == table_name),
        );
    }
    Ok(None)
}

/// What encoding the key Go's `GET_LOCK` locks takes, located by name in the
/// schema (Go inserts through the info schema's `mysql.advisory_locks`,
/// whatever ids the bootstrap gave it and whether its primary key is a
/// clustered common handle, whose key is the record's, or not, whose key is
/// the unique primary index entry's).
#[derive(Clone, Debug)]
pub struct AdvisoryLockTable {
    info: TableInfo,
    codec_info: CodecTableInfo,
    primary_position: usize,
    table_id: i64,
}

impl AdvisoryLockTable {
    /// Locates `mysql.advisory_locks` at one snapshot.
    pub fn locate<S: MetaSnapshot>(snapshot: &mut S) -> Result<Self, BootstrapError> {
        let info = find_system_table(snapshot, "advisory_locks")?.ok_or_else(|| {
            BootstrapError::Encode("mysql.advisory_locks does not exist".to_owned())
        })?;
        let primary_position = info
            .indices
            .iter_deref()
            .position(|index| index.read().primary)
            .ok_or_else(|| {
                BootstrapError::Encode("advisory_locks primary index is absent".to_owned())
            })?;
        Ok(Self {
            codec_info: codec_table_info(&info),
            primary_position,
            table_id: info.id,
            info,
        })
    }

    /// Encodes the unique-primary-index key Go's INSERT into
    /// `mysql.advisory_locks` pessimistically locks.
    pub fn key(&self, lock_name: &str) -> Result<Vec<u8>, BootstrapError> {
        if self.info.is_common_handle {
            // A clustered primary key owns no index entry: the insert locks
            // the record key.
            let column = self
                .info
                .find_public_column_by_name("lock_name")
                .ok_or_else(|| {
                    BootstrapError::Encode("advisory_locks has no lock_name column".to_owned())
                })?;
            let mut values = crate::system_row_write::RowValues::new();
            values.insert(column.read().id, Datum::new_string(lock_name));
            return crate::system_row_write::clustered_record_key(&self.info, &values)
                .map_err(encode_error);
        }
        let primary = &self.codec_info.indices[self.primary_position];
        let mut values = vec![Datum::new_string(lock_name)];
        let (key, distinct) = generate_index_key(
            Encoder::new(new_collation_enabled()),
            None,
            &self.codec_info,
            primary,
            self.table_id,
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
    /// A bootstrap DDL statement could not run as a DDL job.
    Job {
        /// The statement's first line.
        statement: String,
        /// Why the job planners refused it.
        detail: String,
    },
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
            Self::Job { statement, detail } => {
                write!(formatter, "bootstrap statement `{statement}`: {detail}")
            }
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

/// The region splits a bootstrap owes its new tables once they are committed
/// (Go's `splitAndScatterTable` in `InitDDLTables`, and the pre-split of every
/// `CREATE TABLE`). The ids are the ones this bootstrap actually gave its
/// tables, whichever layout it chose.
#[derive(Clone, Debug, Default)]
pub struct BootstrapSplits {
    /// Tables Go writes straight into meta and cuts in one split per batch
    /// (`createAndSplitTables`, no scatter): the next-gen layout's `mysql.*`
    /// tables, then one batch per DDL-table version group this bootstrap
    /// created, in version order.
    pub global_batches: Vec<Vec<i64>>,
    /// The tables a `CREATE TABLE` job created, in creation order; each cuts
    /// its own record range (`preSplitAndScatterTable`).
    pub table_ids: Vec<i64>,
}

/// Everything one bootstrap publishes, in a deterministic order.
#[derive(Clone, Debug)]
pub struct BootstrapWrite {
    /// The region splits owed once the mutations commit.
    pub splits: BootstrapSplits,
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
    plan_mysql_bootstrap_with_layout(
        snapshot,
        start_ts,
        environment,
        BootstrapLayout::of_kernel(),
    )
}

/// Which layout a bootstrap gives the `mysql` schema: Go chooses it from the
/// kernel type the binary was built for (`kerneltype.IsNextGen()`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapLayout {
    /// Go `bootstrapSessionImpl` without `bootstrapSchemas`: `mysql` and the
    /// tables its DDL jobs create take ordinary global ids, every statement is
    /// a job with its own schema version and history row.
    Classic,
    /// Go's `bootstrapSchemas`: `mysql` and its tables at the reserved ids of
    /// `tidb_metadef`, written directly into meta.
    NextGen,
}

impl BootstrapLayout {
    /// The layout of this binary's kernel type.
    #[must_use]
    pub const fn of_kernel() -> Self {
        if tidb_config::kerneltype::is_next_gen() {
            Self::NextGen
        } else {
            Self::Classic
        }
    }
}

/// [`plan_mysql_bootstrap`] under an explicit layout.
pub fn plan_mysql_bootstrap_with_layout<S: MetaSnapshot>(
    snapshot: &mut S,
    start_ts: u64,
    environment: &BootstrapEnvironment,
    layout: BootstrapLayout,
) -> Result<BootstrapWrite, BootstrapError> {
    match layout {
        BootstrapLayout::Classic | BootstrapLayout::NextGen => {
            jobs::plan(snapshot, start_ts, environment, layout)
        }
    }
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
    /// The database the view lives in, by name.
    schema: &'static str,
    /// The statement Go executes, which is the job's query text.
    create_sql: &'static str,
    name: &'static str,
    columns: &'static [&'static str],
    /// `ViewInfo.Security`: 1 is `SQL SECURITY INVOKER`.
    security: i64,
    select_stmt: &'static str,
}

const BOOTSTRAP_VIEWS: &[BootstrapView] = &[
    BootstrapView {
        schema: SYSTEM_DB,
        create_sql: tidb_metadef::system_tables_def::CREATE_TI_DBMDLVIEW,
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
        schema: SYS_DB,
        create_sql: tidb_metadef::system_tables_def::CREATE_SCHEMA_UNUSED_INDEXES_VIEW,
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
    clustered_index_mode: ClusteredIndexDefMode,
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
    let mut info = build_table_info(
        create,
        SYSTEM_DB_CHARSET,
        SYSTEM_DB_COLLATION,
        clustered_index_mode,
    )
    .map_err(admission)?;
    info.id = table.id;
    info.update_ts = start_ts;
    Ok(info)
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

    /// The advisory-lock key is built from the table the schema holds under
    /// that name, not from an id the bootstrap is assumed to have chosen.
    #[test]
    fn advisory_lock_key_is_the_stored_table_primary_index_key() {
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
        assert!(AdvisoryLockTable::locate(&mut snapshot).is_err());
        let write = plan_mysql_bootstrap(&mut snapshot, 7, &environment).unwrap();
        for mutation in &write.mutations {
            snapshot
                .pairs
                .insert(mutation.key().to_vec(), mutation.value().to_vec());
        }
        let info = find_system_table(&mut snapshot, "advisory_locks")
            .unwrap()
            .expect("the bootstrap stored mysql.advisory_locks");
        let primary_id = info
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                index.primary.then_some(index.id)
            })
            .expect("advisory-lock primary index");
        let prefix = tidb_codec::table_key::encode_index_seek_key(info.id, primary_id, &[]);
        let table = AdvisoryLockTable::locate(&mut snapshot).unwrap();
        let key = table.key("rootlock").unwrap();

        assert!(key.starts_with(&prefix));
        assert_ne!(key, table.key("another-lock").unwrap());
    }
}
