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
//! * **No DDL jobs, no per-table schema version.** Go creates each `mysql.*`
//!   table through the DDL job queue and spends a schema version on each. This
//!   node is the only writer, so all 52 tables plus their seed rows land in one
//!   transaction at one new schema version, described by a single
//!   `ActionCreateTables` diff. Go's own next-gen path does the same thing
//!   (`session.go` `createAndSplitTables` writes `CreateTableOrView` directly,
//!   with no job at all), so this is a shape a real TiDB already produces.
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

use tidb_ast::{CiString, DdlStmt, Stmt};
use tidb_datatype::Datum;
use tidb_meta::{key, value};
use tidb_metadef::system::SYSTEM_DATABASE_ID;
use tidb_metadef::{BootstrapTable, BOOTSTRAP_TABLES};
use tidb_model::action_type::ActionType;
use tidb_model::db::DBInfo;
use tidb_model::schema_diff::{AffectedOption, SchemaDiff};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::{MutationSetError, OptimisticMutation};

use crate::cluster_catalog::{ClusterCatalogError, MetaSnapshot};
use crate::mysql_system_tables::SYSTEM_DB;
use crate::table_info_build::{build_table_info, ClusteredIndexDefMode, DdlAdmissionError};

mod rows;

pub use rows::{SeedRow, SeedValue};

/// Go `mysql.DefaultCharset` / the collation TiDB's own `mysql` schema carries.
const SYSTEM_DB_CHARSET: &str = "utf8mb4";
/// The collation paired with [`SYSTEM_DB_CHARSET`].
const SYSTEM_DB_COLLATION: &str = "utf8mb4_bin";

/// Go `bootstrappedVar`.
pub const BOOTSTRAPPED_VAR: &str = "bootstrapped";
/// Go `tidbServerVersionVar`.
pub const TIDB_SERVER_VERSION_VAR: &str = "tidb_server_version";
/// Go `varTrue`.
pub const VAR_TRUE: &str = "True";

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
    pub mutations: Vec<OptimisticMutation>,
    /// The schema version this bootstrap produces.
    pub schema_version: i64,
    /// The diff stored under `Diff:<schema_version>`.
    pub diff: SchemaDiff,
    /// The `mysql.*` tables created, in the order they were written.
    pub created_tables: Vec<TableInfo>,
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
) -> Result<BootstrapWrite, BootstrapError> {
    refuse_if_present(snapshot)?;

    let mut mutations = Vec::new();
    let database = DBInfo {
        id: SYSTEM_DATABASE_ID,
        name: CiString::new(SYSTEM_DB),
        charset: SYSTEM_DB_CHARSET.to_owned(),
        collate: SYSTEM_DB_COLLATION.to_owned(),
        state: SchemaState::PUBLIC,
        ..DBInfo::default()
    };
    mutations.push(OptimisticMutation::meta_put(
        key::database_kv_key(SYSTEM_DATABASE_ID),
        value::serialize_db_info(&database).map_err(encode_error)?,
    )?);

    let mut created_tables = Vec::with_capacity(BOOTSTRAP_TABLES.len());
    let mut affected = Vec::with_capacity(BOOTSTRAP_TABLES.len());
    for table in BOOTSTRAP_TABLES {
        let info = build_bootstrap_table(table, start_ts)?;
        mutations.push(OptimisticMutation::meta_put(
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

    rows::seed(&created_tables, &mut mutations)?;

    // The version bump comes last, so the write set ends with the two keys that
    // make the whole schema observable at once.
    let schema_version = read_schema_version(snapshot)? + 1;
    mutations.push(OptimisticMutation::meta_put(
        key::schema_version_kv_key(),
        value::encode_int_value(schema_version),
    )?);
    let diff = SchemaDiff {
        version: schema_version,
        action_type: ActionType::ACTION_CREATE_TABLES,
        schema_id: SYSTEM_DATABASE_ID,
        affected_options: affected,
        ..SchemaDiff::default()
    };
    mutations.push(OptimisticMutation::meta_put(
        key::schema_diff_kv_key(schema_version),
        value::serialize_schema_diff(&diff).map_err(encode_error)?,
    )?);

    Ok(BootstrapWrite {
        mutations,
        schema_version,
        diff,
        created_tables,
    })
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
) -> Result<BootstrapWrite, BootstrapError> {
    plan_mysql_bootstrap(snapshot, start_ts)
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
        admission(DdlAdmissionError::new(format!(
            "its CREATE TABLE does not parse: {error:?}"
        )))
    })?;
    let Stmt::Ddl(ddl) = &parsed else {
        return Err(admission(DdlAdmissionError::new(
            "its statement is not a DDL",
        )));
    };
    let DdlStmt::CreateTable(create) = ddl.as_ref() else {
        return Err(admission(DdlAdmissionError::new(
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
    for table in BOOTSTRAP_TABLES {
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
