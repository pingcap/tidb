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

//! Go `doDMLWorks`' seed rows: the `root` account and the `mysql.tidb` markers
//! a later start reads back to know the cluster is bootstrapped.
//!
//! Every seeded table here is *non-clustered* — `pk_is_handle` and
//! `is_common_handle` are both false — so each row needs three things, and this
//! module writes all three or the row is invisible to half of TiDB:
//!
//! 1. the record itself, under an implicit `_tidb_rowid` handle;
//! 2. one entry per index, since nothing else backfills them;
//! 3. the row-ID allocator key, so the next writer does not hand out a handle
//!    this bootstrap already used.

use chrono::{Datelike, Timelike};
use tidb_codec::gen_table_record_prefix;
use tidb_datatype::{Datum, Time, TimeType};
use tidb_meta::{key, value};
use tidb_metadef::system::SYSTEM_DATABASE_ID;
use tidb_model::table_info::TableInfo;
use tidb_txnkv::transaction::OptimisticMutation;

use crate::system_row_write::{defaults_row, insert_row, RowEncodeError, NO, YES};

use super::{
    text, BootstrapError, BOOTSTRAPPED_VAR, CLUSTER_ID_VAR, DDL_TABLE_VERSION_VAR,
    NEW_COLLATION_ENABLED_VAR, SYSTEM_TZ_VAR, TIDB_SERVER_VERSION_VAR, VAR_FALSE, VAR_TRUE,
};

/// Go `currentBootstrapVersion` as this node writes it.
///
/// A real TiDB compares the stored value against its own build's constant and
/// runs the upgrade steps between them, so this must be the same number Go's
/// upgrade registry ends at — there is only one such number, and
/// [`crate::upgrade_versions`] already owns it. A second copy here could only
/// ever be a stale one, and a stale one makes a real TiDB run upgrade steps
/// over a schema that was already created at the current version.
pub use crate::upgrade_versions::CURRENT_BOOTSTRAP_VERSION;

/// One value of a seed row, named by its column so a schema that renames or
/// reorders columns fails loudly instead of writing into the wrong one.
#[derive(Clone, Debug)]
pub struct SeedValue {
    /// The column name, lowercase.
    pub column: &'static str,
    /// The value, in the column's own declared type.
    pub value: Datum,
}

/// One row to seed into one `mysql.*` table.
#[derive(Clone, Debug)]
pub struct SeedRow {
    /// The table name, without the `mysql.` qualifier.
    pub table: &'static str,
    /// The values, by column name. A column not named here is left unset,
    /// which reads back as its declared default.
    pub values: Vec<SeedValue>,
}

/// The facts about *this* cluster and host that Go's `doDMLWorks` reads out of
/// its environment rather than out of any constant.
///
/// They are inputs, not defaults: a real TiDB reads `system_tz` and
/// `new_collation_enabled` back on every start and refuses to run without them,
/// so a bootstrap that cannot state them is a bootstrap that produces a cluster
/// no TiDB can open.
#[derive(Clone, Debug)]
pub struct BootstrapEnvironment {
    /// Go `timeutil.InferSystemTZ()`: the host's own zone name.
    pub system_tz: String,
    /// Go `config.NewCollationsEnabledOnFirstBootstrap`, frozen for the life of
    /// the cluster by this very row.
    pub new_collation_enabled: bool,
    /// The PD cluster ID this keyspace belongs to.
    pub cluster_id: u64,
    /// What `CURRENT_TIMESTAMP` evaluates to for this bootstrap, in UTC.
    ///
    /// Go's seed rows get theirs from the `INSERT` that writes them; this one
    /// is stated so a bootstrap plan stays a pure function of its inputs.
    pub current_timestamp: Time,
    /// Go `Mutator.GetDDLTableVersion` as it stands *before* any TiDB has
    /// created its DDL tables — this bootstrap creates none of them, so the
    /// row states what the meta key says rather than what Go's own bootstrap
    /// would have left behind.
    pub ddl_table_version: i64,
}

/// Writes every seed row Go's `doDMLWorks` writes, with its index entries.
pub fn seed(
    tables: &[TableInfo],
    environment: &BootstrapEnvironment,
    mutations: &mut Vec<OptimisticMutation>,
) -> Result<(), BootstrapError> {
    let rows = seed_rows(environment)?;
    for row in &rows {
        let table = tables
            .iter()
            .find(|table| table.name.lowercase() == row.table)
            .ok_or_else(|| {
                BootstrapError::Encode(format!("mysql.{} was not created", row.table))
            })?;
        write_row(table, row, environment.current_timestamp, mutations)?;
    }
    // Every seeded table hands out `_tidb_rowid`s from 1, so the allocator has
    // to record the last one used before another writer asks for the next.
    for table in tables {
        let used = rows
            .iter()
            .filter(|row| row.table == table.name.lowercase())
            .count();
        if used == 0 {
            continue;
        }
        mutations.push(OptimisticMutation::meta_put(
            key::auto_table_id_kv_key(SYSTEM_DATABASE_ID, table.id),
            value::encode_int_value(i64::try_from(used).expect("a seed row count fits in i64")),
        )?);
    }
    Ok(())
}

/// Byte ground truth for Go `doDMLWorks`' `mysql.global_variables` seeding,
/// captured from a real bootstrap rather than re-derived from the sysvar
/// registry's `Scope`/`Value` fields (which are not ported to Rust).
///
/// Generator: `pkg/session` `TestZZDumpGlobalVariables` (a throwaway,
/// `-tags=intest` test, deleted after the capture) ran
/// `CreateStoreAndBootstrap` and dumped
/// `SELECT VARIABLE_NAME, VARIABLE_VALUE FROM mysql.global_variables ORDER BY
/// VARIABLE_NAME` as tab-separated lines. That query already is Go's own
/// `HasGlobalScope` filter plus `GlobalSystemVariableInitialValue` override
/// applied, so this file needs no Scope metadata to reproduce Go's seeded set
/// byte-for-byte — only to be re-captured if the sysvar registry changes.
const GLOBAL_VARIABLES_FIXTURE: &str = include_str!("global_variables_fixture.tsv");

/// Parses [`GLOBAL_VARIABLES_FIXTURE`] into one seed row per global variable.
///
/// A blank line is skipped (there are none in the captured fixture, but an
/// empty file must not panic); a line without a tab is a fixture that no
/// longer matches its own doc comment, so parsing refuses rather than
/// silently dropping a variable Go would have seeded.
fn global_variable_rows() -> Result<Vec<SeedRow>, BootstrapError> {
    GLOBAL_VARIABLES_FIXTURE
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once('\t').ok_or_else(|| {
                BootstrapError::Encode(format!(
                    "global_variables fixture line `{line}` is not NAME\\tVALUE"
                ))
            })?;
            Ok(SeedRow {
                table: "global_variables",
                values: vec![
                    SeedValue {
                        column: "variable_name",
                        value: Datum::Bytes(name.as_bytes().to_vec()),
                    },
                    SeedValue {
                        column: "variable_value",
                        value: Datum::Bytes(value.as_bytes().to_vec()),
                    },
                ],
            })
        })
        .collect()
}

/// Go `doDMLWorks`, as data.
fn seed_rows(environment: &BootstrapEnvironment) -> Result<Vec<SeedRow>, BootstrapError> {
    // The non-secure bootstrap account: `root`@`%` with every static privilege
    // except `Account_locked`, an EMPTY password, and the native-password
    // plugin. An empty `authentication_string` is what makes a fresh cluster
    // reachable with no password at all, which is exactly Go's behaviour.
    let mut root = vec![
        SeedValue {
            column: "host",
            value: text("%"),
        },
        SeedValue {
            column: "user",
            value: text("root"),
        },
        SeedValue {
            column: "authentication_string",
            value: text(""),
        },
        SeedValue {
            column: "plugin",
            value: text("mysql_native_password"),
        },
        SeedValue {
            column: "token_issuer",
            value: text(""),
        },
    ];
    for column in GRANTED_PRIVILEGE_COLUMNS {
        root.push(SeedValue {
            column,
            value: text(YES),
        });
    }
    root.push(SeedValue {
        column: "account_locked",
        value: text(NO),
    });

    let mut rows = vec![
        SeedRow {
            table: "user",
            values: root,
        },
        tidb_variable(BOOTSTRAPPED_VAR, VAR_TRUE, "Bootstrap flag. Do not delete."),
        tidb_variable(
            TIDB_SERVER_VERSION_VAR,
            &CURRENT_BOOTSTRAP_VERSION.to_string(),
            "Bootstrap version. Do not delete.",
        ),
        tidb_variable(
            SYSTEM_TZ_VAR,
            &environment.system_tz,
            "TiDB Global System Timezone.",
        ),
        tidb_variable(
            NEW_COLLATION_ENABLED_VAR,
            if environment.new_collation_enabled {
                VAR_TRUE
            } else {
                VAR_FALSE
            },
            "If the new collations are enabled. Do not edit it.",
        ),
        tidb_variable(
            DDL_TABLE_VERSION_VAR,
            &environment.ddl_table_version.to_string(),
            "DDL Table Version. Do not delete.",
        ),
        tidb_variable(
            CLUSTER_ID_VAR,
            &environment.cluster_id.to_string(),
            "TiDB Cluster ID.",
        ),
    ];
    rows.extend(global_variable_rows()?);
    Ok(rows)
}

fn tidb_variable(name: &str, value: &str, comment: &str) -> SeedRow {
    SeedRow {
        table: "tidb",
        values: vec![
            SeedValue {
                column: "variable_name",
                value: Datum::Bytes(name.as_bytes().to_vec()),
            },
            SeedValue {
                column: "variable_value",
                value: Datum::Bytes(value.as_bytes().to_vec()),
            },
            SeedValue {
                column: "comment",
                value: Datum::Bytes(comment.as_bytes().to_vec()),
            },
        ],
    }
}

/// Every `mysql.user` privilege column Go's non-secure bootstrap sets to `Y`,
/// in the order its own `INSERT` names them.
const GRANTED_PRIVILEGE_COLUMNS: &[&str] = &[
    "select_priv",
    "insert_priv",
    "update_priv",
    "delete_priv",
    "create_priv",
    "drop_priv",
    "process_priv",
    "grant_priv",
    "references_priv",
    "alter_priv",
    "show_db_priv",
    "super_priv",
    "create_tmp_table_priv",
    "lock_tables_priv",
    "execute_priv",
    "create_view_priv",
    "show_view_priv",
    "create_routine_priv",
    "alter_routine_priv",
    "index_priv",
    "create_user_priv",
    "event_priv",
    "repl_slave_priv",
    "repl_client_priv",
    "trigger_priv",
    "create_role_priv",
    "drop_role_priv",
    "shutdown_priv",
    "reload_priv",
    "file_priv",
    "config_priv",
    "create_tablespace_priv",
];

/// Writes one row's record and every index entry that covers it.
/// Writes one row's record and every index entry that covers it.
fn write_row(
    table: &TableInfo,
    row: &SeedRow,
    current_timestamp: Time,
    mutations: &mut Vec<OptimisticMutation>,
) -> Result<(), BootstrapError> {
    // Each seeded table's rows are numbered from 1 in the order they appear.
    let row_id = i64::try_from(
        mutations
            .iter()
            .filter(|mutation| {
                mutation
                    .key()
                    .starts_with(&gen_table_record_prefix(table.id))
            })
            .count(),
    )
    .expect("a seed row count fits in i64")
        + 1;

    // A seed row names only the columns it cares about, but the stored row
    // carries *every* column, because that is what an `INSERT` -- the statement
    // Go bootstraps with -- produces: each unnamed column is materialised from
    // its declared default, `CURRENT_TIMESTAMP` included. Leaving a column out
    // instead is what a real TiDB trips on, either as TiKV's "missing data for
    // NOT NULL column" or as a zero time it refuses to convert.
    let mut values = defaults_row(table, current_timestamp).map_err(encode)?;
    for seed in &row.values {
        let column = table
            .columns
            .iter()
            .find(|column| column.name.lowercase() == seed.column)
            .ok_or_else(|| {
                BootstrapError::Encode(format!(
                    "mysql.{} has no column `{}` to seed",
                    row.table, seed.column
                ))
            })?;
        values.insert(column.id, seed.value.clone());
    }
    mutations.extend(insert_row(table, row_id, &values).map_err(encode)?);
    Ok(())
}

fn encode(error: RowEncodeError) -> BootstrapError {
    BootstrapError::Encode(error.to_string())
}

/// The UTC wall clock as one `TIMESTAMP` value, for
/// [`BootstrapEnvironment::current_timestamp`].
///
/// # Panics
///
/// Never in practice: the fields come from a calendar date that is by
/// construction in range.
#[must_use]
pub fn utc_now_timestamp() -> Time {
    let now = chrono::Utc::now();
    Time::from_date_checked(
        now.year(),
        i32::try_from(now.month()).expect("a month fits in i32"),
        i32::try_from(now.day()).expect("a day fits in i32"),
        i32::try_from(now.hour()).expect("an hour fits in i32"),
        i32::try_from(now.minute()).expect("a minute fits in i32"),
        i32::try_from(now.second()).expect("a second fits in i32"),
        0,
        TimeType::Timestamp,
        0,
    )
    .expect("the current UTC calendar date is a valid timestamp")
}
