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
use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
use tidb_codec::{gen_table_record_prefix, Encoder};
use tidb_datatype::{Collation, Datum, FieldTypeCode, MysqlEnum, Time, TimeType};
use tidb_meta::{key, value};
use tidb_metadef::system::SYSTEM_DATABASE_ID;
use tidb_model::column::ColumnDefaultValue;
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::{
    encode_table_row, generate_index_key, generate_index_value, IndexColumn as CodecIndexColumn,
    IndexInfo as CodecIndexInfo, TableColumn as CodecTableColumn, TableInfo as CodecTableInfo,
};
use tidb_txnkv::transaction::OptimisticMutation;
use tidb_txnkv::{Handle, IntHandle};

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

/// Go `ast.CurrentTimestamp`, as a column default stores it.
const CURRENT_TIMESTAMP: &str = "CURRENT_TIMESTAMP";

/// Go's own `Y`/`N` privilege spelling.
const YES: &str = "Y";
const NO: &str = "N";

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
    let handle = Handle::Int(IntHandle::new(row_id));

    // A seed row names only the columns it cares about, but the stored row
    // carries *every* column, because that is what an `INSERT` — the statement
    // Go bootstraps with — produces: each unnamed column is materialised from
    // its declared default, `CURRENT_TIMESTAMP` included. Leaving a column out
    // instead is what a real TiDB trips on, either as TiKV's "missing data for
    // NOT NULL column" or as a zero time it refuses to convert.
    let mut column_ids = Vec::with_capacity(table.columns.len());
    let mut values = Vec::with_capacity(table.columns.len());
    for column in table.cols() {
        let named = row
            .values
            .iter()
            .find(|seed| seed.column == column.name.lowercase());
        let value = match named {
            Some(seed) => typed_value(&seed.value, column.get_type()),
            None => declared_default(column, row.table, current_timestamp)?,
        };
        column_ids.push(column.id);
        values.push(value);
    }
    for seed in &row.values {
        if !table
            .columns
            .iter()
            .any(|column| column.name.lowercase() == seed.column)
        {
            return Err(BootstrapError::Encode(format!(
                "mysql.{} has no column `{}` to seed",
                row.table, seed.column
            )));
        }
    }
    let encoded = encode_table_row(None, &values, &column_ids, true, None)
        .map_err(|error| BootstrapError::Encode(error.to_string()))?;
    mutations.push(OptimisticMutation::insert(
        encode_row_key_with_handle(table.id, &RecordHandle::Int(row_id)),
        encoded,
    )?);

    let codec_table = codec_table_info(table);
    for (position, index) in table.indices.iter().enumerate() {
        let codec_index = &codec_table.indices[position];
        let mut indexed = Vec::with_capacity(index.columns.len());
        for index_column in &index.columns {
            let position =
                usize::try_from(index_column.offset).expect("a column offset is not negative");
            let column = table.columns.get(position).ok_or_else(|| {
                BootstrapError::Encode(format!(
                    "mysql.{}'s index `{}` names an offset the table does not have",
                    row.table,
                    index.name.original()
                ))
            })?;
            let value = row
                .values
                .iter()
                .find(|seed| seed.column == column.name.lowercase())
                .map_or(Datum::Null, |seed| {
                    typed_value(&seed.value, column.get_type())
                });
            indexed.push(value);
        }
        let (index_key, distinct) = generate_index_key(
            Encoder::new(true),
            None,
            &codec_table,
            codec_index,
            table.id,
            &mut indexed,
            Some(&handle),
        )
        .map_err(|error| BootstrapError::Encode(error.to_string()))?;
        let index_value = generate_index_value(
            true,
            None,
            &codec_table,
            codec_index,
            false,
            distinct,
            false,
            &indexed,
            &handle,
            0,
            &[],
        )
        .map_err(|error| BootstrapError::Encode(error.to_string()))?;
        mutations.push(OptimisticMutation::index_put(index_key, index_value)?);
    }
    Ok(())
}

/// Re-types a seed value for the column it lands in.
///
/// Every seed value is written as text here, because that is how Go's own
/// `INSERT` spells it; a column whose declared type is `ENUM` needs it as the
/// member it names, not as a string, or the row decodes as the wrong type.
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

/// The datum one column's declared `DEFAULT` materialises to.
///
/// This is what an `INSERT` stores for a column the statement does not name: a
/// literal default as itself, `CURRENT_TIMESTAMP` as this bootstrap's own
/// timestamp, and no default at all as `NULL`. An expression default is
/// refused, because evaluating one is not this bootstrap's job and silently
/// storing its unevaluated text writes a row a real TiDB rejects.
fn declared_default(
    column: &tidb_model::column::ColumnInfo,
    table: &str,
    current_timestamp: Time,
) -> Result<Datum, BootstrapError> {
    let refuse = || {
        BootstrapError::Encode(format!(
            "mysql.{table}.{} declares a default this bootstrap cannot materialise",
            column.name.original()
        ))
    };
    if column.default_is_expr {
        return Err(refuse());
    }
    // A `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` column stores that very word as
    // its default: an `INSERT` evaluates it, so a bootstrap that stores the
    // word instead writes a row TiDB rejects as an `Incorrect time value`.
    if let Some(ColumnDefaultValue::Str(bytes)) = column.default_value.as_ref() {
        if String::from_utf8_lossy(bytes).eq_ignore_ascii_case(CURRENT_TIMESTAMP) {
            return Ok(Datum::new_time(current_timestamp));
        }
    }
    let Some(default) = column.default_value.as_ref() else {
        // No declared default: an `INSERT` stores NULL, and the column is
        // nullable or the schema would not have parsed.
        return Ok(Datum::Null);
    };
    let datum = match default {
        ColumnDefaultValue::Int(value) => Datum::Int(*value),
        ColumnDefaultValue::Uint(value) => Datum::UInt(*value),
        ColumnDefaultValue::Bool(value) => Datum::Int(i64::from(*value)),
        ColumnDefaultValue::Float(value) => Datum::Real(*value),
        ColumnDefaultValue::Str(bytes) => {
            let text = Datum::Bytes(bytes.clone());
            // A numeric column's default is stored as its printed form, so it
            // has to be read back as a number before it is encoded as one.
            match column.get_type() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year => {
                    let printed = String::from_utf8_lossy(bytes);
                    let parsed = printed.trim().parse::<i64>().map_err(|_| refuse())?;
                    if column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED)
                    {
                        Datum::UInt(u64::try_from(parsed).map_err(|_| refuse())?)
                    } else {
                        Datum::Int(parsed)
                    }
                }
                code => typed_value(&text, code),
            }
        }
    };
    Ok(datum)
}

fn typed_value(value: &Datum, code: FieldTypeCode) -> Datum {
    match (value, code) {
        (Datum::Bytes(bytes), FieldTypeCode::Enum) => {
            // Go's `mysql.user` privilege enums are `ENUM('N','Y')`, so `N` is
            // member 1 and `Y` is member 2.
            let name = String::from_utf8_lossy(bytes).into_owned();
            let position = if name.eq_ignore_ascii_case(YES) { 2 } else { 1 };
            Datum::new_enum(MysqlEnum::new(name, position), Collation::Binary)
        }
        _ => value.clone(),
    }
}

/// The tablecodec view of one stored `TableInfo`.
///
/// `tidb-tablecodec` keeps its own minimal metadata shape so it does not depend
/// on the full catalog model; the index encoders need this projection of it.
fn codec_table_info(table: &TableInfo) -> CodecTableInfo {
    CodecTableInfo {
        columns: table
            .columns
            .iter()
            .enumerate()
            .map(|(offset, column)| CodecTableColumn {
                id: column.id,
                offset,
                field_type: column.field_type.clone(),
                primary_key: column
                    .field_type
                    .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
                changing_field_type: None,
            })
            .collect(),
        indices: table
            .indices
            .iter()
            .map(|index| CodecIndexInfo {
                id: index.id,
                columns: index
                    .columns
                    .iter()
                    .map(|column| CodecIndexColumn {
                        offset: usize::try_from(column.offset)
                            .expect("a column offset is not negative"),
                        length: i64::from(column.length),
                        use_changing_type: false,
                    })
                    .collect(),
                unique: index.unique,
                global: index.global,
                global_index_version: 0,
                primary: index.primary,
            })
            .collect(),
        pk_is_handle: table.pk_is_handle,
        is_common_handle: table.is_common_handle,
        common_handle_version: u8::try_from(table.common_handle_version).unwrap_or(0),
    }
}
