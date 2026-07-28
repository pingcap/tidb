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

//! Reading `mysql.*` rows a Go TiDB wrote.
//!
//! The bounded read path ([`crate::cluster_catalog::configure_loaded_table`])
//! serves only tables whose handle is a signed `BIGINT` primary key. Every
//! `mysql.*` table in scope here fails that test — Go's bootstrap creates them
//! with a *non-clustered* `PRIMARY KEY (Host, User)`, verified against a live
//! v8.5.7 cluster's stored `TableInfo`: `pk_is_handle=false`,
//! `is_common_handle=false`. That is exactly what makes them readable this
//! way: with no clustered handle, every declared column — including `Host` and
//! `User` — is stored in the row *value*, so the record range plus the row
//! codec is the whole story and no handle needs decoding.
//!
//! Two properties of a real cluster's `mysql.*` shape this module:
//!
//! * **Both row formats occur.** A live v8.5.7 playground stores `mysql.tidb`
//!   in the *old* (v1) row format and later-written rows in v2, because
//!   bootstrap DML runs before `tidb_row_format_version` is in effect. Rather
//!   than branch on the version, this reads every row through
//!   [`tidb_tablecodec::decode_table_row_to_map`], which is Go
//!   `tablecodec.DecodeRowWithMap` and already owns both formats.
//! * **Column identity is the cluster's, not this node's.** A column is
//!   located by name in the stored `TableInfo` and decoded at exactly the type
//!   that `TableInfo` declares, so `ENUM`/`SET` labels come from the cluster's
//!   own `Elems` and a schema this node does not recognize is refused by name
//!   rather than silently misread.
//!
//! Only the columns a caller names are decoded. That is what keeps a table
//! like `mysql.user` — which also carries `JSON`, `TIMESTAMP`, and unsigned
//! integer columns no caller here reads — from ever needing a "type this
//! reader does not support" branch.

use std::collections::BTreeMap;
use std::fmt;

use tidb_codec::gen_table_record_prefix;
use tidb_datatype::{Datum, FieldType};
use tidb_model::schema_state::SchemaState;
use tidb_model::table_info::TableInfo;
use tidb_tablecodec::decode_table_row_to_map;

use crate::cluster_catalog::{ClusterCatalog, ClusterCatalogError, MetaSnapshot};

/// Go `mysql.SystemDB`: the schema every table in this module lives in.
pub const SYSTEM_DB: &str = "mysql";

/// Why a `mysql.*` row could not be read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemTableError {
    /// The schema or table is not in the loaded catalog at all.
    Missing {
        /// Fully qualified `mysql.<table>` name.
        name: String,
    },
    /// The table exists but does not carry a column this reader needs.
    MissingColumn {
        /// Fully qualified `mysql.<table>` name.
        name: String,
        /// Column name as this reader spells it.
        column: String,
    },
    /// The column exists but decoded to a value shape this reader cannot use.
    UnexpectedColumnValue {
        /// Fully qualified `mysql.<table>` name.
        name: String,
        /// Column name as this reader spells it.
        column: String,
        /// What the reader wanted.
        wanted: &'static str,
        /// What the row actually held.
        stored: String,
    },
    /// A stored row value did not decode.
    Decode {
        /// Fully qualified `mysql.<table>` name.
        name: String,
        /// Self-contained explanation.
        detail: String,
    },
    /// The snapshot read itself failed.
    Snapshot(String),
}

impl fmt::Display for SystemTableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { name } => {
                write!(formatter, "the cluster catalog has no table `{name}`")
            }
            Self::MissingColumn { name, column } => {
                write!(formatter, "`{name}` has no column `{column}`")
            }
            Self::UnexpectedColumnValue {
                name,
                column,
                wanted,
                stored,
            } => write!(
                formatter,
                "`{name}`.`{column}` holds {stored}, which this reader cannot read as {wanted}"
            ),
            Self::Decode { name, detail } => {
                write!(formatter, "a `{name}` row did not decode: {detail}")
            }
            Self::Snapshot(detail) => {
                write!(formatter, "system-table snapshot read failed: {detail}")
            }
        }
    }
}

impl std::error::Error for SystemTableError {}

impl From<ClusterCatalogError> for SystemTableError {
    fn from(error: ClusterCatalogError) -> Self {
        Self::Snapshot(error.to_string())
    }
}

/// One `mysql.*` table located in a loaded catalog, projected to the columns
/// one caller reads.
///
/// A named column the stored table does not carry is simply absent from the
/// projection rather than an error: the `mysql.user` privilege columns grew
/// over TiDB versions, so an older cluster legitimately lacks some of them,
/// and [`SystemRow::has_column`] is how a caller tells that apart from a
/// column that is present and set to `N`.
#[derive(Clone, Debug)]
pub struct SystemTableView {
    name: String,
    table_id: i64,
    /// Projected column name -> stored column ID.
    ids: BTreeMap<String, i64>,
    /// Stored column ID -> declared type, the shape the row decoder takes.
    types: BTreeMap<i64, FieldType>,
}

impl SystemTableView {
    /// Locates one `mysql.<table>` in a loaded catalog and projects it to the
    /// named columns.
    ///
    /// Only public columns are projected: a column mid-DDL is not one whose
    /// stored values this node may interpret.
    pub fn locate(
        catalog: &ClusterCatalog,
        table: &str,
        columns: &[&str],
    ) -> Result<Self, SystemTableError> {
        let qualified = format!("{SYSTEM_DB}.{table}");
        let missing = || SystemTableError::Missing {
            name: qualified.clone(),
        };
        let database = catalog
            .databases
            .iter()
            .find(|database| database.info.name.lowercase() == SYSTEM_DB)
            .ok_or_else(missing)?;
        let info = database
            .tables
            .iter()
            .find(|stored| stored.name.lowercase() == table)
            .ok_or_else(missing)?;
        Ok(Self::project(&qualified, info, columns))
    }

    /// Projects one already-located `TableInfo` under the given display name.
    #[must_use]
    pub fn project(name: &str, table: &TableInfo, columns: &[&str]) -> Self {
        let mut ids = BTreeMap::new();
        let mut types = BTreeMap::new();
        for column in table.cols() {
            if column.state != SchemaState::PUBLIC {
                continue;
            }
            let lowercase = column.name.lowercase();
            if !columns.contains(&lowercase) {
                continue;
            }
            ids.insert(lowercase.to_owned(), column.id);
            types.insert(column.id, column.field_type.clone());
        }
        Self {
            name: name.to_owned(),
            table_id: table.id,
            ids,
            types,
        }
    }

    /// The fully qualified `mysql.<table>` name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The stored table ID whose record range holds these rows.
    #[must_use]
    pub const fn table_id(&self) -> i64 {
        self.table_id
    }

    /// Whether the stored table carries a projected column of this name.
    #[must_use]
    pub fn has_column(&self, column: &str) -> bool {
        self.ids.contains_key(column)
    }

    fn column_id(&self, column: &str) -> Result<i64, SystemTableError> {
        self.ids
            .get(column)
            .copied()
            .ok_or_else(|| SystemTableError::MissingColumn {
                name: self.name.clone(),
                column: column.to_owned(),
            })
    }
}

/// Reads every row in one `mysql.*` table's record range, in key order.
///
/// The returned values are the raw stored row bytes; the record key carries no
/// column data for these tables (see the module doc) and is therefore dropped.
pub fn scan_system_table<S: MetaSnapshot>(
    snapshot: &mut S,
    view: &SystemTableView,
) -> Result<Vec<Vec<u8>>, SystemTableError> {
    let prefix = gen_table_record_prefix(view.table_id);
    let pairs = snapshot.scan_prefix(&prefix)?;
    Ok(pairs.into_iter().map(|(_, value)| value).collect())
}

/// One stored row, decoded to the projected columns.
pub struct SystemRow<'view> {
    view: &'view SystemTableView,
    values: BTreeMap<i64, Datum>,
}

impl<'view> SystemRow<'view> {
    /// Decodes one stored row value's projected columns.
    ///
    /// The timezone is deliberately `None`: no projected column is a
    /// `TIMESTAMP`, so there is no value whose meaning a session timezone
    /// could change.
    pub fn parse(view: &'view SystemTableView, value: &[u8]) -> Result<Self, SystemTableError> {
        let values = decode_table_row_to_map(value, &view.types, None).map_err(|error| {
            SystemTableError::Decode {
                name: view.name.clone(),
                detail: error.to_string(),
            }
        })?;
        Ok(Self { view, values })
    }

    /// Whether the stored table carries a projected column of this name.
    #[must_use]
    pub fn has_column(&self, column: &str) -> bool {
        self.view.has_column(column)
    }

    fn value(&self, column: &str) -> Result<Option<&Datum>, SystemTableError> {
        let id = self.view.column_id(column)?;
        // A row that does not carry the column is indistinguishable from one
        // that stores SQL NULL there, and every caller here treats both as the
        // column's declared default.
        Ok(self
            .values
            .get(&id)
            .filter(|datum| !matches!(datum, Datum::Null)))
    }

    fn wrong_value(&self, column: &str, wanted: &'static str, stored: &Datum) -> SystemTableError {
        SystemTableError::UnexpectedColumnValue {
            name: self.view.name.clone(),
            column: column.to_owned(),
            wanted,
            stored: format!("{stored:?}"),
        }
    }

    /// Reads a character column as UTF-8 text.
    pub fn text(&self, column: &str) -> Result<Option<String>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        let bytes = match datum {
            Datum::String(string) => string.bytes(),
            Datum::Bytes(bytes) => bytes.as_slice(),
            other => return Err(self.wrong_value(column, "text", other)),
        };
        String::from_utf8(bytes.to_vec())
            .map(Some)
            .map_err(|error| SystemTableError::Decode {
                name: self.view.name.clone(),
                detail: error.to_string(),
            })
    }

    /// Reads an `ENUM` column as its stored label.
    pub fn enum_label(&self, column: &str) -> Result<Option<String>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Enum(value, _) => Ok(Some(value.name().to_owned())),
            other => Err(self.wrong_value(column, "ENUM", other)),
        }
    }

    /// Whether an `ENUM('N','Y')` privilege column holds `Y`.
    ///
    /// Every `mysql.user`/`mysql.db` privilege column is declared
    /// `ENUM('N','Y') NOT NULL DEFAULT 'N'`, so a value-less column is `N` —
    /// the same answer Go's `SELECT` produces from the declared default.
    pub fn is_yes(&self, column: &str) -> Result<bool, SystemTableError> {
        Ok(self.enum_label(column)?.as_deref() == Some("Y"))
    }

    /// Reads a `SET` column as the labels its stored bitmask selects.
    ///
    /// Go renders a `SET` value as its comma-separated element names
    /// (`types.ParseSetValue`), and an empty selection as the empty string —
    /// which is no labels, not one empty label.
    pub fn set_labels(&self, column: &str) -> Result<Vec<String>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(Vec::new());
        };
        match datum {
            Datum::Set(value, _) if value.name().is_empty() => Ok(Vec::new()),
            Datum::Set(value, _) => Ok(value.name().split(',').map(str::to_owned).collect()),
            other => Err(self.wrong_value(column, "SET", other)),
        }
    }
}

#[cfg(test)]
mod tests {
    use tidb_ast::CiString;
    use tidb_datatype::FieldTypeCode;
    use tidb_model::column::ColumnInfo;

    use super::*;

    /// One `mysql.user` row exactly as a v8.5.7 Go TiDB wrote it, captured
    /// from a live playground's record range. It is the `root`@`%` row Go's
    /// `doDMLWorks` seeds: every privilege `Y`, `Account_locked` `N`, and an
    /// empty `authentication_string`.
    ///
    /// Note the leading `8`, a varint column-ID flag: bootstrap DML runs
    /// before `tidb_row_format_version` takes effect, so a real cluster's
    /// `mysql.*` is in the OLD row format even on a current TiDB. That is
    /// precisely the case a v2-only decoder gets wrong.
    const ROOT_ROW: &[u8] = &[
        8, 2, 2, 2, 37, 8, 4, 2, 8, 114, 111, 111, 116, 8, 6, 2, 0, 8, 8, 2, 42, 109, 121, 115,
        113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 8,
        10, 9, 2, 8, 12, 9, 2, 8, 14, 9, 2, 8, 16, 9, 2, 8, 18, 9, 2, 8, 20, 9, 2, 8, 22, 9, 2, 8,
        24, 9, 2, 8, 26, 9, 2, 8, 28, 9, 2, 8, 30, 9, 2, 8, 32, 9, 2, 8, 34, 9, 2, 8, 36, 9, 2, 8,
        38, 9, 2, 8, 40, 9, 2, 8, 42, 9, 2, 8, 44, 9, 2, 8, 46, 9, 2, 8, 48, 9, 2, 8, 50, 9, 2, 8,
        52, 9, 2, 8, 54, 9, 2, 8, 56, 9, 2, 8, 58, 9, 2, 8, 60, 9, 2, 8, 62, 9, 2, 8, 64, 9, 1, 8,
        66, 9, 2, 8, 68, 9, 2, 8, 70, 9, 2, 8, 72, 9, 2, 8, 74, 9, 2, 8, 82, 2, 0, 8, 84, 9, 1, 8,
        86, 9, 128, 128, 128, 176, 186, 167, 158, 221, 25, 8, 90, 9, 0,
    ];

    /// The `bridge`@`%` row the same cluster wrote for
    /// `CREATE USER 'bridge'@'%' IDENTIFIED BY 'bridgepw'`, then
    /// `GRANT SELECT ON *.*` and `GRANT SUPER ON *.*`: a stage-two password
    /// hash, `Select_priv` and `Super_priv` `Y`, and an unlocked account.
    const BRIDGE_ROW: &[u8] = &[
        8, 2, 2, 2, 37, 8, 4, 2, 12, 98, 114, 105, 100, 103, 101, 8, 6, 2, 82, 42, 55, 53, 54, 51,
        48, 52, 69, 66, 70, 57, 56, 57, 56, 52, 48, 55, 56, 57, 57, 49, 52, 52, 66, 51, 55, 52, 69,
        69, 67, 66, 50, 67, 67, 49, 66, 57, 52, 53, 57, 55, 8, 8, 2, 42, 109, 121, 115, 113, 108,
        95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114, 100, 8, 10, 9, 2, 8,
        12, 9, 1, 8, 14, 9, 1, 8, 16, 9, 1, 8, 18, 9, 1, 8, 20, 9, 1, 8, 22, 9, 1, 8, 24, 9, 1, 8,
        26, 9, 1, 8, 28, 9, 1, 8, 30, 9, 1, 8, 32, 9, 2, 8, 34, 9, 1, 8, 36, 9, 1, 8, 38, 9, 1, 8,
        40, 9, 1, 8, 42, 9, 1, 8, 44, 9, 1, 8, 46, 9, 1, 8, 48, 9, 1, 8, 50, 9, 1, 8, 52, 9, 1, 8,
        54, 9, 1, 8, 56, 9, 1, 8, 58, 9, 1, 8, 60, 9, 1, 8, 62, 9, 1, 8, 64, 9, 1, 8, 66, 9, 1, 8,
        68, 9, 1, 8, 70, 9, 1, 8, 72, 9, 1, 8, 74, 9, 1, 8, 80, 10, 1, 0, 0, 0, 0, 8, 0, 0, 0, 8,
        82, 2, 0, 8, 84, 9, 1, 8, 86, 9, 128, 128, 128, 160, 191, 167, 158, 221, 25, 8, 90, 9, 0,
    ];

    /// The `analyst` row the same cluster wrote for `CREATE ROLE 'analyst'`.
    const ANALYST_ROLE_ROW: &[u8] = &[
        8, 2, 2, 2, 37, 8, 4, 2, 14, 97, 110, 97, 108, 121, 115, 116, 8, 6, 2, 0, 8, 8, 2, 42, 109,
        121, 115, 113, 108, 95, 110, 97, 116, 105, 118, 101, 95, 112, 97, 115, 115, 119, 111, 114,
        100, 8, 10, 9, 1, 8, 12, 9, 1, 8, 14, 9, 1, 8, 16, 9, 1, 8, 18, 9, 1, 8, 20, 9, 1, 8, 22,
        9, 1, 8, 24, 9, 1, 8, 26, 9, 1, 8, 28, 9, 1, 8, 30, 9, 1, 8, 32, 9, 1, 8, 34, 9, 1, 8, 36,
        9, 1, 8, 38, 9, 1, 8, 40, 9, 1, 8, 42, 9, 1, 8, 44, 9, 1, 8, 46, 9, 1, 8, 48, 9, 1, 8, 50,
        9, 1, 8, 52, 9, 1, 8, 54, 9, 1, 8, 56, 9, 1, 8, 58, 9, 1, 8, 60, 9, 1, 8, 62, 9, 1, 8, 64,
        9, 2, 8, 66, 9, 1, 8, 68, 9, 1, 8, 70, 9, 1, 8, 72, 9, 1, 8, 74, 9, 1, 8, 80, 10, 1, 0, 0,
        0, 0, 8, 0, 0, 0, 8, 82, 2, 0, 8, 84, 9, 2, 8, 86, 9, 128, 128, 128, 184, 162, 168, 158,
        221, 25, 8, 90, 9, 0,
    ];

    /// The `bootstrapped = 'True'` row of the same cluster's `mysql.tidb`.
    const BOOTSTRAPPED_ROW: &[u8] = &[
        8, 2, 2, 24, 98, 111, 111, 116, 115, 116, 114, 97, 112, 112, 101, 100, 8, 4, 2, 8, 84, 114,
        117, 101, 8, 6, 2, 60, 66, 111, 111, 116, 115, 116, 114, 97, 112, 32, 102, 108, 97, 103,
        46, 32, 68, 111, 32, 110, 111, 116, 32, 100, 101, 108, 101, 116, 101, 46,
    ];

    /// `TableInfo::cols` places columns by their declared offset, so a
    /// fixture table must give each one its own.
    fn column(offset: i32, id: i64, name: &str, mut field_type: FieldType) -> ColumnInfo {
        if field_type.code() == FieldTypeCode::Enum {
            field_type.set_elems(vec!["N".to_owned(), "Y".to_owned()]);
        }
        let mut column = ColumnInfo::new(id, name, field_type);
        column.offset = offset;
        column
    }

    /// The column IDs, names, and declared types are the live cluster's own,
    /// read back from its stored `TableInfo`.
    fn user_table() -> TableInfo {
        let char_type = FieldType::new(FieldTypeCode::String);
        let blob_type = FieldType::new(FieldTypeCode::Blob);
        let enum_type = FieldType::new(FieldTypeCode::Enum);
        TableInfo {
            id: 4,
            name: CiString::new("user"),
            columns: vec![
                column(0, 1, "Host", char_type.clone()),
                column(1, 2, "User", char_type.clone()),
                column(2, 3, "authentication_string", blob_type),
                column(3, 4, "plugin", char_type),
                column(4, 5, "Select_priv", enum_type.clone()),
                column(5, 16, "Super_priv", enum_type.clone()),
                column(6, 32, "Account_locked", enum_type),
            ],
            ..TableInfo::default()
        }
    }

    fn user_view() -> SystemTableView {
        SystemTableView::project(
            "mysql.user",
            &user_table(),
            &[
                "host",
                "user",
                "authentication_string",
                "plugin",
                "select_priv",
                "super_priv",
                "account_locked",
            ],
        )
    }

    #[test]
    fn a_go_written_root_row_reads_back_as_the_row_go_seeds() {
        let view = user_view();
        let row = SystemRow::parse(&view, ROOT_ROW).expect("the captured row decodes");
        assert_eq!(row.text("host").unwrap().as_deref(), Some("%"));
        assert_eq!(row.text("user").unwrap().as_deref(), Some("root"));
        // Go's non-secure bootstrap INSERT stores the empty string, not NULL;
        // an empty stored value must stay distinguishable from an absent one.
        assert_eq!(
            row.text("authentication_string").unwrap().as_deref(),
            Some("")
        );
        assert_eq!(
            row.text("plugin").unwrap().as_deref(),
            Some("mysql_native_password")
        );
        assert!(row.is_yes("select_priv").unwrap());
        assert!(row.is_yes("super_priv").unwrap());
        assert!(!row.is_yes("account_locked").unwrap());
    }

    #[test]
    fn a_go_created_account_reads_back_with_the_hash_go_stored_for_its_password() {
        let view = user_view();
        let row = SystemRow::parse(&view, BRIDGE_ROW).expect("the captured row decodes");
        assert_eq!(row.text("user").unwrap().as_deref(), Some("bridge"));
        // This exact hash is what `IDENTIFIED BY 'bridgepw'` produced, and
        // what this node's native-password verifier must match a client's
        // scramble against.
        assert_eq!(
            row.text("authentication_string").unwrap().as_deref(),
            Some("*756304EBF9898407899144B374EECB2CC1B94597")
        );
        assert!(row.is_yes("select_priv").unwrap());
        assert!(row.is_yes("super_priv").unwrap());
        assert!(!row.is_yes("account_locked").unwrap());
    }

    #[test]
    fn a_go_created_role_reads_back_as_a_locked_passwordless_account() {
        // `CREATE ROLE 'analyst'` on the same cluster: Go writes a
        // `mysql.user` row with `Account_locked = 'Y'` and no password, which
        // is exactly what makes a role unable to log in.
        let view = user_view();
        let row = SystemRow::parse(&view, ANALYST_ROLE_ROW).expect("the captured row decodes");
        assert_eq!(row.text("user").unwrap().as_deref(), Some("analyst"));
        assert_eq!(
            row.text("authentication_string").unwrap().as_deref(),
            Some("")
        );
        assert!(row.is_yes("account_locked").unwrap());
        assert!(!row.is_yes("select_priv").unwrap());
    }

    #[test]
    fn a_column_the_projection_does_not_carry_is_absent_rather_than_false() {
        // `Shutdown_priv` is a real `mysql.user` column this projection
        // deliberately omits, standing in for the older-cluster case: the
        // reader must be able to say "not there" instead of "N".
        let view = user_view();
        let row = SystemRow::parse(&view, ROOT_ROW).expect("the captured row decodes");
        assert!(!row.has_column("shutdown_priv"));
        assert!(matches!(
            row.is_yes("shutdown_priv"),
            Err(SystemTableError::MissingColumn { .. })
        ));
    }

    #[test]
    fn the_bootstrap_flag_row_reads_back_as_go_wrote_it() {
        let varchar = FieldType::new(FieldTypeCode::Varchar);
        let table = TableInfo {
            id: 18,
            name: CiString::new("tidb"),
            columns: vec![
                column(0, 1, "VARIABLE_NAME", varchar.clone()),
                column(1, 2, "VARIABLE_VALUE", varchar),
            ],
            ..TableInfo::default()
        };
        let view =
            SystemTableView::project("mysql.tidb", &table, &["variable_name", "variable_value"]);
        let row = SystemRow::parse(&view, BOOTSTRAPPED_ROW).expect("the captured row decodes");
        assert_eq!(
            row.text("variable_name").unwrap().as_deref(),
            Some("bootstrapped")
        );
        assert_eq!(row.text("variable_value").unwrap().as_deref(), Some("True"));
    }

    #[test]
    fn reading_a_character_column_as_an_enum_is_refused_rather_than_guessed() {
        let view = user_view();
        let row = SystemRow::parse(&view, ROOT_ROW).expect("the captured row decodes");
        assert!(matches!(
            row.enum_label("plugin"),
            Err(SystemTableError::UnexpectedColumnValue { .. })
        ));
    }
}
