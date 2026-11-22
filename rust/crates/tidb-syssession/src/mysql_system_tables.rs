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

//! Typed reads of `mysql.*` rows that SQL returned: a view naming the
//! columns a caller reads ([`SystemTableView::from_columns`]) and a row of
//! decoded values keyed by column ID ([`SystemRow::from_values`]). The rows
//! come from Go's statements (`loadTable`'s, the statistics readers'); no
//! row is decoded from the store's keys here.

use std::collections::BTreeMap;
use std::fmt;

use tidb_datatype::{Datum, FieldType};
use tidb_infoschema::cluster_catalog::ClusterCatalogError;

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
    /// A view over a table's public columns as `(name, column ID, declared
    /// type)`, projected to `columns`. Its rows arrive already decoded
    /// ([`SystemRow::from_values`]).
    #[must_use]
    pub fn from_columns(
        name: &str,
        table_id: i64,
        stored: impl IntoIterator<Item = (String, i64, FieldType)>,
        columns: &[&str],
    ) -> Self {
        let mut ids = BTreeMap::new();
        let mut types = BTreeMap::new();
        for (column, id, field_type) in stored {
            let lowercase = column.to_lowercase();
            if !columns.contains(&lowercase.as_str()) {
                continue;
            }
            ids.insert(lowercase, id);
            types.insert(id, field_type);
        }
        Self {
            name: name.to_owned(),
            table_id,
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

/// One stored row, decoded to the projected columns.
pub struct SystemRow<'view> {
    view: &'view SystemTableView,
    values: BTreeMap<i64, Datum>,
}

impl<'view> SystemRow<'view> {
    /// A row of already-decoded column values, keyed by column ID.
    #[must_use]
    pub fn from_values(view: &'view SystemTableView, values: BTreeMap<i64, Datum>) -> Self {
        Self { view, values }
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

    /// Reads one projected column as the stored [`Datum`] itself.
    ///
    /// The typed accessors below each know what one `mysql.*` column means.
    /// A reader of an arbitrary *user* table -- which is what `ANALYZE TABLE`
    /// is (`tidb_executor::cluster_analyze`) -- knows the column's declared type
    /// instead and needs the value in the domain that type describes, so this
    /// is the one accessor that does not interpret.
    ///
    /// `None` is a column the stored row does not carry, which is
    /// indistinguishable from SQL NULL and is treated as NULL by every caller.
    pub fn datum(&self, column: &str) -> Result<Option<&Datum>, SystemTableError> {
        self.value(column)
    }

    /// Reads one projected column, keeping "absent from the row value" apart
    /// from "stored as SQL NULL".
    ///
    /// `Some(&Datum::Null)` is a row that stores NULL there; `None` is a row
    /// with no entry for the column at all, which is every row written before
    /// an `ALTER TABLE ... ADD COLUMN` and whose read value is the column's
    /// `OriginDefaultValue`, not NULL. The row codec records an explicit NULL
    /// (`RawRowValue::Null`) separately from a missing column
    /// (`RawRowValue::Missing`), which is what makes the distinction
    /// available at all.
    ///
    /// [`Self::datum`] collapses the two, which is right for the `mysql.*`
    /// readers: their columns predate every row they read.
    pub fn stored_datum(&self, column: &str) -> Result<Option<&Datum>, SystemTableError> {
        let id = self.view.column_id(column)?;
        Ok(self.values.get(&id))
    }

    /// Returns the decoded values keyed by stored column ID. Writers use this
    /// to edit a system row without losing columns they do not interpret.
    #[must_use]
    pub fn into_values(self) -> BTreeMap<i64, Datum> {
        self.values
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

    /// Reads a raw byte column (`BLOB`/`LONGBLOB`) without charset
    /// interpretation.
    ///
    /// `mysql.stats_buckets`'s bounds and `mysql.stats_top_n`'s values are
    /// stored bytes that are frequently *not* valid UTF-8 — an index bound is
    /// a datum-key-encoded key — so reading them as text would corrupt them.
    pub fn bytes(&self, column: &str) -> Result<Option<Vec<u8>>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Bytes(bytes) | Datum::Raw(bytes) => Ok(Some(bytes.clone())),
            Datum::String(string) => Ok(Some(string.bytes().to_vec())),
            other => Err(self.wrong_value(column, "bytes", other)),
        }
    }

    /// Reads an integer column as `i64`.
    ///
    /// A `BIGINT UNSIGNED` column whose stored value exceeds `i64::MAX` is
    /// refused rather than wrapped; use [`Self::u64`] for the columns that
    /// legitimately hold one (`mysql.stats_meta.version` is a TSO).
    pub fn i64(&self, column: &str) -> Result<Option<i64>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Int(value) => Ok(Some(*value)),
            Datum::UInt(value) => i64::try_from(*value)
                .map(Some)
                .map_err(|_| self.wrong_value(column, "a signed integer", datum)),
            other => Err(self.wrong_value(column, "an integer", other)),
        }
    }

    /// Reads an integer column as `u64`.
    pub fn u64(&self, column: &str) -> Result<Option<u64>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::UInt(value) => Ok(Some(*value)),
            Datum::Int(value) => u64::try_from(*value)
                .map(Some)
                .map_err(|_| self.wrong_value(column, "an unsigned integer", datum)),
            other => Err(self.wrong_value(column, "an integer", other)),
        }
    }

    /// Reads a `DOUBLE` column as `f64`.
    pub fn f64(&self, column: &str) -> Result<Option<f64>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Real(value) | Datum::Float32(value) => Ok(Some(*value)),
            Datum::Int(value) => Ok(Some(*value as f64)),
            Datum::UInt(value) => Ok(Some(*value as f64)),
            other => Err(self.wrong_value(column, "a float", other)),
        }
    }

    /// Reads an `ENUM` column as its stored label.
    pub fn enum_label(&self, column: &str) -> Result<Option<String>, SystemTableError> {
        let Some(datum) = self.value(column)? else {
            return Ok(None);
        };
        match datum {
            Datum::Enum(value, _) => std::str::from_utf8(value.name_bytes())
                .map(|label| Some(label.to_owned()))
                .map_err(|_| self.wrong_value(column, "a UTF-8 ENUM label", datum)),
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
            Datum::Set(value, _) => value
                .name_bytes()
                .split(|byte| *byte == b',')
                .map(|label| {
                    std::str::from_utf8(label)
                        .map(str::to_owned)
                        .map_err(|_| self.wrong_value(column, "UTF-8 SET labels", datum))
                })
                .collect(),
            other => Err(self.wrong_value(column, "SET", other)),
        }
    }
}
