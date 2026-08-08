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

//! Self-contained pieces of `pkg/meta/model/table.go`: the cache-status /
//! temp-table / table-lock enums, `SessionInfo`, `TiFlashReplicaInfo`, the
//! table-lock info structs, and the sequence-default constants.
//!
//! [`crate::TableInfo`] and [`crate::PartitionInfo`] own the enclosing table
//! and partition rules; this module owns their persisted sub-structures and
//! independent helpers. The package receipt records representation boundaries
//! that cannot be expressed by the existing owned Rust model.

use chrono::{DateTime, FixedOffset};
use tidb_ast::{
    CiString, ColumnChoice, TableLockType, ViewAlgorithm, ViewCheckOption, ViewSecurity,
};
use tidb_datatype::{ConfigDurationError, FieldType};
use tidb_parser::auth::UserIdentity;

use crate::column::ColumnInfo;
use crate::index::IndexColumn;
use crate::schema_state::SchemaState;

/// Serde adapters for the `ast` enums used by these structs.
///
/// Every one of them is an `int`/`byte` in Go with no `MarshalJSON`, so
/// `encoding/json` writes a bare number and accepts any number back. The Rust
/// counterparts in `tidb-ast` are value-preserving newtypes without serde
/// impls, so each conversion lives here and, like Go, carries the raw integer
/// through unchanged rather than rejecting or folding an unnamed value.
mod ast_enum_serde {
    macro_rules! int_enum_serde {
        ($module:ident, $ty:ty, $repr:ty) => {
            pub mod $module {
                use super::super::*;

                pub fn serialize<S: serde::Serializer>(
                    value: &$ty,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    serde::Serialize::serialize(&value.0, serializer)
                }

                pub fn deserialize<'de, D: serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<$ty, D::Error> {
                    Ok(<$ty>::from(
                        <Option<$repr> as serde::Deserialize>::deserialize(deserializer)?
                            .unwrap_or_default(),
                    ))
                }
            }
        };
    }

    int_enum_serde!(view_algorithm, ViewAlgorithm, i64);
    int_enum_serde!(view_security, ViewSecurity, i64);
    int_enum_serde!(view_check_option, ViewCheckOption, i64);
    int_enum_serde!(column_choice, ColumnChoice, u8);
    int_enum_serde!(table_lock_type, TableLockType, u8);
}

/// Serde adapter for `auth.UserIdentity`, which has no `json` tags in Go and
/// so marshals under its Go field names, and no serde impls in `tidb-parser`.
mod user_identity_serde {
    use super::UserIdentity;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Wire {
        #[serde(rename = "Username", default)]
        username: String,
        #[serde(rename = "Hostname", default)]
        hostname: String,
        #[serde(rename = "CurrentUser", default)]
        current_user: bool,
        #[serde(rename = "AuthUsername", default)]
        auth_username: String,
        #[serde(rename = "AuthHostname", default)]
        auth_hostname: String,
        #[serde(rename = "AuthPlugin", default)]
        auth_plugin: String,
    }

    impl From<&UserIdentity> for Wire {
        fn from(user: &UserIdentity) -> Self {
            Wire {
                username: user.username.clone(),
                hostname: user.hostname.clone(),
                current_user: user.current_user,
                auth_username: user.auth_username.clone(),
                auth_hostname: user.auth_hostname.clone(),
                auth_plugin: user.auth_plugin.clone(),
            }
        }
    }

    impl From<Wire> for UserIdentity {
        fn from(wire: Wire) -> Self {
            UserIdentity {
                username: wire.username,
                hostname: wire.hostname,
                current_user: wire.current_user,
                auth_username: wire.auth_username,
                auth_hostname: wire.auth_hostname,
                auth_plugin: wire.auth_plugin,
            }
        }
    }

    /// A Go nil `*auth.UserIdentity` marshals as `null`.
    pub fn serialize<S: serde::Serializer>(
        value: &Option<Box<UserIdentity>>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(user) => serde::Serialize::serialize(&Wire::from(&**user), serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Box<UserIdentity>>, D::Error> {
        let wire = <Option<Wire> as serde::Deserialize>::deserialize(deserializer)?;
        Ok(wire.map(|wire| Box::new(UserIdentity::from(wire))))
    }
}

/// Go `time.Time.MarshalJSON`: RFC 3339 with nanosecond precision, trailing
/// zeros in the fractional second removed. `chrono`'s own serde impls are
/// behind a feature this crate does not enable, and would not match this
/// trimming anyway.
fn format_go_time(time: &DateTime<FixedOffset>) -> String {
    let mut formatted = time.format("%Y-%m-%dT%H:%M:%S%.9f").to_string();
    if formatted.contains('.') {
        while formatted.ends_with('0') {
            formatted.pop();
        }
        if formatted.ends_with('.') {
            formatted.pop();
        }
    }
    if time.offset().local_minus_utc() == 0 {
        formatted.push('Z');
    } else {
        formatted.push_str(&time.format("%:z").to_string());
    }
    formatted
}

fn parse_go_time<E: serde::de::Error>(text: &str) -> Result<DateTime<FixedOffset>, E> {
    DateTime::parse_from_rfc3339(text).map_err(serde::de::Error::custom)
}

fn go_zero_time() -> DateTime<FixedOffset> {
    DateTime::parse_from_rfc3339("0001-01-01T00:00:00Z").expect("Go's zero time is valid RFC3339")
}

mod go_time_serde {
    use super::{format_go_time, parse_go_time, DateTime, FixedOffset};

    pub fn serialize<S: serde::Serializer>(
        value: &DateTime<FixedOffset>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_go_time(value))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<FixedOffset>, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        parse_go_time(&text)
    }
}

/// Go's integer-backed named types carry no `MarshalJSON`, so `encoding/json`
/// writes them as bare numbers; the newtypes here marshal the same way.
macro_rules! numeric_newtype_serde {
    ($ty:ty, $inner:ty) => {
        impl serde::Serialize for $ty {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serde::Serialize::serialize(&self.0, serializer)
            }
        }

        impl<'de> serde::Deserialize<'de> for $ty {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                Ok(Self(<$inner as serde::Deserialize>::deserialize(
                    deserializer,
                )?))
            }
        }
    };
}

/// Go `WindowRepeatType` (a `byte`): how a statistics window repeats.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WindowRepeatType(pub u8);

numeric_newtype_serde!(WindowRepeatType, u8);

impl WindowRepeatType {
    /// The window does not repeat (Go `Never`, the zero value).
    pub const NEVER: WindowRepeatType = WindowRepeatType(0);
    /// Repeats daily (Go `Day`).
    pub const DAY: WindowRepeatType = WindowRepeatType(1);
    /// Repeats weekly (Go `Week`).
    pub const WEEK: WindowRepeatType = WindowRepeatType(2);
    /// Repeats monthly (Go `Month`).
    pub const MONTH: WindowRepeatType = WindowRepeatType(3);
}

impl std::fmt::Display for WindowRepeatType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            WindowRepeatType::NEVER => "Never",
            WindowRepeatType::DAY => "Day",
            WindowRepeatType::WEEK => "Week",
            WindowRepeatType::MONTH => "Month",
            _ => "",
        })
    }
}

/// Go `StatsWindowSettings`: the analyze-window schedule.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StatsWindowSettings {
    /// The window start time.
    #[serde(
        rename = "window_start",
        default = "go_zero_time",
        with = "go_time_serde"
    )]
    pub window_start: DateTime<FixedOffset>,
    /// The window end time.
    #[serde(
        rename = "window_end",
        default = "go_zero_time",
        with = "go_time_serde"
    )]
    pub window_end: DateTime<FixedOffset>,
    /// How the window repeats.
    #[serde(rename = "repeat_type", default)]
    pub repeat_type: WindowRepeatType,
    /// The repeat interval.
    #[serde(rename = "repeat_interval", default)]
    pub repeat_interval: u64,
}

impl Default for StatsWindowSettings {
    fn default() -> Self {
        StatsWindowSettings {
            window_start: go_zero_time(),
            window_end: go_zero_time(),
            repeat_type: WindowRepeatType::NEVER,
            repeat_interval: 0,
        }
    }
}

/// Go `StatsOptions`: a table's persisted ANALYZE options.
///
/// The window settings are an embedded `*StatsWindowSettings` in Go, so
/// `encoding/json` flattens their four keys into this object and omits them
/// entirely when the pointer is nil. Serde's `flatten` cannot express the nil
/// case (it would decode an absent group as a present, all-default one), so
/// the two impls below are written by hand.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct StatsOptions {
    /// The analyze-window schedule (Go's embedded `*StatsWindowSettings`).
    pub stats_window_settings: Option<Box<StatsWindowSettings>>,
    /// Whether stats are auto-recalculated.
    pub auto_recalc: bool,
    /// Which columns to analyze.
    pub column_choice: ColumnChoice,
    /// The explicit column list.
    pub column_list: Option<Vec<CiString>>,
    /// The sample count.
    pub sample_num: u64,
    /// The sample rate.
    pub sample_rate: f64,
    /// The histogram bucket count.
    pub buckets: u64,
    /// The top-N count.
    pub top_n: u64,
    /// The analyze concurrency.
    pub concurrency: u64,
}

/// Lets [`StatsOptions`]'s hand-written map serializer reuse the one
/// `ColumnChoice` number mapping.
struct ColumnChoiceEntry(ColumnChoice);

impl serde::Serialize for ColumnChoiceEntry {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ast_enum_serde::column_choice::serialize(&self.0, serializer)
    }
}

impl serde::Serialize for StatsOptions {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;
        // Go emits the embedded struct's fields first, and skips them all when
        // the embedded pointer is nil.
        if let Some(window) = &self.stats_window_settings {
            map.serialize_entry("window_start", &format_go_time(&window.window_start))?;
            map.serialize_entry("window_end", &format_go_time(&window.window_end))?;
            map.serialize_entry("repeat_type", &window.repeat_type)?;
            map.serialize_entry("repeat_interval", &window.repeat_interval)?;
        }
        map.serialize_entry("auto_recalc", &self.auto_recalc)?;
        map.serialize_entry("column_choice", &ColumnChoiceEntry(self.column_choice))?;
        map.serialize_entry("column_list", &self.column_list)?;
        map.serialize_entry("sample_num", &self.sample_num)?;
        map.serialize_entry("sample_rate", &self.sample_rate)?;
        map.serialize_entry("buckets", &self.buckets)?;
        map.serialize_entry("topn", &self.top_n)?;
        map.serialize_entry("concurrency", &self.concurrency)?;
        map.end()
    }
}

impl<'de> serde::Deserialize<'de> for StatsOptions {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Present<T> {
            present: bool,
            value: Option<T>,
        }

        impl<T> Default for Present<T> {
            fn default() -> Self {
                Self {
                    present: false,
                    value: None,
                }
            }
        }

        fn present<'de, D, T>(deserializer: D) -> Result<Present<T>, D::Error>
        where
            D: serde::Deserializer<'de>,
            T: serde::Deserialize<'de>,
        {
            Ok(Present {
                present: true,
                value: <Option<T> as serde::Deserialize>::deserialize(deserializer)?,
            })
        }

        #[derive(serde::Deserialize)]
        struct Wire {
            #[serde(default, deserialize_with = "present")]
            window_start: Present<String>,
            #[serde(default, deserialize_with = "present")]
            window_end: Present<String>,
            #[serde(default, deserialize_with = "present")]
            repeat_type: Present<WindowRepeatType>,
            #[serde(default, deserialize_with = "present")]
            repeat_interval: Present<u64>,
            #[serde(default)]
            auto_recalc: bool,
            #[serde(
                default,
                deserialize_with = "ast_enum_serde::column_choice::deserialize"
            )]
            column_choice: ColumnChoice,
            #[serde(default)]
            column_list: Option<Vec<CiString>>,
            #[serde(default)]
            sample_num: u64,
            #[serde(default)]
            sample_rate: f64,
            #[serde(default)]
            buckets: u64,
            #[serde(default, rename = "topn")]
            top_n: u64,
            #[serde(default)]
            concurrency: u64,
        }

        let wire = Wire::deserialize(deserializer)?;
        // The embedded pointer was non-nil exactly when its keys were written.
        let has_window = wire.window_start.present
            || wire.window_end.present
            || wire.repeat_type.present
            || wire.repeat_interval.present;
        let stats_window_settings = if has_window {
            let parse = |text: Option<String>| match text {
                Some(text) => parse_go_time(&text),
                None => Ok(go_zero_time()),
            };
            Some(Box::new(StatsWindowSettings {
                window_start: parse(wire.window_start.value)?,
                window_end: parse(wire.window_end.value)?,
                repeat_type: wire.repeat_type.value.unwrap_or_default(),
                repeat_interval: wire.repeat_interval.value.unwrap_or_default(),
            }))
        } else {
            None
        };

        Ok(StatsOptions {
            stats_window_settings,
            auto_recalc: wire.auto_recalc,
            column_choice: wire.column_choice,
            column_list: wire.column_list,
            sample_num: wire.sample_num,
            sample_rate: wire.sample_rate,
            buckets: wire.buckets,
            top_n: wire.top_n,
            concurrency: wire.concurrency,
        })
    }
}

impl StatsOptions {
    /// Go `NewStatsOptions`: the defaults (`auto_recalc = true`, default
    /// column choice, everything else zero).
    #[must_use]
    pub fn new() -> Self {
        StatsOptions {
            auto_recalc: true,
            column_choice: ColumnChoice::DEFAULT,
            column_list: Some(Vec::new()),
            ..Default::default()
        }
    }
}

/// Go `ViewInfo`: metadata describing a view.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ViewInfo {
    /// The view algorithm.
    #[serde(
        rename = "view_algorithm",
        default,
        with = "ast_enum_serde::view_algorithm"
    )]
    pub algorithm: ViewAlgorithm,
    /// The view definer (Go's `*auth.UserIdentity`).
    #[serde(rename = "view_definer", default, with = "user_identity_serde")]
    pub definer: Option<Box<UserIdentity>>,
    /// The view security context.
    #[serde(
        rename = "view_security",
        default,
        with = "ast_enum_serde::view_security"
    )]
    pub security: ViewSecurity,
    /// The view's SELECT statement text.
    #[serde(
        rename = "view_select",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub select_stmt: String,
    /// The check option.
    #[serde(
        rename = "view_checkoption",
        default,
        with = "ast_enum_serde::view_check_option"
    )]
    pub check_option: ViewCheckOption,
    /// The view column names.
    #[serde(
        rename = "view_cols",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub cols: Vec<CiString>,
}

/// Go `ConstraintInfo`: a table CHECK constraint.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ConstraintInfo {
    /// The constraint ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The constraint name.
    #[serde(rename = "constraint_name", default)]
    pub name: CiString,
    /// The table name.
    #[serde(rename = "tbl_name", default)]
    pub table: CiString,
    /// The columns the constraint depends on.
    #[serde(
        rename = "constraint_cols",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub constraint_cols: Vec<CiString>,
    /// Whether the constraint is enforced.
    #[serde(rename = "enforced", default)]
    pub enforced: bool,
    /// Whether it is a column-level check.
    #[serde(rename = "in_column", default)]
    pub in_column: bool,
    /// The constraint expression.
    #[serde(
        rename = "expr_string",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub expr_string: String,
    /// The online-DDL state of the constraint.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
}

/// Go `FKVersion0`: foreign-key syntax accepted but not enforced.
pub const FK_VERSION0: i64 = 0;
/// Go `FKVersion1`: foreign-key constraint enforced.
pub const FK_VERSION1: i64 = 1;

/// Go `FKInfo`: a foreign-key constraint.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct FKInfo {
    /// The foreign-key ID.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// The foreign-key name.
    #[serde(rename = "fk_name", default)]
    pub name: CiString,
    /// The referenced schema.
    #[serde(rename = "ref_schema", default)]
    pub ref_schema: CiString,
    /// The referenced table.
    #[serde(rename = "ref_table", default)]
    pub ref_table: CiString,
    /// The referenced columns.
    #[serde(
        rename = "ref_cols",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub ref_cols: Vec<CiString>,
    /// The referencing columns.
    #[serde(
        rename = "cols",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub cols: Vec<CiString>,
    /// The `ON DELETE` action (an `ast.ReferOptionType` value).
    #[serde(rename = "on_delete", default)]
    pub on_delete: i64,
    /// The `ON UPDATE` action (an `ast.ReferOptionType` value).
    #[serde(rename = "on_update", default)]
    pub on_update: i64,
    /// The online-DDL state.
    #[serde(rename = "state", default)]
    pub state: SchemaState,
    /// The FK version (see `FK_VERSION*`).
    #[serde(rename = "version", default)]
    pub version: i64,
}

// Mirrors `ast.ReferOptionType.String` for the int-valued FKInfo.On{Delete,
// Update} (ReferOptionType is not yet in tidb-ast). NoOption(0)/unknown -> "".
fn refer_option_string(opt: i64) -> &'static str {
    match opt {
        1 => "RESTRICT",
        2 => "CASCADE",
        3 => "SET NULL",
        4 => "NO ACTION",
        5 => "SET DEFAULT",
        _ => "",
    }
}

impl FKInfo {
    /// Go `FKInfo.String`: the `db`.`tb`, CONSTRAINT ... FOREIGN KEY clause.
    /// The referencing columns use their original case; the referenced
    /// schema/table use their lower-case form, and the schema is omitted when
    /// it equals `db` (all matching Go).
    #[must_use]
    pub fn string(&self, db: &str, tb: &str) -> String {
        let mut buf = String::new();
        buf.push('`');
        buf.push_str(db);
        buf.push_str("`.`");
        buf.push_str(tb);
        buf.push_str("`, CONSTRAINT `");
        buf.push_str(self.name.original());
        buf.push_str("` FOREIGN KEY (");
        for (i, col) in self.cols.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            buf.push('`');
            buf.push_str(col.original());
            buf.push('`');
        }
        buf.push_str(") REFERENCES `");
        if self.ref_schema.lowercase() != db {
            buf.push_str(self.ref_schema.lowercase());
            buf.push_str("`.`");
        }
        buf.push_str(self.ref_table.lowercase());
        buf.push_str("` (");
        for (i, col) in self.ref_cols.iter().enumerate() {
            if i > 0 {
                buf.push_str(", ");
            }
            buf.push('`');
            buf.push_str(col.original());
            buf.push('`');
        }
        buf.push(')');
        // Go tests the numeric value against ReferOptionNoOption (0).
        if self.on_delete != 0 {
            buf.push_str(" ON DELETE ");
            buf.push_str(refer_option_string(self.on_delete));
        }
        if self.on_update != 0 {
            buf.push_str(" ON UPDATE ");
            buf.push_str(refer_option_string(self.on_update));
        }
        buf
    }
}

/// Go `FindFKInfoByName`: finds a foreign key by an already lower-cased name.
#[must_use]
pub fn find_fk_info_by_name<'a>(foreign_keys: &'a [FKInfo], name: &str) -> Option<&'a FKInfo> {
    foreign_keys
        .iter()
        .find(|foreign_key| foreign_key.name.lowercase() == name)
}

/// Go `GetIdxChangingFieldType`: selects the online-DDL changing type only
/// when both the index-column marker and changing type are present.
#[must_use]
pub fn get_idx_changing_field_type<'a>(
    index_column: &IndexColumn,
    column: &'a ColumnInfo,
) -> &'a FieldType {
    if index_column.use_changing_type {
        if let Some(changing) = &column.changing_field_type {
            return changing;
        }
    }
    &column.field_type
}

/// Go `TableNameInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableNameInfo {
    /// Table identifier.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// Case-preserving table name.
    #[serde(rename = "name", default)]
    pub name: CiString,
}

/// Go `ReferredFKInfo`: a foreign key in a child table that cites this table.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ReferredFKInfo {
    /// The referenced columns.
    #[serde(
        rename = "cols",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub cols: Vec<CiString>,
    /// The child schema.
    #[serde(rename = "child_schema", default)]
    pub child_schema: CiString,
    /// The child table.
    #[serde(rename = "child_table", default)]
    pub child_table: CiString,
    /// The child foreign-key name.
    #[serde(rename = "child_fk_name", default)]
    pub child_fk_name: CiString,
}

/// Go `TableItemID`: one statistics load key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct TableItemID {
    /// Physical or logical table identifier.
    pub table_id: i64,
    /// Column or index identifier.
    pub id: i64,
    /// Whether `id` identifies an index rather than a column.
    pub is_index: bool,
    /// Runtime load-failure marker excluded from [`Self::key`].
    pub is_sync_load_failed: bool,
}

impl TableItemID {
    /// Go `TableItemID.Key`. The sync-load-failed bit is intentionally not
    /// part of identity.
    #[must_use]
    pub fn key(self) -> String {
        format!("{}#{}#{}", self.id, self.table_id, self.is_index)
    }
}

/// Go `StatsLoadItem`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct StatsLoadItem {
    /// Table/column-or-index load identity.
    pub table_item_id: TableItemID,
    /// Whether the loader must fetch complete statistics.
    pub full_load: bool,
}

impl StatsLoadItem {
    /// Returns the stable load key including the full-load flag.
    #[must_use]
    pub fn key(self) -> String {
        format!("{}#{}", self.table_item_id.key(), self.full_load)
    }
}

/// Go `DefaultTTLJobInterval`.
pub const DEFAULT_TTL_JOB_INTERVAL: &str = "24h";
/// Go `OldDefaultTTLJobInterval`.
pub const OLD_DEFAULT_TTL_JOB_INTERVAL: &str = "1h";

/// Go `TTLInfo`: a table's TTL (time-to-live) configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TTLInfo {
    /// The TTL column name.
    #[serde(rename = "column", default)]
    pub column_name: CiString,
    /// The TTL interval expression.
    #[serde(
        rename = "interval_expr",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub interval_expr_str: String,
    /// The interval time unit (an `ast.TimeUnitType` value).
    #[serde(rename = "interval_time_unit", default)]
    pub interval_time_unit: i64,
    /// Whether TTL is enabled.
    #[serde(rename = "enable", default)]
    pub enable: bool,
    /// The background-job interval.
    #[serde(
        rename = "job_interval",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub job_interval: String,
}

impl TTLInfo {
    /// Go `GetJobInterval`, in nanoseconds. Empty persisted values retain the
    /// v6.5-compatible one-hour default.
    pub fn get_job_interval(&self) -> Result<i64, ConfigDurationError> {
        let source = if self.job_interval.is_empty() {
            OLD_DEFAULT_TTL_JOB_INTERVAL
        } else {
            &self.job_interval
        };
        tidb_datatype::parse_config_duration(source)
    }
}

/// Go `SequenceInfo`: a sequence object's configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SequenceInfo {
    /// The start value.
    #[serde(rename = "sequence_start", default)]
    pub start: i64,
    /// Whether values are cached.
    #[serde(rename = "sequence_cache", default)]
    pub cache: bool,
    /// Whether the sequence cycles.
    #[serde(rename = "sequence_cycle", default)]
    pub cycle: bool,
    /// The minimum value.
    #[serde(rename = "sequence_min_value", default)]
    pub min_value: i64,
    /// The maximum value.
    #[serde(rename = "sequence_max_value", default)]
    pub max_value: i64,
    /// The increment.
    #[serde(rename = "sequence_increment", default)]
    pub increment: i64,
    /// The cache size.
    #[serde(rename = "sequence_cache_value", default)]
    pub cache_value: i64,
    /// The sequence comment.
    #[serde(
        rename = "sequence_comment",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub comment: String,
}

/// Go `ExchangePartitionInfo`: the partition-exchange metadata of a table.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ExchangePartitionInfo {
    /// The other table's ID (the non-partitioned table's ID when this info is
    /// on a partitioned table, else the partitioned table's ID).
    #[serde(rename = "exchange_partition_id", default)]
    pub exchange_partition_table_id: i64,
    /// The exchanged partition definition ID.
    #[serde(rename = "exchange_partition_def_id", default)]
    pub exchange_partition_def_id: i64,
    /// Deprecated, unused.
    #[serde(rename = "exchange_partition_flag", default)]
    pub xxx_exchange_partition_flag: bool,
}

/// Go `SoftdeleteInfo`: a table's soft-delete configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SoftdeleteInfo {
    /// The retention period.
    #[serde(
        rename = "retention",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_str",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub retention: String,
    /// Whether the purge job is enabled.
    #[serde(
        rename = "job_enable",
        default,
        skip_serializing_if = "crate::serde_helpers::is_false"
    )]
    pub job_enable: bool,
    /// The purge-job interval.
    #[serde(
        rename = "job_interval",
        default,
        skip_serializing_if = "crate::serde_helpers::is_empty_str",
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub job_interval: String,
}

/// Go `TableAffinityInfo`: a table's affinity configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TableAffinityInfo {
    /// The affinity level.
    #[serde(
        rename = "level",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub level: String,
}

/// Go `NewTableAffinityInfoWithLevel`.
pub fn new_table_affinity_info_with_level(
    level: &str,
) -> Result<Option<TableAffinityInfo>, String> {
    let normalized = tidb_mysql::to_lowercase(level);
    match normalized.as_str() {
        "" | "none" => Ok(None),
        "table" | "partition" => Ok(Some(TableAffinityInfo { level: normalized })),
        _ => Err(format!("invalid table affinity level: '{level}'")),
    }
}

/// Go `TableCacheStatusType` (an `int`): the caching state of a table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableCacheStatusType(pub i64);

numeric_newtype_serde!(TableCacheStatusType, i64);

impl TableCacheStatusType {
    /// Caching disabled (Go `TableCacheStatusDisable`, the zero value).
    pub const DISABLE: TableCacheStatusType = TableCacheStatusType(0);
    /// Caching enabled (Go `TableCacheStatusEnable`).
    pub const ENABLE: TableCacheStatusType = TableCacheStatusType(1);
    /// Caching state switching (Go `TableCacheStatusSwitching`).
    pub const SWITCHING: TableCacheStatusType = TableCacheStatusType(2);
}

impl std::fmt::Display for TableCacheStatusType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            TableCacheStatusType::DISABLE => "disable",
            TableCacheStatusType::ENABLE => "enable",
            TableCacheStatusType::SWITCHING => "switching",
            _ => "",
        })
    }
}

/// Go `TempTableType` (a `byte`): whether/how a table is temporary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TempTableType(pub u8);

numeric_newtype_serde!(TempTableType, u8);

impl TempTableType {
    /// Not a temporary table (Go `TempTableNone`, the zero value).
    pub const NONE: TempTableType = TempTableType(0);
    /// A global temporary table (Go `TempTableGlobal`).
    pub const GLOBAL: TempTableType = TempTableType(1);
    /// A local temporary table (Go `TempTableLocal`).
    pub const LOCAL: TempTableType = TempTableType(2);
}

impl std::fmt::Display for TempTableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            TempTableType::GLOBAL => "global",
            TempTableType::LOCAL => "local",
            _ => "",
        })
    }
}

/// Go `TableLockState` (a `byte`): the state of a table lock.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableLockState(pub u8);

numeric_newtype_serde!(TableLockState, u8);

impl TableLockState {
    /// The lock is absent (Go `TableLockStateNone`, the zero value).
    pub const NONE: TableLockState = TableLockState(0);
    /// The lock is pre-locked (Go `TableLockStatePreLock`).
    pub const PRE_LOCK: TableLockState = TableLockState(1);
    /// The lock is public (Go `TableLockStatePublic`).
    pub const PUBLIC: TableLockState = TableLockState(2);
}

impl std::fmt::Display for TableLockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            TableLockState::PRE_LOCK => "pre-lock",
            TableLockState::PUBLIC => "public",
            // TableLockStateNone and any unknown value.
            _ => "none",
        })
    }
}

/// Go `SessionInfo`: a server/session identifier holding a table lock.
/// Neither field carries a `json` tag in Go, so the Go field names are used.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SessionInfo {
    /// The server ID.
    #[serde(
        rename = "ServerID",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub server_id: String,
    /// The session ID.
    #[serde(rename = "SessionID", default)]
    pub session_id: u64,
}

impl std::fmt::Display for SessionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server: {}_session: {}", self.server_id, self.session_id)
    }
}

/// Go `TableLockInfo`: the lock held on a table.
/// No field carries a `json` tag in Go, so the Go field names are used.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableLockInfo {
    /// The lock type.
    #[serde(rename = "Tp", default, with = "ast_enum_serde::table_lock_type")]
    pub tp: TableLockType,
    /// The sessions holding the lock.
    #[serde(
        rename = "Sessions",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub sessions: Vec<SessionInfo>,
    /// The lock state.
    #[serde(rename = "State", default)]
    pub state: TableLockState,
    /// The lock timestamp.
    #[serde(rename = "TS", default)]
    pub ts: u64,
}

/// Go `TableLockTpInfo`: a schema/table/lock-type triple.
/// No field carries a `json` tag in Go, so the Go field names are used.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableLockTpInfo {
    /// The schema ID.
    #[serde(rename = "SchemaID", default)]
    pub schema_id: i64,
    /// The table ID.
    #[serde(rename = "TableID", default)]
    pub table_id: i64,
    /// The lock type.
    #[serde(rename = "Tp", default, with = "ast_enum_serde::table_lock_type")]
    pub tp: TableLockType,
}

/// Go `TiFlashReplicaInfo`: a table's TiFlash replica configuration.
/// No field carries a `json` tag in Go, so the Go field names are used.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TiFlashReplicaInfo {
    /// The replica count.
    #[serde(rename = "Count", default)]
    pub count: u64,
    /// The location labels.
    #[serde(
        rename = "LocationLabels",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub location_labels: Vec<String>,
    /// Whether the replica is available.
    #[serde(rename = "Available", default)]
    pub available: bool,
    /// The IDs of partitions whose replicas are available.
    #[serde(
        rename = "AvailablePartitionIDs",
        default,
        deserialize_with = "crate::serde_helpers::null_default",
        serialize_with = "crate::serde_helpers::null_if_empty"
    )]
    pub available_partition_ids: Vec<i64>,
}

impl TiFlashReplicaInfo {
    /// Go `IsPartitionAvailable`: whether partition `pid`'s replica is ready.
    #[must_use]
    pub fn is_partition_available(&self, pid: i64) -> bool {
        self.available_partition_ids.contains(&pid)
    }
}

// Sequence default constants (Go's `DefaultSequence*`).
/// Default `CACHE` on/off.
pub const DEFAULT_SEQUENCE_CACHE_BOOL: bool = true;
/// Default `CYCLE` on/off.
pub const DEFAULT_SEQUENCE_CYCLE_BOOL: bool = false;
/// Default `ORDER` on/off.
pub const DEFAULT_SEQUENCE_ORDER_BOOL: bool = false;
/// Default cache size.
pub const DEFAULT_SEQUENCE_CACHE_VALUE: i64 = 1000;
/// Default increment.
pub const DEFAULT_SEQUENCE_INCREMENT_VALUE: i64 = 1;
/// Default start value for a positive-increment sequence.
pub const DEFAULT_POSITIVE_SEQUENCE_START_VALUE: i64 = 1;
/// Default start value for a negative-increment sequence.
pub const DEFAULT_NEGATIVE_SEQUENCE_START_VALUE: i64 = -1;
/// Default min value for a positive-increment sequence.
pub const DEFAULT_POSITIVE_SEQUENCE_MIN_VALUE: i64 = 1;
/// Default max value for a positive-increment sequence.
pub const DEFAULT_POSITIVE_SEQUENCE_MAX_VALUE: i64 = 9_223_372_036_854_775_806;
/// Default max value for a negative-increment sequence.
pub const DEFAULT_NEGATIVE_SEQUENCE_MAX_VALUE: i64 = -1;
/// Default min value for a negative-increment sequence.
pub const DEFAULT_NEGATIVE_SEQUENCE_MIN_VALUE: i64 = -9_223_372_036_854_775_807;

#[cfg(test)]
mod tests {
    use super::*;

    fn go_json<T: serde::Serialize>(value: &T) -> String {
        String::from_utf8(crate::serde_helpers::to_go_json(value).unwrap()).unwrap()
    }

    // The five `TableInfo` sub-struct enums that used to reject an
    // unrecognised ordinal outright. Go declares all five as plain
    // `int`/`byte`, so a document written by a newer TiDB decodes there and
    // must decode here: an unknown `column_choice` may not take out the whole
    // table. Each value is preserved byte for byte across the cycle.
    #[test]
    fn unknown_ast_enum_ordinals_survive_round_trip() {
        let view = r#"{"view_algorithm":7,"view_definer":{"Username":"root","Hostname":"%","CurrentUser":false,"AuthUsername":"","AuthHostname":"","AuthPlugin":""},"view_security":5,"view_select":"SELECT 1","view_checkoption":9,"view_cols":null}"#;
        let decoded: ViewInfo = serde_json::from_str(view).unwrap();
        assert_eq!(decoded.algorithm, ViewAlgorithm(7));
        assert_eq!(decoded.security, ViewSecurity(5));
        assert_eq!(decoded.check_option, ViewCheckOption(9));
        // Go's `String` falls through to these defaults rather than erroring.
        assert_eq!(decoded.algorithm.sql(), "UNDEFINED");
        assert_eq!(decoded.security.sql(), "DEFINER");
        assert_eq!(decoded.check_option.sql(), "CASCADED");
        assert_eq!(go_json(&decoded), view);

        let stats = r#"{"auto_recalc":true,"column_choice":4,"column_list":[],"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#;
        let decoded: StatsOptions = serde_json::from_str(stats).unwrap();
        assert_eq!(decoded.column_choice, ColumnChoice(4));
        assert_eq!(decoded.column_choice.sql(), "DEFAULT");
        assert_eq!(go_json(&decoded), stats);

        let lock = r#"{"Tp":6,"Sessions":null,"State":0,"TS":0}"#;
        let decoded: TableLockInfo = serde_json::from_str(lock).unwrap();
        assert_eq!(decoded.tp, TableLockType(6));
        assert_eq!(decoded.tp.sql(), "");
        assert_eq!(go_json(&decoded), lock);
    }

    #[test]
    fn enum_strings() {
        assert_eq!(TableCacheStatusType::DISABLE.to_string(), "disable");
        assert_eq!(TableCacheStatusType::ENABLE.to_string(), "enable");
        assert_eq!(TableCacheStatusType::SWITCHING.to_string(), "switching");
        assert_eq!(TableCacheStatusType(9).to_string(), "");

        assert_eq!(TempTableType::NONE.to_string(), "");
        assert_eq!(TempTableType::GLOBAL.to_string(), "global");
        assert_eq!(TempTableType::LOCAL.to_string(), "local");

        assert_eq!(TableLockState::NONE.to_string(), "none");
        assert_eq!(TableLockState::PRE_LOCK.to_string(), "pre-lock");
        assert_eq!(TableLockState::PUBLIC.to_string(), "public");
        assert_eq!(TableLockState(9).to_string(), "none");
    }

    #[test]
    fn fk_string() {
        let fk = FKInfo {
            name: CiString::new("fk1"),
            ref_schema: CiString::new("db2"),
            ref_table: CiString::new("parent"),
            ref_cols: vec![CiString::new("id"), CiString::new("x")],
            cols: vec![CiString::new("a"), CiString::new("b")],
            on_delete: 2, // CASCADE
            on_update: 0, // NoOption
            ..Default::default()
        };
        assert_eq!(
            fk.string("db1", "child"),
            "`db1`.`child`, CONSTRAINT `fk1` FOREIGN KEY (`a`, `b`) REFERENCES \
             `db2`.`parent` (`id`, `x`) ON DELETE CASCADE"
        );

        // Same-schema reference omits the schema; ON UPDATE included.
        let fk = FKInfo {
            name: CiString::new("fk2"),
            ref_schema: CiString::new("db1"),
            ref_table: CiString::new("parent"),
            ref_cols: vec![CiString::new("id")],
            cols: vec![CiString::new("pid")],
            on_delete: 0,
            on_update: 1, // RESTRICT
            ..Default::default()
        };
        assert_eq!(
            fk.string("db1", "child"),
            "`db1`.`child`, CONSTRAINT `fk2` FOREIGN KEY (`pid`) REFERENCES \
             `parent` (`id`) ON UPDATE RESTRICT"
        );

        let foreign_keys = vec![fk];
        assert!(find_fk_info_by_name(&foreign_keys, "fk2").is_some());
        // Source requires the caller to supply the lower-case lookup key.
        assert!(find_fk_info_by_name(&foreign_keys, "FK2").is_none());
    }

    #[test]
    fn changing_field_type_boundary() {
        let mut column = ColumnInfo {
            field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Long),
            changing_field_type: Some(Box::new(tidb_datatype::FieldType::new(
                tidb_datatype::FieldTypeCode::Varchar,
            ))),
            ..Default::default()
        };
        let mut index_column = IndexColumn::default();
        assert_eq!(
            get_idx_changing_field_type(&index_column, &column).code(),
            tidb_datatype::FieldTypeCode::Long
        );
        index_column.use_changing_type = true;
        assert_eq!(
            get_idx_changing_field_type(&index_column, &column).code(),
            tidb_datatype::FieldTypeCode::Varchar
        );
        column.changing_field_type = None;
        assert_eq!(
            get_idx_changing_field_type(&index_column, &column).code(),
            tidb_datatype::FieldTypeCode::Long
        );
    }

    #[test]
    fn statistics_keys_ignore_only_the_source_excluded_bit() {
        let item = TableItemID {
            table_id: 12,
            id: 34,
            is_index: true,
            is_sync_load_failed: false,
        };
        assert_eq!(item.key(), "34#12#true");
        assert_eq!(
            TableItemID {
                is_sync_load_failed: true,
                ..item
            }
            .key(),
            item.key()
        );
        assert_eq!(
            StatsLoadItem {
                table_item_id: item,
                full_load: false,
            }
            .key(),
            "34#12#true#false"
        );
    }

    #[test]
    fn ttl_interval_and_affinity_boundaries() {
        assert_eq!(
            TTLInfo::default().get_job_interval().unwrap(),
            3_600_000_000_000
        );
        assert_eq!(
            TTLInfo {
                job_interval: "24h".to_owned(),
                ..Default::default()
            }
            .get_job_interval()
            .unwrap(),
            86_400_000_000_000
        );
        assert!(TTLInfo {
            job_interval: "bad".to_owned(),
            ..Default::default()
        }
        .get_job_interval()
        .is_err());

        assert!(new_table_affinity_info_with_level("").unwrap().is_none());
        assert!(new_table_affinity_info_with_level("NONE")
            .unwrap()
            .is_none());
        assert_eq!(
            new_table_affinity_info_with_level("PaRtItIoN")
                .unwrap()
                .unwrap()
                .level,
            "partition"
        );
        assert_eq!(
            new_table_affinity_info_with_level("bogus").unwrap_err(),
            "invalid table affinity level: 'bogus'"
        );
    }

    #[test]
    fn stats_options_and_window() {
        assert_eq!(WindowRepeatType::NEVER.to_string(), "Never");
        assert_eq!(WindowRepeatType::DAY.to_string(), "Day");
        assert_eq!(WindowRepeatType::WEEK.to_string(), "Week");
        assert_eq!(WindowRepeatType::MONTH.to_string(), "Month");
        assert_eq!(WindowRepeatType(9).to_string(), "");

        let opts = StatsOptions::new();
        assert!(opts.auto_recalc);
        assert_eq!(opts.column_choice, ColumnChoice::DEFAULT);
        assert_eq!(opts.column_list, Some(Vec::new()));
        assert!(opts.stats_window_settings.is_none());
        // Default (not the constructor) has auto_recalc false.
        let zero = StatsOptions::default();
        assert!(!zero.auto_recalc);
        assert!(zero.column_list.is_none());
        assert!(go_json(&zero).contains(r#""column_list":null"#));
    }

    #[test]
    fn view_info_basic() {
        let v = ViewInfo {
            select_stmt: "SELECT 1".to_owned(),
            cols: vec![CiString::new("a")],
            definer: Some(Box::new(UserIdentity {
                username: "root".to_owned(),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert_eq!(v.select_stmt, "SELECT 1");
        assert_eq!(v.definer.as_ref().unwrap().username, "root");
        // Clone is a deep copy.
        let c = v.clone();
        assert_eq!(c.cols[0].original(), "a");
    }

    #[test]
    fn session_info_string() {
        let s = SessionInfo {
            server_id: "s1".to_owned(),
            session_id: 42,
        };
        assert_eq!(s.to_string(), "server: s1_session: 42");
    }

    #[test]
    fn data_structs_clone() {
        let ttl = TTLInfo {
            column_name: CiString::new("t"),
            enable: true,
            ..Default::default()
        };
        assert_eq!(ttl.clone(), ttl);

        let seq = SequenceInfo {
            start: 1,
            max_value: 100,
            ..Default::default()
        };
        assert_eq!(seq.clone().max_value, 100);

        let ep = ExchangePartitionInfo {
            exchange_partition_table_id: 5,
            ..Default::default()
        };
        assert_eq!(ep, ep.clone());

        let rfk = ReferredFKInfo {
            child_table: CiString::new("child"),
            ..Default::default()
        };
        assert_eq!(rfk.child_table.original(), "child");
    }

    // Every json tag from pkg/meta/model/table.go, in Go's field order. The
    // expected bytes were captured from encoding/json on the same values.
    //
    // The assertions use the crate's Go-compatible formatter so HTML-sensitive
    // strings and integral floats are pinned in addition to field order/tags.
    #[test]
    fn json_tags_match_go() {
        let view = ViewInfo {
            algorithm: ViewAlgorithm::MERGE,
            definer: Some(Box::new(UserIdentity {
                username: "root".to_owned(),
                hostname: "%".to_owned(),
                ..Default::default()
            })),
            security: ViewSecurity::INVOKER,
            select_stmt: "SELECT 1".to_owned(),
            check_option: ViewCheckOption::CASCADED,
            cols: vec![CiString::new("A")],
        };
        assert_eq!(
            go_json(&view),
            r#"{"view_algorithm":1,"view_definer":{"Username":"root","Hostname":"%","CurrentUser":false,"AuthUsername":"","AuthHostname":"","AuthPlugin":""},"view_security":1,"view_select":"SELECT 1","view_checkoption":1,"view_cols":[{"O":"A","L":"a"}]}"#
        );

        let constraint = ConstraintInfo {
            id: 1,
            name: CiString::new("c1"),
            table: CiString::new("t"),
            constraint_cols: vec![CiString::new("a")],
            enforced: true,
            in_column: false,
            expr_string: "a < 1 && b > 0".to_owned(),
            state: SchemaState::PUBLIC,
        };
        assert_eq!(
            go_json(&constraint),
            r#"{"id":1,"constraint_name":{"O":"c1","L":"c1"},"tbl_name":{"O":"t","L":"t"},"constraint_cols":[{"O":"a","L":"a"}],"enforced":true,"in_column":false,"expr_string":"a \u003c 1 \u0026\u0026 b \u003e 0","state":5}"#
        );

        let sequence = SequenceInfo {
            start: 1,
            cache: true,
            cycle: false,
            min_value: 1,
            max_value: 10,
            increment: 1,
            cache_value: 1000,
            comment: "c".to_owned(),
        };
        assert_eq!(
            go_json(&sequence),
            r#"{"sequence_start":1,"sequence_cache":true,"sequence_cycle":false,"sequence_min_value":1,"sequence_max_value":10,"sequence_increment":1,"sequence_cache_value":1000,"sequence_comment":"c"}"#
        );

        let ttl = TTLInfo {
            column_name: CiString::new("t"),
            interval_expr_str: "1".to_owned(),
            interval_time_unit: 4,
            enable: true,
            job_interval: "1h".to_owned(),
        };
        assert_eq!(
            go_json(&ttl),
            r#"{"column":{"O":"t","L":"t"},"interval_expr":"1","interval_time_unit":4,"enable":true,"job_interval":"1h"}"#
        );

        let lock = TableLockInfo {
            tp: TableLockType::WRITE,
            sessions: vec![SessionInfo {
                server_id: "s".to_owned(),
                session_id: 7,
            }],
            state: TableLockState::PUBLIC,
            ts: 42,
        };
        assert_eq!(
            go_json(&lock),
            r#"{"Tp":4,"Sessions":[{"ServerID":"s","SessionID":7}],"State":2,"TS":42}"#
        );

        let replica = TiFlashReplicaInfo {
            count: 2,
            location_labels: vec!["z1".to_owned()],
            available: true,
            available_partition_ids: vec![1],
        };
        assert_eq!(
            go_json(&replica),
            r#"{"Count":2,"LocationLabels":["z1"],"Available":true,"AvailablePartitionIDs":[1]}"#
        );

        let exchange = ExchangePartitionInfo {
            exchange_partition_table_id: 3,
            exchange_partition_def_id: 4,
            xxx_exchange_partition_flag: true,
        };
        assert_eq!(
            go_json(&exchange),
            r#"{"exchange_partition_id":3,"exchange_partition_def_id":4,"exchange_partition_flag":true}"#
        );
    }

    // Go's `omitempty` drops every zero-valued SoftdeleteInfo field.
    #[test]
    fn softdelete_omitempty() {
        assert_eq!(go_json(&SoftdeleteInfo::default()), "{}");
        assert_eq!(
            go_json(&SoftdeleteInfo {
                retention: "1d".to_owned(),
                job_enable: true,
                job_interval: String::new(),
            }),
            r#"{"retention":"1d","job_enable":true}"#
        );
    }

    // Go marshals a nil slice as `null` and unmarshals `null` back to the zero
    // value; a missing key likewise stays at the zero value.
    #[test]
    fn null_slices_decode_to_empty() {
        let decoded: FKInfo = serde_json::from_str(
            r#"{"id":1,"ref_cols":null,"cols":null,"fk_name":{"O":"f","L":"f"}}"#,
        )
        .unwrap();
        assert_eq!(decoded.id, 1);
        assert!(decoded.ref_cols.is_empty());
        assert!(decoded.cols.is_empty());
        assert_eq!(decoded.state, SchemaState::NONE);
        assert_eq!(decoded.version, FK_VERSION0);

        let decoded: TiFlashReplicaInfo = serde_json::from_str(
            r#"{"Count":1,"LocationLabels":null,"AvailablePartitionIDs":null}"#,
        )
        .unwrap();
        assert_eq!(decoded.count, 1);
        assert!(decoded.location_labels.is_empty());
    }

    // Go's embedded *StatsWindowSettings is flattened when set and skipped
    // entirely when nil.
    #[test]
    fn stats_options_embedded_window() {
        let without = StatsOptions::new();
        let encoded = go_json(&without);
        assert_eq!(
            encoded,
            r#"{"auto_recalc":true,"column_choice":0,"column_list":[],"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#
        );
        let back: StatsOptions = serde_json::from_str(&encoded).unwrap();
        assert!(back.stats_window_settings.is_none());
        assert_eq!(back, without);

        let with = StatsOptions {
            stats_window_settings: Some(Box::new(StatsWindowSettings {
                window_start: DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap(),
                window_end: DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap(),
                repeat_type: WindowRepeatType::DAY,
                repeat_interval: 2,
            })),
            column_choice: ColumnChoice::LIST,
            column_list: Some(vec![CiString::new("a")]),
            ..StatsOptions::new()
        };
        let encoded = go_json(&with);
        assert_eq!(
            encoded,
            r#"{"window_start":"1970-01-01T00:00:00Z","window_end":"1970-01-01T00:00:00Z","repeat_type":1,"repeat_interval":2,"auto_recalc":true,"column_choice":3,"column_list":[{"O":"a","L":"a"}],"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#
        );
        let back: StatsOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(back, with);

        let present_null: StatsOptions = serde_json::from_str(
            r#"{"window_start":null,"auto_recalc":false,"column_choice":0,"column_list":null,"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#,
        )
        .unwrap();
        let window = present_null.stats_window_settings.as_ref().unwrap();
        assert_eq!(window.window_start, go_zero_time());
        assert_eq!(window.window_end, go_zero_time());
        assert_eq!(
            go_json(&present_null),
            r#"{"window_start":"0001-01-01T00:00:00Z","window_end":"0001-01-01T00:00:00Z","repeat_type":0,"repeat_interval":0,"auto_recalc":false,"column_choice":0,"column_list":null,"sample_num":0,"sample_rate":0,"buckets":0,"topn":0,"concurrency":0}"#
        );
    }

    // Go's time.Time marshals as RFC 3339 with trailing fractional zeros cut.
    #[test]
    fn go_time_format() {
        let base = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap();
        assert_eq!(format_go_time(&base), "1970-01-01T00:00:00Z");
        let fractional = base + chrono::Duration::nanoseconds(500_000_000);
        assert_eq!(format_go_time(&fractional), "1970-01-01T00:00:00.5Z");
        let nanos = base + chrono::Duration::nanoseconds(123_456_789);
        assert_eq!(format_go_time(&nanos), "1970-01-01T00:00:00.123456789Z");
        assert_eq!(format_go_time(&go_zero_time()), "0001-01-01T00:00:00Z");

        let offset = DateTime::parse_from_rfc3339("2026-08-08T12:00:00+05:30").unwrap();
        assert_eq!(format_go_time(&offset), "2026-08-08T12:00:00+05:30");
        let decoded: StatsWindowSettings = serde_json::from_str(
            r#"{"window_start":"2026-08-08T12:00:00+05:30","window_end":"0001-01-01T00:00:00Z","repeat_type":0,"repeat_interval":0}"#,
        )
        .unwrap();
        assert_eq!(decoded.window_start.offset().local_minus_utc(), 19_800);
        assert_eq!(
            go_json(&decoded),
            r#"{"window_start":"2026-08-08T12:00:00+05:30","window_end":"0001-01-01T00:00:00Z","repeat_type":0,"repeat_interval":0}"#
        );
    }

    #[test]
    fn tiflash_partition_available() {
        let tr = TiFlashReplicaInfo {
            count: 1,
            available_partition_ids: vec![3, 7, 11],
            ..Default::default()
        };
        assert!(tr.is_partition_available(7));
        assert!(!tr.is_partition_available(5));
    }
}
