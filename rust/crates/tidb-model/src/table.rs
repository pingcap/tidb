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
//! DEFERRED (the keystone): the `TableInfo` struct itself and its ~50 methods,
//! `PartitionInfo`, `ViewInfo`, `SequenceInfo`, and the other sub-structs.
//! `TableInfo` gates DBInfo and much of meta/model; it is being approached
//! bottom-up from these leaves.

use chrono::{DateTime, Utc};
use tidb_ast::{
    CiString, ColumnChoice, TableLockType, ViewAlgorithm, ViewCheckOption, ViewSecurity,
};
use tidb_parser::auth::UserIdentity;

use crate::schema_state::SchemaState;

/// Serde adapters for the `ast` enums used by these structs.
///
/// Every one of them is an `int`/`byte` in Go with no `MarshalJSON`, so
/// `encoding/json` writes a bare number. The Rust counterparts in `tidb-ast`
/// are closed enums without serde impls, so each conversion lives here.
mod ast_enum_serde {
    macro_rules! int_enum_serde {
        ($module:ident, $ty:ty, { $($variant:path => $value:literal),+ $(,)? }) => {
            pub mod $module {
                use super::super::*;

                pub fn serialize<S: serde::Serializer>(
                    value: &$ty,
                    serializer: S,
                ) -> Result<S::Ok, S::Error> {
                    serializer.serialize_i64(match *value { $($variant => $value),+ })
                }

                pub fn deserialize<'de, D: serde::Deserializer<'de>>(
                    deserializer: D,
                ) -> Result<$ty, D::Error> {
                    let raw = <i64 as serde::Deserialize>::deserialize(deserializer)?;
                    match raw {
                        $($value => Ok($variant),)+
                        other => Err(serde::de::Error::custom(format_args!(
                            concat!("invalid ", stringify!($ty), " value: {}"),
                            other
                        ))),
                    }
                }
            }
        };
    }

    int_enum_serde!(view_algorithm, ViewAlgorithm, {
        ViewAlgorithm::Undefined => 0,
        ViewAlgorithm::Merge => 1,
        ViewAlgorithm::Temptable => 2,
    });
    int_enum_serde!(view_security, ViewSecurity, {
        ViewSecurity::Definer => 0,
        ViewSecurity::Invoker => 1,
    });
    int_enum_serde!(view_check_option, ViewCheckOption, {
        ViewCheckOption::Local => 0,
        ViewCheckOption::Cascaded => 1,
    });
    int_enum_serde!(column_choice, ColumnChoice, {
        ColumnChoice::Default => 0,
        ColumnChoice::All => 1,
        ColumnChoice::Predicate => 2,
        ColumnChoice::List => 3,
    });
    int_enum_serde!(table_lock_type, TableLockType, {
        TableLockType::None => 0,
        TableLockType::Read => 1,
        TableLockType::ReadLocal => 2,
        TableLockType::ReadOnly => 3,
        TableLockType::Write => 4,
        TableLockType::WriteLocal => 5,
    });
}

/// Go's `ViewCheckOption` zero value is `CheckOptionLocal`, but the Rust enum
/// in `tidb-ast` defaults to `Cascaded`; a missing JSON key must still decode
/// to Go's zero value.
fn view_check_option_zero() -> ViewCheckOption {
    ViewCheckOption::Local
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
fn format_go_time(time: &DateTime<Utc>) -> String {
    let mut formatted = time.format("%Y-%m-%dT%H:%M:%S%.9f").to_string();
    if formatted.contains('.') {
        while formatted.ends_with('0') {
            formatted.pop();
        }
        if formatted.ends_with('.') {
            formatted.pop();
        }
    }
    formatted.push('Z');
    formatted
}

fn parse_go_time<E: serde::de::Error>(text: &str) -> Result<DateTime<Utc>, E> {
    DateTime::parse_from_rfc3339(text)
        .map(|time| time.with_timezone(&Utc))
        .map_err(serde::de::Error::custom)
}

/// Go's `time.Time` zero value is year 1, but a missing key here only ever
/// arises from a truncated encoding; the struct's own `Default` uses the epoch
/// and this keeps the two consistent.
fn unix_epoch() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}

mod go_time_serde {
    use super::{format_go_time, parse_go_time, DateTime, Utc};

    pub fn serialize<S: serde::Serializer>(
        value: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_go_time(value))
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
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
        default = "unix_epoch",
        with = "go_time_serde"
    )]
    pub window_start: DateTime<Utc>,
    /// The window end time.
    #[serde(rename = "window_end", default = "unix_epoch", with = "go_time_serde")]
    pub window_end: DateTime<Utc>,
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
            window_start: DateTime::<Utc>::UNIX_EPOCH,
            window_end: DateTime::<Utc>::UNIX_EPOCH,
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
    pub column_list: Vec<CiString>,
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
        #[derive(serde::Deserialize)]
        struct Wire {
            #[serde(default)]
            window_start: Option<String>,
            #[serde(default)]
            window_end: Option<String>,
            #[serde(default)]
            repeat_type: Option<WindowRepeatType>,
            #[serde(default)]
            repeat_interval: Option<u64>,
            #[serde(default)]
            auto_recalc: bool,
            #[serde(
                default,
                deserialize_with = "ast_enum_serde::column_choice::deserialize"
            )]
            column_choice: ColumnChoice,
            #[serde(default, deserialize_with = "crate::serde_helpers::null_default")]
            column_list: Vec<CiString>,
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
        let has_window = wire.window_start.is_some()
            || wire.window_end.is_some()
            || wire.repeat_type.is_some()
            || wire.repeat_interval.is_some();
        let stats_window_settings = if has_window {
            let parse = |text: Option<String>| match text {
                Some(text) => parse_go_time(&text),
                None => Ok(unix_epoch()),
            };
            Some(Box::new(StatsWindowSettings {
                window_start: parse(wire.window_start)?,
                window_end: parse(wire.window_end)?,
                repeat_type: wire.repeat_type.unwrap_or_default(),
                repeat_interval: wire.repeat_interval.unwrap_or_default(),
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
            column_choice: ColumnChoice::Default,
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
        default = "view_check_option_zero",
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

/// Go `TTLInfo`: a table's TTL (time-to-live) configuration.
///
/// `get_job_interval` (Go, which parses `job_interval` via
/// `time.ParseDuration`) is deferred until a Go duration parser is available
/// at this layer.
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
        assert_eq!(opts.column_choice, ColumnChoice::Default);
        assert!(opts.stats_window_settings.is_none());
        // Default (not the constructor) has auto_recalc false.
        assert!(!StatsOptions::default().auto_recalc);
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
    // Two encoder-level (not field-level) differences remain against Go, and
    // both belong to whatever serializer writes the final bytes, not to these
    // structs: encoding/json HTML-escapes `<`, `>` and `&` inside strings, and
    // it prints an integral float as `0` where serde_json prints `0.0`.
    // Values written by either side parse identically on the other.
    #[test]
    fn json_tags_match_go() {
        let view = ViewInfo {
            algorithm: ViewAlgorithm::Merge,
            definer: Some(Box::new(UserIdentity {
                username: "root".to_owned(),
                hostname: "%".to_owned(),
                ..Default::default()
            })),
            security: ViewSecurity::Invoker,
            select_stmt: "SELECT 1".to_owned(),
            check_option: ViewCheckOption::Cascaded,
            cols: vec![CiString::new("A")],
        };
        assert_eq!(
            serde_json::to_string(&view).unwrap(),
            r#"{"view_algorithm":1,"view_definer":{"Username":"root","Hostname":"%","CurrentUser":false,"AuthUsername":"","AuthHostname":"","AuthPlugin":""},"view_security":1,"view_select":"SELECT 1","view_checkoption":1,"view_cols":[{"O":"A","L":"a"}]}"#
        );

        let constraint = ConstraintInfo {
            id: 1,
            name: CiString::new("c1"),
            table: CiString::new("t"),
            constraint_cols: vec![CiString::new("a")],
            enforced: true,
            in_column: false,
            expr_string: "a = 1".to_owned(),
            state: SchemaState::PUBLIC,
        };
        assert_eq!(
            serde_json::to_string(&constraint).unwrap(),
            r#"{"id":1,"constraint_name":{"O":"c1","L":"c1"},"tbl_name":{"O":"t","L":"t"},"constraint_cols":[{"O":"a","L":"a"}],"enforced":true,"in_column":false,"expr_string":"a = 1","state":5}"#
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
            serde_json::to_string(&sequence).unwrap(),
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
            serde_json::to_string(&ttl).unwrap(),
            r#"{"column":{"O":"t","L":"t"},"interval_expr":"1","interval_time_unit":4,"enable":true,"job_interval":"1h"}"#
        );

        let lock = TableLockInfo {
            tp: TableLockType::Write,
            sessions: vec![SessionInfo {
                server_id: "s".to_owned(),
                session_id: 7,
            }],
            state: TableLockState::PUBLIC,
            ts: 42,
        };
        assert_eq!(
            serde_json::to_string(&lock).unwrap(),
            r#"{"Tp":4,"Sessions":[{"ServerID":"s","SessionID":7}],"State":2,"TS":42}"#
        );

        let replica = TiFlashReplicaInfo {
            count: 2,
            location_labels: vec!["z1".to_owned()],
            available: true,
            available_partition_ids: vec![1],
        };
        assert_eq!(
            serde_json::to_string(&replica).unwrap(),
            r#"{"Count":2,"LocationLabels":["z1"],"Available":true,"AvailablePartitionIDs":[1]}"#
        );

        let exchange = ExchangePartitionInfo {
            exchange_partition_table_id: 3,
            exchange_partition_def_id: 4,
            xxx_exchange_partition_flag: true,
        };
        assert_eq!(
            serde_json::to_string(&exchange).unwrap(),
            r#"{"exchange_partition_id":3,"exchange_partition_def_id":4,"exchange_partition_flag":true}"#
        );
    }

    // Go's `omitempty` drops every zero-valued SoftdeleteInfo field.
    #[test]
    fn softdelete_omitempty() {
        assert_eq!(
            serde_json::to_string(&SoftdeleteInfo::default()).unwrap(),
            "{}"
        );
        assert_eq!(
            serde_json::to_string(&SoftdeleteInfo {
                retention: "1d".to_owned(),
                job_enable: true,
                job_interval: String::new(),
            })
            .unwrap(),
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
        let encoded = serde_json::to_string(&without).unwrap();
        assert_eq!(
            encoded,
            r#"{"auto_recalc":true,"column_choice":0,"column_list":[],"sample_num":0,"sample_rate":0.0,"buckets":0,"topn":0,"concurrency":0}"#
        );
        let back: StatsOptions = serde_json::from_str(&encoded).unwrap();
        assert!(back.stats_window_settings.is_none());
        assert_eq!(back, without);

        let with = StatsOptions {
            stats_window_settings: Some(Box::new(StatsWindowSettings {
                window_start: DateTime::<Utc>::UNIX_EPOCH,
                window_end: DateTime::<Utc>::UNIX_EPOCH,
                repeat_type: WindowRepeatType::DAY,
                repeat_interval: 2,
            })),
            column_choice: ColumnChoice::List,
            column_list: vec![CiString::new("a")],
            ..StatsOptions::new()
        };
        let encoded = serde_json::to_string(&with).unwrap();
        assert_eq!(
            encoded,
            r#"{"window_start":"1970-01-01T00:00:00Z","window_end":"1970-01-01T00:00:00Z","repeat_type":1,"repeat_interval":2,"auto_recalc":true,"column_choice":3,"column_list":[{"O":"a","L":"a"}],"sample_num":0,"sample_rate":0.0,"buckets":0,"topn":0,"concurrency":0}"#
        );
        let back: StatsOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(back, with);
    }

    // Go's time.Time marshals as RFC 3339 with trailing fractional zeros cut.
    #[test]
    fn go_time_format() {
        let base = DateTime::<Utc>::UNIX_EPOCH;
        assert_eq!(format_go_time(&base), "1970-01-01T00:00:00Z");
        let fractional = base + chrono::Duration::nanoseconds(500_000_000);
        assert_eq!(format_go_time(&fractional), "1970-01-01T00:00:00.5Z");
        let nanos = base + chrono::Duration::nanoseconds(123_456_789);
        assert_eq!(format_go_time(&nanos), "1970-01-01T00:00:00.123456789Z");
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
