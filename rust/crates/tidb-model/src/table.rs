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
//! independent helpers. Representation boundaries that cannot be expressed by
//! the existing owned Rust model remain explicit at their APIs.

use chrono::{DateTime, FixedOffset};
use std::ops::{Deref, DerefMut};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};
use tidb_ast::{
    CiString, ColumnChoice, TableLockType, ViewAlgorithm, ViewCheckOption, ViewSecurity,
};
use tidb_datatype::FieldType;
use tidb_parser::auth::UserIdentity;
use tidb_parser::ConfigDurationError;

use crate::column::ColumnInfo;
use crate::go_runtime::{GoShared, GoSharedPointerSlice};
use tidb_goruntime::go_runtime::GoSliceElementLayout;
use tidb_goruntime::go_runtime::GoSharedSlice;
use crate::index::IndexColumn;
use crate::schema_state::SchemaState;
use crate::serde_helpers::{
    go_json_field_matches, ignore_unknown, impl_go_json_deserialize, impl_go_json_merge_object,
    FatalSeed, GoValueSlice, NullNoopSeed, OptionObjectSliceSeed, OptionValueSliceSeed,
    SharedCiStringSliceSeed, ValueMergeSeed,
};

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

    #[derive(serde::Serialize)]
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

/// Decodes a Go `time.Time` field into its existing value. `time.Time` owns a
/// custom `UnmarshalJSON`: null is a no-op, while a malformed non-null value
/// aborts the enclosing object immediately rather than becoming a recoverable
/// `encoding/json` type error.
struct GoTimeSeed<'a>(&'a mut DateTime<FixedOffset>);

impl<'de> serde::de::DeserializeSeed<'de> for GoTimeSeed<'_> {
    type Value = ();

    fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        let Some(text) = <Option<String> as serde::Deserialize>::deserialize(deserializer)? else {
            return Ok(());
        };
        *self.0 = parse_go_time(&text)?;
        Ok(())
    }
}

mod go_time_serde {
    use super::{format_go_time, DateTime, FixedOffset};

    pub fn serialize<S: serde::Serializer>(
        value: &DateTime<FixedOffset>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_go_time(value))
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
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
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

impl_go_json_merge_object!(StatsWindowSettings, destination, map, key, {
    if go_json_field_matches(&key, "window_start") {
        map.next_value_seed(FatalSeed(GoTimeSeed(&mut destination.window_start)))?;
    } else if go_json_field_matches(&key, "window_end") {
        map.next_value_seed(FatalSeed(GoTimeSeed(&mut destination.window_end)))?;
    } else if go_json_field_matches(&key, "repeat_type") {
        map.next_value_seed(NullNoopSeed(&mut destination.repeat_type))?;
    } else if go_json_field_matches(&key, "repeat_interval") {
        map.next_value_seed(NullNoopSeed(&mut destination.repeat_interval))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(StatsWindowSettings);

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

impl_go_json_merge_object!(StatsOptions, destination, map, key, {
    if go_json_field_matches(&key, "window_start") {
        let window = destination
            .stats_window_settings
            .get_or_insert_with(|| Box::new(StatsWindowSettings::default()));
        map.next_value_seed(FatalSeed(GoTimeSeed(&mut window.window_start)))?;
    } else if go_json_field_matches(&key, "window_end") {
        let window = destination
            .stats_window_settings
            .get_or_insert_with(|| Box::new(StatsWindowSettings::default()));
        map.next_value_seed(FatalSeed(GoTimeSeed(&mut window.window_end)))?;
    } else if go_json_field_matches(&key, "repeat_type") {
        let window = destination
            .stats_window_settings
            .get_or_insert_with(|| Box::new(StatsWindowSettings::default()));
        map.next_value_seed(NullNoopSeed(&mut window.repeat_type))?;
    } else if go_json_field_matches(&key, "repeat_interval") {
        let window = destination
            .stats_window_settings
            .get_or_insert_with(|| Box::new(StatsWindowSettings::default()));
        map.next_value_seed(NullNoopSeed(&mut window.repeat_interval))?;
    } else if go_json_field_matches(&key, "auto_recalc") {
        map.next_value_seed(NullNoopSeed(&mut destination.auto_recalc))?;
    } else if go_json_field_matches(&key, "column_choice") {
        let mut raw = destination.column_choice.0;
        map.next_value_seed(NullNoopSeed(&mut raw))?;
        destination.column_choice = ColumnChoice(raw);
    } else if go_json_field_matches(&key, "column_list") {
        map.next_value_seed(OptionObjectSliceSeed(&mut destination.column_list))?;
    } else if go_json_field_matches(&key, "sample_num") {
        map.next_value_seed(NullNoopSeed(&mut destination.sample_num))?;
    } else if go_json_field_matches(&key, "sample_rate") {
        map.next_value_seed(NullNoopSeed(&mut destination.sample_rate))?;
    } else if go_json_field_matches(&key, "buckets") {
        map.next_value_seed(NullNoopSeed(&mut destination.buckets))?;
    } else if go_json_field_matches(&key, "topn") {
        map.next_value_seed(NullNoopSeed(&mut destination.top_n))?;
    } else if go_json_field_matches(&key, "concurrency") {
        map.next_value_seed(NullNoopSeed(&mut destination.concurrency))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(StatsOptions);

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
#[derive(Clone, Debug, Default, serde::Serialize)]
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
    #[serde(rename = "view_cols")]
    pub cols: GoValueSlice<CiString>,
}

impl_go_json_merge_object!(ViewInfo, destination, map, key, {
    if go_json_field_matches(&key, "view_algorithm") {
        let mut raw = destination.algorithm.0;
        map.next_value_seed(NullNoopSeed(&mut raw))?;
        destination.algorithm = ViewAlgorithm(raw);
    } else if go_json_field_matches(&key, "view_definer") {
        map.next_value_seed(crate::serde_helpers::OptionBoxMergeSeed(
            &mut destination.definer,
        ))?;
    } else if go_json_field_matches(&key, "view_security") {
        let mut raw = destination.security.0;
        map.next_value_seed(NullNoopSeed(&mut raw))?;
        destination.security = ViewSecurity(raw);
    } else if go_json_field_matches(&key, "view_select") {
        map.next_value_seed(NullNoopSeed(&mut destination.select_stmt))?;
    } else if go_json_field_matches(&key, "view_checkoption") {
        let mut raw = destination.check_option.0;
        map.next_value_seed(NullNoopSeed(&mut raw))?;
        destination.check_option = ViewCheckOption(raw);
    } else if go_json_field_matches(&key, "view_cols") {
        map.next_value_seed(OptionObjectSliceSeed(destination.cols.raw_mut()))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(ViewInfo);

/// Go `ConstraintInfo`: a table CHECK constraint.
#[derive(Clone, Debug, Default, serde::Serialize)]
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
    #[serde(rename = "constraint_cols")]
    pub constraint_cols: GoValueSlice<CiString>,
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

impl_go_json_merge_object!(ConstraintInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "constraint_name") {
        map.next_value_seed(ValueMergeSeed(&mut destination.name))?;
    } else if go_json_field_matches(&key, "tbl_name") {
        map.next_value_seed(ValueMergeSeed(&mut destination.table))?;
    } else if go_json_field_matches(&key, "constraint_cols") {
        map.next_value_seed(OptionObjectSliceSeed(destination.constraint_cols.raw_mut()))?;
    } else if go_json_field_matches(&key, "enforced") {
        map.next_value_seed(NullNoopSeed(&mut destination.enforced))?;
    } else if go_json_field_matches(&key, "in_column") {
        map.next_value_seed(NullNoopSeed(&mut destination.in_column))?;
    } else if go_json_field_matches(&key, "expr_string") {
        map.next_value_seed(NullNoopSeed(&mut destination.expr_string))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(ConstraintInfo);

/// Go `FKVersion0`: foreign-key syntax accepted but not enforced.
pub const FK_VERSION0: i64 = 0;
/// Go `FKVersion1`: foreign-key constraint enforced.
pub const FK_VERSION1: i64 = 1;

/// Go `FKInfo`: a foreign-key constraint.
#[derive(Clone, Debug, Default, serde::Serialize)]
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
    #[serde(rename = "ref_cols")]
    pub ref_cols: GoSharedSlice<CiString>,
    /// The referencing columns.
    #[serde(rename = "cols")]
    pub cols: GoSharedSlice<CiString>,
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

impl_go_json_merge_object!(FKInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "fk_name") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.name)))?;
    } else if go_json_field_matches(&key, "ref_schema") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.ref_schema)))?;
    } else if go_json_field_matches(&key, "ref_table") {
        map.next_value_seed(FatalSeed(ValueMergeSeed(&mut destination.ref_table)))?;
    } else if go_json_field_matches(&key, "ref_cols") {
        map.next_value_seed(SharedCiStringSliceSeed(&mut destination.ref_cols))?;
    } else if go_json_field_matches(&key, "cols") {
        map.next_value_seed(SharedCiStringSliceSeed(&mut destination.cols))?;
    } else if go_json_field_matches(&key, "on_delete") {
        map.next_value_seed(NullNoopSeed(&mut destination.on_delete))?;
    } else if go_json_field_matches(&key, "on_update") {
        map.next_value_seed(NullNoopSeed(&mut destination.on_update))?;
    } else if go_json_field_matches(&key, "state") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "version") {
        map.next_value_seed(NullNoopSeed(&mut destination.version))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(FKInfo);

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
    /// Go's named `(*FKInfo).Clone` method. Nil receivers panic; both column
    /// slices use exact `slices.Clone` header/capacity semantics.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            ref_schema: self.ref_schema.clone(),
            ref_table: self.ref_table.clone(),
            ref_cols: self
                .ref_cols
                .slices_clone(32, GoSliceElementLayout::PointerBearing),
            cols: self
                .cols
                .slices_clone(32, GoSliceElementLayout::PointerBearing),
            on_delete: self.on_delete,
            on_update: self.on_update,
            state: self.state,
            version: self.version,
        }
    }

    /// Nil-receiver-capable Go clone boundary.
    #[must_use]
    pub fn clone_pointer(foreign_key: Option<&Self>) -> GoShared<Self> {
        GoShared::new(foreign_key.expect("nil *FKInfo").clone_like_go())
    }

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
        for (i, col) in self.cols.snapshot().iter().enumerate() {
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
        for (i, col) in self.ref_cols.snapshot().iter().enumerate() {
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
pub fn find_fk_info_by_name(
    foreign_keys: &GoSharedPointerSlice<FKInfo>,
    name: &str,
) -> Option<GoShared<FKInfo>> {
    foreign_keys
        .iter_deref()
        .find(|foreign_key| foreign_key.read().name.lowercase() == name)
}

/// Compatibility spelling for callers that previously requested a mutable
/// Rust reference. The returned Go pointer handle is already mutable and
/// preserves write-through identity.
pub fn find_fk_info_by_name_mut(
    foreign_keys: &GoSharedPointerSlice<FKInfo>,
    name: &str,
) -> Option<GoShared<FKInfo>> {
    find_fk_info_by_name(foreign_keys, name)
}

/// Read guard returned by [`get_idx_changing_field_type`].
pub enum FieldTypeRef<'a> {
    /// The column's inline field type.
    Inline(&'a FieldType),
    /// The online-DDL field type behind its shared Go pointer.
    Changing(RwLockReadGuard<'a, FieldType>),
}

impl Deref for FieldTypeRef<'_> {
    type Target = FieldType;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Inline(value) => value,
            Self::Changing(value) => value,
        }
    }
}

/// Mutable counterpart of [`FieldTypeRef`].
pub enum FieldTypeMut<'a> {
    /// The column's inline field type.
    Inline(&'a mut FieldType),
    /// The online-DDL field type behind its shared Go pointer.
    Changing(RwLockWriteGuard<'a, FieldType>),
}

impl Deref for FieldTypeMut<'_> {
    type Target = FieldType;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Inline(value) => value,
            Self::Changing(value) => value,
        }
    }
}

impl DerefMut for FieldTypeMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Inline(value) => value,
            Self::Changing(value) => value,
        }
    }
}

/// Go `GetIdxChangingFieldType`: selects the online-DDL changing type only
/// when both the index-column marker and changing type are present.
#[must_use]
pub fn get_idx_changing_field_type<'a>(
    index_column: &IndexColumn,
    column: &'a ColumnInfo,
) -> FieldTypeRef<'a> {
    if index_column.use_changing_type {
        if let Some(changing) = &column.changing_field_type {
            return FieldTypeRef::Changing(changing.read());
        }
    }
    FieldTypeRef::Inline(&column.field_type)
}

/// Mutable form of Go `GetIdxChangingFieldType`.
pub fn get_idx_changing_field_type_mut<'a>(
    index_column: &IndexColumn,
    column: &'a mut ColumnInfo,
) -> FieldTypeMut<'a> {
    if index_column.use_changing_type {
        if let Some(changing) = &column.changing_field_type {
            return FieldTypeMut::Changing(changing.write());
        }
    }
    FieldTypeMut::Inline(&mut column.field_type)
}

/// Go `TableNameInfo`.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct TableNameInfo {
    /// Table identifier.
    #[serde(rename = "id", default)]
    pub id: i64,
    /// Case-preserving table name.
    #[serde(rename = "name", default)]
    pub name: CiString,
}

impl_go_json_merge_object!(TableNameInfo, destination, map, key, {
    if go_json_field_matches(&key, "id") {
        map.next_value_seed(NullNoopSeed(&mut destination.id))?;
    } else if go_json_field_matches(&key, "name") {
        map.next_value_seed(ValueMergeSeed(&mut destination.name))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(TableNameInfo);

/// Go `ReferredFKInfo`: a foreign key in a child table that cites this table.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct ReferredFKInfo {
    /// The referenced columns.
    #[serde(rename = "cols")]
    pub cols: GoValueSlice<CiString>,
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

impl_go_json_merge_object!(ReferredFKInfo, destination, map, key, {
    if go_json_field_matches(&key, "cols") {
        map.next_value_seed(OptionObjectSliceSeed(destination.cols.raw_mut()))?;
    } else if go_json_field_matches(&key, "child_schema") {
        map.next_value_seed(ValueMergeSeed(&mut destination.child_schema))?;
    } else if go_json_field_matches(&key, "child_table") {
        map.next_value_seed(ValueMergeSeed(&mut destination.child_table))?;
    } else if go_json_field_matches(&key, "child_fk_name") {
        map.next_value_seed(ValueMergeSeed(&mut destination.child_fk_name))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(ReferredFKInfo);

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

/// Go `ast.TimeUnitType`, in its `iota` order: the integer
/// [`TTLInfo::interval_time_unit`] stores and `TimeUnitType.String()` prints
/// back. Index 0 is Go's `TimeUnitInvalid` placeholder and names nothing.
const TIME_UNIT_TYPES: &[&str] = &[
    "",
    "MICROSECOND",
    "SECOND",
    "MINUTE",
    "HOUR",
    "DAY",
    "WEEK",
    "MONTH",
    "QUARTER",
    "YEAR",
    "SECOND_MICROSECOND",
    "MINUTE_MICROSECOND",
    "MINUTE_SECOND",
    "HOUR_MICROSECOND",
    "HOUR_SECOND",
    "HOUR_MINUTE",
    "DAY_MICROSECOND",
    "DAY_SECOND",
    "DAY_MINUTE",
    "DAY_HOUR",
    "YEAR_MONTH",
];

/// The `ast.TimeUnitType` a written unit keyword is, case-insensitively.
/// `None` for a word that is not one, which is Go's `TimeUnitInvalid`.
#[must_use]
pub fn time_unit_type_from_keyword(keyword: &str) -> Option<i64> {
    TIME_UNIT_TYPES
        .iter()
        .position(|candidate| !candidate.is_empty() && candidate.eq_ignore_ascii_case(keyword))
        .map(|index| index as i64)
}

/// Go `TimeUnitType.String()`.
#[must_use]
pub fn time_unit_type_keyword(value: i64) -> Option<&'static str> {
    usize::try_from(value)
        .ok()
        .and_then(|index| TIME_UNIT_TYPES.get(index))
        .filter(|keyword| !keyword.is_empty())
        .copied()
}

/// Go `TTLInfo`: a table's TTL (time-to-live) configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
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

impl_go_json_merge_object!(TTLInfo, destination, map, key, {
    if go_json_field_matches(&key, "column") {
        map.next_value_seed(ValueMergeSeed(&mut destination.column_name))?;
    } else if go_json_field_matches(&key, "interval_expr") {
        map.next_value_seed(NullNoopSeed(&mut destination.interval_expr_str))?;
    } else if go_json_field_matches(&key, "interval_time_unit") {
        map.next_value_seed(NullNoopSeed(&mut destination.interval_time_unit))?;
    } else if go_json_field_matches(&key, "enable") {
        map.next_value_seed(NullNoopSeed(&mut destination.enable))?;
    } else if go_json_field_matches(&key, "job_interval") {
        map.next_value_seed(NullNoopSeed(&mut destination.job_interval))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(TTLInfo);

impl TTLInfo {
    /// Go `GetJobInterval`, in nanoseconds. Empty persisted values retain the
    /// v6.5-compatible one-hour default.
    pub fn get_job_interval(&self) -> Result<i64, ConfigDurationError> {
        let source = if self.job_interval.is_empty() {
            OLD_DEFAULT_TTL_JOB_INTERVAL
        } else {
            &self.job_interval
        };
        tidb_parser::parse_config_duration(source)
    }
}

/// Go `SequenceInfo`: a sequence object's configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
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

impl_go_json_merge_object!(SequenceInfo, destination, map, key, {
    if go_json_field_matches(&key, "sequence_start") {
        map.next_value_seed(NullNoopSeed(&mut destination.start))?;
    } else if go_json_field_matches(&key, "sequence_cache") {
        map.next_value_seed(NullNoopSeed(&mut destination.cache))?;
    } else if go_json_field_matches(&key, "sequence_cycle") {
        map.next_value_seed(NullNoopSeed(&mut destination.cycle))?;
    } else if go_json_field_matches(&key, "sequence_min_value") {
        map.next_value_seed(NullNoopSeed(&mut destination.min_value))?;
    } else if go_json_field_matches(&key, "sequence_max_value") {
        map.next_value_seed(NullNoopSeed(&mut destination.max_value))?;
    } else if go_json_field_matches(&key, "sequence_increment") {
        map.next_value_seed(NullNoopSeed(&mut destination.increment))?;
    } else if go_json_field_matches(&key, "sequence_cache_value") {
        map.next_value_seed(NullNoopSeed(&mut destination.cache_value))?;
    } else if go_json_field_matches(&key, "sequence_comment") {
        map.next_value_seed(NullNoopSeed(&mut destination.comment))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(SequenceInfo);

/// Go `ExchangePartitionInfo`: the partition-exchange metadata of a table.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
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

impl_go_json_merge_object!(ExchangePartitionInfo, destination, map, key, {
    if go_json_field_matches(&key, "exchange_partition_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.exchange_partition_table_id))?;
    } else if go_json_field_matches(&key, "exchange_partition_def_id") {
        map.next_value_seed(NullNoopSeed(&mut destination.exchange_partition_def_id))?;
    } else if go_json_field_matches(&key, "exchange_partition_flag") {
        map.next_value_seed(NullNoopSeed(&mut destination.xxx_exchange_partition_flag))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(ExchangePartitionInfo);

/// Go `SoftdeleteInfo`: a table's soft-delete configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
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

impl_go_json_merge_object!(SoftdeleteInfo, destination, map, key, {
    if go_json_field_matches(&key, "retention") {
        map.next_value_seed(NullNoopSeed(&mut destination.retention))?;
    } else if go_json_field_matches(&key, "job_enable") {
        map.next_value_seed(NullNoopSeed(&mut destination.job_enable))?;
    } else if go_json_field_matches(&key, "job_interval") {
        map.next_value_seed(NullNoopSeed(&mut destination.job_interval))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(SoftdeleteInfo);

/// Go `TableAffinityInfo`: a table's affinity configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct TableAffinityInfo {
    /// The affinity level.
    #[serde(
        rename = "level",
        default,
        deserialize_with = "crate::serde_helpers::null_default"
    )]
    pub level: String,
}

impl_go_json_merge_object!(TableAffinityInfo, destination, map, key, {
    if go_json_field_matches(&key, "level") {
        map.next_value_seed(NullNoopSeed(&mut destination.level))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(TableAffinityInfo);

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
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
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

impl_go_json_merge_object!(SessionInfo, destination, map, key, {
    if go_json_field_matches(&key, "ServerID") {
        map.next_value_seed(NullNoopSeed(&mut destination.server_id))?;
    } else if go_json_field_matches(&key, "SessionID") {
        map.next_value_seed(NullNoopSeed(&mut destination.session_id))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(SessionInfo);

impl std::fmt::Display for SessionInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "server: {}_session: {}", self.server_id, self.session_id)
    }
}

/// Go `TableLockInfo`: the lock held on a table.
/// No field carries a `json` tag in Go, so the Go field names are used.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TableLockInfo {
    /// The lock type.
    #[serde(rename = "Tp", default, with = "ast_enum_serde::table_lock_type")]
    pub tp: TableLockType,
    /// The sessions holding the lock.
    #[serde(rename = "Sessions")]
    pub sessions: GoValueSlice<SessionInfo>,
    /// The lock state.
    #[serde(rename = "State", default)]
    pub state: TableLockState,
    /// The lock timestamp.
    #[serde(rename = "TS", default)]
    pub ts: u64,
}

impl_go_json_merge_object!(TableLockInfo, destination, map, key, {
    if go_json_field_matches(&key, "Tp") {
        let mut raw = destination.tp.0;
        map.next_value_seed(NullNoopSeed(&mut raw))?;
        destination.tp = TableLockType(raw);
    } else if go_json_field_matches(&key, "Sessions") {
        map.next_value_seed(OptionObjectSliceSeed(destination.sessions.raw_mut()))?;
    } else if go_json_field_matches(&key, "State") {
        map.next_value_seed(NullNoopSeed(&mut destination.state))?;
    } else if go_json_field_matches(&key, "TS") {
        map.next_value_seed(NullNoopSeed(&mut destination.ts))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(TableLockInfo);

/// Go `TableLockTpInfo`: a schema/table/lock-type triple.
/// No field carries a `json` tag in Go, so the Go field names are used.
#[derive(Clone, Copy, Debug, Default, serde::Serialize)]
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

impl_go_json_merge_object!(TableLockTpInfo, destination, map, key, {
    if go_json_field_matches(&key, "SchemaID") {
        map.next_value_seed(NullNoopSeed(&mut destination.schema_id))?;
    } else if go_json_field_matches(&key, "TableID") {
        map.next_value_seed(NullNoopSeed(&mut destination.table_id))?;
    } else if go_json_field_matches(&key, "Tp") {
        let mut raw = destination.tp.0;
        map.next_value_seed(NullNoopSeed(&mut raw))?;
        destination.tp = TableLockType(raw);
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(TableLockTpInfo);

/// Go `TiFlashReplicaInfo`: a table's TiFlash replica configuration.
/// No field carries a `json` tag in Go, so the Go field names are used.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct TiFlashReplicaInfo {
    /// The replica count.
    #[serde(rename = "Count", default)]
    pub count: u64,
    /// The location labels.
    #[serde(rename = "LocationLabels")]
    pub location_labels: GoValueSlice<String>,
    /// Whether the replica is available.
    #[serde(rename = "Available", default)]
    pub available: bool,
    /// The IDs of partitions whose replicas are available.
    #[serde(rename = "AvailablePartitionIDs")]
    pub available_partition_ids: GoValueSlice<i64>,
}

impl_go_json_merge_object!(TiFlashReplicaInfo, destination, map, key, {
    if go_json_field_matches(&key, "Count") {
        map.next_value_seed(NullNoopSeed(&mut destination.count))?;
    } else if go_json_field_matches(&key, "LocationLabels") {
        map.next_value_seed(OptionValueSliceSeed(destination.location_labels.raw_mut()))?;
    } else if go_json_field_matches(&key, "Available") {
        map.next_value_seed(NullNoopSeed(&mut destination.available))?;
    } else if go_json_field_matches(&key, "AvailablePartitionIDs") {
        map.next_value_seed(OptionValueSliceSeed(
            destination.available_partition_ids.raw_mut(),
        ))?;
    } else {
        ignore_unknown(&mut map)?;
    }
});

impl_go_json_deserialize!(TiFlashReplicaInfo);

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
#[path = "table_tests.rs"]
mod tests;
