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

//! Go `pkg/session/upgrade_def.go`: the versioned upgrade functions a node
//! runs against a cluster an older TiDB bootstrapped, one per bootstrap
//! version, each a few internal statements on the bootstrap session.
//!
//! Go's `logutil.BgLogger().Fatal` ends the process; here every helper
//! answers the same message as an error, which the startup turns into a
//! failed boot.

use std::collections::{HashMap, HashSet};

use tidb_datatype::Datum;
use tidb_metadef::system_tables_def as metadef;
use tidb_sqlescape::SqlArg;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_terror::TerrorError;
use tidb_vardef::defaults as vardef_defaults;
use tidb_vardef::tidb_vars as vardef;

use crate::bootstrap::{
    init_global_variable_if_not_exists, must_execute, old_password_upgrade, write_oom_action,
    UpgradeResult, UpgradeSession, TIDB_DEF_MEMORY_QUOTA_QUERY,
};
use crate::mysql_bootstrap::{
    CLUSTER_ID_VAR, DDL_TABLE_VERSION_VAR, NEW_COLLATION_ENABLED_VAR, SYSTEM_TZ_VAR,
    TIDB_SERVER_VERSION_VAR, VAR_FALSE, VAR_TRUE,
};

/// Go `version12`.
const VERSION12: i64 = 12;
/// Go `version28`.
const VERSION28: i64 = 28;
/// Go `version38`.
const VERSION38: i64 = 38;
/// Go `version172`.
const VERSION172: i64 = 172;
/// Go `version174`.
pub(crate) const VERSION174: i64 = 174;

/// Go `currentBootstrapVersion`: the largest version in
/// [`UPGRADE_TO_VER_FUNCTIONS`].
pub const CURRENT_BOOTSTRAP_VERSION: i64 = 263;

/// Go `versionedUpgradeFunction`: the upgrade function runs when the
/// bootstrapped version is less than `version`.
pub(crate) struct VersionedUpgradeFunction {
    pub(crate) version: i64,
    pub(crate) func: fn(&mut dyn UpgradeSession, i64) -> UpgradeResult,
}

macro_rules! versioned {
    ($($version:literal => $func:ident),* $(,)?) => {
        &[$(VersionedUpgradeFunction { version: $version, func: $func }),*]
    };
}

/// Go `upgradeToVerFunctions`, ordered by version.
pub(crate) const UPGRADE_TO_VER_FUNCTIONS: &[VersionedUpgradeFunction] = versioned![
    2 => upgrade_to_ver2,
    3 => upgrade_to_ver3,
    4 => upgrade_to_ver4,
    5 => upgrade_to_ver5,
    6 => upgrade_to_ver6,
    7 => upgrade_to_ver7,
    8 => upgrade_to_ver8,
    9 => upgrade_to_ver9,
    10 => upgrade_to_ver10,
    11 => upgrade_to_ver11,
    12 => upgrade_to_ver12,
    13 => upgrade_to_ver13,
    14 => upgrade_to_ver14,
    15 => upgrade_to_ver15,
    16 => upgrade_to_ver16,
    17 => upgrade_to_ver17,
    18 => upgrade_to_ver18,
    19 => upgrade_to_ver19,
    20 => upgrade_to_ver20,
    21 => upgrade_to_ver21,
    22 => upgrade_to_ver22,
    23 => upgrade_to_ver23,
    24 => upgrade_to_ver24,
    25 => upgrade_to_ver25,
    26 => upgrade_to_ver26,
    27 => upgrade_to_ver27,
    28 => upgrade_to_ver28,
    29 => upgrade_to_ver29,
    30 => upgrade_to_ver30,
    31 => upgrade_to_ver31,
    32 => upgrade_to_ver32,
    33 => upgrade_to_ver33,
    34 => upgrade_to_ver34,
    35 => upgrade_to_ver35,
    36 => upgrade_to_ver36,
    37 => upgrade_to_ver37,
    38 => upgrade_to_ver38,
    // We will redo upgradeToVer39 in upgradeToVer46,
    // so upgradeToVer39 is skipped here.
    40 => upgrade_to_ver40,
    41 => upgrade_to_ver41,
    42 => upgrade_to_ver42,
    43 => upgrade_to_ver43,
    44 => upgrade_to_ver44,
    45 => upgrade_to_ver45,
    46 => upgrade_to_ver46,
    47 => upgrade_to_ver47,
    // We will redo upgradeToVer48 and upgradeToVer49 in upgradeToVer55 and upgradeToVer56,
    // so upgradeToVer48 and upgradeToVer49 is skipped here.
    50 => upgrade_to_ver50,
    // We will redo upgradeToVer51 in upgradeToVer63, it is skipped here.
    52 => upgrade_to_ver52,
    53 => upgrade_to_ver53,
    54 => upgrade_to_ver54,
    55 => upgrade_to_ver55,
    56 => upgrade_to_ver56,
    57 => upgrade_to_ver57,
    // We will redo upgradeToVer58 in upgradeToVer64, it is skipped here.
    59 => upgrade_to_ver59,
    60 => upgrade_to_ver60,
    // We will redo upgradeToVer61 in upgradeToVer67, it is skipped here.
    62 => upgrade_to_ver62,
    63 => upgrade_to_ver63,
    64 => upgrade_to_ver64,
    65 => upgrade_to_ver65,
    66 => upgrade_to_ver66,
    67 => upgrade_to_ver67,
    68 => upgrade_to_ver68,
    69 => upgrade_to_ver69,
    70 => upgrade_to_ver70,
    71 => upgrade_to_ver71,
    72 => upgrade_to_ver72,
    73 => upgrade_to_ver73,
    74 => upgrade_to_ver74,
    75 => upgrade_to_ver75,
    76 => upgrade_to_ver76,
    77 => upgrade_to_ver77,
    78 => upgrade_to_ver78,
    79 => upgrade_to_ver79,
    80 => upgrade_to_ver80,
    81 => upgrade_to_ver81,
    82 => upgrade_to_ver82,
    83 => upgrade_to_ver83,
    84 => upgrade_to_ver84,
    85 => upgrade_to_ver85,
    86 => upgrade_to_ver86,
    87 => upgrade_to_ver87,
    88 => upgrade_to_ver88,
    89 => upgrade_to_ver89,
    90 => upgrade_to_ver90,
    91 => upgrade_to_ver91,
    93 => upgrade_to_ver93,
    94 => upgrade_to_ver94,
    95 => upgrade_to_ver95,
    // We will redo upgradeToVer96 in upgradeToVer100, it is skipped here.
    97 => upgrade_to_ver97,
    98 => upgrade_to_ver98,
    100 => upgrade_to_ver100,
    101 => upgrade_to_ver101,
    102 => upgrade_to_ver102,
    103 => upgrade_to_ver103,
    104 => upgrade_to_ver104,
    105 => upgrade_to_ver105,
    106 => upgrade_to_ver106,
    107 => upgrade_to_ver107,
    108 => upgrade_to_ver108,
    109 => upgrade_to_ver109,
    110 => upgrade_to_ver110,
    130 => upgrade_to_ver130,
    131 => upgrade_to_ver131,
    132 => upgrade_to_ver132,
    133 => upgrade_to_ver133,
    134 => upgrade_to_ver134,
    135 => upgrade_to_ver135,
    136 => upgrade_to_ver136,
    137 => upgrade_to_ver137,
    138 => upgrade_to_ver138,
    139 => upgrade_to_ver139,
    140 => upgrade_to_ver140,
    141 => upgrade_to_ver141,
    142 => upgrade_to_ver142,
    143 => upgrade_to_ver143,
    144 => upgrade_to_ver144,
    // We will only use Ver145 to differentiate versions, so it is skipped here.
    146 => upgrade_to_ver146,
    167 => upgrade_to_ver167,
    168 => upgrade_to_ver168,
    169 => upgrade_to_ver169,
    170 => upgrade_to_ver170,
    171 => upgrade_to_ver171,
    172 => upgrade_to_ver172,
    173 => upgrade_to_ver173,
    174 => upgrade_to_ver174,
    175 => upgrade_to_ver175,
    176 => upgrade_to_ver176,
    177 => upgrade_to_ver177,
    178 => upgrade_to_ver178,
    179 => upgrade_to_ver179,
    190 => upgrade_to_ver190,
    191 => upgrade_to_ver191,
    192 => upgrade_to_ver192,
    193 => upgrade_to_ver193,
    194 => upgrade_to_ver194,
    195 => upgrade_to_ver195,
    196 => upgrade_to_ver196,
    197 => upgrade_to_ver197,
    198 => upgrade_to_ver198,
    209 => upgrade_to_ver209,
    210 => upgrade_to_ver210,
    211 => upgrade_to_ver211,
    212 => upgrade_to_ver212,
    213 => upgrade_to_ver213,
    214 => upgrade_to_ver214,
    215 => upgrade_to_ver215,
    216 => upgrade_to_ver216,
    217 => upgrade_to_ver217,
    218 => upgrade_to_ver218,
    239 => upgrade_to_ver239,
    240 => upgrade_to_ver240,
    241 => upgrade_to_ver241,
    242 => upgrade_to_ver242,
    243 => upgrade_to_ver243,
    244 => upgrade_to_ver244,
    245 => upgrade_to_ver245,
    246 => upgrade_to_ver246,
    247 => upgrade_to_ver247,
    248 => upgrade_to_ver248,
    249 => upgrade_to_ver249,
    250 => upgrade_to_ver250,
    251 => upgrade_to_ver251,
    252 => upgrade_to_ver252,
    253 => upgrade_to_ver253,
    254 => upgrade_to_ver254,
    255 => upgrade_to_ver255,
    256 => upgrade_to_ver256,
    257 => upgrade_to_ver257,
    258 => upgrade_to_ver258,
    259 => upgrade_to_ver259,
    260 => upgrade_to_ver260,
    261 => upgrade_to_ver261,
    262 => upgrade_to_ver262,
    263 => upgrade_to_ver263,
];

/// Go `infoschema.ErrColumnExists`.
fn err_column_exists() -> &'static TerrorError {
    &tidb_infoschema::infoschema_errors::ERR_COLUMN_EXISTS
}

/// Go `infoschema.ErrColumnNotExists`.
fn err_column_not_exists() -> &'static TerrorError {
    &tidb_infoschema::infoschema_errors::ERR_COLUMN_NOT_EXISTS
}

/// Go `infoschema.ErrTableExists`.
fn err_table_exists() -> &'static TerrorError {
    &tidb_infoschema::infoschema_errors::ERR_TABLE_EXISTS
}

/// Go `dbterror.ErrCantDropFieldOrKey`.
fn err_cant_drop_field_or_key() -> &'static TerrorError {
    &tidb_dbterror::ERR_CANT_DROP_FIELD_OR_KEY
}

/// Go `dbterror.ErrDupKeyName`.
fn err_dup_key_name() -> &'static TerrorError {
    &tidb_dbterror::ERR_DUP_KEY_NAME
}

/// Go `terror.ErrorEqual(err, target)` for a statement's failure: the same
/// class and code.
pub(crate) fn error_equal(error: &SqlQueryError, target: &TerrorError) -> bool {
    error.identity == Some(target.identity())
}

/// Go `chunk.Row.GetString`.
pub(crate) fn get_string(row: &[Datum], column: usize) -> String {
    String::from_utf8_lossy(row[column].go_bytes()).into_owned()
}

/// Go `chunk.Row.IsNull`.
pub(crate) fn is_null(row: &[Datum], column: usize) -> bool {
    matches!(row[column], Datum::Null)
}

/// Go `upgradeToVer2` updates to version 2.
fn upgrade_to_ver2(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Version 2 add two system variable for DistSQL concurrency controlling.
    // Insert distsql related system variable.
    let dist_sql_vars = [vardef::TIDB_DIST_SQL_SCAN_CONCURRENCY];
    let values = dist_sql_vars
        .iter()
        .map(|v| {
            let default = tidb_variable::sysvar::get_sys_var(v).map_or("", |sv| sv.value);
            format!(r#"("{v}", "{default}")"#)
        })
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT HIGH_PRIORITY IGNORE INTO mysql.global_variables VALUES {};",
        values.join(", ")
    );
    must_execute(s, &sql, &[])
}

/// Go `upgradeToVer3` updates to version 3.
fn upgrade_to_ver3(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Version 3 fix tx_read_only variable value.
    must_execute(
        s,
        "UPDATE HIGH_PRIORITY %n.%n SET variable_value = '0' WHERE variable_name = 'tx_read_only';",
        &[SqlArg::String(b"mysql"), SqlArg::String(b"global_variables")],
    )
}

/// Go `upgradeToVer4` updates to version 4.
fn upgrade_to_ver4(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_STATS_META_TABLE, &[])
}

fn upgrade_to_ver5(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_STATS_HISTOGRAMS_TABLE, &[])?;
    must_execute(s, metadef::CREATE_STATS_BUCKETS_TABLE, &[])
}

fn upgrade_to_ver6(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Super_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_db_priv`", &[err_column_exists()])?;
    // For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Super_priv='Y'", &[])
}

fn upgrade_to_ver7(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Process_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Drop_priv`", &[err_column_exists()])?;
    // For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Process_priv='Y'", &[])
}

fn upgrade_to_ver8(s: &mut dyn UpgradeSession, ver: i64) -> UpgradeResult {
    // This is a dummy upgrade, it checks whether upgradeToVer7 success, if not, do it again.
    if s
        .execute_internal("SELECT HIGH_PRIORITY `Process_priv` FROM mysql.user LIMIT 0")
        .is_ok()
    {
        return Ok(());
    }
    upgrade_to_ver7(s, ver)
}

fn upgrade_to_ver9(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Trigger_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`", &[err_column_exists()])?;
    // For reasons of compatibility, set the non-exists privilege column value to 'Y', as TiDB doesn't check them in older versions.
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Trigger_priv='Y'", &[])
}

/// Go `doReentrantDDL`: one statement whose listed failures mean it already
/// ran.
pub(crate) fn do_reentrant_ddl(
    s: &mut dyn UpgradeSession,
    sql: &str,
    ignorable_errs: &[&TerrorError],
) -> UpgradeResult {
    match s.execute_internal(sql) {
        Ok(_) => Ok(()),
        Err(error) if ignorable_errs.iter().any(|ignorable| error_equal(&error, ignorable)) => Ok(()),
        Err(error) => Err(format!("doReentrantDDL error: {}", error.error_string())),
    }
}

fn upgrade_to_ver10(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_buckets CHANGE COLUMN `value` `upper_bound` BLOB NOT NULL", &[err_column_not_exists(), err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `lower_bound` BLOB", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `null_count` BIGINT(64) NOT NULL DEFAULT 0", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN distinct_ratio", &[err_cant_drop_field_or_key()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms DROP COLUMN use_count_to_estimate", &[err_cant_drop_field_or_key()])
}

fn upgrade_to_ver11(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `References_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`", &[err_column_exists()])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET References_priv='Y'", &[])
}

fn upgrade_to_ver12(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    s.execute_internal("BEGIN")
        .map_err(|error| error.error_string())?;
    let sql = "SELECT HIGH_PRIORITY user, host, password FROM mysql.user WHERE password != ''";
    let mut rs = s.execute_internal(sql);
    if let Err(error) = &rs {
        if error_equal(error, &tidb_plannererrors::ERR_UNKNOWN_COLUMN) {
            let sql = "SELECT HIGH_PRIORITY user, host, authentication_string FROM mysql.user WHERE authentication_string != ''";
            rs = s.execute_internal(sql);
        }
    }
    let rows = rs.map_err(|error| error.error_string())?;
    let mut sqls = Vec::with_capacity(1);
    for row in &rows {
        let user = get_string(row, 0);
        let host = get_string(row, 1);
        let pass = get_string(row, 2);
        let new_pass = old_password_upgrade(&pass)?;
        sqls.push(format!(
            r#"UPDATE HIGH_PRIORITY mysql.user SET password = "{new_pass}" WHERE user="{user}" AND host="{host}""#
        ));
    }

    for sql in &sqls {
        must_execute(s, sql, &[])?;
    }

    let sql = format!(
        r#"INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ("{TIDB_SERVER_VERSION_VAR}", "{VERSION12}", "TiDB bootstrap version.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE="{VERSION12}""#
    );
    must_execute(s, &sql, &[])?;

    must_execute(s, "COMMIT", &[])
}

fn upgrade_to_ver13(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let sqls = [
        "ALTER TABLE mysql.user ADD COLUMN `Create_tmp_table_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Super_priv`",
        "ALTER TABLE mysql.user ADD COLUMN `Lock_tables_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
        "ALTER TABLE mysql.user ADD COLUMN `Create_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
        "ALTER TABLE mysql.user ADD COLUMN `Show_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
        "ALTER TABLE mysql.user ADD COLUMN `Create_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
        "ALTER TABLE mysql.user ADD COLUMN `Alter_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
        "ALTER TABLE mysql.user ADD COLUMN `Event_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_user_priv`",
    ];
    for sql in sqls {
        do_reentrant_ddl(s, sql, &[err_column_exists()])?;
    }
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'", &[])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'", &[])
}

fn upgrade_to_ver14(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let sqls = [
        "ALTER TABLE mysql.db ADD COLUMN `References_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Grant_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Create_tmp_table_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Alter_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Lock_tables_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_tmp_table_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Create_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Lock_tables_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Show_view_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_view_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Create_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Show_view_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Alter_routine_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Create_routine_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Event_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`",
        "ALTER TABLE mysql.db ADD COLUMN `Trigger_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Event_priv`",
    ];
    for sql in sqls {
        do_reentrant_ddl(s, sql, &[err_column_exists()])?;
    }
    Ok(())
}

fn upgrade_to_ver15(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_GCDELETE_RANGE_TABLE, &[])
}

fn upgrade_to_ver16(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `cm_sketch` BLOB", &[err_column_exists()])
}

fn upgrade_to_ver17(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user MODIFY User CHAR(32)", &[])
}

fn upgrade_to_ver18(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `tot_col_size` BIGINT(64) NOT NULL DEFAULT 0", &[err_column_exists()])
}

fn upgrade_to_ver19(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.db MODIFY User CHAR(32)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tables_priv MODIFY User CHAR(32)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.columns_priv MODIFY User CHAR(32)", &[])
}

fn upgrade_to_ver20(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // NOTE: Feedback is deprecated, but we still need to create this table for compatibility.
    do_reentrant_ddl(s, metadef::CREATE_STATS_FEEDBACK_TABLE, &[])
}

fn upgrade_to_ver21(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_GCDELETE_RANGE_DONE_TABLE, &[])?;

    do_reentrant_ddl(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX job_id", &[err_cant_drop_field_or_key()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.gc_delete_range ADD UNIQUE INDEX delete_range_index (job_id, element_id)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.gc_delete_range DROP INDEX element_id", &[err_cant_drop_field_or_key()])
}

fn upgrade_to_ver22(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `stats_ver` BIGINT(64) NOT NULL DEFAULT 0", &[err_column_exists()])
}

fn upgrade_to_ver23(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `flag` BIGINT(64) NOT NULL DEFAULT 0", &[err_column_exists()])
}

/// Go `writeSystemTZ` writes system timezone info into mysql.tidb.
fn write_system_tz(s: &mut dyn UpgradeSession) -> UpgradeResult {
    let tz = tidb_timeutil::infer_system_tz();
    must_execute(
        s,
        r#"INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Global System Timezone.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?"#,
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(SYSTEM_TZ_VAR.as_bytes()),
            SqlArg::String(tz.as_bytes()),
            SqlArg::String(tz.as_bytes()),
        ],
    )
}

/// Go `upgradeToVer24` initializes `System` timezone according to docs/design/2018-09-10-adding-tz-env.md
fn upgrade_to_ver24(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    write_system_tz(s)
}

/// Go `upgradeToVer25` updates tidb_max_chunk_size to new low bound value 32 if previous value is small than 32.
fn upgrade_to_ver25(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let sql = format!(
        "UPDATE HIGH_PRIORITY mysql.global_variables SET VARIABLE_VALUE = '{0}' WHERE VARIABLE_NAME = '{1}' AND VARIABLE_VALUE < {0}",
        vardef_defaults::DEF_INIT_CHUNK_SIZE,
        vardef::TIDB_MAX_CHUNK_SIZE
    );
    must_execute(s, &sql, &[])
}

fn upgrade_to_ver26(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_ROLE_EDGES_TABLE, &[])?;
    must_execute(s, metadef::CREATE_DEFAULT_ROLES_TABLE, &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Create_role_priv` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Drop_role_priv` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Account_locked` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    // user with Create_user_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_role_priv='Y',Drop_role_priv='Y' WHERE Create_user_priv='Y'", &[])?;
    // user with Create_Priv privilege should have Create_view_priv and Show_view_priv after upgrade to v3.0
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_view_priv='Y',Show_view_priv='Y' WHERE Create_priv='Y'", &[])
}

fn upgrade_to_ver27(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `correlation` DOUBLE NOT NULL DEFAULT 0", &[err_column_exists()])
}

fn upgrade_to_ver28(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_BIND_INFO_TABLE, &[])
}

fn upgrade_to_ver29(s: &mut dyn UpgradeSession, ver: i64) -> UpgradeResult {
    // upgradeToVer29 only need to be run when the current version is 28.
    if ver != VERSION28 {
        return Ok(());
    }
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info CHANGE create_time create_time TIMESTAMP(3)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info CHANGE update_time update_time TIMESTAMP(3)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info ADD INDEX sql_index (original_sql(1024),default_db(1024))", &[err_dup_key_name()])
}

fn upgrade_to_ver30(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_STATS_TOP_NTABLE, &[])
}

fn upgrade_to_ver31(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms ADD COLUMN `last_analyze_pos` BLOB DEFAULT NULL", &[err_column_exists()])
}

fn upgrade_to_ver32(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tables_priv MODIFY table_priv SET('Select','Insert','Update','Delete','Create','Drop','Grant', 'Index', 'Alter', 'Create View', 'Show View', 'Trigger', 'References')", &[])
}

fn upgrade_to_ver33(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_EXPR_PUSHDOWN_BLACKLIST_TABLE, &[])
}

fn upgrade_to_ver34(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_OPT_RULE_BLACKLIST_TABLE, &[])
}

fn upgrade_to_ver35(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let sql = format!(
        "UPDATE HIGH_PRIORITY mysql.global_variables SET VARIABLE_NAME = '{}' WHERE VARIABLE_NAME = 'tidb_back_off_weight'",
        vardef::TIDB_BACK_OFF_WEIGHT
    );
    must_execute(s, &sql, &[])
}

fn upgrade_to_ver36(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Shutdown_priv` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    // A root user will have those privileges after upgrading.
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Shutdown_priv='Y' WHERE Super_priv='Y'", &[])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tmp_table_priv='Y',Lock_tables_priv='Y',Create_routine_priv='Y',Alter_routine_priv='Y',Event_priv='Y' WHERE Super_priv='Y'", &[])
}

fn upgrade_to_ver37(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // when upgrade from old tidb and no 'tidb_enable_window_function' in GLOBAL_VARIABLES, init it with 0.
    let sql = format!(
        "INSERT IGNORE INTO  mysql.global_variables (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('{}', '{}')",
        vardef::TIDB_ENABLE_WINDOW_FUNCTION,
        0
    );
    must_execute(s, &sql, &[])
}

fn upgrade_to_ver38(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_GLOBAL_PRIV_TABLE, &[])
}

/// Go `writeNewCollationParameter`.
fn write_new_collation_parameter(s: &mut dyn UpgradeSession, flag: bool) -> UpgradeResult {
    let comment = "If the new collations are enabled. Do not edit it.";
    let b = if flag { VAR_TRUE } else { VAR_FALSE };
    must_execute(
        s,
        "INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(NEW_COLLATION_ENABLED_VAR.as_bytes()),
            SqlArg::String(b.as_bytes()),
            SqlArg::String(comment.as_bytes()),
            SqlArg::String(b.as_bytes()),
        ],
    )
}

fn upgrade_to_ver40(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // There is no way to enable new collation for an existing TiDB cluster.
    write_new_collation_parameter(s, false)
}

fn upgrade_to_ver41(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user CHANGE `password` `authentication_string` TEXT", &[err_column_exists(), err_column_not_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `password` TEXT as (`authentication_string`)", &[err_column_exists()])
}

/// Go `writeDefaultExprPushDownBlacklist` writes default expr pushdown blacklist into mysql.expr_pushdown_blacklist.
fn write_default_expr_push_down_blacklist(s: &mut dyn UpgradeSession) -> UpgradeResult {
    must_execute(
        s,
        "INSERT HIGH_PRIORITY INTO mysql.expr_pushdown_blacklist VALUES('date_add','tiflash', 'DST(daylight saving time) does not take effect in TiFlash date_add')",
        &[],
    )
}

fn upgrade_to_ver42(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `store_type` CHAR(100) NOT NULL DEFAULT 'tikv,tiflash,tidb'", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.expr_pushdown_blacklist ADD COLUMN `reason` VARCHAR(200)", &[err_column_exists()])?;
    write_default_expr_push_down_blacklist(s)
}

/// Go `writeStmtSummaryVars`: convert statement summary global variables to non-empty values.
fn write_stmt_summary_vars(s: &mut dyn UpgradeSession) -> UpgradeResult {
    let sql = "UPDATE %n.%n SET variable_value= %? WHERE variable_name= %? AND variable_value=''";
    let on_off = |b: bool| if b { "ON" } else { "OFF" };
    let pairs = [
        (on_off(vardef_defaults::DEF_TIDB_ENABLE_STMT_SUMMARY).to_owned(), vardef::TIDB_ENABLE_STMT_SUMMARY),
        (on_off(vardef_defaults::DEF_TIDB_STMT_SUMMARY_INTERNAL_QUERY).to_owned(), vardef::TIDB_STMT_SUMMARY_INTERNAL_QUERY),
        (vardef_defaults::DEF_TIDB_STMT_SUMMARY_REFRESH_INTERVAL.to_string(), vardef::TIDB_STMT_SUMMARY_REFRESH_INTERVAL),
        (vardef_defaults::DEF_TIDB_STMT_SUMMARY_HISTORY_SIZE.to_string(), vardef::TIDB_STMT_SUMMARY_HISTORY_SIZE),
        (vardef_defaults::DEF_TIDB_STMT_SUMMARY_MAX_STMT_COUNT.to_string(), vardef::TIDB_STMT_SUMMARY_MAX_STMT_COUNT),
        (vardef_defaults::DEF_TIDB_STMT_SUMMARY_MAX_SQL_LENGTH.to_string(), vardef::TIDB_STMT_SUMMARY_MAX_SQL_LENGTH),
    ];
    for (value, name) in &pairs {
        must_execute(
            s,
            sql,
            &[
                SqlArg::String(b"mysql"),
                SqlArg::String(b"global_variables"),
                SqlArg::String(value.as_bytes()),
                SqlArg::String(name.as_bytes()),
            ],
        )?;
    }
    Ok(())
}

fn upgrade_to_ver43(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    write_stmt_summary_vars(s)
}

fn upgrade_to_ver44(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "DELETE FROM mysql.global_variables where variable_name = \"tidb_isolation_read_engines\"", &[])
}

fn upgrade_to_ver45(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Config_priv` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Config_priv='Y' WHERE Super_priv='Y'", &[])
}

/// Go `upgradeToVer46`: in v3.1.1, we wrongly replace the context of upgradeToVer39 with upgradeToVer44. If we upgrade from v3.1.1 to a newer version,
/// upgradeToVer39 will be missed. So we redo upgradeToVer39 here to make sure the upgrading from v3.1.1 succeed.
fn upgrade_to_ver46(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Reload_priv` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `File_priv` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Reload_priv='Y' WHERE Super_priv='Y'", &[])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET File_priv='Y' WHERE Super_priv='Y'", &[])
}

fn upgrade_to_ver47(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info ADD COLUMN `source` varchar(10) NOT NULL default 'unknown'", &[err_column_exists()])
}

fn upgrade_to_ver50(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_SCHEMA_INDEX_USAGE_TABLE, &[])
}

fn upgrade_to_ver52(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms MODIFY cm_sketch BLOB(6291456)", &[])
}

fn upgrade_to_ver53(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // when upgrade from old tidb and no `tidb_enable_strict_double_type_check` in GLOBAL_VARIABLES, init it with 1`
    let sql = format!(
        "INSERT IGNORE INTO mysql.global_variables (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('{}', '{}')",
        vardef::TIDB_ENABLE_STRICT_DOUBLE_TYPE_CHECK,
        0
    );
    must_execute(s, &sql, &[])
}

fn upgrade_to_ver54(s: &mut dyn UpgradeSession, ver: i64) -> UpgradeResult {
    // The mem-query-quota default value is 32GB by default in v3.0, and 1GB by
    // default in v4.0.
    // If a cluster is upgraded from v3.0.x (bootstrapVer <= version38) to
    // v4.0.9+, we'll write the default value to mysql.tidb. Thus we can get the
    // default value of mem-quota-query, and promise the compatibility even if
    // the tidb-server restarts.
    // If it's a newly deployed cluster, we do not need to write the value into
    // mysql.tidb, since no compatibility problem will happen.

    // This bootstrap task becomes obsolete in TiDB 5.0+, because it appears that the
    // default value of mem-quota-query changes back to 1GB. In TiDB 6.1+ mem-quota-query
    // is no longer a config option, but instead a system variable (tidb_mem_quota_query).

    if ver <= VERSION38 {
        write_memory_quota_query(s)?;
    }
    Ok(())
}

/// Go `upgradeToVer55`: when cherry-pick upgradeToVer52 to v4.0, we wrongly name it upgradeToVer48.
/// If we upgrade from v4.0 to a newer version, the real upgradeToVer48 will be missed.
/// So we redo upgradeToVer48 here to make sure the upgrading from v4.0 succeeds.
fn upgrade_to_ver55(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let def_values: HashMap<&str, &str> = [
        (vardef::TIDB_INDEX_LOOKUP_CONCURRENCY, "4"),
        (vardef::TIDB_INDEX_LOOKUP_JOIN_CONCURRENCY, "4"),
        (vardef::TIDB_HASH_AGG_FINAL_CONCURRENCY, "4"),
        (vardef::TIDB_HASH_AGG_PARTIAL_CONCURRENCY, "4"),
        (vardef::TIDB_WINDOW_CONCURRENCY, "4"),
        (vardef::TIDB_PROJECTION_CONCURRENCY, "4"),
        (vardef::TIDB_HASH_JOIN_CONCURRENCY, "5"),
    ]
    .into_iter()
    .collect();
    let names: Vec<&str> = def_values.keys().copied().collect();

    let select_sql = format!(
        "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('{}')",
        names.join("', '")
    );
    let rows = s
        .execute_internal(&select_sql)
        .map_err(|error| error.error_string())?;
    for row in &rows {
        let n = get_string(row, 0).to_lowercase();
        let v = get_string(row, 1);
        if def_values.get(n.as_str()) != Some(&v.as_str()) {
            return Ok(());
        }
    }

    must_execute(s, "BEGIN", &[])?;
    let v = tidb_variable::session_vars::CONCURRENCY_UNSET.to_string();
    for name in names {
        must_execute(
            s,
            &format!("UPDATE mysql.global_variables SET variable_value='{v}' WHERE variable_name='{name}'"),
            &[],
        )?;
    }
    must_execute(s, "COMMIT", &[])
}

/// Go `upgradeToVer56`: when cherry-pick upgradeToVer54 to v4.0, we wrongly name it upgradeToVer49.
/// If we upgrade from v4.0 to a newer version, the real upgradeToVer49 will be missed.
/// So we redo upgradeToVer49 here to make sure the upgrading from v4.0 succeeds.
fn upgrade_to_ver56(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_STATS_EXTENDED_TABLE, &[])
}

fn upgrade_to_ver57(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    insert_builtin_bind_info_row(s)
}

/// Go `insertBuiltinBindInfoRow`.
pub(crate) fn insert_builtin_bind_info_row(s: &mut dyn UpgradeSession) -> UpgradeResult {
    use tidb_bindinfo::binding::STATUS_BUILTIN;
    use tidb_bindinfo::binding_utils::BUILTIN_PSEUDO_SQL_4_BIND_LOCK;
    must_execute(
        s,
        r#"INSERT HIGH_PRIORITY INTO mysql.bind_info(original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source)
						VALUES (%?, %?, "mysql", %?, "0000-00-00 00:00:00", "0000-00-00 00:00:00", "", "", %?)"#,
        &[
            SqlArg::String(BUILTIN_PSEUDO_SQL_4_BIND_LOCK.as_bytes()),
            SqlArg::String(BUILTIN_PSEUDO_SQL_4_BIND_LOCK.as_bytes()),
            SqlArg::String(STATUS_BUILTIN.as_bytes()),
            SqlArg::String(STATUS_BUILTIN.as_bytes()),
        ],
    )
}

fn upgrade_to_ver59(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // The oom-action default value is log by default in v3.0, and cancel by
    // default in v4.0.11+.
    // If a cluster is upgraded from v3.0.x (bootstrapVer <= version59) to
    // v4.0.11+, we'll write the default value to mysql.tidb. Thus we can get
    // the default value of oom-action, and promise the compatibility even if
    // the tidb-server restarts.
    // If it's a newly deployed cluster, we do not need to write the value into
    // mysql.tidb, since no compatibility problem will happen.
    write_oom_action(s)
}

fn upgrade_to_ver60(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "DROP TABLE IF EXISTS mysql.stats_extended", &[])?;
    do_reentrant_ddl(s, metadef::CREATE_STATS_EXTENDED_TABLE, &[])
}

/// Go `bindInfo`.
struct BindInfo {
    bind_sql: String,
    status: String,
    create_time: String,
    charset: String,
    collation: String,
    source: String,
}

fn upgrade_to_ver67(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "BEGIN PESSIMISTIC", &[])?;
    let result = upgrade_to_ver67_in_txn(s);
    // Go's deferred ROLLBACK runs only for a failure the function did not
    // already make fatal; every failure here is one.
    result?;
    must_execute(s, "COMMIT", &[])
}

fn upgrade_to_ver67_in_txn(s: &mut dyn UpgradeSession) -> UpgradeResult {
    must_execute(s, tidb_bindinfo::binding_utils::LOCK_BIND_INFO_SQL, &[])?;
    let rows = s
        .execute_internal(
            "SELECT bind_sql, default_db, status, create_time, charset, collation, source
			FROM mysql.bind_info
			WHERE source != 'builtin'
			ORDER BY update_time DESC",
        )
        .map_err(|error| format!("upgradeToVer67 error: {}", error.error_string()))?;
    let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
    let mut bind_map: Vec<(String, BindInfo)> = Vec::new();
    update_bind_info(&rows, &mut bind_map)?;

    must_execute(s, "DELETE FROM mysql.bind_info where source != 'builtin'", &[])?;
    let quote = tidb_expr::quote_str;
    for (original, bind) in &bind_map {
        must_execute(
            s,
            &format!(
                "INSERT INTO mysql.bind_info VALUES({}, {}, '', {}, {}, {}, {}, {}, {})",
                quote(original),
                quote(&bind.bind_sql),
                quote(&bind.status),
                quote(&bind.create_time),
                quote(&now),
                quote(&bind.charset),
                quote(&bind.collation),
                quote(&bind.source),
            ),
            &[],
        )?;
    }
    Ok(())
}

/// Go `updateBindInfo`.
fn update_bind_info(rows: &[Vec<Datum>], bind_map: &mut Vec<(String, BindInfo)>) -> UpgradeResult {
    use tidb_bindinfo::binding::{STATUS_BUILTIN, STATUS_ENABLED, STATUS_USING};
    for row in rows {
        let bind = get_string(row, 0);
        let db = get_string(row, 1);
        let status = get_string(row, 2);

        if status != STATUS_ENABLED && status != STATUS_USING && status != STATUS_BUILTIN {
            continue;
        }

        let charset = get_string(row, 4);
        let collation = get_string(row, 5);
        let stmt = tidb_parser::parse(&bind)
            .map_err(|error| format!("updateBindInfo error: {}", error.message))?;
        let restored = tidb_parser::util_parser::restore_with_default_db(&stmt, &db, &bind);
        let origin_with_db = tidb_parser::normalize(&restored, tidb_parser::RedactMode::Enabled);
        if bind_map.iter().any(|(original, _)| *original == origin_with_db) {
            // The results are sorted in descending order of time.
            // And in the following cases, duplicate originWithDB may occur
            //      originalText         	|bindText                                   	|DB
            //		`select * from t` 		|`select /*+ use_index(t, idx) */ * from t` 	|`test`
            // 		`select * from test.t`  |`select /*+ use_index(t, idx) */ * from test.t`|``
            // Therefore, if repeated, we can skip to keep the latest binding.
            continue;
        }
        let create_time = match &row[3] {
            Datum::Time(time) => time.to_string(),
            other => String::from_utf8_lossy(other.go_bytes()).into_owned(),
        };
        bind_map.push((
            origin_with_db,
            BindInfo {
                bind_sql: restored,
                status,
                create_time,
                charset,
                collation,
                source: get_string(row, 6),
            },
        ));
    }
    Ok(())
}

/// Go `writeMemoryQuotaQuery`.
fn write_memory_quota_query(s: &mut dyn UpgradeSession) -> UpgradeResult {
    let comment = "memory_quota_query is 32GB by default in v3.0.x, 1GB by default in v4.0.x+";
    must_execute(
        s,
        "INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, %?) ON DUPLICATE KEY UPDATE VARIABLE_VALUE=%?",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(TIDB_DEF_MEMORY_QUOTA_QUERY.as_bytes()),
            SqlArg::Signed(32 << 30),
            SqlArg::String(comment.as_bytes()),
            SqlArg::Signed(32 << 30),
        ],
    )
}

fn upgrade_to_ver62(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_buckets ADD COLUMN `ndv` bigint not null default 0", &[err_column_exists()])
}

fn upgrade_to_ver63(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Create_tablespace_priv` ENUM('N','Y') DEFAULT 'N'", &[err_column_exists()])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Create_tablespace_priv='Y' where Super_priv='Y'", &[])
}

fn upgrade_to_ver64(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_slave_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Execute_priv`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN `Repl_client_priv` ENUM('N','Y') CHARACTER SET utf8 NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`", &[err_column_exists()])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET Repl_slave_priv='Y',Repl_client_priv='Y' where Super_priv='Y'", &[])
}

fn upgrade_to_ver65(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_STATS_FMSKETCH_TABLE, &[])
}

fn upgrade_to_ver66(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "set @@global.tidb_track_aggregate_memory_usage = 1", &[])
}

fn upgrade_to_ver68(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "DELETE FROM mysql.global_variables where VARIABLE_NAME = 'tidb_enable_clustered_index' and VARIABLE_VALUE = 'OFF'", &[])
}

fn upgrade_to_ver69(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_GLOBAL_GRANTS_TABLE, &[])
}

fn upgrade_to_ver70(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN plugin CHAR(64) AFTER authentication_string", &[err_column_exists()])?;
    must_execute(s, "UPDATE HIGH_PRIORITY mysql.user SET plugin='mysql_native_password'", &[])
}

fn upgrade_to_ver71(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='OFF' WHERE VARIABLE_NAME = 'tidb_multi_statement_mode' AND VARIABLE_VALUE = 'WARN'", &[])
}

fn upgrade_to_ver72(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_meta ADD COLUMN snapshot BIGINT(64) UNSIGNED NOT NULL DEFAULT 0", &[err_column_exists()])
}

fn upgrade_to_ver73(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_CAPTURE_PLAN_BASELINES_BLACKLIST_TABLE, &[])
}

fn upgrade_to_ver74(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // The old default value of `tidb_stmt_summary_max_stmt_count` is 200, we want to enlarge this to the new default value when TiDB upgrade.
    must_execute(
        s,
        &format!(
            "UPDATE mysql.global_variables SET VARIABLE_VALUE='{}' WHERE VARIABLE_NAME = 'tidb_stmt_summary_max_stmt_count' AND CAST(VARIABLE_VALUE AS SIGNED) = 200",
            vardef_defaults::DEF_TIDB_STMT_SUMMARY_MAX_STMT_COUNT
        ),
        &[],
    )
}

fn upgrade_to_ver75(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user MODIFY COLUMN Host CHAR(255)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.global_priv MODIFY COLUMN Host CHAR(255)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.db MODIFY COLUMN Host CHAR(255)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Host CHAR(255)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Host CHAR(255)", &[])
}

fn upgrade_to_ver76(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.columns_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')", &[])
}

fn upgrade_to_ver77(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_COLUMN_STATS_USAGE_TABLE, &[])
}

fn upgrade_to_ver78(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_buckets MODIFY upper_bound LONGBLOB NOT NULL", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_buckets MODIFY lower_bound LONGBLOB", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_histograms MODIFY last_analyze_pos LONGBLOB DEFAULT NULL", &[])
}

fn upgrade_to_ver79(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_TABLE_CACHE_META_TABLE, &[])
}

fn upgrade_to_ver80(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Check whether tidb_analyze_version exists in mysql.global_variables.
    // If it is missing, initialize it to 2. Earlier bootstrap logic used 1 for compatibility,
    // but the supported write version is now 2 after the legacy analyze version 1 write path was removed.
    init_global_variable_if_not_exists(s, vardef::TIDB_ANALYZE_VERSION, SqlArg::Signed(2))
}

/// Go `upgradeToVer81`: for users that upgrade TiDB from a pre-4.0 version, we want to disable index merge by default.
/// This helps minimize query plan regressions.
fn upgrade_to_ver81(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Check if tidb_enable_index_merge exists in mysql.GLOBAL_VARIABLES.
    // If not, insert "tidb_enable_index_merge | off".
    init_global_variable_if_not_exists(s, vardef::TIDB_ENABLE_INDEX_MERGE, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver82(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_ANALYZE_OPTIONS_TABLE, &[])
}

fn upgrade_to_ver83(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_STATS_HISTORY_TABLE, &[])
}

fn upgrade_to_ver84(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_STATS_META_HISTORY_TABLE, &[])
}

fn upgrade_to_ver85(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    use tidb_bindinfo::binding::{STATUS_ENABLED, STATUS_USING};
    must_execute(
        s,
        &format!("UPDATE HIGH_PRIORITY mysql.bind_info SET status= '{STATUS_ENABLED}' WHERE status = '{STATUS_USING}'"),
        &[],
    )
}

fn upgrade_to_ver86(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tables_priv MODIFY COLUMN Column_priv SET('Select','Insert','Update','References')", &[])
}

fn upgrade_to_ver87(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_ANALYZE_JOBS_TABLE, &[])
}

fn upgrade_to_ver88(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user CHANGE `Repl_slave_priv` `Repl_slave_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Execute_priv`", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user CHANGE `Repl_client_priv` `Repl_client_priv` ENUM('N','Y') NOT NULL DEFAULT 'N' AFTER `Repl_slave_priv`", &[])
}

fn upgrade_to_ver89(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_ADVISORY_LOCKS_TABLE, &[])
}

/// Go `importConfigOption` is a one-time import.
/// It is intended to be used to convert a config option to a sysvar.
/// It reads the config value from the tidb-server executing the bootstrap
/// (not guaranteed to be the same on all servers), and writes a message
/// to the error log. The message is important since the behavior is weird
/// (changes to the config file will no longer take effect past this point).
fn import_config_option(
    s: &mut dyn UpgradeSession,
    config_name: &str,
    sv_name: &str,
    val_str: &str,
) -> UpgradeResult {
    let message = format!(
        "{config_name} is now configured by the system variable {sv_name}. One-time importing the value specified in tidb.toml file"
    );
    tidb_logutil::bg_logger().warn(
        &message,
        &[tidb_log::Field::new("value", tidb_log::Value::Str(val_str.to_owned()))],
    );
    // We use insert ignore, since if its a duplicate we don't want to overwrite any user-set values.
    let sql = format!(
        "INSERT IGNORE INTO  mysql.global_variables (`VARIABLE_NAME`, `VARIABLE_VALUE`) VALUES ('{sv_name}', '{val_str}')"
    );
    must_execute(s, &sql, &[])
}

fn bool_to_on_off(b: bool) -> &'static str {
    if b {
        "ON"
    } else {
        "OFF"
    }
}

fn upgrade_to_ver90(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let config = tidb_config::config_tree::config::get_global_config();
    import_config_option(s, "enable-batch-dml", vardef::TIDB_ENABLE_BATCH_DML, bool_to_on_off(config.enable_batch_dml))?;
    import_config_option(s, "mem-quota-query", vardef::TIDB_MEM_QUOTA_QUERY, &config.mem_quota_query.to_string())?;
    import_config_option(s, "query-log-max-len", vardef::TIDB_QUERY_LOG_MAX_LEN, &config.log.query_log_max_len.to_string())?;
    import_config_option(s, "committer-concurrency", vardef::TIDB_COMMITTER_CONCURRENCY, &config.performance.committer_concurrency.to_string())?;
    import_config_option(s, "run-auto-analyze", vardef::TIDB_ENABLE_AUTO_ANALYZE, bool_to_on_off(config.performance.run_auto_analyze))?;
    import_config_option(s, "oom-action", vardef::TIDB_MEM_OOM_ACTION, &config.oom_action)
}

fn upgrade_to_ver91(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let config = tidb_config::config_tree::config::get_global_config();
    import_config_option(s, "prepared-plan-cache.enable", vardef::TIDB_ENABLE_PREP_PLAN_CACHE, bool_to_on_off(config.prepared_plan_cache.enabled))?;

    import_config_option(s, "prepared-plan-cache.capacity", vardef::TIDB_PREP_PLAN_CACHE_SIZE, &(config.prepared_plan_cache.capacity as i64).to_string())?;

    import_config_option(s, "prepared-plan-cache.memory-guard-ratio", vardef::TIDB_PREP_PLAN_CACHE_MEMORY_GUARD_RATIO, &config.prepared_plan_cache.memory_guard_ratio.to_string())
}

fn upgrade_to_ver93(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let config = tidb_config::config_tree::config::get_global_config();
    import_config_option(s, "oom-use-tmp-storage", vardef::TIDB_ENABLE_TMP_STORAGE_ON_OOM, bool_to_on_off(config.oom_use_tmp_storage))
}

fn upgrade_to_ver94(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBMDLVIEW, &[])
}

fn upgrade_to_ver95(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `User_attributes` JSON", &[])
}

fn upgrade_to_ver97(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Check if tidb_opt_range_max_size exists in mysql.GLOBAL_VARIABLES.
    // If not, insert "tidb_opt_range_max_size | 0" since this is the old behavior before we introduce this variable.
    init_global_variable_if_not_exists(s, vardef::TIDB_OPT_RANGE_MAX_SIZE, SqlArg::Signed(0))
}

fn upgrade_to_ver98(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Token_issuer` varchar(255)", &[])
}

/// Go `upgradeToVer99Before`.
pub(crate) fn upgrade_to_ver99_before(s: &mut dyn UpgradeSession) -> UpgradeResult {
    must_execute(
        s,
        "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(vardef::TIDB_ENABLE_MDL.as_bytes()),
            SqlArg::Signed(0),
        ],
    )
}

/// Go `upgradeToVer99After`.
pub(crate) fn upgrade_to_ver99_after(s: &mut dyn UpgradeSession) -> UpgradeResult {
    let sql = format!(
        "UPDATE HIGH_PRIORITY mysql.global_variables SET VARIABLE_VALUE = {} WHERE VARIABLE_NAME = '{}'",
        1,
        vardef::TIDB_ENABLE_MDL
    );
    must_execute(s, &sql, &[])?;
    s.get_store().set_metadata_lock(true)
}

fn upgrade_to_ver100(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let config = tidb_config::config_tree::config::get_global_config();
    let val_str = (config.performance.server_memory_quota as i64).to_string();
    import_config_option(s, "performance.server-memory-quota", vardef::TIDB_SERVER_MEMORY_LIMIT, &val_str)
}

fn upgrade_to_ver101(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_PLAN_REPLAYER_STATUS_TABLE, &[])
}

fn upgrade_to_ver102(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_PLAN_REPLAYER_TASK_TABLE, &[])
}

fn upgrade_to_ver103(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_STATS_TABLE_LOCKED_TABLE, &[])
}

fn upgrade_to_ver104(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info ADD COLUMN IF NOT EXISTS `sql_digest` varchar(64)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info ADD COLUMN IF NOT EXISTS `plan_digest` varchar(64)", &[])
}

/// Go `upgradeToVer105`: for users that upgrade TiDB from a pre-6.0 version, we want to disable tidb cost model2 by default to keep plans unchanged.
fn upgrade_to_ver105(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_COST_MODEL_VERSION, SqlArg::String(b"1"))
}

fn upgrade_to_ver106(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_PASSWORD_HISTORY_TABLE, &[])?;
    do_reentrant_ddl(s, "Alter table mysql.user add COLUMN IF NOT EXISTS `Password_reuse_history` smallint unsigned  DEFAULT NULL AFTER `Create_Tablespace_Priv` ", &[])?;
    do_reentrant_ddl(s, "Alter table mysql.user add COLUMN IF NOT EXISTS `Password_reuse_time` smallint unsigned DEFAULT NULL AFTER `Password_reuse_history`", &[])
}

fn upgrade_to_ver107(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_expired` ENUM('N','Y') NOT NULL DEFAULT 'N'", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_last_changed` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Password_lifetime` SMALLINT UNSIGNED DEFAULT NULL", &[])
}

fn upgrade_to_ver108(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_TI_DBTTLTABLE_STATUS_TABLE, &[])
}

/// Go `upgradeToVer109`: for users that upgrade TiDB from a 6.2-6.4 version, we want to disable tidb gc_aware_memory_track by default.
fn upgrade_to_ver109(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    replace_global_variable(s, vardef::TIDB_ENABLE_GC_AWARE_MEMORY_TRACK, SqlArg::Signed(0))
}

/// Go `upgradeToVer110`: for users that upgrade TiDB from a 5.4-6.4 version, we want to enable tidb tidb_stats_load_pseudo_timeout by default.
fn upgrade_to_ver110(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    replace_global_variable(s, vardef::TIDB_STATS_LOAD_PSEUDO_TIMEOUT, SqlArg::Signed(1))
}

/// Go's `REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);` over
/// `mysql.global_variables`, spelled out in each Go caller.
fn replace_global_variable(s: &mut dyn UpgradeSession, name: &str, value: SqlArg<'_>) -> UpgradeResult {
    must_execute(
        s,
        "REPLACE HIGH_PRIORITY INTO %n.%n VALUES (%?, %?);",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(name.as_bytes()),
            value,
        ],
    )
}

fn upgrade_to_ver130(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_meta_history ADD COLUMN IF NOT EXISTS `source` varchar(40) NOT NULL after `version`;", &[])
}

fn upgrade_to_ver131(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_TI_DBTTLTASK_TABLE, &[])?;
    do_reentrant_ddl(s, metadef::CREATE_TI_DBTTLJOB_HISTORY_TABLE, &[])
}

fn upgrade_to_ver132(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_TI_DBMDLVIEW, &[])
}

fn upgrade_to_ver133(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(
        s,
        "UPDATE HIGH_PRIORITY %n.%n set VARIABLE_VALUE = %? where VARIABLE_NAME = %? and VARIABLE_VALUE = %?;",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(vardef_defaults::DEF_TIDB_SERVER_MEMORY_LIMIT.as_bytes()),
            SqlArg::String(vardef::TIDB_SERVER_MEMORY_LIMIT.as_bytes()),
            SqlArg::String(b"0"),
        ],
    )
}

fn upgrade_to_ver134(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    replace_global_variable(s, tidb_vardef::FOREIGN_KEY_CHECKS, SqlArg::String(b"ON"))?;
    replace_global_variable(s, vardef::TIDB_ENABLE_FOREIGN_KEY, SqlArg::String(b"ON"))?;
    replace_global_variable(s, vardef::TIDB_ENABLE_HISTORICAL_STATS, SqlArg::String(b"ON"))?;
    replace_global_variable(s, vardef::TIDB_ENABLE_PLAN_REPLAYER_CAPTURE, SqlArg::String(b"ON"))?;
    must_execute(
        s,
        "UPDATE HIGH_PRIORITY %n.%n SET VARIABLE_VALUE = %? WHERE VARIABLE_NAME = %? AND VARIABLE_VALUE = %?;",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(b"4"),
            SqlArg::String(vardef::TIDB_STORE_BATCH_SIZE.as_bytes()),
            SqlArg::String(b"0"),
        ],
    )
}

/// Go `upgradeToVer135`: for users that upgrade TiDB from a pre-7.0 version, we want to set tidb_opt_advanced_join_hint to off by default to keep plans unchanged.
fn upgrade_to_ver135(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_OPT_ADVANCED_JOIN_HINT, SqlArg::Bool(false))
}

fn upgrade_to_ver136(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBGLOBAL_TASK_TABLE, &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask DROP INDEX namespace", &[err_cant_drop_field_or_key()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD INDEX idx_task_key(task_key)", &[err_dup_key_name()])
}

fn upgrade_to_ver137(_s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // NOOP, we don't depend on ddl to init the default group due to backward compatible issue.
    Ok(())
}

/// Go `upgradeToVer138`: for users that upgrade TiDB from a version below 7.0, we want to enable tidb tidb_enable_null_aware_anti_join by default.
fn upgrade_to_ver138(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    replace_global_variable(s, vardef::TIDB_OPTIMIZER_ENABLE_NAAJ, SqlArg::String(b"ON"))
}

fn upgrade_to_ver139(_s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    Ok(())
}

fn upgrade_to_ver140(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `task_key` VARCHAR(256) NOT NULL AFTER `id`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD UNIQUE KEY task_key(task_key)", &[err_dup_key_name()])
}

/// Go `upgradeToVer141` sets the value of `tidb_session_plan_cache_size` as `tidb_prepared_plan_cache_size` for compatibility,
/// and update tidb_load_based_replica_read_threshold from 0 to 4.
fn upgrade_to_ver141(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let rows = crate::bootstrap::execute_internal(
        s,
        "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(vardef::TIDB_PREP_PLAN_CACHE_SIZE.as_bytes()),
        ],
    )
    .map_err(|error| error.error_string())?;
    let Some(row) = rows.first() else {
        return Ok(());
    };
    if is_null(row, 0) {
        return Ok(());
    }
    let val = get_string(row, 0);

    must_execute(
        s,
        "INSERT HIGH_PRIORITY IGNORE INTO %n.%n VALUES (%?, %?);",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(vardef::TIDB_SESSION_PLAN_CACHE_SIZE.as_bytes()),
            SqlArg::String(val.as_bytes()),
        ],
    )?;
    let threshold =
        tidb_model::go_duration::format_go_duration(vardef_defaults::DEF_TIDB_LOAD_BASED_REPLICA_READ_THRESHOLD);
    replace_global_variable(
        s,
        vardef::TIDB_LOAD_BASED_REPLICA_READ_THRESHOLD,
        SqlArg::String(threshold.as_bytes()),
    )
}

fn upgrade_to_ver142(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_ENABLE_NON_PREPARED_PLAN_CACHE, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver143(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `error` BLOB", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `error` BLOB", &[err_column_exists()])
}

fn upgrade_to_ver144(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_PLAN_CACHE_INVALIDATION_ON_FRESH_STATS, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver146(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_meta_history ADD INDEX idx_create_time (create_time)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_history ADD INDEX idx_create_time (create_time)", &[err_dup_key_name()])
}

fn upgrade_to_ver167(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `step` INT AFTER `id`", &[err_column_exists()])
}

fn upgrade_to_ver168(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBIMPORT_JOBS_TABLE, &[])
}

fn upgrade_to_ver169(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBRUNAWAY_QUERIES_TABLE, &[])
}

fn upgrade_to_ver170(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBTIMERS_TABLE, &[])
}

fn upgrade_to_ver171(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "ALTER TABLE mysql.tidb_runaway_queries CHANGE COLUMN `tidb_server` `tidb_server` varchar(512)", &[])
}

fn upgrade_to_ver172(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "DROP TABLE IF EXISTS mysql.tidb_runaway_quarantined_watch", &[])?;
    must_execute(s, metadef::CREATE_TI_DBRUNAWAY_WATCH_TABLE, &[])?;
    must_execute(s, metadef::CREATE_TI_DBRUNAWAY_WATCH_DONE_TABLE, &[])
}

fn upgrade_to_ver173(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `summary` JSON", &[err_column_exists()])
}

fn upgrade_to_ver174(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `step` INT AFTER `id`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `error` BLOB", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history DROP INDEX `namespace`", &[err_cant_drop_field_or_key()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD INDEX `idx_task_key`(`task_key`)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD INDEX `idx_state_update_time`(`state_update_time`)", &[err_dup_key_name()])
}

/// Go `upgradeToVer175` updates normalized bindings of `in (?)` to `in (...)` to solve
/// the issue #44298 that bindings for `in (?)` can't work for `in (?, ?, ?)`.
/// After this update, multiple bindings may have the same `original_sql`, but it's OK, and
/// for safety, don't remove duplicated bindings when upgrading.
fn upgrade_to_ver175(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "BEGIN PESSIMISTIC", &[])?;
    let rows = s
        .execute_internal("SELECT original_sql, bind_sql FROM mysql.bind_info WHERE source != 'builtin'")
        .map_err(|error| format!("upgradeToVer175 error: {}", error.error_string()))?;
    let mut update_stmts = Vec::with_capacity(4);
    for row in &rows {
        let original_normalized_sql = get_string(row, 0);
        let bind_sql = get_string(row, 1);
        let new_normalized_sql = tidb_parser::normalize_for_binding(&bind_sql, false);
        // update `in (?)` to `in (...)`
        if original_normalized_sql == new_normalized_sql {
            continue; // no need to update
        }
        // must run those update statements outside this loop, otherwise may cause some concurrency problems,
        // since the current statement over this session has not been finished yet.
        update_stmts.push(format!(
            "UPDATE mysql.bind_info SET original_sql='{new_normalized_sql}' WHERE original_sql='{original_normalized_sql}'"
        ));
    }
    for update_stmt in &update_stmts {
        must_execute(s, update_stmt, &[])?;
    }
    must_execute(s, "COMMIT", &[])
}

fn upgrade_to_ver176(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBGLOBAL_TASK_HISTORY_TABLE, &[])
}

fn upgrade_to_ver177(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // ignore error when upgrading from v7.4 to higher version.
    do_reentrant_ddl(s, metadef::CREATE_DIST_FRAMEWORK_META_TABLE, &[err_table_exists()])?;
    s.set_global_sys_var(vardef::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS, "OFF")
        .map_err(|error| format!("upgradeToVer177 error: {error}"))
}

/// Go `writeDDLTableVersion` writes mDDLTableVersion into mysql.tidb.
fn write_ddl_table_version(s: &mut dyn UpgradeSession) -> UpgradeResult {
    let ddl_table_version = s.get_store().get_ddl_table_version()?;
    must_execute(
        s,
        r#"INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "DDL Table Version. Do not delete.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?"#,
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(DDL_TABLE_VERSION_VAR.as_bytes()),
            SqlArg::Signed(ddl_table_version),
            SqlArg::Signed(ddl_table_version),
        ],
    )
}

fn upgrade_to_ver178(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    write_ddl_table_version(s)
}

fn upgrade_to_ver179(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.global_variables MODIFY COLUMN `VARIABLE_VALUE` varchar(16383)", &[])
}

fn upgrade_to_ver190(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `priority` INT DEFAULT 1 AFTER `state`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `create_time` TIMESTAMP AFTER `priority`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `priority` INT DEFAULT 1 AFTER `state`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `create_time` TIMESTAMP AFTER `priority`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", &[err_column_exists()])?;

    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `concurrency` INT AFTER `checkpoint`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `create_time` TIMESTAMP AFTER `concurrency`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD COLUMN `ordinal` int AFTER `meta`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `concurrency` INT AFTER `checkpoint`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `create_time` TIMESTAMP AFTER `concurrency`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `end_time` TIMESTAMP AFTER `state_update_time`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `ordinal` int AFTER `meta`", &[err_column_exists()])?;

    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD INDEX idx_exec_id(exec_id)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask ADD UNIQUE INDEX uk_task_key_step_ordinal(task_key, step, ordinal)", &[err_dup_key_name()])?;

    do_reentrant_ddl(s, "ALTER TABLE mysql.dist_framework_meta ADD COLUMN `cpu_count` INT DEFAULT 0 AFTER `role`", &[err_column_exists()])?;

    do_reentrant_ddl(s, "ALTER TABLE mysql.dist_framework_meta MODIFY COLUMN `host` VARCHAR(261)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask MODIFY COLUMN `exec_id` VARCHAR(261)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history MODIFY COLUMN `exec_id` VARCHAR(261)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task MODIFY COLUMN `dispatcher_id` VARCHAR(261)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history MODIFY COLUMN `dispatcher_id` VARCHAR(261)", &[])
}

fn upgrade_to_ver191(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let sql = format!(
        "INSERT HIGH_PRIORITY IGNORE INTO mysql.global_variables VALUES('{}', '{}')",
        vardef::TIDB_TXN_MODE,
        tidb_vardef::txn_mode::OPTIMISTIC_TXN_MODE
    );
    must_execute(s, &sql, &[])
}

fn upgrade_to_ver192(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_REQUEST_UNIT_BY_GROUP_TABLE, &[])
}

fn upgrade_to_ver193(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_TI_DBMDLVIEW, &[])
}

fn upgrade_to_ver194(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "DROP TABLE IF EXISTS mysql.load_data_jobs", &[])
}

fn upgrade_to_ver195(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::DROP_MYSQL_INDEX_USAGE_TABLE, &[])
}

fn upgrade_to_ver196(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN target_scope VARCHAR(256) DEFAULT '' AFTER `step`;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN target_scope VARCHAR(256) DEFAULT '' AFTER `step`;", &[err_column_exists()])
}

fn upgrade_to_ver197(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_TI_DBMDLVIEW, &[])
}

fn upgrade_to_ver198(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_mdl_info ADD COLUMN owner_id VARCHAR(64) NOT NULL DEFAULT '';", &[err_column_exists()])
}

fn upgrade_to_ver209(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_RESOURCE_CONTROL_STRICT_MODE, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver210(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Check if tidb_analyze_column_options exists in mysql.GLOBAL_VARIABLES.
    // If not, set tidb_analyze_column_options to ALL since this is the old behavior before we introduce this variable.
    init_global_variable_if_not_exists(s, vardef::TIDB_ANALYZE_COLUMN_OPTIONS, SqlArg::String(b"ALL"))?;

    // Check if tidb_opt_projection_push_down exists in mysql.GLOBAL_VARIABLES.
    // If not, set tidb_opt_projection_push_down to Off since this is the old behavior before we introduce this variable.
    init_global_variable_if_not_exists(s, vardef::TIDB_OPT_PROJECTION_PUSH_DOWN, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver211(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_background_subtask_history ADD COLUMN `summary` JSON", &[err_column_exists()])
}

fn upgrade_to_ver212(s: &mut dyn UpgradeSession, ver: i64) -> UpgradeResult {
    // need to ensure curVersion has the column before rename.
    // version169 created `tidb_runaway_queries` table
    // version172 created `tidb_runaway_watch` and `tidb_runaway_watch_done` tables
    if ver < VERSION172 {
        return Ok(());
    }
    // version212 changed a lots of runaway related table.
    // 1. switchGroup: add column `switch_group_name` to `mysql.tidb_runaway_watch` and `mysql.tidb_runaway_watch_done`.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_watch ADD COLUMN `switch_group_name` VARCHAR(32) DEFAULT '' AFTER `action`;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_watch_done ADD COLUMN `switch_group_name` VARCHAR(32) DEFAULT '' AFTER `action`;", &[err_column_exists()])?;
    // 2. modify column `plan_digest` type, modify column `time` to `start_time,
    // modify column `original_sql` to `sample_sql` and unique union key to `mysql.tidb_runaway_queries`.
    // add column `sql_digest`.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_queries ADD COLUMN `sql_digest` varchar(64) DEFAULT '' AFTER `original_sql`;", &[err_column_exists()])?;
    // add column `repeats`.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_queries ADD COLUMN `repeats` int DEFAULT 1 AFTER `time`;", &[err_column_exists()])?;
    // rename column name from `time` to `start_time`, will auto rebuild the index.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_queries RENAME COLUMN `time` TO `start_time`", &[err_column_not_exists()])?;
    // rename column `original_sql` to `sample_sql`.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_queries RENAME COLUMN `original_sql` TO `sample_sql`", &[err_column_not_exists()])?;
    // modify column type of `plan_digest`.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_queries MODIFY COLUMN `plan_digest` varchar(64) DEFAULT '';", &[err_column_exists()])?;
    // 3. modify column length of `action`.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_queries MODIFY COLUMN `action` VARCHAR(64) NOT NULL;", &[err_column_exists()])?;
    // 4. add column `rule` to `mysql.tidb_runaway_watch`, `mysql.tidb_runaway_watch_done` and `mysql.tidb_runaway_queries`.
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_watch ADD COLUMN `rule` VARCHAR(512) DEFAULT '' AFTER `switch_group_name`;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_watch_done ADD COLUMN `rule` VARCHAR(512) DEFAULT '' AFTER `switch_group_name`;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_queries ADD COLUMN `rule` VARCHAR(512) DEFAULT '' AFTER `tidb_server`;", &[err_column_exists()])
}

fn upgrade_to_ver213(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBPITRIDMAP_TABLE, &[])
}

fn upgrade_to_ver214(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_INDEX_ADVISOR_RESULTS_TABLE, &[])?;
    must_execute(s, metadef::CREATE_TI_DBKERNEL_OPTIONS_TABLE, &[])
}

fn upgrade_to_ver215(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_ENABLE_INL_JOIN_INNER_MULTI_PATTERN, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver216(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='' WHERE VARIABLE_NAME = 'tidb_scatter_region' AND VARIABLE_VALUE = 'OFF'", &[])?;
    must_execute(s, "UPDATE mysql.global_variables SET VARIABLE_VALUE='table' WHERE VARIABLE_NAME = 'tidb_scatter_region' AND VARIABLE_VALUE = 'ON'", &[])
}

fn upgrade_to_ver217(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // If tidb_schema_cache_size does not exist, insert a record and set the value to 0
    // Otherwise do nothing.
    must_execute(s, "INSERT IGNORE INTO mysql.global_variables VALUES ('tidb_schema_cache_size', 0)", &[])
}

fn upgrade_to_ver218(_s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // empty, just make lint happy.
    Ok(())
}

fn upgrade_to_ver239(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN modify_params json AFTER `error`;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN modify_params json AFTER `error`;", &[err_column_exists()])
}

/// Go `addAnalyzeJobsSchemaTableStateIndex` is a DDL statement that adds an index on (table_schema, table_name, state)
/// columns to mysql.analyze_jobs table. This index is currently unused since queries filter on partition_name='',
/// even for non-partitioned tables. It is kept for potential future optimization where queries could use this
/// simpler index directly for non-partitioned tables.
const ADD_ANALYZE_JOBS_SCHEMA_TABLE_STATE_INDEX: &str =
    "ALTER TABLE mysql.analyze_jobs ADD INDEX idx_schema_table_state (table_schema, table_name, state)";
/// Go `addAnalyzeJobsSchemaTablePartitionStateIndex` adds an index on (table_schema, table_name, partition_name, state) to mysql.analyze_jobs
const ADD_ANALYZE_JOBS_SCHEMA_TABLE_PARTITION_STATE_INDEX: &str = "ALTER TABLE mysql.analyze_jobs ADD INDEX idx_schema_table_partition_state (table_schema, table_name, partition_name, state)";

fn upgrade_to_ver240(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, ADD_ANALYZE_JOBS_SCHEMA_TABLE_STATE_INDEX, &[err_dup_key_name()])?;
    do_reentrant_ddl(s, ADD_ANALYZE_JOBS_SCHEMA_TABLE_PARTITION_STATE_INDEX, &[err_dup_key_name()])
}

fn upgrade_to_ver241(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD INDEX i_user (user)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.global_priv ADD INDEX i_user (user)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.db ADD INDEX i_user (user)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tables_priv ADD INDEX i_user (user)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.columns_priv ADD INDEX i_user (user)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.global_grants ADD INDEX i_user (user)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.default_roles ADD INDEX i_user (user)", &[err_dup_key_name()])
}

/// Go `writeClusterID` writes cluster id into mysql.tidb.
fn write_cluster_id(s: &mut dyn UpgradeSession) -> UpgradeResult {
    let cluster_id = s.get_store().get_cluster_id();

    must_execute(
        s,
        r#"INSERT HIGH_PRIORITY INTO %n.%n VALUES (%?, %?, "TiDB Cluster ID.") ON DUPLICATE KEY UPDATE VARIABLE_VALUE= %?"#,
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"tidb"),
            SqlArg::String(CLUSTER_ID_VAR.as_bytes()),
            SqlArg::Unsigned(cluster_id),
            SqlArg::Unsigned(cluster_id),
        ],
    )
}

fn upgrade_to_ver242(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    write_cluster_id(s)?;
    must_execute(s, metadef::CREATE_TI_DBWORKLOAD_VALUES_TABLE, &[])
}

fn upgrade_to_ver243(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN max_node_count INT DEFAULT 0 AFTER `modify_params`;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN max_node_count INT DEFAULT 0 AFTER `modify_params`;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN extra_params json AFTER max_node_count;", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN extra_params json AFTER max_node_count;", &[err_column_exists()])
}

fn upgrade_to_ver244(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.user ADD COLUMN IF NOT EXISTS `Max_user_connections` INT UNSIGNED NOT NULL DEFAULT 0 AFTER `Password_lifetime`", &[])
}

fn upgrade_to_ver245(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN original_sql LONGTEXT NOT NULL", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN bind_sql LONGTEXT NOT NULL", &[])
}

fn upgrade_to_ver246(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // log duplicated digests that will be set to null.
    let rows = s
        .execute_internal(
            "select plan_digest, sql_digest from mysql.bind_info group by plan_digest, sql_digest having count(1) > 1",
        )
        .map_err(|error| {
            format!("failed to get duplicated plan and sql digests: {}", error.error_string())
        })?;
    let mut duplicated_digests = HashSet::new();
    for row in &rows {
        let plan_digest = get_string(row, 0);
        let sql_digest = get_string(row, 1);
        duplicated_digests.insert(format!("{sql_digest}, {plan_digest}"));
    }
    if !duplicated_digests.is_empty() {
        let digest_list = duplicated_digests
            .iter()
            .map(|digest| format!("({digest})"))
            .collect::<Vec<_>>();
        tidb_logutil::bg_logger().warn(
            &format!(
                "set the following (plan digest, sql digest) in mysql.bind_info to null for adding new unique index: {}",
                digest_list.join(", ")
            ),
            &[],
        );
    }

    // to avoid the failure of adding the unique index, remove duplicated rows on these 2 digest columns first.
    // in most cases, there should be no duplicated rows, since now we only store one binding for each sql_digest.
    // compared with upgrading failure, it's OK to set these 2 columns to null.
    do_reentrant_ddl(
        s,
        "UPDATE mysql.bind_info SET plan_digest=null, sql_digest=null
                       WHERE (plan_digest, sql_digest) in (
                         select plan_digest, sql_digest from mysql.bind_info
                         group by plan_digest, sql_digest having count(1) > 1)",
        &[],
    )?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN sql_digest VARCHAR(64) DEFAULT NULL", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info MODIFY COLUMN plan_digest VARCHAR(64) DEFAULT NULL", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info ADD UNIQUE INDEX digest_index(plan_digest, sql_digest)", &[err_dup_key_name()])
}

fn upgrade_to_ver247(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.stats_meta ADD COLUMN last_stats_histograms_version bigint unsigned DEFAULT NULL", &[err_column_exists()])
}

fn upgrade_to_ver248(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_pitr_id_map ADD COLUMN restore_id BIGINT NOT NULL DEFAULT 0", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_pitr_id_map DROP PRIMARY KEY", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_pitr_id_map ADD PRIMARY KEY(restore_id, restored_ts, upstream_cluster_id, segment_id)", &[])
}

fn upgrade_to_ver249(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, metadef::CREATE_TI_DBRESTORE_REGISTRY_TABLE, &[])
}

fn upgrade_to_ver250(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD COLUMN `keyspace` varchar(64) DEFAULT '' AFTER `extra_params`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task ADD INDEX idx_keyspace(keyspace)", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD COLUMN `keyspace` varchar(64) DEFAULT '' AFTER `extra_params`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_global_task_history ADD INDEX idx_keyspace(keyspace)", &[err_dup_key_name()])
}

fn upgrade_to_ver251(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_import_jobs ADD COLUMN `group_key` VARCHAR(256) NOT NULL DEFAULT '' AFTER `created_by`", &[err_column_exists()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_import_jobs ADD INDEX idx_group_key(group_key)", &[err_dup_key_name()])
}

fn upgrade_to_ver252(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info CHANGE create_time create_time TIMESTAMP(6)", &[])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info CHANGE update_time update_time TIMESTAMP(6)", &[])
}

fn upgrade_to_ver253(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.bind_info ADD COLUMN last_used_date DATE DEFAULT NULL AFTER `plan_digest`", &[err_column_exists()])
}

fn upgrade_to_ver254(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_watch ADD INDEX idx_start_time(start_time) COMMENT 'accelerate the speed when syncing new watch records'", &[err_dup_key_name()])?;
    do_reentrant_ddl(s, "ALTER TABLE mysql.tidb_runaway_watch_done ADD INDEX idx_done_time(done_time) COMMENT 'accelerate the speed when syncing done watch records'", &[err_dup_key_name()])
}

/// Go `sqlexec.ExecSQL` of `SELECT VARIABLE_VALUE FROM mysql.global_variables
/// WHERE VARIABLE_NAME=%?;`.
fn select_global_variable(s: &mut dyn UpgradeSession, name: &str) -> Result<Vec<Vec<Datum>>, String> {
    crate::bootstrap::execute_internal(
        s,
        "SELECT VARIABLE_VALUE FROM %n.%n WHERE VARIABLE_NAME=%?;",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(name.as_bytes()),
        ],
    )
    .map_err(|error| error.error_string())
}

fn upgrade_to_ver255(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let rows = select_global_variable(s, vardef::TIDB_ANALYZE_VERSION)?;
    // The value should normally never be null, but check defensively to avoid a potential panic.
    let Some(row) = rows.first() else {
        return Ok(());
    };
    if is_null(row, 0) {
        return Ok(());
    }

    let old_value = get_string(row, 0);
    if old_value != "1" {
        return Ok(());
    }

    // The current default value of tidb_analyze_version is 2.
    let new_value = vardef_defaults::DEF_TIDB_ANALYZE_VERSION.to_string();
    tidb_logutil::bg_logger().warn(
        &format!("Rewriting persisted tidb_analyze_version from {old_value} to {new_value} during upgrade"),
        &[],
    );
    must_execute(
        s,
        "UPDATE HIGH_PRIORITY %n.%n SET VARIABLE_VALUE=%? WHERE VARIABLE_NAME=%? AND VARIABLE_VALUE=%?;",
        &[
            SqlArg::String(b"mysql"),
            SqlArg::String(b"global_variables"),
            SqlArg::String(new_value.as_bytes()),
            SqlArg::String(vardef::TIDB_ANALYZE_VERSION.as_bytes()),
            SqlArg::String(old_value.as_bytes()),
        ],
    )
}

fn upgrade_to_ver256(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_PLAN_CACHE_SKIP_STATS_ON_BINDING, SqlArg::String(b"ON"))
}

fn upgrade_to_ver257(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Keep old behavior for upgraded clusters.
    init_global_variable_if_not_exists(s, vardef::TIDB_ENABLE_NO_BACKSLASH_ESCAPES_IN_LIKE, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver258(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    let rows = select_global_variable(s, vardef::TIDB_DIST_SQL_SCAN_CONCURRENCY)?;
    let Some(row) = rows.first() else {
        return Ok(());
    };
    let value = get_string(row, 0);
    if value.is_empty() {
        return Ok(());
    }
    init_global_variable_if_not_exists(
        s,
        vardef::TIDB_ANALYZE_DIST_SQL_SCAN_CONCURRENCY,
        SqlArg::String(value.as_bytes()),
    )
}

fn upgrade_to_ver259(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    init_global_variable_if_not_exists(s, vardef::TIDB_IGNORE_INLIST_PLAN_DIGEST, SqlArg::String(b"OFF"))
}

fn upgrade_to_ver260(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    must_execute(s, metadef::CREATE_TI_DBMASKING_POLICY_TABLE, &[])
}

fn upgrade_to_ver261(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // the prior default behavior is "0.8", keep it for compatibility for old clusters.
    init_global_variable_if_not_exists(s, vardef::TIDB_DEFAULT_STR_MATCH_SELECTIVITY, SqlArg::String(b"0.8"))
}

/// Go `bindingDigestUpdate`.
struct BindingDigestUpdate {
    row_id: i64,
    original_sql: String,
    sql_digest: String,
    duplicate: bool,
}

fn upgrade_to_ver262(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    use tidb_bindinfo::binding::STATUS_DELETED;
    // Refresh persisted binding digests after the #67363 normalization change.
    // Duplicate detection keeps the first row scanned for each target digest
    // pair, so scan newest bindings first.
    let rows = s
        .execute_internal(
            "SELECT _tidb_rowid, bind_sql, default_db, charset, collation, plan_digest
		FROM mysql.bind_info
		WHERE source != 'builtin'
		ORDER BY update_time DESC, create_time DESC, _tidb_rowid DESC",
        )
        .map_err(|error| format!("upgradeToVer262 error: {}", error.error_string()))?;

    let mut updates: Vec<BindingDigestUpdate> = Vec::new();
    let mut invalid_binding_row_ids: Vec<i64> = Vec::new();
    // Go `bindingDigestPair`: (sql digest, plan digest).
    let mut seen_digest_pair: HashSet<(String, String)> = HashSet::new();
    for row in &rows {
        let row_id = row[0].get_int64();
        let binding_sql = get_string(row, 1);
        let default_db = get_string(row, 2);
        let plan_digest_not_null = !is_null(row, 5);
        let plan_digest = if plan_digest_not_null {
            get_string(row, 5)
        } else {
            String::new()
        };

        let stmt = match tidb_parser::parse(&binding_sql) {
            Ok(stmt) => stmt,
            Err(parse_err) => {
                tidb_logutil::bg_logger().warn(
                    "skip refreshing binding digest because bind_sql cannot be parsed",
                    &[
                        tidb_log::Field::new("bind_sql", tidb_log::Value::Str(binding_sql.clone())),
                        tidb_logutil::zap_error(&parse_err.message),
                    ],
                );
                if plan_digest_not_null {
                    invalid_binding_row_ids.push(row_id);
                }
                continue;
            }
        };
        let (original_sql, sql_digest) =
            tidb_bindinfo::binding::normalize_stmt_for_binding(&stmt, &default_db);
        if original_sql.is_empty() || sql_digest.is_empty() {
            tidb_logutil::bg_logger().warn(
                "skip refreshing binding digest because normalized binding SQL is empty",
                &[tidb_log::Field::new("bind_sql", tidb_log::Value::Str(binding_sql.clone()))],
            );
            if plan_digest_not_null {
                invalid_binding_row_ids.push(row_id);
            }
            continue;
        }
        let mut update = BindingDigestUpdate {
            row_id,
            original_sql,
            sql_digest,
            duplicate: false,
        };
        if plan_digest_not_null {
            // Avoid duplicated key error on the unique index (plan_digest, sql_digest).
            let key = (update.sql_digest.clone(), plan_digest);
            if !seen_digest_pair.insert(key) {
                update.duplicate = true;
            }
        }
        updates.push(update);
    }

    // Update rows independently to avoid one large bootstrap transaction.
    for row_id in invalid_binding_row_ids {
        // Set invalid bindings' plan_digest to null to avoid duplicated key error in unique index (sql_digest, plan_digest).
        must_execute(
            s,
            "UPDATE HIGH_PRIORITY mysql.bind_info SET plan_digest=NULL WHERE _tidb_rowid=%?",
            &[SqlArg::Signed(row_id)],
        )?;
    }
    // Set all duplicated rows to NULL to avoid duplicated key error on the unique index (plan_digest, sql_digest).
    for update in updates.iter().filter(|update| update.duplicate) {
        must_execute(
            s,
            "UPDATE HIGH_PRIORITY mysql.bind_info SET status=%?, sql_digest=NULL, plan_digest=NULL WHERE _tidb_rowid=%?",
            &[SqlArg::String(STATUS_DELETED.as_bytes()), SqlArg::Signed(update.row_id)],
        )?;
    }
    // Actually update the original_sql and sql_digest for non-duplicated rows.
    for update in &updates {
        if update.duplicate || update.original_sql.is_empty() {
            continue;
        }
        must_execute(
            s,
            "UPDATE HIGH_PRIORITY mysql.bind_info SET original_sql=%?, sql_digest=%? WHERE _tidb_rowid=%?",
            &[
                SqlArg::String(update.original_sql.as_bytes()),
                SqlArg::String(update.sql_digest.as_bytes()),
                SqlArg::Signed(update.row_id),
            ],
        )?;
    }
    Ok(())
}

fn upgrade_to_ver263(s: &mut dyn UpgradeSession, _ver: i64) -> UpgradeResult {
    // Fresh clusters materialize these rows during bootstrap, but upgraded clusters can miss them.
    // Backfill only absent rows so @@global reads use defaults while preserving user-set values.
    init_global_variable_if_not_exists(
        s,
        vardef::TIDB_ANALYZE_DEFAULT_NUM_BUCKETS,
        SqlArg::Signed(vardef_defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_BUCKETS),
    )?;
    init_global_variable_if_not_exists(
        s,
        vardef::TIDB_ANALYZE_DEFAULT_NUM_TOP_N,
        SqlArg::Signed(vardef_defaults::DEF_TIDB_ANALYZE_DEFAULT_NUM_TOP_N),
    )
}
