// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! MySQL protocol constants, SQL modes, release versions, and priorities.

#![allow(non_upper_case_globals)]

use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::sync::{OnceLock, RwLock};

use tidb_error::mysql::{errcode::ErrWrongValueForVar, FormatArg, SqlError};

const MYSQL_COMPATIBILITY_VERSION: &str = "8.0.11";
/// Fixed separator embedded in TiDB's MySQL-compatible server version.
pub const VersionSeparator: &str = "-TiDB-";
const TIDBX_RELEASE_VERSION_PREFIX: &str = "CLOUD.";
/// Classic development-build placeholder.
pub const LEGACY_TIDB_RELEASE_VERSION_PLACEHOLDER: &str = "v8.4.0-this-is-a-placeholder";
/// Next-generation development-build placeholder.
pub const TIDBX_PLACEHOLDER_RELEASE_VERSION: &str = "v26.3.0-this-is-a-placeholder";
/// Build-time default release version. Cargo/build environments may inject the
/// same value that the Go linker writes into `TiDBReleaseVersion`.
pub const TIDB_RELEASE_VERSION: &str = match option_env!("TIDB_RELEASE_VERSION") {
    Some(version) => version,
    None => LEGACY_TIDB_RELEASE_VERSION_PLACEHOLDER,
};
/// Classic development-build server-version placeholder.
/// Runtime consumers must use [`runtime_versions`], whose default incorporates
/// an injected [`TIDB_RELEASE_VERSION`].
pub const LEGACY_SERVER_VERSION_PLACEHOLDER: &str = "8.0.11-TiDB-v8.4.0-this-is-a-placeholder";
/// Earliest accepted next-generation release year.
pub const TiDBXVerMinYear: u64 = 2025;
/// Latest accepted next-generation release year.
pub const TiDBXVerMaxYear: u64 = 2099;

/// Process-wide versions consumed by handshakes, status reporting, and SQL
/// builtins. A single lock keeps release/server reads and updates coherent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeVersions {
    /// TiDB release version, corresponding to Go's mutable
    /// `TiDBReleaseVersion` package variable.
    pub tidb_release_version: String,
    /// MySQL-compatible server version, corresponding to Go's mutable
    /// `ServerVersion` package variable.
    pub server_version: String,
}

impl RuntimeVersions {
    fn build_default() -> Self {
        Self {
            tidb_release_version: TIDB_RELEASE_VERSION.to_owned(),
            server_version: format!(
                "{MYSQL_COMPATIBILITY_VERSION}{VersionSeparator}{TIDB_RELEASE_VERSION}"
            ),
        }
    }
}

static RUNTIME_VERSIONS: OnceLock<RwLock<RuntimeVersions>> = OnceLock::new();

fn runtime_version_state() -> &'static RwLock<RuntimeVersions> {
    RUNTIME_VERSIONS.get_or_init(|| RwLock::new(RuntimeVersions::build_default()))
}

/// Returns one coherent snapshot of the mutable process-wide versions.
#[must_use]
pub fn runtime_versions() -> RuntimeVersions {
    runtime_version_state()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

/// Atomically replaces both process-wide versions.
///
/// Keeping the pair update atomic avoids a handshake observing a release from
/// one configuration and a server version from another.
pub fn set_runtime_versions(release_version: impl Into<String>, server_version: impl Into<String>) {
    *runtime_version_state()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = RuntimeVersions {
        tidb_release_version: release_version.into(),
        server_version: server_version.into(),
    };
}

/// Restores the build-injected defaults. This is primarily useful for
/// embedding/tests that repeatedly construct a server in one process.
pub fn reset_runtime_versions() {
    *runtime_version_state()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = RuntimeVersions::build_default();
}

/// Rewrites only the classic development placeholder into its next-gen form.
#[must_use]
pub fn normalize_tidb_release_version_for_next_gen(version: &str) -> &str {
    if version == LEGACY_TIDB_RELEASE_VERSION_PLACEHOLDER {
        TIDBX_PLACEHOLDER_RELEASE_VERSION
    } else {
        version
    }
}

/// Exact validation failure returned by next-generation version conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidReleaseVersion(String);
impl fmt::Display for InvalidReleaseVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl Error for InvalidReleaseVersion {}

fn valid_semver_identifier(identifier: &str) -> bool {
    identifier.is_empty()
        || identifier.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        })
}

fn parse_coreos_semver(raw: &str) -> Option<(i64, i64, i64, &str)> {
    // This deliberately follows coreos/go-semver v0.3.1's Set method rather
    // than the stricter Rust semver crate: split metadata first, then
    // prerelease, accept leading zeroes, and parse all numeric fields as i64.
    let (without_metadata, metadata) = raw.split_once('+').map_or((raw, ""), |parts| parts);
    let (version, prerelease) = without_metadata
        .split_once('-')
        .map_or((without_metadata, ""), |parts| parts);
    if !valid_semver_identifier(prerelease) || !valid_semver_identifier(metadata) {
        return None;
    }
    let mut parts = version.splitn(3, '.');
    let major = parts.next()?.parse::<i64>().ok()?;
    let minor = parts.next()?.parse::<i64>().ok()?;
    let patch = parts.next()?.parse::<i64>().ok()?;
    Some((major, minor, patch, prerelease))
}

/// Converts `vYY.month.patch[-pre]` into `CLOUD.YYYYMM.patch[-pre]`.
pub fn build_tidbx_release_version(release: &str) -> Result<String, InvalidReleaseVersion> {
    let Some(raw) = release.strip_prefix('v') else {
        return Err(InvalidReleaseVersion(format!(
            "invalid TiDB release version {release:?}, should start with 'v'"
        )));
    };
    let (major, minor, patch, prerelease) = parse_coreos_semver(raw).ok_or_else(|| {
        InvalidReleaseVersion(format!(
            "invalid TiDB release version {release:?}, expect a semantic version"
        ))
    })?;
    // Validate before addition so hostile or simply malformed i64 input cannot
    // overflow in debug or release builds.
    let major = u64::try_from(major).ok();
    let min_major = TiDBXVerMinYear.saturating_sub(2000);
    let max_major = TiDBXVerMaxYear.saturating_sub(2000);
    if !major.is_some_and(|major| (min_major..=max_major).contains(&major))
        || !(1..=12).contains(&minor)
    {
        return Err(InvalidReleaseVersion(format!("invalid TiDB release version {release:?}, the semantic version part should be in [2-digit-year].[month].[fix-version]-[xxx] format")));
    }
    let year = 2000_u64 + major.expect("validated release-year offset");
    debug_assert!((TiDBXVerMinYear..=TiDBXVerMaxYear).contains(&year));
    let pre = if prerelease.is_empty() {
        String::new()
    } else {
        format!("-{prerelease}")
    };
    Ok(format!(
        "{TIDBX_RELEASE_VERSION_PREFIX}{year}{:02}.{}{pre}",
        minor, patch
    ))
}

/// Converts a next-generation release into the MySQL server-version string.
pub fn build_tidbx_server_version(release: &str) -> Result<String, InvalidReleaseVersion> {
    Ok(format!(
        "{MYSQL_COMPATIBILITY_VERSION}{VersionSeparator}{}",
        build_tidbx_release_version(release)?
    ))
}

macro_rules! constants {
    ($ty:ty; $($name:ident = $value:expr;)+) => {$(
        #[doc = concat!("Source-compatible `", stringify!($name), "` constant.")]
        pub const $name: $ty = $value;
    )+};
}
constants! { u8;
    OKHeader = 0x00; ErrHeader = 0xff; EOFHeader = 0xfe; LocalInFileHeader = 0xfb;
    AuthSwitchRequest = 0xfe; TypeNoCache = 0xff;
}
constants! { u16;
    ServerStatusInTrans = 0x0001; ServerStatusAutocommit = 0x0002;
    ServerMoreResultsExists = 0x0008; ServerStatusNoGoodIndexUsed = 0x0010;
    ServerStatusNoIndexUsed = 0x0020; ServerStatusCursorExists = 0x0040;
    ServerStatusLastRowSend = 0x0080; ServerStatusDBDropped = 0x0100;
    ServerStatusNoBackslashEscaped = 0x0200; ServerStatusMetadataChanged = 0x0400;
    ServerStatusWasSlow = 0x0800; ServerPSOutParams = 0x1000;
}
/// Returns whether a server-status word advertises an existing cursor.
#[must_use]
pub const fn has_cursor_exists_flag(status: u16) -> bool {
    status & ServerStatusCursorExists != 0
}

constants! { usize;
    MaxPayloadLen = (1 << 24) - 1; MaxTableNameLength = 64;
    MaxDatabaseNameLength = 64; MaxColumnNameLength = 64; MaxKeyParts = 16;
    MaxIndexIdentifierLen = 64; MaxForeignKeyIdentifierLen = 64;
    MaxConstraintIdentifierLen = 64; MaxViewIdentifierLen = 64;
    MaxAliasIdentifierLen = 256; MaxUserDefinedVariableLen = 64; ErrTextLength = 80;
}

constants! { u8;
    ComSleep = 0; ComQuit = 1; ComInitDB = 2; ComQuery = 3; ComFieldList = 4;
    ComCreateDB = 5; ComDropDB = 6; ComRefresh = 7; ComShutdown = 8;
    ComStatistics = 9; ComProcessInfo = 10; ComConnect = 11; ComProcessKill = 12;
    ComDebug = 13; ComPing = 14; ComTime = 15; ComDelayedInsert = 16;
    ComChangeUser = 17; ComBinlogDump = 18; ComTableDump = 19; ComConnectOut = 20;
    ComRegisterSlave = 21; ComStmtPrepare = 22; ComStmtExecute = 23;
    ComStmtSendLongData = 24; ComStmtClose = 25; ComStmtReset = 26;
    ComSetOption = 27; ComStmtFetch = 28; ComDaemon = 29;
    ComBinlogDumpGtid = 30; ComResetConnection = 31; ComEnd = 32;
}

constants! { u32;
    ClientLongPassword = 1 << 0; ClientFoundRows = 1 << 1; ClientLongFlag = 1 << 2;
    ClientConnectWithDB = 1 << 3; ClientNoSchema = 1 << 4; ClientCompress = 1 << 5;
    ClientODBC = 1 << 6; ClientLocalFiles = 1 << 7; ClientIgnoreSpace = 1 << 8;
    ClientProtocol41 = 1 << 9; ClientInteractive = 1 << 10; ClientSSL = 1 << 11;
    ClientIgnoreSigpipe = 1 << 12; ClientTransactions = 1 << 13; ClientReserved = 1 << 14;
    ClientSecureConnection = 1 << 15; ClientMultiStatements = 1 << 16;
    ClientMultiResults = 1 << 17; ClientPSMultiResults = 1 << 18; ClientPluginAuth = 1 << 19;
    ClientConnectAtts = 1 << 20; ClientPluginAuthLenencClientData = 1 << 21;
    ClientHandleExpiredPasswords = 1 << 22; ClientSessionTrack = 1 << 23;
    ClientDeprecateEOF = 1 << 24; ClientOptionalResultsetMetadata = 1 << 25;
    ClientZstdCompressionAlgorithm = 1 << 26;
}

macro_rules! strings {
    ($($name:ident = $value:literal;)+) => {$(
        #[doc = concat!("Source-compatible `", stringify!($name), "` value.")]
        pub const $name: &str = $value;
    )+};
}
strings! {
    AuthNativePassword = "mysql_native_password"; AuthCachingSha2Password = "caching_sha2_password";
    AuthTiDBSM3Password = "tidb_sm3_password"; AuthMySQLClearPassword = "mysql_clear_password";
    AuthSocket = "auth_socket"; AuthTiDBSessionToken = "tidb_session_token";
    AuthTiDBAuthToken = "tidb_auth_token"; AuthLDAPSimple = "authentication_ldap_simple";
    AuthLDAPSASL = "authentication_ldap_sasl"; SystemDB = "mysql"; SysDB = "sys";
    GlobalPrivTable = "global_priv"; UserTable = "User"; DBTable = "DB";
    TablePrivTable = "Tables_priv"; ColumnPrivTable = "Columns_priv";
    GlobalVariablesTable = "GLOBAL_VARIABLES"; GlobalStatusTable = "GLOBAL_STATUS";
    TiDBTable = "tidb"; RoleEdgeTable = "role_edges"; DefaultRoleTable = "default_roles";
    PasswordHistoryTable = "password_history"; WorkloadSchema = "workload_schema";
}

constants! { u64;
    NotFixedDec = 31; MaxIntWidth = 20; MaxRealWidth = 23; MaxFloatingTypeScale = 30;
    MaxFloatingTypeWidth = 255; MaxDecimalScale = 30; MaxDecimalWidth = 65;
    MaxDateWidth = 10; MaxDatetimeWidthNoFsp = 19; MaxDatetimeWidthWithFsp = 26;
    MaxDatetimeFullWidth = 29; MaxDurationWidthNoFsp = 10; MaxDurationWidthWithFsp = 17;
    MaxBlobWidth = 16_777_216; MaxLongBlobWidth = 4_294_967_295; MaxBitDisplayWidth = 64;
    MaxFloatPrecisionLength = 24; MaxDoublePrecisionLength = 53;
    MaxFieldCharLength = 255; MaxFieldVarCharLength = 65_535; MaxTypeSetMembers = 64;
    PWDHashLen = 40; SHAPWDHashLen = 70; SM3PWDHashLen = 70;
    PartitionCountLimit = 8192; CursorTypeReadOnly = 1; CursorTypeForUpdate = 2;
    CursorTypeScrollable = 4; ZlibCompressDefaultLevel = 6;
}
strings! {
    DefaultSQLMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
    PrimaryKeyName = "PRIMARY";
    DefaultDecimal = "99999999999999999999999999999999999999999999999999999999999999999";
}
constants! { u8; CompressionNone = 0; CompressionZlib = 1; CompressionZstd = 2; }

/// Supported authentication plugins, in source order.
pub const DEFAULT_AUTH_PLUGINS: &[&str] = &[
    AuthNativePassword,
    AuthCachingSha2Password,
    AuthTiDBSM3Password,
    AuthLDAPSASL,
    AuthLDAPSimple,
    AuthSocket,
    AuthTiDBSessionToken,
    AuthTiDBAuthToken,
    AuthMySQLClearPassword,
];

/// Complete source command-to-diagnostic-name map.
pub const COMMAND_NAMES: &[(u8, &str)] = &[
    (ComSleep, "Sleep"),
    (ComQuit, "Quit"),
    (ComInitDB, "Init DB"),
    (ComQuery, "Query"),
    (ComFieldList, "Field List"),
    (ComCreateDB, "Create DB"),
    (ComDropDB, "Drop DB"),
    (ComRefresh, "Refresh"),
    (ComShutdown, "Shutdown"),
    (ComStatistics, "Statistics"),
    (ComProcessInfo, "Processlist"),
    (ComConnect, "Connect"),
    (ComProcessKill, "Kill"),
    (ComDebug, "Debug"),
    (ComPing, "Ping"),
    (ComTime, "Time"),
    (ComDelayedInsert, "Delayed Insert"),
    (ComChangeUser, "Change User"),
    (ComBinlogDump, "Binlog Dump"),
    (ComTableDump, "Table Dump"),
    (ComConnectOut, "Connect out"),
    (ComRegisterSlave, "Register Slave"),
    (ComStmtPrepare, "Prepare"),
    (ComStmtExecute, "Execute"),
    (ComStmtSendLongData, "Long Data"),
    (ComStmtClose, "Close stmt"),
    (ComStmtReset, "Reset stmt"),
    (ComSetOption, "Set option"),
    (ComStmtFetch, "Fetch"),
    (ComDaemon, "Daemon"),
    (ComBinlogDumpGtid, "Binlog Dump"),
    (ComResetConnection, "Reset connect"),
];
/// Returns a command's source diagnostic name.
#[must_use]
pub fn command_name(command: u8) -> Option<&'static str> {
    COMMAND_NAMES
        .iter()
        .find_map(|(code, name)| (*code == command).then_some(*name))
}

/// Complete exported source physical-length map.
pub const DEFAULT_LENGTH_OF_MYSQL_TYPES: &[(u8, usize)] = &[
    (crate::types::TypeYear, 1),
    (crate::types::TypeDate, 3),
    (crate::types::TypeDuration, 3),
    (crate::types::TypeDatetime, 8),
    (crate::types::TypeTimestamp, 4),
    (crate::types::TypeTiny, 1),
    (crate::types::TypeShort, 2),
    (crate::types::TypeInt24, 3),
    (crate::types::TypeLong, 4),
    (crate::types::TypeLonglong, 8),
    (crate::types::TypeFloat, 4),
    (crate::types::TypeDouble, 8),
    (crate::types::TypeEnum, 2),
    (crate::types::TypeString, 1),
    (crate::types::TypeSet, 8),
];
/// Complete exported source fractional-seconds-length map.
pub const DEFAULT_LENGTH_OF_TIME_FRACTION: &[(i32, usize)] =
    &[(0, 0), (1, 1), (2, 1), (3, 2), (4, 2), (5, 3), (6, 3)];

/// Returns the default physical storage length for a MySQL type.
#[must_use]
pub const fn default_mysql_type_length(tp: u8) -> Option<usize> {
    use crate::types::*;
    match tp {
        TypeYear => Some(1),
        TypeDate | TypeDuration | TypeInt24 => Some(3),
        TypeDatetime | TypeLonglong | TypeDouble | TypeSet => Some(8),
        TypeTimestamp | TypeLong | TypeFloat => Some(4),
        TypeTiny | TypeString => Some(1),
        TypeShort | TypeEnum => Some(2),
        _ => None,
    }
}
/// Returns the storage bytes for a fractional-seconds precision.
#[must_use]
pub const fn default_time_fraction_length(fsp: i32) -> Option<usize> {
    match fsp {
        0 => Some(0),
        1 | 2 => Some(1),
        3 | 4 => Some(2),
        5 | 6 => Some(3),
        _ => None,
    }
}

/// MySQL `sql_mode` bitset.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(transparent)]
pub struct SqlMode(pub i64);
impl std::ops::BitOr for SqlMode {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}
impl std::ops::BitAnd for SqlMode {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}
impl std::ops::Not for SqlMode {
    type Output = Self;
    fn not(self) -> Self {
        Self(!self.0)
    }
}

macro_rules! sql_modes {
    ($($name:ident = $bit:expr;)+) => {$(
        #[doc = concat!("Source-compatible `", stringify!($name), "` SQL mode.")]
        pub const $name: SqlMode = SqlMode(1_i64 << $bit);
    )+};
}
sql_modes! {
    ModeRealAsFloat=0; ModePipesAsConcat=1; ModeANSIQuotes=2; ModeIgnoreSpace=3;
    ModeNotUsed=4; ModeOnlyFullGroupBy=5; ModeNoUnsignedSubtraction=6; ModeNoDirInCreate=7;
    ModePostgreSQL=8; ModeOracle=9; ModeMsSQL=10; ModeDb2=11; ModeMaxdb=12;
    ModeNoKeyOptions=13; ModeNoTableOptions=14; ModeNoFieldOptions=15; ModeMySQL323=16;
    ModeMySQL40=17; ModeANSI=18; ModeNoAutoValueOnZero=19; ModeNoBackslashEscapes=20;
    ModeStrictTransTables=21; ModeStrictAllTables=22; ModeNoZeroInDate=23;
    ModeNoZeroDate=24; ModeInvalidDates=25; ModeErrorForDivisionByZero=26;
    ModeTraditional=27; ModeNoAutoCreateUser=28; ModeHighNotPrecedence=29;
    ModeNoEngineSubstitution=30; ModePadCharToFullLength=31; ModeAllowInvalidDates=32;
}
/// Empty SQL mode.
pub const ModeNone: SqlMode = SqlMode(0);

impl SqlMode {
    const fn has(self, mode: Self) -> bool {
        self.0 & mode.0 == mode.0
    }
    /// Tests `NO_ZERO_DATE`.
    pub const fn has_no_zero_date_mode(self) -> bool {
        self.has(ModeNoZeroDate)
    }
    /// Tests `NO_ZERO_IN_DATE`.
    pub const fn has_no_zero_in_date_mode(self) -> bool {
        self.has(ModeNoZeroInDate)
    }
    /// Tests `ERROR_FOR_DIVISION_BY_ZERO`.
    pub const fn has_error_for_division_by_zero_mode(self) -> bool {
        self.has(ModeErrorForDivisionByZero)
    }
    /// Tests `ONLY_FULL_GROUP_BY`.
    pub const fn has_only_full_group_by(self) -> bool {
        self.has(ModeOnlyFullGroupBy)
    }
    /// Tests either strict-table mode.
    pub const fn has_strict_mode(self) -> bool {
        self.has(ModeStrictTransTables) || self.has(ModeStrictAllTables)
    }
    /// Tests `PIPES_AS_CONCAT`.
    pub const fn has_pipes_as_concat_mode(self) -> bool {
        self.has(ModePipesAsConcat)
    }
    /// Tests `NO_UNSIGNED_SUBTRACTION`.
    pub const fn has_no_unsigned_subtraction_mode(self) -> bool {
        self.has(ModeNoUnsignedSubtraction)
    }
    /// Tests `HIGH_NOT_PRECEDENCE`.
    pub const fn has_high_not_precedence_mode(self) -> bool {
        self.has(ModeHighNotPrecedence)
    }
    /// Tests `ANSI_QUOTES`.
    pub const fn has_ansi_quotes_mode(self) -> bool {
        self.has(ModeANSIQuotes)
    }
    /// Tests `REAL_AS_FLOAT`.
    pub const fn has_real_as_float_mode(self) -> bool {
        self.has(ModeRealAsFloat)
    }
    /// Tests `PAD_CHAR_TO_FULL_LENGTH`.
    pub const fn has_pad_char_to_full_length_mode(self) -> bool {
        self.has(ModePadCharToFullLength)
    }
    /// Tests `NO_BACKSLASH_ESCAPES`.
    pub const fn has_no_backslash_escapes_mode(self) -> bool {
        self.has(ModeNoBackslashEscapes)
    }
    /// Tests `IGNORE_SPACE`.
    pub const fn has_ignore_space_mode(self) -> bool {
        self.has(ModeIgnoreSpace)
    }
    /// Tests `NO_AUTO_CREATE_USER`.
    pub const fn has_no_auto_create_user_mode(self) -> bool {
        self.has(ModeNoAutoCreateUser)
    }
    /// Tests `ALLOW_INVALID_DATES`.
    pub const fn has_allow_invalid_dates_mode(self) -> bool {
        self.has(ModeAllowInvalidDates)
    }
}
/// Deletes bits from an SQL mode.
#[must_use]
pub const fn delete_sql_mode(original: SqlMode, delete: SqlMode) -> SqlMode {
    SqlMode(original.0 & !delete.0)
}
/// Adds bits to an SQL mode.
#[must_use]
pub const fn set_sql_mode(original: SqlMode, add: SqlMode) -> SqlMode {
    SqlMode(original.0 | add.0)
}

/// Complete source SQL-mode-name map.
pub const SQL_MODE_NAMES: &[(&str, SqlMode)] = &[
    ("REAL_AS_FLOAT", ModeRealAsFloat),
    ("PIPES_AS_CONCAT", ModePipesAsConcat),
    ("ANSI_QUOTES", ModeANSIQuotes),
    ("IGNORE_SPACE", ModeIgnoreSpace),
    ("NOT_USED", ModeNotUsed),
    ("ONLY_FULL_GROUP_BY", ModeOnlyFullGroupBy),
    ("NO_UNSIGNED_SUBTRACTION", ModeNoUnsignedSubtraction),
    ("NO_DIR_IN_CREATE", ModeNoDirInCreate),
    ("POSTGRESQL", ModePostgreSQL),
    ("ORACLE", ModeOracle),
    ("MSSQL", ModeMsSQL),
    ("DB2", ModeDb2),
    ("MAXDB", ModeMaxdb),
    ("NO_KEY_OPTIONS", ModeNoKeyOptions),
    ("NO_TABLE_OPTIONS", ModeNoTableOptions),
    ("NO_FIELD_OPTIONS", ModeNoFieldOptions),
    ("MYSQL323", ModeMySQL323),
    ("MYSQL40", ModeMySQL40),
    ("ANSI", ModeANSI),
    ("NO_AUTO_VALUE_ON_ZERO", ModeNoAutoValueOnZero),
    ("NO_BACKSLASH_ESCAPES", ModeNoBackslashEscapes),
    ("STRICT_TRANS_TABLES", ModeStrictTransTables),
    ("STRICT_ALL_TABLES", ModeStrictAllTables),
    ("NO_ZERO_IN_DATE", ModeNoZeroInDate),
    ("NO_ZERO_DATE", ModeNoZeroDate),
    ("INVALID_DATES", ModeInvalidDates),
    ("ERROR_FOR_DIVISION_BY_ZERO", ModeErrorForDivisionByZero),
    ("TRADITIONAL", ModeTraditional),
    ("NO_AUTO_CREATE_USER", ModeNoAutoCreateUser),
    ("HIGH_NOT_PRECEDENCE", ModeHighNotPrecedence),
    ("NO_ENGINE_SUBSTITUTION", ModeNoEngineSubstitution),
    ("PAD_CHAR_TO_FULL_LENGTH", ModePadCharToFullLength),
    ("ALLOW_INVALID_DATES", ModeAllowInvalidDates),
];

/// Resolves a source combination mode to its ordered expansion.
#[must_use]
pub fn combination_sql_mode(name: &str) -> Option<&'static [&'static str]> {
    match name {
        "ANSI" => Some(&[
            "REAL_AS_FLOAT",
            "PIPES_AS_CONCAT",
            "ANSI_QUOTES",
            "IGNORE_SPACE",
            "ONLY_FULL_GROUP_BY",
        ]),
        "DB2" | "MSSQL" | "POSTGRESQL" => Some(&[
            "PIPES_AS_CONCAT",
            "ANSI_QUOTES",
            "IGNORE_SPACE",
            "NO_KEY_OPTIONS",
            "NO_TABLE_OPTIONS",
            "NO_FIELD_OPTIONS",
        ]),
        "MAXDB" | "ORACLE" => Some(&[
            "PIPES_AS_CONCAT",
            "ANSI_QUOTES",
            "IGNORE_SPACE",
            "NO_KEY_OPTIONS",
            "NO_TABLE_OPTIONS",
            "NO_FIELD_OPTIONS",
            "NO_AUTO_CREATE_USER",
        ]),
        "MYSQL323" => Some(&["MYSQL323", "HIGH_NOT_PRECEDENCE"]),
        "MYSQL40" => Some(&["MYSQL40", "HIGH_NOT_PRECEDENCE"]),
        "TRADITIONAL" => Some(&[
            "STRICT_TRANS_TABLES",
            "STRICT_ALL_TABLES",
            "NO_ZERO_IN_DATE",
            "NO_ZERO_DATE",
            "ERROR_FOR_DIVISION_BY_ZERO",
            "NO_AUTO_CREATE_USER",
            "NO_ENGINE_SUBSTITUTION",
        ]),
        _ => None,
    }
}

/// Uppercases, expands combinations, and removes duplicates in source order.
#[must_use]
pub fn format_sql_mode_str(input: &str) -> String {
    let upper = crate::to_uppercase(input.trim_end_matches(' '));
    let mut seen = HashSet::new();
    let mut output = Vec::new();
    for part in upper.split(',').filter(|part| !part.is_empty()) {
        if let Some(parts) = combination_sql_mode(part) {
            for item in parts {
                if seen.insert(*item) {
                    output.push(*item);
                }
            }
        }
        if seen.insert(part) {
            output.push(part);
        }
    }
    output.join(",")
}

/// Invalid SQL-mode token plus the valid prefix accumulated before it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvalidSqlMode {
    /// Valid prefix bits.
    pub partial: SqlMode,
    /// Exact invalid token.
    pub value: String,
    /// Authoritative MySQL error identity and catalog-rendered message.
    pub sql_error: SqlError,
}
impl fmt::Display for InvalidSqlMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.sql_error.fmt(f)
    }
}
impl Error for InvalidSqlMode {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.sql_error)
    }
}

/// Parses an already-formatted comma-separated SQL mode, retaining Go's
/// partial-result contract on the first invalid token.
pub fn get_sql_mode(input: &str) -> Result<SqlMode, InvalidSqlMode> {
    let mut result = ModeNone;
    for value in input.split(',') {
        match SQL_MODE_NAMES
            .iter()
            .find_map(|(name, mode)| (*name == value).then_some(*mode))
        {
            Some(mode) => result = result | mode,
            None if value.is_empty() => {}
            None => {
                let sql_error = SqlError::new(
                    ErrWrongValueForVar,
                    &[FormatArg::from("sql_mode"), FormatArg::from(value)],
                );
                return Err(InvalidSqlMode {
                    partial: result,
                    value: value.to_owned(),
                    sql_error,
                });
            }
        }
    }
    Ok(result)
}

/// Statement scheduling priority.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Priority {
    /// No keyword.
    #[default]
    None,
    /// LOW_PRIORITY.
    Low,
    /// HIGH_PRIORITY.
    High,
    /// DELAYED.
    Delayed,
}
impl Priority {
    /// Source map spelling, including `NO_PRIORITY` for the zero value.
    #[must_use]
    pub const fn as_name(self) -> &'static str {
        match self {
            Self::None => "NO_PRIORITY",
            Self::Low => "LOW_PRIORITY",
            Self::High => "HIGH_PRIORITY",
            Self::Delayed => "DELAYED",
        }
    }
    /// SQL text emitted by Restore.
    #[must_use]
    pub const fn restore(self) -> &'static str {
        match self {
            Self::None => "",
            Self::Low => "LOW_PRIORITY",
            Self::High => "HIGH_PRIORITY",
            Self::Delayed => "DELAYED",
        }
    }
}
/// Parses a priority case-insensitively, defaulting to no priority.
#[must_use]
pub fn priority_from_str(value: &str) -> Priority {
    match crate::to_uppercase(value).as_str() {
        "HIGH_PRIORITY" => Priority::High,
        "LOW_PRIORITY" => Priority::Low,
        "DELAYED" => Priority::Delayed,
        _ => Priority::None,
    }
}
