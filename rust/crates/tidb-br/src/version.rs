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

//! Go `br/pkg/version` and `br/pkg/version/build`: the cluster version
//! checks BR runs before a backup or restore, and the TiDB server version
//! parser. `FetchVersion` (a `database/sql` query, used by the Dumpling and
//! Lightning CLIs) is not ported.

use std::borrow::Cow;
use std::sync::{LazyLock, Mutex};

use regex::Regex;
use tidb_go_semver::Version;
use tidb_log::{Field, Value};
use tidb_proto::metapb;

use crate::errors::{self, Result, ERR_VERSION_MISMATCH};

/// Go `br/pkg/version/build`.
pub mod build {
    /// Go `ReleaseVersionForTest`.
    pub const RELEASE_VERSION_FOR_TEST: &str = "nightly-dirty";

    /// Go assigning `build.ReleaseVersion` (tests do).
    static RELEASE_VERSION: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

    /// Go `build.ReleaseVersion = v`; `None` restores the build's value.
    pub fn set_release_version(v: Option<&str>) {
        *RELEASE_VERSION.lock().unwrap() = v.map(str::to_owned);
    }

    /// Go `ReleaseVersion` (`getReleaseVersion`): TiDB's release version,
    /// or [`RELEASE_VERSION_FOR_TEST`] for a placeholder build.
    #[must_use]
    pub fn release_version() -> String {
        if let Some(v) = RELEASE_VERSION.lock().unwrap().clone() {
            return v;
        }
        let v = tidb_mysql::consts::TIDB_RELEASE_VERSION;
        if v != "None" && !v.contains("this-is-a-placeholder") {
            v.to_owned()
        } else {
            RELEASE_VERSION_FOR_TEST.to_owned()
        }
    }

    /// Go `GitBranch`.
    #[must_use]
    pub fn git_branch() -> &'static str {
        tidb_versioninfo::TIDB_GIT_BRANCH
    }
}

/// Go `minTiKVVersion`.
static MIN_TIKV_VERSION: LazyLock<Version> = LazyLock::new(|| tidb_go_semver::new("3.1.0-beta.2"));
/// Go `incompatibleTiKVMajor3`.
static INCOMPATIBLE_TIKV_MAJOR3: LazyLock<Version> = LazyLock::new(|| tidb_go_semver::new("3.1.0"));
/// Go `incompatibleTiKVMajor4`.
static INCOMPATIBLE_TIKV_MAJOR4: LazyLock<Version> = LazyLock::new(|| tidb_go_semver::new("4.0.0-rc.1"));
/// Go `compatibleTiFlashMajor3`.
static COMPATIBLE_TIFLASH_MAJOR3: LazyLock<Version> = LazyLock::new(|| tidb_go_semver::new("3.1.0"));
/// Go `compatibleTiFlashMajor4`.
static COMPATIBLE_TIFLASH_MAJOR4: LazyLock<Version> = LazyLock::new(|| tidb_go_semver::new("4.0.0"));
/// Go `versionHash`.
static VERSION_HASH: LazyLock<Regex> = LazyLock::new(|| Regex::new("-[0-9]+-g[0-9a-f]{7,}").unwrap());

/// Go `checkpointSupportError`, set by [`check_version_for_br`].
static CHECKPOINT_SUPPORT_ERROR: Mutex<Option<errors::Error>> = Mutex::new(None);
/// Go `pitrSupportBatchKVFiles`, set by [`check_version_for_br_pitr`].
static PITR_SUPPORT_BATCH_KV_FILES: Mutex<bool> = Mutex::new(false);

/// Go `CURRENT_BACKUP_SUPPORT_TABLE_INFO_VERSION` (`model.TableInfoVersion5`).
pub const CURRENT_BACKUP_SUPPORT_TABLE_INFO_VERSION: u16 = 5;

/// Go `NextMajorVersion`.
#[must_use]
pub fn next_major_version() -> Version {
    match tidb_go_semver::new_version(&remove_v_and_hash(&build::release_version())) {
        Ok(mut v) => {
            v.bump_major();
            v
        }
        Err(_) => Version { major: i64::MAX, pre_release: "nightly".into(), ..Version::default() },
    }
}

/// Go `removeVAndHash`.
fn remove_v_and_hash(v: &str) -> String {
    let v = VERSION_HASH.replace_all(v, regex::NoExpand(""));
    let v = v.strip_suffix("-dirty").unwrap_or(&v);
    v.strip_prefix('v').unwrap_or(v).to_owned()
}

/// Go `engine.IsTiFlash`.
#[must_use]
pub fn is_tiflash(store: &metapb::Store) -> bool {
    store.labels.iter().any(|l| l.key == "engine" && (l.value == "tiflash_compute" || l.value == "tiflash"))
}

/// Go `checkTiFlashVersion`.
fn check_tiflash_version(store: &metapb::Store) -> Result<()> {
    let flash = tidb_go_semver::new_version(&remove_v_and_hash(&store.version)).map_err(|err| {
        errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!("failed to parse TiFlash {} version {}, err {err}", store.peer_address, store.version),
        )
    })?;
    if flash.major == 3 && flash.less_than(&COMPATIBLE_TIFLASH_MAJOR3) {
        return Err(errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!(
                "incompatible TiFlash {} version {}, try update it to {}",
                store.peer_address, store.version, *COMPATIBLE_TIFLASH_MAJOR3
            ),
        ));
    }
    if flash.major == 4 && flash.less_than(&COMPATIBLE_TIFLASH_MAJOR4) {
        return Err(errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!(
                "incompatible TiFlash {} version {}, try update it to {}",
                store.peer_address, store.version, *COMPATIBLE_TIFLASH_MAJOR4
            ),
        ));
    }
    Ok(())
}

/// Go `VerChecker`.
pub type VerChecker<'a> = &'a dyn Fn(&metapb::Store, &Version) -> Result<()>;

/// Go `CheckClusterVersion`: every non-tombstone store against `checker`;
/// TiFlash stores against TiFlash's own minimums.
///
/// # Errors
/// PD's error, a version mismatch, or the checker's error.
pub fn check_cluster_version(
    ctx: &tidb_gocontext::Context,
    client: &dyn crate::pdclient::PdClient,
    checker: VerChecker<'_>,
) -> Result<()> {
    let stores = client.get_all_stores(ctx, true)?;
    for s in &stores {
        let tiflash = is_tiflash(s);
        tidb_log::debug(
            "checking compatibility of store in cluster",
            &[
                Field::new("ID", Value::U64(s.id)),
                Field::new("TiFlash?", Value::Bool(tiflash)),
                Field::new("address", Value::Str(s.address.clone())),
                Field::new("version", Value::Str(s.version.clone())),
            ],
        );
        if tiflash {
            check_tiflash_version(s)?;
            continue;
        }
        let tikv_version_string = remove_v_and_hash(&s.version);
        let tikv_version = tidb_go_semver::new_version(&tikv_version_string).map_err(|err| {
            errors::annotatef(
                &ERR_VERSION_MISMATCH,
                format!("{err}: TiKV node {} version {tikv_version_string} is invalid", s.address),
            )
        })?;
        checker(s, &tikv_version)?;
    }
    Ok(())
}

/// Go `CheckVersionForBackup`.
pub fn check_version_for_backup(backup_version: Version) -> impl Fn(&metapb::Store, &Version) -> Result<()> {
    move |_, ver| {
        if backup_version.major > ver.major && backup_version.major - ver.major > 1 {
            return Err(errors::annotatef(
                &ERR_VERSION_MISMATCH,
                format!(
                    "backup with cluster version {backup_version} cannot be restored at cluster of version {ver}: major version mismatches"
                ),
            ));
        }
        Ok(())
    }
}

/// Go `CheckVersionForBRPiTR`.
///
/// # Errors
/// A version mismatch.
pub fn check_version_for_br_pitr(s: &metapb::Store, tikv_version: &Version) -> Result<()> {
    let release = build::release_version();
    if release == build::RELEASE_VERSION_FOR_TEST {
        return Ok(());
    }
    let br_version = tidb_go_semver::new_version(&remove_v_and_hash(&release)).map_err(|err| {
        errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!("{err}: invalid version, please recompile using `git fetch origin --tags && make build`"),
        )
    })?;
    if tikv_version.major < 6 || (tikv_version.major == 6 && tikv_version.minor == 0) {
        return Err(errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!(
                "TiKV node {} version {tikv_version} is too low when use PiTR, please update tikv's version to at least v6.1.0(v6.2.0+ recommanded)",
                s.address
            ),
        ));
    }
    *PITR_SUPPORT_BATCH_KV_FILES.lock().unwrap() = !(tikv_version.major < 6 || (tikv_version.major == 6 && tikv_version.minor < 5));
    if br_version.major == 6 && br_version.minor == 1 {
        if tikv_version.major != 6 || tikv_version.minor != 1 {
            return Err(errors::annotatef(
                &ERR_VERSION_MISMATCH,
                format!(
                    "TiKV node {} version {tikv_version} and BR {release} version mismatch when use PiTR v6.1.0, please use the same version of BR",
                    s.address
                ),
            ));
        }
    } else if tikv_version.major == 6 && tikv_version.minor <= 1 {
        return Err(errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!(
                "TiKV node {} version {tikv_version} and BR {release} version mismatch when use PiTR v6.2.0+, please use the tikv with version v6.2.0+",
                s.address
            ),
        ));
    }
    Ok(())
}

/// Go `CheckVersionForDDL`.
///
/// # Errors
/// A TiKV older than 6.2.0.
pub fn check_version_for_ddl(_: &metapb::Store, tikv_version: &Version) -> Result<()> {
    let require = tidb_go_semver::new("6.2.0-alpha");
    if tikv_version.compare(&require) < 0 {
        return Err(errors::errorf(format!(
            "detected the old version of tidb cluster, require: >= 6.2.0, but got {tikv_version}"
        )));
    }
    Ok(())
}

/// Go `CheckVersionForKeyspaceBR`.
///
/// # Errors
/// A TiKV older than 6.6.0.
pub fn check_version_for_keyspace_br(_: &metapb::Store, tikv_version: &Version) -> Result<()> {
    let require = tidb_go_semver::new("6.6.0-alpha");
    if tikv_version.compare(&require) < 0 {
        return Err(errors::errorf(format!(
            "detected the old version of tidb cluster, require: >= 6.6.0, but got {tikv_version}"
        )));
    }
    Ok(())
}

/// Go `CheckVersionForBR`. Also records whether the store supports
/// checkpoints ([`check_checkpoint_support`]).
///
/// # Errors
/// A version mismatch.
pub fn check_version_for_br(s: &metapb::Store, tikv_version: &Version) -> Result<()> {
    let release = build::release_version();
    if release == build::RELEASE_VERSION_FOR_TEST {
        return Ok(());
    }
    let br_version = tidb_go_semver::new_version(&remove_v_and_hash(&release)).map_err(|err| {
        errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!("{err}: invalid version, please recompile using `git fetch origin --tags && make build`"),
        )
    })?;
    let mismatch = |text: String| errors::annotatef(&ERR_VERSION_MISMATCH, text);
    if tikv_version.compare(&MIN_TIKV_VERSION) < 0 {
        return Err(mismatch(format!(
            "TiKV node {} version {tikv_version} don't support BR, please upgrade cluster to {release}",
            s.address
        )));
    }
    if br_version.major < tikv_version.major || br_version.major - tikv_version.major > 2 {
        return Err(mismatch(format!(
            "TiKV node {} version {tikv_version} and BR {release} major version mismatch, please use the same version of BR",
            s.address
        )));
    }
    if tikv_version.major == 3
        && tikv_version.compare(&INCOMPATIBLE_TIKV_MAJOR3) < 0
        && br_version.compare(&INCOMPATIBLE_TIKV_MAJOR3) >= 0
    {
        return Err(mismatch(format!(
            "TiKV node {} version {tikv_version} and BR {release} version mismatch, please use the same version of BR",
            s.address
        )));
    }
    if tikv_version.major == 4
        && tikv_version.compare(&INCOMPATIBLE_TIKV_MAJOR4) < 0
        && br_version.compare(&INCOMPATIBLE_TIKV_MAJOR4) >= 0
    {
        return Err(mismatch(format!(
            "TiKV node {} version {tikv_version} and BR {release} version mismatch, please use the same version of BR",
            s.address
        )));
    }
    let mut checkpoint = CHECKPOINT_SUPPORT_ERROR.lock().unwrap();
    *checkpoint = None;
    if tikv_version.major < 6 || (tikv_version.major == 6 && tikv_version.minor < 5) {
        *checkpoint = Some(mismatch(format!(
            "TiKV node {} version {tikv_version} is too low when use checkpoint, please update tikv's version to at least v6.5.0",
            s.address
        )));
    }
    if build::git_branch() != "master" && tikv_version.compare(&br_version) > 0 {
        tidb_log::warn(&format!("BR version is outdated, please consider use version {tikv_version} of BR"), &[]);
    }
    Ok(())
}

/// Go `CheckVersion`.
///
/// # Errors
/// A version outside `[min, max.major)`.
pub fn check_version(component: &str, actual: &Version, min: &Version, max: &Version) -> Result<()> {
    if actual.compare(min) < 0 {
        return Err(errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!("{component} version too old, required to be in [{min}, {max}), found '{actual}'"),
        ));
    }
    if actual.major >= max.major {
        return Err(errors::annotatef(
            &ERR_VERSION_MISMATCH,
            format!(
                "{component} version too new, expected to be within [{min}, {}.0.0), found '{actual}'",
                max.major
            ),
        ));
    }
    Ok(())
}

/// Go `ExtractTiDBVersion`.
///
/// # Errors
/// Not a TiDB version, or an invalid semantic version.
pub fn extract_tidb_version(version: &str) -> Result<Version> {
    let trimmed = version.strip_suffix("-dirty").unwrap_or(version);
    let versions: Vec<&str> = trimmed.split('-').collect();
    let end = match versions.len() {
        3 | 4 => versions.len(),
        5 | 6 => versions.len() - 2,
        _ => {
            return Err(errors::annotatef(&ERR_VERSION_MISMATCH, format!("not a valid TiDB version: {version}")));
        }
    };
    let raw = versions[2..end].join("-");
    let raw = raw.strip_prefix('v').unwrap_or(&raw);
    tidb_go_semver::new_version(raw).map_err(errors::errorf)
}

/// Go `CheckTiDBVersion`.
///
/// # Errors
/// Not a TiDB server, or [`check_version`]'s error.
pub fn check_tidb_version(version_str: &str, min: &Version, max: &Version) -> Result<()> {
    let info = parse_server_info(version_str);
    if info.server_type != ServerType::TiDB {
        return Err(errors::errorf(format!("server with version '{version_str}' is not TiDB")));
    }
    check_version("TiDB", &info.server_version, min, max)
}

/// Go `NormalizeBackupVersion`: `None` for an unparsable version.
#[must_use]
pub fn normalize_backup_version(version: &str) -> Option<Version> {
    let trimmed = version.trim();
    let unquoted =
        tidb_go_strconv::unquote_view(trimmed.as_bytes()).unwrap_or(Cow::Borrowed(trimmed.as_bytes()));
    let unquoted = String::from_utf8_lossy(&unquoted);
    let normalized = unquoted.trim();
    match tidb_go_semver::new_version(normalized) {
        Ok(v) => Some(v),
        Err(err) => {
            tidb_log::warn(
                "cannot parse backup version",
                &[Field::new("version", Value::Str(normalized.to_owned())), Field::new("error", Value::Str(err))],
            );
            None
        }
    }
}

/// Go `CheckCheckpointSupport`.
///
/// # Errors
/// The last [`check_version_for_br`]'s checkpoint error.
pub fn check_checkpoint_support() -> Result<()> {
    CHECKPOINT_SUPPORT_ERROR.lock().unwrap().clone().map_or(Ok(()), Err)
}

/// Go `CheckPITRSupportBatchKVFiles`.
#[must_use]
pub fn check_pitr_support_batch_kv_files() -> bool {
    *PITR_SUPPORT_BATCH_KV_FILES.lock().unwrap()
}

/// Go `ServerType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerType {
    /// Go `ServerTypeUnknown`.
    Unknown,
    /// Go `ServerTypeMySQL`.
    MySQL,
    /// Go `ServerTypeMariaDB`.
    MariaDB,
    /// Go `ServerTypeTiDB`.
    TiDB,
    /// Go `ServerTypeDumpService`.
    DumpService,
}

impl std::fmt::Display for ServerType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unknown => "Unknown",
            Self::MySQL => "MySQL",
            Self::MariaDB => "MariaDB",
            Self::TiDB => "TiDB",
            Self::DumpService => "DumpService",
        })
    }
}

/// Go `ServerInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerInfo {
    /// Go `ServerType`.
    pub server_type: ServerType,
    /// Go `ServerVersion`.
    pub server_version: Version,
    /// Go `HasTiKV`.
    pub has_tikv: bool,
    /// Go `NextGen`.
    pub next_gen: bool,
}

static MYSQL_VERSION_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?").unwrap());
static TIDB_VERSION_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"-[v]?\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?").unwrap());
static TIDB_RELEASE_VERSION_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"v\d+\.\d+\.\d+([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?").unwrap());
static TIDB_CLOUD_SERVER_VERSION_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"TiDB-CLOUD\.(\d{4})(\d{2})\.(\d+)([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?").unwrap());
static TIDB_CLOUD_RELEASE_VERSION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"Release Version:\s*CLOUD\.(\d{4})(\d{2})\.(\d+)([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*)?").unwrap()
});

/// Go `parseTiDBXVersionToSemver`.
fn parse_tidbx_version_to_semver(version_str: &str) -> String {
    let caps = TIDB_CLOUD_SERVER_VERSION_REGEX
        .captures(version_str)
        .or_else(|| TIDB_CLOUD_RELEASE_VERSION_REGEX.captures(version_str));
    let Some(caps) = caps else { return String::new() };
    let group = |i: usize| caps.get(i).map_or("", |m| m.as_str());
    let Ok(year) = group(1).parse::<u64>() else { return String::new() };
    if !(tidb_mysql::consts::TiDBXVerMinYear..=tidb_mysql::consts::TiDBXVerMaxYear).contains(&year) {
        return String::new();
    }
    let Ok(month) = group(2).parse::<u64>() else { return String::new() };
    if !(1..=12).contains(&month) {
        return String::new();
    }
    let Ok(patch) = group(3).parse::<i64>() else { return String::new() };
    format!("{}.{month}.{patch}{}", year - 2000, group(4))
}

/// Go `ParseServerInfo`: the server type and version of a `version()` /
/// `tidb_version()` result; an unparsable version is 0.0.0.
#[must_use]
pub fn parse_server_info(src: &str) -> ServerInfo {
    let lower = src.to_lowercase();
    let mut release = false;
    let server_type = if lower.contains("release version:") {
        release = true;
        ServerType::TiDB
    } else if lower.contains("tidb") {
        ServerType::TiDB
    } else if lower.contains("mariadb") {
        ServerType::MariaDB
    } else if MYSQL_VERSION_REGEX.is_match(&lower) {
        ServerType::MySQL
    } else {
        ServerType::Unknown
    };
    let mut next_gen = false;
    let version_str = if server_type == ServerType::TiDB {
        let mut v = if release {
            TIDB_RELEASE_VERSION_REGEX.find(src).map_or("", |m| m.as_str()).to_owned()
        } else {
            let v = TIDB_VERSION_REGEX.find(src).map_or("", |m| m.as_str());
            v.strip_prefix('-').unwrap_or(v).to_owned()
        };
        if v.is_empty() {
            v = parse_tidbx_version_to_semver(src);
            next_gen = !v.is_empty();
        }
        v.strip_prefix('v').unwrap_or(&v).to_owned()
    } else {
        MYSQL_VERSION_REGEX.find(src).map_or("", |m| m.as_str()).to_owned()
    };
    let server_version = tidb_go_semver::new_version(&version_str).unwrap_or_else(|_| {
        tidb_log::warn(
            "fail to parse version, fallback to 0.0.0",
            &[Field::new("version", Value::Str(version_str.clone()))],
        );
        tidb_go_semver::new("0.0.0")
    });
    tidb_log::info(
        "detect server version",
        &[
            Field::new("type", Value::Str(server_type.to_string())),
            Field::new("version", Value::Str(server_version.to_string())),
        ],
    );
    ServerInfo { server_type, server_version, has_tikv: false, next_gen }
}

#[cfg(test)]
mod tests;
