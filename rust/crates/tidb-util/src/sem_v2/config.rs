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

//! Go `config.go`: the SEM configuration, its JSON form, and its validation.

use serde::{Deserialize, Deserializer, Serialize};

use super::{get_sys_var, sql_rule_by_name, tidb_release_version, SysVarScope};

/// Go `Config`: the configuration for SEM.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Config {
    /// Go `version`, the version of this config. Not used for now because
    /// there is only one version of the SEM config.
    #[serde(default, deserialize_with = "null_as_default")]
    pub version: String,

    /// Go `tidb_version`: the minimum TiDB version this config requires.
    #[serde(rename = "tidb_version", default, deserialize_with = "null_as_default")]
    pub tidb_version: String,

    /// Go `restricted_databases`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub restricted_databases: Vec<String>,

    /// Go `restricted_tables`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub restricted_tables: Vec<TableRestriction>,

    /// Go `restricted_variables`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub restricted_variables: Vec<VariableRestriction>,

    /// Go `restricted_status_variables` (field `RestrictedStatusVar`).
    #[serde(
        rename = "restricted_status_variables",
        default,
        deserialize_with = "null_as_default"
    )]
    pub restricted_status_var: Vec<String>,

    /// Go `restricted_privileges`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub restricted_privileges: Vec<String>,

    /// Go `restricted_sql`: restricted SQL statements and rules.
    #[serde(default, deserialize_with = "null_as_default")]
    pub restricted_sql: SQLRestriction,

    /// Go `restricted_hints`: optimizer hints stripped from queries and
    /// bindings with a warning. A hint that overrides a system variable (see
    /// [`super::HINT_GUARD_VARS`]) is only stripped while that variable is
    /// hidden or read-only.
    #[serde(
        default,
        deserialize_with = "null_as_default",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub restricted_hints: Vec<String>,
}

/// Go's `encoding/json` decodes a JSON `null` into a slice or string as the
/// zero value; serde rejects it by default.
fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// Go `TableRestriction`: the configuration for a restricted table.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct TableRestriction {
    /// Go `schema`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub schema: String,
    /// Go `name`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub name: String,
    /// Go `hidden`.
    #[serde(default)]
    pub hidden: bool,
    /// Go `columns`: the special configuration for columns in the table.
    #[serde(default, deserialize_with = "null_as_default")]
    pub columns: Vec<ColumnRestriction>,
}

/// Go `ColumnRestriction`: the configuration for a restricted column.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ColumnRestriction {
    /// Go `name`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub name: String,
    /// Go `hidden`.
    #[serde(default)]
    pub hidden: bool,
    /// Go `value`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub value: String,
}

/// Go `VariableRestriction`: the configuration for a restricted variable.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct VariableRestriction {
    /// Go `name`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub name: String,
    /// Go `hidden`.
    #[serde(default)]
    pub hidden: bool,
    /// Go `readonly`.
    #[serde(default)]
    pub readonly: bool,
    /// Go `value`.
    #[serde(default, deserialize_with = "null_as_default")]
    pub value: String,
}

/// Go `SQLRestriction`: restricted SQL statements and rules.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct SQLRestriction {
    /// Go `sql`: the list of restricted SQL statements.
    #[serde(default, deserialize_with = "null_as_default")]
    pub sql: Vec<String>,
    /// Go `rule`: the list of restricted SQL rules.
    #[serde(default, deserialize_with = "null_as_default")]
    pub rule: Vec<String>,
}

/// Go `parseSEMConfigFromFile`: reads a SEM configuration from a file.
///
/// # Errors
///
/// Reports the file-open and JSON-decode failures Go wraps.
pub fn parse_sem_config_from_file(file_path: &str) -> Result<Config, String> {
    // Go cleans the path before opening; `std::fs::read` performs no shell or
    // glob expansion, so the clean is a no-op for this reader.
    let content = std::fs::read(file_path)
        .map_err(|error| format!("failed to open file {file_path}: {error}"))?;
    serde_json::from_slice(&content)
        .map_err(|error| format!("failed to decode JSON from file {file_path}: {error}"))
}

/// Go `validateSEMConfig`.
///
/// # Errors
///
/// Reports an unparsable or too-new required version, an unknown restricted
/// variable, a value set on a tunable variable, and an unknown SQL rule.
pub fn validate_sem_config(cfg: &Config) -> Result<(), String> {
    // validate the TiDBVersion
    let release = tidb_release_version();
    let current_version = SemVersion::parse(release.strip_prefix('v').unwrap_or(&release))
        .map_err(|error| format!("failed to parse current TiDB version: {error}"))?;
    let required = cfg
        .tidb_version
        .strip_prefix('v')
        .unwrap_or(&cfg.tidb_version);
    let min_required_version = SemVersion::parse(required).map_err(|error| {
        format!(
            "failed to parse minimum required TiDB version {}: {error}",
            cfg.tidb_version
        )
    })?;
    if current_version < min_required_version {
        return Err(format!(
            "current TiDB version {current_version} is less than the required version {min_required_version}"
        ));
    }

    // validate the variable configuration
    for var_def in &cfg.restricted_variables {
        let Some(sys_var) = get_sys_var(&var_def.name) else {
            return Err(format!(
                "restricted variable {} is not a valid system variable",
                var_def.name
            ));
        };
        if !var_def.value.is_empty() && sys_var.scope != SysVarScope::None {
            return Err(format!(
                "restricted variable {} has a value set, but it is not a readonly variable",
                var_def.name
            ));
        }
    }

    // validate the SQL rules exist
    for rule_name in &cfg.restricted_sql.rule {
        if sql_rule_by_name(rule_name).is_none() {
            return Err(format!("unknown SQL rule: {rule_name}"));
        }
    }

    Ok(())
}

/// Go `semver.Version` from `github.com/coreos/go-semver/semver`, hand-rolled
/// because no semver crate is available offline (see the module boundaries).
///
/// `NewVersion` requires all three numeric components; the pre-release and
/// metadata suffixes are optional. Ordering compares the numeric triple, then
/// treats a pre-release as lower than the same release, then compares the
/// dot-separated pre-release identifiers (numeric ones numerically). Build
/// metadata is ignored in comparisons, exactly as `go-semver` does.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SemVersion {
    /// Major component.
    pub major: u64,
    /// Minor component.
    pub minor: u64,
    /// Patch component.
    pub patch: u64,
    /// Pre-release suffix (after `-`), empty when absent.
    pub pre_release: String,
    /// Build metadata (after `+`), empty when absent.
    pub metadata: String,
}

impl SemVersion {
    /// Go `semver.NewVersion`.
    ///
    /// # Errors
    ///
    /// Returns `go-semver`'s message for a malformed version.
    pub fn parse(text: &str) -> Result<Self, String> {
        let bad = || format!("{text} is not in dotted-tri format");

        let (rest, metadata) = match text.split_once('+') {
            Some((rest, metadata)) => (rest, metadata.to_owned()),
            None => (text, String::new()),
        };
        let (numbers, pre_release) = match rest.split_once('-') {
            Some((numbers, pre)) => (numbers, pre.to_owned()),
            None => (rest, String::new()),
        };

        let mut parts = numbers.split('.');
        let mut next = || -> Result<u64, String> {
            parts
                .next()
                .ok_or_else(bad)?
                .parse::<u64>()
                .map_err(|_| bad())
        };
        let major = next()?;
        let minor = next()?;
        let patch = next()?;
        if parts.next().is_some() {
            return Err(bad());
        }

        Ok(Self {
            major,
            minor,
            patch,
            pre_release,
            metadata,
        })
    }
}

impl std::fmt::Display for SemVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)?;
        if !self.pre_release.is_empty() {
            write!(formatter, "-{}", self.pre_release)?;
        }
        if !self.metadata.is_empty() {
            write!(formatter, "+{}", self.metadata)?;
        }
        Ok(())
    }
}

impl Ord for SemVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        let numeric =
            (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch));
        if numeric != Ordering::Equal {
            return numeric;
        }
        match (self.pre_release.is_empty(), other.pre_release.is_empty()) {
            (true, true) => Ordering::Equal,
            // A release outranks any pre-release of the same triple.
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => compare_pre_release(&self.pre_release, &other.pre_release),
        }
    }
}

impl PartialOrd for SemVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn compare_pre_release(left: &str, right: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let mut left = left.split('.');
    let mut right = right.split('.');
    loop {
        match (left.next(), right.next()) {
            (None, None) => return Ordering::Equal,
            // A shorter set of identifiers has lower precedence.
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(one), Some(two)) => {
                let order = match (one.parse::<u64>(), two.parse::<u64>()) {
                    (Ok(one), Ok(two)) => one.cmp(&two),
                    // Numeric identifiers always have lower precedence.
                    (Ok(_), Err(_)) => Ordering::Less,
                    (Err(_), Ok(_)) => Ordering::Greater,
                    (Err(_), Err(_)) => one.cmp(two),
                };
                if order != Ordering::Equal {
                    return order;
                }
            }
        }
    }
}
