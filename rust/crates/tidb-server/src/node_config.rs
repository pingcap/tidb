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

//! Bounded startup configuration for the first standalone Rust SQL node.
//!
//! The source TiDB binary accepts a much larger configuration surface. This
//! milestone admits only the values consumed by the executable read path: one
//! loopback TCP listener, plaintext PD seeds, and one ordered signed-BIGINT
//! table-column catalog. Unknown or duplicate options fail startup so an
//! operator cannot believe an unsupported TiDB setting was applied.

use std::collections::HashSet;
use std::fmt;
use std::net::IpAddr;

use tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET;

/// Storage shape of one configured signed-BIGINT column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredReadColumnKind {
    /// The table's sole signed integer clustered primary key.
    ClusteredPrimaryKey,
    /// A signed stored non-null column decoded from the TiKV row payload.
    StoredNotNull,
}

impl ConfiguredReadColumnKind {
    pub(crate) const fn descriptor_name(self) -> &'static str {
        match self {
            Self::ClusteredPrimaryKey => "clustered-pk",
            Self::StoredNotNull => "stored-not-null",
        }
    }
}

/// One atomic configured column descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredReadColumn {
    /// Table-visible column name.
    pub name: String,
    /// Stable column identifier from TiDB schema metadata.
    pub id: i64,
    /// Physical storage role admitted by this milestone.
    pub kind: ConfiguredReadColumnKind,
}

/// The sole table shape admitted by the first deployable read-only node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredReadTable {
    /// Schema name matched case-insensitively by the bounded planner.
    pub database: String,
    /// Table name matched case-insensitively by the bounded planner.
    pub table: String,
    /// Physical TiKV table identifier resolved by the fixture/owner.
    pub table_id: i64,
    /// Checked columns in configured order.
    pub columns: Vec<ConfiguredReadColumn>,
}

/// Complete startup input consumed by the serial SQL node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    /// Loopback address on which MySQL protocol connections are accepted.
    pub host: IpAddr,
    /// MySQL protocol port. Zero requests an ephemeral test port.
    pub port: u16,
    /// Plaintext PD endpoints in configured order.
    pub pd_endpoints: Vec<String>,
    /// One checked table exposed by the bounded planner.
    pub read_table: ConfiguredReadTable,
    /// Maximum accepted logical MySQL packet size.
    pub max_allowed_packet: usize,
}

/// Startup configuration failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NodeConfigError {
    /// The caller requested usage text instead of startup.
    HelpRequested,
    /// A required option was omitted.
    MissingOption(&'static str),
    /// One option appeared more than once.
    DuplicateOption(String),
    /// The bounded executable does not implement this option.
    UnknownOption(String),
    /// An option did not have a following value.
    MissingValue(String),
    /// An option value was malformed or outside the admitted domain.
    InvalidValue {
        /// Option name.
        option: String,
        /// Stable reason for rejection.
        reason: String,
    },
    /// Only the real TiKV store is supported by this executable.
    UnsupportedStore(String),
    /// The empty-password milestone must never bind a non-loopback address.
    NonLoopbackHost(IpAddr),
}

impl fmt::Display for NodeConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HelpRequested => formatter.write_str("help requested"),
            Self::MissingOption(option) => write!(formatter, "missing required option {option}"),
            Self::DuplicateOption(option) => write!(formatter, "duplicate option {option}"),
            Self::UnknownOption(option) => write!(formatter, "unsupported option {option}"),
            Self::MissingValue(option) => write!(formatter, "missing value for {option}"),
            Self::InvalidValue { option, reason } => {
                write!(formatter, "invalid value for {option}: {reason}")
            }
            Self::UnsupportedStore(store) => {
                write!(formatter, "unsupported store {store:?}; only tikv is executable")
            }
            Self::NonLoopbackHost(host) => write!(
                formatter,
                "refusing non-loopback MySQL listener {host} while only empty-password root authentication is implemented"
            ),
        }
    }
}

impl std::error::Error for NodeConfigError {}

impl NodeConfig {
    /// Parses the source-shaped command line, including the executable name.
    ///
    /// Both `--name value` and `--name=value` are accepted. The parser is
    /// deliberately one-use and rejects duplicate values instead of applying
    /// last-option-wins behavior to security or topology settings.
    pub fn parse<I, S>(arguments: I) -> Result<Self, NodeConfigError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut arguments = arguments.into_iter().map(Into::into);
        let _program = arguments.next();
        let mut pending = arguments.peekable();

        let mut host = None;
        let mut port = None;
        let mut path = None;
        let mut store = None;
        let mut database = None;
        let mut table = None;
        let mut table_id = None;
        let mut columns = Vec::new();
        let mut max_allowed_packet = None;

        while let Some(argument) = pending.next() {
            if argument == "--help" || argument == "-h" {
                return Err(NodeConfigError::HelpRequested);
            }
            if argument == "-P" {
                let value = pending
                    .next()
                    .filter(|value| !value.starts_with('-'))
                    .ok_or_else(|| NodeConfigError::MissingValue("-P".to_owned()))?;
                set_once(&mut port, "-P", value)?;
                continue;
            }
            let (option, inline_value) = split_option(&argument)?;
            let value = match inline_value {
                Some(value) => value.to_owned(),
                None => pending
                    .next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or_else(|| NodeConfigError::MissingValue(option.to_owned()))?,
            };
            match option {
                "--host" => set_once(&mut host, option, value)?,
                "--port" => set_once(&mut port, option, value)?,
                "--path" => set_once(&mut path, option, value)?,
                "--store" => set_once(&mut store, option, value)?,
                "--database" => set_once(&mut database, option, value)?,
                "--table" => set_once(&mut table, option, value)?,
                "--table-id" => set_once(&mut table_id, option, value)?,
                "--column" => columns.push(parse_column_descriptor(value)?),
                "--max-allowed-packet" => {
                    set_once(&mut max_allowed_packet, option, value)?;
                }
                _ => return Err(NodeConfigError::UnknownOption(option.to_owned())),
            }
        }

        let host = parse_ip("--host", host.as_deref().unwrap_or("127.0.0.1"))?;
        if !host.is_loopback() {
            return Err(NodeConfigError::NonLoopbackHost(host));
        }
        let port = parse_number("--port", port.as_deref().unwrap_or("4000"))?;
        let store = store.as_deref().unwrap_or("tikv");
        if !store.eq_ignore_ascii_case("tikv") {
            return Err(NodeConfigError::UnsupportedStore(store.to_owned()));
        }
        let pd_endpoints = parse_pd_endpoints(required(path, "--path")?)?;
        let database = parse_identifier("--database", required(database, "--database")?)?;
        let table = parse_identifier("--table", required(table, "--table")?)?;
        let table_id = parse_positive_id("--table-id", required(table_id, "--table-id")?)?;
        validate_columns(&columns)?;
        let max_allowed_packet = match max_allowed_packet {
            Some(value) => parse_positive_number("--max-allowed-packet", &value)?,
            None => DEFAULT_MAX_ALLOWED_PACKET,
        };

        Ok(Self {
            host,
            port,
            pd_endpoints,
            read_table: ConfiguredReadTable {
                database,
                table,
                table_id,
                columns,
            },
            max_allowed_packet,
        })
    }

    /// Stable usage text printed by the executable for `--help`.
    #[must_use]
    pub const fn help_text() -> &'static str {
        "Usage: tidb-server --path <pd[,pd...]> --database <db> --table <table> \
--table-id <id> --column <name>:<id>:<clustered-pk|stored-not-null> \
[--column <name>:<id>:<clustered-pk|stored-not-null> ...] \
[--host <loopback-ip>] [-P <port>|--port <port>] [--store tikv] \
[--max-allowed-packet <bytes>]"
    }
}

fn split_option(argument: &str) -> Result<(&str, Option<&str>), NodeConfigError> {
    if !argument.starts_with("--") {
        return Err(NodeConfigError::UnknownOption(argument.to_owned()));
    }
    match argument.split_once('=') {
        Some((option, "")) => Err(NodeConfigError::MissingValue(option.to_owned())),
        Some((option, value)) => Ok((option, Some(value))),
        None => Ok((argument, None)),
    }
}

fn set_once(slot: &mut Option<String>, option: &str, value: String) -> Result<(), NodeConfigError> {
    if slot.replace(value).is_some() {
        return Err(NodeConfigError::DuplicateOption(option.to_owned()));
    }
    Ok(())
}

fn required(value: Option<String>, option: &'static str) -> Result<String, NodeConfigError> {
    value.ok_or(NodeConfigError::MissingOption(option))
}

fn parse_ip(option: &str, value: &str) -> Result<IpAddr, NodeConfigError> {
    value
        .parse()
        .map_err(|_| invalid(option, "expected an IP address"))
}

fn parse_number<T>(option: &str, value: &str) -> Result<T, NodeConfigError>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| invalid(option, "expected an unsigned decimal integer"))
}

fn parse_positive_number<T>(option: &str, value: &str) -> Result<T, NodeConfigError>
where
    T: std::str::FromStr + Default + PartialEq,
{
    let parsed = parse_number(option, value)?;
    if parsed == T::default() {
        return Err(invalid(option, "value must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_positive_id(option: &str, value: String) -> Result<i64, NodeConfigError> {
    let parsed = value
        .parse::<i64>()
        .map_err(|_| invalid(option, "expected a signed decimal integer"))?;
    if parsed <= 0 {
        return Err(invalid(option, "value must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_identifier(option: &str, value: String) -> Result<String, NodeConfigError> {
    if value.is_empty() || value.as_bytes().contains(&0) {
        return Err(invalid(
            option,
            "identifier must be nonempty and contain no NUL",
        ));
    }
    Ok(value)
}

fn parse_column_descriptor(value: String) -> Result<ConfiguredReadColumn, NodeConfigError> {
    let mut fields = value.split(':');
    let (Some(name), Some(id), Some(kind), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return Err(invalid(
            "--column",
            "expected <name>:<id>:<clustered-pk|stored-not-null>",
        ));
    };
    let name = parse_identifier("--column", name.to_owned())?;
    let id = parse_positive_id("--column", id.to_owned())?;
    let kind = match kind {
        "clustered-pk" => ConfiguredReadColumnKind::ClusteredPrimaryKey,
        "stored-not-null" => ConfiguredReadColumnKind::StoredNotNull,
        _ => {
            return Err(invalid(
                "--column",
                "column kind must be clustered-pk or stored-not-null",
            ));
        }
    };
    Ok(ConfiguredReadColumn { name, id, kind })
}

fn validate_columns(columns: &[ConfiguredReadColumn]) -> Result<(), NodeConfigError> {
    if columns.is_empty() {
        return Err(NodeConfigError::MissingOption("--column"));
    }

    let mut names = HashSet::with_capacity(columns.len());
    let mut ids = HashSet::with_capacity(columns.len());
    let mut clustered_primary_keys = 0;
    for column in columns {
        if !names.insert(column.name.to_lowercase()) {
            return Err(invalid(
                "--column",
                "column names must be unique case-insensitively",
            ));
        }
        if !ids.insert(column.id) {
            return Err(invalid("--column", "column IDs must be unique"));
        }
        if column.kind == ConfiguredReadColumnKind::ClusteredPrimaryKey {
            clustered_primary_keys += 1;
        }
    }
    if clustered_primary_keys != 1 {
        return Err(invalid(
            "--column",
            "exactly one column must be clustered-pk",
        ));
    }
    Ok(())
}

fn parse_pd_endpoints(value: String) -> Result<Vec<String>, NodeConfigError> {
    let endpoints = value
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if endpoints.is_empty() || endpoints.iter().any(String::is_empty) {
        return Err(invalid(
            "--path",
            "expected a comma-separated PD endpoint list",
        ));
    }
    for endpoint in &endpoints {
        if endpoint.contains("//") || !endpoint.contains(':') {
            return Err(invalid(
                "--path",
                "PD endpoints must be plaintext host:port values",
            ));
        }
    }
    Ok(endpoints)
}

fn invalid(option: &str, reason: &str) -> NodeConfigError {
    NodeConfigError::InvalidValue {
        option: option.to_owned(),
        reason: reason.to_owned(),
    }
}
