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
//! loopback TCP listener, plaintext PD seeds, and one ordered list of
//! signed-BIGINT table-column catalogs. Unknown or duplicate options fail startup so an
//! operator cannot believe an unsupported TiDB setting was applied.

use std::collections::HashSet;
use std::fmt;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET;

const DEFAULT_MAX_CONNECTIONS: usize = 8;
const MAX_CONNECTION_WORKERS: usize = 256;
const DEFAULT_CONNECTION_TIMEOUT_MS: u64 = 30_000;
// The current configured reader only exposes fixed-width signed BIGINT rows.
// Keep the first in-memory TopN vertical deliberately small until the executor
// owns spill and general memory-quota semantics.
const DEFAULT_MAX_TOPN_ROWS: usize = 1_024;
const MAX_CONFIGURED_TOPN_ROWS: usize = 65_536;
const MAX_CONFIGURED_READ_TABLES: usize = 2;
const MAX_CONFIGURED_READ_COLUMNS: usize = 4096;
const MAX_CONFIGURED_READ_INDEXES: usize = 64;

/// Storage shape of one configured column.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfiguredReadColumnKind {
    /// The table's sole signed integer clustered primary key.
    ClusteredPrimaryKey,
    /// A signed `BIGINT` stored non-null column decoded from the TiKV row payload.
    StoredNotNull,
    /// A signed `INT` (int32-domain) stored non-null column.
    StoredIntNotNull,
    /// A `CHAR(max_length)` stored non-null column (utf8mb4 bytes).
    StoredCharNotNull {
        /// Declared character length, per the SQL `CHAR(N)` width.
        max_length: u32,
    },
}

impl ConfiguredReadColumnKind {
    /// Reconstructs the command-line descriptor field for this kind. A
    /// `CHAR(N)` column round-trips its length as a fourth `:N` field.
    pub(crate) fn descriptor_name(self) -> String {
        match self {
            Self::ClusteredPrimaryKey => "clustered-pk".to_owned(),
            Self::StoredNotNull => "stored-not-null".to_owned(),
            Self::StoredIntNotNull => "stored-int-not-null".to_owned(),
            Self::StoredCharNotNull { max_length } => {
                format!("stored-char-not-null:{max_length}")
            }
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

/// One non-unique secondary index descriptor over a single stored column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfiguredReadIndex {
    /// Index-visible name, retained for diagnostics.
    pub name: String,
    /// Stable index identifier from TiDB schema metadata.
    pub index_id: i64,
    /// Stable identifier of the single indexed column.
    pub column_id: i64,
}

/// One table shape admitted by the deployable read-only node.
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
    /// Non-unique secondary indexes maintained by the write path, in configured
    /// order. Empty for a table without any declared index.
    pub indexes: Vec<ConfiguredReadIndex>,
}

/// One `<database>.<table>` name whose schema the node reads from the cluster.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadedTableName {
    /// Database name, matched case-insensitively against the stored catalog.
    pub database: String,
    /// Table name, matched case-insensitively against the stored catalog.
    pub table: String,
}

/// Complete startup input consumed by the concurrent SQL node.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    /// Loopback address on which MySQL protocol connections are accepted.
    pub host: IpAddr,
    /// MySQL protocol port. Zero requests an ephemeral test port.
    pub port: u16,
    /// Plaintext PD endpoints in configured order.
    pub pd_endpoints: Vec<String>,
    /// Checked tables exposed to the bounded planner in command-line order.
    pub read_tables: Vec<ConfiguredReadTable>,
    /// Tables whose schema is read from the cluster's own stored catalog at
    /// startup instead of being described on the command line, as
    /// `<database>.<table>` pairs in command-line order.
    pub load_tables: Vec<LoadedTableName>,
    /// Maximum accepted logical MySQL packet size.
    pub max_allowed_packet: usize,
    /// Required immutable native-password account file.
    pub auth_file: PathBuf,
    /// Fixed connection-worker count and accepted-socket queue capacity.
    pub max_connections: usize,
    /// Handshake, idle-command, and socket-write deadline for one connection.
    pub connection_timeout: Duration,
    /// Process-wide maximum ORDER BY LIMIT heap cardinality for the bounded executor.
    pub max_topn_rows: usize,
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
    /// Native password without TLS must never bind a non-loopback address.
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
                write!(
                    formatter,
                    "unsupported store {store:?}; only tikv is executable"
                )
            }
            Self::NonLoopbackHost(host) => write!(
                formatter,
                "refusing non-loopback MySQL listener {host} while TLS is not implemented"
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
        let mut read_tables = Vec::new();
        let mut load_tables = Vec::new();
        let mut max_allowed_packet = None;
        let mut auth_file = None;
        let mut max_connections = None;
        let mut connection_timeout_ms = None;
        let mut max_topn_rows = None;

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
            if argument == "--read-table" {
                read_tables.push(parse_read_table(&mut pending)?);
                continue;
            }
            if argument == "--load-table" {
                let value = pending
                    .next()
                    .filter(|value| !value.starts_with("--"))
                    .ok_or_else(|| NodeConfigError::MissingValue("--load-table".to_owned()))?;
                load_tables.push(parse_loaded_table_name(&value)?);
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
                "--read-table" => {
                    return Err(invalid(
                        option,
                        "expected separate <database> <table> <table-id> <column-count> values",
                    ));
                }
                "--max-allowed-packet" => {
                    set_once(&mut max_allowed_packet, option, value)?;
                }
                "--auth-file" => set_once(&mut auth_file, option, value)?,
                "--max-connections" => set_once(&mut max_connections, option, value)?,
                "--connection-timeout-ms" => {
                    set_once(&mut connection_timeout_ms, option, value)?;
                }
                "--load-table" => load_tables.push(parse_loaded_table_name(&value)?),
                "--max-topn-rows" => set_once(&mut max_topn_rows, option, value)?,
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
        validate_read_tables(&read_tables, &load_tables)?;
        validate_load_tables(&read_tables, &load_tables)?;
        let max_allowed_packet = match max_allowed_packet {
            Some(value) => parse_positive_number("--max-allowed-packet", &value)?,
            None => DEFAULT_MAX_ALLOWED_PACKET,
        };
        let auth_file = PathBuf::from(required(auth_file, "--auth-file")?);
        let max_connections = match max_connections {
            Some(value) => parse_positive_number("--max-connections", &value)?,
            None => DEFAULT_MAX_CONNECTIONS,
        };
        if max_connections > MAX_CONNECTION_WORKERS {
            return Err(invalid("--max-connections", "value must not exceed 256"));
        }
        let connection_timeout = Duration::from_millis(match connection_timeout_ms {
            Some(value) => parse_positive_number("--connection-timeout-ms", &value)?,
            None => DEFAULT_CONNECTION_TIMEOUT_MS,
        });
        let max_topn_rows = match max_topn_rows {
            Some(value) => parse_positive_number("--max-topn-rows", &value)?,
            None => DEFAULT_MAX_TOPN_ROWS,
        };
        if max_topn_rows > MAX_CONFIGURED_TOPN_ROWS {
            return Err(invalid("--max-topn-rows", "value must not exceed 65536"));
        }

        Ok(Self {
            host,
            port,
            pd_endpoints,
            read_tables,
            load_tables,
            max_allowed_packet,
            auth_file,
            max_connections,
            connection_timeout,
            max_topn_rows,
        })
    }

    /// Stable usage text printed by the executable for `--help`.
    #[must_use]
    pub const fn help_text() -> &'static str {
        "Usage: tidb-server --path <pd[,pd...]> \
[--read-table <database> <table> <table-id> <column-count> \
<name>:<id>:<clustered-pk|stored-not-null> \
[<name>:<id>:<clustered-pk|stored-not-null> ...]] \
[--read-table <database> <table> <table-id> <column-count> <column> ...] \
[--load-table <database>.<table> ...] \
[--max-connections <count>] [--connection-timeout-ms <milliseconds>] \
[--max-topn-rows <rows>] \
--auth-file <mode-0600-tsv> \
[--host <loopback-ip>] [-P <port>|--port <port>] [--store tikv] \
[--max-allowed-packet <bytes>]"
    }
}

fn parse_read_table<I>(
    arguments: &mut std::iter::Peekable<I>,
) -> Result<ConfiguredReadTable, NodeConfigError>
where
    I: Iterator<Item = String>,
{
    let database = parse_identifier("--read-table", next_read_table_value(arguments)?)?;
    let table = parse_identifier("--read-table", next_read_table_value(arguments)?)?;
    let table_id = parse_positive_id("--read-table", next_read_table_value(arguments)?)?;
    let column_count: usize =
        parse_positive_number("--read-table", &next_read_table_value(arguments)?)?;
    if column_count > MAX_CONFIGURED_READ_COLUMNS {
        return Err(invalid("--read-table", "column count must not exceed 4096"));
    }
    let mut columns = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        columns.push(parse_column_descriptor(
            "--read-table",
            next_read_table_value(arguments)?,
        )?);
    }
    validate_columns("--read-table", &columns)?;
    let indexes = parse_optional_indexes("--read-table", arguments, &columns)?;
    Ok(ConfiguredReadTable {
        database,
        table,
        table_id,
        columns,
        indexes,
    })
}

/// Parses the optional trailing secondary-index section of a `--read-table`.
///
/// The section is backward compatible: a table with no index simply omits it,
/// so parsing stops as soon as the next token is another option or the end of
/// the arguments. When present, one count precedes that many
/// `name:index_id:column_id` descriptors, each over an existing column.
fn parse_optional_indexes<I>(
    option: &str,
    arguments: &mut std::iter::Peekable<I>,
    columns: &[ConfiguredReadColumn],
) -> Result<Vec<ConfiguredReadIndex>, NodeConfigError>
where
    I: Iterator<Item = String>,
{
    let has_index_section = matches!(arguments.peek(), Some(value) if !value.starts_with('-'));
    if !has_index_section {
        return Ok(Vec::new());
    }
    let index_count: usize = parse_number(option, &next_read_table_value(arguments)?)?;
    if index_count > MAX_CONFIGURED_READ_INDEXES {
        return Err(invalid(option, "index count must not exceed 64"));
    }
    let mut indexes = Vec::with_capacity(index_count);
    for _ in 0..index_count {
        indexes.push(parse_index_descriptor(
            option,
            next_read_table_value(arguments)?,
            columns,
        )?);
    }
    Ok(indexes)
}

/// Parses one `name:index_id:column_id` non-unique index descriptor.
fn parse_index_descriptor(
    option: &str,
    value: String,
    columns: &[ConfiguredReadColumn],
) -> Result<ConfiguredReadIndex, NodeConfigError> {
    let fields: Vec<&str> = value.split(':').collect();
    let [name, index_id, column_id] = fields.as_slice() else {
        return Err(invalid(
            option,
            "index descriptor must be name:index_id:column_id",
        ));
    };
    let name = parse_identifier(option, (*name).to_owned())?;
    let index_id = parse_positive_id(option, (*index_id).to_owned())?;
    let column_id = parse_positive_id(option, (*column_id).to_owned())?;
    if !columns.iter().any(|column| column.id == column_id) {
        return Err(invalid(
            option,
            "index column id does not match any configured column",
        ));
    }
    Ok(ConfiguredReadIndex {
        name,
        index_id,
        column_id,
    })
}

fn next_read_table_value<I>(
    arguments: &mut std::iter::Peekable<I>,
) -> Result<String, NodeConfigError>
where
    I: Iterator<Item = String>,
{
    match arguments.peek() {
        Some(value) if !value.starts_with('-') => Ok(arguments
            .next()
            .expect("peeked read-table value must remain available")),
        _ => Err(NodeConfigError::MissingValue("--read-table".to_owned())),
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

fn parse_column_descriptor(
    option: &str,
    value: String,
) -> Result<ConfiguredReadColumn, NodeConfigError> {
    let mut fields = value.split(':');
    let (Some(name), Some(id), Some(kind)) = (fields.next(), fields.next(), fields.next()) else {
        return Err(invalid(
            option,
            "expected <name>:<id>:<kind>[:<char-length>]",
        ));
    };
    // Only `stored-char-not-null` carries a fourth `:<char-length>` field.
    let extra = fields.next();
    if fields.next().is_some() {
        return Err(invalid(
            option,
            "too many ':'-separated fields in column descriptor",
        ));
    }
    let name = parse_identifier(option, name.to_owned())?;
    let id = parse_positive_id(option, id.to_owned())?;
    let kind = match (kind, extra) {
        ("clustered-pk", None) => ConfiguredReadColumnKind::ClusteredPrimaryKey,
        ("stored-not-null", None) => ConfiguredReadColumnKind::StoredNotNull,
        ("stored-int-not-null", None) => ConfiguredReadColumnKind::StoredIntNotNull,
        ("stored-char-not-null", Some(length)) => ConfiguredReadColumnKind::StoredCharNotNull {
            max_length: parse_char_length(option, length)?,
        },
        ("stored-char-not-null", None) => {
            return Err(invalid(
                option,
                "stored-char-not-null requires a :<char-length> field",
            ));
        }
        (_, Some(_)) => {
            return Err(invalid(
                option,
                "only stored-char-not-null takes a :<char-length> field",
            ));
        }
        _ => {
            return Err(invalid(
                option,
                "column kind must be clustered-pk, stored-not-null, stored-int-not-null, or stored-char-not-null:<N>",
            ));
        }
    };
    Ok(ConfiguredReadColumn { name, id, kind })
}

/// Parses and range-checks a `CHAR(N)` length: a positive integer in MySQL's
/// `1..=255` character-count range.
fn parse_char_length(option: &str, value: &str) -> Result<u32, NodeConfigError> {
    let length: u32 = value
        .parse()
        .map_err(|_| invalid(option, "char length must be a positive integer"))?;
    if !(1..=255).contains(&length) {
        return Err(invalid(option, "char length must be between 1 and 255"));
    }
    Ok(length)
}

fn validate_columns(option: &str, columns: &[ConfiguredReadColumn]) -> Result<(), NodeConfigError> {
    if columns.is_empty() {
        return Err(invalid(option, "at least one column is required"));
    }

    let mut names = HashSet::with_capacity(columns.len());
    let mut ids = HashSet::with_capacity(columns.len());
    let mut clustered_primary_keys = 0;
    for column in columns {
        if !names.insert(column.name.to_lowercase()) {
            return Err(invalid(
                option,
                "column names must be unique case-insensitively",
            ));
        }
        if !ids.insert(column.id) {
            return Err(invalid(option, "column IDs must be unique"));
        }
        if column.kind == ConfiguredReadColumnKind::ClusteredPrimaryKey {
            clustered_primary_keys += 1;
        }
    }
    if clustered_primary_keys != 1 {
        return Err(invalid(option, "exactly one column must be clustered-pk"));
    }
    Ok(())
}

/// Splits `<database>.<table>`, the only shape `--load-table` accepts: the
/// cluster's stored catalog supplies everything else about the table.
fn parse_loaded_table_name(value: &str) -> Result<LoadedTableName, NodeConfigError> {
    let Some((database, table)) = value.split_once('.') else {
        return Err(invalid("--load-table", "expected <database>.<table>"));
    };
    Ok(LoadedTableName {
        database: parse_identifier("--load-table", database.to_owned())?,
        table: parse_identifier("--load-table", table.to_owned())?,
    })
}

fn validate_load_tables(
    read_tables: &[ConfiguredReadTable],
    load_tables: &[LoadedTableName],
) -> Result<(), NodeConfigError> {
    if load_tables.len() > MAX_CONFIGURED_READ_TABLES {
        return Err(invalid(
            "--load-table",
            "loaded table count must not exceed two",
        ));
    }
    let mut names = HashSet::with_capacity(load_tables.len());
    for loaded in load_tables {
        let name = (loaded.database.to_lowercase(), loaded.table.to_lowercase());
        if !names.insert(name.clone()) {
            return Err(invalid(
                "--load-table",
                "loaded table names must be unique case-insensitively",
            ));
        }
        if read_tables
            .iter()
            .any(|table| (table.database.to_lowercase(), table.table.to_lowercase()) == name)
        {
            return Err(invalid(
                "--load-table",
                "a table cannot be both described on the command line and loaded",
            ));
        }
    }
    Ok(())
}

fn validate_read_tables(
    tables: &[ConfiguredReadTable],
    load_tables: &[LoadedTableName],
) -> Result<(), NodeConfigError> {
    if tables.is_empty() && load_tables.is_empty() {
        return Err(NodeConfigError::MissingOption("--read-table"));
    }
    if tables.len() > MAX_CONFIGURED_READ_TABLES {
        return Err(invalid(
            "--read-table",
            "configured table count must not exceed two",
        ));
    }
    let mut names = HashSet::with_capacity(tables.len());
    let mut ids = HashSet::with_capacity(tables.len());
    for table in tables {
        if !names.insert((table.database.to_lowercase(), table.table.to_lowercase())) {
            return Err(invalid(
                "--read-table",
                "table names must be unique case-insensitively within each database",
            ));
        }
        if !ids.insert(table.table_id) {
            return Err(invalid("--read-table", "table IDs must be unique"));
        }
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

#[cfg(test)]
mod tests {
    use super::{parse_column_descriptor, ConfiguredReadColumnKind};

    /// The status-publication descriptor (`descriptor_name`) must round-trip
    /// back through `parse_column_descriptor` — including the CHAR length —
    /// so a published node's shape re-parses to the same columns.
    #[test]
    fn descriptor_name_round_trips_through_parse() {
        let kinds = [
            ConfiguredReadColumnKind::ClusteredPrimaryKey,
            ConfiguredReadColumnKind::StoredNotNull,
            ConfiguredReadColumnKind::StoredIntNotNull,
            ConfiguredReadColumnKind::StoredCharNotNull { max_length: 120 },
            ConfiguredReadColumnKind::StoredCharNotNull { max_length: 1 },
            ConfiguredReadColumnKind::StoredCharNotNull { max_length: 255 },
        ];
        for kind in kinds {
            let descriptor = format!("c:3:{}", kind.descriptor_name());
            let parsed = parse_column_descriptor("--read-table", descriptor).unwrap();
            assert_eq!(parsed.name, "c");
            assert_eq!(parsed.id, 3);
            assert_eq!(parsed.kind, kind, "round-trip of {kind:?}");
        }
    }
}
