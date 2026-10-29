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

//! Go `sql_rule.go`: the named rules that decide whether a statement is
//! restricted under SEM.
//!
//! The rules read a small, fixed part of the AST. [`StmtView`] is that part,
//! narrowed so this crate does not depend on the parser (see the module
//! boundaries).

// boundary: `pkg/parser/ast.TableOptionType`, narrowed to the options
// `checkTTLOptions` tests.
/// The table options SEM's TTL rule looks for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableOptionType {
    /// Go `ast.TableOptionTTL`.
    Ttl,
    /// Go `ast.TableOptionTTLEnable`.
    TtlEnable,
    /// Go `ast.TableOptionTTLJobInterval`.
    TtlJobInterval,
    /// Any option SEM does not restrict.
    Other,
}

// boundary: `pkg/parser/ast.AlterTableType`, narrowed to the specs SEM tests.
/// The `ALTER TABLE` spec kinds SEM's rules look for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlterTableType {
    /// Go `ast.AlterTableRemoveTTL`.
    RemoveTtl,
    /// Go `ast.AlterTableOption`.
    Option,
    /// Go `ast.AlterTableAttributes`.
    Attributes,
    /// Go `ast.AlterTablePartitionAttributes`.
    PartitionAttributes,
    /// Any spec SEM does not restrict.
    Other,
}

/// Go `ast.AlterTableSpec`, narrowed to `Tp` and `Options`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTableSpec {
    /// Go `AlterTableSpec.Tp`.
    pub tp: AlterTableType,
    /// Go `AlterTableSpec.Options`.
    pub options: Vec<TableOptionType>,
}

/// The statement shapes SEM's SQL rules distinguish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StmtKind {
    /// Go `*ast.CreateTableStmt`, carrying `Options`.
    CreateTable {
        /// Go `CreateTableStmt.Options`.
        options: Vec<TableOptionType>,
    },
    /// Go `*ast.AlterTableStmt`, carrying `Specs`.
    AlterTable {
        /// Go `AlterTableStmt.Specs`.
        specs: Vec<AlterTableSpec>,
    },
    /// Go `*ast.SelectStmt`, carrying whether `SelectIntoOpt` is set.
    Select {
        /// Whether Go's `SelectStmt.SelectIntoOpt` is non-nil.
        select_into: bool,
    },
    /// Go `*ast.ImportIntoStmt`.
    ImportInto {
        /// Whether Go's `ImportIntoStmt.Select` is non-nil.
        from_select: bool,
        /// Go `ImportIntoStmt.Path`.
        path: String,
    },
    /// Go `*ast.LoadDataStmt`.
    LoadData {
        /// Whether Go's `LoadDataStmt.FileLocRef` is `ast.FileLocClient`.
        file_loc_client: bool,
        /// Go `LoadDataStmt.Path`.
        path: String,
    },
    /// Any other statement.
    Other,
}

// boundary: `pkg/parser/ast.StmtNode`, narrowed to what SEM reads.
/// One statement, as SEM's rules see it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StmtView {
    /// Go `ast.StmtNode.SEMCommand()`.
    pub sem_command: String,
    /// The statement's shape.
    pub kind: StmtKind,
}

impl StmtView {
    /// A statement of the given kind with no `SEMCommand`.
    #[must_use]
    pub fn new(kind: StmtKind) -> Self {
        Self {
            sem_command: String::new(),
            kind,
        }
    }

    /// Go `ast.StmtNode.SEMCommand()`.
    #[must_use]
    pub fn sem_command(&self) -> &str {
        &self.sem_command
    }
}

/// Go `SQLRule`: decides whether a statement should be restricted.
pub type SQLRule = fn(&StmtView) -> bool;

/// Go `checkTTLOptions`.
fn check_ttl_options(options: &[TableOptionType]) -> bool {
    options.iter().any(|option| {
        matches!(
            option,
            TableOptionType::Ttl | TableOptionType::TtlEnable | TableOptionType::TtlJobInterval
        )
    })
}

/// Go `sqlRuleNameMap` lookup.
#[must_use]
pub(super) fn sql_rule_by_name(name: &str) -> Option<SQLRule> {
    match name {
        "time_to_live" => Some(time_to_live_sql_rule),
        "alter_table_attributes" => Some(alter_table_attributes_rule),
        "import_with_external_id" => Some(import_with_external_id_rule),
        "select_into_file" => Some(select_into_file_rule),
        "import_from_local" => Some(import_from_local_rule),
        _ => None,
    }
}

/// Go `TimeToLiveSQLRule`: true when the statement touches TTL options.
pub fn time_to_live_sql_rule(stmt: &StmtView) -> bool {
    match &stmt.kind {
        StmtKind::CreateTable { options } => check_ttl_options(options),
        StmtKind::AlterTable { specs } => specs.iter().any(|spec| match spec.tp {
            AlterTableType::RemoveTtl => true,
            AlterTableType::Option => check_ttl_options(&spec.options),
            _ => false,
        }),
        _ => false,
    }
}

/// Go `AlterTableAttributesRule`: true when the statement alters table or
/// partition attributes.
pub fn alter_table_attributes_rule(stmt: &StmtView) -> bool {
    match &stmt.kind {
        StmtKind::AlterTable { specs } => specs.iter().any(|spec| {
            matches!(
                spec.tp,
                AlterTableType::Attributes | AlterTableType::PartitionAttributes
            )
        }),
        _ => false,
    }
}

/// Go `ImportWithExternalIDRule`, kept for compatibility with existing SEM
/// configs. Import external ID checks are handled outside the restricted SQL
/// rule list.
pub fn import_with_external_id_rule(_stmt: &StmtView) -> bool {
    false
}

/// Go `SelectIntoFileRule`: true for `SELECT ... INTO OUTFILE`.
pub fn select_into_file_rule(stmt: &StmtView) -> bool {
    matches!(stmt.kind, StmtKind::Select { select_into: true })
}

/// Go `ImportFromLocalRule`: true for `IMPORT INTO` or `LOAD DATA ... INFILE`
/// reading a local file.
pub fn import_from_local_rule(stmt: &StmtView) -> bool {
    match &stmt.kind {
        StmtKind::ImportInto { from_select, path } => {
            // Allow `IMPORT INTO ... FROM SELECT ...`, whose path is empty.
            if *from_select {
                return false;
            }
            is_local_url(path)
        }
        StmtKind::LoadData {
            file_loc_client,
            path,
        } => {
            if *file_loc_client {
                return false;
            }
            is_local_url(path)
        }
        _ => false,
    }
}

/// Go `objstore.IsLocal(u)` composed with `url.Parse`, inlined: a URL is local
/// when its scheme is `local`, `file`, or absent.
#[must_use]
fn is_local_url(raw: &str) -> bool {
    parsed_url_scheme(raw).is_some_and(|scheme| {
        scheme.is_none_or(|scheme| {
            scheme.eq_ignore_ascii_case("local") || scheme.eq_ignore_ascii_case("file")
        })
    })
}

fn valid_url_escapes(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return false;
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    true
}

fn valid_authority(authority: &str) -> bool {
    let host_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if let Some(open) = host_port.find('[') {
        let Some(close_offset) = host_port[open + 1..].find(']') else {
            return false;
        };
        let close = open + 1 + close_offset;
        if host_port[close + 1..].contains(['[', ']']) {
            return false;
        }
        let suffix = &host_port[close + 1..];
        return suffix.is_empty()
            || suffix
                .strip_prefix(':')
                .is_some_and(|port| port.bytes().all(|byte| byte.is_ascii_digit()));
    }
    if host_port.contains(']') || host_port.contains('%') {
        return false;
    }
    match host_port.rsplit_once(':') {
        None => true,
        Some((host, port)) => !host.contains(':') && port.bytes().all(|byte| byte.is_ascii_digit()),
    }
}

/// The `Scheme` and parse-error portion of Go `net/url.Parse` used by
/// `ImportFromLocalRule`. Go canonicalizes the parsed scheme before
/// `objstore.IsLocal` compares it with `local` and `file`.
fn parsed_url_scheme(raw: &str) -> Option<Option<&str>> {
    if raw.bytes().any(|byte| byte.is_ascii_control()) {
        return None;
    }

    let without_fragment = raw.split_once('#').map_or(raw, |(head, _)| head);
    let path_and_authority = without_fragment
        .split_once('?')
        .map_or(without_fragment, |(head, _)| head);
    if !valid_url_escapes(path_and_authority)
        || raw
            .split_once('#')
            .is_some_and(|(_, fragment)| !valid_url_escapes(fragment))
    {
        return None;
    }

    let mut scheme = None;
    for (index, byte) in raw.bytes().enumerate() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' if index == 0 => {}
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'+' | b'-' | b'.' if index > 0 => {}
            b':' if index > 0 => {
                scheme = Some(&raw[..index]);
                break;
            }
            _ => break,
        }
    }

    if scheme.is_none() && !path_and_authority.starts_with("//") {
        let first_segment = path_and_authority.split('/').next().unwrap_or_default();
        if first_segment.contains(':') {
            return None;
        }
    }

    let after_scheme = scheme.map_or(raw, |scheme| &raw[scheme.len() + 1..]);
    if let Some(authority_and_path) = after_scheme.strip_prefix("//") {
        let authority = authority_and_path
            .split(['/', '?', '#'])
            .next()
            .unwrap_or_default();
        if !valid_authority(authority) {
            return None;
        }
    }

    Some(scheme)
}
