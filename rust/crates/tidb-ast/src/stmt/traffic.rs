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

//! AST and restore contracts translated from `pkg/parser/traffic_parser.go`.

use std::collections::HashSet;

use crate::util::{back_quote, escape_string_literal};

/// A traffic-capture, traffic-replay, or traffic-job command.
///
/// Capture and replay use distinct option enums so options that Go ignores
/// for the other operation cannot be constructed accidentally in Rust.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrafficStmt {
    /// `TRAFFIC CAPTURE TO 'path' [option ...]`.
    Capture {
        /// Capture destination.
        dir: String,
        /// Options in source order.
        options: Vec<TrafficCaptureOption>,
    },
    /// `TRAFFIC REPLAY FROM 'path' [option ...]`.
    Replay {
        /// Replay source.
        dir: String,
        /// Options in source order.
        options: Vec<TrafficReplayOption>,
    },
    /// `SHOW TRAFFIC JOBS`.
    ShowJobs,
    /// `CANCEL TRAFFIC JOBS`.
    CancelJobs,
}

/// An option accepted by `TRAFFIC CAPTURE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrafficCaptureOption {
    /// `DURATION = 'value'`.
    Duration(String),
    /// `ENCRYPTION_METHOD = 'value'`.
    EncryptionMethod(String),
    /// `COMPRESS = TRUE|FALSE`.
    Compress(bool),
}

/// An option accepted by `TRAFFIC REPLAY`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrafficReplayOption {
    /// `USER = 'value'`.
    User(String),
    /// `PASSWORD = 'value'`.
    Password(String),
    /// `SPEED = value`, retaining Go's token spelling.
    Speed(String),
    /// `READONLY = TRUE|FALSE`.
    ReadOnly(bool),
}

impl TrafficStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        match self {
            Self::Capture { dir, options } => {
                out.push_str("TRAFFIC CAPTURE TO ");
                push_plain_string(out, dir);
                for option in options {
                    out.push(' ');
                    option.restore_into(out);
                }
            }
            Self::Replay { dir, options } => {
                out.push_str("TRAFFIC REPLAY FROM ");
                push_plain_string(out, dir);
                for option in options {
                    out.push(' ');
                    option.restore_into(out);
                }
            }
            Self::ShowJobs => out.push_str("SHOW TRAFFIC JOBS"),
            Self::CancelJobs => out.push_str("CANCEL TRAFFIC JOBS"),
        }
    }

    /// Restores logging-safe SQL, matching Go's `TrafficStmt.SecureText`.
    pub fn secure_text(&self) -> String {
        let redacted = match self {
            Self::Capture { dir, options } => Self::Capture {
                dir: redact_url(dir),
                options: options.clone(),
            },
            Self::Replay { dir, options } => Self::Replay {
                dir: redact_url(dir),
                options: options
                    .iter()
                    .map(|option| match option {
                        TrafficReplayOption::Password(_) => {
                            TrafficReplayOption::Password("xxxxxx".to_string())
                        }
                        other => other.clone(),
                    })
                    .collect(),
            },
            Self::ShowJobs => Self::ShowJobs,
            Self::CancelJobs => Self::CancelJobs,
        };
        let mut out = String::new();
        redacted.restore_into(&mut out);
        out
    }
}

impl TrafficCaptureOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Duration(value) => {
                out.push_str("DURATION = ");
                push_plain_string(out, value);
            }
            Self::EncryptionMethod(value) => {
                out.push_str("ENCRYPTION_METHOD = ");
                push_plain_string(out, value);
            }
            Self::Compress(value) => {
                out.push_str("COMPRESS = ");
                out.push_str(if *value { "TRUE" } else { "FALSE" });
            }
        }
    }
}

impl TrafficReplayOption {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::User(value) => {
                out.push_str("USER = ");
                push_plain_string(out, value);
            }
            Self::Password(value) => {
                out.push_str("PASSWORD = ");
                push_plain_string(out, value);
            }
            Self::Speed(value) => {
                out.push_str("SPEED = ");
                out.push_str(value);
            }
            Self::ReadOnly(value) => {
                out.push_str("READONLY = ");
                out.push_str(if *value { "TRUE" } else { "FALSE" });
            }
        }
    }
}

/// A `REFRESH STATS` operation translated from the same Go source unit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshStatsStmt {
    /// Non-empty object list in source order.
    pub objects: Vec<StatsObject>,
    /// Explicit refresh strategy, when present.
    pub mode: Option<RefreshStatsMode>,
    /// Whether `CLUSTER` was written.
    pub cluster_wide: bool,
}

/// Explicit `REFRESH STATS` strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshStatsMode {
    /// Lite statistics initialization.
    Lite,
    /// Full statistics initialization.
    Full,
}

/// A scoped statistics object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatsObject {
    /// A table, optionally qualified by database.
    Table {
        /// Optional database qualifier.
        database: Option<String>,
        /// Table name.
        table: String,
    },
    /// Every table in one database (`database.*`).
    Database(String),
    /// Every database and table (`*.*`).
    Global,
}

impl RefreshStatsStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("REFRESH STATS ");
        for (index, object) in self.objects.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            object.restore_into(out);
        }
        if let Some(mode) = self.mode {
            out.push(' ');
            out.push_str(match mode {
                RefreshStatsMode::Lite => "LITE",
                RefreshStatsMode::Full => "FULL",
            });
        }
        if self.cluster_wide {
            out.push_str(" CLUSTER");
        }
    }

    /// Removes duplicate and shadowed targets using Go's case-insensitive
    /// `RefreshStatsStmt.Dedup` rules while retaining source order.
    pub fn dedup(&mut self) {
        let mut databases = HashSet::new();
        let mut tables = HashSet::new();
        let mut result = Vec::with_capacity(self.objects.len());

        for object in std::mem::take(&mut self.objects) {
            match &object {
                StatsObject::Global => {
                    result.clear();
                    result.push(object);
                    break;
                }
                StatsObject::Database(database) => {
                    let database_key = database.to_lowercase();
                    if !databases.insert(database_key.clone()) {
                        continue;
                    }
                    for existing in &result {
                        if let StatsObject::Table {
                            database: Some(existing_database),
                            table,
                        } = existing
                        {
                            if existing_database.to_lowercase() == database_key {
                                tables.remove(&(database_key.clone(), table.to_lowercase()));
                            }
                        }
                    }
                    result.retain(|existing| {
                        !matches!(existing,
                            StatsObject::Table { database: Some(existing_database), .. }
                                if existing_database.to_lowercase() == database_key)
                    });
                    result.push(object);
                }
                StatsObject::Table { database, table } => {
                    let database_key = database
                        .as_ref()
                        .map(|name| name.to_lowercase())
                        .unwrap_or_default();
                    if !database_key.is_empty() && databases.contains(&database_key) {
                        continue;
                    }
                    if tables.insert((database_key, table.to_lowercase())) {
                        result.push(object);
                    }
                }
            }
        }

        self.objects = result;
    }
}

impl StatsObject {
    fn restore_into(&self, out: &mut String) {
        match self {
            Self::Table { database, table } => {
                if let Some(database) = database {
                    out.push_str(&back_quote(database));
                    out.push('.');
                }
                out.push_str(&back_quote(table));
            }
            Self::Database(database) => {
                out.push_str(&back_quote(database));
                out.push_str(".*");
            }
            Self::Global => out.push_str("*.*"),
        }
    }
}

fn push_plain_string(out: &mut String, value: &str) {
    out.push('\'');
    out.push_str(&escape_string_literal(value));
    out.push('\'');
}

fn redact_url(value: &str) -> String {
    let Some((scheme, _)) = value.split_once("://") else {
        return value.to_string();
    };
    let sensitive_keys: &[&str] = match scheme.to_ascii_lowercase().as_str() {
        "s3" | "ks3" | "oss" => &["access-key", "secret-access-key", "session-token"],
        "azure" | "azblob" => &["account-key", "encryption-key", "sas-token"],
        _ => return value.to_string(),
    };
    let Some((base, query_and_fragment)) = value.split_once('?') else {
        return value.to_string();
    };
    let (query, fragment) = query_and_fragment
        .split_once('#')
        .map_or((query_and_fragment, None), |(query, fragment)| {
            (query, Some(fragment))
        });
    let mut fields: Vec<(String, String)> = query
        .split('&')
        .filter(|field| !field.is_empty())
        .map(|field| {
            let (key, value) = field.split_once('=').unwrap_or((field, ""));
            let normalized = key.to_ascii_lowercase().replace('_', "-");
            let value = if sensitive_keys.contains(&normalized.as_str()) {
                "xxxxxx"
            } else {
                value
            };
            (key.to_string(), value.to_string())
        })
        .collect();
    fields.sort();

    let mut result = format!("{base}?");
    for (index, (key, value)) in fields.iter().enumerate() {
        if index > 0 {
            result.push('&');
        }
        result.push_str(key);
        result.push('=');
        result.push_str(value);
    }
    if let Some(fragment) = fragment {
        result.push('#');
        result.push_str(fragment);
    }
    result
}
