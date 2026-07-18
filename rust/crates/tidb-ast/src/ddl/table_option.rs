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

//! `CREATE`/`ALTER TABLE` table-option AST payload and restore boundary.

use crate::util::{back_quote, escape_string_literal, push_name_path};
use crate::{Expr, RestoreContext};

const FEATURE_AFFINITY: &str = "affinity";
const FEATURE_TTL: &str = "ttl";

/// One trailing `CREATE TABLE` table option. Each variant's value is
/// captured already canonicalized the way it restores (`Engine`'s value
/// keeps the exact case written — MySQL/TiDB never canonicalize it — while
/// `CharacterSet`/`Collate` uppercase, matching real TiDB's restore; see
/// `godump restore` probes behind this design).
#[derive(Debug, Clone, PartialEq)]
pub enum TableOption {
    /// `ENGINE [=] name` — the storage engine name, case preserved verbatim.
    Engine(String),
    /// `AUTO_INCREMENT [=] n` — the starting auto-increment value.
    AutoIncrement(String),
    /// `[DEFAULT] {CHARACTER SET | CHARSET} [=] name`.
    CharacterSet(String),
    /// `[DEFAULT] COLLATE [=] name`.
    Collate(String),
    /// `COMMENT [=] '...'`.
    Comment(String),
    /// `ROW_FORMAT [=] name`.
    RowFormat(String),
    /// `KEY_BLOCK_SIZE [=] n`.
    KeyBlockSize(String),
    /// `COMPRESSION [=] '...'`.
    Compression(String),
    /// `TABLESPACE [=] name`.
    Tablespace(String),
    /// `SHARD_ROW_ID_BITS [=] n`.
    ShardRowIdBits(String),
    /// `PRE_SPLIT_REGIONS [=] n`.
    PreSplitRegions(String),
    /// `AUTO_ID_CACHE [=] n`.
    AutoIdCache(String),
    /// `MAX_ROWS [=] n`.
    MaxRows(String),
    /// `MIN_ROWS [=] n`.
    MinRows(String),
    /// `AVG_ROW_LENGTH [=] n`.
    AvgRowLength(String),
    /// `CHECKSUM [=] n`.
    Checksum(String),
    /// `DELAY_KEY_WRITE [=] n`.
    DelayKeyWrite(String),
    /// `STATS_PERSISTENT [=] (DEFAULT | n)`.
    StatsPersistent,
    /// `PACK_KEYS [=] (DEFAULT | n)`.
    PackKeys,
    /// `AUTO_RANDOM_BASE [=] n`.
    AutoRandomBase(String),
    /// `FORCE AUTO_RANDOM_BASE [=] n` in an `ALTER TABLE` option list.
    ///
    /// Go stores the FORCE bit on the same `TableOptionAutoRandomBase`
    /// payload. Keeping it explicit in the Rust enum prevents a FORCE
    /// modifier from being silently dropped while leaving CREATE TABLE's
    /// ordinary `AUTO_RANDOM_BASE` option unchanged.
    ForceAutoRandomBase(String),
    /// `CONNECTION [=] '...'`.
    Connection(String),
    /// `PASSWORD [=] '...'`.
    Password(String),
    /// `STATS_AUTO_RECALC [=] (DEFAULT | n)`.
    StatsAutoRecalc(String),
    /// `NODEGROUP [=] n`.
    Nodegroup(String),
    /// `DATA DIRECTORY [=] '...'`.
    DataDirectory(String),
    /// `INDEX DIRECTORY [=] '...'`.
    IndexDirectory(String),
    /// `AUTOEXTEND_SIZE [=] n`.
    AutoextendSize(String),
    /// MariaDB compatibility option `PAGE_CHECKSUM [=] n`.
    PageChecksum(String),
    /// MariaDB compatibility option `PAGE_COMPRESSED [=] n`.
    PageCompressed(String),
    /// MariaDB compatibility option `PAGE_COMPRESSION_LEVEL [=] n`.
    PageCompressionLevel(String),
    /// MariaDB compatibility option `TRANSACTIONAL [=] n`.
    Transactional(String),
    /// MariaDB compatibility option `IETF_QUOTES [=] value`.
    IetfQuotes(String),
    /// MariaDB compatibility option `SEQUENCE [=] n`.
    Sequence(String),
    /// `UNION [=] (table_name [, table_name ...])` for MERGE tables.
    Union(Vec<Vec<String>>),
    /// `INSERT_METHOD [=] name`.
    InsertMethod(String),
    /// `[DEFAULT] ENCRYPTION [=] '...'`.
    Encryption(String),
    /// `SECONDARY_ENGINE [=] value`.
    SecondaryEngine(String),
    /// `SECONDARY_ENGINE [=] NULL`.
    SecondaryEngineNull,
    /// `SECONDARY_ENGINE_ATTRIBUTE [=] '...'`.
    SecondaryEngineAttribute(String),
    /// `ENGINE_ATTRIBUTE [=] value`.
    EngineAttribute(String),
    /// `[DEFAULT] PLACEMENT POLICY (SET DEFAULT | [=] (DEFAULT | name))`.
    PlacementPolicy(String),
    /// `STATS_BUCKETS [=] n`.
    StatsBuckets(String),
    /// `STATS_TOPN [=] n`.
    StatsTopN(String),
    /// `STATS_SAMPLE_RATE [=] n`.
    StatsSampleRate(String),
    /// `STATS_COL_CHOICE [=] '...'`.
    StatsColChoice(String),
    /// `STATS_COL_LIST [=] '...'`.
    StatsColList(String),
    /// `TTL [=] col + INTERVAL value unit`.
    Ttl {
        /// The row-time column name (restored back-quoted).
        column: String,
        /// The interval magnitude (a literal expression, restored as written).
        value: Box<Expr>,
        /// The time-unit keyword, canonically uppercased (`DAY`, ...).
        unit: String,
    },
    /// `TTL_ENABLE [=] ('ON' | 'OFF')`.
    TtlEnable(bool),
    /// `TTL_JOB_INTERVAL [=] '...'`.
    TtlJobInterval(String),
    /// `AFFINITY [=] 'level'`.
    ///
    /// Go deliberately keeps the literal spelling here. Semantic validation
    /// (`none`, `table`, or `partition`) belongs to its later DDL layer, not
    /// to the shared parser production.
    Affinity(String),
}

impl TableOption {
    /// Restores this one option without a leading separator. CREATE TABLE
    /// and ALTER TABLE own their different option-list separators, while
    /// the Go `TableOption.Restore` text itself is shared.
    pub(crate) fn restore_into(&self, out: &mut String) {
        self.restore_into_with_context(out, RestoreContext::default());
    }

    /// Restores one option under the statement-wide formatting context.
    ///
    /// Most options have one spelling. AFFINITY is TiDB-only syntax, so Go
    /// wraps exactly that option in a feature-special comment when requested.
    pub(crate) fn restore_into_with_context(&self, out: &mut String, context: RestoreContext) {
        match self {
            Self::Engine(v) => push_table_option_bare(out, "ENGINE", v),
            Self::AutoIncrement(v) => push_table_option_bare(out, "AUTO_INCREMENT", v),
            Self::CharacterSet(v) => push_table_option_bare(out, "DEFAULT CHARACTER SET", v),
            Self::Collate(v) => push_table_option_bare(out, "DEFAULT COLLATE", v),
            Self::Comment(v) => push_table_option_string(out, "COMMENT", v),
            Self::RowFormat(v) => push_table_option_bare(out, "ROW_FORMAT", v),
            Self::KeyBlockSize(v) => push_table_option_bare(out, "KEY_BLOCK_SIZE", v),
            Self::Compression(v) => push_table_option_string(out, "COMPRESSION", v),
            Self::Tablespace(v) => push_table_option_name(out, "TABLESPACE", v),
            Self::ShardRowIdBits(v) => push_table_option_bare(out, "SHARD_ROW_ID_BITS", v),
            Self::PreSplitRegions(v) => push_table_option_bare(out, "PRE_SPLIT_REGIONS", v),
            Self::AutoIdCache(v) => push_table_option_bare(out, "AUTO_ID_CACHE", v),
            Self::MaxRows(v) => push_table_option_bare(out, "MAX_ROWS", v),
            Self::MinRows(v) => push_table_option_bare(out, "MIN_ROWS", v),
            Self::AvgRowLength(v) => push_table_option_bare(out, "AVG_ROW_LENGTH", v),
            Self::Checksum(v) => push_table_option_bare(out, "CHECKSUM", v),
            Self::DelayKeyWrite(v) => push_table_option_bare(out, "DELAY_KEY_WRITE", v),
            Self::StatsPersistent => out.push_str(
                "STATS_PERSISTENT = DEFAULT /* TableOptionStatsPersistent is not supported */ ",
            ),
            Self::PackKeys => {
                out.push_str("PACK_KEYS = DEFAULT /* TableOptionPackKeys is not supported */ ")
            }
            Self::AutoRandomBase(v) => push_table_option_bare(out, "AUTO_RANDOM_BASE", v),
            Self::ForceAutoRandomBase(v) => {
                out.push_str("FORCE AUTO_RANDOM_BASE = ");
                out.push_str(v);
            }
            Self::Connection(v) => push_table_option_string(out, "CONNECTION", v),
            Self::Password(v) => push_table_option_string(out, "PASSWORD", v),
            Self::StatsAutoRecalc(v) => push_table_option_bare(out, "STATS_AUTO_RECALC", v),
            Self::Nodegroup(v) => push_table_option_bare(out, "NODEGROUP", v),
            Self::DataDirectory(v) => push_table_option_string(out, "DATA DIRECTORY", v),
            Self::IndexDirectory(v) => push_table_option_string(out, "INDEX DIRECTORY", v),
            Self::AutoextendSize(v) => push_table_option_bare(out, "AUTOEXTEND_SIZE", v),
            Self::PageChecksum(v) => push_table_option_bare(out, "PAGE_CHECKSUM", v),
            Self::PageCompressed(v) => push_table_option_bare(out, "PAGE_COMPRESSED", v),
            Self::PageCompressionLevel(v) => {
                push_table_option_bare(out, "PAGE_COMPRESSION_LEVEL", v)
            }
            Self::Transactional(v) => push_table_option_bare(out, "TRANSACTIONAL", v),
            Self::IetfQuotes(v) => push_table_option_bare(out, "IETF_QUOTES", v),
            Self::Sequence(v) => push_table_option_bare(out, "SEQUENCE", v),
            Self::Union(tables) => {
                out.push_str("UNION = (");
                for (index, table) in tables.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    push_name_path(out, table);
                }
                out.push(')');
            }
            Self::InsertMethod(v) => push_table_option_bare(out, "INSERT_METHOD", v),
            Self::Encryption(v) => push_table_option_string(out, "ENCRYPTION", v),
            Self::SecondaryEngine(v) => push_table_option_string(out, "SECONDARY_ENGINE", v),
            Self::SecondaryEngineNull => out.push_str("SECONDARY_ENGINE = NULL"),
            Self::SecondaryEngineAttribute(v) => {
                push_table_option_string(out, "SECONDARY_ENGINE_ATTRIBUTE", v)
            }
            Self::EngineAttribute(v) => push_table_option_string(out, "ENGINE_ATTRIBUTE", v),
            Self::PlacementPolicy(v) => push_table_option_name(out, "PLACEMENT POLICY", v),
            Self::StatsBuckets(v) => push_table_option_bare(out, "STATS_BUCKETS", v),
            Self::StatsTopN(v) => push_table_option_bare(out, "STATS_TOPN", v),
            Self::StatsSampleRate(v) => push_table_option_bare(out, "STATS_SAMPLE_RATE", v),
            Self::StatsColChoice(v) => push_table_option_string(out, "STATS_COL_CHOICE", v),
            Self::StatsColList(v) => push_table_option_string(out, "STATS_COL_LIST", v),
            Self::Ttl {
                column,
                value,
                unit,
            } => {
                context.write_with_tidb_special_comment(out, FEATURE_TTL, |out| {
                    out.push_str("TTL = ");
                    out.push_str(&back_quote(column));
                    out.push_str(" + INTERVAL ");
                    value.restore_into(out);
                    out.push(' ');
                    out.push_str(unit);
                });
            }
            Self::TtlEnable(on) => {
                context.write_with_tidb_special_comment(out, FEATURE_TTL, |out| {
                    out.push_str("TTL_ENABLE = '");
                    out.push_str(if *on { "ON" } else { "OFF" });
                    out.push('\'');
                });
            }
            Self::TtlJobInterval(v) => {
                context.write_with_tidb_special_comment(out, FEATURE_TTL, |out| {
                    push_table_option_string(out, "TTL_JOB_INTERVAL", v)
                })
            }
            Self::Affinity(v) => {
                context.write_with_tidb_special_comment(out, FEATURE_AFFINITY, |out| {
                    push_table_option_string(out, "AFFINITY", v)
                })
            }
        }
    }
}

fn push_table_option_string(out: &mut String, keyword: &str, value: &str) {
    out.push_str(keyword);
    out.push_str(" = '");
    out.push_str(&escape_string_literal(value));
    out.push('\'');
}

fn push_table_option_bare(out: &mut String, keyword: &str, value: &str) {
    out.push_str(keyword);
    out.push_str(" = ");
    out.push_str(value);
}

fn push_table_option_name(out: &mut String, keyword: &str, value: &str) {
    out.push_str(keyword);
    out.push_str(" = ");
    out.push_str(&back_quote(value));
}
