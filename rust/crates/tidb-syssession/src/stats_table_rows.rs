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

//! The error of storing one table's statistics, which the statistics writers
//! (Go's restricted SQL over `mysql.stats_*`) share.

use crate::mysql_system_tables::SystemTableError;
use tidb_table::origin_default::RowEncodeError;

/// Why one table's statistics could not be stored.
#[derive(Debug)]
pub enum StatsWriteError {
    /// A `mysql.stats_*` table the cluster does not have.
    MissingTable(String),
    /// The current rows could not be read.
    Read(SystemTableError),
    /// A row could not be encoded.
    Encode(RowEncodeError),
    /// A histogram bound could not be converted to the blob the column stores.
    Bound(String),
    /// A loaded CMSketch could not be encoded for `stats_histograms`.
    Cms(String),
    /// A predicate-column timestamp could not be parsed as Go TIMESTAMP(6).
    PredicateTime(String),
    /// The exact statistics version needed for a history row is no longer current.
    HistoricalMeta(String),
    /// A historical statistics JSON object could not be compressed.
    HistoricalData(String),
    /// Updating a statistics row's table ID would collide with an existing row.
    DuplicateIdentity(String),
}

impl std::fmt::Display for StatsWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingTable(name) => write!(formatter, "this cluster has no `{name}`"),
            Self::Read(error) => write!(formatter, "{error}"),
            Self::Encode(error) => write!(formatter, "{error}"),
            Self::Bound(detail) => write!(formatter, "a histogram bound did not convert: {detail}"),
            Self::Cms(detail) => write!(formatter, "a CMSketch did not encode: {detail}"),
            Self::PredicateTime(detail) => {
                write!(
                    formatter,
                    "a predicate-column timestamp did not parse: {detail}"
                )
            }
            Self::HistoricalMeta(detail) => formatter.write_str(detail),
            Self::HistoricalData(detail) => formatter.write_str(detail),
            Self::DuplicateIdentity(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for StatsWriteError {}

impl From<SystemTableError> for StatsWriteError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<RowEncodeError> for StatsWriteError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}
