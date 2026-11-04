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

//! Go `metrics.go`.

use serde::{Deserialize, Serialize};
use tidb_ast::CiString;

/// Go `TableReadCostMetrics`: the intermediate state and result of the table
/// read analysis. It has no JSON tags in Go, so its stored form names the Go
/// fields.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TableReadCostMetrics {
    /// Go `DbName`.
    #[serde(rename = "DbName")]
    pub db_name: CiString,
    /// Go `TableName`.
    #[serde(rename = "TableName")]
    pub table_name: CiString,
    /// Go `TableScanTime` (a `time.Duration`, nanoseconds): the sum of
    /// scan time times read frequency over the table's records.
    #[serde(rename = "TableScanTime")]
    pub table_scan_time: i64,
    /// Go `TableMemUsage`, bytes: the sum of memory usage times read
    /// frequency.
    #[serde(rename = "TableMemUsage")]
    pub table_mem_usage: i64,
    /// Go `ReadFrequency`: the sum of the records' read frequency.
    #[serde(rename = "ReadFrequency")]
    pub read_frequency: i64,
    /// Go `TableReadCost`: scan-time share plus memory share, 0 to 2.
    #[serde(rename = "TableReadCost")]
    pub table_read_cost: f64,
}
