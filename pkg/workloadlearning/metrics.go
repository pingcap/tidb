// Copyright 2025 PingCAP, Inc.
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

package workloadlearning

import "github.com/pingcap/tidb/pkg/parser/ast"

// ReadTableCostMetrics is used to indicate the intermediate status and results analyzed through read workload
// for function "HandleReadTableCost".
type ReadTableCostMetrics struct {
	DbName    ast.CIStr
	TableName ast.CIStr
	// TableScanTime[t] = sum(scan-time * readFrequency) of all records in statement_summary where table-name = t
	TableScanTime float64
	// TableMemUsage[t] = sum(mem-usage * readFrequency) of all records in statement_summary where table-name = t
	TableMemUsage float64
	// ReadFrequency[t] = sum(read-frequency) of all records in statement_summary where table-name = t
	ReadFrequency int64
	// TableCost[t] = TableScanTime[t] / totalScanTime  + TableMemUsage[t] / totalMemUsage
	// range between 0 ~ 2
	TableCost float64
}
