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

import (
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// TableReadCostMetrics is used to indicate the intermediate status and results analyzed through table read workload
// for function "HandleTableReadCost".
type TableReadCostMetrics struct {
	// TODO(Elsa): Add the json tag for the field
	DbName    ast.CIStr
	TableName ast.CIStr
	// TableScanTime[t] = sum(scan-time * readFrequency) of all records in statement_summary where table-name = t
	TableScanTime time.Duration
	// TableMemUsage[t] = sum(mem-usage * readFrequency) of all records in statement_summary where table-name = t
	// Unit is bytes
	TableMemUsage int64
	// ReadFrequency[t] = sum(read-frequency) of all records in statement_summary where table-name = t
	ReadFrequency int64
	// TableReadCost[t] = TableScanTime[t] / totalScanTime  + TableMemUsage[t] / totalMemUsage
	// range between 0 ~ 2
	TableReadCost float64
}
