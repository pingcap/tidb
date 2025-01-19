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

package workloadbasedlearning

// ReadTableCostMetrics is used to indicate the intermediate status and results analyzed through read workload
// for function "HandleReadTableCost".
type ReadTableCostMetrics struct {
	tableName string
	// tableScanTime[t] = sum(scan-time * readFrequency) of all records in statement_summary where table-name = t
	tableScanTime float64
	// tableMemUsage[t] = sum(mem-usage * readFrequency) of all records in statement_summary where table-name = t
	tableMemUsage float64
	// readFrequency[t] = sum(read-frequency) of all records in statement_summary where table-name = t
	readFrequency int64
	// tableCost[t] = tableScanTime[t] / totalScanTime  + tableMemUsage[t] / totalMemUsage
	// range between 0 ~ 2
	tableCost float64
}
