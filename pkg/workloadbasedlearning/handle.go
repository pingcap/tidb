// Copyright 2024 PingCAP, Inc.
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

// Package workloadbasedlearning implements the Workload-Based Learning Optimizer.
// The Workload-Based Learning Optimizer introduces a new module in TiDB that leverages captured workload history to
// enhance the database query optimizer.
// By learning from historical data, this module helps the optimizer make smarter decisions, such as identify hot and cold tables,
// analyze resource consumption, etc.
// The workload analysis results can be used to directly suggest a better path,
// or to indirectly influence the cost model and stats so that the optimizer can select the best plan more intelligently and adaptively.
package workloadbasedlearning

// Handle The entry point for all workload-based learning related tasks
type Handle struct {
}

// NewWorkloadBasedLearningHandle Create a new WorkloadBasedLearningHandle
// WorkloadBasedLearningHandle is Singleton pattern
func NewWorkloadBasedLearningHandle() *Handle {
	return &Handle{}
}

// HandleReadTableCost Start a new round of analysis of all historical read queries.
// According to abstracted table cost metrics, calculate the percentage of read scan time and memory usage for each table.
// The result will be saved to the table "mysql.workload_values".
// Dataflow
//  1. Abstract middle table cost metrics(scan time, memory usage, read frequency)
//     from every record in statement_summary/statement_stats
//
// 2,3. Group by tablename, get the total scan time, total memory usage, and every table scan time, memory usage,
//
//	read frequency
//
// 4. Calculate table cost for each table, table cost = table scan time / total scan time + table mem usage / total mem usage
// 5. Save all table cost metrics[per table](scan time, table cost, etc) to table "mysql.workload_values"
func (handle *Handle) HandleReadTableCost() {
	// step1: abstract middle table cost metrics from every record in statement_summary
	middleMetrics := handle.analyzeBasedOnStatementSummary()
	if len(middleMetrics) == 0 {
		return
	}
	// step2: group by tablename, sum(table-scan-time), sum(table-mem-usage), sum(read-frequency)
	// step3: calculate the total scan time and total memory usage
	tableNameToMetrics := make(map[string]*ReadTableCostMetrics)
	totalScanTime := 0.0
	totalMemUsage := 0.0
	for _, middleMetric := range middleMetrics {
		metric, ok := tableNameToMetrics[middleMetric.tableName]
		if !ok {
			tableNameToMetrics[middleMetric.tableName] = middleMetric
		} else {
			metric.tableScanTime += middleMetric.tableScanTime * float64(middleMetric.readFrequency)
			metric.tableMemUsage += middleMetric.tableMemUsage * float64(middleMetric.readFrequency)
			metric.readFrequency += middleMetric.readFrequency
		}
		totalScanTime += middleMetric.tableScanTime
		totalMemUsage += middleMetric.tableMemUsage
	}
	if totalScanTime == 0 || totalMemUsage == 0 {
		return
	}
	// step4: calculate the percentage of scan time and memory usage for each table
	for _, metric := range tableNameToMetrics {
		metric.tableCost = metric.tableScanTime/totalScanTime + metric.tableMemUsage/totalMemUsage
	}
	// TODO step5: save the table cost metrics to table "mysql.workload_values"
}

func (handle *Handle) analyzeBasedOnStatementSummary() []*ReadTableCostMetrics {
	// step1: get all record from statement_summary
	// step2: abstract table cost metrics from each record
	return nil
}

// TODO
func (handle *Handle) analyzeBasedOnStatementStats() []*ReadTableCostMetrics {
	// step1: get all record from statement_stats
	// step2: abstract table cost metrics from each record
	return nil
}
