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

// Package workloadlearning implements the Workload-Based Learning Optimizer.
// The Workload-Based Learning Optimizer introduces a new module in TiDB that leverages captured workload history to
// enhance the database query optimizer.
// By learning from historical data, this module helps the optimizer make smarter decisions, such as identify hot and cold tables,
// analyze resource consumption, etc.
// The workload analysis results can be used to directly suggest a better path,
// or to indirectly influence the cost model and stats so that the optimizer can select the best plan more intelligently and adaptively.
package workloadlearning

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"go.uber.org/zap"
)

const batchInsertSize = 1000
const (
	// The category of workload-based learning
	feedbackCategory = "Feedback"
)
const (
	// The type of workload-based learning
	tableCostType = "TableCost"
)

// Handle The entry point for all workload-based learning related tasks
type Handle struct {
	sysSessionPool util.SessionPool
}

// NewWorkloadLearningHandle Create a new WorkloadLearningHandle
// WorkloadLearningHandle is Singleton pattern
func NewWorkloadLearningHandle(pool util.SessionPool) *Handle {
	return &Handle{pool}
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
func (handle *Handle) HandleReadTableCost(infoSchema infoschema.InfoSchema) {
	// step1: abstract middle table cost metrics from every record in statement_summary
	middleMetrics, startTime, endTime := handle.analyzeBasedOnStatementStats()
	if len(middleMetrics) == 0 {
		return
	}
	// step2: group by tablename, sum(table-scan-time), sum(table-mem-usage), sum(read-frequency)
	// step3: calculate the total scan time and total memory usage
	tableNameToMetrics := make(map[ast.CIStr]*ReadTableCostMetrics)
	totalScanTime := 0.0
	totalMemUsage := 0.0
	for _, middleMetric := range middleMetrics {
		metric, ok := tableNameToMetrics[middleMetric.TableName]
		if !ok {
			tableNameToMetrics[middleMetric.TableName] = middleMetric
		} else {
			metric.TableScanTime += middleMetric.TableScanTime * float64(middleMetric.ReadFrequency)
			metric.TableMemUsage += middleMetric.TableMemUsage * float64(middleMetric.ReadFrequency)
			metric.ReadFrequency += middleMetric.ReadFrequency
		}
		totalScanTime += middleMetric.TableScanTime
		totalMemUsage += middleMetric.TableMemUsage
	}
	if totalScanTime == 0 || totalMemUsage == 0 {
		return
	}
	// step4: calculate the percentage of scan time and memory usage for each table
	for _, metric := range tableNameToMetrics {
		metric.TableCost = metric.TableScanTime/totalScanTime + metric.TableMemUsage/totalMemUsage
	}
	// step5: save the table cost metrics to table "mysql.workload_values"
	handle.SaveReadTableCostMetrics(tableNameToMetrics, startTime, endTime, infoSchema)
}

func (handle *Handle) analyzeBasedOnStatementSummary() []*ReadTableCostMetrics {
	// step1: get all record from statement_summary
	// step2: abstract table cost metrics from each record
	return nil
}

// TODO
func (handle *Handle) analyzeBasedOnStatementStats() ([]*ReadTableCostMetrics, time.Time, time.Time) {
	// step1: get all record from statement_stats
	// step2: abstract table cost metrics from each record
	// TODO change the mock value
	return nil, time.Now(), time.Now()
}

// SaveReadTableCostMetrics table cost metrics, workload-based start and end time, version,
func (handle *Handle) SaveReadTableCostMetrics(metrics map[ast.CIStr]*ReadTableCostMetrics,
	startTime, endTime time.Time, infoSchema infoschema.InfoSchema) {
	// TODO save the workload job info such as start end time into workload_jobs table
	// step1: create a new session, context, txn for saving table cost metrics
	se, err := handle.sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("get system session failed when saving table cost metrics", zap.Error(err))
		return
	}
	defer handle.sysSessionPool.Put(se)
	sctx := se.(sessionctx.Context)
	exec := sctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnWorkloadLearning)
	// begin a new txn
	err = sessiontxn.NewTxn(context.Background(), sctx)
	if err != nil {
		logutil.BgLogger().Warn("get txn failed when saving table cost metrics", zap.Error(err))
		return
	}
	txn, err := sctx.Txn(true)
	if err != nil {
		logutil.BgLogger().Warn("failed to get txn when saving table cost metrics", zap.Error(err))
		return
	}
	// enable plan cache
	sctx.GetSessionVars().EnableNonPreparedPlanCache = true

	// step2: insert new version table cost metrics by batch using one common txn and context
	version := txn.StartTS()
	// build insert sql by batch(1000 tables)
	i := 0
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "insert into mysql.workload_values (version, category, type, table_id, value) values ")
	for _, metric := range metrics {
		tbl, err := infoSchema.TableByName(ctx, metric.DbName, metric.TableName)
		if err != nil {
			logutil.BgLogger().Warn("failed to save this table cost metrics due to table id not found in info schema",
				zap.String("db_name", metric.DbName.String()),
				zap.String("table_name", metric.TableName.String()),
				zap.Float64("table_scan_time", metric.TableScanTime),
				zap.Float64("table_mem_usage", metric.TableMemUsage),
				zap.Int64("read_frequency", metric.ReadFrequency),
				zap.Float64("table_cost", metric.TableCost),
				zap.Error(err))
			continue
		}
		metricBytes, err := json.Marshal(metric)
		if err != nil {
			logutil.BgLogger().Warn("marshal table cost metrics failed",
				zap.String("db_name", metric.DbName.String()),
				zap.String("table_name", metric.TableName.String()),
				zap.Float64("table_scan_time", metric.TableScanTime),
				zap.Float64("table_mem_usage", metric.TableMemUsage),
				zap.Int64("read_frequency", metric.ReadFrequency),
				zap.Float64("table_cost", metric.TableCost),
				zap.Error(err))
			continue
		}
		sqlescape.MustFormatSQL(sql, "(%?, %?, %?, %?, %?)",
			version, feedbackCategory, tableCostType, tbl.Meta().ID, json.RawMessage(metricBytes))
		// TODO check the txn record limit
		if i%batchInsertSize == batchInsertSize-1 {
			_, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
			if err != nil {
				logutil.BgLogger().Warn("insert new version table cost metrics failed", zap.Error(err))
				return
			}
			sql.Reset()
			sql.WriteString("insert into mysql.workload_values (version, category, type, table_id, value) values ")
		} else {
			sql.WriteString(", ")
		}
		i++
	}
	// insert the last batch
	if sql.Len() != 0 {
		// remove the tail comma
		sql := sql.String()[:sql.Len()-2]
		_, _, err := exec.ExecRestrictedSQL(ctx, nil, sql)
		if err != nil {
			logutil.BgLogger().Warn("insert new version table cost metrics failed", zap.Error(err))
			return
		}
	}
	// step3: commit the txn, finish the save
	err = txn.Commit(context.Background())
	if err != nil {
		logutil.BgLogger().Warn("commit txn failed when saving table cost metrics", zap.Error(err))
	}
}
