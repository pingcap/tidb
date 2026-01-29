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
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

const batchInsertSize = 1000

// The default memory usage of point get and batch point get
// Unit is byte
const defaultPointGetMemUsage = 1
const (
	// The category of workload-based learning
	feedbackCategory = "Feedback"
)
const (
	// The type of workload-based learning
	tableReadCost = "TableReadCost"
)

const (
	// The workload repo related constants
	// Copied from pkg/util/workloadrepo/const.go
	// If we directly import the const.go, it will cause import cycle. (domain -> workloadlearning-> workloadrepo-> domain)
	// The workloadrepo should be changed to (domain -> workloadrepo), but for now just copied those constant to avoid import cycle.
	defSnapshotInterval = 3600
	histSnapshotsTable  = "HIST_SNAPSHOTS"
)

const (
	workloadQueryTemplate = "select summary_end.DIGEST, summary_end.DIGEST_TEXT, summary_end.BINARY_PLAN, " +
		"if(summary_start.EXEC_COUNT is NULL, summary_end.EXEC_COUNT, summary_end.EXEC_COUNT - summary_start.EXEC_COUNT) as EXEC_COUNT FROM " +
		"(SELECT DIGEST, DIGEST_TEXT, BINARY_PLAN, EXEC_COUNT " +
		"FROM %?.HIST_TIDB_STATEMENTS_STATS WHERE STMT_TYPE = 'Select' AND SNAP_ID = %?) summary_end " +
		"left join (SELECT DIGEST, DIGEST_TEXT, BINARY_PLAN, EXEC_COUNT " +
		"FROM %?.HIST_TIDB_STATEMENTS_STATS WHERE STMT_TYPE = 'Select' AND SNAP_ID = %?) summary_start " +
		"on summary_end.DIGEST = summary_start.DIGEST"
)

// Handle The entry point for all workload-based learning related tasks
type Handle struct {
	sysSessionPool util.DestroyableSessionPool
}

// NewWorkloadLearningHandle Create a new WorkloadLearningHandle
// WorkloadLearningHandle is Singleton pattern
func NewWorkloadLearningHandle(pool util.DestroyableSessionPool) *Handle {
	return &Handle{pool}
}

// HandleTableReadCost Start a new round of analysis of all historical table read queries.
// According to abstracted table cost metrics, calculate the percentage of read scan time and memory usage for each table.
// The result will be saved to the table "mysql.tidb_workload_values".
// Dataflow
//  1. Abstract middle table cost metrics(scan time, memory usage, read frequency)
//     from every record in statement_summary/statement_stats
//
// 2,3. Group by tablename, get the total scan time, total memory usage, and every table scan time, memory usage,
//
//	read frequency
//
// 4. Calculate table cost for each table, table cost = table scan time / total scan time + table mem usage / total mem usage
// 5. Save all table cost metrics[per table](scan time, table cost, etc) to table "mysql.tidb_workload_values"
func (handle *Handle) HandleTableReadCost(infoSchema infoschema.InfoSchema) {
	// step1: abstract middle table cost metrics from every record in statement_summary
	tableIDToMetrics, startTime, endTime := handle.analyzeBasedOnStatementStats(infoSchema)
	if tableIDToMetrics == nil {
		logutil.BgLogger().Warn("Failed to analyze table cost metrics from statement stats, skip saving table read cost metrics")
		return
	}
	// step5: save the table cost metrics to table "mysql.tidb_workload_values"
	handle.SaveTableReadCostMetrics(tableIDToMetrics, startTime, endTime)
}

// analyzeBasedOnStatementStats Analyze table cost metrics based on the
func (handle *Handle) analyzeBasedOnStatementStats(infoSchema infoschema.InfoSchema) (tableIDToMetrics map[int64]*TableReadCostMetrics, startTime time.Time, endTime time.Time) {
	// Calculate time range for the last 7 days
	tableIDToMetrics = nil
	endTime = time.Now()
	startTime = endTime.AddDate(0, 0, -7)

	// step1: create a new session, context for getting the records from ClusterTableTiDBStatementsStats/snapshot
	se, err := handle.sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("get system session failed when fetch data from statements_stats", zap.Error(err))
		return nil, startTime, endTime
	}
	defer func() {
		if err == nil { // only recycle when no error
			handle.sysSessionPool.Put(se)
		} else {
			// Note: Otherwise, the session will be leaked.
			handle.sysSessionPool.Destroy(se)
		}
	}()
	sctx := se.(sessionctx.Context)
	exec := sctx.GetRestrictedSQLExecutor()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnWorkloadLearning)

	// Step2: extract metrics from ClusterTableTiDBStatementsStats
	// TODO: use constant for table name and field name/values
	// TODO: control the cpu, memory , concurrency and timeout for the query
	startSnapshotID, err := findClosestSnapshotIDByTime(ctx, exec, startTime)
	if err != nil {
		logutil.ErrVerboseLogger().Warn("Failed to query HIST_SNAPSHOTS table to find start snapshot id",
			zap.Time("start_time", startTime),
			zap.Error(err))
		return nil, startTime, endTime
	}
	endSnapshotID, err := findClosestSnapshotIDByTime(ctx, exec, endTime)
	if err != nil {
		logutil.ErrVerboseLogger().Warn("Failed to query HIST_SNAPSHOTS table to find end snapshot id",
			zap.Time("end_time", endTime),
			zap.Error(err))
		return nil, startTime, endTime
	}
	sql, err := sqlescape.EscapeSQL(workloadQueryTemplate, mysql.WorkloadSchema, endSnapshotID, mysql.WorkloadSchema, startSnapshotID)
	if err != nil {
		logutil.ErrVerboseLogger().Warn("Failed to construct SQL query for ClusterTableTiDBStatementsStats",
			zap.Uint64("end_snapshot_id", endSnapshotID),
			zap.Uint64("start_snapshot_id", startSnapshotID),
			zap.Error(err))
		return nil, startTime, endTime
	}
	// Step2.1: get statements stats record from ClusterTableTiDBStatementsStats
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql)
	if err != nil {
		logutil.ErrVerboseLogger().Warn("Failed to query CLUSTER_TIDB_STATEMENTS_STATS table",
			zap.Time("end_time", endTime),
			zap.Error(err))
		return nil, startTime, endTime
	}
	tableIDToMetrics = make(map[int64]*TableReadCostMetrics)
	for _, row := range rows {
		if row.IsNull(0) || row.IsNull(1) || row.IsNull(2) || row.IsNull(3) {
			logutil.ErrVerboseLogger().Debug("Skip this row because there is null in the row")
			continue
		}
		digest := row.GetString(0)
		sql := row.GetString(1)
		binaryPlan := row.GetString(2)
		readFrequency := row.GetInt64(3)
		// Step2.2: extract scan-time, memory-usage from planString
		currentRecordMetrics, err := extractScanAndMemoryFromBinaryPlan(binaryPlan)
		if err != nil {
			logutil.ErrVerboseLogger().Warn("failed to abstract scan and memory from binary plan",
				zap.String("digest", digest),
				zap.String("sql", sql),
				zap.Error(err))
			continue
		}
		// Step2.3: accumulate all metrics from each record
		AccumulateMetricsGroupByTableID(ctx, currentRecordMetrics, readFrequency, tableIDToMetrics, infoSchema)
	}

	// Step3: compute the table cost metrics by table scan time / total scan time + table mem usage / total mem usage
	// Step3.1: compute total scan time and memory usage
	// todo handle overflow
	var totalScanTime, totalMemUsage int64
	for _, metric := range tableIDToMetrics {
		totalMemUsage += metric.TableMemUsage
		totalScanTime = totalScanTime + metric.TableScanTime.Nanoseconds()
	}
	// Step3.2: compute table cost metrics
	for _, metric := range tableIDToMetrics {
		var scanTimePercentage, memoryUsagePercentage float64
		if totalScanTime != 0 {
			scanTimePercentage = float64(metric.TableScanTime.Nanoseconds()) / float64(totalScanTime)
		}
		if totalMemUsage != 0 {
			memoryUsagePercentage = float64(metric.TableMemUsage) / float64(totalMemUsage)
		}
		metric.TableReadCost = scanTimePercentage + memoryUsagePercentage
	}

	return tableIDToMetrics, startTime, endTime
}

// findClosestSnapshotIDByTime finds the closest snapshot ID in the HIST_SNAPSHOTS table
// It means that the snapshot is a **past** snapshot whose time is before the given time and **closest** to the given time.
// Only find the **one closest** snapshot ID, so the result is a single uint64 value.
func findClosestSnapshotIDByTime(ctx context.Context, exec sqlexec.RestrictedSQLExecutor, time time.Time) (uint64, error) {
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, "select SNAPSHOT_ID from %?.%? where BEGIN_TIME > %? and END_TIME != NULL "+
		"order by (%? - BEGIN_TIME) asc limit 1",
		mysql.WorkloadSchema, histSnapshotsTable,
		time.AddDate(0, 0, -(int(defSnapshotInterval)*2)), time)
	// Due to the time cost of snapshot worker, to ensure that a snapshot ID can be found within the "between and" time interval,
	// the time interval is extended to within two past snapshot interval.
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, fmt.Errorf("no closest snapshot found by time in hist_snapshot table, time: %v", time)
	}
	for _, row := range rows {
		snapshotID := row.GetUint64(0)
		return snapshotID, nil
	}
	return 0, fmt.Errorf("failed to find closest snapshot by time in hist_snapshot table, time: %v", time)
}

// SaveTableReadCostMetrics table cost metrics, workload-based start and end time, version,
func (handle *Handle) SaveTableReadCostMetrics(metrics map[int64]*TableReadCostMetrics,
	_, _ time.Time) {
	// TODO save the workload job info such as start end time into workload_jobs table
	// step1: create a new session, context, txn for saving table cost metrics
	se, err := handle.sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("get system session failed when saving table cost metrics", zap.Error(err))
		return
	}
	defer func() {
		if err == nil { // only recycle when no error
			handle.sysSessionPool.Put(se)
		} else {
			// Note: Otherwise, the session will be leaked.
			handle.sysSessionPool.Destroy(se)
		}
	}()
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
	sqlescape.MustFormatSQL(sql, "insert into mysql.tidb_workload_values (version, category, type, table_id, value) values ")
	for tableID, metric := range metrics {
		metricBytes, err := json.Marshal(metric)
		if err != nil {
			logutil.BgLogger().Warn("marshal table cost metrics failed",
				zap.String("db_name", metric.DbName.String()),
				zap.String("table_name", metric.TableName.String()),
				zap.Duration("table_scan_time", metric.TableScanTime),
				zap.Int64("table_mem_usage", metric.TableMemUsage),
				zap.Int64("read_frequency", metric.ReadFrequency),
				zap.Float64("table_read_cost", metric.TableReadCost),
				zap.Error(err))
			continue
		}
		sqlescape.MustFormatSQL(sql, "(%?, %?, %?, %?, %?)",
			version, feedbackCategory, tableReadCost, tableID, json.RawMessage(metricBytes))
		// TODO check the txn record limit
		if i%batchInsertSize == batchInsertSize-1 {
			_, _, err := exec.ExecRestrictedSQL(ctx, nil, sql.String())
			if err != nil {
				logutil.BgLogger().Warn("insert new version table cost metrics failed", zap.Error(err))
				return
			}
			sql.Reset()
			sql.WriteString("insert into mysql.tidb_workload_values (version, category, type, table_id, value) values ")
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
	// TODO: saving the workload job info such as start end time into workload_jobs table
}

// extractScanAndMemoryFromBinaryPlan abstract the scan time and memory usage from one plan string
// The input is the field BINARY_PLAN in **one row** of table mysql.CLUSTER_TIDB_STATEMENTS_STATS
// The result should be **list** because every query maybe related to multiple tables such as: select * from t1, t2
// Every scan operator in the plan string should be extracted to one TableReadCostMetrics without any grouping
func extractScanAndMemoryFromBinaryPlan(binaryPlan string) ([]*TableReadCostMetrics, error) {
	// Step1: convert binaryPlan to tipb.ExplainData
	protoBytes, err := plancodec.Decompress(binaryPlan)
	if err != nil {
		return nil, err
	}
	explainData := &tipb.ExplainData{}
	err = explainData.Unmarshal(protoBytes)
	if err != nil {
		return nil, err
	}
	if explainData.DiscardedDueToTooLong {
		return nil, fmt.Errorf("plan is too long, discarded it, failed to convert to ExplainData")
	}
	if !explainData.WithRuntimeStats {
		return nil, fmt.Errorf("plan is not with runtime stats, failed to convert to ExplainData")
	}

	// Step2: abstract the scan time and memory usage from explainData
	operatorExtractMetrics := make([]*TableReadCostMetrics, 1)
	// extract scan and memory from main part plan
	operatorExtractMetrics, err = extractMetricsFromOperatorTree(explainData.Main, operatorExtractMetrics)
	if err != nil {
		return nil, err
	}
	// extract scan and memory from CTES part plan
	for _, cte := range explainData.Ctes {
		operatorExtractMetrics, err = extractMetricsFromOperatorTree(cte, operatorExtractMetrics)
		if err != nil {
			return nil, err
		}
	}
	// extract scan and memory from SubQueries part plan
	for _, subQ := range explainData.Subqueries {
		operatorExtractMetrics, err = extractMetricsFromOperatorTree(subQ, operatorExtractMetrics)
		if err != nil {
			return nil, err
		}
	}

	return operatorExtractMetrics, nil
}

// AccumulateMetricsGroupByTableID previousMetrics += currentRecordMetrics * frequency
// Step1: find the table id for all currentRecordMetrics
// Step2: Multiply the scan time and memory usage by frequency for all currentRecordMetrics
// Step3: Group by table id and sum with previous metrics
func AccumulateMetricsGroupByTableID(ctx context.Context, currentRecordMetrics []*TableReadCostMetrics, frequency int64, previousMetrics map[int64]*TableReadCostMetrics, infoSchema infoschema.InfoSchema) {
	// Multiply exec count for each operator cpu and memory metrics
	// Group by table id and sum the scan time and memory usage for each operatorExtractMetrics
	// (TODO) The alias table name is recorded in operator instead of real table name. So we cannot handle the alias table name case now.
	for _, operatorExtractMetric := range currentRecordMetrics {
		// Step1: find the table id for all currentRecordMetrics
		tbl, err := infoSchema.TableByName(ctx, operatorExtractMetric.DbName, operatorExtractMetric.TableName)
		if err != nil {
			logutil.BgLogger().Warn("failed to find the table id by table name and db name",
				zap.String("db_name", operatorExtractMetric.DbName.String()),
				zap.String("table_name", operatorExtractMetric.TableName.String()),
				zap.Error(err))
			continue
		}
		tableID := tbl.Meta().ID

		// Step2: Multiply the scan time and memory usage by frequency
		operatorExtractMetric.TableScanTime = time.Duration(operatorExtractMetric.TableScanTime.Nanoseconds() * frequency)
		operatorExtractMetric.TableMemUsage *= frequency
		operatorExtractMetric.ReadFrequency = frequency

		// Step3: Group by table id and sum with previous metrics
		if previousMetrics[tableID] == nil {
			previousMetrics[tableID] = operatorExtractMetric
		} else {
			previousMetrics[tableID].TableScanTime += operatorExtractMetric.TableScanTime
			previousMetrics[tableID].TableMemUsage += operatorExtractMetric.TableMemUsage
			previousMetrics[tableID].ReadFrequency += operatorExtractMetric.ReadFrequency
		}
	}
}

func extractMetricsFromOperatorTree(op *tipb.ExplainOperator, operatorMetrics []*TableReadCostMetrics) ([]*TableReadCostMetrics, error) {
	// Step1: extract operator metrics
	// Step1.1: get the operator type from op.name
	operatorType, err := extractOperatorTypeFromName(op.Name)
	if err != nil {
		return operatorMetrics, err
	}
	switch operatorType {
	case plancodec.TypeIndexLookUp, plancodec.TypeIndexReader:
		// Handle the case like:
		//  └─IndexLookUp_x | IndexReader_x
		//    └─xxx
		//      └─IndexRangeScan_x | IndexFullScan_x
		// Get the time and memory in this layer and find the table name from child IndexXXXScan layer
		memUsage := op.MemoryBytes
		scanTime, err := extractScanTimeFromExecutionInfo(op)
		if err != nil {
			return operatorMetrics, fmt.Errorf("failed to extract scan time from execution info, operator name: %s", op.Name)
		}
		dbName, tableName := extractTableNameFromIndexScan(op)
		if dbName == "" || tableName == "" {
			return operatorMetrics, fmt.Errorf("failed to get table name from children index xxx operator, operator name: %s", op.Name)
		}
		operatorMetrics = append(operatorMetrics,
			&TableReadCostMetrics{
				DbName:        ast.NewCIStr(dbName),
				TableName:     ast.NewCIStr(tableName),
				TableScanTime: scanTime,
				TableMemUsage: memUsage,
			})
	case plancodec.TypePointGet, plancodec.TypeBatchPointGet:
		if len(op.AccessObjects) == 0 {
			return operatorMetrics, fmt.Errorf("failed to get table name while access object is empty, operator name: %s", op.Name)
		}
		// Attention: cannot handle the multi access objects from one operator
		dbName, tableName := extractTableNameFromAccessObject(op.AccessObjects[0])
		if dbName == "" || tableName == "" {
			return operatorMetrics, fmt.Errorf("failed to get table name from access object, operator name: %s, access object: %s", op.Name, op.AccessObjects[0].String())
		}
		scanTime, err := extractScanTimeFromExecutionInfo(op)
		if err != nil {
			return operatorMetrics, fmt.Errorf("failed to extract scan time from execution info, operator name: %s, err: %v", op.Name, err)
		}
		memUsage := defaultPointGetMemUsage
		operatorMetrics = append(operatorMetrics,
			&TableReadCostMetrics{
				DbName:        ast.NewCIStr(dbName),
				TableName:     ast.NewCIStr(tableName),
				TableScanTime: scanTime,
				TableMemUsage: int64(memUsage),
			})
	case plancodec.TypeTableReader:
		// Ignore TiFlash query
		isTiFlashOp := checkTiFlashOperator(op)
		if isTiFlashOp {
			return operatorMetrics, nil
		}
		// Handle the case like:
		//  └─TableReader_x
		//    └─xxx
		//      └─TableFullScan_x | TableRangeScan_x
		// Get the time and memory in this layer and find the table name from child TableXXXScan layer
		scanTime, err := extractScanTimeFromExecutionInfo(op)
		if err != nil {
			return operatorMetrics, fmt.Errorf("failed to extract scan time from execution info, operator name: %s", op.Name)
		}
		memUsage := op.MemoryBytes
		dbName, tableName := extractTableNameFromChildrenTableScan(op)
		if dbName == "" || tableName == "" {
			return operatorMetrics, fmt.Errorf("failed to get table name from children table xxx operator, operator name: %s", op.Name)
		}
		operatorMetrics = append(operatorMetrics,
			&TableReadCostMetrics{
				DbName:        ast.NewCIStr(dbName),
				TableName:     ast.NewCIStr(tableName),
				TableScanTime: scanTime,
				TableMemUsage: memUsage,
			})
	case plancodec.TypeIndexMerge:
		// Handle the case like:
		//  └─IndexMerge_x
		//    └─xxx
		//      └─IndexxxxScan
		//      └─IndexxxxScan
		// Get the memory in this layer and average memory to every single indexxxxscan
		// Get time and table name from every child indexxxxscan layer
		totalMemoryUsage := op.MemoryBytes
		patialMetrics, err := extractPartialMetricsFromChildrenIndexMerge(op)
		if err != nil || len(patialMetrics) == 0 {
			return operatorMetrics, fmt.Errorf("failed to get time and table name from children operator of index merge, operator name: %s", op.Name)
		}
		memUsage := totalMemoryUsage / int64(len(patialMetrics))
		for _, patialMetric := range patialMetrics {
			patialMetric.TableMemUsage = memUsage
			operatorMetrics = append(operatorMetrics, patialMetric)
		}
	default:
		// TODO: extand the operator type indexjoin, indexmergejoin, tiflash
	}
	// Step2: Recursively extract the children layer
	if len(op.Children) == 0 {
		return operatorMetrics, nil
	}
	for _, child := range op.Children {
		operatorMetrics, err = extractMetricsFromOperatorTree(child, operatorMetrics)
		if err != nil {
			return operatorMetrics, err
		}
	}
	return operatorMetrics, nil
}

func checkTiFlashOperator(op *tipb.ExplainOperator) bool {
	if len(op.Children) == 0 {
		return false
	}
	for _, child := range op.Children {
		if child.TaskType == tipb.TaskType_mpp {
			return true
		}
		checkTiFlashOperator(child) // Recursive check
	}
	return false
}

func extractTableNameFromAccessObject(accessObject *tipb.AccessObject) (dbName string, tableName string) {
	switch ao := accessObject.AccessObject.(type) {
	case *tipb.AccessObject_DynamicPartitionObjects:
		if ao == nil || ao.DynamicPartitionObjects == nil {
			return "", ""
		}
		aos := ao.DynamicPartitionObjects.Objects
		if len(aos) == 0 {
			return "", ""
		}
		// If it involves multiple tables, we also need to print the table name.
		for _, access := range aos {
			if access == nil {
				continue
			}
			return access.Database, access.Table
		}
	case *tipb.AccessObject_ScanObject:
		if ao == nil || ao.ScanObject == nil {
			return "", ""
		}
		return ao.ScanObject.Database, ao.ScanObject.Table
	}
	return "", ""
}

func extractTableNameFromChildrenTableScan(op *tipb.ExplainOperator) (dbName string, tableName string) {
	if len(op.Children) == 0 {
		return "", ""
	}
	for _, child := range op.Children {
		childOT, err := extractOperatorTypeFromName(child.Name)
		if err != nil {
			return "", ""
		}
		if childOT == plancodec.TypeTableFullScan || childOT == plancodec.TypeTableRangeScan {
			dbName, tableName = extractTableNameFromAccessObject(child.AccessObjects[0])
			return dbName, tableName
		}
		// Recursive search for the children layer
		dbName, tableName = extractTableNameFromChildrenTableScan(child)
		if dbName != "" && tableName != "" {
			return dbName, tableName
		}
	}
	return "", ""
}

func extractTableNameFromIndexScan(op *tipb.ExplainOperator) (dbName string, tableName string) {
	if len(op.Children) == 0 {
		return "", ""
	}
	for _, child := range op.Children {
		childOT, err := extractOperatorTypeFromName(child.Name)
		if err != nil {
			return "", ""
		}
		if childOT == plancodec.TypeIndexRangeScan || childOT == plancodec.TypeIndexFullScan {
			dbName, tableName = extractTableNameFromAccessObject(child.AccessObjects[0])
			return dbName, tableName
		}
		// Recursive search for the children layer
		dbName, tableName = extractTableNameFromIndexScan(child)
		if dbName != "" && tableName != "" {
			return dbName, tableName
		}
	}
	return "", ""
}

func extractPartialMetricsFromChildrenIndexMerge(op *tipb.ExplainOperator) ([]*TableReadCostMetrics, error) {
	if len(op.Children) == 0 {
		return nil, nil
	}
	// Find the layer of IndexxxxScan
	children0OperatorType, err := extractOperatorTypeFromName(op.Children[0].Name)
	if err != nil {
		return nil, err
	}
	if children0OperatorType == plancodec.TypeIndexRangeScan || children0OperatorType == plancodec.TypeIndexFullScan {
		result := make([]*TableReadCostMetrics, 0, len(op.Children))
		for _, child := range op.Children {
			dbName, tableName := extractTableNameFromAccessObject(child.AccessObjects[0])
			if dbName == "" || tableName == "" {
				return nil, fmt.Errorf("failed to get table name from children index xxx operator, operator name: %s", child.Name)
			}
			scanTime, err := extractScanTimeFromExecutionInfo(child)
			if err != nil {
				return nil, fmt.Errorf("failed to extract scan time from execution info, operator name: %s", child.Name)
			}
			partialMetric := &TableReadCostMetrics{
				DbName:        ast.NewCIStr(dbName),
				TableName:     ast.NewCIStr(tableName),
				TableScanTime: scanTime,
			}
			result = append(result, partialMetric)
		}
		return result, nil
	}
	// Recursively find the children layer
	var result []*TableReadCostMetrics
	for _, child := range op.Children {
		result, err = extractPartialMetricsFromChildrenIndexMerge(child)
		if len(result) != 0 {
			return result, nil
		}
	}
	return nil, err
}

// extractOperatorTypeFromName extracts the operator type from the operator name.
// The operator name pattern seems to be like "OperatorType_ID"
// For example, "TableReader_1", "IndexLookUp_2", "PointGet_3", etc.
// The function will return the operator type part, such as "TableReader", "IndexLookUp", "PointGet", etc.
func extractOperatorTypeFromName(name string) (string, error) {
	names := strings.Split(name, "_")
	if len(names) != 2 {
		return "", fmt.Errorf("failed to extract operator type from operator name: %s", name)
	}
	return names[0], nil
}

func extractScanTimeFromExecutionInfo(op *tipb.ExplainOperator) (time.Duration, error) {
	var scanTime time.Duration
	var err error
	if len(op.RootBasicExecInfo) > 0 {
		scanTime, err = extractScanTimeFromString(op.RootBasicExecInfo)
	}
	if scanTime == 0 && len(op.RootGroupExecInfo) > 0 {
		// try groupExecInfo instead
		scanTime, err = extractScanTimeFromString(op.RootGroupExecInfo[0])
	}
	if scanTime == 0 && len(op.CopExecInfo) > 0 {
		// try copExecInfo instead
		scanTime, err = extractScanTimeFromString(op.CopExecInfo)
	}
	return scanTime, err
}

// extractScanTimeFromString extracts the scan time from execution info string that contains "time: <duration>,"
// The execution info string pattern seems to be like:
// "time:274.5µs, loops:1, cop_task:......"
// We need the duration after "time:" and before the first comma.
func extractScanTimeFromString(input string) (time.Duration, error) {
	start := strings.Index(input, "time:")
	if start == -1 {
		return 0, fmt.Errorf("'time:' not found in input string")
	}
	start += len("time:") // move start index after "time:"

	end := strings.Index(input[start:], ",")
	if end == -1 {
		return 0, fmt.Errorf("',' not found after 'time:'")
	}
	end += start // compute the end index

	// extract timeStr excluding "time:" and "," and empty str
	timeStr := strings.TrimSpace(input[start:end])

	// parse str to Duration
	duration, err := time.ParseDuration(timeStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse duration: %v", err)
	}
	return duration, nil
}

// DBNameExtractor is an AST visitor that extracts all database names from query.
// It is used to check whether the query is cross-database query or not.
type DBNameExtractor struct {
	// DBs is a map to store the database names found in the AST nodes.
	DBs map[string]struct{}
}

// Enter is called when entering a node in the AST.
// It extracts the database name from TableName AST node and saving it in DBs map.
func (e *DBNameExtractor) Enter(n ast.Node) (ast.Node, bool) {
	if table, ok := n.(*ast.TableName); ok {
		if e.DBs == nil {
			e.DBs = make(map[string]struct{})
		}
		e.DBs[table.Schema.L] = struct{}{}
	}
	return n, false
}

// Leave is called when leaving a node in the AST.
func (*DBNameExtractor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}
