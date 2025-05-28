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
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
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
	tableIdToMetrics, startTime, endTime := handle.analyzeBasedOnStatementStats(infoSchema)
	// step5: save the table cost metrics to table "mysql.tidb_workload_values"
	handle.SaveTableReadCostMetrics(tableIdToMetrics, startTime, endTime)
}

func (*Handle) analyzeBasedOnStatementSummary() []*TableReadCostMetrics {
	// step1: get all record from statement_summary
	// step2: abstract table cost metrics from each record
	return nil
}

// analyzeBasedOnStatementStats Analyze table cost metrics based on the
func (handle *Handle) analyzeBasedOnStatementStats(infoSchema infoschema.InfoSchema) (map[int64]*TableReadCostMetrics, time.Time, time.Time) {
	// Calculate time range for the last 7 days
	endTime := time.Now()
	startTime := endTime.AddDate(0, 0, -7)

	// step1: create a new session, context for getting the records from ClusterTableTiDBStatementsStats/snapshot
	se, err := handle.sysSessionPool.Get()
	if err != nil {
		logutil.BgLogger().Warn("get system session failed when fetch data from statements_stats", zap.Error(err))
		return nil, startTime, startTime
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
	// TODO: change to the snapshot id
	sql := `select summary_latest.DIGEST, summary_latest.DIGEST_TEXT, summary_latest.BINARY_PLAN,
            ifNULL(summary_start.EXEC_COUNT, summary_latest.EXEC_COUNT, summary_latest.EXEC_COUNT - summary_start.EXEC_COUNT) as EXEC_COUNT
            (SELECT DIGEST, DIGEST_TEXT, BINARY_PLAN, EXEC_COUNT
	        FROM INFORMATION_SCHEMA.CLUSTER_TIDB_STATEMENTS_STATS
	        WHERE LOWER(STMT_TYPE) = 'Select'
	        AND SUMMARY_END_TIME >= '%?') summary_latest left join
            (SELECT DIGEST, DIGEST_TEXT, BINARY_PLAN, EXEC_COUNT
	        FROM INFORMATION_SCHEMA.CLUSTER_TIDB_STATEMENTS_STATS
	        WHERE LOWER(STMT_TYPE) = 'Select'
	        AND SUMMARY_END_TIME >= '%?') summary_start on summary_latest.DIGEST = summary_start.DIGEST`
	// Step2.1: get statements stats record from ClusterTableTiDBStatementsStats
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql, endTime.Format("2006-01-02 15:04:05"))
	if err != nil {
		logutil.ErrVerboseLogger().Warn("Failed to query CLUSTER_TIDB_STATEMENTS_STATS table",
			zap.Time("end_time", endTime),
			zap.Error(err))
		return nil, startTime, startTime
	}
	tableIdToMetrics := make(map[int64]*TableReadCostMetrics)
	for _, row := range rows {
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
		accumulateMetricsGroupByTableId(currentRecordMetrics, readFrequency, tableIdToMetrics, infoSchema, ctx)
	}

	// Step3: compute the table cost metrics by table scan time / total scan time + table mem usage / total mem usage
	// Step3.1: compute total scan time and memory usage
	// todo handle overflow
	var totalScanTime, totalMemUsage int64
	for _, metric := range tableIdToMetrics {
		totalMemUsage += metric.TableMemUsage
		totalScanTime = totalScanTime + metric.TableScanTime.Nanoseconds()
	}
	// Step3.2: compute table cost metrics
	for _, metric := range tableIdToMetrics {
		var scanTimePercentage, memoryUsagePercentage float64
		if totalScanTime != 0 {
			scanTimePercentage = float64(metric.TableScanTime.Nanoseconds()) / float64(totalScanTime)
		}
		if totalMemUsage != 0 {
			memoryUsagePercentage = float64(metric.TableMemUsage) / float64(totalMemUsage)
		}
		metric.TableReadCost = scanTimePercentage + memoryUsagePercentage
	}

	return tableIdToMetrics, startTime, endTime
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
	for tableId, metric := range metrics {
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
			version, feedbackCategory, tableReadCost, tableId, json.RawMessage(metricBytes))
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

func checkCrossDB(sql string) (bool, error) {
	// TODO check duplicate alias name
	p := parser.New()
	// notes: this is only use for check whether query cross db, so the charset and collation doesn't matter in here.
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return false, err
	}

	extractor := &TableNameExtractor{}
	if len(stmtNodes) != 1 {
		// TODO: support multi-statement in one single statement_record
		return false, nil
	}
	for _, stmt := range stmtNodes {
		stmt.Accept(extractor)
		if len(extractor.DBs) > 1 {
			return true, nil
		}
	}
	return false, nil
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
	err = extractMetricsFromOperatorTree(explainData.Main, operatorExtractMetrics)
	if err != nil {
		return nil, err
	}
	// extract scan and memory from CTES part plan
	for _, cte := range explainData.Ctes {
		err = extractMetricsFromOperatorTree(cte, operatorExtractMetrics)
		if err != nil {
			return nil, err
		}
	}

	return operatorExtractMetrics, nil
}

func accumulateMetricsGroupByTableId(currentRecordMetrics []*TableReadCostMetrics, frequency int64, previousMetrics map[int64]*TableReadCostMetrics, infoSchema infoschema.InfoSchema, ctx context.Context) {
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
		tableId := tbl.Meta().ID

		// Step2: Multiply the scan time and memory usage by frequency
		operatorExtractMetric.TableScanTime = time.Duration(operatorExtractMetric.TableScanTime.Nanoseconds() * frequency)
		operatorExtractMetric.TableMemUsage *= frequency
		operatorExtractMetric.ReadFrequency = frequency

		// Step3: Group by table id and sum with previous metrics
		if previousMetrics[tableId] == nil {
			previousMetrics[tableId] = operatorExtractMetric
		} else {
			previousMetrics[tableId].TableScanTime += operatorExtractMetric.TableScanTime
			previousMetrics[tableId].TableMemUsage += operatorExtractMetric.TableMemUsage
			previousMetrics[tableId].ReadFrequency += operatorExtractMetric.ReadFrequency
		}
	}
}

func extractMetricsFromOperatorTree(op *tipb.ExplainOperator, operatorMetrics []*TableReadCostMetrics) error {
	// Step1: extract operator metrics
	// Step1.1: get the operator type from op.name
	operatorType, err := extractOperatorTypeFromName(op.Name)
	if err != nil {
		return err
	}
	switch operatorType {
	case plancodec.TypeIndexLookUp:
	case plancodec.TypeIndexReader:
		// Handle the case like:
		//  └─IndexLookUp_x | IndexReader_x
		//    └─xxx
		//      └─IndexRangeScan_x | IndexFullScan_x
		// Get the time and memory in this layer and find the table name from child IndexXXXScan layer
		memUsage := op.MemoryBytes
		scanTime, err := extractScanTimeFromExecutionInfo(op)
		if err != nil {
			return fmt.Errorf("failed to extract scan time from execution info, operator name: %s", op.Name)
		}
		dbName, tableName := extractTableNameFromIndexScan(op)
		if dbName == "" || tableName == "" {
			return fmt.Errorf("failed to get table name from children index xxx operator, operator name: %s", op.Name)
		}
		operatorMetrics = append(operatorMetrics,
			&TableReadCostMetrics{
				DbName:        ast.NewCIStr(dbName),
				TableName:     ast.NewCIStr(tableName),
				TableScanTime: scanTime,
				TableMemUsage: memUsage,
			})
	case plancodec.TypePointGet:
	case plancodec.TypeBatchPointGet:
		if len(op.AccessObjects) == 0 {
			return fmt.Errorf("faile to get table name while access object is empty, operator name: %s", op.Name)
		}
		// Attention: cannot handle the multi access objects from one operator
		dbName, tableName := extractTableNameFromAccessObject(op.AccessObjects[0])
		if dbName == "" || tableName == "" {
			return fmt.Errorf("failed to get table name from access object, operator name: %s, access object: %s", op.Name, op.AccessObjects[0].String())
		}
		scanTime, err := extractScanTimeFromExecutionInfo(op)
		if err != nil {
			return fmt.Errorf("failed to extract scan time from execution info, operator name: %s, err: %v", op.Name, err)
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
			return nil
		}
		// Handle the case like:
		//  └─TableReader_x
		//    └─xxx
		//      └─TableFullScan_x | TableRangeScan_x
		// Get the time and memory in this layer and find the table name from child TableXXXScan layer
		scanTime, err := extractScanTimeFromExecutionInfo(op)
		if err != nil {
			return fmt.Errorf("failed to extract scan time from execution info, operator name: %s", op.Name)
		}
		memUsage := op.MemoryBytes
		dbName, tableName := extractTableNameFromChildrenTableScan(op)
		if dbName == "" || tableName == "" {
			return fmt.Errorf("failed to get table name from children table xxx operator, operator name: %s", op.Name)
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
			return fmt.Errorf("failed to get time and table name from children operator of index merge, operator name: %s", op.Name)
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
		return nil
	}
	for _, child := range op.Children {
		err = extractMetricsFromOperatorTree(child, operatorMetrics)
		if err != nil {
			return err
		}
	}
	return nil
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

func extractTableNameFromAccessObject(accessObject *tipb.AccessObject) (string, string) {
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
	default:
		return "", ""
	}
	return "", ""
}

func extractTableNameFromChildrenTableScan(op *tipb.ExplainOperator) (string, string) {
	if len(op.Children) == 0 {
		return "", ""
	}
	for _, child := range op.Children {
		childOT, err := extractOperatorTypeFromName(child.Name)
		if err != nil {
			return "", ""
		}
		if childOT == plancodec.TypeTableFullScan || childOT == plancodec.TypeTableRangeScan {
			dbName, tableName := extractTableNameFromAccessObject(child.AccessObjects[0])
			return dbName, tableName
		}
		// Recursive search for the children layer
		dbName, tableName := extractTableNameFromChildrenTableScan(child)
		if dbName != "" && tableName != "" {
			return dbName, tableName
		}
	}
	return "", ""
}

func extractTableNameFromIndexScan(op *tipb.ExplainOperator) (string, string) {
	if len(op.Children) == 0 {
		return "", ""
	}
	for _, child := range op.Children {
		childOT, err := extractOperatorTypeFromName(child.Name)
		if err != nil {
			return "", ""
		}
		if childOT == plancodec.TypeIndexRangeScan || childOT == plancodec.TypeIndexFullScan {
			dbName, tableName := extractTableNameFromAccessObject(child.AccessObjects[0])
			return dbName, tableName
		}
		// Recursive search for the children layer
		dbName, tableName := extractTableNameFromIndexScan(child)
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
	} else {
		// Recursively find the children layer
		var err error
		var result []*TableReadCostMetrics
		for _, child := range op.Children {
			result, err = extractPartialMetricsFromChildrenIndexMerge(child)
			if len(result) != 0 {
				return result, nil
			}
		}
		return nil, err
	}
}

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

type TableNameExtractor struct {
	DBs map[string]struct{}
}

func (e *TableNameExtractor) Enter(n ast.Node) (ast.Node, bool) {
	if table, ok := n.(*ast.TableName); ok {
		if e.DBs == nil {
			e.DBs = make(map[string]struct{})
		}
		e.DBs[table.Schema.L] = struct{}{}
	}
	return n, false
}

func (e *TableNameExtractor) Leave(n ast.Node) (ast.Node, bool) {
	return n, true
}
