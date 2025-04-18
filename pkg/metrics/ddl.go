// Copyright 2018 PingCAP, Inc.
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

package metrics

import (
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics for the DDL package.
var (
	JobsGauge            *prometheus.GaugeVec
	HandleJobHistogram   *prometheus.HistogramVec
	BatchAddIdxHistogram *prometheus.HistogramVec

	SyncerInit    = "init"
	SyncerRestart = "restart"
	SyncerClear   = "clear"
	SyncerRewatch = "rewatch"

	StateSyncerInit = "init_global_state"

	DeploySyncerHistogram      *prometheus.HistogramVec
	UpdateSelfVersionHistogram *prometheus.HistogramVec

	OwnerUpdateGlobalVersion = "update_global_version"
	OwnerCheckAllVersions    = "check_all_versions"

	UpdateGlobalState = "update_global_state"

	OwnerHandleSyncerHistogram *prometheus.HistogramVec

	// Metrics for job_worker.go.
	WorkerAddDDLJob    = "add_job"
	DDLWorkerHistogram *prometheus.HistogramVec

	// DDLRunOneStep is the label for the DDL worker operation run_one_step.
	//
	// if a DDL job runs successfully, the cost time is mostly in below structure:
	//
	// run_job
	// ├─ step-1
	// │  ├─ transit_one_step
	// │  │  ├─ run_one_step
	// │  │  │  ├─ lock_schema_ver
	// │  │  │  ├─ incr_schema_ver
	// │  │  │  ├─ async_notify
	// │  │  ├─ other common works such as register MDL, commit, etc.
	// │  ├─ wait_schema_synced
	// │  ├─ clean_mdl_info
	// ├─ step-2/3/4 ... similar as above -> done state
	// ├─ handle_job_done
	DDLRunOneStep           = "run_one_step"
	DDLWaitSchemaSynced     = "wait_schema_synced"
	DDLIncrSchemaVerOpHist  prometheus.Observer
	DDLLockSchemaVerOpHist  prometheus.Observer
	DDLRunJobOpHist         prometheus.Observer
	DDLHandleJobDoneOpHist  prometheus.Observer
	DDLTransitOneStepOpHist prometheus.Observer
	DDLLockVerDurationHist  prometheus.Observer
	DDLCleanMDLInfoHist     prometheus.Observer
	RetryableErrorCount     *prometheus.CounterVec

	CreateDDLInstance = "create_ddl_instance"
	CreateDDL         = "create_ddl"
	DDLOwner          = "owner"
	DDLCounter        *prometheus.CounterVec

	BackfillTotalCounter  *prometheus.CounterVec
	BackfillProgressGauge *prometheus.GaugeVec
	DDLJobTableDuration   *prometheus.HistogramVec
	DDLRunningJobCount    *prometheus.GaugeVec
	AddIndexScanRate      *prometheus.HistogramVec
)

// InitDDLMetrics initializes defines DDL metrics.
func InitDDLMetrics() {
	JobsGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "waiting_jobs",
			Help:      "Gauge of jobs.",
		}, []string{LblType})

	HandleJobHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "handle_job_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle jobs",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24hours
		}, []string{LblType, LblResult})

	BatchAddIdxHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "batch_add_idx_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of batch handle data",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType})

	DeploySyncerHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "deploy_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of deploy syncer",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	UpdateSelfVersionHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "update_self_ver_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of update self version",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblResult})

	OwnerHandleSyncerHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "owner_handle_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle syncer",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	DDLWorkerHistogram = NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "worker_operation_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of ddl worker operations",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType, LblAction, LblResult})

	DDLCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "worker_operation_total",
			Help:      "Counter of creating ddl/worker and isowner.",
		}, []string{LblType})

	BackfillTotalCounter = NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "add_index_total",
			Help:      "Speed of add index",
		}, []string{LblType})

	BackfillProgressGauge = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "backfill_percentage_progress",
			Help:      "Percentage progress of backfill",
		}, []string{LblType})

	DDLJobTableDuration = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "ddl",
		Name:      "job_table_duration_seconds",
		Help:      "Bucketed histogram of processing time (s) of the 3 DDL job tables",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
	}, []string{LblType})

	DDLRunningJobCount = NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "running_job_count",
			Help:      "Running DDL jobs count",
		}, []string{LblType})

	AddIndexScanRate = NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "ddl",
		Name:      "scan_rate",
		Help:      "scan rate",
		Buckets:   prometheus.ExponentialBuckets(0.05, 2, 20),
	}, []string{LblType})

	RetryableErrorCount = NewCounterVec(prometheus.CounterOpts{
		Namespace: "tidb",
		Subsystem: "ddl",
		Name:      "retryable_error_total",
		Help:      "Retryable error count during ddl.",
	}, []string{LblType})

	// those metrics are for diagnose performance issues of running multiple DDLs
	// is a short time window, so we don't need to add label for DDL type.
	DDLIncrSchemaVerOpHist = DDLWorkerHistogram.WithLabelValues("incr_schema_ver", "*", "*")
	DDLLockSchemaVerOpHist = DDLWorkerHistogram.WithLabelValues("lock_schema_ver", "*", "*")
	DDLRunJobOpHist = DDLWorkerHistogram.WithLabelValues("run_job", "*", "*")
	DDLHandleJobDoneOpHist = DDLWorkerHistogram.WithLabelValues("handle_job_done", "*", "*")
	DDLTransitOneStepOpHist = DDLWorkerHistogram.WithLabelValues("transit_one_step", "*", "*")
	DDLLockVerDurationHist = DDLWorkerHistogram.WithLabelValues("lock_ver_duration", "*", "*")
	DDLCleanMDLInfoHist = DDLWorkerHistogram.WithLabelValues("clean_mdl_info", "*", "*")
}

// Label constants.
const (
	LblAction = "action"

	// Used by BackfillProgressGauge
	LblAddIndex       = "add_index"
	LblAddIndexMerge  = "add_index_merge_tmp"
	LblModifyColumn   = "modify_column"
	LblReorgPartition = "reorganize_partition"

	// Used by BackfillTotalCounter
	LblAddIdxRate         = "add_idx_rate"
	LblMergeTmpIdxRate    = "merge_tmp_idx_rate"
	LblCleanupIdxRate     = "cleanup_idx_rate"
	LblUpdateColRate      = "update_col_rate"
	LblReorgPartitionRate = "reorg_partition_rate"
)

// generateReorgLabel returns the label with schema name, table name and optional column/index names.
// Multiple columns/indexes can be concatenated with "+".
func generateReorgLabel(label, schemaName, tableName, colOrIdxNames string) string {
	var stringBuilder strings.Builder
	if len(colOrIdxNames) == 0 {
		stringBuilder.Grow(len(label) + len(schemaName) + len(tableName) + 2)
	} else {
		stringBuilder.Grow(len(label) + len(schemaName) + len(tableName) + len(colOrIdxNames) + 3)
	}
	stringBuilder.WriteString(label)
	stringBuilder.WriteString("-")
	stringBuilder.WriteString(schemaName)
	stringBuilder.WriteString("-")
	stringBuilder.WriteString(tableName)
	if len(colOrIdxNames) > 0 {
		stringBuilder.WriteString("-")
		stringBuilder.WriteString(colOrIdxNames)
	}
	return stringBuilder.String()
}

// GetBackfillTotalByLabel returns the Counter showing the speed of backfilling for the given type label.
func GetBackfillTotalByLabel(label, schemaName, tableName, optionalColOrIdxName string) prometheus.Counter {
	return BackfillTotalCounter.WithLabelValues(generateReorgLabel(label, schemaName, tableName, optionalColOrIdxName))
}

// GetBackfillProgressByLabel returns the Gauge showing the percentage progress for the given type label.
func GetBackfillProgressByLabel(label, schemaName, tableName, optionalColOrIdxName string) prometheus.Gauge {
	return BackfillProgressGauge.WithLabelValues(generateReorgLabel(label, schemaName, tableName, optionalColOrIdxName))
}
