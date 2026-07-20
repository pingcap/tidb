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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type backfillMetricRegistry struct {
	mu      sync.Mutex
	byTblID map[int64]map[string]struct{}
}

func (r *backfillMetricRegistry) register(tableID int64, typeLabel string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.byTblID[tableID]; !ok {
		r.byTblID[tableID] = make(map[string]struct{}, 8)
	}
	r.byTblID[tableID][typeLabel] = struct{}{}
}

func (r *backfillMetricRegistry) clear(tableID int64) []string {
	r.mu.Lock()
	labels, ok := r.byTblID[tableID]
	if ok {
		delete(r.byTblID, tableID)
	}
	r.mu.Unlock()
	if !ok {
		return nil
	}

	out := make([]string, 0, len(labels))
	for label := range labels {
		out = append(out, label)
	}
	return out
}

// Metrics for the DDL package.
var (
	backfillMetricsRegistry = &backfillMetricRegistry{
		byTblID: make(map[int64]map[string]struct{}, 64),
	}

	JobsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "waiting_jobs",
			Help:      "Gauge of jobs.",
		}, []string{LblType})

	HandleJobHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "handle_job_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle jobs",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 24), // 10ms ~ 24hours
		}, []string{LblType, LblResult})

	BatchAddIdxHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "batch_add_idx_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of batch handle data",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType})

	SyncerInit            = "init"
	SyncerRestart         = "restart"
	SyncerClear           = "clear"
	SyncerRewatch         = "rewatch"
	DeploySyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "deploy_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of deploy syncer",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	UpdateSelfVersionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "update_self_ver_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of update self version",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblResult})

	OwnerUpdateGlobalVersion   = "update_global_version"
	OwnerCheckAllVersions      = "check_all_versions"
	OwnerHandleSyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "owner_handle_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle syncer",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
		}, []string{LblType, LblResult})

	// Metrics for ddl_worker.go.
	WorkerNotifyDDLJob      = "notify_job"
	WorkerAddDDLJob         = "add_job"
	WorkerRunDDLJob         = "run_job"
	WorkerFinishDDLJob      = "finish_job"
	WorkerWaitSchemaChanged = "wait_schema_changed"
	DDLWorkerHistogram      = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "worker_operation_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of ddl worker operations",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 28), // 1ms ~ 1.5days
		}, []string{LblType, LblAction, LblResult})

	CreateDDLInstance = "create_ddl_instance"
	CreateDDL         = "create_ddl"
	DDLOwner          = "owner"
	DDLCounter        = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "worker_operation_total",
			Help:      "Counter of creating ddl/worker and isowner.",
		}, []string{LblType})

	BackfillTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "add_index_total",
			Help:      "Speed of add index",
		}, []string{LblType})

	BackfillProgressGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "backfill_percentage_progress",
			Help:      "Percentage progress of backfill",
		}, []string{LblType})

	DDLJobTableDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "ddl",
		Name:      "job_table_duration_seconds",
		Help:      "Bucketed histogram of processing time (s) of the 3 DDL job tables",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
	}, []string{LblType})

	DDLRunningJobCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "running_job_count",
			Help:      "Running DDL jobs count",
		}, []string{LblType})
)

// Label constants.
const (
	LblAction = "action"

	LblAddIndex           = "add_index"
	LblAddIndexMerge      = "add_index_merge_tmp"
	LblModifyColumn       = "modify_column"
	LblReorgPartition     = "reorganize_partition"
	LblAddIdxRate         = "add_idx_rate"
	LblMergeTmpIdxRate    = "merge_tmp_idx_rate"
	LblCleanupIdxRate     = "cleanup_idx_rate"
	LblUpdateColRate      = "update_col_rate"
	LblReorgPartitionRate = "reorg_partition_rate"
)

// GenerateReorgLabel returns the label with schema name and table name.
func GenerateReorgLabel(label string, schemaName string, tableName string, optionalColOrIdxName ...string) string {
	colOrIdxName := ""
	if len(optionalColOrIdxName) > 0 {
		colOrIdxName = optionalColOrIdxName[0]
	}
	var stringBuilder strings.Builder
	if len(colOrIdxName) == 0 {
		stringBuilder.Grow(len(label) + len(schemaName) + len(tableName) + 2)
	} else {
		stringBuilder.Grow(len(label) + len(schemaName) + len(tableName) + len(colOrIdxName) + 3)
	}
	stringBuilder.WriteString(label)
	stringBuilder.WriteString("_")
	stringBuilder.WriteString(schemaName)
	stringBuilder.WriteString("_")
	stringBuilder.WriteString(tableName)
	if len(colOrIdxName) > 0 {
		stringBuilder.WriteString("_")
		stringBuilder.WriteString(colOrIdxName)
	}
	return stringBuilder.String()
}

// GetBackfillProgressByLabel returns the Gauge showing the percentage progress for the given type label.
func GetBackfillProgressByLabel(label string, schemaName string, tableName string, optionalColOrIdxName ...string) prometheus.Gauge {
	return BackfillProgressGauge.WithLabelValues(GenerateReorgLabel(label, schemaName, tableName, optionalColOrIdxName...))
}

// GetBackfillTotalByTableID returns the Counter for the given table ID and type label.
func GetBackfillTotalByTableID(tableID int64, label, schemaName, tableName, optionalColOrIdxName string) prometheus.Counter {
	typeLabel := GenerateReorgLabel(label, schemaName, tableName, optionalColOrIdxName)
	backfillMetricsRegistry.register(tableID, typeLabel)
	return BackfillTotalCounter.WithLabelValues(typeLabel)
}

// GetBackfillProgressByTableID returns the Gauge for the given table ID and type label.
func GetBackfillProgressByTableID(tableID int64, label, schemaName, tableName, optionalColOrIdxName string) prometheus.Gauge {
	typeLabel := GenerateReorgLabel(label, schemaName, tableName, optionalColOrIdxName)
	backfillMetricsRegistry.register(tableID, typeLabel)
	return BackfillProgressGauge.WithLabelValues(typeLabel)
}

// DDLClearBackfillMetrics deletes the backfill metrics registered under a table ID.
func DDLClearBackfillMetrics(tableID int64) {
	for _, typeLabel := range backfillMetricsRegistry.clear(tableID) {
		BackfillProgressGauge.DeleteLabelValues(typeLabel)
		BackfillTotalCounter.DeleteLabelValues(typeLabel)
	}
}

// DDLHasBackfillMetrics reports whether any backfill metrics are registered.
func DDLHasBackfillMetrics() bool {
	backfillMetricsRegistry.mu.Lock()
	defer backfillMetricsRegistry.mu.Unlock()
	return len(backfillMetricsRegistry.byTblID) > 0
}

// GetBackfillLabelsForTest returns the registered labels for a table ID.
func GetBackfillLabelsForTest(tableID int64) map[string]struct{} {
	backfillMetricsRegistry.mu.Lock()
	defer backfillMetricsRegistry.mu.Unlock()

	labels, ok := backfillMetricsRegistry.byTblID[tableID]
	if !ok {
		return nil
	}

	out := make(map[string]struct{}, len(labels))
	for label := range labels {
		out[label] = struct{}{}
	}
	return out
}
