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
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics for the DDL package.
var (
	JobsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "waiting_jobs",
			Help:      "Gauge of jobs.",
		}, []string{"action"})

	HandleJobHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "handle_job_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle jobs",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"action", "result_state"})

	BatchAddIdxHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "batch_add_idx_succ",
			Help:      "Bucketed histogram of processing time (s) of batch handle data",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
		})

	SyncerInit    = "syncer_init"
	SyncerRestart = "syncer_restart"
	SyncerClear   = "syncer_clear"

	DeploySyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "deploy_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of deploy syncer",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"state", "result_state"})

	UpdateSelfVersionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "update_self_ver_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of update self version",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"result_state"})

	OwnerUpdateGlobalVersion   = "update_global_version"
	OwnerGetGlobalVersion      = "get_global_version"
	OwnerCheckAllVersions      = "check_all_versions"
	OwnerHandleSyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "owner_handle_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle syncer",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"op", "result_state"})

	// Metrics for ddl_worker.go.
	WorkerAddDDLJob         = "add_ddl_job"
	WorkerFinishDDLJob      = "finish_ddl_job"
	WorkerWaitSchemaChanged = "wait_schema_changed"
	CreateDDLWorker         = "create_ddl_worker"
	DDLWorkerHistogram      = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "ddl_worker_operation",
			Help:      "Bucketed histogram of processing time (s) of ddl worker operations",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
		}, []string{"op", "result_state"})

	DDLOwnerCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "ddl_owner",
			Help:      "Counter of isOwner.",
		})
)

func init() {
	prometheus.MustRegister(JobsGauge)
	prometheus.MustRegister(HandleJobHistogram)
	prometheus.MustRegister(BatchAddIdxHistogram)
	prometheus.MustRegister(DeploySyncerHistogram)
	prometheus.MustRegister(UpdateSelfVersionHistogram)
	prometheus.MustRegister(OwnerHandleSyncerHistogram)
	prometheus.MustRegister(DDLWorkerHistogram)
	prometheus.MustRegister(DDLOwnerCounter)
}
