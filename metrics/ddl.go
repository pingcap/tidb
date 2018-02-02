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

var (
	// JobsGauge is the metrics for waiting ddl job.
	JobsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "waiting_jobs",
			Help:      "Gauge of jobs.",
		}, []string{"action"})

	// HandleJobHistogram is the metrics for handle ddl job duration.
	HandleJobHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "handle_job_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle jobs",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"action", "result_state"})

	// BatchAddIdxHistogram is the histogram of processing time (s) of batch handle data.
	BatchAddIdxHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "batch_add_idx_succ",
			Help:      "Bucketed histogram of processing time (s) of batch handle data",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
		})

	// SyncerInit event.
	SyncerInit = "syncer_init"
	// SyncerRestart event.
	SyncerRestart = "syncer_restart"
	// SyncerClear event.
	SyncerClear = "syncer_clear"

	// DeploySyncerHistogram is the histogram of processing time (s) of deploy syncer.
	DeploySyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "deploy_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of deploy syncer",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"state", "result_state"})

	// UpdateSelfVersionHistogram is the histogram of processing time (s) of update self version.
	UpdateSelfVersionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "update_self_ver_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of update self version",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"result_state"})

	// OwnerUpdateGlobalVersion is the action of updating global schema version by DDL owner.
	OwnerUpdateGlobalVersion = "update_global_version"
	// OwnerGetGlobalVersion is the action of getting global schema version by DDL owner.
	OwnerGetGlobalVersion = "get_global_version"
	// OwnerCheckAllVersions is the action of checking global schema version by DDL owner.
	OwnerCheckAllVersions = "check_all_versions"
	// OwnerHandleSyncerHistogram is the histogram of processing time (s) of handle syncer.
	OwnerHandleSyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "owner_handle_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle syncer",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"op", "result_state"})
)

func init() {
	prometheus.MustRegister(JobsGauge)
	prometheus.MustRegister(HandleJobHistogram)
	prometheus.MustRegister(BatchAddIdxHistogram)
	prometheus.MustRegister(DeploySyncerHistogram)
	prometheus.MustRegister(UpdateSelfVersionHistogram)
	prometheus.MustRegister(OwnerHandleSyncerHistogram)
}
