// Copyright 2016 PingCAP, Inc.
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

package ddl

import "github.com/prometheus/client_golang/prometheus"

var (
	jobsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "waiting_jobs",
			Help:      "Gauge of jobs.",
		}, []string{"action"})

	// operation result state
	opSucc             = "op_succ"
	opFailed           = "op_failed"
	handleJobHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "handle_job_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle jobs",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"action", "result_state"})

	batchAddIdxHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "batch_add_idx_succ",
			Help:      "Bucketed histogram of processing time (s) of batch handle data",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
		})

	// The syncer inits, restarts or clears.
	syncerInit            = "syncer_init"
	syncerRestart         = "syncer_restart"
	syncerClear           = "syncer_clear"
	deploySyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "deploy_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of deploy syncer",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"state", "result_state"})
	// The syncer updates its own version.
	updateSelfVersionHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "update_self_ver_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of update self version",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"result_state"})
	// The owner handles syncer's version.
	ownerUpdateGlobalVersion   = "update_global_version"
	ownerGetGlobalVersion      = "get_global_version"
	ownerCheckAllVersions      = "check_all_versions"
	ownerHandleSyncerHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "owner_handle_syncer_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle syncer",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"op", "result_state"})
)

func init() {
	prometheus.MustRegister(jobsGauge)
	prometheus.MustRegister(handleJobHistogram)
	prometheus.MustRegister(batchAddIdxHistogram)
	prometheus.MustRegister(deploySyncerHistogram)
	prometheus.MustRegister(updateSelfVersionHistogram)
	prometheus.MustRegister(ownerHandleSyncerHistogram)
}

func returnRetLable(err error) string {
	if err == nil {
		return opSucc
	}
	return opFailed
}
