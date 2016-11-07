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
		}, []string{"type", "action"})

	// handle job result state.
	handleJobSucc      = "handle_job_succ"
	handleJobFailed    = "handle_job_failed"
	handleJobHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "handle_job_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handle jobs",
			Buckets:   prometheus.ExponentialBuckets(0.01, 2, 20),
		}, []string{"type", "action", "result_state"})

	// handle batch data type.
	batchAddCol              = "batch_add_col"
	batchAddIdx              = "batch_add_idx"
	batchDelData             = "batch_del_data"
	batchHandleDataHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "ddl",
			Name:      "batch_add_or_del_data_succ",
			Help:      "Bucketed histogram of processing time (s) of batch handle data",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20),
		}, []string{"handle_data_type"})
)

func init() {
	prometheus.MustRegister(jobsGauge)
	prometheus.MustRegister(handleJobHistogram)
	prometheus.MustRegister(batchHandleDataHistogram)
}
