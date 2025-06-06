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
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics
var (
	GlobalAutoID      = "global"
	TableAutoIDAlloc  = "alloc"
	TableAutoIDRebase = "rebase"
	AutoIDHistogram   *prometheus.HistogramVec

	GetSchemaDiff    = "get_schema_diff"
	SetSchemaDiff    = "set_schema_diff"
	GetHistoryDDLJob = "get_history_ddl_job"

	MetaHistogram          *prometheus.HistogramVec
	ResetAutoIDConnCounter prometheus.Counter
)

// InitMetaMetrics initializes meta metrics.
func InitMetaMetrics() {
	AutoIDHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "autoid",
			Name:      "operation_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of handled autoid.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblResult})

	MetaHistogram = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "meta",
			Name:      "operation_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) of tidb meta data operations.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 29), // 0.5ms ~ 1.5days
		}, []string{LblType, LblResult})

	ResetAutoIDConnCounter = metricscommon.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "meta",
			Name:      "autoid_client_conn_reset_total",
			Help:      "Counter of resetting autoid client connection.",
		})
}
