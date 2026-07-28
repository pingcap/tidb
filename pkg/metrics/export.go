// Copyright 2026 PingCAP, Inc.
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

// Metrics of EXPORT TABLE, the rate() of the counters gives the export speed,
// and together with the subtask progress they show how far a task has gone.
var (
	// ExportRowsCounter records the rows exported by EXPORT TABLE tasks.
	ExportRowsCounter *prometheus.CounterVec
	// ExportBytesCounter records the encoded bytes written by EXPORT TABLE tasks.
	ExportBytesCounter *prometheus.CounterVec
	// ExportFilesCounter records the data files created by EXPORT TABLE tasks.
	ExportFilesCounter *prometheus.CounterVec
)

// InitExportMetrics initializes EXPORT TABLE metrics.
func InitExportMetrics() {
	ExportRowsCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "export",
			Name:      "rows_total",
			Help:      "Counter of rows exported by EXPORT TABLE tasks.",
		}, []string{"task_id"})
	ExportBytesCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "export",
			Name:      "bytes_total",
			Help:      "Counter of encoded bytes written by EXPORT TABLE tasks.",
		}, []string{"task_id"})
	ExportFilesCounter = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "export",
			Name:      "files_total",
			Help:      "Counter of data files created by EXPORT TABLE tasks.",
		}, []string{"task_id"})
}
