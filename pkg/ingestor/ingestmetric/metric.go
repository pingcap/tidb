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

package ingestmetric

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	lblAPI = "api"
	// LabelWriteAPI is the label value for the write API.
	LabelWriteAPI = "write"
	// LabelIngestAPI is the label value for the ingest API.
	LabelIngestAPI = "ingest"
)

var (
	// WriteIngestAPIDuration records the duration of write and ingest APIs in nextgen.
	WriteIngestAPIDuration *prometheus.HistogramVec
	// WriteAPIDuration records the duration of write API.
	WriteAPIDuration prometheus.Observer
	// IngestAPIDuration records the duration of ingest API.
	IngestAPIDuration prometheus.Observer
)

// InitIngestMetrics initializes ingest metrics.
func InitIngestMetrics() {
	WriteIngestAPIDuration = metricscommon.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "ingestor",
		Name:      "write_ingest_api_duration",
		Help:      "write and ingest API duration",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 524s
	}, []string{lblAPI})
	WriteAPIDuration = WriteIngestAPIDuration.WithLabelValues(LabelWriteAPI)
	IngestAPIDuration = WriteIngestAPIDuration.WithLabelValues(LabelIngestAPI)
}

// Register registers ingest metrics.
func Register(register prometheus.Registerer) {
	register.MustRegister(WriteIngestAPIDuration)
}
