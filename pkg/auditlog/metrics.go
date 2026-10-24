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

package auditlog

import (
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// AuditLogEventsTotal counts the total number of audit log events captured.
	AuditLogEventsTotal *prometheus.CounterVec

	// AuditLogWriteErrors counts the number of errors encountered during audit log writing.
	AuditLogWriteErrors *prometheus.CounterVec

	// AuditLogFlushDuration tracks the time taken to flush audit log batches.
	AuditLogFlushDuration *prometheus.HistogramVec

	// AuditLogBufferUsage tracks the current buffer usage of the audit log writer.
	AuditLogBufferUsage *prometheus.GaugeVec

	// AuditLogActiveRules tracks the number of active audit rules.
	AuditLogActiveRules *prometheus.GaugeVec
)

// InitAuditMetrics initializes all audit log related metrics.
func InitAuditMetrics() {
	AuditLogEventsTotal = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "events_total",
			Help:      "Total number of audit log events captured.",
		}, []string{"query_type", "status"})

	AuditLogWriteErrors = metricscommon.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "write_errors_total",
			Help:      "Total number of errors during audit log writing.",
		}, []string{"error_type"})

	AuditLogFlushDuration = metricscommon.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "flush_duration_seconds",
			Help:      "Time taken to flush audit log batches.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 12),
		}, []string{})

	AuditLogBufferUsage = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "buffer_usage",
			Help:      "Current number of events in the audit log buffer.",
		}, []string{})

	AuditLogActiveRules = metricscommon.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "tidb",
			Subsystem: "audit",
			Name:      "active_rules",
			Help:      "Number of active audit rules.",
		}, []string{})
}

// RegisterAuditMetrics registers all audit log metrics with the given registry.
func RegisterAuditMetrics(r prometheus.Registerer) {
	r.MustRegister(AuditLogEventsTotal)
	r.MustRegister(AuditLogWriteErrors)
	r.MustRegister(AuditLogFlushDuration)
	r.MustRegister(AuditLogBufferUsage)
	r.MustRegister(AuditLogActiveRules)
}
