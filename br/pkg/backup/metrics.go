// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package backup

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	backupRegionCounters = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "br",
			Subsystem: "raw",
			Name:      "backup_region",
			Help:      "Backup region statistic.",
		}, []string{"type"})

	backupRegionHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "br",
			Subsystem: "raw",
			Name:      "backup_region_seconds",
			Help:      "Backup region latency distributions.",
			Buckets:   prometheus.ExponentialBuckets(0.05, 2, 16),
		})
)

func init() { // nolint:gochecknoinits
	prometheus.MustRegister(backupRegionCounters)
	prometheus.MustRegister(backupRegionHistogram)
}
