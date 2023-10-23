// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	RestoreInFlightCounters = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "br",
			Subsystem: "restore",
			Name:      "channel",
			Help:      "Restore speed statistic.",
		}, []string{"type"})
)

func initRestoreMetrics() { // nolint:gochecknoinits
	prometheus.MustRegister(RestoreInFlightCounters)
}
