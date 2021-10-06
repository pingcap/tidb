// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package logutil

import (
	"fmt"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// MetricTableCreatedCounter counts how many tables created.
// TODO: when br decided to introduce Prometheus, move this to its metric package.
var MetricTableCreatedCounter = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "BR",
	Name:      "table_created",
	Help:      "The count of tables have been created.",
})

// RateTracer is a trivial rate tracer based on a prometheus counter.
// It traces the average speed from it was created.
type RateTracer struct {
	start time.Time
	base  float64
	prometheus.Counter
}

// TraceRateOver make a trivial rater based on a counter.
// the current value of this counter would be omitted.
func TraceRateOver(counter prometheus.Counter) RateTracer {
	return RateTracer{
		start:   time.Now(),
		Counter: counter,
		base:    metric.ReadCounter(counter),
	}
}

// Rate returns the average rate from when it was created.
func (r *RateTracer) Rate() float64 {
	return r.RateAt(time.Now())
}

// RateAt returns the rate until some instant. This function is mainly for testing.
// WARN: the counter value for calculating is still its CURRENT VALUE.
func (r *RateTracer) RateAt(instant time.Time) float64 {
	return (metric.ReadCounter(r.Counter) - r.base) / instant.Sub(r.start).Seconds()
}

// L make a logger with the current speed.
func (r *RateTracer) L() *zap.Logger {
	return log.With(zap.String("speed", fmt.Sprintf("%.2f ops/s", r.Rate())))
}
