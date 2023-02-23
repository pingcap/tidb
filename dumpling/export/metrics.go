// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"math"

	"github.com/pingcap/tidb/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/atomic"
)

type metrics struct {
	finishedSizeGauge              *prometheus.GaugeVec
	finishedRowsGauge              *prometheus.GaugeVec
	finishedTablesCounter          *prometheus.CounterVec
	estimateTotalRowsCounter       *prometheus.CounterVec
	writeTimeHistogram             *prometheus.HistogramVec
	receiveWriteChunkTimeHistogram *prometheus.HistogramVec
	errorCount                     *prometheus.CounterVec
	taskChannelCapacity            *prometheus.GaugeVec
	// todo: add these to metrics
	totalChunks     atomic.Int64
	completedChunks atomic.Int64
	progressReady   atomic.Bool
}

func newMetrics(f promutil.Factory, constLabels prometheus.Labels) *metrics {
	m := metrics{}
	m.finishedSizeGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   "dumpling",
			Subsystem:   "dump",
			Name:        "finished_size",
			Help:        "counter for dumpling finished file size",
			ConstLabels: constLabels,
		}, []string{})
	m.estimateTotalRowsCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   "dumpling",
			Subsystem:   "dump",
			Name:        "estimate_total_rows",
			Help:        "estimate total rows for dumpling tables",
			ConstLabels: constLabels,
		}, []string{})
	m.finishedRowsGauge = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   "dumpling",
			Subsystem:   "dump",
			Name:        "finished_rows",
			Help:        "counter for dumpling finished rows",
			ConstLabels: constLabels,
		}, []string{})
	m.finishedTablesCounter = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   "dumpling",
			Subsystem:   "dump",
			Name:        "finished_tables",
			Help:        "counter for dumpling finished tables",
			ConstLabels: constLabels,
		}, []string{})
	m.writeTimeHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "dumpling",
			Subsystem:   "write",
			Name:        "write_duration_time",
			Help:        "Bucketed histogram of write time (s) of files",
			Buckets:     prometheus.ExponentialBuckets(0.00005, 2, 20),
			ConstLabels: constLabels,
		}, []string{})
	m.receiveWriteChunkTimeHistogram = f.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace:   "dumpling",
			Subsystem:   "write",
			Name:        "receive_chunk_duration_time",
			Help:        "Bucketed histogram of receiving time (s) of chunks",
			Buckets:     prometheus.ExponentialBuckets(0.00005, 2, 20),
			ConstLabels: constLabels,
		}, []string{})
	m.errorCount = f.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   "dumpling",
			Subsystem:   "dump",
			Name:        "error_count",
			Help:        "Total error count during dumping progress",
			ConstLabels: constLabels,
		}, []string{})
	m.taskChannelCapacity = f.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   "dumpling",
			Subsystem:   "dump",
			Name:        "channel_capacity",
			Help:        "The task channel capacity during dumping progress",
			ConstLabels: constLabels,
		}, []string{})
	return &m
}

func (m *metrics) registerTo(registry promutil.Registry) {
	registry.MustRegister(m.finishedSizeGauge)
	registry.MustRegister(m.finishedRowsGauge)
	registry.MustRegister(m.estimateTotalRowsCounter)
	registry.MustRegister(m.finishedTablesCounter)
	registry.MustRegister(m.writeTimeHistogram)
	registry.MustRegister(m.receiveWriteChunkTimeHistogram)
	registry.MustRegister(m.errorCount)
	registry.MustRegister(m.taskChannelCapacity)
}

func (m *metrics) unregisterFrom(registry promutil.Registry) {
	registry.Unregister(m.finishedSizeGauge)
	registry.Unregister(m.finishedRowsGauge)
	registry.Unregister(m.estimateTotalRowsCounter)
	registry.Unregister(m.finishedTablesCounter)
	registry.Unregister(m.writeTimeHistogram)
	registry.Unregister(m.receiveWriteChunkTimeHistogram)
	registry.Unregister(m.errorCount)
	registry.Unregister(m.taskChannelCapacity)
}

// ReadCounter reports the current value of the counter.
func ReadCounter(counterVec *prometheus.CounterVec) float64 {
	if counterVec == nil {
		return math.NaN()
	}
	counter := counterVec.With(nil)
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Counter.GetValue()
}

// AddCounter adds a counter.
func AddCounter(counterVec *prometheus.CounterVec, v float64) {
	if counterVec == nil {
		return
	}
	counterVec.With(nil).Add(v)
}

// IncCounter incs a counter.
func IncCounter(counterVec *prometheus.CounterVec) {
	if counterVec == nil {
		return
	}
	counterVec.With(nil).Inc()
}

// ObserveHistogram observes a histogram
func ObserveHistogram(histogramVec *prometheus.HistogramVec, v float64) {
	if histogramVec == nil {
		return
	}
	histogramVec.With(nil).Observe(v)
}

// ReadGauge reports the current value of the gauge.
func ReadGauge(gaugeVec *prometheus.GaugeVec) float64 {
	if gaugeVec == nil {
		return math.NaN()
	}
	gauge := gaugeVec.With(nil)
	var metric dto.Metric
	if err := gauge.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Gauge.GetValue()
}

// AddGauge adds a gauge
func AddGauge(gaugeVec *prometheus.GaugeVec, v float64) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(nil).Add(v)
}

// SubGauge subs a gauge
func SubGauge(gaugeVec *prometheus.GaugeVec, v float64) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(nil).Sub(v)
}

// IncGauge incs a gauge
func IncGauge(gaugeVec *prometheus.GaugeVec) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(nil).Inc()
}

// DecGauge decs a gauge
func DecGauge(gaugeVec *prometheus.GaugeVec) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(nil).Dec()
}
