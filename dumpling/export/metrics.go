// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var (
	finishedSizeGauge              *prometheus.GaugeVec
	finishedRowsGauge              *prometheus.GaugeVec
	finishedTablesCounter          *prometheus.CounterVec
	estimateTotalRowsCounter       *prometheus.CounterVec
	writeTimeHistogram             *prometheus.HistogramVec
	receiveWriteChunkTimeHistogram *prometheus.HistogramVec
	errorCount                     *prometheus.CounterVec
	taskChannelCapacity            *prometheus.GaugeVec
)

// InitMetricsVector inits metrics vectors.
// This function must run before RegisterMetrics
func InitMetricsVector(labels prometheus.Labels) {
	labelNames := make([]string, 0, len(labels))
	for name := range labels {
		labelNames = append(labelNames, name)
	}
	finishedSizeGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_size",
			Help:      "counter for dumpling finished file size",
		}, labelNames)
	estimateTotalRowsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "estimate_total_rows",
			Help:      "estimate total rows for dumpling tables",
		}, labelNames)
	finishedRowsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_rows",
			Help:      "counter for dumpling finished rows",
		}, labelNames)
	finishedTablesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_tables",
			Help:      "counter for dumpling finished tables",
		}, labelNames)
	writeTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dumpling",
			Subsystem: "write",
			Name:      "write_duration_time",
			Help:      "Bucketed histogram of write time (s) of files",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, labelNames)
	receiveWriteChunkTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dumpling",
			Subsystem: "write",
			Name:      "receive_chunk_duration_time",
			Help:      "Bucketed histogram of receiving time (s) of chunks",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, labelNames)
	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "error_count",
			Help:      "Total error count during dumping progress",
		}, labelNames)
	taskChannelCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "channel_capacity",
			Help:      "The task channel capacity during dumping progress",
		}, labelNames)
}

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	if finishedSizeGauge == nil {
		return
	}
	registry.MustRegister(finishedSizeGauge)
	registry.MustRegister(finishedRowsGauge)
	registry.MustRegister(estimateTotalRowsCounter)
	registry.MustRegister(finishedTablesCounter)
	registry.MustRegister(writeTimeHistogram)
	registry.MustRegister(receiveWriteChunkTimeHistogram)
	registry.MustRegister(errorCount)
	registry.MustRegister(taskChannelCapacity)
}

// RemoveLabelValuesWithTaskInMetrics removes metrics of specified labels.
func RemoveLabelValuesWithTaskInMetrics(labels prometheus.Labels) {
	if finishedSizeGauge == nil {
		return
	}
	finishedSizeGauge.Delete(labels)
	finishedRowsGauge.Delete(labels)
	estimateTotalRowsCounter.Delete(labels)
	finishedTablesCounter.Delete(labels)
	writeTimeHistogram.Delete(labels)
	receiveWriteChunkTimeHistogram.Delete(labels)
	errorCount.Delete(labels)
	taskChannelCapacity.Delete(labels)
}

// ReadCounter reports the current value of the counter.
func ReadCounter(counterVec *prometheus.CounterVec, labels prometheus.Labels) float64 {
	if counterVec == nil {
		return math.NaN()
	}
	counter := counterVec.With(labels)
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Counter.GetValue()
}

// AddCounter adds a counter.
func AddCounter(counterVec *prometheus.CounterVec, labels prometheus.Labels, v float64) {
	if counterVec == nil {
		return
	}
	counterVec.With(labels).Add(v)
}

// IncCounter incs a counter.
func IncCounter(counterVec *prometheus.CounterVec, labels prometheus.Labels) {
	if counterVec == nil {
		return
	}
	counterVec.With(labels).Inc()
}

// ObserveHistogram observes a histogram
func ObserveHistogram(histogramVec *prometheus.HistogramVec, labels prometheus.Labels, v float64) {
	if histogramVec == nil {
		return
	}
	histogramVec.With(labels).Observe(v)
}

// ReadGauge reports the current value of the gauge.
func ReadGauge(gaugeVec *prometheus.GaugeVec, labels prometheus.Labels) float64 {
	if gaugeVec == nil {
		return math.NaN()
	}
	gauge := gaugeVec.With(labels)
	var metric dto.Metric
	if err := gauge.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Gauge.GetValue()
}

// AddGauge adds a gauge
func AddGauge(gaugeVec *prometheus.GaugeVec, labels prometheus.Labels, v float64) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(labels).Add(v)
}

// SubGauge subs a gauge
func SubGauge(gaugeVec *prometheus.GaugeVec, labels prometheus.Labels, v float64) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(labels).Sub(v)
}

// IncGauge incs a gauge
func IncGauge(gaugeVec *prometheus.GaugeVec, labels prometheus.Labels) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(labels).Inc()
}

// DecGauge decs a gauge
func DecGauge(gaugeVec *prometheus.GaugeVec, labels prometheus.Labels) {
	if gaugeVec == nil {
		return
	}
	gaugeVec.With(labels).Dec()
}
