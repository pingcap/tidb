// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"math"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

var (
	finishedSizeCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_size",
			Help:      "counter for dumpling finished file size",
		}, []string{})
	finishedRowsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_rows",
			Help:      "counter for dumpling finished rows",
		}, []string{})
	finishedTablesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "finished_tables",
			Help:      "counter for dumpling finished tables",
		}, []string{})
	writeTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dumpling",
			Subsystem: "write",
			Name:      "write_duration_time",
			Help:      "Bucketed histogram of write time (s) of files",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, []string{})
	receiveWriteChunkTimeHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "dumpling",
			Subsystem: "write",
			Name:      "receive_chunk_duration_time",
			Help:      "Bucketed histogram of write time (s) of files",
			Buckets:   prometheus.ExponentialBuckets(0.00005, 2, 20),
		}, []string{})
	errorCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "error_count",
			Help:      "Total error count during dumping progress",
		}, []string{})
	taskChannelCapacity = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "dumpling",
			Subsystem: "dump",
			Name:      "channel_capacity",
			Help:      "The task channel capacity during dumping progress",
		}, []string{})
)

// RegisterMetrics registers metrics.
func RegisterMetrics(registry *prometheus.Registry) {
	registry.MustRegister(finishedSizeCounter)
	registry.MustRegister(finishedRowsCounter)
	registry.MustRegister(finishedTablesCounter)
	registry.MustRegister(writeTimeHistogram)
	registry.MustRegister(receiveWriteChunkTimeHistogram)
	registry.MustRegister(errorCount)
	registry.MustRegister(taskChannelCapacity)
}

// RemoveLabelValuesWithTaskInMetrics removes metrics of specified labels.
func RemoveLabelValuesWithTaskInMetrics(labels prometheus.Labels) {
	finishedSizeCounter.Delete(labels)
	finishedRowsCounter.Delete(labels)
	finishedTablesCounter.Delete(labels)
	writeTimeHistogram.Delete(labels)
	receiveWriteChunkTimeHistogram.Delete(labels)
	errorCount.Delete(labels)
	taskChannelCapacity.Delete(labels)
}

// ReadCounter reports the current value of the counter.
func ReadCounter(counter prometheus.Counter) float64 {
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		return math.NaN()
	}
	return metric.Counter.GetValue()
}
