// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package tidb_metrics

import (
	"github.com/rcrowley/go-metrics"
	"log"
	"os"
	"time"
)

var (
	mtxReg              metrics.Registry
	metricsCounterMap   = make(map[string]metrics.Counter)
	metricsHistogramMap = make(map[string]metrics.Histogram)
)

const (
	HbaseStoreIncCounter     = "themis.get.counter"
	HbaseStoreIncTimeSum     = "themis.put.counter"
	HbaseStoreIncAverageTime = "themis.delete.counter"
)

func RegMetircs(r metrics.Registry) {
	if mtxReg != nil {
		return
	}

	mtxReg = r
	metricsCounterMap[HbaseStoreIncCounter] = metrics.NewCounter()
	metricsCounterMap[HbaseStoreIncTimeSum] = metrics.NewCounter()
	metricsHistogramMap[HbaseStoreIncAverageTime] = metrics.NewHistogram(metrics.NewUniformSample(1028))

	for k, v := range metricsCounterMap {
		mtxReg.Register(k, v)
	}
	for k, v := range metricsHistogramMap {
		mtxReg.Register(k, v)
	}

	metrics.Log(mtxReg, 30e9, log.New(os.Stderr, "metrics: ", log.Lmicroseconds))
}

// c: call number counter, sumC:call total time counter, h:record call average time
func RecordMetrics(c string, sumC string, h string, startTime time.Time) {
	RecordCounterMetrics(c, 1)
	RecordSumAndAverageTimeMetrics(sumC, h, startTime)
}

// sumC:call total time counter, h:record call average time
func RecordSumAndAverageTimeMetrics(sumC string, h string, startTime time.Time) {
	// ms
	d := time.Since(startTime).Nanoseconds() / 1000000
	RecordCounterMetrics(sumC, d)
	RecordHistogramMetrics(h, d)
}

func RecordCounterMetrics(metricsItem string, n int64) {
	c := metricsCounterMap[metricsItem]
	if c != nil {
		c.Inc(n)
	}
}

func RecordHistogramMetrics(metricsItem string, n int64) {
	h := metricsHistogramMap[metricsItem]
	if h != nil {
		h.Update(n)
	}
}
