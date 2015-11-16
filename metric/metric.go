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

package metric

import (
	"os"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
)

var (
	r    = metrics.NewRegistry()
	once sync.Once
)

// Register registers a new metric for observation.
func Register(name string, m interface{}) {
	r.Register(name, m)
}

// Inc increases specific counter metric.
func Inc(name string, i int64) {
	if c := r.GetOrRegister(name, metrics.NewCounter()); c != nil {
		c.(metrics.Counter).Inc(i)
	}
}

// RecordTime records time elapse from startTime for given metric.
func RecordTime(name string, startTime time.Time) {
	if h := r.GetOrRegister(name, metrics.NewHistogram(metrics.NewUniformSample(100))); h != nil {
		elapse := time.Since(startTime).Nanoseconds() / int64(time.Millisecond)
		h.(metrics.Histogram).Update(elapse)
	}
}

// RunMetric reports metric result over a given time interval.
func RunMetric(interval time.Duration) {
	once.Do(func() {
		go func() {
			metrics.Write(r, interval, os.Stdout)
		}()
	})
}
