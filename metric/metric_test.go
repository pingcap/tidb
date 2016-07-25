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
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/rcrowley/go-metrics"
)

func TestT(t *testing.T) {
	TestingT(t)
}

type testSuite struct{}

var _ = Suite(&testSuite{})

const (
	testMetricName     = "test-metric"
	testTimeMetricName = "time-metric"
)

func (t *testSuite) TestRegMetric(c *C) {
	Register(testMetricName, metrics.NewCounter())
	Register(testMetricName, metrics.NewHistogram(metrics.NewUniformSample(1000)))

	v := r.Get(testMetricName)
	c.Assert(v, NotNil)

	// Will not overwrite
	_, ok := v.(metrics.Counter)
	c.Assert(ok, IsTrue)

	Inc(testMetricName, 1)
	Inc(testMetricName, 1)
	c.Assert(r.Get(testMetricName).(metrics.Counter).Count(), Equals, int64(2))

	Register(testTimeMetricName, metrics.NewHistogram(metrics.NewUniformSample(1000)))
	start := time.Now()
	time.Sleep(100 * time.Millisecond)
	RecordTime(testTimeMetricName, start)

	start = time.Now()
	time.Sleep(20 * time.Millisecond)
	RecordTime(testTimeMetricName, start)

	c.Assert(r.Get(testTimeMetricName).(metrics.Histogram).Max(), GreaterEqual, int64(100))
	c.Assert(r.Get(testTimeMetricName).(metrics.Histogram).Min(), Less, int64(100))
}

func (t *testSuite) TestTPSMetrics(c *C) {

	// Test tpsMetrics with manual tick.
	tm := newTPSMetrics()
	for i := 1; i < 10; i++ {
		for j := 0; j < 5; j++ {
			tm.Add(int64(i))
		}
		tm.tick()
		c.Assert(tm.Get(), Equals, int64(i*5))
		tm.tick()
		c.Assert(tm.Get(), Equals, int64(0))
	}

	// Test TPSMetrics with auto tick.
	m := NewTPSMetrics()
	for i := 1; i < 6; i++ {
		for j := 0; j < 5; j++ {
			m.Add(int64(i))
			time.Sleep(220 * time.Millisecond)
		}
		// It is hard to get the accurate tps because there is another timeline in tpsMetrics.
		// We could only get the upper/lower boundary for tps
		maxTPS := int64(i * 5)
		minTPS := int64((i - 1) * 4)
		c.Assert(m.Get(), LessEqual, maxTPS)
		c.Assert(m.Get(), GreaterEqual, minTPS)
	}
}
