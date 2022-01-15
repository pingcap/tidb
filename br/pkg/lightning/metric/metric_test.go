// Copyright 2019 PingCAP, Inc.
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

package metric_test

import (
	"errors"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/prometheus/client_golang/prometheus"
)

type testMetricSuite struct{}

func (s *testMetricSuite) SetUpSuite(c *C)    {}
func (s *testMetricSuite) TearDownSuite(c *C) {}

var _ = Suite(&testMetricSuite{})

func TestMetric(t *testing.T) {
	TestingT(t)
}

func (s *testMetricSuite) TestReadCounter(c *C) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{})
	counter.Add(1256.0)
	counter.Add(2214.0)
	c.Assert(metric.ReadCounter(counter), Equals, 3470.0)
}

func (s *testMetricSuite) TestReadHistogramSum(c *C) {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{})
	histogram.Observe(11131.5)
	histogram.Observe(15261.0)
	c.Assert(metric.ReadHistogramSum(histogram), Equals, 26392.5)
}

func (s *testMetricSuite) TestRecordEngineCount(c *C) {
	metric.RecordEngineCount("table1", nil)
	metric.RecordEngineCount("table1", errors.New("mock error"))
	successCounter, err := metric.ProcessedEngineCounter.GetMetricWithLabelValues("table1", "success")
	c.Assert(err, IsNil)
	c.Assert(metric.ReadCounter(successCounter), Equals, 1.0)
	failureCount, err := metric.ProcessedEngineCounter.GetMetricWithLabelValues("table1", "failure")
	c.Assert(err, IsNil)
	c.Assert(metric.ReadCounter(failureCount), Equals, 1.0)
}
