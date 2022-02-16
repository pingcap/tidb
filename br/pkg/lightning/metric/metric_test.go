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

	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestReadCounter(t *testing.T) {
	counter := prometheus.NewCounter(prometheus.CounterOpts{})
	counter.Add(1256.0)
	counter.Add(2214.0)
	require.Equal(t, 3470.0, metric.ReadCounter(counter))
}

func TestReadHistogramSum(t *testing.T) {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{})
	histogram.Observe(11131.5)
	histogram.Observe(15261.0)
	require.Equal(t, 26392.5, metric.ReadHistogramSum(histogram))
}

func TestRecordEngineCount(t *testing.T) {
	metric.RecordEngineCount("table1", nil)
	metric.RecordEngineCount("table1", errors.New("mock error"))
	successCounter, err := metric.ProcessedEngineCounter.GetMetricWithLabelValues("table1", "success")
	require.NoError(t, err)
	require.Equal(t, 1.0, metric.ReadCounter(successCounter))
	failureCount, err := metric.ProcessedEngineCounter.GetMetricWithLabelValues("table1", "failure")
	require.NoError(t, err)
	require.Equal(t, 1.0, metric.ReadCounter(failureCount))
}
