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
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
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
	m := metric.NewMetrics(promutil.NewDefaultFactory())
	m.RecordEngineCount("table1", nil)
	m.RecordEngineCount("table1", errors.New("mock error"))
	successCounter, err := m.ProcessedEngineCounter.GetMetricWithLabelValues("table1", "success")
	require.NoError(t, err)
	require.Equal(t, 1.0, metric.ReadCounter(successCounter))
	failureCount, err := m.ProcessedEngineCounter.GetMetricWithLabelValues("table1", "failure")
	require.NoError(t, err)
	require.Equal(t, 1.0, metric.ReadCounter(failureCount))
}

func TestMetricsRegister(t *testing.T) {
	getMetricCount := func(r *prometheus.Registry) int {
		ch := make(chan *prometheus.Desc)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Describe(ch)
			close(ch)
		}()
		var count int
		for range ch {
			count++
		}
		wg.Wait()
		return count
	}
	cm := metric.NewCommon(promutil.NewDefaultFactory(), "test", "", nil)
	r := prometheus.NewRegistry()
	require.Zero(t, getMetricCount(r))
	cm.RegisterTo(r)
	require.Equal(t, 8, getMetricCount(r))
	cm.UnregisterFrom(r)
	require.Zero(t, getMetricCount(r))

	m := metric.NewMetrics(promutil.NewDefaultFactory())
	r = prometheus.NewRegistry()
	m.RegisterTo(r)
	require.Equal(t, 22, getMetricCount(r))
	assert.True(t, r.Unregister(m.ImporterEngineCounter))
	assert.True(t, r.Unregister(m.IdleWorkersGauge))
	assert.True(t, r.Unregister(m.KvEncoderCounter))
	assert.True(t, r.Unregister(m.TableCounter))
	assert.True(t, r.Unregister(m.ProcessedEngineCounter))
	assert.True(t, r.Unregister(m.ChunkCounter))
	assert.True(t, r.Unregister(m.BytesCounter))
	assert.True(t, r.Unregister(m.RowsCounter))
	assert.True(t, r.Unregister(m.ImportSecondsHistogram))
	assert.True(t, r.Unregister(m.ChunkParserReadBlockSecondsHistogram))
	assert.True(t, r.Unregister(m.ApplyWorkerSecondsHistogram))
	assert.True(t, r.Unregister(m.RowReadSecondsHistogram))
	assert.True(t, r.Unregister(m.RowReadBytesHistogram))
	assert.True(t, r.Unregister(m.RowEncodeSecondsHistogram))
	assert.True(t, r.Unregister(m.RowKVDeliverSecondsHistogram))
	assert.True(t, r.Unregister(m.BlockDeliverSecondsHistogram))
	assert.True(t, r.Unregister(m.BlockDeliverBytesHistogram))
	assert.True(t, r.Unregister(m.BlockDeliverKVPairsHistogram))
	assert.True(t, r.Unregister(m.ChecksumSecondsHistogram))
	assert.True(t, r.Unregister(m.SSTSecondsHistogram))
	assert.True(t, r.Unregister(m.LocalStorageUsageBytesGauge))
	assert.True(t, r.Unregister(m.ProgressGauge))
	require.Zero(t, getMetricCount(r))
}

func TestMetricsUnregister(t *testing.T) {
	m := metric.NewMetrics(promutil.NewDefaultFactory())
	r := prometheus.NewRegistry()
	m.RegisterTo(r)
	m.UnregisterFrom(r)
	assert.False(t, r.Unregister(m.ImporterEngineCounter))
	assert.False(t, r.Unregister(m.IdleWorkersGauge))
	assert.False(t, r.Unregister(m.KvEncoderCounter))
	assert.False(t, r.Unregister(m.TableCounter))
	assert.False(t, r.Unregister(m.ProcessedEngineCounter))
	assert.False(t, r.Unregister(m.ChunkCounter))
	assert.False(t, r.Unregister(m.BytesCounter))
	assert.False(t, r.Unregister(m.RowsCounter))
	assert.False(t, r.Unregister(m.ImportSecondsHistogram))
	assert.False(t, r.Unregister(m.ChunkParserReadBlockSecondsHistogram))
	assert.False(t, r.Unregister(m.ApplyWorkerSecondsHistogram))
	assert.False(t, r.Unregister(m.RowReadSecondsHistogram))
	assert.False(t, r.Unregister(m.RowReadBytesHistogram))
	assert.False(t, r.Unregister(m.RowEncodeSecondsHistogram))
	assert.False(t, r.Unregister(m.RowKVDeliverSecondsHistogram))
	assert.False(t, r.Unregister(m.BlockDeliverSecondsHistogram))
	assert.False(t, r.Unregister(m.BlockDeliverBytesHistogram))
	assert.False(t, r.Unregister(m.BlockDeliverKVPairsHistogram))
	assert.False(t, r.Unregister(m.ChecksumSecondsHistogram))
	assert.False(t, r.Unregister(m.SSTSecondsHistogram))
	assert.False(t, r.Unregister(m.LocalStorageUsageBytesGauge))
	assert.False(t, r.Unregister(m.ProgressGauge))
}

func TestContext(t *testing.T) {
	metrics := metric.NewMetrics(promutil.NewDefaultFactory())
	ctx := metric.WithMetric(context.Background(), metrics)
	m, ok := metric.FromContext(ctx)
	require.True(t, ok)
	require.NotNil(t, m)
	m2, ok := metric.GetCommonMetric(ctx)
	require.True(t, ok)
	require.NotNil(t, m2)

	ctx = metric.WithCommonMetric(context.Background(), metrics.Common)
	m, ok = metric.FromContext(ctx)
	require.False(t, ok)
	require.Nil(t, m)
	m2, ok = metric.GetCommonMetric(ctx)
	require.True(t, ok)
	require.NotNil(t, m2)
}
