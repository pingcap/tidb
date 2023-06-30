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
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/util/promutil"
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
	m := metric.NewMetrics(promutil.NewDefaultFactory())
	r := prometheus.NewRegistry()
	m.RegisterTo(r)
	require.True(t, r.Unregister(m.ImporterEngineCounter))
	require.True(t, r.Unregister(m.IdleWorkersGauge))
	require.True(t, r.Unregister(m.KvEncoderCounter))
	require.True(t, r.Unregister(m.TableCounter))
	require.True(t, r.Unregister(m.ProcessedEngineCounter))
	require.True(t, r.Unregister(m.ChunkCounter))
	require.True(t, r.Unregister(m.BytesCounter))
	require.True(t, r.Unregister(m.RowsCounter))
	require.True(t, r.Unregister(m.ImportSecondsHistogram))
	require.True(t, r.Unregister(m.ChunkParserReadBlockSecondsHistogram))
	require.True(t, r.Unregister(m.ApplyWorkerSecondsHistogram))
	require.True(t, r.Unregister(m.RowReadSecondsHistogram))
	require.True(t, r.Unregister(m.RowReadBytesHistogram))
	require.True(t, r.Unregister(m.RowEncodeSecondsHistogram))
	require.True(t, r.Unregister(m.RowKVDeliverSecondsHistogram))
	require.True(t, r.Unregister(m.BlockDeliverSecondsHistogram))
	require.True(t, r.Unregister(m.BlockDeliverBytesHistogram))
	require.True(t, r.Unregister(m.BlockDeliverKVPairsHistogram))
	require.True(t, r.Unregister(m.ChecksumSecondsHistogram))
	require.True(t, r.Unregister(m.SSTSecondsHistogram))
	require.True(t, r.Unregister(m.LocalStorageUsageBytesGauge))
	require.True(t, r.Unregister(m.ProgressGauge))
}

func TestMetricsUnregister(t *testing.T) {
	m := metric.NewMetrics(promutil.NewDefaultFactory())
	r := prometheus.NewRegistry()
	m.RegisterTo(r)
	m.UnregisterFrom(r)
	require.False(t, r.Unregister(m.ImporterEngineCounter))
	require.False(t, r.Unregister(m.IdleWorkersGauge))
	require.False(t, r.Unregister(m.KvEncoderCounter))
	require.False(t, r.Unregister(m.TableCounter))
	require.False(t, r.Unregister(m.ProcessedEngineCounter))
	require.False(t, r.Unregister(m.ChunkCounter))
	require.False(t, r.Unregister(m.BytesCounter))
	require.False(t, r.Unregister(m.RowsCounter))
	require.False(t, r.Unregister(m.ImportSecondsHistogram))
	require.False(t, r.Unregister(m.ChunkParserReadBlockSecondsHistogram))
	require.False(t, r.Unregister(m.ApplyWorkerSecondsHistogram))
	require.False(t, r.Unregister(m.RowReadSecondsHistogram))
	require.False(t, r.Unregister(m.RowReadBytesHistogram))
	require.False(t, r.Unregister(m.RowEncodeSecondsHistogram))
	require.False(t, r.Unregister(m.RowKVDeliverSecondsHistogram))
	require.False(t, r.Unregister(m.BlockDeliverSecondsHistogram))
	require.False(t, r.Unregister(m.BlockDeliverBytesHistogram))
	require.False(t, r.Unregister(m.BlockDeliverKVPairsHistogram))
	require.False(t, r.Unregister(m.ChecksumSecondsHistogram))
	require.False(t, r.Unregister(m.SSTSecondsHistogram))
	require.False(t, r.Unregister(m.LocalStorageUsageBytesGauge))
	require.False(t, r.Unregister(m.ProgressGauge))
}

func TestContext(t *testing.T) {
	ctx := metric.NewContext(context.Background(), metric.NewMetrics(promutil.NewDefaultFactory()))
	m, ok := metric.FromContext(ctx)
	require.True(t, ok)
	require.NotNil(t, m)
}
