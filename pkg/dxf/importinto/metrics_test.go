// Copyright 2023 PingCAP, Inc.
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

package importinto

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestMetricManager(t *testing.T) {
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

	backup := prometheus.DefaultRegisterer
	prometheus.DefaultRegisterer = prometheus.NewRegistry()
	t.Cleanup(func() {
		metricsManager = &taskMetricManager{
			metricsMap: make(map[int64]*taskMetrics),
		}
		prometheus.DefaultRegisterer = backup
	})
	metrics1 := metricsManager.getOrCreateMetrics(1)
	require.Len(t, metricsManager.metricsMap, 1)
	require.Equal(t, 1, metricsManager.metricsMap[1].counter)
	require.Equal(t, 8, getMetricCount(prometheus.DefaultRegisterer.(*prometheus.Registry)))
	metrics11 := metricsManager.getOrCreateMetrics(1)
	require.Equal(t, metrics1, metrics11)
	require.Equal(t, 2, metricsManager.metricsMap[1].counter)
	require.Equal(t, 8, getMetricCount(prometheus.DefaultRegisterer.(*prometheus.Registry)))
	metricsManager.getOrCreateMetrics(2)
	require.Len(t, metricsManager.metricsMap, 2)
	require.Equal(t, 2, metricsManager.metricsMap[1].counter)
	require.Equal(t, 1, metricsManager.metricsMap[2].counter)
	require.Equal(t, 16, getMetricCount(prometheus.DefaultRegisterer.(*prometheus.Registry)))

	metricsManager.unregister(1)
	require.Len(t, metricsManager.metricsMap, 2)
	require.Equal(t, 1, metricsManager.metricsMap[1].counter)
	require.Equal(t, 1, metricsManager.metricsMap[2].counter)
	require.Equal(t, 16, getMetricCount(prometheus.DefaultRegisterer.(*prometheus.Registry)))
	metricsManager.unregister(1)
	require.Len(t, metricsManager.metricsMap, 1)
	require.Equal(t, 1, metricsManager.metricsMap[2].counter)
	require.Equal(t, 8, getMetricCount(prometheus.DefaultRegisterer.(*prometheus.Registry)))
	metricsManager.unregister(2)
	require.Len(t, metricsManager.metricsMap, 0)
	require.Equal(t, 0, getMetricCount(prometheus.DefaultRegisterer.(*prometheus.Registry)))
}
