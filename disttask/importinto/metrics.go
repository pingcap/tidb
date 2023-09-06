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
	"strconv"
	"sync"

	"github.com/pingcap/tidb/br/pkg/lightning/metric"
	"github.com/pingcap/tidb/disttask/framework/proto"
	tidbmetrics "github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

type taskMetrics struct {
	metrics *metric.Common
	counter int
}

// taskMetricManager manages the metrics of tasks.
// we have a set of metrics for each task, but both dispatcher and scheduler
// might use it, so add a manager to manage lifecycle of metrics for tasks.
// there might be a better way to do this.
type taskMetricManager struct {
	sync.RWMutex
	metricsMap map[string]*taskMetrics
}

var metricsManager = &taskMetricManager{
	metricsMap: make(map[string]*taskMetrics),
}

func (m *taskMetricManager) getMetrics(taskID int64) *metric.Common {
	m.Lock()
	defer m.Unlock()
	key := strconv.FormatInt(taskID, 10)
	metrics := tidbmetrics.GetRegisteredImportMetrics(promutil.NewDefaultFactory(),
		prometheus.Labels{
			proto.TaskIDLabelName: key,
		})
	if tm, ok := m.metricsMap[key]; !ok {
		tm = &taskMetrics{
			metrics: metrics,
			counter: 1,
		}
		m.metricsMap[key] = tm
	} else {
		tm.counter++
	}
	return metrics
}

func (m *taskMetricManager) unregister(taskID int64) {
	m.Lock()
	defer m.Unlock()
	key := strconv.FormatInt(taskID, 10)
	if tm, ok := m.metricsMap[key]; ok {
		tm.counter--
		if tm.counter == 0 {
			tidbmetrics.UnregisterImportMetrics(tm.metrics)
			delete(m.metricsMap, key)
		}
	}
}
