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

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

type taskMetrics struct {
	metrics *metric.Common
	counter int
}

// taskMetricManager manages the metrics of IMPORT INTO tasks.
// we have a set of metrics for each task, with different task_id const label.
// metrics is passed by context value to avoid passing parameters everywhere.
// both scheduler and taskExecutor might use it, to avoid registered again,
// we add a manager to manage lifecycle of metrics for tasks.
type taskMetricManager struct {
	sync.RWMutex
	metricsMap map[int64]*taskMetrics
}

var metricsManager = &taskMetricManager{
	metricsMap: make(map[int64]*taskMetrics),
}

// getOrCreateMetrics gets or creates the metrics for the task.
// if the metrics has been created, the counter will be increased.
func (m *taskMetricManager) getOrCreateMetrics(taskID int64) *metric.Common {
	m.Lock()
	defer m.Unlock()
	tm, ok := m.metricsMap[taskID]
	if !ok {
		metrics := tidbmetrics.GetRegisteredImportMetrics(promutil.NewDefaultFactory(),
			prometheus.Labels{
				proto.TaskIDLabelName: strconv.FormatInt(taskID, 10),
			})
		tm = &taskMetrics{
			metrics: metrics,
		}
		m.metricsMap[taskID] = tm
	}

	tm.counter++

	return tm.metrics
}

// unregister count down the metrics for the task.
// if the counter is 0, the metrics will be unregistered.
func (m *taskMetricManager) unregister(taskID int64) {
	m.Lock()
	defer m.Unlock()
	if tm, ok := m.metricsMap[taskID]; ok {
		tm.counter--
		if tm.counter == 0 {
			tidbmetrics.UnregisterImportMetrics(tm.metrics)
			delete(m.metricsMap, taskID)
		}
	}
}
