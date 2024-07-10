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

package metrics

import (
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/prometheus/client_golang/prometheus"
)

const importMetricSubsystem = "import"

// GetRegisteredImportMetrics returns the registered import metrics.
func GetRegisteredImportMetrics(factory promutil.Factory, constLabels prometheus.Labels) *metric.Common {
	metrics := metric.NewCommon(factory, TiDB, importMetricSubsystem, constLabels)
	metrics.RegisterTo(prometheus.DefaultRegisterer)
	return metrics
}

// UnregisterImportMetrics unregisters the registered import metrics.
func UnregisterImportMetrics(metrics *metric.Common) {
	metrics.UnregisterFrom(prometheus.DefaultRegisterer)
}
