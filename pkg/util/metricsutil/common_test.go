// Copyright 2026 PingCAP, Inc.
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

package metricsutil

import (
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	metricscommon "github.com/pingcap/tidb/pkg/metrics/common"
	"github.com/stretchr/testify/require"
)

func TestRegisterMetricsWithKeyspaceObservabilityValues(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	t.Cleanup(func() {
		metricscommon.SetConstLabels()
	})

	labels := cloneConstLabels()
	labels["label_a"] = "value_a"
	require.Equal(t, "value_a", labels["label_a"])

	metricscommon.SetConstLabels("base_label", "base_value")
	config.UpdateGlobal(func(conf *config.Config) {
		conf.KeyspaceObservabilityValues = config.KeyspaceObservabilityValues{
			MetricLabels: map[string]string{"label_a": "value_a"},
		}
	})

	require.NoError(t, registerMetrics())
	labels = metricscommon.GetConstLabels()
	require.Equal(t, "base_value", labels["base_label"])
	require.Equal(t, "value_a", labels["label_a"])
}
