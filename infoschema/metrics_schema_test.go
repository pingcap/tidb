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

package infoschema_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/util/set"
	"github.com/stretchr/testify/require"
)

func TestMetricSchemaDef(t *testing.T) {
	for name, def := range infoschema.MetricTableMap {
		if strings.Contains(def.PromQL, "$QUANTILE") || strings.Contains(def.PromQL, "histogram_quantile") {
			require.Greaterf(t, def.Quantile, float64(0), "the quantile of metric table %v should > 0", name)
		} else {
			require.Equalf(t, float64(0), def.Quantile, "metric table %v has quantile, but doesn't contain $QUANTILE in promQL ", name)
		}
		if strings.Contains(def.PromQL, "$LABEL_CONDITIONS") {
			require.Greaterf(t, len(def.Labels), 0, "the labels of metric table %v should not be nil", name)
		} else {
			li := strings.Index(def.PromQL, "{")
			ri := strings.Index(def.PromQL, "}")
			// ri - li > 1 means already has label conditions, so no need $LABEL_CONDITIONS anymore.
			if !(ri-li > 1) {
				require.Lenf(t, def.Labels, 0, "metric table %v has labels, but doesn't contain $LABEL_CONDITIONS in promQL", name)
			}
		}

		if strings.Contains(def.PromQL, " by (") {
			for _, label := range def.Labels {
				require.Containsf(t, def.PromQL, label, "metric table %v has labels, but doesn't contain label %v in promQL", name, label)
			}
		}
		if name != strings.ToLower(name) {
			require.Equal(t, strings.ToLower(name), name, "metric table name %v should be lower case", name)
		}
		// INSTANCE must be the first label
		if set.NewStringSet(def.Labels...).Exist("instance") {
			require.Equalf(t, "instance", def.Labels[0], "metrics table %v: expect `instance`is the first label but got %v", name, def.Labels)
		}
	}
}
