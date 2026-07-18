// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metricscommon

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestGetMergedConstLabels(t *testing.T) {
	bak := constLabels
	t.Cleanup(func() {
		constLabels = bak
	})
	SetConstLabels()
	require.EqualValues(t, prometheus.Labels{}, GetMergedConstLabels(nil))
	require.EqualValues(t, prometheus.Labels{"c": "3"}, GetMergedConstLabels(prometheus.Labels{"c": "3"}))

	SetConstLabels("a", "1", "b", "2")
	require.EqualValues(t, prometheus.Labels{"a": "1", "b": "2"}, GetMergedConstLabels(nil))
	require.EqualValues(t, prometheus.Labels{"a": "1", "b": "2"}, GetMergedConstLabels(prometheus.Labels{}))
	require.EqualValues(t, prometheus.Labels{"a": "1", "b": "2", "c": "3"}, GetMergedConstLabels(prometheus.Labels{"c": "3"}))
	// constLabels has higher priority
	require.EqualValues(t, prometheus.Labels{"a": "1", "b": "2"}, GetMergedConstLabels(prometheus.Labels{"a": "100"}))
}
