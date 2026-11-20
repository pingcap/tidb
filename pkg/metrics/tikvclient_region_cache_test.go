// Copyright 2025 PingCAP, Inc.
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
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegionCacheOperationsCounter(t *testing.T) {
	t.Parallel()

	require.NotNil(t, RegionCacheOperationsCounter, "RegionCacheOperationsCounter should be initialized")

	// Increase a couple of labeled series and validate values.
	RegionCacheOperationsCounter.WithLabelValues("batch_locate", "ok").Add(1)
	RegionCacheOperationsCounter.WithLabelValues("on_send_fail", "err").Add(2)

	ok := testutil.ToFloat64(RegionCacheOperationsCounter.WithLabelValues("batch_locate", "ok"))
	err := testutil.ToFloat64(RegionCacheOperationsCounter.WithLabelValues("on_send_fail", "err"))
	require.Equal(t, 1.0, ok)
	require.Equal(t, 2.0, err)
}


