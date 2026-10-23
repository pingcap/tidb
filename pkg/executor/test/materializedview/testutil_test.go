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

package materializedview

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func newMViewTestKit(t testing.TB, store kv.Storage) *testkit.TestKit {
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_mview_enable = on")
	return tk
}

func waitMVTaskCancelWatcherRequested(t *testing.T, watchNamePrefix string) <-chan struct{} {
	t.Helper()

	requestedCh := make(chan struct{})
	var requestedChClosed atomic.Bool
	testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/executor/mvTaskCancelWatcherRequested", func(watchName string) {
		if strings.HasPrefix(watchName, watchNamePrefix) && requestedChClosed.CompareAndSwap(false, true) {
			close(requestedCh)
		}
	})
	return requestedCh
}

func readAffectedRowsMetricValue(t *testing.T, label string) float64 {
	t.Helper()

	counter, err := metrics.AffectedRowsCounter.GetMetricWithLabelValues(label)
	require.NoError(t, err)
	pb := &dto.Metric{}
	require.NoError(t, counter.Write(pb))
	return pb.GetCounter().GetValue()
}

func differentIsolationReadEnginesForTest(current string) string {
	for _, candidate := range []string{
		"tikv",
		"tiflash",
		"tidb",
		"tikv,tidb",
		"tikv,tiflash",
		"tikv,tiflash,tidb",
	} {
		if candidate != current {
			return candidate
		}
	}
	return "tikv"
}
