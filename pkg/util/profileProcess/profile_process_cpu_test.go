// Copyright 2024 PingCAP, Inc.
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

package profileprocess

import (
	"context"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/pingcap/tidb/pkg/util/cpuprofile/testutil"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/require"
)

func TestPProfCPUProfile(t *testing.T) {
	// short the interval to speed up the test.
	interval := time.Millisecond * 400
	defCollectTickerInterval = interval
	cpuprofile.DefProfileDuration = interval

	err := cpuprofile.StartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.StopCPUProfiler()

	topsqlstate.EnableTopSQL()
	mu := &mockUpdater{}
	sqlCPUCollector := NewProcessCPUProfiler(mu)
	sqlCPUCollector.Start()
	defer sqlCPUCollector.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testutil.MockCPULoad(ctx, "sql_global_uid")
	time.Sleep(interval * 2)
	/*// Test disable then re-enable.
	topsqlstate.DisableTopSQL()
	time.Sleep(interval * 2)
	dataChLen := len(mc.dataCh)
	deltaLen := 0
	topsqlstate.EnableTopSQL()
	for i := 0; i < 10; i++ {
		t1 := time.Now()
		data = <-mc.dataCh
		require.True(t, time.Since(t1) < interval*4)
		if len(data) > 0 {
			deltaLen++
			if deltaLen > dataChLen {
				// Here we can ensure that we receive new data after "re-enable".
				break
			}
		}
	}
	require.True(t, len(data) > 0)
	require.True(t, deltaLen > dataChLen)
	require.Equal(t, []byte("sql_digest value"), data[0].SQLDigest)
	*/
}

type mockUpdater struct {
}

// UpdateProcessCPUTime updates specific process's tidb CPU time when the process is still running
// It implements ProcessCPUTimeUpdater interface
func (s *mockUpdater) UpdateProcessCPUTime(connID uint64, sqlID uint64, cpuTime time.Duration) {
	logutil.BgLogger().Info("", zap.Uint64("", connID), zap.Uint64("", sqlID), zap.Float64("", cpuTime.Seconds()))
}
