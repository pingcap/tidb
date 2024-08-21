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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/pingcap/tidb/pkg/util/cpuprofile/testutil"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/require"
)

func TestPProfCPUProfile(t *testing.T) {
	// short the interval to speed up the test.
	interval := time.Millisecond * 200
	defCollectTickerInterval = interval
	cpuprofile.DefProfileDuration = interval

	err := cpuprofile.StartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.StopCPUProfiler()

	topsqlstate.EnableTopSQL()
	updater := &mockUpdater{}
	updater.data = make(map[uint64]uint64)
	sqlCPUCollector := NewProcessCPUProfiler(updater)
	sqlCPUCollector.Start()
	defer sqlCPUCollector.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testutil.MockCPULoadV2(ctx, "0_0", "1_0", "2_1", "3_1")
	time.Sleep(interval * 4)
	updater.Lock()
	currDataLen := len(updater.data)
	updater.Unlock()
	require.Equal(t, 4, currDataLen)

	// Test disable then re-enable.
	topsqlstate.DisableTopSQL()
	time.Sleep(interval * 4)
	updater.Lock()
	updater.data = make(map[uint64]uint64)
	updater.Unlock()

	time.Sleep(interval * 4)
	updater.Lock()
	currDataLen = len(updater.data)
	updater.Unlock()
	require.Equal(t, 0, currDataLen)

	topsqlstate.EnableTopSQL()
	time.Sleep(interval * 4)
	updater.Lock()
	currDataLen = len(updater.data)
	updater.Unlock()
	require.Equal(t, 4, currDataLen)
}

type mockUpdater struct {
	sync.Mutex
	data map[uint64]uint64
}

// UpdateProcessCPUTime updates specific process's tidb CPU time when the process is still running
// It implements ProcessCPUTimeUpdater interface
func (s *mockUpdater) UpdateProcessCPUTime(connID uint64, sqlID uint64, _ time.Duration) {
	s.Lock()
	defer s.Unlock()
	s.data[connID] = sqlID
}
