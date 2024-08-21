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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/pingcap/tidb/pkg/util/cpuprofile/testutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	topsqlstate "github.com/pingcap/tidb/pkg/util/topsql/state"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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
	testutil.MockCPULoad(ctx, "sql_global")
	time.Sleep(interval * 2)
}

type mockUpdater struct {
}

// UpdateProcessCPUTime updates specific process's tidb CPU time when the process is still running
// It implements ProcessCPUTimeUpdater interface
func (s *mockUpdater) UpdateProcessCPUTime(connID uint64, sqlID uint64, cpuTime time.Duration) {
	logutil.BgLogger().Info("", zap.Uint64("", connID), zap.Uint64("", sqlID), zap.Float64("", cpuTime.Seconds()))
}
