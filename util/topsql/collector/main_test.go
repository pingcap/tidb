// Copyright 2021 PingCAP, Inc.
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

package collector

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/cpuprofile/testutil"
	"github.com/pingcap/tidb/util/testbridge"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.SetupForCommonTest()
	goleak.VerifyTestMain(m)
}

func TestPProfCPUProfile(t *testing.T) {
	// short the interval to speed up the test.
	interval := time.Millisecond * 400
	defCollectTickerInterval = interval
	cpuprofile.DefProfileDuration = interval

	err := cpuprofile.StartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.StopCPUProfiler()

	topsqlstate.EnableTopSQL()
	mc := &mockCollector{
		dataCh: make(chan []SQLCPUTimeRecord, 10),
	}
	sqlCPUCollector := NewSQLCPUCollector(mc)
	sqlCPUCollector.Start()
	defer sqlCPUCollector.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testutil.MockCPULoad(ctx, "sql", "sql_digest", "plan_digest")

	data := <-mc.dataCh
	require.True(t, len(data) > 0)
	require.Equal(t, []byte("sql_digest value"), data[0].SQLDigest)

	// Test after disabled, shouldn't receive any data.
	topsqlstate.DisableTopSQL()
	time.Sleep(interval * 2)
	require.Equal(t, 0, len(mc.dataCh))

	// Test after re-enable.
	topsqlstate.EnableTopSQL()
	for i := 0; i < 10; i++ {
		t1 := time.Now()
		data = <-mc.dataCh
		require.True(t, time.Since(t1) < interval*4)
		if len(data) > 0 {
			break
		}
	}
	require.True(t, len(data) > 0)
	require.Equal(t, []byte("sql_digest value"), data[0].SQLDigest)
}

type mockCollector struct {
	dataCh chan []SQLCPUTimeRecord
}

// Collect implements the Collector interface.
func (c *mockCollector) Collect(records []SQLCPUTimeRecord) {
	c.dataCh <- records
}
