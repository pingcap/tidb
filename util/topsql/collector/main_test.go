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

	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/cpuprofile/testutil"
	topsqlstate "github.com/pingcap/tidb/util/topsql/state"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	goleak.VerifyTestMain(m, opts...)
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

	// Test disable then re-enable.
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
}

func TestSQLStatsTune(t *testing.T) {
	s := &sqlStats{plans: map[string]int64{"plan-1": 80}, total: 100}
	s.tune()
	require.Equal(t, int64(100), s.total)
	require.Equal(t, int64(100), s.plans["plan-1"])

	s = &sqlStats{plans: map[string]int64{"plan-1": 30, "plan-2": 30}, total: 100}
	s.tune()
	require.Equal(t, int64(100), s.total)
	require.Equal(t, int64(30), s.plans["plan-1"])
	require.Equal(t, int64(30), s.plans["plan-2"])
	require.Equal(t, int64(40), s.plans[""])
}

type mockCollector struct {
	dataCh chan []SQLCPUTimeRecord
}

// Collect implements the Collector interface.
func (c *mockCollector) Collect(records []SQLCPUTimeRecord) {
	c.dataCh <- records
}
