// Copyright 2023 PingCAP, Inc.
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

package stmtsummary

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func readGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, gauge.Write(m))
	return m.GetGauge().GetValue()
}

func TestStmtWindow(t *testing.T) {
	ss := NewStmtSummary4Test(5)
	defer ss.Close()
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	require.Equal(t, 5, ss.window.lru.Size())
	require.Equal(t, 2, ss.window.evicted.count())
	require.Equal(t, int64(4), ss.window.evicted.other.ExecCount) // digest1 digest1 digest2 digest2
	require.Equal(t, int64(2), ss.window.evictedCount.Load())
	_, err := json.Marshal(ss.window.evicted.other)
	require.NoError(t, err)
	ss.Clear()
	require.Equal(t, 0, ss.window.lru.Size())
	require.Equal(t, 0, ss.window.evicted.count())
	require.Equal(t, int64(0), ss.window.evicted.other.ExecCount)
	require.Equal(t, int64(0), ss.window.evictedCount.Load())
}

func TestStmtSummary(t *testing.T) {
	ss := NewStmtSummary4Test(3)
	defer ss.Close()

	w := ss.window
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))
	ss.Add(GenerateStmtExecInfo4Test("digest4"))
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	require.Equal(t, 3, w.lru.Size())
	require.Equal(t, 2, w.evicted.count())

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7"))
	w = ss.window
	require.Equal(t, 2, w.lru.Size())
	require.Equal(t, 0, w.evicted.count())

	ss.Clear()
	require.Equal(t, 0, w.lru.Size())
}

func TestStmtSummaryGroupByUser(t *testing.T) {
	ss := NewStmtSummary4Test(100)
	defer ss.Close()

	// Two statements, same digest, different users: without the flag they
	// should merge into one record.
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	ss.Add(stmtExecInfoWithUser("digest1", "bob"))
	require.Equal(t, 1, ss.window.lru.Size())

	// Switching the flag on clears the window. Re-emitting produces two rows.
	require.NoError(t, ss.SetGroupByUser(true))
	require.Equal(t, 0, ss.window.lru.Size())
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	ss.Add(stmtExecInfoWithUser("digest1", "bob"))
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	require.Equal(t, 2, ss.window.lru.Size())

	// When grouping by user, each record's AuthUsers must hold exactly one
	// user — the one that groups it — so SAMPLE_USER naturally reflects the
	// grouping dimension without a dedicated column.
	users := map[string]int64{}
	for _, v := range ss.window.lru.Values() {
		r := v.(*lockedStmtRecord)
		require.Len(t, r.AuthUsers, 1)
		for u := range r.AuthUsers {
			users[u] = r.ExecCount
		}
	}
	require.Equal(t, int64(2), users["alice"])
	require.Equal(t, int64(1), users["bob"])

	// Turning the flag off again clears and reverts to single-record merging.
	require.NoError(t, ss.SetGroupByUser(false))
	ss.Add(stmtExecInfoWithUser("digest1", "alice"))
	ss.Add(stmtExecInfoWithUser("digest1", "bob"))
	require.Equal(t, 1, ss.window.lru.Size())
	for _, v := range ss.window.lru.Values() {
		r := v.(*lockedStmtRecord)
		require.Len(t, r.AuthUsers, 2) // both users merged when grouping is off
	}
}

// stmtExecInfoWithUser returns a StmtExecInfo whose digest and User fields are
// set; everything else is the generic test fixture.
func stmtExecInfoWithUser(digest, user string) *stmtsummary.StmtExecInfo {
	info := GenerateStmtExecInfo4Test(digest)
	info.User = user
	return info
}

func TestWindowEvictedCountResetOnRotate(t *testing.T) {
	ss := NewStmtSummary4Test(2)
	defer ss.Close()
	require.NoError(t, ss.SetMaxStmtCount(2))
	metrics.SetStmtSummaryWindowMetrics(metrics.StmtSummaryTypeV2, 0, 0)
	t.Cleanup(func() {
		metrics.SetStmtSummaryWindowMetrics(metrics.StmtSummaryTypeV2, 0, 0)
	})

	// Fill the LRU cache and trigger evictions.
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3")) // evicts digest1
	ss.Add(GenerateStmtExecInfo4Test("digest4")) // evicts digest2
	require.Equal(t, 2, ss.window.lru.Size())
	require.Equal(t, int64(2), ss.window.evictedCount.Load())
	ss.windowLock.Lock()
	ss.updateMetrics()
	ss.windowLock.Unlock()
	require.Equal(t, 2.0, readGaugeValue(t, metrics.StmtSummaryWindowRecordCount.WithLabelValues(metrics.StmtSummaryTypeV2)))
	require.Equal(t, 2.0, readGaugeValue(t, metrics.StmtSummaryWindowEvictedCount.WithLabelValues(metrics.StmtSummaryTypeV2)))

	// Rotate creates a new window with a fresh counter.
	ss.rotate(timeNow())
	require.Equal(t, int64(0), ss.window.evictedCount.Load())
	ss.windowLock.Lock()
	ss.updateMetrics()
	ss.windowLock.Unlock()
	require.Equal(t, 0.0, readGaugeValue(t, metrics.StmtSummaryWindowEvictedCount.WithLabelValues(metrics.StmtSummaryTypeV2)))

	// Add more records in the new window.
	ss.Add(GenerateStmtExecInfo4Test("digest5"))
	ss.Add(GenerateStmtExecInfo4Test("digest6"))
	ss.Add(GenerateStmtExecInfo4Test("digest7")) // evicts digest5
	require.Equal(t, int64(1), ss.window.evictedCount.Load())
	require.Equal(t, 2, ss.window.lru.Size())
	ss.windowLock.Lock()
	ss.updateMetrics()
	ss.windowLock.Unlock()
	require.Equal(t, 2.0, readGaugeValue(t, metrics.StmtSummaryWindowRecordCount.WithLabelValues(metrics.StmtSummaryTypeV2)))
	require.Equal(t, 1.0, readGaugeValue(t, metrics.StmtSummaryWindowEvictedCount.WithLabelValues(metrics.StmtSummaryTypeV2)))
}

func TestStmtSummaryFlush(t *testing.T) {
	storage := &mockStmtStorage{}
	ss := NewStmtSummary4Test(1000)
	ss.storage = storage

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.rotate(timeNow())

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3"))

	ss.Close()

	storage.Lock()
	require.Equal(t, 3, len(storage.windows))
	storage.Unlock()
}

func TestDefaultConfig(t *testing.T) {
	cfg := &Config{
		Filename: filepath.Join(t.TempDir(), "test.log"),
	}
	ss, err := NewStmtSummary(cfg)
	require.NoError(t, err)
	defer ss.Close()

	// Verify RefreshInterval (should be 1800 = 30 min)
	require.Equal(t, uint32(1800), ss.RefreshInterval())
}
