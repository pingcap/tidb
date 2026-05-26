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
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

func TestStmtSummaryPersistEvicted(t *testing.T) {
	begin := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
	evictAt := begin.Add(42 * time.Second)
	now := begin
	oldTimeNow := timeNow
	timeNow = func() time.Time {
		return now
	}
	t.Cleanup(func() {
		timeNow = oldTimeNow
	})

	storage := &mockStmtStorage{}
	ss := NewStmtSummary4Test(2)
	ss.storage = storage
	defer ss.Close()
	require.NoError(t, ss.SetPersistEvicted(true))

	// With capacity 2, the 3rd and later distinct digests evict older entries
	// and should each land in storage.evicted.
	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	now = evictAt
	ss.Add(GenerateStmtExecInfo4Test("digest3")) // evicts digest1
	ss.Add(GenerateStmtExecInfo4Test("digest4")) // evicts digest2

	// The log is async; wait briefly for drain.
	require.Eventually(t, func() bool {
		storage.Lock()
		defer storage.Unlock()
		return len(storage.evicted) == 2
	}, time.Second, 10*time.Millisecond, "expected 2 evicted records to be logged")

	storage.Lock()
	digests := []string{storage.evicted[0].Digest, storage.evicted[1].Digest}
	for _, record := range storage.evicted {
		require.Equal(t, begin.Unix(), record.Begin)
		require.Equal(t, evictAt.Unix(), record.End)
	}
	storage.Unlock()
	require.ElementsMatch(t, []string{"digest1", "digest2"}, digests)

	// Disable and verify no further log writes.
	require.NoError(t, ss.SetPersistEvicted(false))
	ss.Add(GenerateStmtExecInfo4Test("digest5")) // evicts digest3
	require.Never(t, func() bool {
		storage.Lock()
		defer storage.Unlock()
		return len(storage.evicted) != 2
	}, 100*time.Millisecond, 10*time.Millisecond, "evicted count should remain 2 after disabling")
}

func TestStmtSummaryPersistEvictedDoesNotPersistLoggedRecordsAsAggregate(t *testing.T) {
	var logBuf bytes.Buffer
	storage := &stmtLogStorage{
		logger: zap.New(zapcore.NewCore(&stmtLogEncoder{}, zapcore.AddSync(&logBuf), zapcore.InfoLevel)),
	}

	ss := NewStmtSummary4Test(2)
	ss.storage = storage
	require.NoError(t, ss.SetPersistEvicted(true))

	ss.Add(GenerateStmtExecInfo4Test("digest1"))
	ss.Add(GenerateStmtExecInfo4Test("digest2"))
	ss.Add(GenerateStmtExecInfo4Test("digest3")) // evicts digest1
	ss.Add(GenerateStmtExecInfo4Test("digest4")) // evicts digest2
	ss.Close()

	type loggedRecord struct {
		Digest    string `json:"digest"`
		ExecCount int64  `json:"exec_count"`
		Evicted   bool   `json:"evicted"`
	}

	var totalExecCount int64
	evictedDigests := make([]string, 0, 2)
	for _, line := range strings.Split(strings.TrimSpace(logBuf.String()), "\n") {
		var record loggedRecord
		require.NoError(t, json.Unmarshal([]byte(line), &record))
		totalExecCount += record.ExecCount
		if record.Evicted {
			evictedDigests = append(evictedDigests, record.Digest)
			continue
		}
		require.NotEmpty(t, record.Digest, "logged evicted records should not also be persisted as the aggregate row")
	}

	require.ElementsMatch(t, []string{"digest1", "digest2"}, evictedDigests)
	require.Equal(t, int64(4), totalExecCount)
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
