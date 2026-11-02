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

package diagnosticlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	tikverr "github.com/tikv/client-go/v2/error"
	"golang.org/x/time/rate"
)

type testWriteCloser struct {
	mu sync.Mutex
	bytes.Buffer
	fail bool
}

func (w *testWriteCloser) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail {
		return 0, errors.New("disk full")
	}
	return w.Buffer.Write(b)
}
func (w *testWriteCloser) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Buffer.String()
}
func (*testWriteCloser) Close() error { return nil }

func deadlockFixture(t *testing.T) *tikverr.ErrDeadlock {
	tag, err := (&tipb.ResourceGroupTag{SqlDigest: bytes.Repeat([]byte{0xab}, 32)}).Marshal()
	require.NoError(t, err)
	return &tikverr.ErrDeadlock{Deadlock: &kvrpcpb.Deadlock{WaitChain: []*deadlock.WaitForEntry{
		{Txn: math.MaxUint64, WaitForTxn: 9007199254740993, Key: []byte{0xab, 0xcd}, ResourceGroupTag: tag},
		{Txn: 9007199254740993, WaitForTxn: math.MaxUint64},
	}}}
}

func TestDiagnosticDeadlockEncodingAndLimits(t *testing.T) {
	event := deadlockFixture(t)
	at := time.Date(2026, 10, 23, 2, 0, 0, 0, time.UTC)
	data, truncated := encodeDeadlock(math.MaxUint64, event, at, 64, 65536)
	require.False(t, truncated)
	var record deadlockRecord
	require.NoError(t, json.Unmarshal(data, &record))
	require.Equal(t, "18446744073709551615", record.TxnID)
	require.Equal(t, "9007199254740993", record.WaitChain[0].WaitingFor)
	require.Equal(t, "ABCD", *record.WaitChain[0].Key)
	require.Equal(t, strings.Repeat("ab", 32), *record.WaitChain[0].SQLDigest)
	require.Nil(t, record.WaitChain[1].Key)
	require.Nil(t, record.WaitChain[1].SQLDigest)
	one, truncated := encodeDeadlock(1, event, at, 1, 65536)
	require.True(t, truncated)
	require.NoError(t, json.Unmarshal(one, &record))
	require.Equal(t, 1, record.OutputWaitChainLength)
	require.Equal(t, 2, record.OriginalWaitChainLength)
	bounded, truncated := encodeDeadlock(1, event, at, 64, len(one))
	require.True(t, truncated)
	require.LessOrEqual(t, len(bounded), len(one))
	require.NoError(t, json.Unmarshal(bounded, &record))
	require.Equal(t, "ABCD", *record.WaitChain[0].Key)
	event.WaitChain[0].Key = make([]byte, 65536)
	bounded, truncated = encodeDeadlock(1, event, at, 64, 65536)
	require.True(t, truncated)
	require.NoError(t, json.Unmarshal(bounded, &record))
	require.Empty(t, record.WaitChain)
	if root := os.Getenv("DIAGNOSTIC_FIXTURE_DIR"); root != "" {
		require.NoError(t, os.MkdirAll(root, 0o750))
		require.NoError(t, os.WriteFile(filepath.Join(root, "tidb-deadlock.log"), data, 0o640))
	}
}

func TestDiagnosticDeadlockIndependentBudgetsAndFailure(t *testing.T) {
	writer := &testWriteCloser{}
	logger := newDeadlockLogger(writer)
	t.Cleanup(func() { require.NoError(t, logger.close()) })
	logger.limiters[0] = rate.NewLimiter(0, 0)
	event := deadlockFixture(t)
	logger.record(1, event)
	require.Empty(t, writer.String())
	event.IsRetryable = true
	logger.record(1, event)
	require.Eventually(t, func() bool { return writer.String() != "" }, 3*time.Second, time.Millisecond)
	before := testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "write_failed"))
	writer.mu.Lock()
	writer.fail = true
	writer.mu.Unlock()
	logger.record(1, event)
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "write_failed")) == before+1
	}, 3*time.Second, time.Millisecond)
}

func TestDiagnosticInitializeDisabledAndIndependent(t *testing.T) {
	defer func() { require.NoError(t, Close()) }()
	root := filepath.Join(t.TempDir(), "absent")
	require.NoError(t, Initialize(nil, root))
	RecordDeadlock(1, deadlockFixture(t))
	require.False(t, IndexUsageEnabled())
	_, err := os.Stat(root)
	require.True(t, os.IsNotExist(err))
	require.NoError(t, Initialize([]string{"index_usage"}, root))
	require.True(t, IndexUsageEnabled())
	RecordDeadlock(1, deadlockFixture(t))
	_, err = os.Stat(filepath.Join(root, "tidb-deadlock.log"))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, Initialize([]string{"deadlock"}, root))
	require.False(t, IndexUsageEnabled())
	RecordDeadlock(1, deadlockFixture(t))
	require.NoError(t, Close())
	content, err := os.ReadFile(filepath.Join(root, "tidb-deadlock.log"))
	require.NoError(t, err)
	require.True(t, json.Valid(bytes.TrimSpace(content)))
}

func TestDiagnosticPublisherAtomicRetentionAndFailure(t *testing.T) {
	directory := t.TempDir()
	publisher := NewPublisher(directory)
	at := time.Date(2026, 10, 23, 2, 0, 3, 123456789, time.FixedZone("test", 3600))
	write := func(w io.Writer, at time.Time) error {
		return json.NewEncoder(w).Encode(struct {
			RecordType   string    `json:"record_type"`
			SnapshotTime time.Time `json:"snapshot_time"`
			RowCount     int       `json:"row_count"`
		}{"footer", at, 0})
	}
	first, err := publisher.Publish(at, write)
	require.NoError(t, err)
	require.Regexp(t, `index-usage-20261023T010003\.123456Z-[0-9a-f]{32}\.jsonl$`, first)
	firstData, err := os.ReadFile(first)
	require.NoError(t, err)
	require.Contains(t, string(firstData), `"snapshot_time":"2026-10-23T01:00:03.123456Z"`)
	_, err = publisher.Publish(at, func(w io.Writer, _ time.Time) error {
		_, writeErr := io.WriteString(w, "incomplete\n")
		require.NoError(t, writeErr)
		return errors.New("injected write failure")
	})
	require.ErrorContains(t, err, "injected write failure")
	after, err := os.ReadFile(first)
	require.NoError(t, err)
	require.Equal(t, firstData, after)
	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	second, err := publisher.Publish(at, func(w io.Writer, at time.Time) error {
		entries, err := os.ReadDir(directory)
		require.NoError(t, err)
		formal := 0
		for _, entry := range entries {
			if snapshotName.MatchString(entry.Name()) {
				formal++
			}
		}
		require.Equal(t, 1, formal, "the new file must remain invisible while writing")
		_, busyErr := publisher.Publish(at, write)
		require.ErrorContains(t, busyErr, "already running")
		return write(w, at)
	})
	require.NoError(t, err)
	require.NotEqual(t, first, second)
	_, err = os.Stat(first)
	require.True(t, os.IsNotExist(err))
	entries, err = os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, filepath.Base(second), entries[0].Name())
}
