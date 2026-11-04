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
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

type blockingDiagnosticWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func TestDiagnosticDeadlockQueueBoundsAndShutdown(t *testing.T) {
	for _, budget := range []string{"records", "bytes"} {
		t.Run(budget, func(t *testing.T) {
			w := &blockingDiagnosticWriter{entered: make(chan struct{}), release: make(chan struct{})}
			opts := defaultDeadlockLoggerOptions()
			opts.batchBytes = 1 // force the first event into the blocked write
			opts.closeTimeout = 20 * time.Millisecond
			if budget == "records" {
				opts.maxRecords = 1
			} else {
				opts.maxBytes = 700 // one fixture fits, two do not
			}
			l := newDeadlockLoggerWithOptions(w, opts)
			t.Cleanup(func() { close(w.release); require.NoError(t, l.close()) })
			event := deadlockFixture(t)
			l.record(1, event)
			select {
			case <-w.entered:
			case <-time.After(time.Second):
				t.Fatal("writer not entered")
			}
			before := testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "queue_full"))
			l.record(2, event)
			require.Equal(t, before+1, testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "queue_full")))
			require.ErrorContains(t, l.close(), "timed out")
			before = testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "closed"))
			l.record(3, event)
			require.Equal(t, before+1, testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "closed")))
		})
	}
}

func TestDiagnosticDeadlockOwnsQueuedDataAndDrains(t *testing.T) {
	w := &testWriteCloser{}
	opts := defaultDeadlockLoggerOptions()
	opts.flushInterval = time.Hour
	l := newDeadlockLoggerWithOptions(w, opts)
	event := deadlockFixture(t)
	l.record(1, event)
	event.WaitChain[0].Key[0] = 0
	event.WaitChain[0].Txn = 0
	require.NoError(t, l.close())
	var record deadlockRecord
	require.NoError(t, json.Unmarshal(bytes.TrimSpace([]byte(w.String())), &record))
	require.Equal(t, "ABCD", *record.WaitChain[0].Key)
	require.Equal(t, "18446744073709551615", record.WaitChain[0].TxnID)
	require.NoError(t, l.close())
}

type shortDiagnosticWriter struct{}

func (shortDiagnosticWriter) Write(b []byte) (int, error) { return len(b) - 1, nil }
func (shortDiagnosticWriter) Close() error                { return nil }

type recoveringDiagnosticWriter struct {
	bytes.Buffer
	failed bool
}

func (w *recoveringDiagnosticWriter) Write(b []byte) (int, error) {
	if !w.failed {
		w.failed = true
		return w.Buffer.Write(b[:len(b)-4])
	}
	return w.Buffer.Write(b)
}

func (*recoveringDiagnosticWriter) Close() error { return nil }

func TestDiagnosticDeadlockShortWriteAndConcurrentClose(t *testing.T) {
	before := testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "write_failed"))
	l := newDeadlockLogger(shortDiagnosticWriter{})
	l.record(1, deadlockFixture(t))
	require.NoError(t, l.close())
	require.Equal(t, before+1, testutil.ToFloat64(metrics.DiagnosticLogCounter.WithLabelValues("deadlock", "write_failed")))

	t.Run("partial write does not corrupt the next event", func(t *testing.T) {
		w := &recoveringDiagnosticWriter{}
		opts := defaultDeadlockLoggerOptions()
		opts.batchBytes = 1
		l := newDeadlockLoggerWithOptions(w, opts)
		l.record(1, deadlockFixture(t))
		l.record(2, deadlockFixture(t))
		require.NoError(t, l.close())
		lines := bytes.Split(bytes.TrimSpace(w.Bytes()), []byte{'\n'})
		var record deadlockRecord
		require.NoError(t, json.Unmarshal(lines[len(lines)-1], &record))
		require.Equal(t, "2", record.TxnID)
	})

	l = newDeadlockLogger(&testWriteCloser{})
	event := deadlockFixture(t)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				l.record(uint64(i), event)
			}
		}()
	}
	require.NoError(t, l.close())
	wg.Wait()
	require.NoError(t, l.close())
}

func (w *blockingDiagnosticWriter) Write(b []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(b), nil
}

func (*blockingDiagnosticWriter) Close() error { return nil }

func TestDiagnosticDeadlockSlowDiskDoesNotBlockCallback(t *testing.T) {
	w := &blockingDiagnosticWriter{entered: make(chan struct{}), release: make(chan struct{})}
	l := newDeadlockLogger(w)
	t.Cleanup(func() { close(w.release); require.NoError(t, l.close()) })
	event := deadlockFixture(t)
	returned := make(chan struct{})
	go func() { l.record(1, event); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("deadlock callback waited for the blocked file writer")
	}
	select {
	case <-w.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("low-volume event was not flushed by the background worker")
	}
}
