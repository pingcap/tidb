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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/resourcegrouptag"
	tikverr "github.com/tikv/client-go/v2/error"
	"golang.org/x/time/rate"
)

// These limits are implementation candidates pending workload validation.
const (
	maxDeadlockEdges = 64
	maxDeadlockBytes = 64 * 1024
)

type deadlockEdge struct {
	TxnID        string  `json:"txn_id"`
	WaitingFor   string  `json:"waiting_for"`
	SQLDigest    *string `json:"sql_digest"`
	Key          *string `json:"key"`
	ResourceType string  `json:"resource_type"`
}

type deadlockRecord struct {
	RecordType              string         `json:"record_type"`
	ObservedAt              time.Time      `json:"observed_at"`
	TxnID                   string         `json:"txn_id"`
	Retryable               bool           `json:"retryable"`
	WaitChain               []deadlockEdge `json:"wait_chain"`
	OriginalWaitChainLength int            `json:"original_wait_chain_length"`
	OutputWaitChainLength   int            `json:"output_wait_chain_length"`
	Truncated               bool           `json:"truncated"`
}

// The budgets include queued records and the batch currently being written.
// They are deliberately finite even if the file writer stalls indefinitely.
type deadlockLoggerOptions struct {
	maxRecords    int
	maxBytes      int
	batchBytes    int
	flushInterval time.Duration
	closeTimeout  time.Duration
}

func defaultDeadlockLoggerOptions() deadlockLoggerOptions {
	return deadlockLoggerOptions{
		maxRecords: 1024, maxBytes: 8 * 1024 * 1024, batchBytes: 64 * 1024,
		flushInterval: 100 * time.Millisecond, closeTimeout: 5 * time.Second,
	}
}

type deadlockLogger struct {
	// mu protects admission and accounting only; no file I/O holds this lock.
	mu             sync.Mutex
	writer         io.WriteCloser
	limiters       [2]*rate.Limiter
	options        deadlockLoggerOptions
	queue          chan []byte
	done           chan struct{}
	closed         bool
	pendingRecords int
	pendingBytes   int
	closeErr       error // published by closing done
}

func newDeadlockLogger(writer io.WriteCloser) *deadlockLogger {
	return newDeadlockLoggerWithOptions(writer, defaultDeadlockLoggerOptions())
}

func newDeadlockLoggerWithOptions(writer io.WriteCloser, options deadlockLoggerOptions) *deadlockLogger {
	l := &deadlockLogger{
		writer: writer, options: options, queue: make(chan []byte, options.maxRecords), done: make(chan struct{}),
		limiters: [2]*rate.Limiter{rate.NewLimiter(100, 100), rate.NewLimiter(100, 100)},
	}
	go l.run()
	return l
}

func (l *deadlockLogger) record(txnID uint64, event *tikverr.ErrDeadlock) {
	if event == nil || event.Deadlock == nil {
		return
	}
	class := 0
	if event.IsRetryable {
		class = 1
	}
	if !l.limiters[class].Allow() {
		observe("deadlock", "rate_limited")
		return
	}
	line, truncated := encodeDeadlock(txnID, event, time.Now(), maxDeadlockEdges, maxDeadlockBytes)
	if len(line) == 0 {
		observe("deadlock", "oversized")
		return
	}
	if truncated {
		observe("deadlock", "truncated")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		observe("deadlock", "closed")
		return
	}
	if l.pendingRecords >= l.options.maxRecords || len(line) > l.options.maxBytes-l.pendingBytes {
		observe("deadlock", "queue_full")
		return
	}
	select {
	case l.queue <- line:
		l.pendingRecords++
		l.pendingBytes += len(line)
	default:
		observe("deadlock", "queue_full")
	}
}

// run is the sole owner of file writes, rotation and close. A full or slow file
// never creates extra workers and never causes admission to wait for disk.
func (l *deadlockLogger) run() {
	ticker := time.NewTicker(l.options.flushInterval)
	defer ticker.Stop()
	defer close(l.done)
	batch := make([]byte, 0, l.options.batchBytes)
	records := 0
	needsSeparator := false
	flush := func() {
		if records == 0 {
			return
		}
		started := time.Now()
		payload := batch
		if needsSeparator {
			// Terminate a partially written line before the next batch. The failed
			// batch is not retried, but it must not corrupt a later valid event.
			payload = append([]byte{'\n'}, batch...)
		}
		n, err := l.writer.Write(payload)
		if n > 0 && n <= len(payload) {
			needsSeparator = payload[n-1] != '\n'
		}
		metrics.DiagnosticLogWriteDuration.WithLabelValues("deadlock").Observe(time.Since(started).Seconds())
		result := "written"
		if err != nil || n != len(payload) {
			result = "write_failed"
		}
		metrics.DiagnosticLogCounter.WithLabelValues("deadlock", result).Add(float64(records))
		l.mu.Lock()
		l.pendingRecords -= records
		l.pendingBytes -= len(batch)
		l.mu.Unlock()
		batch = batch[:0]
		records = 0
	}
	for {
		select {
		case line, ok := <-l.queue:
			if !ok {
				flush()
				l.closeErr = l.writer.Close()
				return
			}
			if len(batch)+len(line) > l.options.batchBytes {
				flush()
			}
			batch = append(batch, line...)
			records++
			if len(batch) >= l.options.batchBytes {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// close first stops admission, then allows a bounded shutdown drain. File writes
// cannot be cancelled: on timeout the one worker remains responsible for close
// if the disk recovers; callers never spawn a replacement writer or wait forever.
func (l *deadlockLogger) close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.queue)
	}
	l.mu.Unlock()
	timer := time.NewTimer(l.options.closeTimeout)
	defer timer.Stop()
	select {
	case <-l.done:
		return l.closeErr
	case <-timer.C:
		observe("deadlock", "close_timeout")
		return fmt.Errorf("diagnostic deadlock logger shutdown timed out after %s", l.options.closeTimeout)
	}
}

func encodeDeadlock(txnID uint64, event *tikverr.ErrDeadlock, observed time.Time, edgeLimit, byteLimit int) ([]byte, bool) {
	record := deadlockRecord{RecordType: "deadlock", ObservedAt: observed.UTC(), TxnID: strconv.FormatUint(txnID, 10), Retryable: event.IsRetryable,
		WaitChain: make([]deadlockEdge, 0, min(len(event.WaitChain), edgeLimit)), OriginalWaitChainLength: len(event.WaitChain), Truncated: len(event.WaitChain) > edgeLimit}
	for _, raw := range event.WaitChain[:min(len(event.WaitChain), edgeLimit)] {
		// Guard allocation before hexadecimal expansion or protobuf decoding.
		// A resource tag beyond the line budget cannot supply a bounded digest.
		if raw == nil || len(raw.Key) > byteLimit/2 {
			record.Truncated = true
			break
		}
		edge := deadlockEdge{TxnID: strconv.FormatUint(raw.Txn, 10), WaitingFor: strconv.FormatUint(raw.WaitForTxn, 10), ResourceType: "unknown"}
		if len(raw.ResourceGroupTag) <= byteLimit {
			if digest, err := resourcegrouptag.DecodeResourceGroupTag(raw.ResourceGroupTag); err == nil && len(digest) == 32 {
				s := hex.EncodeToString(digest)
				edge.SQLDigest = &s
			}
		}
		if len(raw.Key) > 0 {
			key := strings.ToUpper(hex.EncodeToString(raw.Key))
			edge.Key = &key
		}
		if tablecodec.IsRecordKey(raw.Key) {
			edge.ResourceType = "record"
		} else if tablecodec.IsIndexKey(raw.Key) {
			edge.ResourceType = "index"
		}
		record.WaitChain = append(record.WaitChain, edge)
		record.OutputWaitChainLength = len(record.WaitChain)
		record.Truncated = len(record.WaitChain) < record.OriginalWaitChainLength
		encoded, err := json.Marshal(record)
		if err != nil {
			return nil, record.Truncated
		}
		if len(encoded)+1 > byteLimit {
			record.WaitChain = record.WaitChain[:len(record.WaitChain)-1]
			record.Truncated = true
			break
		}
	}
	record.OutputWaitChainLength = len(record.WaitChain)
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, record.Truncated
	}
	if len(encoded)+1 > byteLimit {
		return nil, record.Truncated
	}
	return append(encoded, '\n'), record.Truncated
}
