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

type deadlockHeader struct {
	RecordType string    `json:"record_type"`
	ObservedAt time.Time `json:"observed_at"`
	TxnID      string    `json:"txn_id"`
	Retryable  bool      `json:"retryable"`
}

// The budgets include in-flight encoders, queued records and the batch being written.
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
	// Reserve room before inspecting the wait chain. Charge the actual line
	// size once encoding completes and release the reservation on every exit.
	reserveBytes := min(maxDeadlockBytes, l.options.maxBytes)
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		observe("deadlock", "closed")
		return
	}
	if l.pendingRecords >= l.options.maxRecords || reserveBytes > l.options.maxBytes-l.pendingBytes {
		l.mu.Unlock()
		observe("deadlock", "queue_full")
		return
	}
	l.pendingRecords++
	l.pendingBytes += reserveBytes
	l.mu.Unlock()

	line, truncated := encodeDeadlock(txnID, event, time.Now(), maxDeadlockEdges, maxDeadlockBytes)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pendingRecords--
	l.pendingBytes -= reserveBytes
	switch {
	case l.closed:
		observe("deadlock", "closed")
		return
	case len(line) == 0:
		observe("deadlock", "oversized")
		return
	case len(line) > l.options.maxBytes-l.pendingBytes:
		observe("deadlock", "queue_full")
		return
	}
	select {
	case l.queue <- line:
		l.pendingRecords++
		l.pendingBytes += len(line)
		if truncated {
			observe("deadlock", "truncated")
		}
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
	const (
		originalPrefix  = `],"original_wait_chain_length":`
		outputPrefix    = `,"output_wait_chain_length":`
		truncatedPrefix = `,"truncated":`
	)
	originalCount := len(event.WaitChain)
	header, err := json.Marshal(deadlockHeader{
		RecordType: "deadlock", ObservedAt: observed.UTC(), TxnID: strconv.FormatUint(txnID, 10), Retryable: event.IsRetryable,
	})
	if err != nil {
		return nil, originalCount != 0
	}
	line := append(header[:len(header)-1], `,"wait_chain":[`...)
	var digits [20]byte
	originalLength := strconv.AppendInt(digits[:0], int64(originalCount), 10)
	footerBytes := len(originalPrefix) + len(originalLength) + len(outputPrefix) + len(truncatedPrefix) + len("}\n")
	count := 0
	for _, raw := range event.WaitChain[:min(originalCount, edgeLimit)] {
		// Guard allocation before hexadecimal expansion or protobuf decoding.
		// A resource tag beyond the line budget cannot supply a bounded digest.
		if raw == nil || len(raw.Key) > byteLimit/2 {
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
			const upperHex = "0123456789ABCDEF"
			var keyBuilder strings.Builder
			keyBuilder.Grow(2 * len(raw.Key))
			for _, value := range raw.Key {
				keyBuilder.WriteByte(upperHex[value>>4])
				keyBuilder.WriteByte(upperHex[value&0x0f])
			}
			key := keyBuilder.String()
			edge.Key = &key
		}
		if tablecodec.IsRecordKey(raw.Key) {
			edge.ResourceType = "record"
		} else if tablecodec.IsIndexKey(raw.Key) {
			edge.ResourceType = "index"
		}
		encoded, err := json.Marshal(edge)
		if err != nil {
			return nil, count < originalCount
		}
		var outputDigits [20]byte
		needed := len(line) + len(encoded) + footerBytes + len(strconv.AppendInt(outputDigits[:0], int64(count+1), 10)) + len("false")
		if count > 0 {
			needed++ // Comma between entries.
		}
		if count+1 < originalCount {
			needed-- // true is one byte shorter than false.
		}
		if needed > byteLimit {
			break
		}
		if count > 0 {
			line = append(line, ',')
		}
		line = append(line, encoded...)
		count++
	}
	truncated := count < originalCount
	line = append(line, originalPrefix...)
	line = append(line, originalLength...)
	line = append(line, outputPrefix...)
	line = strconv.AppendInt(line, int64(count), 10)
	line = append(line, truncatedPrefix...)
	line = strconv.AppendBool(line, truncated)
	line = append(line, '}', '\n')
	if len(line) > byteLimit {
		return nil, truncated
	}
	return line, truncated
}
