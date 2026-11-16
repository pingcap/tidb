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

// Package diagnosticlog owns optional, best-effort diagnostic JSONL files.
package diagnosticlog

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/pkg/metrics"
	tikverr "github.com/tikv/client-go/v2/error"
	"gopkg.in/natefinch/lumberjack.v2"
)

type runtime struct {
	deadlock   *deadlockLogger
	indexUsage *Publisher
}

var global atomic.Pointer[runtime]

func observe(kind, result string) {
	metrics.DiagnosticLogCounter.WithLabelValues(kind, result).Inc()
}

// Initialize installs the startup-only selection. The directory must be bound by
// deployment to the original node identity for the entire lifetime of its files.
func Initialize(types []string, directory string) error {
	var deadlock, indexUsage bool
	for _, kind := range types {
		switch kind {
		case "deadlock":
			deadlock = true
		case "index_usage":
			indexUsage = true
		default:
			return fmt.Errorf("unsupported diagnostic log type %q", kind)
		}
	}
	next := &runtime{}
	if deadlock || indexUsage {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			return err
		}
	}
	if deadlock {
		// Conservative initial rotation limits for the opt-in diagnostic stream.
		writer := &deadlockFileWriter{Logger: &lumberjack.Logger{
			Filename: filepath.Join(directory, "tidb-deadlock.log"), MaxSize: 100, MaxBackups: 3, MaxAge: 7,
		}}
		next.deadlock = newDeadlockLogger(writer)
	}
	if indexUsage {
		next.indexUsage = NewPublisher(filepath.Join(directory, "index-usage"))
	}
	old := global.Swap(next)
	if old != nil && old.deadlock != nil {
		return old.deadlock.close()
	}
	return nil
}

// Close closes the event writer after SQL processing and domain workers stop.
func Close() error {
	previous := global.Swap(nil)
	if previous != nil && previous.deadlock != nil {
		return previous.deadlock.close()
	}
	return nil
}

// DeadlockEnabled lets callers use the existing history callback when logging is off.
func DeadlockEnabled() bool {
	current := global.Load()
	return current != nil && current.deadlock != nil
}

// RecordDeadlock is independent of the in-memory deadlock history's filters.
func RecordDeadlock(txnID uint64, event *tikverr.ErrDeadlock) {
	current := global.Load()
	if current != nil && current.deadlock != nil {
		current.deadlock.record(txnID, event)
	}
}

// IndexUsageEnabled reports the startup selection without consulting dynamic config.
func IndexUsageEnabled() bool {
	current := global.Load()
	return current != nil && current.indexUsage != nil
}

// PublishIndexUsage captures collection-start time and publishes only a complete file.
func PublishIndexUsage(ctx context.Context, write func(io.Writer, time.Time) error) error {
	current := global.Load()
	if current == nil || current.indexUsage == nil {
		return nil
	}
	_, err := current.indexUsage.publishWithContext(ctx, time.Now(), write)
	return err
}

// deadlockFileWriter checks the inherited file on the worker's first write.
// Finish a torn line in the old file so the next event starts on its own line.
type deadlockFileWriter struct {
	*lumberjack.Logger
	tailChecked      bool
	prependSeparator bool
}

func (w *deadlockFileWriter) Write(data []byte) (int, error) {
	if !w.tailChecked {
		prepend, err := w.repairTail()
		if err != nil {
			return 0, err
		}
		w.prependSeparator = prepend
		w.tailChecked = true
	}
	if !w.prependSeparator {
		return w.Logger.Write(data)
	}
	// The old file had no room for the separator. Keep it pending if rotation
	// fails before any bytes reach the new file.
	n, err := w.Logger.Write(append([]byte{'\n'}, data...))
	if n > 0 {
		w.prependSeparator = false
		n-- // The separator is outside the caller's payload.
	}
	return n, err
}

// repairTail terminates the old line before Lumberjack may rename the file.
// It requests a separator on the next write if the old file is already full.
func (w *deadlockFileWriter) repairTail() (bool, error) {
	f, err := os.OpenFile(w.Filename, os.O_RDWR|os.O_APPEND, 0)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return false, err
	}
	if info.Size() == 0 {
		return false, nil
	}
	var tail [1]byte
	if _, err := f.ReadAt(tail[:], info.Size()-1); err != nil {
		return false, err
	}
	if tail[0] == '\n' {
		return false, nil
	}
	if info.Size() >= int64(w.MaxSize)<<20 {
		return true, nil
	}
	_, err = f.Write([]byte{'\n'})
	return false, err
}
