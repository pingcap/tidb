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

package auditlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAuditWriter(t *testing.T) {
	w := NewAuditWriter(WriterConfig{})
	assert.Equal(t, defaultBufferSize, w.config.BufferSize)
	assert.Equal(t, defaultBatchSize, w.config.BatchSize)
	assert.Equal(t, defaultFlushInterval, w.config.FlushInterval)
	assert.Equal(t, int64(maxLogFileSize), w.config.MaxFileSize)
}

func TestAuditWriterStartStop(t *testing.T) {
	InitAuditMetrics()
	dir := t.TempDir()
	w := NewAuditWriter(WriterConfig{
		LogDir:        dir,
		BufferSize:    100,
		BatchSize:     10,
		FlushInterval: 100 * time.Millisecond,
	})

	err := w.Start()
	require.NoError(t, err)

	// Verify log directory was created
	_, err = os.Stat(dir)
	assert.NoError(t, err)

	// Write some events
	for i := 0; i < 5; i++ {
		w.Write(&AuditEvent{
			Timestamp: time.Now(),
			User:      "test_user",
			Database:  "test_db",
			QueryType: QueryTypeSelect,
			Query:     "SELECT 1",
			Success:   true,
			RuleID:    "rule-1",
		})
	}

	// Wait for flush
	time.Sleep(300 * time.Millisecond)

	// Verify log file was created
	files, err := filepath.Glob(filepath.Join(dir, "audit-*.log"))
	require.NoError(t, err)
	assert.NotEmpty(t, files)

	w.Stop()
}

func TestAuditWriterGenerateFileName(t *testing.T) {
	w := NewAuditWriter(WriterConfig{LogDir: "/tmp/test"})
	fn := w.generateFileName()
	assert.Contains(t, fn, "/tmp/test/audit-")
	assert.Contains(t, fn, ".log")
}
