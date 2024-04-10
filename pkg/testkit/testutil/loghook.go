// Copyright 2023 PingCAP, Inc.
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

package testutil

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type logEntry struct {
	entry  zapcore.Entry
	fields []zapcore.Field
}

func (l *logEntry) CheckMsg(t *testing.T, msg string) {
	require.Equal(t, msg, l.entry.Message)
}

func (l *logEntry) CheckField(t *testing.T, requireFields ...zapcore.Field) {
	for _, rf := range requireFields {
		var f *zapcore.Field
		for i, field := range l.fields {
			if field.Equals(rf) {
				f = &l.fields[i]
				break
			}
		}
		require.NotNilf(t, f, "matched log fields %s:%s not found in log", rf.Key, rf)
	}
}

func (l *logEntry) CheckFieldNotEmpty(t *testing.T, fieldName string) {
	var f *zapcore.Field
	for i, field := range l.fields {
		if field.Key == fieldName {
			f = &l.fields[i]
			break
		}
	}
	require.NotNilf(t, f, "log field %s not found in log", fieldName)
	require.NotEmpty(t, f.String)
}

// LogHook captures logs, mainly for testing
type LogHook struct {
	zapcore.Core
	Logs          []logEntry
	enc           zapcore.Encoder
	messageFilter string
}

// Write captures the log and save it
func (h *LogHook) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	h.Logs = append(h.Logs, logEntry{entry: entry, fields: fields})
	return nil
}

// Check implements the string filter
func (h *LogHook) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if len(h.messageFilter) > 0 && !strings.Contains(entry.Message, h.messageFilter) {
		return nil
	}
	return ce.AddCore(entry, h)
}

func (h *LogHook) encode(entry *logEntry) (string, error) {
	buffer, err := h.enc.EncodeEntry(entry.entry, entry.fields)
	if err != nil {
		return "", err
	}
	return buffer.String(), nil
}

// CheckLogCount is a helper function to assert the number of logs captured
func (h *LogHook) CheckLogCount(t *testing.T, expected int) {
	logsStr := make([]string, len(h.Logs))
	for i, item := range h.Logs {
		var err error
		logsStr[i], err = h.encode(&item)
		require.NoError(t, err)
	}
	// Check the length of strings, so that in case the test fails, the error message will be printed.
	require.Len(t, logsStr, expected)
}

// WithLogHook is a helper function to use with LogHook. It returns a context whose logger is replaced with the hook.
func WithLogHook(ctx context.Context, t *testing.T, msgFilter string) (newCtx context.Context, hook *LogHook) {
	conf := &log.Config{Level: os.Getenv("log_level"), File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	enc, err := log.NewTextEncoder(&log.Config{Format: "text"})
	require.NoError(t, err)
	hook = &LogHook{r.Core, nil, enc, msgFilter}
	logger := zap.New(hook)
	newCtx = context.WithValue(ctx, logutil.CtxLogKey, logger)
	return
}
