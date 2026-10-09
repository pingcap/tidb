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

package logutil

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/stretchr/testify/require"
)

type mockRotateLogger struct {
	writeCount  int
	rotateCount int
	rotateErr   error
}

func (m *mockRotateLogger) Write(p []byte) (int, error) {
	m.writeCount++
	return len(p), nil
}

func (m *mockRotateLogger) Rotate() error {
	m.rotateCount++
	return m.rotateErr
}

func TestRotateByDayWriteSyncer(t *testing.T) {
	mockLogger := &mockRotateLogger{}
	writer := newRotateByDayWriteSyncer(mockLogger)

	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.Local)
	nowSeries := []time.Time{
		base,
		base.Add(2 * time.Hour),
		base.Add(24 * time.Hour),
		base.Add(25 * time.Hour),
		base.Add(48 * time.Hour),
	}
	idx := 0
	writer.nowFunc = func() time.Time {
		tm := nowSeries[idx]
		idx++
		return tm
	}

	for range nowSeries {
		n, err := writer.Write([]byte("x"))
		require.NoError(t, err)
		require.Equal(t, 1, n)
	}

	require.Equal(t, 5, mockLogger.writeCount)
	require.Equal(t, 2, mockLogger.rotateCount)
}

func TestRotateByDayWriteSyncerRotateError(t *testing.T) {
	mockLogger := &mockRotateLogger{rotateErr: errors.New("rotate failed")}
	writer := newRotateByDayWriteSyncer(mockLogger)

	base := time.Date(2026, 6, 27, 10, 0, 0, 0, time.Local)
	nowSeries := []time.Time{
		base,
		base.Add(24 * time.Hour),
	}
	idx := 0
	writer.nowFunc = func() time.Time {
		tm := nowSeries[idx]
		idx++
		return tm
	}

	_, err := writer.Write([]byte("x"))
	require.NoError(t, err)

	_, err = writer.Write([]byte("x"))
	require.ErrorContains(t, err, "rotate failed")
	require.Equal(t, 1, mockLogger.writeCount)
	require.Equal(t, 1, mockLogger.rotateCount)
}

func TestRotateByDayInvalidCompression(t *testing.T) {
	fileCfg := FileLogConfig{
		FileLogConfig: log.FileLogConfig{
			Filename:    filepath.Join(t.TempDir(), "tidb.log"),
			MaxSize:     10,
			MaxDays:     1,
			MaxBackups:  1,
			Compression: "invalid",
		},
		RotateByDay: true,
	}
	conf := NewLogConfig("warn", DefaultLogFormat, "", "", fileCfg, false)
	err := InitLogger(conf)
	require.ErrorContains(t, err, "can't set compression")
}
