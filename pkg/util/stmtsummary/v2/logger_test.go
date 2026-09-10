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

package stmtsummary

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/redact"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestStmtLogRedactsSampleAtSerialization(t *testing.T) {
	oldMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(oldMode) })
	t.Cleanup(config.RestoreFunc())
	// The stored sample can contain a prepared-argument suffix or be truncated.
	// ON must use the captured normalized text instead of parsing this sample.
	const raw = "select ? [arguments: 中文‹secret›](len:99)"
	const normalized = "select ?"
	for _, additional := range []bool{false, true} {
		config.UpdateGlobal(func(c *config.Config) {
			c.KeyspaceObservability = config.KeyspaceObservability{}
			if additional {
				c.KeyspaceObservability.Fields = []config.KeyspaceObservabilityField{{Source: "tenant", StmtLogField: "tenant"}}
			}
			require.NoError(t, c.ResolveKeyspaceObservability(map[string]string{"tenant": "test"}))
		})
		for _, evicted := range []bool{false, true} {
			record := &StmtRecord{redactSampleSQLAtPersist: true, SampleSQL: raw, NormalizedSQL: normalized, Digest: "digest", ExecCount: 2}
			// Reusing the same record tests both directions of a runtime mode change.
			for _, tc := range []struct{ mode, sample string }{
				{"OFF", raw}, {"ON", normalized},
				{"MARKER", "‹select ? [arguments: 中文‹‹secret››](len:99)›"},
				{"OFF", raw}, {"", raw},
			} {
				errors.RedactLogEnabled.Store(tc.mode)
				data, err := marshalStmtRecordWithEvicted(record, evicted)
				require.NoError(t, err)
				var fields map[string]any
				require.NoError(t, json.Unmarshal(data, &fields))
				require.Equal(t, tc.sample, fields["sample_sql"], "mode=%s additional=%t evicted=%t", tc.mode, additional, evicted)
				require.Equal(t, normalized, fields["normalized_sql"])
				require.Equal(t, float64(2), fields["exec_count"])
				if evicted {
					require.Equal(t, true, fields["evicted"])
				} else {
					require.NotContains(t, fields, "evicted")
				}
				if additional {
					require.Equal(t, map[string]any{"tenant": "test"}, fields["additional_fields"])
				} else {
					require.NotContains(t, fields, "additional_fields")
				}
				require.Equal(t, raw, record.SampleSQL, "serialization must not change the memory sample")
			}
		}
	}
}

// gatedSampleStorage stops the first write before serialization so the test can
// change the mode after capture/queueing without depending on timer scheduling.
type gatedSampleStorage struct {
	*stmtLogStorage
	once    sync.Once
	entered chan struct{}
	proceed chan struct{}
}

func (s *gatedSampleStorage) wait() {
	s.once.Do(func() { close(s.entered); <-s.proceed })
}

func (s *gatedSampleStorage) persist(w *stmtWindow, end time.Time) {
	s.wait()
	s.stmtLogStorage.persist(w, end)
}

func (s *gatedSampleStorage) logEvicted(records []*StmtRecord) {
	s.wait()
	s.stmtLogStorage.logEvicted(records)
}

// This lazy source follows the executor's capture-mode contract. The SQL tests
// exercise the real executor for session modes, prepared values and sensitive SQL.
type captureRedactionLazyInfo struct {
	stmtsummary.StmtExecLazyInfo
	mode string
}

func (info captureRedactionLazyInfo) GetOriginalSQL(redactAtCapture bool) string {
	const raw = "select '中文‹secret›'"
	if !redactAtCapture {
		return raw
	}
	if info.mode == "ON" {
		return "select ?"
	}
	return redact.String(info.mode, raw)
}

func TestStmtLogRedactionLifecycle(t *testing.T) {
	oldTiming := vardef.StmtSummaryRedactTiming.Load()
	t.Cleanup(func() { vardef.StmtSummaryRedactTiming.Store(oldTiming) })
	oldMode := errors.RedactLogEnabled.Load()
	t.Cleanup(func() { errors.RedactLogEnabled.Store(oldMode) })
	for _, timing := range []string{vardef.StmtSummaryRedactTimingCapture, vardef.StmtSummaryRedactTimingPersist} {
		for _, action := range []string{"rotate", "flush", "close", "evict"} {
			for _, transition := range []struct{ before, after string }{
				{"OFF", "ON"}, {"ON", "MARKER"}, {"MARKER", "OFF"},
			} {
				t.Run(timing+"/"+action+"/"+transition.before+"-"+transition.after, func(t *testing.T) {
					vardef.StmtSummaryRedactTiming.Store(timing)
					errors.RedactLogEnabled.Store(transition.before)
					filename := filepath.Join(t.TempDir(), "statements.log")
					file, err := os.Create(filename)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, file.Close()) })
					storage := &gatedSampleStorage{
						stmtLogStorage: &stmtLogStorage{logger: zap.New(zapcore.NewCore(&stmtLogEncoder{}, zapcore.AddSync(file), zapcore.InfoLevel))},
						entered:        make(chan struct{}), proceed: make(chan struct{}),
					}
					ss := NewStmtSummary4Test(1)
					ss.storage = storage
					// Unblock any writer even when an assertion fails before release.
					var release sync.Once
					unblock := func() { release.Do(func() { close(storage.proceed) }) }
					t.Cleanup(func() { unblock(); ss.Close() })
					require.NoError(t, ss.SetPersistEvicted(true))
					info := GenerateStmtExecInfo4Test("sample")
					info.NormalizedSQL = "select ?"
					info.LazyInfo = captureRedactionLazyInfo{StmtExecLazyInfo: info.LazyInfo, mode: transition.before}
					ss.Add(info)
					memoryRecord := ss.window.lru.Values()[0].(*lockedStmtRecord)
					raw := info.LazyInfo.GetOriginalSQL(false)
					memorySample := info.LazyInfo.GetOriginalSQL(timing == vardef.StmtSummaryRedactTimingCapture)
					require.Equal(t, memorySample, memoryRecord.SampleSQL)
					done := make(chan struct{})
					go func() {
						defer close(done)
						switch action {
						case "rotate":
							ss.rotate(timeNow())
						case "flush":
							ss.flush()
						case "close":
							ss.Close()
						case "evict":
							ss.Add(GenerateStmtExecInfo4Test("second"))
						}
					}()
					select {
					case <-storage.entered:
					case <-time.After(5 * time.Second):
						t.Fatal("writer did not reach serialization")
					}
					errors.RedactLogEnabled.Store(transition.after)
					// Flip timing after capture/queueing. Existing samples must retain
					// their selected timing through every flush and eviction path.
					if timing == vardef.StmtSummaryRedactTimingCapture {
						vardef.StmtSummaryRedactTiming.Store(vardef.StmtSummaryRedactTimingPersist)
					} else {
						vardef.StmtSummaryRedactTiming.Store(vardef.StmtSummaryRedactTimingCapture)
					}
					unblock()
					<-done
					ss.Close()
					data, err := os.ReadFile(filename)
					require.NoError(t, err)
					found := false
					for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
						var record evictedStmtRecord
						require.NoError(t, json.Unmarshal([]byte(line), &record))
						if record.Digest != "sample" {
							continue
						}
						found = true
						expected := raw
						switch transition.after {
						case "ON":
							expected = "select ?"
						case "MARKER":
							expected = "‹select '中文‹‹secret››'›"
						}
						if timing == vardef.StmtSummaryRedactTimingCapture {
							expected = memorySample
						}
						require.Equal(t, expected, record.SampleSQL)
						require.Equal(t, action == "evict", record.Evicted)
					}
					require.True(t, found, "sample must reach the real file")
					require.Equal(t, memorySample, memoryRecord.SampleSQL, "serialization must not change retained memory samples")
				})
			}
		}
	}
}
