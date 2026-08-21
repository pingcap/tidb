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

package memory

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func newHeapProfileArbitratorForTest(limit int64) *MemArbitrator {
	m := NewMemArbitrator(limit, 1, 3, 0, &memStateRecorderForTest{
		load: func() (*RuntimeMemStateV1, error) {
			return nil, nil
		},
		store: func(*RuntimeMemStateV1) error {
			return nil
		},
	})
	m.SetWorkMode(ArbitratorModeStandard)
	return m
}

func setHeapProfileMemInuse(m *MemArbitrator, value int64) {
	m.setRuntimeMemStats(memStats{
		HeapAlloc:  value,
		HeapInuse:  value,
		MemOffHeap: 0,
	})
}

func heapProfileTestStartTime() time.Time {
	return time.Date(2026, time.August, 14, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
}

type heapProfileTestEnv struct {
	m        *MemArbitrator
	p        *heapProfileCollector
	now      time.Time
	writes   int
	writeErr error
}

func newHeapProfileTestEnv(t *testing.T) *heapProfileTestEnv {
	t.Helper()
	return newHeapProfileTestEnvAt(t, filepath.Join(t.TempDir(), heapProfileDirName))
}

func newHeapProfileTestEnvAt(t *testing.T, dir string) *heapProfileTestEnv {
	t.Helper()
	e := &heapProfileTestEnv{
		m:   newHeapProfileArbitratorForTest(1000),
		now: heapProfileTestStartTime(),
	}
	e.p = newHeapProfileCollector(dir)
	e.p.now = func() time.Time { return e.now }
	e.p.writeProfile = func(w io.Writer) error {
		e.writes++
		if e.writeErr != nil {
			return e.writeErr
		}
		_, err := io.WriteString(w, "profile")
		return err
	}
	return e
}

func (e *heapProfileTestEnv) tryCapture(memInuse int64) {
	setHeapProfileMemInuse(e.m, memInuse)
	e.p.tryCapture(e.m)
}

func heapProfileNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestHeapProfileTriggerState(t *testing.T) {
	t.Run("capture reached thresholds and reset", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		start := e.now
		e.tryCapture(600)
		require.Zero(t, e.writes)

		e.now = start.Add(time.Minute)
		e.tryCapture(720)
		e.now = start.Add(time.Minute + 10*time.Second)
		e.tryCapture(820)
		e.now = start.Add(time.Minute + 20*time.Second)
		e.tryCapture(870)
		require.Equal(t, 3, e.writes)
		names := heapProfileNames(t, e.p.dir)
		require.Contains(t, names, "2026-08-14T10-01-00+0800.70pct.pprof")
		require.Contains(t, names, "2026-08-14T10-01-10+0800.80pct.pprof")
		require.Contains(t, names, "2026-08-14T10-01-20+0800.85pct.pprof")

		e.now = start.Add(4 * time.Minute)
		e.tryCapture(760)
		require.Equal(t, 3, e.writes)
		e.now = start.Add(5 * time.Minute)
		e.tryCapture(640)
		e.now = start.Add(6 * time.Minute)
		e.tryCapture(740)
		require.Equal(t, 4, e.writes)
	})

	t.Run("capture highest reached threshold", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		e.tryCapture(870)
		require.Equal(t, 1, e.writes)
		require.ElementsMatch(t, []string{
			"2026-08-14T10-00-00+0800.85pct.pprof",
			"2026-08-14T10-00-00+0800.85pct.meta.json",
		}, heapProfileNames(t, e.p.dir))
	})

	t.Run("enforce cooldown", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		start := e.now
		e.tryCapture(720)
		require.Equal(t, 1, e.writes)

		e.now = start.Add(10 * time.Second)
		e.tryCapture(640)
		e.now = start.Add(20 * time.Second)
		e.tryCapture(720)
		require.Equal(t, 1, e.writes)

		e.now = start.Add(30 * time.Second)
		e.tryCapture(870)
		require.Equal(t, 2, e.writes)
		require.Contains(t, heapProfileNames(t, e.p.dir), "2026-08-14T10-00-30+0800.85pct.pprof")

		e.now = start.Add(40 * time.Second)
		e.tryCapture(640)
		e.now = start.Add(50 * time.Second)
		e.tryCapture(870)
		require.Equal(t, 2, e.writes)
		e.now = start.Add(80 * time.Second)
		e.tryCapture(720)
		require.Equal(t, 2, e.writes)
		e.now = start.Add(91 * time.Second)
		e.tryCapture(720)
		require.Equal(t, 3, e.writes)
	})

	t.Run("retry after memory risk", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		e.m.heapController.memRisk.startTime.unixMilli.Store(1)
		e.tryCapture(700)
		require.Zero(t, e.writes)
		require.Zero(t, e.p.trigger.attempted)

		e.m.heapController.memRisk.startTime.unixMilli.Store(0)
		e.tryCapture(700)
		require.Equal(t, 1, e.writes)
		require.Equal(t, uint32(0b001), e.p.trigger.attempted)
	})

	t.Run("retry setup failure after cooldown", func(t *testing.T) {
		blockedDir := filepath.Join(t.TempDir(), heapProfileDirName)
		require.NoError(t, os.WriteFile(blockedDir, []byte("not a directory"), 0600))
		e := newHeapProfileTestEnvAt(t, blockedDir)
		start := e.now
		e.tryCapture(700)
		require.Zero(t, e.writes)
		require.Zero(t, e.p.trigger.attempted)
		require.Equal(t, start, e.p.trigger.lastCaptureAt)
		require.Equal(t, 70, e.p.trigger.lastCaptureThreshold)

		require.NoError(t, os.Remove(blockedDir))
		e.now = start.Add(100 * time.Millisecond)
		e.tryCapture(700)
		require.Zero(t, e.writes)
		e.now = start.Add(heapProfileMinInterval)
		e.tryCapture(700)
		require.Equal(t, 1, e.writes)
		require.Equal(t, uint32(0b001), e.p.trigger.attempted)
	})

	t.Run("consume threshold after write starts", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		e.writeErr = io.ErrClosedPipe
		e.tryCapture(700)
		e.tryCapture(700)
		require.Equal(t, 1, e.writes)
		require.Equal(t, uint32(0b001), e.p.trigger.attempted)
	})

	t.Run("higher threshold bypasses cooldown", func(t *testing.T) {
		blockedDir := filepath.Join(t.TempDir(), heapProfileDirName)
		require.NoError(t, os.WriteFile(blockedDir, []byte("not a directory"), 0600))
		e := newHeapProfileTestEnvAt(t, blockedDir)
		start := e.now
		e.tryCapture(700)
		require.Zero(t, e.writes)
		require.Zero(t, e.p.trigger.attempted)

		require.NoError(t, os.Remove(blockedDir))
		e.now = start.Add(100 * time.Millisecond)
		e.tryCapture(800)
		require.Equal(t, 1, e.writes)
		require.Equal(t, uint32(0b011), e.p.trigger.attempted)
	})
}

func TestHeapProfileCaptureMetadata(t *testing.T) {
	dir := filepath.Join(t.TempDir(), heapProfileDirName)
	e := newHeapProfileTestEnvAt(t, dir)
	setHeapProfileMemInuse(e.m, 700)

	require.True(t, e.p.capture(e.m, 70))

	base := "2026-08-14T10-00-00+0800.70pct"
	profilePath := filepath.Join(dir, base+".pprof")
	metadataPath := filepath.Join(dir, base+heapProfileMetadataSuffix)
	profile, err := os.ReadFile(profilePath)
	require.NoError(t, err)
	require.Equal(t, "profile", string(profile))
	info, err := os.Stat(profilePath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())

	metadataBytes, err := os.ReadFile(metadataPath)
	require.NoError(t, err)
	var metadata heapProfileMetadata
	require.NoError(t, json.Unmarshal(metadataBytes, &metadata))
	require.Equal(t, 1, metadata.Version)
	require.Equal(t, e.now.Format(time.RFC3339), metadata.StartTime)
	require.Equal(t, 70, metadata.ThresholdPct)
	require.Equal(t, int64(700), metadata.StartState.MemInuse)
	require.Equal(t, int64(0), metadata.DurationMs)
	require.Contains(t, string(metadataBytes), "\n  \"start_time\"")
	require.Contains(t, string(metadataBytes), "\"mem_inuse_bytes\"")
	require.True(t, strings.HasSuffix(string(metadataBytes), "\n"))
	require.NotContains(t, string(metadataBytes), "\"mem_inuse\"")
	require.NotContains(t, string(metadataBytes), "profile_size_bytes")
}

func TestHeapProfileTriggerCutoffAndLimitChange(t *testing.T) {
	e := newHeapProfileTestEnv(t)
	e.tryCapture(910)
	e.tryCapture(820)
	require.Zero(t, e.writes)

	e.tryCapture(640)
	e.tryCapture(700)
	require.Equal(t, 1, e.writes)

	e.now = e.now.Add(heapProfileMinInterval)
	e.m.SetLimit(2000)
	e.tryCapture(1400)
	require.Equal(t, 2, e.writes)
}

func TestHeapProfileCaptureFailureAndSkip(t *testing.T) {
	t.Run("write failure", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		e.writeErr = io.ErrClosedPipe
		setHeapProfileMemInuse(e.m, 700)
		require.True(t, e.p.capture(e.m, 70))
		require.Empty(t, heapProfileNames(t, e.p.dir))
	})

	t.Run("disabled", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		e.m.SetWorkMode(ArbitratorModeDisable)
		setHeapProfileMemInuse(e.m, 700)
		require.False(t, e.p.capture(e.m, 70))
		require.Zero(t, e.writes)
	})

	t.Run("memory risk", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		e.m.heapController.memRisk.startTime.unixMilli.Store(1)
		setHeapProfileMemInuse(e.m, 700)
		require.False(t, e.p.capture(e.m, 70))
		require.Zero(t, e.writes)
	})

	t.Run("cutoff", func(t *testing.T) {
		e := newHeapProfileTestEnv(t)
		setHeapProfileMemInuse(e.m, 900)
		require.False(t, e.p.capture(e.m, 70))
		require.Zero(t, e.writes)
	})
}

func TestHeapProfileRetention(t *testing.T) {
	dir := filepath.Join(t.TempDir(), heapProfileDirName)
	require.NoError(t, os.MkdirAll(dir, 0750))
	start := heapProfileTestStartTime()
	groupCount := heapProfileMaxGroups + 2
	for i := range groupCount {
		base := start.Add(time.Duration(i)*time.Minute).Format(heapProfileTimestampLayout) + ".70pct"
		require.NoError(t, os.WriteFile(filepath.Join(dir, base+".pprof"), []byte("profile"), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, base+heapProfileMetadataSuffix), []byte("{}"), 0600))
	}
	orphanMetadata := start.Add(time.Duration(groupCount)*time.Minute).Format(heapProfileTimestampLayout) + ".70pct" + heapProfileMetadataSuffix
	require.NoError(t, os.WriteFile(filepath.Join(dir, orphanMetadata), []byte("{}"), 0600))
	unknownFiles := []string{
		"heap-manual.pprof",
		start.Format(heapProfileTimestampLayout) + ".70pct.json",
		start.Format(heapProfileTimestampLayout) + ".90pct.pprof",
		start.Format(heapProfileTimestampLayout) + "-70pct.pprof",
	}
	for _, name := range unknownFiles {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("unknown"), 0600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".heap-profile.stale.tmp"), []byte("tmp"), 0600))

	p := &heapProfileCollector{dir: dir}
	p.enforceRetention()
	names := heapProfileNames(t, dir)
	for i := 0; i < groupCount-heapProfileMaxGroups; i++ {
		base := start.Add(time.Duration(i)*time.Minute).Format(heapProfileTimestampLayout) + ".70pct"
		require.NotContains(t, names, base+".pprof")
		require.NotContains(t, names, base+heapProfileMetadataSuffix)
	}
	for i := groupCount - heapProfileMaxGroups; i < groupCount; i++ {
		base := start.Add(time.Duration(i)*time.Minute).Format(heapProfileTimestampLayout) + ".70pct"
		require.Contains(t, names, base+".pprof")
		require.Contains(t, names, base+heapProfileMetadataSuffix)
	}
	require.NotContains(t, names, orphanMetadata)
	for _, name := range unknownFiles {
		require.Contains(t, names, name)
	}
	for _, name := range names {
		require.False(t, strings.HasSuffix(name, ".tmp"))
	}

	base, captureTime, isProfile, ok := parseHeapProfileFileName("2026-08-14T10-00-00-0700.85pct.meta.json")
	require.True(t, ok)
	require.False(t, isProfile)
	require.Equal(t, "2026-08-14T10-00-00-0700.85pct", base)
	_, offset := captureTime.Zone()
	require.Equal(t, -7*60*60, offset)
	_, _, _, ok = parseHeapProfileFileName("2026-08-14T10-00-00+0800.90pct.pprof")
	require.False(t, ok)
}

func TestHandleGlobalMemArbitratorRuntime(t *testing.T) {
	baseDir := t.TempDir()
	SetupGlobalMemArbitratorForTest(baseDir)
	defer CleanupGlobalMemArbitratorForTest()
	require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeStandardName))
	m := GlobalMemArbitrator()
	require.NotNil(t, m)
	m.stop()
	globalArbitrator.runtimeHandler.heapProfiler.Store(nil)
	m.heapController.heapInuse.Store(-1)

	HandleGlobalMemArbitratorRuntime()
	require.GreaterOrEqual(t, m.heapController.heapInuse.Load(), int64(0))

	profiler := newHeapProfileCollector(filepath.Join(baseDir, heapProfileDirName))
	profiler.trigger = heapProfileTriggerState{
		lastCaptureAt:        time.Now(),
		lastLimit:            1024,
		attempted:            7,
		lastCaptureThreshold: 85,
		closed:               true,
	}
	globalArbitrator.runtimeHandler.heapProfiler.Store(profiler)
	require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeDisableName))
	require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeStandardName))

	HandleGlobalMemArbitratorRuntime()
	require.Equal(t, heapProfileTriggerState{lastLimit: m.limit()}, profiler.trigger)

	t.Run("skip reentrant runtime handling", func(t *testing.T) {
		globalArbitrator.runtimeHandler.Lock()
		defer globalArbitrator.runtimeHandler.Unlock()
		globalArbitrator.runtimeHandler.reset.Store(true)

		HandleGlobalMemArbitratorRuntime()
		require.True(t, globalArbitrator.runtimeHandler.reset.Load())
		globalArbitrator.runtimeHandler.reset.Store(false)
	})
}
