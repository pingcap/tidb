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

func heapProfileNames(t *testing.T, dir string) []string {
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestHeapProfileTriggerState(t *testing.T) {
	m := newHeapProfileArbitratorForTest(1000)
	p := newHeapProfileCollector(filepath.Join(t.TempDir(), heapProfileDirName))
	var writeCount int
	p.writeProfile = func(w io.Writer) error {
		writeCount++
		_, err := io.WriteString(w, "profile")
		return err
	}

	start := time.Date(2026, time.August, 14, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	currentTime := start
	p.now = func() time.Time { return currentTime }
	setHeapProfileMemInuse(m, 600)
	p.tryCapture(m)
	require.Equal(t, 0, writeCount)

	currentTime = start.Add(time.Minute)
	setHeapProfileMemInuse(m, 720)
	p.tryCapture(m)
	currentTime = start.Add(time.Minute + 10*time.Second)
	setHeapProfileMemInuse(m, 820)
	p.tryCapture(m)
	currentTime = start.Add(time.Minute + 20*time.Second)
	setHeapProfileMemInuse(m, 870)
	p.tryCapture(m)
	require.Equal(t, 3, writeCount)
	require.Contains(t, heapProfileNames(t, p.dir), "heap-20260814T100100+0800-70pct.pprof")
	require.Contains(t, heapProfileNames(t, p.dir), "heap-20260814T100110+0800-80pct.pprof")
	require.Contains(t, heapProfileNames(t, p.dir), "heap-20260814T100120+0800-85pct.pprof")

	currentTime = start.Add(4 * time.Minute)
	setHeapProfileMemInuse(m, 760)
	p.tryCapture(m)
	require.Equal(t, 3, writeCount)
	currentTime = start.Add(5 * time.Minute)
	setHeapProfileMemInuse(m, 640)
	p.tryCapture(m)
	currentTime = start.Add(6 * time.Minute)
	setHeapProfileMemInuse(m, 740)
	p.tryCapture(m)
	require.Equal(t, 4, writeCount)

	m2 := newHeapProfileArbitratorForTest(1000)
	p2 := newHeapProfileCollector(filepath.Join(t.TempDir(), heapProfileDirName))
	p2.writeProfile = p.writeProfile
	p2.now = func() time.Time { return start }
	setHeapProfileMemInuse(m2, 870)
	p2.tryCapture(m2)
	require.Equal(t, 5, writeCount)
	names := heapProfileNames(t, p2.dir)
	require.Len(t, names, 2)
	require.Contains(t, names, "heap-20260814T100000+0800-85pct.pprof")
	require.Contains(t, names, "heap-20260814T100000+0800-85pct.json")

	m3 := newHeapProfileArbitratorForTest(1000)
	p3 := newHeapProfileCollector(filepath.Join(t.TempDir(), heapProfileDirName))
	cooldownTime := start
	p3.now = func() time.Time { return cooldownTime }
	cooldownWrites := 0
	p3.writeProfile = func(w io.Writer) error {
		cooldownWrites++
		_, err := io.WriteString(w, "profile")
		return err
	}
	setHeapProfileMemInuse(m3, 720)
	p3.tryCapture(m3)
	require.Equal(t, 1, cooldownWrites)

	cooldownTime = start.Add(10 * time.Second)
	setHeapProfileMemInuse(m3, 640)
	p3.tryCapture(m3)
	cooldownTime = start.Add(20 * time.Second)
	setHeapProfileMemInuse(m3, 720)
	p3.tryCapture(m3)
	require.Equal(t, 1, cooldownWrites)

	cooldownTime = start.Add(30 * time.Second)
	setHeapProfileMemInuse(m3, 870)
	p3.tryCapture(m3)
	require.Equal(t, 2, cooldownWrites)
	require.Contains(t, heapProfileNames(t, p3.dir), "heap-20260814T100030+0800-85pct.pprof")

	cooldownTime = start.Add(40 * time.Second)
	setHeapProfileMemInuse(m3, 640)
	p3.tryCapture(m3)
	cooldownTime = start.Add(50 * time.Second)
	setHeapProfileMemInuse(m3, 870)
	p3.tryCapture(m3)
	require.Equal(t, 2, cooldownWrites)
	cooldownTime = start.Add(80 * time.Second)
	setHeapProfileMemInuse(m3, 720)
	p3.tryCapture(m3)
	require.Equal(t, 2, cooldownWrites)
	cooldownTime = start.Add(91 * time.Second)
	p3.tryCapture(m3)
	require.Equal(t, 3, cooldownWrites)
}

func TestHeapProfileCaptureMetadata(t *testing.T) {
	dir := filepath.Join(t.TempDir(), heapProfileDirName)
	p := newHeapProfileCollector(dir)
	start := time.Date(2026, time.August, 14, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	p.now = func() time.Time { return start.Add(time.Millisecond) }
	p.writeProfile = func(w io.Writer) error {
		_, err := io.WriteString(w, "profile")
		return err
	}
	m := newHeapProfileArbitratorForTest(1000)
	setHeapProfileMemInuse(m, 700)

	p.capture(m, 70)

	base := "heap-20260814T100000+0800-70pct"
	profilePath := filepath.Join(dir, base+".pprof")
	metadataPath := filepath.Join(dir, base+".json")
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
	require.Equal(t, start.Format(time.RFC3339), metadata.StartTime)
	require.Equal(t, 70, metadata.ThresholdPct)
	require.Equal(t, int64(700), metadata.StartState.MemInuse)
	require.Equal(t, int64(0), metadata.DurationMs)
	require.NotContains(t, string(metadataBytes), "profile_size_bytes")
}

func TestHeapProfileTriggerCutoffAndLimitChange(t *testing.T) {
	m := newHeapProfileArbitratorForTest(1000)
	p := newHeapProfileCollector(filepath.Join(t.TempDir(), heapProfileDirName))
	writeCount := 0
	p.writeProfile = func(w io.Writer) error {
		writeCount++
		_, err := io.WriteString(w, "profile")
		return err
	}
	currentTime := time.Date(2026, time.August, 14, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	p.now = func() time.Time { return currentTime }
	setHeapProfileMemInuse(m, 910)
	p.tryCapture(m)
	setHeapProfileMemInuse(m, 820)
	p.tryCapture(m)
	require.Equal(t, 0, writeCount)

	setHeapProfileMemInuse(m, 640)
	p.tryCapture(m)
	setHeapProfileMemInuse(m, 700)
	p.tryCapture(m)
	require.Equal(t, 1, writeCount)

	currentTime = currentTime.Add(heapProfileMinInterval)
	m.SetLimit(2000)
	setHeapProfileMemInuse(m, 1400)
	p.tryCapture(m)
	require.Equal(t, 2, writeCount)
}

func TestHeapProfileCaptureFailureAndSkip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), heapProfileDirName)
	p := newHeapProfileCollector(dir)
	p.writeProfile = func(io.Writer) error {
		return io.ErrClosedPipe
	}
	m := newHeapProfileArbitratorForTest(1000)
	setHeapProfileMemInuse(m, 700)
	p.capture(m, 70)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries)

	writeCount := 0
	p.writeProfile = func(io.Writer) error {
		writeCount++
		return nil
	}
	m.SetWorkMode(ArbitratorModeDisable)
	p.capture(m, 70)
	require.Equal(t, 0, writeCount)

	m.SetWorkMode(ArbitratorModeStandard)
	m.heapController.memRisk.startTime.unixMilli.Store(1)
	p.capture(m, 70)
	require.Equal(t, 0, writeCount)
}

func TestHeapProfileRetention(t *testing.T) {
	dir := filepath.Join(t.TempDir(), heapProfileDirName)
	require.NoError(t, os.MkdirAll(dir, 0750))
	start := time.Date(2026, time.August, 14, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	for i := 0; i < 12; i++ {
		base := "heap-" + start.Add(time.Duration(i)*time.Minute).Format("20060102T150405Z0700") + "-70pct"
		require.NoError(t, os.WriteFile(filepath.Join(dir, base+".pprof"), []byte("profile"), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, base+".json"), []byte("{}"), 0600))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "heap-orphan.json"), []byte("{}"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".heap-profile.stale.tmp"), []byte("tmp"), 0600))

	p := &heapProfileCollector{dir: dir}
	p.enforceRetention()
	names := heapProfileNames(t, dir)
	require.Len(t, names, heapProfileMaxGroups*2)
	for i := 0; i < 12-heapProfileMaxGroups; i++ {
		base := "heap-" + start.Add(time.Duration(i)*time.Minute).Format("20060102T150405Z0700") + "-70pct"
		require.NotContains(t, names, base+".pprof")
		require.NotContains(t, names, base+".json")
	}
	for i := 12 - heapProfileMaxGroups; i < 12; i++ {
		base := "heap-" + start.Add(time.Duration(i)*time.Minute).Format("20060102T150405Z0700") + "-70pct"
		require.Contains(t, names, base+".pprof")
		require.Contains(t, names, base+".json")
	}
	for _, name := range names {
		require.False(t, strings.Contains(name, "orphan") || strings.HasSuffix(name, ".tmp"))
	}
}

func TestHandleGlobalMemArbitratorRuntimeWithoutCollector(t *testing.T) {
	baseDir := t.TempDir()
	SetupGlobalMemArbitratorForTest(baseDir)
	defer CleanupGlobalMemArbitratorForTest()
	require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeStandardName))
	m := GlobalMemArbitrator()
	require.NotNil(t, m)
	m.stop()
	globalArbitrator.heapProfiler.Store(nil)
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
	globalArbitrator.heapProfiler.Store(profiler)
	require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeDisableName))
	require.True(t, SetGlobalMemArbitratorWorkMode(ArbitratorModeStandardName))

	HandleGlobalMemArbitratorRuntime()
	require.True(t, profiler.trigger.lastCaptureAt.IsZero())
	require.Equal(t, m.limit(), profiler.trigger.lastLimit)
	require.Zero(t, profiler.trigger.attempted)
	require.Zero(t, profiler.trigger.lastCaptureThreshold)
	require.False(t, profiler.trigger.closed)
}
