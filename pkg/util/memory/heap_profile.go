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
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	heapProfileLevel70Milli int64 = 700
	heapProfileLevel80Milli int64 = 800
	heapProfileLevel85Milli int64 = 850
	heapProfileResetMilli   int64 = 650
	heapProfileCutoffMilli  int64 = 900
	heapProfileMinInterval        = time.Minute
	heapProfileMaxGroups          = 10
	heapProfileDirName            = "heap_profiles"
)

var heapProfileLevels = [...]struct {
	ratioMilli int64
	threshold  int
}{
	{ratioMilli: heapProfileLevel70Milli, threshold: 70},
	{ratioMilli: heapProfileLevel80Milli, threshold: 80},
	{ratioMilli: heapProfileLevel85Milli, threshold: 85},
}

type heapProfileTriggerState struct {
	lastCaptureAt time.Time
	lastLimit     int64
	attempted     uint32
	closed        bool
}

type heapProfileCollector struct {
	now          func() time.Time
	writeProfile func(io.Writer) error
	dir          string
	trigger      heapProfileTriggerState
}

type heapProfileArbitratorSnapshot struct {
	heapProfileMetadataState
	captureCutoff int64
}

type heapProfileMetadataState struct {
	HeapAlloc  int64 `json:"heap_alloc"`
	HeapInuse  int64 `json:"heap_inuse"`
	MemInuse   int64 `json:"mem_inuse"`
	QuotaAlloc int64 `json:"quota_alloc"`
	Limit      int64 `json:"limit"`
}

type heapProfileMetadata struct {
	StartTime    string                   `json:"start_time"`
	StartState   heapProfileMetadataState `json:"start_state"`
	Version      int                      `json:"version"`
	ThresholdPct int                      `json:"threshold_pct"`
	DurationMs   int64                    `json:"duration_ms"`
}

type heapProfileFileGroup struct {
	modTime      time.Time
	base         string
	profilePath  string
	metadataPath string
}

func newHeapProfileCollector(dir string) *heapProfileCollector {
	p := &heapProfileCollector{
		dir:          dir,
		now:          now,
		writeProfile: writeHeapProfile,
	}
	p.enforceRetention()
	return p
}

func writeHeapProfile(w io.Writer) error {
	profile := pprof.Lookup("heap")
	if profile == nil {
		return errors.New("heap profile is unavailable")
	}
	return profile.WriteTo(w, 0)
}

func (m *MemArbitrator) heapProfileSnapshot() heapProfileArbitratorSnapshot {
	limit := m.limit()
	return heapProfileArbitratorSnapshot{
		heapProfileMetadataState: heapProfileMetadataState{
			HeapAlloc:  m.heapController.heapAlloc.Load(),
			HeapInuse:  m.heapController.heapInuse.Load(),
			MemInuse:   m.heapController.memInuse.Load(),
			QuotaAlloc: m.allocated(),
			Limit:      limit,
		},
		captureCutoff: multiRatio(limit, heapProfileCutoffMilli),
	}
}

func (p *heapProfileCollector) resetTriggerState() {
	p.trigger = heapProfileTriggerState{}
}

func (p *heapProfileCollector) tryCapture(m *MemArbitrator) {
	snapshot := m.heapProfileSnapshot()
	if snapshot.Limit <= 0 {
		return
	}

	currentRatio := calcRatio(snapshot.MemInuse, snapshot.Limit)
	state := &p.trigger

	if state.lastLimit != snapshot.Limit {
		state.lastLimit = snapshot.Limit
		state.attempted = 0
		state.closed = false
	}

	if currentRatio < heapProfileResetMilli {
		state.attempted = 0
		state.closed = false
		return
	}

	if currentRatio >= heapProfileCutoffMilli {
		state.closed = true
	}

	if state.closed {
		return
	}

	highest := -1
	reachedMask := uint32(0)
	for i, level := range heapProfileLevels {
		if currentRatio < level.ratioMilli {
			continue
		}
		reachedMask |= 1 << i
		if state.attempted&(1<<i) == 0 {
			highest = i
		}
	}

	if highest < 0 {
		return
	}

	level := heapProfileLevels[highest]
	if level.ratioMilli == heapProfileLevel70Milli &&
		!state.lastCaptureAt.IsZero() &&
		p.currentTime().Sub(state.lastCaptureAt) < heapProfileMinInterval {
		return
	}

	// Mark thresholds before capturing so a failed capture is not retried every tick.
	state.attempted |= reachedMask
	p.capture(m, level.threshold)
}

func (p *heapProfileCollector) capture(m *MemArbitrator, threshold int) {
	snapshot := m.heapProfileSnapshot()
	if m.WorkMode() == ArbitratorModeDisable ||
		m.AtMemRisk() ||
		snapshot.Limit <= 0 ||
		snapshot.MemInuse >= snapshot.captureCutoff ||
		p.writeProfile == nil {
		return
	}
	if err := os.MkdirAll(p.dir, 0750); err != nil {
		return
	}

	tmp, err := os.CreateTemp(p.dir, ".heap-profile.*.tmp")
	if err != nil {
		return
	}
	tmpPath := tmp.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmp.Chmod(0600); err != nil {
		return
	}

	startTime := p.currentTime()
	p.trigger.lastCaptureAt = startTime
	timestamp := startTime.Format("20060102T150405Z0700")
	base := "heap-" + timestamp + "-" + strconv.Itoa(threshold) + "pct"
	profilePath := filepath.Join(p.dir, base+".pprof")

	if err := p.writeProfile(tmp); err != nil {
		return
	}
	if err := tmp.Close(); err != nil {
		return
	}
	closed = true
	if err := os.Rename(tmpPath, profilePath); err != nil {
		return
	}
	renamed = true

	metadata := heapProfileMetadata{
		Version:      1,
		StartTime:    startTime.Format(time.RFC3339),
		ThresholdPct: threshold,
		StartState:   snapshot.heapProfileMetadataState,
		DurationMs:   p.currentTime().Sub(startTime).Milliseconds(),
	}
	_ = p.writeMetadataAtomically(base+".json", metadata)
	p.enforceRetention()
}

func (p *heapProfileCollector) writeMetadataAtomically(name string, metadata heapProfileMetadata) error {
	data, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(p.dir, ".heap-metadata.*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	closed := false
	renamed := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !renamed {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := tmp.Chmod(0600); err != nil {
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	closed = true
	if err := os.Rename(tmpPath, filepath.Join(p.dir, name)); err != nil {
		return err
	}
	renamed = true
	return nil
}

func (p *heapProfileCollector) enforceRetention() {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return
	}

	groups := make(map[string]*heapProfileFileGroup)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if isHeapProfileTemp(name) {
			_ = os.Remove(filepath.Join(p.dir, name))
			continue
		}
		if !isHeapProfileFinal(name) {
			continue
		}

		base := strings.TrimSuffix(strings.TrimSuffix(name, ".pprof"), ".json")
		group := groups[base]
		if group == nil {
			group = &heapProfileFileGroup{base: base}
			groups[base] = group
		}
		path := filepath.Join(p.dir, name)
		if info, err := entry.Info(); err == nil && info.ModTime().After(group.modTime) {
			group.modTime = info.ModTime()
		}
		if strings.HasSuffix(name, ".pprof") {
			group.profilePath = path
		} else {
			group.metadataPath = path
		}
	}

	profiles := make([]*heapProfileFileGroup, 0, len(groups))
	for base, group := range groups {
		if group.profilePath == "" {
			_ = os.Remove(group.metadataPath)
			delete(groups, base)
			continue
		}
		profiles = append(profiles, group)
	}

	sort.Slice(profiles, func(i, j int) bool {
		iTime, iOK := heapProfileTimestamp(profiles[i].base)
		jTime, jOK := heapProfileTimestamp(profiles[j].base)
		if !iOK {
			iTime = profiles[i].modTime
		}
		if !jOK {
			jTime = profiles[j].modTime
		}
		if !iTime.Equal(jTime) {
			return iTime.Before(jTime)
		}
		return profiles[i].base < profiles[j].base
	})

	if len(profiles) <= heapProfileMaxGroups {
		return
	}
	for _, group := range profiles[:len(profiles)-heapProfileMaxGroups] {
		_ = os.Remove(group.profilePath)
		if group.metadataPath != "" {
			_ = os.Remove(group.metadataPath)
		}
	}
}

func isHeapProfileTemp(name string) bool {
	return (strings.HasPrefix(name, ".heap-profile.") || strings.HasPrefix(name, ".heap-metadata.")) && strings.HasSuffix(name, ".tmp")
}

func isHeapProfileFinal(name string) bool {
	return strings.HasPrefix(name, "heap-") && (strings.HasSuffix(name, ".pprof") || strings.HasSuffix(name, ".json"))
}

func heapProfileTimestamp(base string) (time.Time, bool) {
	if !strings.HasPrefix(base, "heap-") {
		return time.Time{}, false
	}
	rest := strings.TrimPrefix(base, "heap-")
	separator := strings.LastIndexByte(rest, '-')
	if separator <= 0 {
		return time.Time{}, false
	}
	timestamp, err := time.Parse("20060102T150405Z0700", rest[:separator])
	if err != nil {
		return time.Time{}, false
	}
	return timestamp, true
}

func (p *heapProfileCollector) currentTime() time.Time {
	if p.now != nil {
		return p.now()
	}
	return now()
}
