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
	heapProfileLevel70Milli    int64 = 700
	heapProfileLevel80Milli    int64 = 800
	heapProfileLevel85Milli    int64 = 850
	heapProfileResetMilli      int64 = 650
	heapProfileCutoffMilli     int64 = 900
	heapProfileMinInterval           = time.Minute
	heapProfileMaxGroups             = 4
	heapProfileDirName               = "heap_profiles"
	heapProfileTimestampLayout       = "2006-01-02T15-04-05Z0700"
	heapProfileMetadataSuffix        = ".meta.json"
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
	lastCaptureAt        time.Time
	lastLimit            int64
	lastCaptureThreshold int
	attempted            uint32
	closed               bool
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
	HeapAlloc  int64 `json:"heap_alloc_bytes"`
	HeapInuse  int64 `json:"heap_inuse_bytes"`
	MemInuse   int64 `json:"mem_inuse_bytes"`
	QuotaAlloc int64 `json:"quota_alloc_bytes"`
	Limit      int64 `json:"limit_bytes"`
}

type heapProfileMetadata struct {
	StartTime    string                   `json:"start_time"`
	Version      int                      `json:"version"`
	ThresholdPct int                      `json:"threshold_pct"`
	DurationMs   int64                    `json:"duration_ms"`
	StartState   heapProfileMetadataState `json:"start_state"`
}

type heapProfileFileGroup struct {
	captureTime  time.Time
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
	if !state.lastCaptureAt.IsZero() &&
		level.threshold <= state.lastCaptureThreshold &&
		p.currentTime().Sub(state.lastCaptureAt) < heapProfileMinInterval {
		return
	}

	if p.capture(m, level.threshold) {
		state.attempted |= reachedMask
	}
}

// capture returns true once writeProfile has been called, even if the profile
// cannot be persisted afterward.
func (p *heapProfileCollector) capture(m *MemArbitrator, threshold int) bool {
	snapshot := m.heapProfileSnapshot()
	if m.WorkMode() == ArbitratorModeDisable ||
		m.AtMemRisk() ||
		snapshot.Limit <= 0 ||
		snapshot.MemInuse >= snapshot.captureCutoff ||
		p.writeProfile == nil {
		return false
	}

	// Record the attempt before file initialization so failures do not retry on
	// every runtime tick.
	p.trigger.lastCaptureAt = p.currentTime()
	p.trigger.lastCaptureThreshold = threshold
	if err := os.MkdirAll(p.dir, 0750); err != nil {
		return false
	}

	tmp, err := os.CreateTemp(p.dir, ".heap-profile.*.tmp")
	if err != nil {
		return false
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
		return false
	}

	startTime := p.currentTime()
	p.trigger.lastCaptureAt = startTime
	timestamp := startTime.Format(heapProfileTimestampLayout)
	base := timestamp + "." + strconv.Itoa(threshold) + "pct"
	profilePath := filepath.Join(p.dir, base+".pprof")

	if err := p.writeProfile(tmp); err != nil {
		return true
	}
	if err := tmp.Close(); err != nil {
		return true
	}
	closed = true
	if err := os.Rename(tmpPath, profilePath); err != nil {
		return true
	}
	renamed = true

	metadata := heapProfileMetadata{
		Version:      1,
		StartTime:    startTime.Format(time.RFC3339),
		ThresholdPct: threshold,
		StartState:   snapshot.heapProfileMetadataState,
		DurationMs:   p.currentTime().Sub(startTime).Milliseconds(),
	}
	_ = p.writeMetadataAtomically(base+heapProfileMetadataSuffix, metadata)
	p.enforceRetention()
	return true
}

func (p *heapProfileCollector) writeMetadataAtomically(name string, metadata heapProfileMetadata) error {
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
	encoder := json.NewEncoder(tmp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(metadata); err != nil {
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
		base, captureTime, isProfile, ok := parseHeapProfileFileName(name)
		if !ok {
			continue
		}

		group := groups[base]
		if group == nil {
			group = &heapProfileFileGroup{base: base, captureTime: captureTime}
			groups[base] = group
		}
		path := filepath.Join(p.dir, name)
		if isProfile {
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
		if !profiles[i].captureTime.Equal(profiles[j].captureTime) {
			return profiles[i].captureTime.Before(profiles[j].captureTime)
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

func parseHeapProfileFileName(name string) (base string, captureTime time.Time, isProfile, ok bool) {
	switch {
	case strings.HasSuffix(name, ".pprof"):
		base = strings.TrimSuffix(name, ".pprof")
		isProfile = true
	case strings.HasSuffix(name, heapProfileMetadataSuffix):
		base = strings.TrimSuffix(name, heapProfileMetadataSuffix)
	default:
		return "", time.Time{}, false, false
	}

	separator := strings.LastIndexByte(base, '.')
	if separator <= 0 {
		return "", time.Time{}, false, false
	}
	thresholdText := strings.TrimSuffix(base[separator+1:], "pct")
	if thresholdText == base[separator+1:] {
		return "", time.Time{}, false, false
	}
	threshold, err := strconv.Atoi(thresholdText)
	if err != nil || strconv.Itoa(threshold) != thresholdText || !isHeapProfileThreshold(threshold) {
		return "", time.Time{}, false, false
	}

	captureTime, err = time.Parse(heapProfileTimestampLayout, base[:separator])
	if err != nil {
		return "", time.Time{}, false, false
	}
	return base, captureTime, isProfile, true
}

func isHeapProfileThreshold(threshold int) bool {
	for _, level := range heapProfileLevels {
		if level.threshold == threshold {
			return true
		}
	}
	return false
}

func (p *heapProfileCollector) currentTime() time.Time {
	if p.now != nil {
		return p.now()
	}
	return now()
}
