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

package stmtctx

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// ModelInferenceRole indicates how the model function is used in the plan.
type ModelInferenceRole string

const (
	// ModelInferenceRolePredicate indicates model inference is used in a predicate.
	ModelInferenceRolePredicate ModelInferenceRole = "predicate"
	// ModelInferenceRoleProjection indicates model inference is used in a projection.
	ModelInferenceRoleProjection ModelInferenceRole = "projection"
)

type modelInferenceKey struct {
	planID    int
	modelID   int64
	versionID int64
	role      ModelInferenceRole
}

type modelInferenceEntry struct {
	calls          int64
	errors         int64
	totalInferTime time.Duration
	totalBatchSize int64
	maxBatchSize   int64
	totalLoadTime  time.Duration
	loadErrors     int64
}

// ModelInferenceSummary exposes aggregated model inference stats for a statement.
type ModelInferenceSummary struct {
	PlanID             int
	ModelID            int64
	VersionID          int64
	Role               ModelInferenceRole
	Calls              int64
	Errors             int64
	TotalInferenceTime time.Duration
	TotalBatchSize     int64
	MaxBatchSize       int64
	TotalLoadTime      time.Duration
	LoadErrors         int64
}

// AvgBatchSize returns the average batch size for the inference summary.
func (s ModelInferenceSummary) AvgBatchSize() float64 {
	if s.Calls == 0 {
		return 0
	}
	return float64(s.TotalBatchSize) / float64(s.Calls)
}

// ModelInferenceStats aggregates inference stats for a statement.
type ModelInferenceStats struct {
	mu      sync.Mutex
	entries map[modelInferenceKey]*modelInferenceEntry
}

// NewModelInferenceStats creates a new ModelInferenceStats instance.
func NewModelInferenceStats() *ModelInferenceStats {
	return &ModelInferenceStats{
		entries: make(map[modelInferenceKey]*modelInferenceEntry),
	}
}

// RecordInference records a model inference invocation.
func (s *ModelInferenceStats) RecordInference(planID int, modelID, versionID int64, role ModelInferenceRole, batchSize int, duration time.Duration, err error) {
	if s == nil {
		return
	}
	if batchSize < 0 {
		batchSize = 0
	}
	key := modelInferenceKey{planID: planID, modelID: modelID, versionID: versionID, role: role}
	s.mu.Lock()
	entry := s.entries[key]
	if entry == nil {
		entry = &modelInferenceEntry{}
		s.entries[key] = entry
	}
	entry.calls++
	if err != nil {
		entry.errors++
	}
	entry.totalInferTime += duration
	entry.totalBatchSize += int64(batchSize)
	if int64(batchSize) > entry.maxBatchSize {
		entry.maxBatchSize = int64(batchSize)
	}
	s.mu.Unlock()
}

// RecordLoad records a model load attempt.
func (s *ModelInferenceStats) RecordLoad(planID int, modelID, versionID int64, role ModelInferenceRole, duration time.Duration, err error) {
	if s == nil {
		return
	}
	key := modelInferenceKey{planID: planID, modelID: modelID, versionID: versionID, role: role}
	s.mu.Lock()
	entry := s.entries[key]
	if entry == nil {
		entry = &modelInferenceEntry{}
		s.entries[key] = entry
	}
	entry.totalLoadTime += duration
	if err != nil {
		entry.loadErrors++
	}
	s.mu.Unlock()
}

// PlanSummaries returns per-plan summaries for explain analyze.
func (s *ModelInferenceStats) PlanSummaries() map[int][]ModelInferenceSummary {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int][]ModelInferenceSummary, len(s.entries))
	for key, entry := range s.entries {
		summary := ModelInferenceSummary{
			PlanID:             key.planID,
			ModelID:            key.modelID,
			VersionID:          key.versionID,
			Role:               key.role,
			Calls:              entry.calls,
			Errors:             entry.errors,
			TotalInferenceTime: entry.totalInferTime,
			TotalBatchSize:     entry.totalBatchSize,
			MaxBatchSize:       entry.maxBatchSize,
			TotalLoadTime:      entry.totalLoadTime,
			LoadErrors:         entry.loadErrors,
		}
		out[key.planID] = append(out[key.planID], summary)
	}
	for planID, summaries := range out {
		sort.Slice(summaries, func(i, j int) bool {
			if summaries[i].ModelID != summaries[j].ModelID {
				return summaries[i].ModelID < summaries[j].ModelID
			}
			if summaries[i].VersionID != summaries[j].VersionID {
				return summaries[i].VersionID < summaries[j].VersionID
			}
			return summaries[i].Role < summaries[j].Role
		})
		out[planID] = summaries
	}
	return out
}

// SlowLogSummaries returns summaries merged across plan IDs for slow logs.
func (s *ModelInferenceStats) SlowLogSummaries() []ModelInferenceSummary {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	type slowLogKey struct {
		modelID   int64
		versionID int64
		role      ModelInferenceRole
	}
	rollup := make(map[slowLogKey]*modelInferenceEntry, len(s.entries))
	for key, entry := range s.entries {
		sKey := slowLogKey{modelID: key.modelID, versionID: key.versionID, role: key.role}
		agg := rollup[sKey]
		if agg == nil {
			agg = &modelInferenceEntry{}
			rollup[sKey] = agg
		}
		agg.calls += entry.calls
		agg.errors += entry.errors
		agg.totalInferTime += entry.totalInferTime
		agg.totalBatchSize += entry.totalBatchSize
		if entry.maxBatchSize > agg.maxBatchSize {
			agg.maxBatchSize = entry.maxBatchSize
		}
		agg.totalLoadTime += entry.totalLoadTime
		agg.loadErrors += entry.loadErrors
	}
	out := make([]ModelInferenceSummary, 0, len(rollup))
	for key, entry := range rollup {
		out = append(out, ModelInferenceSummary{
			PlanID:             0,
			ModelID:            key.modelID,
			VersionID:          key.versionID,
			Role:               key.role,
			Calls:              entry.calls,
			Errors:             entry.errors,
			TotalInferenceTime: entry.totalInferTime,
			TotalBatchSize:     entry.totalBatchSize,
			MaxBatchSize:       entry.maxBatchSize,
			TotalLoadTime:      entry.totalLoadTime,
			LoadErrors:         entry.loadErrors,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ModelID != out[j].ModelID {
			return out[i].ModelID < out[j].ModelID
		}
		if out[i].VersionID != out[j].VersionID {
			return out[i].VersionID < out[j].VersionID
		}
		return out[i].Role < out[j].Role
	})
	return out
}

type modelInferenceSlowLogEntry struct {
	ModelID    int64              `json:"model_id"`
	VersionID  int64              `json:"version_id"`
	Role       ModelInferenceRole `json:"role"`
	Calls      int64              `json:"calls"`
	Errors     int64              `json:"errors"`
	TotalMS    float64            `json:"total_ms"`
	AvgBatch   float64            `json:"avg_batch"`
	MaxBatch   int64              `json:"max_batch"`
	LoadMS     float64            `json:"load_ms"`
	LoadErrors int64              `json:"load_errors"`
}

// SlowLogJSON returns JSON payload for slow log.
func (s *ModelInferenceStats) SlowLogJSON() (string, error) {
	summaries := s.SlowLogSummaries()
	if len(summaries) == 0 {
		return "", nil
	}
	entries := make([]modelInferenceSlowLogEntry, 0, len(summaries))
	for _, summary := range summaries {
		entries = append(entries, modelInferenceSlowLogEntry{
			ModelID:    summary.ModelID,
			VersionID:  summary.VersionID,
			Role:       summary.Role,
			Calls:      summary.Calls,
			Errors:     summary.Errors,
			TotalMS:    summary.TotalInferenceTime.Seconds() * 1000,
			AvgBatch:   summary.AvgBatchSize(),
			MaxBatch:   summary.MaxBatchSize,
			LoadMS:     summary.TotalLoadTime.Seconds() * 1000,
			LoadErrors: summary.LoadErrors,
		})
	}
	payload, err := json.Marshal(entries)
	if err != nil {
		return "", err
	}
	return string(payload), nil
}
