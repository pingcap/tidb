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

package service

import (
	"fmt"
	"maps"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/br/pkg/stream/crr/internal/checkpoint"
)

const (
	stateStarting = "starting"
	stateRunning  = "running"
	stateDegraded = "degraded"
	stateStopped  = "stopped"

	phaseIdle = "idle"
)

func normalizeStorageSubDir(subDir string) (string, error) {
	trimmed := strings.Trim(subDir, "/")
	if trimmed == "" {
		return "", fmt.Errorf("state storage subdir must not be empty")
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("state storage subdir must stay within upstream storage, got %q", subDir)
	}
	return cleaned, nil
}

func GetStatusFileName(subDir string) (string, error) {
	normalizedSubDir, err := normalizeStorageSubDir(subDir)
	if err != nil {
		return "", err
	}
	return path.Join(normalizedSubDir, "resume-state.json"), nil
}

// StatusStatistic summarizes the current round's file-related work.
type StatusStatistic struct {
	UpstreamReadMetaFileCount       int            `json:"upstream_read_meta_file_count"`
	EstimatedSyncLogFileCount       int            `json:"estimated_sync_log_file_count"`
	DownstreamCheckFileCount        int            `json:"downstream_check_file_count"`
	PlannedFileSuffixCounts         map[string]int `json:"planned_file_suffix_counts,omitempty"`
	DownstreamCheckFileSuffixCounts map[string]int `json:"downstream_check_file_suffix_counts,omitempty"`
}

// StatusSnapshot is the externally visible CRR worker status.
type StatusSnapshot struct {
	TaskName string `json:"task_name"`

	Live  bool   `json:"live"`
	Ready bool   `json:"ready"`
	State string `json:"state"`
	Phase string `json:"phase"`

	CurrentRound      uint64 `json:"current_round"`
	LastLoopIteration uint64 `json:"last_loop_iteration"`

	LastUpstreamCheckpoint uint64 `json:"last_upstream_checkpoint"`
	SafeCheckpoint         uint64 `json:"safe_checkpoint"`
	SyncedTS               uint64 `json:"synced_ts"`

	AliveStoreCount  int             `json:"alive_store_count"`
	PendingFileCount int             `json:"pending_file_count"`
	Statistic        StatusStatistic `json:"statistic"`

	LastSuccessTime     time.Time `json:"last_success_time"`
	LastError           string    `json:"last_error,omitempty"`
	LastErrorTime       time.Time `json:"last_error_time"`
	ConsecutiveFailures uint64    `json:"consecutive_failures"`
	LastEventTime       time.Time `json:"last_event_time"`
}

type statusStore struct {
	mu       sync.RWMutex
	snapshot StatusSnapshot
}

func newStatusStore(taskName string) *statusStore {
	s := &statusStore{
		snapshot: StatusSnapshot{
			TaskName: taskName,
			State:    stateStarting,
			Phase:    phaseIdle,
		},
	}
	observeStatusMetrics(&s.snapshot)
	return s
}

func (s *statusStore) start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Live = true
	s.snapshot.Ready = true
	s.snapshot.State = stateRunning
	s.snapshot.Phase = phaseIdle
	observeStatusMetrics(&s.snapshot)
}

func (s *statusStore) stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.Live = false
	s.snapshot.Ready = false
	s.snapshot.State = stateStopped
	observeStatusMetrics(&s.snapshot)
}

func (s *statusStore) setPersistentState(state PersistentState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.SafeCheckpoint = state.LastCheckpoint
	s.snapshot.SyncedTS = state.SyncedTS
	observeStatusMetrics(&s.snapshot)
}

func (s *statusStore) clearFailure() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.State == stateStopped {
		return
	}
	s.snapshot.Ready = true
	s.snapshot.State = stateRunning
	s.snapshot.LastError = ""
	s.snapshot.LastErrorTime = time.Time{}
	s.snapshot.ConsecutiveFailures = 0
	observeStatusMetrics(&s.snapshot)
}

func (s *statusStore) beginRound() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot.CurrentRound++
	s.snapshot.LastLoopIteration = 0
	s.snapshot.PendingFileCount = 0
	s.snapshot.Phase = phaseIdle
	observeStatusMetrics(&s.snapshot)
	return s.snapshot.CurrentRound
}

func (s *statusStore) applyEvent(event checkpoint.CheckpointEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.snapshot.LastEventTime = event.Time
	s.snapshot.Phase = string(event.Type)
	s.snapshot.LastLoopIteration = event.LoopIteration
	if event.UpstreamCheckpoint > 0 {
		s.snapshot.LastUpstreamCheckpoint = event.UpstreamCheckpoint
	}
	if event.SafeCheckpoint > 0 {
		s.snapshot.SafeCheckpoint = event.SafeCheckpoint
	}
	if event.SyncedTS > 0 {
		s.snapshot.SyncedTS = event.SyncedTS
	}
	if event.Type == checkpoint.EventRoundPlanned || event.Type == checkpoint.EventCheckpointAdvanced {
		s.snapshot.AliveStoreCount = event.AliveStoreCount
	}
	s.snapshot.PendingFileCount = event.PendingFileCount
	if event.Statistic != nil {
		s.snapshot.Statistic = newStatusStatistic(*event.Statistic)
	}

	switch event.Type {
	case checkpoint.EventCheckpointAdvanced:
		s.snapshot.Ready = true
		s.snapshot.State = stateRunning
		s.snapshot.LastSuccessTime = event.Time
		s.snapshot.LastError = ""
		s.snapshot.LastErrorTime = time.Time{}
		s.snapshot.ConsecutiveFailures = 0
	case checkpoint.EventCalculationFailed:
		s.snapshot.Ready = false
		s.snapshot.State = stateDegraded
		if event.Err != nil {
			s.snapshot.LastError = event.Err.Error()
		}
		s.snapshot.LastErrorTime = event.Time
		s.snapshot.ConsecutiveFailures++
	default:
		if s.snapshot.State != stateDegraded {
			s.snapshot.State = stateRunning
		}
	}
	observeStatusMetrics(&s.snapshot)
}

func (s *statusStore) snapshotCopy() StatusSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := s.snapshot
	snapshot.Statistic.PlannedFileSuffixCounts = maps.Clone(snapshot.Statistic.PlannedFileSuffixCounts)
	snapshot.Statistic.DownstreamCheckFileSuffixCounts = maps.Clone(snapshot.Statistic.DownstreamCheckFileSuffixCounts)
	return snapshot
}

type statusObserver struct {
	status *statusStore
}

func newStatusObserver(status *statusStore) *statusObserver {
	return &statusObserver{status: status}
}

func (o *statusObserver) BeginCalculationRound() uint64 {
	return o.status.beginRound()
}

func (o *statusObserver) OnCheckpointEvent(event checkpoint.CheckpointEvent) {
	o.status.applyEvent(event)
}

func newStatusStatistic(stat checkpoint.FileStatistic) StatusStatistic {
	return StatusStatistic{
		UpstreamReadMetaFileCount:       stat.UpstreamReadMetaFileCount,
		EstimatedSyncLogFileCount:       stat.EstimatedSyncLogFileCount,
		DownstreamCheckFileCount:        stat.DownstreamCheckFileCount,
		PlannedFileSuffixCounts:         maps.Clone(stat.PlannedFileSuffixCounts),
		DownstreamCheckFileSuffixCounts: maps.Clone(stat.DownstreamCheckFileSuffixCounts),
	}
}
