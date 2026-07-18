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

package export

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
)

const (
	maxCSEDumperStderrSize    = 64 << 10
	cseDumperObservabilityEnv = "CSE_DUMPER_OBSERVABILITY_V1"
	cseDumperStagePrefix      = "CSE_DUMPER_STAGE_V1 "
	cseDumperStatsPrefix      = "CSE_DUMPER_STATS_V1 "
)

type cseDumperStderr struct {
	mu        sync.Mutex
	data      []byte
	truncated bool
}

func (w *cseDumperStderr) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	written := len(data)
	if len(data) >= maxCSEDumperStderrSize {
		w.data = append(w.data[:0], data[len(data)-maxCSEDumperStderrSize:]...)
		w.truncated = true
		return written, nil
	}
	overflow := len(w.data) + len(data) - maxCSEDumperStderrSize
	if overflow > 0 {
		copy(w.data, w.data[overflow:])
		w.data = w.data[:len(w.data)-overflow]
		w.truncated = true
	}
	w.data = append(w.data, data...)
	return written, nil
}

func (w *cseDumperStderr) snapshot() (data []byte, truncated bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return bytes.Clone(w.data), w.truncated
}

type cseDumperMachineStats struct {
	Version                    int     `json:"version"`
	BuildVersion               string  `json:"build_version"`
	BuildGitHash               string  `json:"build_git_hash"`
	BuildProfile               string  `json:"build_profile"`
	Success                    bool    `json:"success"`
	Stage                      string  `json:"stage"`
	ScanStage                  string  `json:"scan_stage"`
	ScanShardID                uint64  `json:"scan_shard_id"`
	ScanShardVersion           uint64  `json:"scan_shard_ver"`
	ScanCompleted              bool    `json:"scan_completed"`
	KeyspaceID                 uint32  `json:"keyspace_id"`
	BackupTS                   uint64  `json:"backup_ts"`
	ManifestBytes              uint64  `json:"manifest_bytes"`
	ManifestShards             uint64  `json:"manifest_shards"`
	ContentFiles               uint64  `json:"content_files"`
	PlaintextShards            uint64  `json:"plaintext_shards"`
	LegacyShards               uint64  `json:"legacy_shards"`
	CMEKShards                 uint64  `json:"cmek_shards"`
	MixedEncryptionShards      uint64  `json:"mixed_encryption_shards"`
	ShardsScanned              uint64  `json:"shards_scanned"`
	SlowShards                 uint64  `json:"slow_shards"`
	ScannedPlaintextShards     uint64  `json:"scanned_plaintext_shards"`
	ScannedLegacyShards        uint64  `json:"scanned_legacy_shards"`
	ScannedCMEKShards          uint64  `json:"scanned_cmek_shards"`
	ScannedMixedShards         uint64  `json:"scanned_mixed_encryption_shards"`
	L0Candidates               uint64  `json:"l0_candidates"`
	L0Retained                 uint64  `json:"l0_retained"`
	LNCandidates               uint64  `json:"ln_candidates"`
	LNRetained                 uint64  `json:"ln_retained"`
	MissingSSTBounds           uint64  `json:"missing_sst_bounds"`
	SSTCandidateBytesHint      uint64  `json:"sst_candidate_bytes_hint"`
	SSTRetainedBytesHint       uint64  `json:"sst_retained_bytes_hint"`
	ContentReadAttempts        uint64  `json:"content_read_attempts"`
	ContentReads               uint64  `json:"content_reads"`
	ContentReadFailures        uint64  `json:"content_read_failures"`
	SlowContentReads           uint64  `json:"slow_content_reads"`
	ContentBytes               uint64  `json:"content_bytes"`
	Rows                       uint64  `json:"rows"`
	KeyBytes                   uint64  `json:"key_bytes"`
	ValueBytes                 uint64  `json:"value_bytes"`
	StdoutBytes                uint64  `json:"stdout_bytes"`
	StdoutWriteCalls           uint64  `json:"stdout_write_calls"`
	StdoutWriteFailures        uint64  `json:"stdout_write_failures"`
	LegacyEncryption           bool    `json:"legacy_encryption"`
	FirstRowProducedNanos      *uint64 `json:"first_row_produced_ns"`
	FirstStdoutWriteNanos      *uint64 `json:"first_stdout_write_ns"`
	ScanFirstRowNanos          *uint64 `json:"scan_first_row_ns"`
	ManifestLoadNanos          uint64  `json:"manifest_load_ns"`
	LegacyKeyLoadNanos         uint64  `json:"legacy_key_load_ns"`
	ReaderInitNanos            uint64  `json:"reader_init_ns"`
	RangeFilterNanos           uint64  `json:"range_filter_ns"`
	SnapshotLoadNanos          uint64  `json:"snapshot_load_ns"`
	IteratorInitNanos          uint64  `json:"iterator_init_ns"`
	PlaintextSnapshotLoadNanos uint64  `json:"plaintext_snapshot_load_ns"`
	LegacySnapshotLoadNanos    uint64  `json:"legacy_snapshot_load_ns"`
	CMEKSnapshotLoadNanos      uint64  `json:"cmek_snapshot_load_ns"`
	MixedSnapshotLoadNanos     uint64  `json:"mixed_encryption_snapshot_load_ns"`
	ContentReadNanos           uint64  `json:"content_read_ns"`
	MaxContentReadNanos        uint64  `json:"max_content_read_ns"`
	IterateEmitNanos           uint64  `json:"iterate_emit_ns"`
	MaxIterateEmitNanos        uint64  `json:"max_iterate_emit_ns"`
	MaxRangeFilterNanos        uint64  `json:"max_range_filter_ns"`
	MaxSnapshotLoadNanos       uint64  `json:"max_snapshot_load_ns"`
	MaxIteratorInitNanos       uint64  `json:"max_iterator_init_ns"`
	StdoutWriteNanos           uint64  `json:"stdout_write_ns"`
	MaxStdoutWriteNanos        uint64  `json:"max_stdout_write_ns"`
	ScanNanos                  uint64  `json:"scan_ns"`
	FlushNanos                 uint64  `json:"flush_ns"`
	TotalNanos                 uint64  `json:"total_ns"`
	PeakRSSBytes               *uint64 `json:"peak_rss_bytes"`
}

func parseCSEDumperMachineStats(data []byte) (cseDumperMachineStats, bool, error) {
	var stats cseDumperMachineStats
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		payload, ok := bytes.CutPrefix(line, []byte(cseDumperStatsPrefix))
		if !ok {
			continue
		}
		if err := json.Unmarshal(payload, &stats); err != nil {
			return cseDumperMachineStats{}, true, errors.Annotate(err, "decode cse-ctl dumper statistics")
		}
		if stats.Version != 1 {
			return cseDumperMachineStats{}, true, errors.Errorf(
				"unsupported cse-ctl dumper statistics version %d", stats.Version)
		}
		return stats, true, nil
	}
	return cseDumperMachineStats{}, false, nil
}

func cseDumperStage(data []byte) string {
	var stage string
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if value, ok := bytes.CutPrefix(line, []byte(cseDumperStagePrefix)); ok {
			stage = string(bytes.TrimSpace(value))
		}
	}
	return stage
}

func cseDumperHumanStderr(data []byte) []byte {
	lines := make([][]byte, 0)
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if bytes.HasPrefix(line, []byte(cseDumperStagePrefix)) ||
			bytes.HasPrefix(line, []byte(cseDumperStatsPrefix)) {
			continue
		}
		lines = append(lines, line)
	}
	return bytes.TrimSpace(bytes.Join(lines, []byte{'\n'}))
}

type cseDumperScanStats struct {
	pid             int
	duration        time.Duration
	processSpawn    time.Duration
	firstRowLatency time.Duration
	rows            uint64
	keyBytes        uint64
	valueBytes      uint64
	protocolRead    time.Duration
	maxProtocolRead time.Duration
	processWait     time.Duration
	childUserCPU    time.Duration
	childSystemCPU  time.Duration
	exitCode        *int
	stderrTruncated bool
	cseStage        string
	cse             cseDumperMachineStats
	hasCSEStats     bool
	cseStatsError   string
}

type cseDumperScanObserver struct {
	started  func(*cseDumperScan)
	finished func(*cseDumperScan, error)
}

type cseDumperProcessObservation struct {
	startedAt            time.Time
	processSpawnDuration time.Duration
	firstRowLatencyNano  atomic.Int64
	rows                 atomic.Uint64
	keyBytes             atomic.Uint64
	valueBytes           atomic.Uint64
	protocolReadNano     atomic.Int64
	maxProtocolReadNano  atomic.Int64
	processWaitNano      atomic.Int64
	slowLogIndex         atomic.Uint64
}

func newCSEDumperProcessObservation() cseDumperProcessObservation {
	return cseDumperProcessObservation{startedAt: time.Now()}
}

func (o *cseDumperProcessObservation) recordSpawn() {
	o.processSpawnDuration = time.Since(o.startedAt)
}

func (o *cseDumperProcessObservation) readRow(
	input *bufio.Reader,
	keyBuffer, valueBuffer []byte,
) (key, value []byte, end bool, err error) {
	startedAt := time.Now()
	key, value, end, err = readPackedRow(input, keyBuffer, valueBuffer)
	duration := time.Since(startedAt)
	o.protocolReadNano.Add(duration.Nanoseconds())
	updatePackedMaxDuration(&o.maxProtocolReadNano, duration)
	if err == nil && !end {
		if o.rows.Load() == 0 {
			o.firstRowLatencyNano.Store(time.Since(o.startedAt).Nanoseconds())
		}
		o.rows.Add(1)
		o.keyBytes.Add(uint64(len(key)))
		o.valueBytes.Add(uint64(len(value)))
	}
	return key, value, end, err
}

func (o *cseDumperProcessObservation) wait(wait func() error) error {
	startedAt := time.Now()
	err := wait()
	o.processWaitNano.Store(time.Since(startedAt).Nanoseconds())
	return err
}

func (s *cseDumperScan) pid() int {
	if s.cmd.Process != nil {
		return s.cmd.Process.Pid
	}
	return 0
}

func (s *cseDumperScan) liveStats() cseDumperScanStats {
	stderr, _ := s.stderr.snapshot()
	observation := &s.observation
	return cseDumperScanStats{
		pid:             s.pid(),
		duration:        time.Since(observation.startedAt),
		processSpawn:    observation.processSpawnDuration,
		firstRowLatency: time.Duration(observation.firstRowLatencyNano.Load()),
		rows:            observation.rows.Load(),
		keyBytes:        observation.keyBytes.Load(),
		valueBytes:      observation.valueBytes.Load(),
		protocolRead:    time.Duration(observation.protocolReadNano.Load()),
		maxProtocolRead: time.Duration(observation.maxProtocolReadNano.Load()),
		processWait:     time.Duration(observation.processWaitNano.Load()),
		cseStage:        cseDumperStage(stderr),
	}
}

func (s *cseDumperScan) stats() cseDumperScanStats {
	stats := s.liveStats()
	stderr, truncated := s.stderr.snapshot()
	stats.stderrTruncated = truncated
	cseStats, found, err := parseCSEDumperMachineStats(stderr)
	if err != nil {
		stats.cseStatsError = err.Error()
	} else if found {
		stats.cse = cseStats
		stats.hasCSEStats = true
	}
	if state := s.cmd.ProcessState; state != nil {
		stats.childUserCPU = state.UserTime()
		stats.childSystemCPU = state.SystemTime()
		exitCode := state.ExitCode()
		stats.exitCode = &exitCode
	}
	return stats
}

func (s *cseDumperScan) diagnostics() string {
	stderr, _ := s.stderr.snapshot()
	_, hasCSEStats, _ := parseCSEDumperMachineStats(stderr)
	var selected []string
	for _, line := range strings.Split(string(stderr), "\n") {
		if (!hasCSEStats && strings.Contains(line, "packed dumper scan finished")) ||
			strings.Contains(line, "packed dumper retained SSTs without complete bounds") ||
			strings.Contains(line, "slow packed backup shard scan finished") ||
			strings.Contains(line, "slow packed backup content read") {
			selected = append(selected, line)
		}
	}
	return strings.Join(selected, "\n")
}
