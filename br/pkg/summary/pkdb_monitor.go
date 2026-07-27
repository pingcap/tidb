// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package summary

import (
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
)

var monitor = newTaskMonitor(log.Info)

// InitMonitor initializes the task monitor logger.
func InitMonitor() {
	monitor = newTaskMonitor(Log)
}

// TaskStatusConfig configures BR task status textfile export.
type TaskStatusConfig struct {
	TaskType          string
	Command           string
	Scope             string
	PDAddrs           []string
	Storage           string
	TaskTS            uint64
	StatusTextfileDir string
}

type taskMonitor struct {
	mu            sync.Mutex
	taskStatus    *taskStatus
	successStatus bool
	failureUnits  map[string]struct{}
	log           logFunc
}

func newTaskMonitor(logf logFunc) *taskMonitor {
	return &taskMonitor{
		failureUnits: make(map[string]struct{}),
		log:          logf,
	}
}

func (m *taskMonitor) startTask(cfg TaskStatusConfig) {
	status := newTaskStatus(cfg)
	m.mu.Lock()
	m.taskStatus = status
	m.successStatus = false
	m.failureUnits = make(map[string]struct{})
	snapshot := status.snapshot()
	m.mu.Unlock()
	m.persistTaskStatusSnapshot(snapshot)
}

func (m *taskMonitor) setTaskPhase(phase string) {
	m.mu.Lock()
	if m.taskStatus == nil {
		m.mu.Unlock()
		return
	}
	snapshot := m.taskStatus.setPhase(phase, time.Now())
	m.mu.Unlock()
	m.persistTaskStatusSnapshot(snapshot)
}

func (m *taskMonitor) recordTaskSize(name string, arg any) {
	if name != BackupDataSize && name != RestoreDataSize {
		return
	}
	size, ok := arg.(uint64)
	if !ok {
		return
	}

	m.mu.Lock()
	if m.taskStatus == nil {
		m.mu.Unlock()
		return
	}
	snapshot := m.taskStatus.addSize(size, time.Now())
	m.mu.Unlock()
	m.persistTaskStatusSnapshot(snapshot)
}

func (m *taskMonitor) recordTaskFailure(name string) {
	m.mu.Lock()
	if m.taskStatus == nil {
		m.mu.Unlock()
		return
	}
	var snapshot *taskStatus
	if _, ok := m.failureUnits[name]; !ok {
		m.failureUnits[name] = struct{}{}
		snapshot = m.taskStatus.addFailure(time.Now())
	}
	m.mu.Unlock()
	m.persistTaskStatusSnapshot(snapshot)
}

func (m *taskMonitor) setTaskSuccess(success bool) {
	m.mu.Lock()
	if m.taskStatus == nil {
		m.mu.Unlock()
		return
	}
	m.successStatus = success
	snapshot := m.taskStatus.setSuccess(success, time.Now())
	m.mu.Unlock()
	m.persistTaskStatusSnapshot(snapshot)
}

func (m *taskMonitor) finishTask() {
	m.mu.Lock()
	if m.taskStatus == nil {
		m.mu.Unlock()
		return
	}
	success := len(m.failureUnits) == 0 && m.successStatus
	snapshot := m.taskStatus.finish(success, time.Now())
	m.taskStatus = nil
	m.mu.Unlock()
	m.exportFinalTaskStatus(snapshot)
}

func (m *taskMonitor) persistTaskStatusSnapshot(snapshot *taskStatus) {
	if err := writeTaskStatusSnapshot(snapshot); err != nil {
		log.Warn("failed to write BR task status metrics to textfile", zap.Error(err))
	}
}

func (m *taskMonitor) exportFinalTaskStatus(snapshot *taskStatus) {
	if snapshot == nil || snapshot.textfileDir == "" {
		return
	}

	if err := writeTaskStatusSnapshot(snapshot); err != nil {
		log.Warn("failed to write final BR task status metrics to textfile", zap.Error(err))
		return
	}
	m.log("BR task status metrics exported", zap.String("file", filepath.Join(snapshot.textfileDir, snapshot.fileName)))
}

type taskStatus struct {
	statusID    string
	taskType    string
	command     string
	scope       string
	pdAddrs     string
	storage     string
	taskTS      string
	textfileDir string
	fileName    string

	phase          string
	startTime      time.Time
	lastUpdateTime time.Time
	endTime        time.Time
	running        bool
	success        bool
	finished       bool
	sizeBytes      uint64
}

func newTaskStatus(cfg TaskStatusConfig) *taskStatus {
	if cfg.StatusTextfileDir == "" {
		return nil
	}
	now := time.Now()
	startTime := strconv.FormatInt(now.UnixNano(), 10)
	pdAddrs := normalizeTaskStatusPDAddrs(cfg.PDAddrs)
	storage := ast.RedactURL(cfg.Storage)
	taskTS := strconv.FormatUint(cfg.TaskTS, 10)
	statusID := uuid.NewString()
	return &taskStatus{
		statusID:       statusID,
		taskType:       cfg.TaskType,
		command:        cfg.Command,
		scope:          cfg.Scope,
		pdAddrs:        pdAddrs,
		storage:        storage,
		taskTS:         taskTS,
		textfileDir:    cfg.StatusTextfileDir,
		fileName:       "br-" + statusID + "-" + startTime + ".prom",
		startTime:      now,
		lastUpdateTime: now,
		running:        true,
	}
}

func normalizeTaskStatusPDAddrs(pdAddrs []string) string {
	if len(pdAddrs) == 0 {
		return ""
	}
	normalized := append([]string(nil), pdAddrs...)
	sort.Strings(normalized)
	return strings.Join(normalized, ",")
}

func (s *taskStatus) snapshot() *taskStatus {
	if s == nil {
		return nil
	}
	snapshot := *s
	return &snapshot
}

func (s *taskStatus) setPhase(phase string, now time.Time) *taskStatus {
	if s == nil {
		return nil
	}
	s.phase = phase
	s.lastUpdateTime = now
	return s.snapshot()
}

func (s *taskStatus) addSize(size uint64, now time.Time) *taskStatus {
	if s == nil {
		return nil
	}
	s.sizeBytes += size
	s.lastUpdateTime = now
	return s.snapshot()
}

func (s *taskStatus) addFailure(now time.Time) *taskStatus {
	if s == nil {
		return nil
	}
	s.lastUpdateTime = now
	return s.snapshot()
}

func (s *taskStatus) setSuccess(success bool, now time.Time) *taskStatus {
	if s == nil {
		return nil
	}
	s.success = success
	s.lastUpdateTime = now
	return s.snapshot()
}

func (s *taskStatus) finish(success bool, now time.Time) *taskStatus {
	if s == nil {
		return nil
	}
	s.running = false
	s.finished = true
	s.success = success
	s.endTime = now
	s.lastUpdateTime = now
	if success {
		s.phase = "success"
	} else {
		s.phase = "failed"
	}
	return s.snapshot()
}

func writeTaskStatusSnapshot(snapshot *taskStatus) (err error) {
	if snapshot == nil || snapshot.textfileDir == "" {
		return nil
	}
	if err := os.MkdirAll(snapshot.textfileDir, 0o755); err != nil {
		return err
	}
	tmpFile, err := os.CreateTemp(snapshot.textfileDir, "."+strings.TrimSuffix(snapshot.fileName, ".prom")+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()
	cleanup := true
	closed := false
	defer func() {
		if !closed {
			if closeErr := tmpFile.Close(); err == nil {
				err = closeErr
			}
		}
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err = snapshot.writeProm(tmpFile); err != nil {
		return err
	}
	if err = tmpFile.Chmod(0o644); err != nil {
		return err
	}
	if err = tmpFile.Sync(); err != nil {
		return err
	}
	if err = tmpFile.Close(); err != nil {
		return err
	}
	closed = true
	if err = os.Rename(tmpName, filepath.Join(snapshot.textfileDir, snapshot.fileName)); err != nil {
		return err
	}
	cleanup = false
	return syncDir(snapshot.textfileDir)
}

func (s *taskStatus) writeProm(w io.Writer) error {
	registry := prometheus.NewRegistry()
	baseLabels := prometheus.Labels{
		"status_id": s.statusID,
		"task_type": s.taskType,
		"command":   s.command,
	}
	registerGauge := func(name, help string, labels prometheus.Labels, value float64) error {
		gauge := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        name,
			Help:        help,
			ConstLabels: labels,
		})
		gauge.Set(value)
		return registry.Register(gauge)
	}

	running := 0.0
	if s.running {
		running = 1
	}
	successTimestamp := 0.0
	failureTimestamp := 0.0
	duration := 0.0
	if s.finished {
		duration = s.endTime.Sub(s.startTime).Seconds()
		if s.success {
			successTimestamp = float64(s.endTime.UnixNano()) / float64(time.Second)
		} else {
			failureTimestamp = float64(s.endTime.UnixNano()) / float64(time.Second)
		}
	}

	if err := registerGauge("br_task_running", "Whether a BR task is currently running.", baseLabels, running); err != nil {
		return err
	}
	if err := registerGauge("br_task_start_timestamp_seconds", "BR task start time as Unix timestamp.", baseLabels, float64(s.startTime.UnixNano())/float64(time.Second)); err != nil {
		return err
	}
	if err := registerGauge("br_task_last_update_timestamp_seconds", "BR task last status update time as Unix timestamp.", baseLabels, float64(s.lastUpdateTime.UnixNano())/float64(time.Second)); err != nil {
		return err
	}
	if err := registerGauge("br_task_last_success_timestamp_seconds", "BR task last success time as Unix timestamp.", baseLabels, successTimestamp); err != nil {
		return err
	}
	if err := registerGauge("br_task_last_failure_timestamp_seconds", "BR task last failure time as Unix timestamp.", baseLabels, failureTimestamp); err != nil {
		return err
	}
	if err := registerGauge("br_task_last_duration_seconds", "BR task final duration in seconds.", baseLabels, duration); err != nil {
		return err
	}
	if err := registerGauge("br_task_last_size_bytes", "BR task processed data size in bytes.", baseLabels, float64(s.sizeBytes)); err != nil {
		return err
	}
	infoLabels := make(prometheus.Labels, len(baseLabels)+4)
	for key, value := range baseLabels {
		infoLabels[key] = value
	}
	infoLabels["scope"] = s.scope
	infoLabels["pd_addrs"] = s.pdAddrs
	infoLabels["storage"] = s.storage
	infoLabels["task_ts"] = s.taskTS
	if err := registerGauge("br_task_info", "BR task metadata.", infoLabels, 1); err != nil {
		return err
	}

	if s.phase != "" {
		phaseLabels := make(prometheus.Labels, len(baseLabels)+1)
		for key, value := range baseLabels {
			phaseLabels[key] = value
		}
		phaseLabels["phase"] = s.phase
		if err := registerGauge("br_task_phase", "Current BR task phase.", phaseLabels, 1); err != nil {
			return err
		}
	}

	metricFamilies, err := registry.Gather()
	if err != nil {
		return err
	}

	encoder := expfmt.NewEncoder(w, expfmt.FmtText)
	for _, metricFamily := range metricFamilies {
		if err := encoder.Encode(metricFamily); err != nil {
			return err
		}
	}
	if closer, ok := encoder.(expfmt.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(dir string) error {
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err = dirFile.Sync(); err != nil {
		_ = dirFile.Close()
		return err
	}
	return dirFile.Close()
}
