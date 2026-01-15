// Copyright 2025 PingCAP, Inc.
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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
)

const (
	memStateVer             = "v1"
	memStateStoreNamePrefix = "mem-state."
	memStateStoreNameSuffix = ".json"
)

var (
	globalArbitrator struct {
		softLimit struct {
			originText atomic.Value
			sync.Mutex
		}
		workMode struct {
			originText atomic.Value
			sync.Mutex
		}
		v struct {
			atomic.Pointer[MemArbitrator]
			sync.Mutex
		}
		enable struct { // register callbacks after the global memory arbitrator is enabled
			callbacks []func()
			atomic.Bool
			sync.Mutex
		}
		metrics struct {
			last struct {
				updateUtimeSec atomic.Int64
				execMetricsCounter
			}
			pools struct {
				internal        atomic.Int64
				internalSession atomic.Int64

				small   atomic.Int64
				big     atomic.Int64
				intoBig atomic.Int64
			}
			init  atomic.Bool
			reset atomic.Bool
			sync.Mutex
		}
	}
	mockinitGlobalMemArbitrator func() *MemArbitrator
)

func reportGlobalMemArbitratorMetrics() {
	m := globalArbitrator.v.Load()
	if m == nil {
		return
	}

	curUtimeSec := nowUnixSec()
	if curUtimeSec < globalArbitrator.metrics.last.updateUtimeSec.Load()+1 { // every 1 second
		return
	}

	globalArbitrator.metrics.Lock()
	defer globalArbitrator.metrics.Unlock()

	globalArbitrator.metrics.last.updateUtimeSec.Store(curUtimeSec)

	{ // waiting task
		setWaitingTask := func(label string, value int64) {
			metrics.SetGlobalMemArbitratorGauge(metrics.GlobalMemArbitratorWaitingTask, label, value)
		}
		p := m.TaskNumByPattern()
		setWaitingTask("total", m.TaskNum())
		setWaitingTask("priority-low", p[ArbitrationPriorityLow])
		setWaitingTask("priority-medium", p[ArbitrationPriorityMedium])
		setWaitingTask("priority-high", p[ArbitrationPriorityHigh])
		setWaitingTask("wait-averse", p[ArbitrationWaitAverse])
	}
	{ // quota
		setQuota := func(label string, value int64) {
			metrics.SetGlobalMemArbitratorGauge(metrics.GlobalMemArbitratorQuota, label, value)
		}
		setQuota("allocated", m.allocated())
		setQuota("out-of-control", m.OutOfControl())
		setQuota("buffer", m.reservedBuffer())
		setQuota("available", m.available())
		setQuota("tracked-heap", m.avoidance.heapTracked.Load())
		setQuota("awaitfree-pool-cap", m.awaitFreePoolCap())
		setQuota("awaitfree-pool-used", m.approxAwaitFreePoolUsed().quota)
		setQuota("awaitfree-pool-tracked-heap", m.approxAwaitFreePoolUsed().trackedHeap)
		setQuota("mem-inuse", m.heapController.memInuse.Load())
		setQuota("soft-limit", m.softLimit())
		setQuota("wait-alloc", m.WaitingAllocSize())
		{
			blockedAt := int64(0)
			if blockedSize, utimeSec := m.lastBlockedAt(); curUtimeSec <= utimeSec+5 { // within 5 seconds
				blockedAt = blockedSize
			}
			setQuota("blocked-at", blockedAt)
		}
		setQuota("medium-pool", m.poolMediumQuota())
	}
	{ // memory magnification
		memMagnif := float64(0)
		if quota := m.allocated(); quota > 0 {
			f := min(calcRatio(m.heapController.lastGC.heapAlloc.Load(), quota), defMaxMagnif)
			memMagnif = float64(f) / kilo
		}
		metrics.GlobalMemArbitratorRuntimeMemMagnifi.Set(memMagnif)
	}
	{ // root pool
		setRootPool := func(label string, value int64) {
			metrics.SetGlobalMemArbitratorGauge(metrics.GlobalMemArbitratorRootPool, label, value)
		}
		setRootPool("root-pool", m.RootPoolNum())
		setRootPool("session-internal", globalArbitrator.metrics.pools.internalSession.Load())
		setRootPool("under-kill", m.underKill.approxSize())
		setRootPool("under-cancel", m.underCancel.approxSize())
		setRootPool("digest-cache", m.digestProfileCache.num.Load())
		setRootPool("sql-big", globalArbitrator.metrics.pools.big.Load())
		setRootPool("sql-small", globalArbitrator.metrics.pools.small.Load())
		setRootPool("sql-internal", globalArbitrator.metrics.pools.internal.Load())
		setRootPool("sql-into-big", globalArbitrator.metrics.pools.intoBig.Load())
	}
	{ // counter
		newExecMetrics := m.ExecMetrics()
		doReportGlobalMemArbitratorCounter(&globalArbitrator.metrics.last.execMetricsCounter, &newExecMetrics, false)
		globalArbitrator.metrics.last.execMetricsCounter = newExecMetrics
	}
}

func doReportGlobalMemArbitratorCounter(oriExecMetrics, newExecMetrics *execMetricsCounter, init bool) {
	addTaskExecCount := func(label string, value int64) {
		if value <= 0 && !init {
			return
		}
		metrics.AddGlobalMemArbitratorCounter(metrics.GlobalMemArbitratorTaskExecCounter, label, value)
	}
	addEventCount := func(label string, value int64) {
		if value <= 0 && !init {
			return
		}
		metrics.AddGlobalMemArbitratorCounter(metrics.GlobalMemArbitratorEventCounter, label, value)
	}

	addTaskExecCount("success", (newExecMetrics.Task.Succ - oriExecMetrics.Task.Succ))
	addTaskExecCount("fail", (newExecMetrics.Task.Fail - oriExecMetrics.Task.Fail))
	addTaskExecCount("success-prio-low", (newExecMetrics.Task.SuccByPriority[ArbitrationPriorityLow] - oriExecMetrics.Task.SuccByPriority[ArbitrationPriorityLow]))
	addTaskExecCount("success-prio-medium", (newExecMetrics.Task.SuccByPriority[ArbitrationPriorityMedium] - oriExecMetrics.Task.SuccByPriority[ArbitrationPriorityMedium]))
	addTaskExecCount("success-prio-high", (newExecMetrics.Task.SuccByPriority[ArbitrationPriorityHigh] - oriExecMetrics.Task.SuccByPriority[ArbitrationPriorityHigh]))
	addTaskExecCount("cancel-standard-mode", (newExecMetrics.Cancel.StandardMode - oriExecMetrics.Cancel.StandardMode))
	addTaskExecCount("cancel-wait-averse", (newExecMetrics.Cancel.WaitAverse - oriExecMetrics.Cancel.WaitAverse))
	addTaskExecCount("cancel-prio-low", (newExecMetrics.Cancel.PriorityMode[ArbitrationPriorityLow] - oriExecMetrics.Cancel.PriorityMode[ArbitrationPriorityLow]))
	addTaskExecCount("cancel-prio-medium", (newExecMetrics.Cancel.PriorityMode[ArbitrationPriorityMedium] - oriExecMetrics.Cancel.PriorityMode[ArbitrationPriorityMedium]))
	addTaskExecCount("cancel-prio-high", (newExecMetrics.Cancel.PriorityMode[ArbitrationPriorityHigh] - oriExecMetrics.Cancel.PriorityMode[ArbitrationPriorityHigh]))
	addTaskExecCount("kill-prio-low", (newExecMetrics.Risk.OOMKill[ArbitrationPriorityLow] - oriExecMetrics.Risk.OOMKill[ArbitrationPriorityLow]))
	addTaskExecCount("kill-prio-medium", (newExecMetrics.Risk.OOMKill[ArbitrationPriorityMedium] - oriExecMetrics.Risk.OOMKill[ArbitrationPriorityMedium]))
	addTaskExecCount("kill-prio-high", (newExecMetrics.Risk.OOMKill[ArbitrationPriorityHigh] - oriExecMetrics.Risk.OOMKill[ArbitrationPriorityHigh]))

	addEventCount("mem-risk", (newExecMetrics.Risk.Mem - oriExecMetrics.Risk.Mem))
	addEventCount("oom-risk", (newExecMetrics.Risk.OOM - oriExecMetrics.Risk.OOM))
	addEventCount("awaitfree-pool-grow-succ", (newExecMetrics.AwaitFree.Succ - oriExecMetrics.AwaitFree.Succ))
	addEventCount("awaitfree-pool-grow-fail", (newExecMetrics.AwaitFree.Fail - oriExecMetrics.AwaitFree.Fail))
	addEventCount("awaitfree-pool-shrink", (newExecMetrics.AwaitFree.Shrink - oriExecMetrics.AwaitFree.Shrink))
	addEventCount("awaitfree-pool-force-shrink", (newExecMetrics.AwaitFree.ForceShrink - oriExecMetrics.AwaitFree.ForceShrink))
	addEventCount("gc", (newExecMetrics.Action.GC - oriExecMetrics.Action.GC))
	addEventCount("update-memstats", (newExecMetrics.Action.UpdateRuntimeMemStats - oriExecMetrics.Action.UpdateRuntimeMemStats))
	addEventCount("record-memstate-succ", (newExecMetrics.Action.RecordMemState.Succ - oriExecMetrics.Action.RecordMemState.Succ))
	addEventCount("record-memstate-fail", (newExecMetrics.Action.RecordMemState.Fail - oriExecMetrics.Action.RecordMemState.Fail))
	addEventCount("shrink-digest-cache", (newExecMetrics.ShrinkDigest - oriExecMetrics.ShrinkDigest))
}

// HandleGlobalMemArbitratorRuntime is used to handle runtime memory stats.
func HandleGlobalMemArbitratorRuntime(s *runtime.MemStats) {
	m := GlobalMemArbitrator()
	if m == nil {
		if globalArbitrator.metrics.reset.Load() {
			resetGlobalMemArbitratorMetrics()
		}
		return
	}
	m.HandleRuntimeStats(intoRuntimeMemStats(s))
	reportGlobalMemArbitratorMetrics()
}

func resetGlobalMemArbitratorMetrics() {
	if !globalArbitrator.metrics.reset.Swap(false) {
		return
	}

	globalArbitrator.metrics.Lock()
	defer globalArbitrator.metrics.Unlock()

	metrics.ResetGlobalMemArbitratorGauge()
	metrics.GlobalMemArbitratorRuntimeMemMagnifi.Set(0)
}

// GetGlobalMemArbitratorSoftLimitText returns the text of the global memory arbitrator soft limit.
func GetGlobalMemArbitratorSoftLimitText() string {
	return globalArbitrator.softLimit.originText.Load().(string)
}

func doSetGlobalMemArbitratorSoftLimit() {
	globalArbitrator.softLimit.Lock()
	defer globalArbitrator.softLimit.Unlock()

	var mode SoftLimitMode

	str := GetGlobalMemArbitratorSoftLimitText()
	softLimit := int64(0)
	softLimitRate := float64(0)

	switch str {
	case ArbitratorSoftLimitModDisableName:
		mode = SoftLimitModeDisable
	case ArbitratorSoftLimitModeAutoName:
		mode = SoftLimitModeAuto
	default:
		mode = SoftLimitModeSpecified
		if intValue, err := strconv.ParseUint(str, 10, 64); err == nil && int64(intValue) > 1 {
			softLimit = int64(intValue)
		} else if floatValue, err := strconv.ParseFloat(str, 64); err == nil && floatValue > 0 && floatValue <= 1 {
			softLimitRate = floatValue
		} else {
			mode = SoftLimitModeDisable
		}
	}

	globalArbitrator.v.Load().SetSoftLimit(softLimit, softLimitRate, mode)
}

// SetGlobalMemArbitratorSoftLimit sets the soft limit of the global memory arbitrator.
func SetGlobalMemArbitratorSoftLimit(str string) {
	if GetGlobalMemArbitratorSoftLimitText() == str {
		return
	}
	{
		globalArbitrator.softLimit.Lock()

		globalArbitrator.softLimit.originText.Store(str)

		globalArbitrator.softLimit.Unlock()
	}
	m := GlobalMemArbitrator()
	if m == nil {
		return
	}
	doSetGlobalMemArbitratorSoftLimit()
}

// GlobalMemArbitrator returns the global memory arbitrator if it is enabled.
func GlobalMemArbitrator() *MemArbitrator {
	m := globalArbitrator.v.Load()
	if m.WorkMode() == ArbitratorModeDisable {
		return nil
	}
	return m
}

// UsingGlobalMemArbitration returns true if the global memory arbitration policy is used.
// It needs to return true when the work mode of the global memory arbitrator is changeing from disable to other modes.
func UsingGlobalMemArbitration() bool {
	return globalArbitrator.enable.Load()
}

// CleanupGlobalMemArbitratorForTest stops the async runner of the global memory arbitrator (suggest to use in tests only).
func CleanupGlobalMemArbitratorForTest() {
	SetGlobalMemArbitratorWorkMode(ArbitratorModeDisable.String())

	globalArbitrator.v.Lock()
	defer globalArbitrator.v.Unlock()

	m := globalArbitrator.v.Load()
	if m == nil {
		return
	}
	m.stop()
	globalArbitrator.v.Store(nil)

	mockNow = nil
	mockDebugInject = nil
	mockWinupCB = nil
}

// SetupGlobalMemArbitratorForTest sets up the global memory arbitrator for tests.
func SetupGlobalMemArbitratorForTest(baseDir string) {
	globalArbitrator.v.Lock()
	defer globalArbitrator.v.Unlock()

	if globalArbitrator.v.Load() != nil {
		panic("the global memory arbitrator is already set up")
	}

	_ = os.Remove(runtimeMemStateRecorderFilePath(baseDir))
	mockinitGlobalMemArbitrator = func() *MemArbitrator {
		m := NewMemArbitrator(
			0,
			4,
			defPoolQuotaShards,
			64*byteSizeKB, /* 64k ~ */
			newMemStateRecorder(baseDir),
		)
		m.AutoRun(
			MemArbitratorActions{
				Info:  logutil.BgLogger().Info,
				Warn:  logutil.BgLogger().Warn,
				Error: logutil.BgLogger().Error,
				UpdateRuntimeMemStats: func() {
				},
				GC: func() {
				},
			},
			defAwaitFreePoolAllocAlignSize,
			4,
			defTaskTickDur,
		)
		globalArbitrator.v.Store(m)
		return m
	}
}

// GetGlobalMemArbitratorWorkModeText returns the text of the global memory arbitrator work mode.
func GetGlobalMemArbitratorWorkModeText() string {
	return globalArbitrator.workMode.originText.Load().(string)
}
func setGlobalMemArbitratorWorkModeText(str string) {
	globalArbitrator.workMode.originText.Store(str)
}

// SetGlobalMemArbitratorWorkMode sets the work mode of the global memory arbitrator.
func SetGlobalMemArbitratorWorkMode(str string) bool {
	if !globalArbitrator.metrics.init.Load() {
		globalArbitrator.metrics.Lock()

		if !globalArbitrator.metrics.init.Load() {
			for mode := range maxArbitratorMode {
				metrics.GlobalMemArbitratorWorkMode.WithLabelValues(mode.String()).Set(0)
			}
			metrics.GlobalMemArbitratorWorkMode.WithLabelValues(GetGlobalMemArbitratorWorkModeText()).Set(1)
			execMetricsCounter := &globalArbitrator.metrics.last.execMetricsCounter
			doReportGlobalMemArbitratorCounter(execMetricsCounter, execMetricsCounter, true)
			globalArbitrator.metrics.init.Store(true)
		}

		globalArbitrator.metrics.Unlock()
	}

	if GetGlobalMemArbitratorWorkModeText() == str {
		return false
	}

	globalArbitrator.workMode.Lock()
	defer globalArbitrator.workMode.Unlock()

	if GetGlobalMemArbitratorWorkModeText() == str {
		return false
	}

	setGlobalMemArbitratorWorkModeText(str)

	newMode := ArbitratorModeDisable
	switch str {
	case ArbitratorModeStandardName:
		newMode = ArbitratorModeStandard
	case ArbitratorModePriorityName:
		newMode = ArbitratorModePriority
	}

	m := globalArbitrator.v.Load()
	oriMode := m.WorkMode()
	if oriMode == newMode {
		return false
	}

	if m == nil {
		m = initGlobalMemArbitrator()
	}

	metrics.GlobalMemArbitratorWorkMode.WithLabelValues(newMode.String()).Set(1)
	metrics.GlobalMemArbitratorWorkMode.WithLabelValues(oriMode.String()).Set(0)

	// from other modes to disable mode
	if newMode == ArbitratorModeDisable {
		m.SetWorkMode(newMode)
		globalArbitrator.enable.Store(false)
		globalArbitrator.metrics.reset.Store(true)
		return true
	}

	globalArbitrator.enable.Store(true) // set before changing the work mode

	// from disable mode to other modes
	if oriMode == ArbitratorModeDisable {
		doSetGlobalMemArbitratorLimit()
		doSetGlobalMemArbitratorSoftLimit()
	}
	m.SetWorkMode(newMode)
	return true
}

func doSetGlobalMemArbitratorLimit() {
	for _, callback := range globalArbitrator.enable.callbacks {
		callback()
	}
	globalArbitrator.v.Load().SetLimit(ServerMemoryLimit.Load())
}

// AjustGlobalMemArbitratorLimit adjusts the quota limit of the global memory arbitrator through the server memory limit.
func AjustGlobalMemArbitratorLimit() {
	m := GlobalMemArbitrator()
	if m == nil {
		return
	}

	if m.limit() == int64(ServerMemoryLimit.Load()) {
		return
	}

	doSetGlobalMemArbitratorLimit()
}

// RegisterCallbackForGlobalMemArbitrator registers a callback to be called after the global memory arbitrator is enabled.
func RegisterCallbackForGlobalMemArbitrator(f func()) {
	globalArbitrator.enable.Lock()

	globalArbitrator.enable.callbacks = append(globalArbitrator.enable.callbacks, f)

	globalArbitrator.enable.Unlock()
}

func initGlobalMemArbitrator() (m *MemArbitrator) {
	if intest.InTest {
		return mockinitGlobalMemArbitrator()
	}

	globalArbitrator.v.Lock()
	defer globalArbitrator.v.Unlock()

	if m = globalArbitrator.v.Load(); m != nil {
		return
	}

	cfg := config.GetGlobalConfig()
	baseDir := filepath.Join(cfg.TempDir, fmt.Sprintf("mem_arbitrator-%d", cfg.Port))

	limit := ServerMemoryLimit.Load()
	if limit == 0 {
		limit = GetMemTotalIgnoreErr()
	}

	m = NewMemArbitrator(
		int64(limit),
		defPoolStatusShards,
		defPoolQuotaShards,
		64*byteSizeKB, /* 64k ~ */
		newMemStateRecorder(baseDir),
	)

	m.AutoRun(
		MemArbitratorActions{
			Info:  logutil.BgLogger().Info,
			Warn:  logutil.BgLogger().Warn,
			Error: logutil.BgLogger().Error,
			UpdateRuntimeMemStats: func() {
				m.SetRuntimeMemStats(runtimeMemStats())
			},
			GC: func() {
				runtime.GC() //nolint: revive
			},
		},
		defAwaitFreePoolAllocAlignSize,
		defAwaitFreePoolShardNum,
		defTaskTickDur,
	)

	globalArbitrator.v.Store(m)
	return
}

// RemovePoolFromGlobalMemArbitrator removes a pool from the global memory arbitrator by its UID.
func RemovePoolFromGlobalMemArbitrator(uid uint64) bool {
	m := globalArbitrator.v.Load()
	if m == nil {
		return false
	}
	return m.RemoveRootPoolByID(uid)
}

func init() {
	workMode := ArbitratorModeDisable
	setGlobalMemArbitratorWorkModeText(workMode.String())
	globalArbitrator.softLimit.originText.Store(ArbitratorSoftLimitModDisableName)
	globalArbitrator.enable.callbacks = make([]func(), 0, 1)
}

type runtimeMemStateRecorder struct {
	baseDir  string
	filePath string
}

func runtimeMemStateRecorderFilePath(baseDir string) string {
	return filepath.Join(baseDir, memStateStoreNamePrefix+memStateVer+memStateStoreNameSuffix)
}

func newMemStateRecorder(baseDir string) *runtimeMemStateRecorder {
	return &runtimeMemStateRecorder{
		baseDir:  baseDir,
		filePath: runtimeMemStateRecorderFilePath(baseDir),
	}
}

func (m *runtimeMemStateRecorder) Store(memState *RuntimeMemStateV1) error {
	if _, err := os.Stat(m.baseDir); err != nil && !os.IsExist(err) {
		err = os.MkdirAll(m.baseDir, 0750)
		if err != nil {
			return fmt.Errorf("failed to create dir `%s`, err: %v", m.baseDir, err)
		}
	}

	buff, err := json.Marshal(memState)
	if err != nil {
		return err
	}

	f, err := os.CreateTemp(m.baseDir, ".mem_state.*.json")

	if err != nil {
		return err
	}

	_, err = f.Write(buff)

	f.Close()

	if err != nil {
		return err
	}

	return os.Rename(f.Name(), m.filePath)
}

func (m *runtimeMemStateRecorder) Load() (*RuntimeMemStateV1, error) {
	entries, err := os.ReadDir(m.baseDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read dir `%s`: %w", m.baseDir, err)
	}
	var realPath string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if name := entry.Name(); len(name) >= len(memStateStoreNamePrefix) && name[:len(memStateStoreNamePrefix)] == memStateStoreNamePrefix {
			suffix := name[len(memStateStoreNamePrefix):]
			suffixes := strings.Split(suffix, ".")
			if len(suffixes) < 2 {
				continue
			}
			if suffixes[0] == memStateVer { // v1
				realPath = filepath.Join(m.baseDir, name)
				break
			}
		}
	}

	if realPath == "" {
		return nil, nil
	}

	buff, err := os.ReadFile(realPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file `%s`: %w", realPath, err)
	}
	memState := RuntimeMemStateV1{}
	err = json.Unmarshal(buff, &memState)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal mem state: %w", err)
	}
	return &memState, nil
}

func intoRuntimeMemStats(s *runtime.MemStats) RuntimeMemStats {
	return RuntimeMemStats{
		HeapAlloc:  int64(s.HeapAlloc),
		HeapInuse:  int64(s.HeapInuse),
		TotalFree:  int64(s.TotalAlloc - s.Alloc),
		MemOffHeap: int64(s.Sys - s.HeapSys),
		LastGC:     int64(s.LastGC),
	}
}
