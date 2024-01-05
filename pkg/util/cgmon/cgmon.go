package cgmon

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/cgroup"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/zap"
)

const (
	refreshInterval = 10 * time.Second
)

var (
	started         bool
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	cfgMaxProcs     int
	lastMaxProcs    int
	lastMemoryLimit uint64
)

// StartCgroupMonitor uses to start the cgroup monitoring.
// WARN: this function is not thread-safe.
func StartCgroupMonitor() {
	if started {
		return
	}
	started = true
	// Get configured maxprocs.
	cfgMaxProcs = runtime.GOMAXPROCS(0)
	ctx, cancel = context.WithCancel(context.Background())
	wg.Add(1)
	go refreshCgroupLoop()
	log.Info("cgroup monitor started")
}

// StopCgroupMonitor uses to stop the cgroup monitoring.
// WARN: this function is not thread-safe.
func StopCgroupMonitor() {
	if !started {
		return
	}
	started = false
	if cancel != nil {
		cancel()
	}
	wg.Wait()
	log.Info("cgroup monitor stopped")
}

func refreshCgroupLoop() {
	ticker := time.NewTicker(refreshInterval)
	defer func() {
		util.Recover("cgmon", "refreshCgroupLoop", nil, false)
		wg.Done()
		ticker.Stop()
	}()

	refreshCgroupCPU()
	refreshCgroupMemory()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshCgroupCPU()
			refreshCgroupMemory()
		}
	}
}

func refreshCgroupCPU() {
	// Get the number of CPUs.
	quota := runtime.NumCPU()

	// Get CPU quota from cgroup.
	cpuPeriod, cpuQuota, err := cgroup.GetCPUPeriodAndQuota()
	if err != nil {
		log.Warn("failed to get cgroup cpu quota", zap.Error(err))
		return
	}
	if cpuPeriod > 0 && cpuQuota > 0 {
		ratio := float64(cpuQuota) / float64(cpuPeriod)
		if ratio < float64(quota) {
			quota = int(math.Ceil(ratio))
		}
	}

	if quota != lastMaxProcs && quota < cfgMaxProcs {
		runtime.GOMAXPROCS(quota)
		log.Info(fmt.Sprintf("maxprocs set to %v", quota))
		metrics.MaxProcs.Set(float64(quota))
		lastMaxProcs = quota
	} else if lastMaxProcs == 0 {
		log.Info(fmt.Sprintf("maxprocs set to %v", cfgMaxProcs))
		metrics.MaxProcs.Set(float64(cfgMaxProcs))
		lastMaxProcs = cfgMaxProcs
	}
}

func refreshCgroupMemory() {
	memLimit, err := cgroup.GetMemoryLimit()
	if err != nil {
		log.Warn("failed to get cgroup memory limit", zap.Error(err))
		return
	}
	vmem, err := mem.VirtualMemory()
	if err != nil {
		log.Warn("failed to get system memory size", zap.Error(err))
		return
	}
	if memLimit > vmem.Total {
		memLimit = vmem.Total
	}
	if memLimit != lastMemoryLimit {
		log.Info(fmt.Sprintf("memory limit set to %v", memLimit))
		metrics.MemoryLimit.Set(float64(memLimit))
		lastMemoryLimit = memLimit
	}
}
