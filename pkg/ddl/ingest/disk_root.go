// Copyright 2022 PingCAP, Inc.
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

package ingest

import (
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	lcom "github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/size"
	"go.uber.org/zap"
)

// ResourceTracker reports the current local disk usage in bytes.
type ResourceTracker interface {
	GetDiskUsage() uint64
}

// DiskRoot is used to track the disk usage for the lightning backfill process.
type DiskRoot interface {
	Add(id int64, tracker ResourceTracker)
	Remove(id int64)
	Count() int

	UpdateUsage()
	ShouldImport() bool
	UsageInfo() string
	PreCheckUsage() error
	StartupCheck() error
}

const (
	capacityThreshold = 0.9
	// TiDB nodes typically have 2 GiB memory per CPU slot, and local sort flushes
	// a similar-sized batch.
	localSortBytesPerSlot = 2 * size.GB
)

// diskRootImpl implements DiskRoot interface.
type diskRootImpl struct {
	path     string
	capacity uint64
	used     uint64
	bcUsed   uint64
	mu       sync.RWMutex
	items    map[int64]ResourceTracker
	updating atomic.Bool
}

// NewDiskRootImpl creates a new DiskRoot.
func NewDiskRootImpl(path string) DiskRoot {
	return &diskRootImpl{
		path:  path,
		items: make(map[int64]ResourceTracker),
	}
}

// TrackerCountForTest is only used for test.
var TrackerCountForTest = atomic.Int64{}

// Add adds a tracker to disk root.
func (d *diskRootImpl) Add(id int64, tracker ResourceTracker) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.items[id] = tracker
	TrackerCountForTest.Add(1)
}

// Remove removes a tracker from disk root.
func (d *diskRootImpl) Remove(id int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.items, id)
	TrackerCountForTest.Add(-1)
}

// Count is only used for test.
func (d *diskRootImpl) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}

// UpdateUsage implements DiskRoot interface.
func (d *diskRootImpl) UpdateUsage() {
	if !d.updating.CompareAndSwap(false, true) {
		return
	}
	var capacity, used uint64
	sz, err := lcom.GetStorageSize(d.path)
	if err != nil {
		logutil.DDLIngestLogger().Error(LitErrGetStorageQuota, zap.Error(err))
	} else {
		capacity, used = sz.Capacity, sz.Capacity-sz.Available
	}
	d.updating.Store(false)
	d.mu.Lock()
	var totalUsage uint64
	for _, tracker := range d.items {
		totalUsage += tracker.GetDiskUsage()
	}
	d.bcUsed = totalUsage
	d.capacity = capacity
	d.used = used
	d.mu.Unlock()
}

// ShouldImport implements DiskRoot interface.
func (d *diskRootImpl) ShouldImport() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.bcUsed > vardef.DDLDiskQuota.Load() {
		logutil.DDLIngestLogger().Info("disk usage is over quota",
			zap.Uint64("quota", vardef.DDLDiskQuota.Load()),
			zap.String("usage", d.usageInfo()))
		return true
	}
	if d.used == 0 && d.capacity == 0 {
		return false
	}
	if float64(d.used) >= float64(d.capacity)*capacityThreshold {
		logutil.DDLIngestLogger().Warn("available disk space is less than 10%, "+
			"this may degrade the performance, "+
			"please make sure the disk available space is larger than @@tidb_ddl_disk_quota before adding index",
			zap.String("usage", d.usageInfo()))
		return true
	}
	return false
}

// UsageInfo implements DiskRoot interface.
func (d *diskRootImpl) UsageInfo() string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.usageInfo()
}

func (d *diskRootImpl) usageInfo() string {
	return fmt.Sprintf("disk usage: %d/%d, backend usage: %d", d.used, d.capacity, d.bcUsed)
}

// PreCheckUsage implements DiskRoot interface.
func (d *diskRootImpl) PreCheckUsage() error {
	failpoint.Inject("mockIngestCheckEnvFailed", func(_ failpoint.Value) {
		failpoint.Return(dbterror.ErrIngestCheckEnvFailed.FastGenByArgs("mock error"))
	})
	err := os.MkdirAll(d.path, 0700)
	if err != nil {
		return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(err.Error())
	}
	sz, err := lcom.GetStorageSize(d.path)
	if err != nil {
		return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(err.Error())
	}
	if riskOfDiskFull(sz.Available, sz.Capacity) {
		logutil.DDLIngestLogger().Warn("available disk space is less than 10%, cannot use ingest mode",
			zap.String("sort path", d.path),
			zap.String("usage", d.usageInfo()))
		if runtime.GOOS == "darwin" {
			// darwin's disk is too expensive and we only use it in the development environment. so we ignore the error.
			return nil
		}
		msg := fmt.Sprintf("no enough space in %s", d.path)
		return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(msg)
	}
	return nil
}

// StartupCheck implements DiskRoot interface.
func (d *diskRootImpl) StartupCheck() error {
	sz, err := lcom.GetStorageSize(d.path)
	if err != nil {
		return errors.Trace(err)
	}
	quota := vardef.DDLDiskQuota.Load()
	if sz.Available < quota {
		return errors.Errorf("the available disk space(%d) in %s should be greater than @@tidb_ddl_disk_quota(%d)",
			sz.Available, d.path, quota)
	}
	return nil
}

// reservedCapacityBytes returns the minimum disk capacity to keep available.
func reservedCapacityBytes(capacity uint64) uint64 {
	return capacity - uint64(float64(capacity)*capacityThreshold)
}

func riskOfDiskFull(available, capacity uint64) bool {
	return available < reservedCapacityBytes(capacity)
}

// CheckLocalSortFreeDisk performs a best-effort precheck of the current task's
// estimated growth before local sort starts, reducing the risk of frequent small
// SST imports when the local disk has little free space.
func CheckLocalSortFreeDisk(execID string, currentTaskRuntimeSlots int) error {
	sortPath, err := GenIngestTempDataDir()
	if err != nil {
		return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(err.Error())
	}
	sz, err := lcom.GetStorageSize(sortPath)
	if err != nil {
		return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(err.Error())
	}

	return checkLocalSortFreeDisk(localSortFreeDiskCheck{
		execID:                  execID,
		sortPath:                sortPath,
		availableBytes:          sz.Available,
		totalCapacityBytes:      sz.Capacity,
		currentTaskRuntimeSlots: currentTaskRuntimeSlots,
	})
}

type localSortFreeDiskCheck struct {
	execID                  string
	sortPath                string
	availableBytes          uint64
	totalCapacityBytes      uint64
	currentTaskRuntimeSlots int
}

func checkLocalSortFreeDisk(p localSortFreeDiskCheck) error {
	// Cap the estimate at tidb_ddl_disk_quota because exceeding the quota
	// triggers an import that releases local disk space.
	currentTaskEstimatedGrowthBytes := min(
		uint64(p.currentTaskRuntimeSlots)*localSortBytesPerSlot,
		vardef.DDLDiskQuota.Load(),
	)
	freeThresholdBytes := reservedCapacityBytes(p.totalCapacityBytes) + currentTaskEstimatedGrowthBytes
	if p.availableBytes > freeThresholdBytes {
		logutil.DDLIngestLogger().Info("local sort free disk check passed",
			zap.Uint64("freeDiskThresholdBytes", freeThresholdBytes),
			zap.Uint64("availableBytes", p.availableBytes),
			zap.String("sortPath", p.sortPath),
			zap.Uint64("totalCapacityBytes", p.totalCapacityBytes),
			zap.Int("currentTaskRuntimeSlots", p.currentTaskRuntimeSlots),
			zap.Uint64("currentTaskEstimatedGrowthBytes", currentTaskEstimatedGrowthBytes),
			zap.Uint64("localSortBytesPerSlot", localSortBytesPerSlot))
		return nil
	}

	return dbterror.ErrIngestCheckEnvFailed.FastGenByArgs(
		fmt.Sprintf(
			"insufficient free disk space on TiDB node %s at %s: %d bytes available; available free disk space must be greater than %d bytes; the add-index job cannot start because low disk space would degrade SST ingestion. Free disk space on this TiDB node by removing unnecessary logs or files",
			p.execID,
			p.sortPath,
			p.availableBytes,
			freeThresholdBytes,
		),
	)
}
