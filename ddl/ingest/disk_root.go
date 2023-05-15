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
	"sync"

	"github.com/pingcap/errors"
	lcom "github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// DiskRoot is used to track the disk usage for the lightning backfill process.
type DiskRoot interface {
	UpdateUsage()
	ShouldImport() bool
	UsageInfo() string
	PreCheckUsage() error
	StartupCheck() error
}

const capacityThreshold = 0.9

// diskRootImpl implements DiskRoot interface.
type diskRootImpl struct {
	path     string
	capacity uint64
	used     uint64
	bcUsed   uint64
	bcCtx    *litBackendCtxMgr
	mu       sync.RWMutex
}

// NewDiskRootImpl creates a new DiskRoot.
func NewDiskRootImpl(path string, bcCtx *litBackendCtxMgr) DiskRoot {
	return &diskRootImpl{
		path:  path,
		bcCtx: bcCtx,
	}
}

// UpdateUsage implements DiskRoot interface.
func (d *diskRootImpl) UpdateUsage() {
	bcUsed := d.bcCtx.TotalDiskUsage()
	var capacity, used uint64
	sz, err := lcom.GetStorageSize(d.path)
	if err != nil {
		logutil.BgLogger().Error(LitErrGetStorageQuota, zap.Error(err))
	} else {
		capacity, used = sz.Capacity, sz.Capacity-sz.Available
	}
	d.mu.Lock()
	d.bcUsed = bcUsed
	d.capacity = capacity
	d.used = used
	d.mu.Unlock()
}

// ShouldImport implements DiskRoot interface.
func (d *diskRootImpl) ShouldImport() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.bcUsed > variable.DDLDiskQuota.Load() {
		logutil.BgLogger().Info("[ddl-ingest] disk usage is over quota",
			zap.Uint64("quota", variable.DDLDiskQuota.Load()),
			zap.String("usage", d.usageInfo()))
		return true
	}
	if d.used == 0 && d.capacity == 0 {
		return false
	}
	if float64(d.used) >= float64(d.capacity)*capacityThreshold {
		logutil.BgLogger().Warn("[ddl-ingest] available disk space is less than 10%, "+
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
	sz, err := lcom.GetStorageSize(d.path)
	if err != nil {
		return errors.Trace(err)
	}
	if RiskOfDiskFull(sz.Available, sz.Capacity) {
		sortPath := ConfigSortPath()
		return errors.Errorf("sort path: %s, %s, please clean up the disk and retry", sortPath, d.UsageInfo())
	}
	return nil
}

// StartupCheck implements DiskRoot interface.
func (d *diskRootImpl) StartupCheck() error {
	sz, err := lcom.GetStorageSize(d.path)
	if err != nil {
		return errors.Trace(err)
	}
	quota := variable.DDLDiskQuota.Load()
	if sz.Available < quota {
		sortPath := ConfigSortPath()
		return errors.Errorf("the available disk space(%d) in %s should be greater than @@tidb_ddl_disk_quota(%d)",
			sz.Available, sortPath, quota)
	}
	return nil
}

// RiskOfDiskFull checks if the disk has less than 10% space.
func RiskOfDiskFull(available, capacity uint64) bool {
	return float64(available) < (1-capacityThreshold)*float64(capacity)
}
