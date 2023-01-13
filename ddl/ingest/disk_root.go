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
	lcom "github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"go.uber.org/zap"
)

// DiskRoot is used to track the disk usage for the lightning backfill process.
type DiskRoot interface {
	CurrentUsage() uint64
	MaxQuota() uint64
	UpdateUsageAndQuota() error
}

const capacityThreshold = 0.9

// diskRootImpl implements DiskRoot interface.
type diskRootImpl struct {
	path         string
	currentUsage uint64
	maxQuota     uint64
	bcCtx        *backendCtxManager
}

// NewDiskRootImpl creates a new DiskRoot.
func NewDiskRootImpl(path string, bcCtx *backendCtxManager) DiskRoot {
	return &diskRootImpl{
		path:  path,
		bcCtx: bcCtx,
	}
}

// CurrentUsage implements DiskRoot interface.
func (d *diskRootImpl) CurrentUsage() uint64 {
	return d.currentUsage
}

// MaxQuota implements DiskRoot interface.
func (d *diskRootImpl) MaxQuota() uint64 {
	return d.maxQuota
}

// UpdateUsageAndQuota implements DiskRoot interface.
func (d *diskRootImpl) UpdateUsageAndQuota() error {
	d.currentUsage = d.bcCtx.TotalDiskUsage()
	sz, err := lcom.GetStorageSize(d.path)
	if err != nil {
		logutil.BgLogger().Error(LitErrGetStorageQuota, zap.Error(err))
		return err
	}
	d.maxQuota = mathutil.Min(variable.DDLDiskQuota.Load(), uint64(capacityThreshold*float64(sz.Capacity)))
	return nil
}
