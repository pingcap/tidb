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
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/util/generic"
	"github.com/pingcap/tidb/pkg/util/logutil"
	kvutil "github.com/tikv/client-go/v2/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// BackendCtxMgr is used to manage the backend context.
type BackendCtxMgr interface {
	CheckAvailable() (bool, error)
	Register(ctx context.Context, unique bool, jobID int64, etcdClient *clientv3.Client, pdAddr string, resourceGroupName string) (BackendCtx, error)
	Unregister(jobID int64)
	Load(jobID int64) (BackendCtx, bool)

	MarkJobProcessing(jobID int64) (ok bool)
	MarkJobFinish()
}

type litBackendCtxMgr struct {
	generic.SyncMap[int64, *litBackendCtx]
	memRoot         MemRoot
	diskRoot        DiskRoot
	processingJobID int64
	lastLoggingTime time.Time
	mu              sync.Mutex
}

func newLitBackendCtxMgr(path string, memQuota uint64) BackendCtxMgr {
	mgr := &litBackendCtxMgr{
		SyncMap:  generic.NewSyncMap[int64, *litBackendCtx](10),
		memRoot:  nil,
		diskRoot: nil,
	}
	mgr.memRoot = NewMemRootImpl(int64(memQuota), mgr)
	mgr.diskRoot = NewDiskRootImpl(path, mgr)
	LitMemRoot = mgr.memRoot
	LitDiskRoot = mgr.diskRoot
	LitDiskRoot.UpdateUsage()
	err := LitDiskRoot.StartupCheck()
	if err != nil {
		logutil.BgLogger().Warn("ingest backfill may not be available", zap.String("category", "ddl-ingest"), zap.Error(err))
	}
	return mgr
}

// MarkJobProcessing marks ingest backfill is processing.
func (m *litBackendCtxMgr) MarkJobProcessing(jobID int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.processingJobID == 0 || m.processingJobID == jobID {
		m.processingJobID = jobID
		return true
	}
	if time.Since(m.lastLoggingTime) > 1*time.Minute {
		logutil.BgLogger().Info("ingest backfill worker is already in used by another DDL job",
			zap.String("category", "ddl-ingest"),
			zap.Int64("processing job ID", m.processingJobID))
		m.lastLoggingTime = time.Now()
	}
	return false
}

// MarkJobFinish marks ingest backfill is finished.
func (m *litBackendCtxMgr) MarkJobFinish() {
	m.mu.Lock()
	m.processingJobID = 0
	m.mu.Unlock()
}

// CheckAvailable checks if the ingest backfill is available.
func (m *litBackendCtxMgr) CheckAvailable() (bool, error) {
	if err := m.diskRoot.PreCheckUsage(); err != nil {
		logutil.BgLogger().Info("ingest backfill is not available", zap.String("category", "ddl-ingest"), zap.Error(err))
		return false, err
	}
	return true, nil
}

// Register creates a new backend and registers it to the backend context.
func (m *litBackendCtxMgr) Register(ctx context.Context, unique bool, jobID int64, etcdClient *clientv3.Client, pdAddr string, resourceGroupName string) (BackendCtx, error) {
	bc, exist := m.Load(jobID)
	if !exist {
		m.memRoot.RefreshConsumption()
		ok := m.memRoot.CheckConsume(StructSizeBackendCtx)
		if !ok {
			return nil, genBackendAllocMemFailedErr(ctx, m.memRoot, jobID)
		}
		cfg, err := genConfig(ctx, m.memRoot, jobID, unique)
		if err != nil {
			logutil.Logger(ctx).Warn(LitWarnConfigError, zap.Int64("job ID", jobID), zap.Error(err))
			return nil, err
		}
		cfg.Lightning.TiDB.PdAddr = pdAddr
		bd, err := createLocalBackend(ctx, cfg, resourceGroupName)
		if err != nil {
			logutil.Logger(ctx).Error(LitErrCreateBackendFail, zap.Int64("job ID", jobID), zap.Error(err))
			return nil, err
		}

		bcCtx := newBackendContext(ctx, jobID, bd, cfg.Lightning, defaultImportantVariables, m.memRoot, m.diskRoot, etcdClient)
		m.Store(jobID, bcCtx)

		m.memRoot.Consume(StructSizeBackendCtx)
		logutil.Logger(ctx).Info(LitInfoCreateBackend, zap.Int64("job ID", jobID),
			zap.Int64("current memory usage", m.memRoot.CurrentUsage()),
			zap.Int64("max memory quota", m.memRoot.MaxMemoryQuota()),
			zap.Bool("is unique index", unique))
		return bcCtx, nil
	}
	return bc, nil
}

func createLocalBackend(ctx context.Context, cfg *Config, resourceGroupName string) (*local.Backend, error) {
	tls, err := cfg.Lightning.ToTLS()
	if err != nil {
		logutil.Logger(ctx).Error(LitErrCreateBackendFail, zap.Error(err))
		return nil, err
	}

	logutil.BgLogger().Info("create local backend for adding index", zap.String("category", "ddl-ingest"), zap.String("keyspaceName", cfg.KeyspaceName))
	regionSizeGetter := &local.TableRegionSizeGetterImpl{
		DB: nil,
	}
	// We disable the switch TiKV mode feature for now,
	// because the impact is not fully tested.
	var raftKV2SwitchModeDuration time.Duration
	backendConfig := local.NewBackendConfig(cfg.Lightning, int(LitRLimit), cfg.KeyspaceName, resourceGroupName, kvutil.ExplicitTypeDDL, raftKV2SwitchModeDuration)
	return local.NewBackend(ctx, tls, backendConfig, regionSizeGetter)
}

const checkpointUpdateInterval = 10 * time.Minute

func newBackendContext(ctx context.Context, jobID int64, be *local.Backend, cfg *config.Config, vars map[string]string, memRoot MemRoot, diskRoot DiskRoot, etcdClient *clientv3.Client) *litBackendCtx {
	bCtx := &litBackendCtx{
		SyncMap:        generic.NewSyncMap[int64, *engineInfo](10),
		MemRoot:        memRoot,
		DiskRoot:       diskRoot,
		jobID:          jobID,
		backend:        be,
		ctx:            ctx,
		cfg:            cfg,
		sysVars:        vars,
		diskRoot:       diskRoot,
		updateInterval: checkpointUpdateInterval,
		etcdClient:     etcdClient,
	}
	bCtx.timeOfLastFlush.Store(time.Now())
	return bCtx
}

// Unregister removes a backend context from the backend context manager.
func (m *litBackendCtxMgr) Unregister(jobID int64) {
	bc, exist := m.SyncMap.Delete(jobID)
	if !exist {
		return
	}
	bc.unregisterAll(jobID)
	bc.backend.Close()
	if bc.checkpointMgr != nil {
		bc.checkpointMgr.Close()
	}
	m.memRoot.Release(StructSizeBackendCtx)
	m.memRoot.ReleaseWithTag(EncodeBackendTag(jobID))
	logutil.Logger(bc.ctx).Info(LitInfoCloseBackend, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", m.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", m.memRoot.MaxMemoryQuota()))
}

func (m *litBackendCtxMgr) Load(jobID int64) (BackendCtx, bool) {
	return m.SyncMap.Load(jobID)
}

// TotalDiskUsage returns the total disk usage of all backends.
func (m *litBackendCtxMgr) TotalDiskUsage() uint64 {
	var totalDiskUsed uint64
	for _, key := range m.Keys() {
		bc, exists := m.SyncMap.Load(key)
		if exists {
			_, _, bcDiskUsed, _ := local.CheckDiskQuota(bc.backend, math.MaxInt64)
			totalDiskUsed += uint64(bcDiskUsed)
		}
	}
	return totalDiskUsed
}

// UpdateMemoryUsage collects the memory usages from all the backend and updates it to the memRoot.
func (m *litBackendCtxMgr) UpdateMemoryUsage() {
	for _, key := range m.Keys() {
		bc, exists := m.SyncMap.Load(key)
		if exists {
			curSize := bc.backend.TotalMemoryConsume()
			m.memRoot.ReleaseWithTag(EncodeBackendTag(bc.jobID))
			m.memRoot.ConsumeWithTag(EncodeBackendTag(bc.jobID), curSize)
		}
	}
}

// EncodeBackendTag encodes the job ID to backend tag.
// The backend tag is also used as the file name of the local index data files.
func EncodeBackendTag(jobID int64) string {
	return fmt.Sprintf("%d", jobID)
}

// DecodeBackendTag decodes the backend tag to job ID.
func DecodeBackendTag(name string) (int64, error) {
	return strconv.ParseInt(name, 10, 64)
}
