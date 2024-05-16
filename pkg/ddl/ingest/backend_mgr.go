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
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/failpoint"
	ddllogutil "github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/util/logutil"
	kvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

// BackendCtxMgr is used to manage the BackendCtx.
type BackendCtxMgr interface {
	// CheckMoreTasksAvailable checks if it can run more ingest backfill tasks.
	CheckMoreTasksAvailable(ctx context.Context) (bool, error)
	// Register uses jobID to identify the BackendCtx. If there's already a
	// BackendCtx with the same jobID, it will be returned. Otherwise, a new
	// BackendCtx will be created and returned.
	Register(
		ctx context.Context,
		jobID int64,
		unique bool,
		etcdClient *clientv3.Client,
		pdSvcDiscovery pd.ServiceDiscovery,
		resourceGroupName string,
	) (BackendCtx, error)
	Unregister(jobID int64)
	// Load returns the registered BackendCtx with the given jobID.
	Load(jobID int64) (BackendCtx, bool)
}

// FilterProcessingJobIDsFunc is a function type to return processing ones from
// input job IDs.
type FilterProcessingJobIDsFunc func([]int64) ([]int64, error)

// litBackendCtxMgr manages multiple litBackendCtx for each DDL job. Each
// litBackendCtx can use some local disk space and memory resource which are
// controlled by litBackendCtxMgr.
type litBackendCtxMgr struct {
	// the lifetime of entries in backends should cover all other resources so it can
	// be used as a lightweight indicator when interacts with other resources.
	// Currently, the entry must be created not after disk folder is created and
	// memory usage is tracked, and vice versa when considering deletion.
	backends struct {
		mu sync.RWMutex
		m  map[int64]*litBackendCtx
	}
	// all disk resources of litBackendCtx should be used under path. Currently the
	// hierarchy is ${path}/${jobID} for each litBackendCtx.
	path     string
	memRoot  MemRoot
	diskRoot DiskRoot

	filterProcessingJobIDs FilterProcessingJobIDsFunc
}

// NewLitBackendCtxMgr creates a new litBackendCtxMgr.
func NewLitBackendCtxMgr(path string, memQuota uint64, getProcessingJobIDs FilterProcessingJobIDsFunc) BackendCtxMgr {
	mgr := &litBackendCtxMgr{
		path:                   path,
		filterProcessingJobIDs: getProcessingJobIDs,
	}
	mgr.backends.m = make(map[int64]*litBackendCtx, 4)
	mgr.memRoot = NewMemRootImpl(int64(memQuota), mgr)
	mgr.diskRoot = NewDiskRootImpl(path, mgr)
	LitMemRoot = mgr.memRoot
	litDiskRoot = mgr.diskRoot
	litDiskRoot.UpdateUsage()
	err := litDiskRoot.StartupCheck()
	if err != nil {
		ddllogutil.DDLIngestLogger().Warn("ingest backfill may not be available", zap.Error(err))
	}
	return mgr
}

// CheckMoreTasksAvailable implements BackendCtxMgr.CheckMoreTaskAvailable interface.
func (m *litBackendCtxMgr) CheckMoreTasksAvailable(ctx context.Context) (bool, error) {
	m.cleanupSortPath(ctx)
	if err := m.diskRoot.PreCheckUsage(); err != nil {
		ddllogutil.DDLIngestLogger().Info("ingest backfill is not available", zap.Error(err))
		return false, err
	}
	return true, nil
}

// ResignOwnerForTest is only used for test.
var ResignOwnerForTest = atomic.NewBool(false)

// Register creates a new backend and registers it to the backend context.
func (m *litBackendCtxMgr) Register(
	ctx context.Context,
	jobID int64,
	unique bool,
	etcdClient *clientv3.Client,
	pdSvcDiscovery pd.ServiceDiscovery,
	resourceGroupName string,
) (BackendCtx, error) {
	bc, exist := m.Load(jobID)
	if exist {
		return bc, nil
	}

	m.memRoot.RefreshConsumption()
	ok := m.memRoot.CheckConsume(structSizeBackendCtx)
	if !ok {
		return nil, genBackendAllocMemFailedErr(ctx, m.memRoot, jobID)
	}
	cfg, err := genConfig(ctx, m.encodeJobSortPath(jobID), m.memRoot, unique, resourceGroupName)
	if err != nil {
		logutil.Logger(ctx).Warn(LitWarnConfigError, zap.Int64("job ID", jobID), zap.Error(err))
		return nil, err
	}
	failpoint.Inject("beforeCreateLocalBackend", func() {
		ResignOwnerForTest.Store(true)
	})
	// lock backends because createLocalBackend will let lightning create the sort
	// folder, which may cause cleanupSortPath wrongly delete the sort folder if only
	// checking the existence of the entry in backends.
	m.backends.mu.Lock()
	bd, err := createLocalBackend(ctx, cfg, pdSvcDiscovery)
	if err != nil {
		m.backends.mu.Unlock()
		logutil.Logger(ctx).Error(LitErrCreateBackendFail, zap.Int64("job ID", jobID), zap.Error(err))
		return nil, err
	}

	bcCtx := newBackendContext(ctx, jobID, bd, cfg.lightning, defaultImportantVariables, m.memRoot, m.diskRoot, etcdClient)
	m.backends.m[jobID] = bcCtx
	m.memRoot.Consume(structSizeBackendCtx)
	m.backends.mu.Unlock()

	logutil.Logger(ctx).Info(LitInfoCreateBackend, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", m.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", m.memRoot.MaxMemoryQuota()),
		zap.Bool("is unique index", unique))
	return bcCtx, nil
}

func (m *litBackendCtxMgr) encodeJobSortPath(jobID int64) string {
	return filepath.Join(m.path, encodeBackendTag(jobID))
}

// cleanupSortPath is used to clean up the temp data of the previous jobs.
// Because we don't remove all the files after the support of checkpoint, there
// maybe some stale files in the sort path if TiDB is killed during the backfill
// process.
func (m *litBackendCtxMgr) cleanupSortPath(ctx context.Context) {
	err := os.MkdirAll(m.path, 0700)
	if err != nil {
		logutil.Logger(ctx).Error(LitErrCreateDirFail, zap.Error(err))
		return
	}
	entries, err := os.ReadDir(m.path)
	if err != nil {
		logutil.Logger(ctx).Error(LitErrReadSortPath, zap.Error(err))
		return
	}
	toCheckJobIDs := make(map[int64]struct{}, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		jobID, err := decodeBackendTag(entry.Name())
		if err != nil {
			logutil.Logger(ctx).Error(LitErrCleanSortPath, zap.Error(err))
			continue
		}
		if _, ok := m.Load(jobID); ok {
			continue
		}
		toCheckJobIDs[jobID] = struct{}{}
	}

	if len(toCheckJobIDs) == 0 {
		return
	}

	idSlice := maps.Keys(toCheckJobIDs)
	slices.Sort(idSlice)
	processing, err := m.filterProcessingJobIDs(idSlice)
	if err != nil {
		logutil.Logger(ctx).Error(LitErrCleanSortPath, zap.Error(err))
		return
	}

	for _, id := range processing {
		delete(toCheckJobIDs, id)
	}

	if len(toCheckJobIDs) == 0 {
		return
	}

	for id := range toCheckJobIDs {
		logutil.Logger(ctx).Info("remove stale temp index data",
			zap.Int64("jobID", id))
		err = os.RemoveAll(m.encodeJobSortPath(id))
		if err != nil {
			logutil.Logger(ctx).Error(LitErrCleanSortPath, zap.Error(err))
		}
	}

	failpoint.Inject("ownerResignAfterDispatchLoopCheck", func() {
		close(local.WaitRMFolderChForTest)
	})
}

func createLocalBackend(
	ctx context.Context,
	cfg *litConfig,
	pdSvcDiscovery pd.ServiceDiscovery,
) (*local.Backend, error) {
	tls, err := cfg.lightning.ToTLS()
	if err != nil {
		logutil.Logger(ctx).Error(LitErrCreateBackendFail, zap.Error(err))
		return nil, err
	}

	ddllogutil.DDLIngestLogger().Info("create local backend for adding index",
		zap.String("sortDir", cfg.lightning.TikvImporter.SortedKVDir),
		zap.String("keyspaceName", cfg.keyspaceName))
	// We disable the switch TiKV mode feature for now,
	// because the impact is not fully tested.
	var raftKV2SwitchModeDuration time.Duration
	backendConfig := local.NewBackendConfig(cfg.lightning, int(litRLimit), cfg.keyspaceName, cfg.resourceGroup, kvutil.ExplicitTypeDDL, raftKV2SwitchModeDuration)
	return local.NewBackend(ctx, tls, backendConfig, pdSvcDiscovery)
}

const checkpointUpdateInterval = 10 * time.Minute

func newBackendContext(
	ctx context.Context,
	jobID int64,
	be *local.Backend,
	cfg *config.Config,
	vars map[string]string,
	memRoot MemRoot,
	diskRoot DiskRoot,
	etcdClient *clientv3.Client,
) *litBackendCtx {
	bCtx := &litBackendCtx{
		engines:        make(map[int64]*engineInfo, 10),
		memRoot:        memRoot,
		diskRoot:       diskRoot,
		jobID:          jobID,
		backend:        be,
		ctx:            ctx,
		cfg:            cfg,
		sysVars:        vars,
		updateInterval: checkpointUpdateInterval,
		etcdClient:     etcdClient,
	}
	bCtx.timeOfLastFlush.Store(time.Now())
	return bCtx
}

// Unregister removes a backend context from the backend context manager.
func (m *litBackendCtxMgr) Unregister(jobID int64) {
	m.backends.mu.RLock()
	_, exist := m.backends.m[jobID]
	m.backends.mu.RUnlock()
	if !exist {
		return
	}

	m.backends.mu.Lock()
	defer m.backends.mu.Unlock()
	bc, exist := m.backends.m[jobID]
	if !exist {
		return
	}
	bc.UnregisterEngines()
	bc.backend.Close()
	if bc.checkpointMgr != nil {
		bc.checkpointMgr.Close()
	}
	m.memRoot.Release(structSizeBackendCtx)
	m.memRoot.ReleaseWithTag(encodeBackendTag(jobID))
	logutil.Logger(bc.ctx).Info(LitInfoCloseBackend, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", m.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", m.memRoot.MaxMemoryQuota()))
	delete(m.backends.m, jobID)
}

func (m *litBackendCtxMgr) Load(jobID int64) (BackendCtx, bool) {
	m.backends.mu.RLock()
	defer m.backends.mu.RUnlock()
	ret, ok := m.backends.m[jobID]
	return ret, ok
}

// TotalDiskUsage returns the total disk usage of all backends.
func (m *litBackendCtxMgr) TotalDiskUsage() uint64 {
	var totalDiskUsed uint64
	m.backends.mu.RLock()
	defer m.backends.mu.RUnlock()

	for _, bc := range m.backends.m {
		_, _, bcDiskUsed, _ := local.CheckDiskQuota(bc.backend, math.MaxInt64)
		totalDiskUsed += uint64(bcDiskUsed)
	}
	return totalDiskUsed
}

// UpdateMemoryUsage collects the memory usages from all the backend and updates it to the memRoot.
func (m *litBackendCtxMgr) UpdateMemoryUsage() {
	m.backends.mu.RLock()
	defer m.backends.mu.RUnlock()

	for _, bc := range m.backends.m {
		curSize := bc.backend.TotalMemoryConsume()
		m.memRoot.ReleaseWithTag(encodeBackendTag(bc.jobID))
		m.memRoot.ConsumeWithTag(encodeBackendTag(bc.jobID), curSize)
	}
}

// encodeBackendTag encodes the job ID to backend tag.
// The backend tag is also used as the file name of the local index data files.
func encodeBackendTag(jobID int64) string {
	return strconv.FormatInt(jobID, 10)
}

// decodeBackendTag decodes the backend tag to job ID.
func decodeBackendTag(name string) (int64, error) {
	return strconv.ParseInt(name, 10, 64)
}
