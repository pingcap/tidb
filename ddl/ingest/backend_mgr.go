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
	"database/sql"
	"fmt"
	"math"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/errormanager"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/generic"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type backendCtxManager struct {
	generic.SyncMap[int64, *BackendContext]
	memRoot  MemRoot
	diskRoot DiskRoot
}

func (m *backendCtxManager) init(memRoot MemRoot, diskRoot DiskRoot) {
	m.SyncMap = generic.NewSyncMap[int64, *BackendContext](10)
	m.memRoot = memRoot
	m.diskRoot = diskRoot
}

// Register creates a new backend and registers it to the backend context.
func (m *backendCtxManager) Register(ctx context.Context, unique bool, jobID int64, _ mysql.SQLMode) (*BackendContext, error) {
	bc, exist := m.Load(jobID)
	if !exist {
		m.memRoot.RefreshConsumption()
		ok := m.memRoot.CheckConsume(StructSizeBackendCtx)
		if !ok {
			return nil, genBackendAllocMemFailedErr(m.memRoot, jobID)
		}
		cfg, err := genConfig(m.memRoot, jobID, unique)
		if err != nil {
			logutil.BgLogger().Warn(LitWarnConfigError, zap.Int64("job ID", jobID), zap.Error(err))
			return nil, err
		}
		bd, err := createLocalBackend(ctx, cfg, glueLit{})
		if err != nil {
			logutil.BgLogger().Error(LitErrCreateBackendFail, zap.Int64("job ID", jobID), zap.Error(err))
			return nil, err
		}

		bcCtx := newBackendContext(ctx, jobID, &bd, cfg.Lightning, defaultImportantVariables, m.memRoot, m.diskRoot)
		m.Store(jobID, bcCtx)

		m.memRoot.Consume(StructSizeBackendCtx)
		logutil.BgLogger().Info(LitInfoCreateBackend, zap.Int64("job ID", jobID),
			zap.Int64("current memory usage", m.memRoot.CurrentUsage()),
			zap.Int64("max memory quota", m.memRoot.MaxMemoryQuota()),
			zap.Bool("is unique index", unique))
		return bcCtx, nil
	}
	return bc, nil
}

func createLocalBackend(ctx context.Context, cfg *Config, glue glue.Glue) (backend.Backend, error) {
	tls, err := cfg.Lightning.ToTLS()
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateBackendFail, zap.Error(err))
		return backend.Backend{}, err
	}

	logutil.BgLogger().Info("[ddl-ingest] create local backend for adding index", zap.String("keyspaceName", cfg.KeyspaceName))
	errorMgr := errormanager.New(nil, cfg.Lightning, log.Logger{Logger: logutil.BgLogger()})
	return local.NewLocalBackend(ctx, tls, cfg.Lightning, glue, int(LitRLimit), errorMgr, cfg.KeyspaceName)
}

func newBackendContext(ctx context.Context, jobID int64, be *backend.Backend,
	cfg *config.Config, vars map[string]string, memRoot MemRoot, diskRoot DiskRoot) *BackendContext {
	bc := &BackendContext{
		jobID:    jobID,
		backend:  be,
		ctx:      ctx,
		cfg:      cfg,
		sysVars:  vars,
		diskRoot: diskRoot,
	}
	bc.EngMgr.init(memRoot, diskRoot)
	return bc
}

// Unregister removes a backend context from the backend context manager.
func (m *backendCtxManager) Unregister(jobID int64) {
	bc, exist := m.Load(jobID)
	if !exist {
		return
	}
	bc.EngMgr.UnregisterAll(jobID)
	bc.backend.Close()
	m.memRoot.Release(StructSizeBackendCtx)
	m.Delete(jobID)
	m.memRoot.ReleaseWithTag(encodeBackendTag(jobID))
	logutil.BgLogger().Info(LitInfoCloseBackend, zap.Int64("job ID", jobID),
		zap.Int64("current memory usage", m.memRoot.CurrentUsage()),
		zap.Int64("max memory quota", m.memRoot.MaxMemoryQuota()))
}

// TotalDiskUsage returns the total disk usage of all backends.
func (m *backendCtxManager) TotalDiskUsage() uint64 {
	var totalDiskUsed uint64
	for _, key := range m.Keys() {
		bc, exists := m.Load(key)
		if exists {
			_, _, bcDiskUsed, _ := bc.backend.CheckDiskQuota(math.MaxInt64)
			totalDiskUsed += uint64(bcDiskUsed)
		}
	}
	return totalDiskUsed
}

// UpdateMemoryUsage collects the memory usages from all the backend and updates it to the memRoot.
func (m *backendCtxManager) UpdateMemoryUsage() {
	for _, key := range m.Keys() {
		bc, exists := m.Load(key)
		if exists {
			curSize := bc.backend.TotalMemoryConsume()
			m.memRoot.ReleaseWithTag(encodeBackendTag(bc.jobID))
			m.memRoot.ConsumeWithTag(encodeBackendTag(bc.jobID), curSize)
		}
	}
}

// glueLit is used as a placeholder for the local backend initialization.
type glueLit struct{}

// OwnsSQLExecutor Implement interface OwnsSQLExecutor.
func (glueLit) OwnsSQLExecutor() bool {
	return false
}

// GetSQLExecutor Implement interface GetSQLExecutor.
func (glueLit) GetSQLExecutor() glue.SQLExecutor {
	return nil
}

// GetDB Implement interface GetDB.
func (glueLit) GetDB() (*sql.DB, error) {
	return nil, nil
}

// GetParser Implement interface GetParser.
func (glueLit) GetParser() *parser.Parser {
	return nil
}

// GetTables Implement interface GetTables.
func (glueLit) GetTables(context.Context, string) ([]*model.TableInfo, error) {
	return nil, nil
}

// GetSession Implement interface GetSession.
func (glueLit) GetSession(context.Context) (checkpoints.Session, error) {
	return nil, nil
}

// OpenCheckpointsDB Implement interface OpenCheckpointsDB.
func (glueLit) OpenCheckpointsDB(context.Context, *config.Config) (checkpoints.DB, error) {
	return nil, nil
}

// Record is used to report some information (key, value) to host TiDB, including progress, stage currently.
func (glueLit) Record(string, uint64) {
}

func encodeBackendTag(jobID int64) string {
	return fmt.Sprintf("%d", jobID)
}
