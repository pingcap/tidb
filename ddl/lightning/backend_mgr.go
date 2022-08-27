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

package lightning

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/pingcap/errors"
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
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type backendCtxManager struct {
	resourceManager[BackendContext]
	MemRoot MemRoot
}

func (m *backendCtxManager) init(memRoot MemRoot) {
	m.resourceManager.init(10)
	m.MemRoot = memRoot
}

// Register creates a new backend and registers it to the backend context.
func (m *backendCtxManager) Register(ctx context.Context, unique bool, jobID int64, _ mysql.SQLMode) (*BackendContext, error) {
	// Firstly, get backend context from backend cache.
	bc, exist := m.Load(jobID)
	// If bc not exist, build a new backend for reorg task, otherwise reuse exist backend
	// to continue the task.
	if !exist {
		m.MemRoot.RefreshConsumption()
		ok := m.MemRoot.TestConsume(StructSizeBackendCtx)
		if !ok {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("backend key", jobID),
				zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
				zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)))
			return nil, errors.New(LitErrOutMaxMem)
		}
		cfg, err := generateLightningConfig(m.MemRoot, jobID, unique)
		if err != nil {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.Int64("backend key", jobID),
				zap.String("Generate config for lightning error:", err.Error()))
			return nil, err
		}
		bd, err := createLocalBackend(ctx, cfg, glueLit{})
		if err != nil {
			logutil.BgLogger().Error(LitErrCreateBackendFail, zap.Int64("backend key", jobID),
				zap.String("Error", err.Error()), zap.Stack("stack trace"))
			return nil, err
		}

		// Init important variables
		sysVars := obtainImportantVariables()

		bcCtx := newBackendContext(ctx, jobID, &bd, cfg, sysVars, m.MemRoot)
		m.Store(jobID, bcCtx)

		// Count memory usage.
		m.MemRoot.Consume(StructSizeBackendCtx)
		logutil.BgLogger().Info(LitInfoCreateBackend, zap.Int64("backend key", jobID),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)),
			zap.String("Unique Index:", strconv.FormatBool(unique)))
		return bcCtx, nil
	}
	return bc, nil
}

// Load loads a backend context.
func (m *backendCtxManager) Load(jobID int64) (*BackendContext, bool) {
	key := GenBackendContextKey(jobID)
	return m.resourceManager.Load(key)
}

// Store stores a backend context.
func (m *backendCtxManager) Store(jobID int64, bc *BackendContext) {
	key := GenBackendContextKey(jobID)
	m.resourceManager.Store(key, bc)
}

// Drop removes a backend context.
func (m *backendCtxManager) Drop(jobID int64) {
	key := GenBackendContextKey(jobID)
	m.resourceManager.Drop(key)
}

func createLocalBackend(ctx context.Context, cfg *config.Config, glue glue.Glue) (backend.Backend, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateBackendFail, zap.Error(err))
		return backend.Backend{}, err
	}

	errorMgr := errormanager.New(nil, cfg, log.Logger{Logger: logutil.BgLogger()})
	return local.NewLocalBackend(ctx, tls, cfg, glue, int(GlobalEnv.limit), errorMgr)
}

func newBackendContext(ctx context.Context, jobID int64, be *backend.Backend,
	cfg *config.Config, vars map[string]string, memRoot MemRoot) *BackendContext {
	key := GenBackendContextKey(jobID)
	bc := &BackendContext{
		key:     key,
		backend: be,
		ctx:     ctx,
		cfg:     cfg,
		sysVars: vars,
	}
	bc.EngMgr.init(memRoot)
	return bc
}

// Unregister removes a backend context from the backend context manager.
func (m *backendCtxManager) Unregister(jobID int64) {
	bc, exist := m.Load(jobID)
	if !exist {
		return
	}
	bc.EngMgr.UnregisterAll()
	bc.backend.Close()
	m.MemRoot.Release(StructSizeBackendCtx)
	m.Drop(jobID)
	m.MemRoot.ReleaseWithTag(bc.key)
	logutil.BgLogger().Info(LitInfoCloseBackend, zap.Int64("backend key", jobID),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.MemRoot.CurrentUsage(), 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.MemRoot.MaxMemoryQuota(), 10)))
}

// CheckDiskQuota checks the disk quota.
func (m *backendCtxManager) CheckDiskQuota(quota int64) int64 {
	var totalDiskUsed int64
	for _, key := range m.Keys() {
		bc, exists := m.resourceManager.Load(key)
		if exists {
			_, _, bcDiskUsed, _ := bc.backend.CheckDiskQuota(quota)
			totalDiskUsed += bcDiskUsed
		}
	}
	return totalDiskUsed
}

// UpdateMemoryUsage collects the memory usages from all the backend and updates it to the memRoot.
func (m *backendCtxManager) UpdateMemoryUsage() {
	for _, key := range m.Keys() {
		bc, exists := m.resourceManager.Load(key)
		if exists {
			curSize := bc.backend.TotalMemoryConsume()
			m.MemRoot.ReleaseWithTag(bc.key)
			m.MemRoot.ConsumeWithTag(bc.key, curSize)
		}
	}
}

// GenBackendContextKey generate a backend key from job id for a DDL job.
func GenBackendContextKey(jobID int64) string {
	return strconv.FormatInt(jobID, 10)
}

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
