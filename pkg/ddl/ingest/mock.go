// Copyright 2023 PingCAP, Inc.
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
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// MockBackendCtxMgr is a mock backend context manager.
type MockBackendCtxMgr struct {
	sessCtxProvider func() sessionctx.Context
	runningJobs     map[int64]*MockBackendCtx
}

// NewMockBackendCtxMgr creates a new mock backend context manager.
func NewMockBackendCtxMgr(sessCtxProvider func() sessionctx.Context) *MockBackendCtxMgr {
	return &MockBackendCtxMgr{
		sessCtxProvider: sessCtxProvider,
		runningJobs:     make(map[int64]*MockBackendCtx),
	}
}

// CheckMoreTasksAvailable implements BackendCtxMgr.CheckMoreTaskAvailable interface.
func (m *MockBackendCtxMgr) CheckMoreTasksAvailable(context.Context) (bool, error) {
	return len(m.runningJobs) == 0, nil
}

// Register implements BackendCtxMgr.Register interface.
func (m *MockBackendCtxMgr) Register(ctx context.Context, jobID int64, unique bool, etcdClient *clientv3.Client, pdSvcDiscovery pd.ServiceDiscovery, resourceGroupName string) (BackendCtx, error) {
	logutil.DDLIngestLogger().Info("mock backend mgr register", zap.Int64("jobID", jobID))
	if mockCtx, ok := m.runningJobs[jobID]; ok {
		return mockCtx, nil
	}
	sessCtx := m.sessCtxProvider()
	mockCtx := &MockBackendCtx{
		mu:      sync.Mutex{},
		sessCtx: sessCtx,
		jobID:   jobID,
	}
	m.runningJobs[jobID] = mockCtx
	return mockCtx, nil
}

// Unregister implements BackendCtxMgr.Unregister interface.
func (m *MockBackendCtxMgr) Unregister(jobID int64) {
	if mCtx, ok := m.runningJobs[jobID]; ok {
		mCtx.sessCtx.StmtCommit(context.Background())
		err := mCtx.sessCtx.CommitTxn(context.Background())
		logutil.DDLIngestLogger().Info("mock backend mgr unregister", zap.Int64("jobID", jobID), zap.Error(err))
		delete(m.runningJobs, jobID)
		if mCtx.checkpointMgr != nil {
			mCtx.checkpointMgr.Close()
		}
	}
}

// Load implements BackendCtxMgr.Load interface.
func (m *MockBackendCtxMgr) Load(jobID int64) (BackendCtx, bool) {
	logutil.DDLIngestLogger().Info("mock backend mgr load", zap.Int64("jobID", jobID))
	if mockCtx, ok := m.runningJobs[jobID]; ok {
		return mockCtx, true
	}
	return nil, false
}

// ResetSessCtx is only used for mocking test.
func (m *MockBackendCtxMgr) ResetSessCtx() {
	for _, mockCtx := range m.runningJobs {
		mockCtx.sessCtx = m.sessCtxProvider()
	}
}

// MockBackendCtx is a mock backend context.
type MockBackendCtx struct {
	sessCtx       sessionctx.Context
	mu            sync.Mutex
	jobID         int64
	checkpointMgr *CheckpointManager
}

// Register implements BackendCtx.Register interface.
func (m *MockBackendCtx) Register(indexIDs []int64, _ string) ([]Engine, error) {
	logutil.DDLIngestLogger().Info("mock backend ctx register", zap.Int64("jobID", m.jobID), zap.Int64s("indexIDs", indexIDs))
	ret := make([]Engine, 0, len(indexIDs))
	for range indexIDs {
		ret = append(ret, &MockEngineInfo{sessCtx: m.sessCtx, mu: &m.mu})
	}
	return ret, nil
}

// UnregisterEngines implements BackendCtx.UnregisterEngines interface.
func (*MockBackendCtx) UnregisterEngines() {
	logutil.DDLIngestLogger().Info("mock backend ctx unregister")
}

// ImportStarted implements BackendCtx interface.
func (*MockBackendCtx) ImportStarted() bool {
	return false
}

// CollectRemoteDuplicateRows implements BackendCtx.CollectRemoteDuplicateRows interface.
func (*MockBackendCtx) CollectRemoteDuplicateRows(indexID int64, _ table.Table) error {
	logutil.DDLIngestLogger().Info("mock backend ctx collect remote duplicate rows", zap.Int64("indexID", indexID))
	return nil
}

// FinishImport implements BackendCtx.FinishImport interface.
func (*MockBackendCtx) FinishImport(indexID int64, _ bool, _ table.Table) error {
	logutil.DDLIngestLogger().Info("mock backend ctx finish import", zap.Int64("indexID", indexID))
	return nil
}

// Flush implements BackendCtx.Flush interface.
func (*MockBackendCtx) Flush(_ FlushMode) (flushed bool, imported bool, errIdxID int64, err error) {
	return false, false, 0, nil
}

// Done implements BackendCtx.Done interface.
func (*MockBackendCtx) Done() bool {
	return false
}

// SetDone implements BackendCtx.SetDone interface.
func (*MockBackendCtx) SetDone() {
}

// AttachCheckpointManager attaches a checkpoint manager to the backend context.
func (m *MockBackendCtx) AttachCheckpointManager(mgr *CheckpointManager) {
	m.checkpointMgr = mgr
}

// GetCheckpointManager returns the checkpoint manager attached to the backend context.
func (m *MockBackendCtx) GetCheckpointManager() *CheckpointManager {
	return m.checkpointMgr
}

// GetLocalBackend returns the local backend.
func (m *MockBackendCtx) GetLocalBackend() *local.Backend {
	b := &local.Backend{}
	b.LocalStoreDir = filepath.Join(os.TempDir(), "mock_backend", strconv.FormatInt(m.jobID, 10))
	return b
}

// MockWriteHook the hook for write in mock engine.
type MockWriteHook func(key, val []byte)

// MockEngineInfo is a mock engine info.
type MockEngineInfo struct {
	sessCtx sessionctx.Context
	mu      *sync.Mutex

	onWrite MockWriteHook
}

// NewMockEngineInfo creates a new mock engine info.
func NewMockEngineInfo(sessCtx sessionctx.Context) *MockEngineInfo {
	return &MockEngineInfo{
		sessCtx: sessCtx,
		mu:      &sync.Mutex{},
	}
}

// Flush implements Engine.Flush interface.
func (*MockEngineInfo) Flush() error {
	return nil
}

// ImportAndClean implements Engine.ImportAndClean interface.
func (*MockEngineInfo) ImportAndClean() error {
	return nil
}

// Clean implements Engine.Clean interface.
func (*MockEngineInfo) Clean() {
}

// SetHook set the write hook.
func (m *MockEngineInfo) SetHook(onWrite func(key, val []byte)) {
	m.onWrite = onWrite
}

// CreateWriter implements Engine.CreateWriter interface.
func (m *MockEngineInfo) CreateWriter(id int) (Writer, error) {
	logutil.DDLIngestLogger().Info("mock engine info create writer", zap.Int("id", id))
	return &MockWriter{sessCtx: m.sessCtx, mu: m.mu, onWrite: m.onWrite}, nil
}

// MockWriter is a mock writer.
type MockWriter struct {
	sessCtx sessionctx.Context
	mu      *sync.Mutex
	onWrite MockWriteHook
}

// WriteRow implements Writer.WriteRow interface.
func (m *MockWriter) WriteRow(_ context.Context, key, idxVal []byte, _ kv.Handle) error {
	logutil.DDLIngestLogger().Info("mock writer write row",
		zap.String("key", hex.EncodeToString(key)),
		zap.String("idxVal", hex.EncodeToString(idxVal)))
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.onWrite != nil {
		m.onWrite(key, idxVal)
		return nil
	}
	txn, err := m.sessCtx.Txn(true)
	if err != nil {
		return err
	}
	err = txn.Set(key, idxVal)
	if err != nil {
		return err
	}
	if MockExecAfterWriteRow != nil {
		MockExecAfterWriteRow()
	}
	return nil
}

// LockForWrite implements Writer.LockForWrite interface.
func (*MockWriter) LockForWrite() func() {
	return func() {}
}

// MockExecAfterWriteRow is only used for test.
var MockExecAfterWriteRow func()
