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
	"sync"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
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

// CheckAvailable implements BackendCtxMgr.Available interface.
func (*MockBackendCtxMgr) CheckAvailable() (bool, error) {
	return true, nil
}

// Register implements BackendCtxMgr.Register interface.
func (m *MockBackendCtxMgr) Register(_ context.Context, _ bool, jobID int64) (BackendCtx, error) {
	logutil.BgLogger().Info("mock backend mgr register", zap.Int64("jobID", jobID))
	if mockCtx, ok := m.runningJobs[jobID]; ok {
		return mockCtx, nil
	}
	sessCtx := m.sessCtxProvider()
	mockCtx := &MockBackendCtx{
		mu:      sync.Mutex{},
		sessCtx: sessCtx,
	}
	m.runningJobs[jobID] = mockCtx
	return mockCtx, nil
}

// Unregister implements BackendCtxMgr.Unregister interface.
func (m *MockBackendCtxMgr) Unregister(jobID int64) {
	if mCtx, ok := m.runningJobs[jobID]; ok {
		mCtx.sessCtx.StmtCommit(context.Background())
		err := mCtx.sessCtx.CommitTxn(context.Background())
		logutil.BgLogger().Info("mock backend mgr unregister", zap.Int64("jobID", jobID), zap.Error(err))
		delete(m.runningJobs, jobID)
	}
}

// Load implements BackendCtxMgr.Load interface.
func (m *MockBackendCtxMgr) Load(jobID int64) (BackendCtx, bool) {
	logutil.BgLogger().Info("mock backend mgr load", zap.Int64("jobID", jobID))
	if mockCtx, ok := m.runningJobs[jobID]; ok {
		return mockCtx, true
	}
	return nil, false
}

// MockBackendCtx is a mock backend context.
type MockBackendCtx struct {
	sessCtx sessionctx.Context
	mu      sync.Mutex
}

// Register implements BackendCtx.Register interface.
func (m *MockBackendCtx) Register(jobID, indexID int64, _, _ string) (Engine, error) {
	logutil.BgLogger().Info("mock backend ctx register", zap.Int64("jobID", jobID), zap.Int64("indexID", indexID))
	return &MockEngineInfo{sessCtx: m.sessCtx, mu: &m.mu}, nil
}

// Unregister implements BackendCtx.Unregister interface.
func (*MockBackendCtx) Unregister(jobID, indexID int64) {
	logutil.BgLogger().Info("mock backend ctx unregister", zap.Int64("jobID", jobID), zap.Int64("indexID", indexID))
}

// CollectRemoteDuplicateRows implements BackendCtx.CollectRemoteDuplicateRows interface.
func (*MockBackendCtx) CollectRemoteDuplicateRows(indexID int64, _ table.Table) error {
	logutil.BgLogger().Info("mock backend ctx collect remote duplicate rows", zap.Int64("indexID", indexID))
	return nil
}

// FinishImport implements BackendCtx.FinishImport interface.
func (*MockBackendCtx) FinishImport(indexID int64, _ bool, _ table.Table) error {
	logutil.BgLogger().Info("mock backend ctx finish import", zap.Int64("indexID", indexID))
	return nil
}

// ImportAndClean implements BackendCtx.ImportAndClean interface.
func (*MockBackendCtx) ImportAndClean(indexID int64) error {
	logutil.BgLogger().Info("mock backend ctx import and clean", zap.Int64("indexID", indexID))
	return nil
}

// ResetWorkers implements BackendCtx.ResetWorkers interface.
func (*MockBackendCtx) ResetWorkers(_, _ int64) {
}

// Flush implements BackendCtx.Flush interface.
func (*MockBackendCtx) Flush(_ int64, _ bool) (flushed bool, imported bool, err error) {
	return false, false, nil
}

// Done implements BackendCtx.Done interface.
func (*MockBackendCtx) Done() bool {
	return false
}

// SetDone implements BackendCtx.SetDone interface.
func (*MockBackendCtx) SetDone() {
}

// MockEngineInfo is a mock engine info.
type MockEngineInfo struct {
	sessCtx sessionctx.Context
	mu      *sync.Mutex
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

// CreateWriter implements Engine.CreateWriter interface.
func (m *MockEngineInfo) CreateWriter(id int, _ bool) (Writer, error) {
	logutil.BgLogger().Info("mock engine info create writer", zap.Int("id", id))
	return &MockWriter{sessCtx: m.sessCtx, mu: m.mu}, nil
}

// MockWriter is a mock writer.
type MockWriter struct {
	sessCtx sessionctx.Context
	mu      *sync.Mutex
}

// WriteRow implements Writer.WriteRow interface.
func (m *MockWriter) WriteRow(key, idxVal []byte, _ kv.Handle) error {
	logutil.BgLogger().Info("mock writer write row",
		zap.String("key", hex.EncodeToString(key)),
		zap.String("idxVal", hex.EncodeToString(idxVal)))
	m.mu.Lock()
	defer m.mu.Unlock()
	txn, err := m.sessCtx.Txn(true)
	if err != nil {
		return err
	}
	return txn.Set(key, idxVal)
}
