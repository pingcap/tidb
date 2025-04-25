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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/logutil"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend"
	"github.com/pingcap/tidb/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessiontxn"
	"github.com/pingcap/tidb/pkg/table"
	"go.uber.org/zap"
)

// NewMockBackendCtx creates a MockBackendCtx.
func NewMockBackendCtx(job *model.Job, sessCtx sessionctx.Context, cpMgr *CheckpointManager) BackendCtx {
	logutil.DDLIngestLogger().Info("mock backend mgr register", zap.Int64("jobID", job.ID))
	mockCtx := &MockBackendCtx{
		mu:            sync.Mutex{},
		sessCtx:       sessCtx,
		jobID:         job.ID,
		checkpointMgr: cpMgr,
	}
	return mockCtx
}

// MockBackendCtx is a mock backend context.
type MockBackendCtx struct {
	sessCtx       sessionctx.Context
	mu            sync.Mutex
	jobID         int64
	checkpointMgr *CheckpointManager
}

// Register implements BackendCtx.Register interface.
func (m *MockBackendCtx) Register(indexIDs []int64, _ []bool, _ table.Table) ([]Engine, error) {
	logutil.DDLIngestLogger().Info("mock backend ctx register", zap.Int64("jobID", m.jobID), zap.Int64s("indexIDs", indexIDs))
	ret := make([]Engine, 0, len(indexIDs))
	for range indexIDs {
		ret = append(ret, &MockEngineInfo{sessCtx: m.sessCtx, mu: &m.mu})
	}
	err := sessiontxn.NewTxn(context.Background(), m.sessCtx)
	if err != nil {
		return nil, err
	}
	m.sessCtx.GetSessionVars().SetInTxn(true)
	return ret, nil
}

// FinishAndUnregisterEngines implements BackendCtx interface.
func (m *MockBackendCtx) FinishAndUnregisterEngines(_ UnregisterOpt) error {
	m.sessCtx.StmtCommit(context.Background())
	err := m.sessCtx.CommitTxn(context.Background())
	logutil.DDLIngestLogger().Info("mock backend ctx unregister", zap.Error(err))
	return nil
}

// CollectRemoteDuplicateRows implements BackendCtx.CollectRemoteDuplicateRows interface.
func (*MockBackendCtx) CollectRemoteDuplicateRows(indexID int64, _ table.Table) error {
	logutil.DDLIngestLogger().Info("mock backend ctx collect remote duplicate rows", zap.Int64("indexID", indexID))
	return nil
}

// IngestIfQuotaExceeded implements BackendCtx.IngestIfQuotaExceeded interface.
func (m *MockBackendCtx) IngestIfQuotaExceeded(_ context.Context, taskID, cnt int) error {
	if m.checkpointMgr != nil {
		m.checkpointMgr.UpdateWrittenKeys(taskID, cnt)
	}
	return nil
}

// Ingest implements BackendCtx.Ingest interface.
func (m *MockBackendCtx) Ingest(_ context.Context) error {
	if m.checkpointMgr != nil {
		return m.checkpointMgr.AdvanceWatermark(true)
	}
	return nil
}

// NextStartKey implements CheckpointOperator interface.
func (m *MockBackendCtx) NextStartKey() kv.Key {
	if m.checkpointMgr != nil {
		return m.checkpointMgr.NextKeyToProcess()
	}
	return nil
}

// TotalKeyCount implements CheckpointOperator interface.
func (m *MockBackendCtx) TotalKeyCount() int {
	if m.checkpointMgr != nil {
		return m.checkpointMgr.TotalKeyCount()
	}
	return 0
}

// AddChunk implements CheckpointOperator interface.
func (m *MockBackendCtx) AddChunk(id int, endKey kv.Key) {
	if m.checkpointMgr != nil {
		m.checkpointMgr.Register(id, endKey)
	}
}

// UpdateChunk implements CheckpointOperator interface.
func (m *MockBackendCtx) UpdateChunk(id int, count int, done bool) {
	if m.checkpointMgr != nil {
		m.checkpointMgr.UpdateTotalKeys(id, count, done)
	}
}

// FinishChunk implements CheckpointOperator interface.
func (m *MockBackendCtx) FinishChunk(id int, count int) {
	if m.checkpointMgr != nil {
		m.checkpointMgr.UpdateWrittenKeys(id, count)
	}
}

// GetImportTS implements CheckpointOperator interface.
func (m *MockBackendCtx) GetImportTS() uint64 {
	if m.checkpointMgr != nil {
		return m.checkpointMgr.GetTS()
	}
	return 0
}

// AdvanceWatermark implements CheckpointOperator interface.
func (m *MockBackendCtx) AdvanceWatermark(imported bool) error {
	if m.checkpointMgr != nil {
		return m.checkpointMgr.AdvanceWatermark(imported)
	}
	return nil
}

// GetLocalBackend returns the local backend.
func (m *MockBackendCtx) GetLocalBackend() *local.Backend {
	b := &local.Backend{}
	b.LocalStoreDir = filepath.Join(os.TempDir(), "mock_backend", strconv.FormatInt(m.jobID, 10))
	return b
}

// Close implements BackendCtx.
func (m *MockBackendCtx) Close() {
	logutil.DDLIngestLogger().Info("mock backend context close", zap.Int64("jobID", m.jobID))
	BackendCounterForTest.Dec()
}

// GetDiskUsage returns current disk usage of underlying backend.
func (bc *MockBackendCtx) GetDiskUsage() uint64 {
	return 0
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

// Close implements Engine.Close interface.
func (*MockEngineInfo) Close(_ bool) {
}

// SetHook set the write hook.
func (m *MockEngineInfo) SetHook(onWrite func(key, val []byte)) {
	m.onWrite = onWrite
}

// CreateWriter implements Engine.CreateWriter interface.
func (m *MockEngineInfo) CreateWriter(id int, _ *backend.LocalWriterConfig) (Writer, error) {
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

	failpoint.InjectCall("onMockWriterWriteRow")
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
	failpoint.InjectCall("afterMockWriterWriteRow")
	return nil
}

// LockForWrite implements Writer.LockForWrite interface.
func (*MockWriter) LockForWrite() func() {
	return func() {}
}

// MockExecAfterWriteRow is only used for test.
var MockExecAfterWriteRow func()
