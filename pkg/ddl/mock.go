// Copyright 2017 PingCAP, Inc.
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

package ddl

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/syncer"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	clientv3 "go.etcd.io/etcd/client/v3"
	atomicutil "go.uber.org/atomic"
)

// SetBatchInsertDeleteRangeSize sets the batch insert/delete range size in the test
func SetBatchInsertDeleteRangeSize(i int) {
	batchInsertDeleteRangeSize = i
}

var _ syncer.SchemaSyncer = &MockSchemaSyncer{}

const mockCheckVersInterval = 2 * time.Millisecond

// MockSchemaSyncer is a mock schema syncer, it is exported for testing.
type MockSchemaSyncer struct {
	selfSchemaVersion int64
	mdlSchemaVersions sync.Map
	globalVerCh       chan clientv3.WatchResponse
	mockSession       chan struct{}
}

// NewMockSchemaSyncer creates a new mock SchemaSyncer.
func NewMockSchemaSyncer() syncer.SchemaSyncer {
	return &MockSchemaSyncer{}
}

// Init implements SchemaSyncer.Init interface.
func (s *MockSchemaSyncer) Init(_ context.Context) error {
	s.mdlSchemaVersions = sync.Map{}
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	s.mockSession = make(chan struct{}, 1)
	return nil
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *MockSchemaSyncer) GlobalVersionCh() clientv3.WatchChan {
	return s.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (*MockSchemaSyncer) WatchGlobalSchemaVer(context.Context) {}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *MockSchemaSyncer) UpdateSelfVersion(_ context.Context, jobID int64, version int64) error {
	failpoint.Inject("mockUpdateMDLToETCDError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock update mdl to etcd error"))
		}
	})
	if variable.EnableMDL.Load() {
		s.mdlSchemaVersions.Store(jobID, version)
	} else {
		atomic.StoreInt64(&s.selfSchemaVersion, version)
	}
	return nil
}

// Done implements SchemaSyncer.Done interface.
func (s *MockSchemaSyncer) Done() <-chan struct{} {
	return s.mockSession
}

// CloseSession mockSession, it is exported for testing.
func (s *MockSchemaSyncer) CloseSession() {
	close(s.mockSession)
}

// Restart implements SchemaSyncer.Restart interface.
func (s *MockSchemaSyncer) Restart(_ context.Context) error {
	s.mockSession = make(chan struct{}, 1)
	return nil
}

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *MockSchemaSyncer) OwnerUpdateGlobalVersion(_ context.Context, _ int64) error {
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
	default:
	}
	return nil
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *MockSchemaSyncer) OwnerCheckAllVersions(ctx context.Context, jobID int64, latestVer int64) error {
	ticker := time.NewTicker(mockCheckVersInterval)
	defer ticker.Stop()

	failpoint.Inject("mockOwnerCheckAllVersionSlow", func(val failpoint.Value) {
		if v, ok := val.(int); ok && v == int(jobID) {
			time.Sleep(2 * time.Second)
		}
	})

	for {
		select {
		case <-ctx.Done():
			failpoint.Inject("checkOwnerCheckAllVersionsWaitTime", func(v failpoint.Value) {
				if v.(bool) {
					panic("shouldn't happen")
				}
			})
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			if variable.EnableMDL.Load() {
				ver, ok := s.mdlSchemaVersions.Load(jobID)
				if ok && ver.(int64) >= latestVer {
					return nil
				}
			} else {
				ver := atomic.LoadInt64(&s.selfSchemaVersion)
				if ver >= latestVer {
					return nil
				}
			}
		}
	}
}

// SyncJobSchemaVerLoop implements SchemaSyncer.SyncJobSchemaVerLoop interface.
func (*MockSchemaSyncer) SyncJobSchemaVerLoop(context.Context) {
}

// Close implements SchemaSyncer.Close interface.
func (*MockSchemaSyncer) Close() {}

// NewMockStateSyncer creates a new mock StateSyncer.
func NewMockStateSyncer() syncer.StateSyncer {
	return &MockStateSyncer{}
}

// clusterState mocks cluster state.
// We move it from MockStateSyncer to here. Because we want to make it unaffected by ddl close.
var clusterState *atomicutil.Pointer[syncer.StateInfo]

// MockStateSyncer is a mock state syncer, it is exported for testing.
type MockStateSyncer struct {
	globalVerCh chan clientv3.WatchResponse
	mockSession chan struct{}
}

// Init implements StateSyncer.Init interface.
func (s *MockStateSyncer) Init(context.Context) error {
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	s.mockSession = make(chan struct{}, 1)
	state := syncer.NewStateInfo(syncer.StateNormalRunning)
	if clusterState == nil {
		clusterState = atomicutil.NewPointer(state)
	}
	return nil
}

// UpdateGlobalState implements StateSyncer.UpdateGlobalState interface.
func (s *MockStateSyncer) UpdateGlobalState(_ context.Context, stateInfo *syncer.StateInfo) error {
	failpoint.Inject("mockUpgradingState", func(val failpoint.Value) {
		if val.(bool) {
			clusterState.Store(stateInfo)
			failpoint.Return(nil)
		}
	})
	s.globalVerCh <- clientv3.WatchResponse{}
	clusterState.Store(stateInfo)
	return nil
}

// GetGlobalState implements StateSyncer.GetGlobalState interface.
func (*MockStateSyncer) GetGlobalState(context.Context) (*syncer.StateInfo, error) {
	return clusterState.Load(), nil
}

// IsUpgradingState implements StateSyncer.IsUpgradingState interface.
func (*MockStateSyncer) IsUpgradingState() bool {
	return clusterState.Load().State == syncer.StateUpgrading
}

// WatchChan implements StateSyncer.WatchChan interface.
func (s *MockStateSyncer) WatchChan() clientv3.WatchChan {
	return s.globalVerCh
}

// Rewatch implements StateSyncer.Rewatch interface.
func (*MockStateSyncer) Rewatch(context.Context) {}

type mockDelRange struct {
}

// newMockDelRangeManager creates a mock delRangeManager only used for test.
func newMockDelRangeManager() delRangeManager {
	return &mockDelRange{}
}

// addDelRangeJob implements delRangeManager interface.
func (*mockDelRange) addDelRangeJob(_ context.Context, _ *model.Job) error {
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (*mockDelRange) removeFromGCDeleteRange(_ context.Context, _ int64) error {
	return nil
}

// start implements delRangeManager interface.
func (*mockDelRange) start() {}

// clear implements delRangeManager interface.
func (*mockDelRange) clear() {}

// MockTableInfo mocks a table info by create table stmt ast and a specified table id.
func MockTableInfo(ctx sessionctx.Context, stmt *ast.CreateTableStmt, tableID int64) (*model.TableInfo, error) {
	chs, coll := charset.GetDefaultCharsetAndCollate()
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, stmt.Cols, stmt.Constraints, chs, coll)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl, err := BuildTableInfo(ctx, stmt.Table.Name, cols, newConstraints, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl.ID = tableID

	if err = setTableAutoRandomBits(ctx, tbl, stmt.Cols); err != nil {
		return nil, errors.Trace(err)
	}

	// The specified charset will be handled in handleTableOptions
	if err = handleTableOptions(stmt.Options, tbl); err != nil {
		return nil, errors.Trace(err)
	}

	return tbl, nil
}
