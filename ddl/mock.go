// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://wwm.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/ddl/util"
	"github.com/pingcap/tidb/v4/sessionctx"
	"go.etcd.io/etcd/clientv3"
)

var _ util.SchemaSyncer = &MockSchemaSyncer{}

const mockCheckVersInterval = 2 * time.Millisecond

// MockSchemaSyncer is a mock schema syncer, it is exported for tesing.
type MockSchemaSyncer struct {
	selfSchemaVersion int64
	globalVerCh       chan clientv3.WatchResponse
	mockSession       chan struct{}
}

// NewMockSchemaSyncer creates a new mock SchemaSyncer.
func NewMockSchemaSyncer() util.SchemaSyncer {
	return &MockSchemaSyncer{}
}

// Init implements SchemaSyncer.Init interface.
func (s *MockSchemaSyncer) Init(ctx context.Context) error {
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	s.mockSession = make(chan struct{}, 1)
	return nil
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *MockSchemaSyncer) GlobalVersionCh() clientv3.WatchChan {
	return s.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *MockSchemaSyncer) WatchGlobalSchemaVer(context.Context) {}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *MockSchemaSyncer) UpdateSelfVersion(ctx context.Context, version int64) error {
	atomic.StoreInt64(&s.selfSchemaVersion, version)
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

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *MockSchemaSyncer) RemoveSelfVersionPath() error { return nil }

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *MockSchemaSyncer) OwnerUpdateGlobalVersion(ctx context.Context, version int64) error {
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
	default:
	}
	return nil
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *MockSchemaSyncer) MustGetGlobalVersion(ctx context.Context) (int64, error) {
	return 0, nil
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *MockSchemaSyncer) OwnerCheckAllVersions(ctx context.Context, latestVer int64) error {
	ticker := time.NewTicker(mockCheckVersInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			ver := atomic.LoadInt64(&s.selfSchemaVersion)
			if ver == latestVer {
				return nil
			}
		}
	}
}

// NotifyCleanExpiredPaths implements SchemaSyncer.NotifyCleanExpiredPaths interface.
func (s *MockSchemaSyncer) NotifyCleanExpiredPaths() bool { return true }

// StartCleanWork implements SchemaSyncer.StartCleanWork interface.
func (s *MockSchemaSyncer) StartCleanWork() {}

// CloseCleanWork implements SchemaSyncer.CloseCleanWork interface.
func (s *MockSchemaSyncer) CloseCleanWork() {}

type mockDelRange struct {
}

// newMockDelRangeManager creates a mock delRangeManager only used for test.
func newMockDelRangeManager() delRangeManager {
	return &mockDelRange{}
}

// addDelRangeJob implements delRangeManager interface.
func (dr *mockDelRange) addDelRangeJob(job *model.Job) error {
	return nil
}

// removeFromGCDeleteRange implements delRangeManager interface.
func (dr *mockDelRange) removeFromGCDeleteRange(jobID int64, tableIDs []int64) error {
	return nil
}

// start implements delRangeManager interface.
func (dr *mockDelRange) start() {}

// clear implements delRangeManager interface.
func (dr *mockDelRange) clear() {}

// MockTableInfo mocks a table info by create table stmt ast and a specified table id.
func MockTableInfo(ctx sessionctx.Context, stmt *ast.CreateTableStmt, tableID int64) (*model.TableInfo, error) {
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, stmt.Cols, stmt.Constraints, "", "", "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl, err := buildTableInfo(ctx, stmt.Table.Name, cols, newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl.ID = tableID

	// The specified charset will be handled in handleTableOptions
	if err = handleTableOptions(stmt.Options, tbl); err != nil {
		return nil, errors.Trace(err)
	}

	if err = resolveDefaultTableCharsetAndCollation(tbl, "", ""); err != nil {
		return nil, errors.Trace(err)
	}

	return tbl, nil
}
