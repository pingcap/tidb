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
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"golang.org/x/net/context"
)

var _ SchemaSyncer = &mockSchemaSyncer{}

const mockCheckVersInterval = 2 * time.Millisecond

type mockSchemaSyncer struct {
	selfSchemaVersion int64
	globalVerCh       chan clientv3.WatchResponse
}

// NewMockSchemaSyncer creates a new mock SchemaSyncer.
func NewMockSchemaSyncer() SchemaSyncer {
	return &mockSchemaSyncer{}
}

// Init implements SchemaSyncer.Init interface.
func (s *mockSchemaSyncer) Init(ctx context.Context) error {
	s.globalVerCh = make(chan clientv3.WatchResponse, 1)
	return nil
}

// GlobalVersionCh implements SchemaSyncer.GlobalVersionCh interface.
func (s *mockSchemaSyncer) GlobalVersionCh() clientv3.WatchChan {
	return s.globalVerCh
}

// WatchGlobalSchemaVer implements SchemaSyncer.WatchGlobalSchemaVer interface.
func (s *mockSchemaSyncer) WatchGlobalSchemaVer(context.Context) {}

// UpdateSelfVersion implements SchemaSyncer.UpdateSelfVersion interface.
func (s *mockSchemaSyncer) UpdateSelfVersion(ctx context.Context, version int64) error {
	atomic.StoreInt64(&s.selfSchemaVersion, version)
	return nil
}

// Done implements SchemaSyncer.Done interface.
func (s *mockSchemaSyncer) Done() <-chan struct{} {
	return make(chan struct{}, 1)
}

// Restart implements SchemaSyncer.Restart interface.
func (s *mockSchemaSyncer) Restart(_ context.Context) error { return nil }

// RemoveSelfVersionPath implements SchemaSyncer.RemoveSelfVersionPath interface.
func (s *mockSchemaSyncer) RemoveSelfVersionPath() error { return nil }

// OwnerUpdateGlobalVersion implements SchemaSyncer.OwnerUpdateGlobalVersion interface.
func (s *mockSchemaSyncer) OwnerUpdateGlobalVersion(ctx context.Context, version int64) error {
	select {
	case s.globalVerCh <- clientv3.WatchResponse{}:
	default:
	}
	return nil
}

// MustGetGlobalVersion implements SchemaSyncer.MustGetGlobalVersion interface.
func (s *mockSchemaSyncer) MustGetGlobalVersion(ctx context.Context) (int64, error) {
	return 0, nil
}

// OwnerCheckAllVersions implements SchemaSyncer.OwnerCheckAllVersions interface.
func (s *mockSchemaSyncer) OwnerCheckAllVersions(ctx context.Context, latestVer int64) error {
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

// start implements delRangeManager interface.
func (dr *mockDelRange) start() {
	return
}

// clear implements delRangeManager interface.
func (dr *mockDelRange) clear() {
	return
}

// MockTableInfo mocks a table info by create table stmt ast and a specified table id.
func MockTableInfo(ctx sessionctx.Context, stmt *ast.CreateTableStmt, tableID int64) (*model.TableInfo, error) {
	cols, newConstraints, err := buildColumnsAndConstraints(ctx, stmt.Cols, stmt.Constraints)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl, err := buildTableInfo(ctx, nil, stmt.Table.Name, cols, newConstraints)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tbl.ID = tableID
	return tbl, nil
}
