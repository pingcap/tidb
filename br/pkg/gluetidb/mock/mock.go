// Copyright 2024 PingCAP, Inc.
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

package mock

import (
	"context"
	"log"

	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx"
	pd "github.com/tikv/pd/client"
)

// mockSession is used for test.
type mockSession struct {
	se         sessiontypes.Session
	globalVars map[string]string
}

// GetSessionCtx implements glue.Glue
func (s *mockSession) GetSessionCtx() sessionctx.Context {
	return s.se
}

// Execute implements glue.Session.
func (s *mockSession) Execute(ctx context.Context, sql string) error {
	return s.ExecuteInternal(ctx, sql)
}

func (s *mockSession) ExecuteInternal(ctx context.Context, sql string, args ...any) error {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnBR)
	rs, err := s.se.ExecuteInternal(ctx, sql, args...)
	if err != nil {
		return err
	}
	// Some of SQLs (like ADMIN RECOVER INDEX) may lazily take effect
	// when we are polling the result set.
	// At least call `next` once for triggering theirs side effect.
	// (Maybe we'd better drain all returned rows?)
	if rs != nil {
		//nolint: errcheck
		defer rs.Close()
		c := rs.NewChunk(nil)
		if err := rs.Next(ctx, c); err != nil {
			return nil
		}
	}
	return nil
}

// CreateDatabase implements glue.Session.
func (*mockSession) CreateDatabase(_ context.Context, _ *model.DBInfo) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// CreatePlacementPolicy implements glue.Session.
func (*mockSession) CreatePlacementPolicy(_ context.Context, _ *model.PolicyInfo) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// CreateTables implements glue.BatchCreateTableSession.
func (*mockSession) CreateTables(_ context.Context, _ map[string][]*model.TableInfo,
	_ ...ddl.CreateTableWithInfoConfigurier) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// CreateTable implements glue.Session.
func (*mockSession) CreateTable(_ context.Context, _ model.CIStr,
	_ *model.TableInfo, _ ...ddl.CreateTableWithInfoConfigurier) error {
	log.Fatal("unimplemented CreateDatabase for mock session")
	return nil
}

// Close implements glue.Session.
func (s *mockSession) Close() {
	s.se.Close()
}

// GetGlobalVariables implements glue.Session.
func (s *mockSession) GetGlobalVariable(name string) (string, error) {
	if ret, ok := s.globalVars[name]; ok {
		return ret, nil
	}
	return "True", nil
}

// MockGlue only used for test
type MockGlue struct {
	se         sessiontypes.Session
	GlobalVars map[string]string
}

func (m *MockGlue) SetSession(se sessiontypes.Session) {
	m.se = se
}

// GetDomain implements glue.Glue.
func (*MockGlue) GetDomain(store kv.Storage) (*domain.Domain, error) {
	return nil, nil
}

// CreateSession implements glue.Glue.
func (m *MockGlue) CreateSession(store kv.Storage) (glue.Session, error) {
	glueSession := &mockSession{
		se:         m.se,
		globalVars: m.GlobalVars,
	}
	return glueSession, nil
}

// Open implements glue.Glue.
func (*MockGlue) Open(path string, option pd.SecurityOption) (kv.Storage, error) {
	return nil, nil
}

// OwnsStorage implements glue.Glue.
func (*MockGlue) OwnsStorage() bool {
	return true
}

// StartProgress implements glue.Glue.
func (*MockGlue) StartProgress(ctx context.Context, cmdName string, total int64, redirectLog bool) glue.Progress {
	return nil
}

// Record implements glue.Glue.
func (*MockGlue) Record(name string, value uint64) {
}

// GetVersion implements glue.Glue.
func (*MockGlue) GetVersion() string {
	return "mock glue"
}

// UseOneShotSession implements glue.Glue.
func (m *MockGlue) UseOneShotSession(store kv.Storage, closeDomain bool, fn func(glue.Session) error) error {
	glueSession := &mockSession{
		se: m.se,
	}
	return fn(glueSession)
}
