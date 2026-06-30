// Copyright 2026 PingCAP, Inc.
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

package tablemode

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	sqlsvrapimock "github.com/pingcap/tidb/pkg/domain/sqlsvrapi/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type StoreWithKS struct {
	kv.Storage
	ks string
}

func (s *StoreWithKS) GetKeyspace() string {
	return s.ks
}

func TestResetTableModeOnCleanUpRoutesCrossKeyspace(t *testing.T) {
	if !kerneltype.IsNextGen() {
		t.Skip("cross-keyspace route is nextgen-only")
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	se := utilmock.NewContext()
	se.Store = &StoreWithKS{ks: keyspace.System}
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return se, nil
	}, 1, 1, time.Second)
	defer pool.Close()
	taskMgr := storage.NewTaskManager(pool)

	plan := importer.Plan{
		Keyspace: "ks",
		DBName:   "test",
		DBID:     100,
		TableInfo: &model.TableInfo{
			ID:   200,
			Name: ast.NewCIStr("t"),
		},
	}

	runtimeHandle := sqlsvrapimock.NewMockKSRuntimeHandle(ctrl)
	runtimeHandle.EXPECT().AlterTableMode(gomock.Any(), model.AlterTableModeTarget{
		SchemaID:   100,
		SchemaName: ast.NewCIStr("test"),
		TableID:    200,
		TableName:  ast.NewCIStr("t"),
		TargetMode: model.TableModeNormal,
	}).Return(nil)
	runtimeHandle.EXPECT().Release()
	server := sqlsvrapimock.NewMockServer(ctrl)
	server.EXPECT().AcquireKSRuntime("ks", "DXF/cleanup/123").Return(runtimeHandle, nil)
	se.BindDomainAndSchValidator(server, nil)

	err := ResetWithTaskRuntime(
		context.Background(), taskMgr, plan.Keyspace, 123, &plan)
	require.NoError(t, err)
}
