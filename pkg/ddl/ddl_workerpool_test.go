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

package ddl

import (
	"context"
	"testing"

	"github.com/ngaut/pools"
	sess "github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestDDLWorkerPool(t *testing.T) {
	f := func() func() (pools.Resource, error) {
		return func() (pools.Resource, error) {
			wk := newWorker(context.Background(), addIdxWorker, nil, nil, nil)
			return wk, nil
		}
	}
	pool := newDDLWorkerPool(pools.NewResourcePool(f(), 1, 2, 0), jobTypeReorg)
	require.Equal(t, 1, pool.available())
	pool.close()
	require.Zero(t, pool.available())
	pool.put(nil)
	require.Zero(t, pool.available())
}

type mockSessionResource struct {
	*mock.Context
}

func (*mockSessionResource) Close() {}

func newMockDDLSessionPool() *sess.Pool {
	resourcePool := pools.NewResourcePool(func() (pools.Resource, error) {
		return &mockSessionResource{Context: mock.NewContext()}, nil
	}, 1, 1, 0)
	return sess.NewSessionPool(resourcePool)
}

func newRangePartitionTableInfo() *model.TableInfo {
	colA := &model.ColumnInfo{ID: 1, Name: ast.NewCIStr("a"), Offset: 0}
	colA.FieldType = *types.NewFieldType(mysql.TypeLong)
	colB := &model.ColumnInfo{ID: 2, Name: ast.NewCIStr("b"), Offset: 1}
	colB.FieldType = *types.NewFieldType(mysql.TypeLong)
	return &model.TableInfo{
		Name:    ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{colA, colB},
		Partition: &model.PartitionInfo{
			Type:   ast.PartitionTypeRange,
			Expr:   "`a`",
			Enable: true,
			Definitions: []model.PartitionDefinition{
				{ID: 101, Name: ast.NewCIStr("p0"), LessThan: []string{"10"}},
				{ID: 102, Name: ast.NewCIStr("pMax"), LessThan: []string{"MAXVALUE"}},
			},
		},
	}
}

func TestPostCheckPartitionModifiableColumnErrorBranches(t *testing.T) {
	t.Run("session pool get failure", func(t *testing.T) {
		tblInfo := newRangePartitionTableInfo()
		sessPool := newMockDDLSessionPool()
		sessPool.Close()
		w := &worker{sessPool: sessPool}
		err := postCheckPartitionModifiableColumn(w, tblInfo, tblInfo.Columns[0], tblInfo.Columns[0].Clone())
		require.ErrorContains(t, err, "session pool is closed")
	})

	t.Run("extract partition columns parse error", func(t *testing.T) {
		tblInfo := newRangePartitionTableInfo()
		tblInfo.Partition.Expr = "`a`+"
		sessPool := newMockDDLSessionPool()
		defer sessPool.Close()
		w := &worker{sessPool: sessPool}
		err := postCheckPartitionModifiableColumn(w, tblInfo, tblInfo.Columns[0], tblInfo.Columns[0].Clone())
		require.Error(t, err)
		require.ErrorContains(t, err, "line 1 column")
	})

	t.Run("generated partition info parse error", func(t *testing.T) {
		tblInfo := newRangePartitionTableInfo()
		tblInfo.Partition.Type = ast.PartitionTypeNone
		sessPool := newMockDDLSessionPool()
		defer sessPool.Close()
		w := &worker{sessPool: sessPool}
		err := postCheckPartitionModifiableColumn(w, tblInfo, tblInfo.Columns[0], tblInfo.Columns[0].Clone())
		require.ErrorContains(t, err, "cannot parse generated PartitionInfo")
	})
}
