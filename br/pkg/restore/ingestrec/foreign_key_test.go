// Copyright 2025 PingCAP, Inc.
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

package ingestrec_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/br/pkg/restore/ingestrec"
	"github.com/pingcap/tidb/br/pkg/utiltest"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestForeignKeyRecordManager(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)

	tk.MustExec("create table test.parent (id int, index i1(id))")
	tk.MustExec("create table test.child (id int, pid int, index i1(pid), foreign key (pid) references test.parent (id) on delete cascade)")

	infoSchema := s.Mock.InfoSchema()
	childTableInfo, err := infoSchema.TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("child"))
	require.NoError(t, err)
	parentTableInfo, err := infoSchema.TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("parent"))
	require.NoError(t, err)
	childTableIndexI1 := childTableInfo.Indices[0]
	parentTableIndexI1 := parentTableInfo.Indices[0]
	{
		foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), parentTableInfo)
		require.NoError(t, err)
		require.Len(t, childTableForeignKeyRecordManager.GetFKRecordMap(), 1)
		require.Len(t, childTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetReferredFKRecordMap(), 1)
		foreignKeyRecordManager.Merge(childTableForeignKeyRecordManager)
		foreignKeyRecordManager.Merge(parentTableForeignKeyRecordManager)
		require.Len(t, foreignKeyRecordManager.GetFKRecordMap(), 1)
	}
	{
		foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), parentTableInfo)
		require.NoError(t, err)
		childTableForeignKeyRecordManager.RemoveForeignKeys(childTableInfo, childTableIndexI1)
		require.Len(t, childTableForeignKeyRecordManager.GetFKRecordMap(), 0)
		require.Len(t, childTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetReferredFKRecordMap(), 1)
		foreignKeyRecordManager.Merge(childTableForeignKeyRecordManager)
		foreignKeyRecordManager.Merge(parentTableForeignKeyRecordManager)
		require.Len(t, foreignKeyRecordManager.GetFKRecordMap(), 1)
	}
	{
		foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), parentTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager.RemoveForeignKeys(parentTableInfo, parentTableIndexI1)
		require.Len(t, childTableForeignKeyRecordManager.GetFKRecordMap(), 1)
		require.Len(t, childTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
		foreignKeyRecordManager.Merge(childTableForeignKeyRecordManager)
		foreignKeyRecordManager.Merge(parentTableForeignKeyRecordManager)
		require.Len(t, foreignKeyRecordManager.GetFKRecordMap(), 1)
	}
	{
		foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), parentTableInfo)
		require.NoError(t, err)
		childTableForeignKeyRecordManager.RemoveForeignKeys(childTableInfo, childTableIndexI1)
		parentTableForeignKeyRecordManager.RemoveForeignKeys(parentTableInfo, parentTableIndexI1)
		require.Len(t, childTableForeignKeyRecordManager.GetFKRecordMap(), 0)
		require.Len(t, childTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetFKRecordMap(), 0)
		require.Len(t, parentTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
		foreignKeyRecordManager.Merge(childTableForeignKeyRecordManager)
		foreignKeyRecordManager.Merge(parentTableForeignKeyRecordManager)
		require.Len(t, foreignKeyRecordManager.GetFKRecordMap(), 0)
	}
}

func TestForeignKeyRecordManagerForPK1(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)

	tk.MustExec("create table test.parent (id int primary key, pid int, index i1(id, pid))")
	tk.MustExec("create table test.child (id int, pid int, index i1(pid), foreign key (pid) references test.parent (id) on delete cascade)")

	infoSchema := s.Mock.InfoSchema()
	childTableInfo, err := infoSchema.TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("child"))
	require.NoError(t, err)
	parentTableInfo, err := infoSchema.TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("parent"))
	require.NoError(t, err)
	childTableIndexI1 := childTableInfo.Indices[0]

	foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
	childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), childTableInfo)
	require.NoError(t, err)
	parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), parentTableInfo)
	require.NoError(t, err)
	childTableForeignKeyRecordManager.RemoveForeignKeys(childTableInfo, childTableIndexI1)
	require.Len(t, childTableForeignKeyRecordManager.GetFKRecordMap(), 0)
	require.Len(t, childTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
	require.Len(t, parentTableForeignKeyRecordManager.GetFKRecordMap(), 0)
	require.Len(t, parentTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
	foreignKeyRecordManager.Merge(childTableForeignKeyRecordManager)
	foreignKeyRecordManager.Merge(parentTableForeignKeyRecordManager)
	require.Len(t, foreignKeyRecordManager.GetFKRecordMap(), 0)
}

func TestForeignKeyRecordManagerForPK2(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)

	tk.MustExec("create table test.parent (id int, pid int, index i1(id, pid))")
	tk.MustExec("create table test.child (id int, pid int primary key, index i1(pid, id), foreign key (pid) references test.parent (id) on delete cascade)")

	infoSchema := s.Mock.InfoSchema()
	childTableInfo, err := infoSchema.TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("child"))
	require.NoError(t, err)
	parentTableInfo, err := infoSchema.TableInfoByName(pmodel.NewCIStr("test"), pmodel.NewCIStr("parent"))
	require.NoError(t, err)
	parentTableIndexI1 := parentTableInfo.Indices[0]

	foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
	childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), childTableInfo)
	require.NoError(t, err)
	parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, pmodel.NewCIStr("test"), parentTableInfo)
	require.NoError(t, err)
	parentTableForeignKeyRecordManager.RemoveForeignKeys(parentTableInfo, parentTableIndexI1)
	require.Len(t, childTableForeignKeyRecordManager.GetFKRecordMap(), 0)
	require.Len(t, childTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
	require.Len(t, parentTableForeignKeyRecordManager.GetFKRecordMap(), 0)
	require.Len(t, parentTableForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
	foreignKeyRecordManager.Merge(childTableForeignKeyRecordManager)
	foreignKeyRecordManager.Merge(parentTableForeignKeyRecordManager)
	require.Len(t, foreignKeyRecordManager.GetFKRecordMap(), 0)
}
