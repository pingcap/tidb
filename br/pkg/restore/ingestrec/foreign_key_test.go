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
	"github.com/pingcap/tidb/pkg/parser/ast"
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
	childTableInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("child"))
	require.NoError(t, err)
	parentTableInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("parent"))
	require.NoError(t, err)
	childTableIndexI1 := childTableInfo.Indices[0]
	parentTableIndexI1 := parentTableInfo.Indices[0]
	{
		foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), parentTableInfo)
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
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), parentTableInfo)
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
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), parentTableInfo)
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
		childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), childTableInfo)
		require.NoError(t, err)
		parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), parentTableInfo)
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

	tk.MustExec("create table test.partial_parent (id int, index parent_idx(id))")
	tk.MustExec("create table test.partial_child (id int, pid int, marker int, index unsafe_pid(pid) where marker is not null, index safe_pid(pid) where pid is not null, foreign key (pid) references test.partial_parent (id))")

	infoSchema = s.Mock.InfoSchema()
	partialChildTableInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("partial_child"))
	require.NoError(t, err)
	unsafeIdx := partialChildTableInfo.FindIndexByName("unsafe_pid")
	require.NotNil(t, unsafeIdx)
	safeIdx := partialChildTableInfo.FindIndexByName("safe_pid")
	require.NotNil(t, safeIdx)

	partialChildForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), partialChildTableInfo)
	require.NoError(t, err)
	require.Len(t, partialChildForeignKeyRecordManager.GetFKRecordMap(), 1)
	partialChildForeignKeyRecordManager.RemoveForeignKeys(partialChildTableInfo, unsafeIdx)
	require.Len(t, partialChildForeignKeyRecordManager.GetFKRecordMap(), 1)
	partialChildForeignKeyRecordManager.RemoveForeignKeys(partialChildTableInfo, safeIdx)
	require.Len(t, partialChildForeignKeyRecordManager.GetFKRecordMap(), 0)

	tk.MustExec("create table test.partial_ref_parent (id int, marker int, index unsafe_id(id) where marker is not null, index safe_id(id) where id is not null)")
	tk.MustExec("create table test.partial_ref_child (id int, pid int, index child_pid(pid), foreign key (pid) references test.partial_ref_parent (id))")

	infoSchema = s.Mock.InfoSchema()
	partialRefParentInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("partial_ref_parent"))
	require.NoError(t, err)
	unsafeRefIdx := partialRefParentInfo.FindIndexByName("unsafe_id")
	require.NotNil(t, unsafeRefIdx)
	safeRefIdx := partialRefParentInfo.FindIndexByName("safe_id")
	require.NotNil(t, safeRefIdx)

	partialRefParentForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), partialRefParentInfo)
	require.NoError(t, err)
	require.Len(t, partialRefParentForeignKeyRecordManager.GetReferredFKRecordMap(), 1)
	partialRefParentForeignKeyRecordManager.RemoveForeignKeys(partialRefParentInfo, unsafeRefIdx)
	require.Len(t, partialRefParentForeignKeyRecordManager.GetReferredFKRecordMap(), 1)
	partialRefParentForeignKeyRecordManager.RemoveForeignKeys(partialRefParentInfo, safeRefIdx)
	require.Len(t, partialRefParentForeignKeyRecordManager.GetReferredFKRecordMap(), 0)
}

func TestForeignKeyRecordManagerForPK1(t *testing.T) {
	ctx := context.Background()
	s := utiltest.CreateRestoreSchemaSuite(t)
	tk := testkit.NewTestKit(t, s.Mock.Storage)

	tk.MustExec("create table test.parent (id int primary key, pid int, index i1(id, pid))")
	tk.MustExec("create table test.child (id int, pid int, index i1(pid), foreign key (pid) references test.parent (id) on delete cascade)")

	infoSchema := s.Mock.InfoSchema()
	childTableInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("child"))
	require.NoError(t, err)
	parentTableInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("parent"))
	require.NoError(t, err)
	childTableIndexI1 := childTableInfo.Indices[0]

	foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
	childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), childTableInfo)
	require.NoError(t, err)
	parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), parentTableInfo)
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
	childTableInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("child"))
	require.NoError(t, err)
	parentTableInfo, err := infoSchema.TableInfoByName(ast.NewCIStr("test"), ast.NewCIStr("parent"))
	require.NoError(t, err)
	parentTableIndexI1 := parentTableInfo.Indices[0]

	foreignKeyRecordManager := ingestrec.NewForeignKeyRecordManager()
	childTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), childTableInfo)
	require.NoError(t, err)
	parentTableForeignKeyRecordManager, err := ingestrec.NewForeignKeyRecordManagerForTables(ctx, infoSchema, ast.NewCIStr("test"), parentTableInfo)
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
