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

package ddl

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

type recoverTableForeignKeySnapshotReader struct {
	meta.Reader
	table    *model.TableInfo
	err      error
	reads    int
	schemaID int64
	tableID  int64
}

func (r *recoverTableForeignKeySnapshotReader) GetTable(schemaID, tableID int64) (*model.TableInfo, error) {
	r.reads++
	r.schemaID, r.tableID = schemaID, tableID
	return r.table, r.err
}

type recoverTableForeignKeyInfoSchema struct {
	infoschema.InfoSchema
	referred []*model.ReferredFKInfo
	schema   string
	table    string
}

func (is *recoverTableForeignKeyInfoSchema) GetTableReferredForeignKeys(schema, table string) []*model.ReferredFKInfo {
	is.schema, is.table = schema, table
	return is.referred
}

func TestCheckRecoverTableForeignKeys(t *testing.T) {
	decodeErr := errors.New("invalid historical table metadata")
	for _, tc := range []struct {
		name      string
		change    func(*model.RecoverTableInfo, *recoverTableForeignKeySnapshotReader, *recoverTableForeignKeyInfoSchema)
		wantError func(error) bool
		permanent bool
		reads     int
	}{
		{name: "self rename", reads: 1},
		{name: "unchanged self", change: func(info *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
			info.TableInfo.Name = ast.NewCIStr("old")
			info.TableInfo.ForeignKeys[0].RefTable = info.TableInfo.Name
			reader.err = decodeErr
		}},
		{name: "original external endpoint", reads: 1, permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(_ *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				reader.table.ForeignKeys[0].RefTable = ast.NewCIStr("restored")
			}},
		{name: "original external schema", reads: 1, permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(_ *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				reader.table.ForeignKeys[0].RefSchema = ast.NewCIStr("other")
			}},
		{name: "missing old schema", permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(info *model.RecoverTableInfo, _ *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				info.OldSchemaName = ""
			}},
		{name: "missing old table", permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(info *model.RecoverTableInfo, _ *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				info.OldTableName = ""
			}},
		{name: "missing snapshot timestamp", permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(info *model.RecoverTableInfo, _ *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				info.SnapshotTS = 0
			}},
		{name: "missing historical table", reads: 1, permanent: true, wantError: infoschema.ErrTableNotExists.Equal,
			change: func(_ *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				reader.table = nil
			}},
		{name: "mismatched historical table", reads: 1, permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(_ *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				reader.table.Name = ast.NewCIStr("other")
			}},
		{name: "missing historical schema", reads: 1, permanent: true, wantError: meta.ErrDBNotExists.Equal,
			change: func(_ *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				reader.err = meta.ErrDBNotExists
			}},
		{name: "retryable metadata read", reads: 1, wantError: kv.ErrTxnRetryable.Equal,
			change: func(_ *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				reader.err = kv.ErrTxnRetryable
			}},
		{name: "metadata decode error", reads: 1, wantError: func(err error) bool { return errors.Is(err, decodeErr) },
			change: func(_ *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				reader.err = decodeErr
			}},
		{name: "legacy untracked foreign key", change: func(info *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
			info.TableInfo.ForeignKeys[0].Version = 0
			info.TableInfo.ForeignKeys[0].RefTable = ast.NewCIStr("other")
			info.OldSchemaName, info.OldTableName, info.SnapshotTS = "", "", 0
			reader.err = decodeErr
		}},
		{name: "no foreign key", change: func(info *model.RecoverTableInfo, reader *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
			info.TableInfo.ForeignKeys = nil
			info.OldSchemaName, info.OldTableName, info.SnapshotTS = "", "", 0
			reader.err = decodeErr
		}},
		{name: "current external endpoint", reads: 1, permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(info *model.RecoverTableInfo, _ *recoverTableForeignKeySnapshotReader, _ *recoverTableForeignKeyInfoSchema) {
				info.TableInfo.ForeignKeys[0].RefTable = ast.NewCIStr("other")
			}},
		{name: "incoming target reference", reads: 1, permanent: true, wantError: dbterror.ErrGeneralUnsupportedDDL.Equal,
			change: func(_ *model.RecoverTableInfo, _ *recoverTableForeignKeySnapshotReader, is *recoverTableForeignKeyInfoSchema) {
				is.referred = []*model.ReferredFKInfo{{}}
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			original := &model.TableInfo{ID: 42, Name: ast.NewCIStr("old"), ForeignKeys: []*model.FKInfo{{
				Version: model.FKVersion1, RefSchema: ast.NewCIStr("test"), RefTable: ast.NewCIStr("old"),
			}}}
			restored := original.Clone()
			restored.Name = ast.NewCIStr("restored")
			restored.ForeignKeys[0].RefTable = restored.Name
			info := &model.RecoverTableInfo{
				SchemaID: 1, TableInfo: restored, SnapshotTS: 100, OldSchemaName: "test", OldTableName: "old",
			}
			reader := &recoverTableForeignKeySnapshotReader{table: original}
			is := &recoverTableForeignKeyInfoSchema{}
			if tc.change != nil {
				tc.change(info, reader, is)
			}
			err := checkRecoverTableForeignKeys(is, "test", info, reader)
			if tc.wantError == nil {
				require.NoError(t, err)
			} else {
				require.True(t, tc.wantError(err), "unexpected error type: %v", err)
			}
			require.Equal(t, tc.permanent, isPermanentRecoverTableForeignKeyError(err))
			require.Equal(t, tc.reads, reader.reads)
			if reader.reads > 0 {
				require.Equal(t, info.SchemaID, reader.schemaID)
				require.Equal(t, restored.ID, reader.tableID)
			}
			if is.table != "" {
				require.Equal(t, "test", is.schema)
				require.Equal(t, restored.Name.L, is.table)
			}
		})
	}
}

func TestIsSessionDoneHandlesWrappedQueryInterrupted(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)

	done, killed := isSessionDone(sctx)
	require.True(t, done)
	require.Equal(t, uint32(1), killed)
}

func TestConvertKillFlag(t *testing.T) {
	require.NoError(t, convertKillFlag(0))
	err := convertKillFlag(1)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(err), err)
}

func TestWaitPendingTableThresholdAbortsOnKill(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)

	finished, _, _, forceCheck, killed := (&executor{}).waitPendingTableThreshold(sctx, 1, 1, 0, 0, 1)
	require.True(t, finished)
	require.False(t, forceCheck)
	require.Equal(t, uint32(1), killed)
	require.True(t, exeerrors.ErrQueryInterrupted.Equal(convertKillFlag(killed)))
}

func TestIsRetryableDDLCancelErr(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		retry bool
	}{
		{name: "finished", err: dbterror.ErrCancelFinishedDDLJob, retry: false},
		{name: "finished wrapped", err: dbterror.ErrCancelFinishedDDLJob.GenWithStackByArgs(1), retry: false},
		{name: "cannot cancel", err: dbterror.ErrCannotCancelDDLJob, retry: false},
		{name: "cannot cancel wrapped", err: dbterror.ErrCannotCancelDDLJob.GenWithStackByArgs(1), retry: false},
		{name: "not found", err: dbterror.ErrDDLJobNotFound, retry: false},
		{name: "not found wrapped", err: dbterror.ErrDDLJobNotFound.GenWithStackByArgs(1), retry: false},
		{name: "retryable processJobs error", err: errors.New("mock failed admin command on ddl jobs"), retry: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.retry, isRetryableDDLCancelErr(tt.err))
		})
	}
}

func TestBuildQueryStringFromJobs(t *testing.T) {
	testCases := []struct {
		name     string
		jobs     []*JobWrapper
		expected string
	}{
		{
			name:     "Empty jobs",
			jobs:     []*JobWrapper{},
			expected: "",
		},
		{
			name:     "Single create table job",
			jobs:     []*JobWrapper{{Job: &model.Job{Query: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));"}}},
			expected: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));",
		},
		{
			name: "Multiple create table jobs with trailing semicolons",
			jobs: []*JobWrapper{
				{Job: &model.Job{Query: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));"}},
				{Job: &model.Job{Query: "CREATE TABLE products (id INT PRIMARY KEY, description TEXT);"}},
			},
			expected: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id INT PRIMARY KEY, description TEXT);",
		},
		{
			name: "Multiple create table jobs with and without trailing semicolons",
			jobs: []*JobWrapper{
				{Job: &model.Job{Query: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))"}},
				{Job: &model.Job{Query: "CREATE TABLE products (id INT PRIMARY KEY, description TEXT);"}},
				{Job: &model.Job{Query: "   CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product_id INT) "}},
			},
			expected: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255)); CREATE TABLE products (id INT PRIMARY KEY, description TEXT); CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, product_id INT);",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual := buildQueryStringFromJobs(tc.jobs)
			require.Equal(t, tc.expected, actual, "Query strings do not match")
		})
	}
}

func TestMergeCreateTableJobsOfSameSchema(t *testing.T) {
	job1 := NewJobWrapperWithArgs(&model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   1,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Query:      "create table db1.t1 (c1 int, c2 int)",
	}, &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.CIStr{O: "t1", L: "t1"}}}, false)
	job2 := NewJobWrapperWithArgs(&model.Job{
		Version:    model.GetJobVerInUse(),
		SchemaID:   1,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Query:      "create table db1.t2 (c1 int, c2 int);",
	}, &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.CIStr{O: "t2", L: "t2"}}, FKCheck: true}, false)
	job, err := mergeCreateTableJobsOfSameSchema([]*JobWrapper{job1, job2})
	require.NoError(t, err)
	require.Equal(t, "create table db1.t1 (c1 int, c2 int); create table db1.t2 (c1 int, c2 int);", job.Query)
}

func TestMergeCreateTableJobs(t *testing.T) {
	t.Run("0 or 1 job", func(t *testing.T) {
		newWs, err := mergeCreateTableJobs([]*JobWrapper{})
		require.NoError(t, err)
		require.Empty(t, newWs)
		jobWs := []*JobWrapper{{Job: &model.Job{}}}
		newWs, err = mergeCreateTableJobs(jobWs)
		require.NoError(t, err)
		require.EqualValues(t, jobWs, newWs)
	})

	t.Run("non create table are not merged", func(t *testing.T) {
		jobWs := []*JobWrapper{
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t1")}}},
			{Job: &model.Job{SchemaName: "db", Type: model.ActionAddColumn}},
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t2")}}},
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		require.NoError(t, err)
		require.Len(t, newWs, 2)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			if a.Type != b.Type {
				return int(a.Type - b.Type)
			}
			return 0
		})
		require.Equal(t, model.ActionAddColumn, newWs[0].Type)
		require.Equal(t, model.ActionCreateTables, newWs[1].Type)
	})

	t.Run("jobs of pre allocated ids are not merged", func(t *testing.T) {
		jobWs := []*JobWrapper{
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t1")}}, IDAllocated: true},
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t2")}}},
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			argsA := a.JobArgs.(*model.CreateTableArgs)
			argsB := b.JobArgs.(*model.CreateTableArgs)
			return strings.Compare(argsA.TableInfo.Name.L, argsB.TableInfo.Name.L)
		})
		require.NoError(t, err)
		require.EqualValues(t, jobWs, newWs)
	})

	t.Run("jobs of foreign keys are not merged", func(t *testing.T) {
		jobWs := []*JobWrapper{
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{ForeignKeys: []*model.FKInfo{{}}}}},
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t2")}}},
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			argsA := a.JobArgs.(*model.CreateTableArgs)
			argsB := b.JobArgs.(*model.CreateTableArgs)
			return strings.Compare(argsA.TableInfo.Name.L, argsB.TableInfo.Name.L)
		})
		require.NoError(t, err)
		require.EqualValues(t, jobWs, newWs)
	})

	t.Run("jobs of different schema are not merged", func(t *testing.T) {
		jobWs := []*JobWrapper{
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db1", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t1")}}},
			{Job: &model.Job{Version: model.GetJobVerInUse(), SchemaName: "db2", Type: model.ActionCreateTable},
				JobArgs: &model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t2")}}},
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			return strings.Compare(a.SchemaName, b.SchemaName)
		})
		require.NoError(t, err)
		require.EqualValues(t, jobWs, newWs)
	})

	t.Run("max batch size 8", func(t *testing.T) {
		jobWs := make([]*JobWrapper, 0, 100)
		jobWs = append(jobWs, NewJobWrapper(&model.Job{SchemaName: "db0", Type: model.ActionAddColumn}, false))
		jobW := NewJobWrapperWithArgs(&model.Job{Version: model.GetJobVerInUse(), SchemaName: "db1", Type: model.ActionCreateTable},
			&model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr("t1")}}, true)
		jobWs = append(jobWs, jobW)
		jobW = NewJobWrapperWithArgs(&model.Job{Version: model.GetJobVerInUse(), SchemaName: "db2", Type: model.ActionCreateTable},
			&model.CreateTableArgs{TableInfo: &model.TableInfo{ForeignKeys: []*model.FKInfo{{}}}}, false)
		jobWs = append(jobWs, jobW)
		for db, cnt := range map[string]int{
			"db3": 9,
			"db4": 7,
			"db5": 22,
		} {
			for i := range cnt {
				tblName := fmt.Sprintf("t%d", i)
				jobW := NewJobWrapperWithArgs(&model.Job{Version: model.GetJobVerInUse(), SchemaName: db, Type: model.ActionCreateTable},
					&model.CreateTableArgs{TableInfo: &model.TableInfo{Name: ast.NewCIStr(tblName)}}, false)
				jobWs = append(jobWs, jobW)
			}
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			return strings.Compare(a.SchemaName, b.SchemaName)
		})
		require.NoError(t, err)
		// 3 non-mergeable + 2 + 1 + 3
		require.Len(t, newWs, 9)
		require.Equal(t, model.ActionAddColumn, newWs[0].Type)
		require.Equal(t, model.ActionCreateTable, newWs[1].Type)
		require.Equal(t, "db1", newWs[1].SchemaName)
		require.Equal(t, model.ActionCreateTable, newWs[2].Type)
		require.Equal(t, "db2", newWs[2].SchemaName)

		schemaCnts := make(map[string][]int, 3)
		for i := 3; i < 9; i++ {
			require.Equal(t, model.ActionCreateTables, newWs[i].Type)
			args := newWs[i].JobArgs.(*model.BatchCreateTableArgs)
			schemaCnts[newWs[i].SchemaName] = append(schemaCnts[newWs[i].SchemaName], len(args.Tables))
			require.Equal(t, len(args.Tables), len(newWs[i].ResultCh))
		}
		for k := range schemaCnts {
			slices.Sort(schemaCnts[k])
		}
		require.Equal(t, map[string][]int{
			"db3": {4, 5},
			"db4": {7},
			"db5": {7, 7, 8},
		}, schemaCnts)
	})
}

func TestIsUndroppableTable(t *testing.T) {
	for _, tt := range []struct {
		schema, table                   string
		alwaysProtected                 bool
		protectedInNextGenByBuiltinName bool
	}{
		{mysql.WorkloadSchema, "any_table", true, false},
		{mysql.SystemDB, "tidb", true, true},
		{mysql.SystemDB, "gc_delete_range", true, true},
		{mysql.SystemDB, "gc_delete_range_done", true, true},
		{mysql.SystemDB, "user", false, true},
		{mysql.SystemDB, "custom_table", false, false},
		{mysql.SysDB, "schema_unused_indexes", false, false},
		{"test", "test_table", false, false},
	} {
		t.Run(tt.schema+"."+tt.table, func(t *testing.T) {
			want := tt.alwaysProtected || kerneltype.IsNextGen() && tt.protectedInNextGenByBuiltinName
			require.Equal(t, want, isUndroppableTable(tt.schema, tt.table))
		})
	}
	t.Run("system schemas", func(t *testing.T) {
		for _, name := range []string{mysql.SystemDB, mysql.SysDB, "test"} {
			require.Equal(t, kerneltype.IsNextGen() && (name == mysql.SystemDB || name == mysql.SysDB), isSystemSchemaInNextGen(name))
		}
	})
}

func TestMaterializedViewPartitionDependencyConstraints(t *testing.T) {
	t.Run("base table dependencies", func(t *testing.T) {
		baseWithMLog := &model.TableInfo{
			MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: 1},
		}
		err := checkBaseTableMaterializedViewDependencyConstraints(baseWithMLog, "ALTER TABLE ... REMOVE PARTITIONING")
		require.ErrorContains(t, err, "ALTER TABLE ... REMOVE PARTITIONING with materialized view log")

		baseWithMViews := &model.TableInfo{
			MaterializedViewBase: &model.MaterializedViewBaseInfo{MViewIDs: []int64{2}},
		}
		err = checkBaseTableMaterializedViewDependencyConstraints(baseWithMViews, "ALTER TABLE ... PARTITION BY")
		require.ErrorContains(t, err, "ALTER TABLE ... PARTITION BY with materialized view dependencies")

		require.NoError(t, checkBaseTableMaterializedViewDependencyConstraints(&model.TableInfo{}, "ALTER TABLE ... PARTITION BY"))
	})

	t.Run("exchange table roles", func(t *testing.T) {
		err := checkExchangePartitionMaterializedViewConstraints(
			&model.TableInfo{MaterializedViewLog: &model.MaterializedViewLogInfo{}},
			"non-partitioned table",
		)
		require.ErrorContains(t, err, "EXCHANGE PARTITION on non-partitioned table with materialized view log")

		err = checkExchangePartitionMaterializedViewConstraints(
			&model.TableInfo{MaterializedView: &model.MaterializedViewInfo{}},
			"non-partitioned table",
		)
		require.ErrorContains(t, err, "EXCHANGE PARTITION on non-partitioned table materialized view table")

		err = checkExchangePartitionMaterializedViewConstraints(
			&model.TableInfo{MaterializedViewBase: &model.MaterializedViewBaseInfo{MViewIDs: []int64{2}}},
			"partitioned table",
		)
		require.ErrorContains(t, err, "EXCHANGE PARTITION on partitioned table with materialized view dependencies")

		shadowTable := &model.TableInfo{
			MaterializedViewShadow: &model.MaterializedViewShadowInfo{SourceMViewID: 42},
		}
		err = checkExchangePartitionMaterializedViewConstraints(shadowTable, "partitioned table")
		require.ErrorContains(t, err, "EXCHANGE PARTITION on partitioned table materialized view shadow table")
		err = checkTableMaterializedViewConstraints(nil, shadowTable, "DROP TABLE")
		require.ErrorContains(t, err, "DROP TABLE on materialized view shadow table")
		baseWithMLog := &model.TableInfo{
			MaterializedViewBase: &model.MaterializedViewBaseInfo{MLogID: 7},
		}
		err = checkTableMaterializedViewConstraints(nil, baseWithMLog, "DROP TABLE")
		require.ErrorContains(t, err, "DROP TABLE on base table with materialized view log")
		err = checkTableMaterializedViewConstraints(nil, baseWithMLog, "TRUNCATE TABLE")
		require.ErrorContains(t, err, "TRUNCATE TABLE on base table with materialized view log")
		err = CheckIndexOperationMaterializedViewConstraints(nil, shadowTable, "DROP INDEX", false)
		require.ErrorContains(t, err, "DROP INDEX on materialized view shadow table")

		likeInfo, err := BuildTableInfoWithLike(
			ast.Ident{Name: ast.NewCIStr("shadow_copy")},
			shadowTable,
			&ast.CreateTableStmt{},
		)
		require.NoError(t, err)
		require.Nil(t, likeInfo.MaterializedViewShadow)

		maintenanceVars := &variable.SessionVars{
			InMViewMaintenance: true,
			InRestrictedSQL:    true,
		}
		require.NoError(t, checkTableMaterializedViewConstraints(maintenanceVars, shadowTable, "DROP TABLE"))
		require.NoError(t, CheckIndexOperationMaterializedViewConstraints(maintenanceVars, shadowTable, "DROP INDEX", false))
	})
}
