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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

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
	tests := []struct {
		name        string
		schema      string
		table       string
		tableID     int64
		want        bool
		skipClassic bool
		skipNextGen bool
	}{
		// Test reserved ID in next gen
		{
			name:        "reserved ID upper bound in next gen",
			schema:      "test",
			table:       "test_table",
			tableID:     metadef.ReservedGlobalIDUpperBound,
			want:        true,
			skipClassic: true,
		},
		{
			name:        "reserved ID lower bound in next gen",
			schema:      "test",
			table:       "test_table",
			tableID:     metadef.ReservedGlobalIDLowerBound + 1,
			want:        true,
			skipClassic: true,
		},
		{
			name:        "non-reserved ID in next gen",
			schema:      "test",
			table:       "test_table",
			tableID:     100,
			want:        false,
			skipClassic: true,
		},
		{
			name:        "reserved ID in classic",
			schema:      "test",
			table:       "test_table",
			tableID:     metadef.ReservedGlobalIDUpperBound,
			want:        false,
			skipNextGen: true,
		},
		// Test WorkloadSchema
		{
			name:    "table in workload_schema",
			schema:  mysql.WorkloadSchema,
			table:   "any_table",
			tableID: 100,
			want:    true,
		},
		// Test SystemDB with system tables
		{
			name:    "tidb table in mysql schema",
			schema:  mysql.SystemDB,
			table:   "tidb",
			tableID: 100,
			want:    true,
		},
		{
			name:    "gc_delete_range table in mysql schema",
			schema:  mysql.SystemDB,
			table:   "gc_delete_range",
			tableID: 100,
			want:    true,
		},
		{
			name:    "gc_delete_range_done table in mysql schema",
			schema:  mysql.SystemDB,
			table:   "gc_delete_range_done",
			tableID: 100,
			want:    true,
		},
		// Test SystemDB with non-system tables
		{
			name:    "non-system table in mysql schema",
			schema:  mysql.SystemDB,
			table:   "user",
			tableID: 100,
			want:    false,
		},
		// Test non-system schemas
		{
			name:    "table in test schema",
			schema:  "test",
			table:   "test_table",
			tableID: 100,
			want:    false,
		},
		{
			name:    "table in information_schema",
			schema:  "information_schema",
			table:   "tables",
			tableID: 100,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipClassic && kerneltype.IsClassic() {
				t.Skip("This test is only for nextgen kernel.")
			}
			if tt.skipNextGen && kerneltype.IsNextGen() {
				t.Skip("This test is only for classic kernel.")
			}

			tableInfo := &model.TableInfo{
				ID:   tt.tableID,
				Name: ast.NewCIStr(tt.table),
			}

			result := isUndroppableTable(tt.schema, tt.table, tableInfo)
			require.Equal(t, tt.want, result, "schema=%s, table=%s, tableID=%d", tt.schema, tt.table, tt.tableID)
		})
	}
}
