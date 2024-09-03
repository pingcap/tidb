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

	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
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
	job1 := NewJobWrapper(&model.Job{
		SchemaID:   1,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{&model.TableInfo{Name: pmodel.CIStr{O: "t1", L: "t1"}}, false},
		Query:      "create table db1.t1 (c1 int, c2 int)",
	}, false)
	job2 := NewJobWrapper(&model.Job{
		SchemaID:   1,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []any{&model.TableInfo{Name: pmodel.CIStr{O: "t2", L: "t2"}}, &model.TableInfo{}},
		Query:      "create table db1.t2 (c1 int, c2 int);",
	}, false)
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
			{Job: &model.Job{SchemaName: "db", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t1")}, false}}},
			{Job: &model.Job{SchemaName: "db", Type: model.ActionAddColumn}},
			{Job: &model.Job{SchemaName: "db", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t2")}, false}}},
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
			{Job: &model.Job{SchemaName: "db", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t1")}, false}}, IDAllocated: true},
			{Job: &model.Job{SchemaName: "db", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t2")}, false}}},
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			if aName, bName := a.Args[0].(*model.TableInfo).Name.L, b.Args[0].(*model.TableInfo).Name.L; aName != bName {
				return strings.Compare(aName, bName)
			}
			return 0
		})
		require.NoError(t, err)
		require.EqualValues(t, jobWs, newWs)
	})

	t.Run("jobs of foreign keys are not merged", func(t *testing.T) {
		jobWs := []*JobWrapper{
			{Job: &model.Job{SchemaName: "db", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{ForeignKeys: []*model.FKInfo{{}}}, false}}},
			{Job: &model.Job{SchemaName: "db", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t2")}, false}}},
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			if aName, bName := a.Args[0].(*model.TableInfo).Name.L, b.Args[0].(*model.TableInfo).Name.L; aName != bName {
				return strings.Compare(aName, bName)
			}
			return 0
		})
		require.NoError(t, err)
		require.EqualValues(t, jobWs, newWs)
	})

	t.Run("jobs of different schema are not merged", func(t *testing.T) {
		jobWs := []*JobWrapper{
			{Job: &model.Job{SchemaName: "db1", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t1")}, false}}},
			{Job: &model.Job{SchemaName: "db2", Type: model.ActionCreateTable,
				Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t2")}, false}}},
		}
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			if aName, bName := a.SchemaName, b.SchemaName; aName != bName {
				return strings.Compare(aName, bName)
			}
			return 0
		})
		require.NoError(t, err)
		require.EqualValues(t, jobWs, newWs)
	})

	t.Run("max batch size 8", func(t *testing.T) {
		jobWs := make([]*JobWrapper, 0, 100)
		for db, cnt := range map[string]int{
			"db0": 9,
			"db1": 7,
			"db2": 22,
		} {
			for i := 0; i < cnt; i++ {
				tblName := fmt.Sprintf("t%d", i)
				jobWs = append(jobWs, NewJobWrapper(&model.Job{SchemaName: db, Type: model.ActionCreateTable,
					Args: []any{&model.TableInfo{Name: pmodel.NewCIStr(tblName)}, false}}, false))
			}
		}
		jobWs = append(jobWs, NewJobWrapper(&model.Job{SchemaName: "dbx", Type: model.ActionAddColumn}, false))
		jobWs = append(jobWs, NewJobWrapper(&model.Job{SchemaName: "dbxx", Type: model.ActionCreateTable,
			Args: []any{&model.TableInfo{Name: pmodel.NewCIStr("t1")}, false}}, true))
		jobWs = append(jobWs, NewJobWrapper(&model.Job{SchemaName: "dbxxx", Type: model.ActionCreateTable,
			Args: []any{&model.TableInfo{ForeignKeys: []*model.FKInfo{{}}}, false}}, false))
		newWs, err := mergeCreateTableJobs(jobWs)
		slices.SortFunc(newWs, func(a, b *JobWrapper) int {
			if a.Type != b.Type {
				return int(b.Type - a.Type)
			}
			if aName, bName := a.SchemaName, b.SchemaName; aName != bName {
				return strings.Compare(aName, bName)
			}
			aTableInfo, aOK := a.Args[0].(*model.TableInfo)
			bTableInfo, bOK := b.Args[0].(*model.TableInfo)
			if aOK && bOK && aTableInfo.Name.L != bTableInfo.Name.L {
				return strings.Compare(aTableInfo.Name.L, bTableInfo.Name.L)
			}

			return 0
		})
		require.NoError(t, err)
		// 3 non-mergeable + 2 + 1 + 3
		require.Len(t, newWs, 9)
		require.Equal(t, model.ActionAddColumn, newWs[0].Type)
		require.Equal(t, model.ActionCreateTable, newWs[1].Type)
		require.Equal(t, "dbxx", newWs[1].SchemaName)
		require.Equal(t, model.ActionCreateTable, newWs[2].Type)
		require.Equal(t, "dbxxx", newWs[2].SchemaName)

		schemaCnts := make(map[string][]int, 3)
		for i := 3; i < 9; i++ {
			require.Equal(t, model.ActionCreateTables, newWs[i].Type)
			infos := newWs[i].Args[0].([]*model.TableInfo)
			schemaCnts[newWs[i].SchemaName] = append(schemaCnts[newWs[i].SchemaName], len(infos))
			require.Equal(t, len(infos), len(newWs[i].ResultCh))
		}
		for k := range schemaCnts {
			slices.Sort(schemaCnts[k])
		}
		require.Equal(t, map[string][]int{
			"db0": {4, 5},
			"db1": {7},
			"db2": {7, 7, 8},
		}, schemaCnts)
	})
}
