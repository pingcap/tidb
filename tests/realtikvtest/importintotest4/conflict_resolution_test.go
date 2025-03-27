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

package importintotest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest/addindextestutil"
	"github.com/tikv/client-go/v2/util"
)

func (s *mockGCSSuite) testSingleFileConflictResolution(tblSQL string, sourceContent string, resultRows []string) {
	s.T().Helper()
	s.testConflictResolution(tblSQL, []string{sourceContent}, resultRows)
}

func (s *mockGCSSuite) testConflictResolution(tblSQL string, sourceContents []string, resultRows []string) int64 {
	return s.testConflictResolutionWithOptions(tblSQL, sourceContents, resultRows, "")
}

func (s *mockGCSSuite) testConflictResolutionWithOptions(tblSQL string, sourceContents []string, resultRows []string, options string) int64 {
	return s.testConflictResolutionWithColumnVarsAndOptions(tblSQL, sourceContents, resultRows, "", options)
}

func (s *mockGCSSuite) testConflictResolutionWithColumnVarsAndOptions(tblSQL string,
	sourceContents []string, resultRows []string, columnVars string, options string) int64 {
	s.T().Helper()
	totalRowCount := 0
	for i, content := range sourceContents {
		rows := strings.Split(content, "\n")
		for _, r := range rows {
			if len(r) > 0 {
				totalRowCount++
			}
		}
		filename := fmt.Sprintf("t.%d.csv", i)
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "conflicts", Name: filename},
			Content:     []byte(content),
		})
	}
	s.T().Cleanup(func() {
		addindextestutil.RemoveAllObjects(s.T(), s.server, "conflicts")
		addindextestutil.RemoveAllObjects(s.T(), s.server, "sorted")
	})

	s.prepareAndUseDB("conflicts")
	s.tk.MustExec(tblSQL)

	sortStorageURI := fmt.Sprintf("gs://sorted?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t %s FROM 'gs://conflicts/t.*.csv?endpoint=%s'
		with __max_engine_size = '1', cloud_storage_uri='%s'`, columnVars, gcsEndpoint, sortStorageURI)
	if len(options) > 0 {
		importSQL = fmt.Sprintf("%s, %s", importSQL, options)
	}
	result := s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	s.Equal(fmt.Sprintf("%d", len(resultRows)), result[0][7])
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(resultRows...))
	s.tk.MustExec("admin check table t")
	jobID, err := strconv.Atoi(result[0][0].(string))
	s.NoError(err)

	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(int64(jobID))
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	task, err2 := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	s.NoError(err2)
	subtasks, err := taskManager.GetSubtasksWithHistory(ctx, task.ID, proto.ImportStepCollectConflicts)
	s.NoError(err)
	s.Len(subtasks, 1)
	subtaskM := &importinto.CollectConflictsStepMeta{}
	s.NoError(json.Unmarshal(subtasks[0].Meta, subtaskM))
	object, err := s.server.GetObject("sorted", subtaskM.ConflictedRowFilename)
	s.NoError(err)
	conflictedRows := strings.Split(strings.Trim(string(object.Content), "\n"), "\n")
	s.Equal(totalRowCount-len(resultRows), len(conflictedRows))

	return int64(jobID)
}

func (s *mockGCSSuite) TestGlobalSortConflictResolutionBasicCases() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	/****************************************************************************
	* ALL CASES CONTAINS NON-UNIQUE SECONDARY INDEXES, SO WE DON'T TEST IT SEPARATELY.
	****************************************************************************/

	s.Run("all row duplicated", func() {
		for _, tblSQL := range []string{
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			`create table t(a int primary key nonclustered, b int, c int, unique(b), index(c))`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
0,0,0
0,0,0
0,0,0
0,0,0
0,0,0
0,0,0
`,
				[]string{},
			)
		}
	})

	s.Run("whole row duplicated, but not all", func() {
		// Note: '2,2,3' is not whole row duplicated, but it only differs in the
		// column that have non-unique index, so it's the same as '2,2,2' from the
		// conflict resolution perspective.
		for _, tblSQL := range []string{
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			`create table t(a int primary key nonclustered, b int, c int, unique(b), index(c))`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
0,0,0
1,1,1
1,1,1
1,1,1
2,2,2
2,2,3
3,\N,3
3,\N,3
`,
				[]string{"0 0 0"},
			)
		}
	})

	s.Run("pk conflict", func() {
		for _, tblSQL := range []string{
			`create table t(a int primary key clustered, b int, index(b))`,
			`create table t(a int primary key nonclustered, b int, index(b))`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
0,0
1,1
1,2
2,2
2,2
2,3
`,
				[]string{"0 0"},
			)
		}
	})

	s.Run("single uk conflict", func() {
		for _, tblSQL := range []string{
			`create table t(a int, b int, c int, unique(b), index(c))`,
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			`create table t(a int primary key nonclustered, b int, c int, unique(b), index(c))`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
0,0,0
1,1,0
2,1,1
3,2,1
4,2,2
5,2,2
`,
				[]string{"0 0 0"},
			)
		}
	})

	s.Run("mixing pk/single-uk conflict, no cycle", func() {
		for _, tblSQL := range []string{
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			`create table t(a int primary key nonclustered, b int, c int, unique(b), index(c))`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
0,0,0
1,1,1
1,1,1
2,2,1
3,2,2
4,2,3
4,3,3
5,3,2
`,
				[]string{"0 0 0"},
			)
		}
	})

	s.Run("mixing pk/single-uk conflict, cycle", func() {
		// A---B---C---D
		// └-----------┘
		for _, tblSQL := range []string{
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			`create table t(a int primary key nonclustered, b int, c int, unique(b), index(c))`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
0,0,0
1,1,1
1,2,1
2,2,1
1,2,2
`,
				[]string{"0 0 0"},
			)
		}
	})

	s.Run("multiple-uk conflict, no cycle", func() {
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, unique(a), unique(b), index(c))`,
			`
1,0,0,0
2,1,1,1
3,1,1,1
4,2,2,1
5,3,2,2
6,4,2,3
7,4,3,3
8,5,3,2
`,
			[]string{"1 0 0 0"},
		)
	})

	s.Run("multiple-uk conflict, cycle", func() {
		// A---B---C---D---E
		// └---------------┘
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, unique(a), unique(b), index(c))`,
			`
1,0,0,0
2,1,1,1
3,1,2,1
4,2,2,1
5,2,3,2
6,1,3,2
`,
			[]string{"1 0 0 0"},
		)
	})

	s.Run("multiple-uk conflict, complex cycle", func() {
		// A---B---C
		//  \  |  /
		//   \ | /
		//     D
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			`
1,0,0,0,0
2,1,1,1,1
3,1,2,2,2
4,3,2,3,3
5,1,5,3,5
`,
			[]string{"1 0 0 0 0"},
		)
	})

	s.Run("mixing above cases", func() {
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			`
1,0,0,0,0
2,1,1,1,1
3,1,2,2,2
4,3,2,3,3
5,1,5,3,5
6,6,6,6,6
6,6,6,6,6
6,6,6,6,6
6,6,6,6,6
7,7,7,7,7
7,8,8,8,8
9,8,9,9,9
10,10,9,10,10
11,11,11,10,11
`,
			[]string{"1 0 0 0 0"},
		)
	})

	s.Run("mixing cases, also consider column-vars and set clause", func() {
		s.testConflictResolutionWithColumnVarsAndOptions(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			[]string{`
abc,0,0,1,0,0
abc,1,1,2,1,1
abc,2,2,3,1,2
abc,3,3,4,3,2
abc,3,5,5,1,5
abc,6,6,6,6,6
abc,6,6,6,6,6
abc,6,6,6,6,6
abc,6,6,6,6,6
abc,7,7,7,7,7
abc,8,8,7,8,8
abc,9,9,9,8,9
abc,10,10,10,10,9
abc,10,11,11,11,11
`},
			[]string{"1 0 0 0 0"},
			"(@1,@2,d,pk,a,b) set c=@2",
			"",
		)
	})

	s.Run("partition table", func() {
		for _, tblSQL := range []string{
			`create table t(pk int, park int, a int, b int, c int, d int,
				primary key(pk, park) clustered, unique(park,a), unique(park,b), unique(park,c), index(d))
			partition by range(park)(
				partition p0 values less than (6),
				partition p1 values less than (12),
				partition p2 values less than (18),
				partition p3 values less than (MAXVALUE)
			)`,
			`create table t(pk int, park int, a int, b int, c int, d int,
				primary key(pk, park) nonclustered, unique(park,a), unique(park,b), unique(park,c), index(d))
			partition by range(park)(
				partition p0 values less than (6),
				partition p1 values less than (12),
				partition p2 values less than (18),
				partition p3 values less than (MAXVALUE)
			)`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
1,1,0,0,0,0
2,2,1,1,1,1
3,2,1,2,2,2
4,2,3,2,3,3
5,2,1,5,3,5

6,6,6,6,6,6
6,6,6,6,6,6
6,6,6,6,6,6
6,6,6,6,6,6
7,8,7,7,7,7
7,8,8,8,8,8
9,8,8,9,9,9
10,8,10,9,10,10
11,8,11,11,10,11

12,12,12,12,12,12
12,12,12,12,12,12
12,12,12,12,12,14
13,13,13,13,13,13
`,
				[]string{"1 1 0 0 0 0", "13 13 13 13 13 13"},
			)
		}
	})

	s.Run("multiple value index, also consider shard of auto row id", func() {
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a json, b int, c int, d json,
			unique((CAST(a->'$' AS UNSIGNED ARRAY))), unique(b), unique(c), index((CAST(d->'$' AS UNSIGNED ARRAY))))`,
			`create table t(pk int primary key nonclustered, a json, b int, c int, d json,
			unique((CAST(a->'$' AS UNSIGNED ARRAY))), unique(b), unique(c), index((CAST(d->'$' AS UNSIGNED ARRAY))))`,
			`create table t(pk int primary key nonclustered, a json, b int, c int, d json,
			unique((CAST(a->'$' AS UNSIGNED ARRAY))), unique(b), unique(c), index((CAST(d->'$' AS UNSIGNED ARRAY))))
			SHARD_ROW_ID_BITS = 4`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				`
1,"[0]",0,0,"[0,1,11,2]"

2,"[1,21]",1,1,"[1,11,111,12]"
3,"[1,31]",2,2,"[2,21,211,22]"
4,"[3,43]",2,3,"[3,31,311,32]"
5,"[1,51,52]",5,3,"[5,51,511,52]"
`,
				[]string{"1 [0] 0 0 [0, 1, 11, 2]"},
			)
		}
	})
}

func (s *mockGCSSuite) TestGlobalSortConflictResolutionMultipleSubtasks() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	s.Run("duplicate file content, all row duplicated", func() {
		fileContents := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			fileContents = append(fileContents, "0,0,0\n0,0,0\n0,0,0\n0,0,0\n0,0,0\n0,0,0\n0,0,0")
		}
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a int, b int, unique(a), index(b))`,
			`create table t(pk int primary key nonclustered, a int, b int, unique(a), index(b))`,
		} {
			s.testConflictResolution(tblSQL,
				fileContents,
				[]string{},
			)
		}
	})

	s.Run("duplicate file content, not all row duplicated", func() {
		fileContents := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			fileContents = append(fileContents, "1,1,1\n2,2,2\n3,3,3\n4,4,4\n4,4,4\n4,4,4\n4,4,4")
		}
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a int, b int, unique(a), index(b))`,
			`create table t(pk int primary key nonclustered, a int, b int, unique(a), index(b))`,
		} {
			s.testConflictResolution(tblSQL,
				fileContents,
				[]string{},
			)
		}
	})

	s.Run("rows in conflict group remains in one file", func() {
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			`create table t(pk int primary key nonclustered, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
		} {
			s.testConflictResolution(tblSQL,
				[]string{
					"1,0,0,0,0\n2,1,1,1,1\n3,1,2,2,2\n4,3,2,3,3\n5,1,5,3,5",
					"6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n",
					"7,7,7,7,7\n7,8,8,8,8\n9,8,9,9,9\n10,10,9,10,10\n11,11,11,10,11",
				},
				[]string{"1 0 0 0 0"},
			)
		}
	})

	s.Run("rows in conflict group split into multiple files", func() {
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			`create table t(pk int primary key nonclustered, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
		} {
			s.testConflictResolution(tblSQL,
				[]string{
					"1,0,0,0,0\n2,1,1,1,1\n3,1,2,2,2",
					"4,3,2,3,3\n5,1,5,3,5\n6,6,6,6,6\n6,6,6,6,6",
					"6,6,6,6,6\n6,6,6,6,6\n7,7,7,7,7\n7,8,8,8,8",
					"9,8,9,9,9\n10,10,9,10,10\n11,11,11,10,11",
				},
				[]string{"1 0 0 0 0"},
			)
		}
	})

	s.Run("mixing above cases", func() {
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			`create table t(pk int primary key nonclustered, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
		} {
			s.testConflictResolution(tblSQL,
				[]string{
					"1,0,0,0,0\n2,1,1,1,1\n3,1,2,2,2\n4,3,2,3,3\n5,1,5,3,5",
					"6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n",
					"7,7,7,7,7\n7,8,8,8,8\n9,8,9,9,9\n10,10,9,10,10\n11,11,11,10,11",
					"2,1,1,1,1\n3,1,2,2,2",
					"4,3,2,3,3\n5,1,5,3,5\n6,6,6,6,6\n6,6,6,6,6",
					"6,6,6,6,6\n6,6,6,6,6\n7,7,7,7,7\n7,8,8,8,8",
					"9,8,9,9,9\n10,10,9,10,10\n11,11,11,10,11",
					"12,12,12,12\n12,12,12,12\n12,12,12,12\n12,12,12,12",
					"12,12,12,12\n12,12,12,12\n12,12,12,12\n12,12,12,12",
				},
				[]string{"1 0 0 0 0"},
			)
		}
	})
}

func (s *mockGCSSuite) checkMergeStepConflictInfo(jobID int64) {
	s.T().Helper()

	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(jobID)
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	task, err2 := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	s.NoError(err2)
	subtasks, err := taskManager.GetSubtasksWithHistory(ctx, task.ID, proto.ImportStepMergeSort)
	s.NoError(err)

	taskMeta := importinto.TaskMeta{}
	s.NoError(json.Unmarshal(task.Meta, &taskMeta))
	kvGroupCanConflict := make(map[string]bool, len(taskMeta.Plan.TableInfo.Indices)+1)
	kvGroupCanConflict["data"] = taskMeta.Plan.TableInfo.HasClusteredIndex()
	for _, idx := range taskMeta.Plan.TableInfo.Indices {
		kvGroupCanConflict[importinto.IndexID2KVGroup(idx.ID)] = idx.Unique
	}
	for _, st := range subtasks {
		m := importinto.MergeSortStepMeta{}
		s.NoError(json.Unmarshal(st.Meta, &m))
		if kvGroupCanConflict[m.KVGroup] {
			s.Greater(m.ConflictInfo.Count, uint64(0))
		} else {
			// this is the non-unique index
			s.Zero(m.ConflictInfo.Count)
		}
	}
}

func (s *mockGCSSuite) TestGlobalSortConflictFoundInMergeSort() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	s.Run("duplicate file content, all row duplicated", func() {
		fileContents := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			fileContents = append(fileContents, "0,0,0\n0,0,0\n0,0,0\n0,0,0")
		}
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a int, b int, unique(a), index(b))`,
			`create table t(pk int primary key nonclustered, a int, b int, unique(a), index(b))`,
		} {
			jobID := s.testConflictResolutionWithOptions(tblSQL,
				fileContents,
				[]string{},
				"__force_merge_step",
			)
			s.checkMergeStepConflictInfo(jobID)
		}
	})

	s.Run("duplicate file content, not all row duplicated", func() {
		fileContents := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			fileContents = append(fileContents, "1,1,1\n2,2,2\n3,3,3\n4,4,4\n4,4,4\n4,4,4\n4,4,4")
		}
		for _, tblSQL := range []string{
			`create table t(pk int primary key clustered, a int, b int, unique(a), index(b))`,
			`create table t(pk int primary key nonclustered, a int, b int, unique(a), index(b))`,
		} {
			jobID := s.testConflictResolutionWithOptions(tblSQL,
				fileContents,
				[]string{},
				"__force_merge_step",
			)
			s.checkMergeStepConflictInfo(jobID)
		}
	})
}
