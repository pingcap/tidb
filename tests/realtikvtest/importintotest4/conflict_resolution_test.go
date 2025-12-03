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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/disttask/importinto/conflictedkv"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest/testutils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *mockGCSSuite) testSingleFileConflictResolution(tblSQL string, sourceContent string, resultRows []string) {
	s.T().Helper()
	s.testConflictResolution(tblSQL, []string{sourceContent}, resultRows)
}

func (s *mockGCSSuite) testConflictResolution(tblSQL string, sourceContents []string, resultRows []string) int64 {
	s.T().Helper()
	return s.testConflictResolutionWithOptions(tblSQL, sourceContents, resultRows, "")
}

func (s *mockGCSSuite) testConflictResolutionWithOptions(tblSQL string, sourceContents []string, resultRows []string, options string) int64 {
	s.T().Helper()
	return s.testConflictResolutionWithColumnVarsAndOptions(tblSQL, sourceContents, resultRows, "", options)
}

func (s *mockGCSSuite) getSourceRowCount(sourceContents []string) int {
	totalRowCount := 0
	for _, content := range sourceContents {
		rows := strings.Split(content, "\n")
		for _, r := range rows {
			if len(r) > 0 {
				totalRowCount++
			}
		}
	}
	return totalRowCount
}

func (s *mockGCSSuite) getTaskByJob(jobID int64) *proto.Task {
	s.T().Helper()
	taskKey := importinto.TaskKey(jobID)
	task, err2 := s.taskMgr.GetTaskByKeyWithHistory(s.ctx, taskKey)
	s.NoError(err2)
	return task
}

func (s *mockGCSSuite) getSubtasksOfStep(taskID int64, step proto.Step) []*proto.Subtask {
	s.T().Helper()
	subtasks, err := s.taskMgr.GetSubtasksWithHistory(s.ctx, taskID, step)
	s.NoError(err)
	return subtasks
}

func (s *mockGCSSuite) testConflictResolutionWithColumnVarsAndOptions(tblSQL string,
	sourceContents []string, resultRows []string, columnVars string, options string) int64 {
	s.T().Helper()
	totalRowCount := s.getSourceRowCount(sourceContents)
	for i, content := range sourceContents {
		filename := fmt.Sprintf("t.%d.csv", i)
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "conflicts", Name: filename},
			Content:     []byte(content),
		})
	}
	// we need the intermediate files to check meta.
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/lightning/backend/external/skipCleanUpFiles", `return(true)`)
	s.T().Cleanup(func() {
		testutils.RemoveAllObjects(s.T(), s.server, "conflicts")
		testutils.RemoveAllObjects(s.T(), s.server, "sorted")
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
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(resultRows...))
	s.tk.MustExec("admin check table t")
	jobID, err := strconv.Atoi(result[0][0].(string))
	s.NoError(err)

	task := s.getTaskByJob(int64(jobID))
	s.checkExternalFields(task.ID, false)

	// check lines inside the file that records the conflicted rows.
	subtasks := s.getSubtasksOfStep(task.ID, proto.ImportStepCollectConflicts)
	s.Len(subtasks, 1)
	subtaskM := &importinto.CollectConflictsStepMeta{}
	s.NoError(json.Unmarshal(subtasks[0].Meta, subtaskM))
	if !subtaskM.TooManyConflictsFromIndex {
		s.Equal(fmt.Sprintf("%d", len(resultRows)), result[0][8])

		totalConflictedRowCnt := 0
		for _, filename := range subtaskM.ConflictedRowFilenames {
			object, err := s.server.GetObject("sorted", filename)
			s.NoError(err)
			conflictedRows := strings.Split(strings.Trim(string(object.Content), "\n"), "\n")
			totalConflictedRowCnt += len(conflictedRows)
		}
		s.EqualValues(totalRowCount-len(resultRows), totalConflictedRowCnt)
	}

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
			//`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
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

	task := s.getTaskByJob(jobID)
	subtasks := s.getSubtasksOfStep(task.ID, proto.ImportStepMergeSort)

	taskMeta := importinto.TaskMeta{}
	s.NoError(json.Unmarshal(task.Meta, &taskMeta))
	kvGroupCanConflict := make(map[string]bool, len(taskMeta.Plan.TableInfo.Indices)+1)
	kvGroupCanConflict["data"] = taskMeta.Plan.TableInfo.HasClusteredIndex()
	for _, idx := range taskMeta.Plan.TableInfo.Indices {
		kvGroupCanConflict[external.IndexID2KVGroup(idx.ID)] = idx.Unique
	}
	store, err := importer.GetSortStore(s.ctx, taskMeta.Plan.CloudStorageURI)
	s.NoError(err)
	for _, st := range subtasks {
		m := importinto.MergeSortStepMeta{}
		s.NoError(json.Unmarshal(st.Meta, &m))
		s.NotEmpty(m.ExternalPath)
		s.NoError(m.BaseExternalMeta.ReadJSONFromExternalStorage(s.ctx, store, &m))
		if kvGroupCanConflict[m.KVGroup] {
			s.Greater(m.ConflictInfo.Count, uint64(0))
		} else {
			// this is the non-unique index
			s.Zero(m.ConflictInfo.Count)
		}
		s.Equal(m.RecordedConflictKVCount, m.ConflictInfo.Count)
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

func (s *mockGCSSuite) TestGlobalSortRetryOnConflictResolutionStep() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	doTestWithFP := func(t *testing.T, fpName string) {
		t.Helper()
		fileContents := []string{
			"0,0,0,0,0",
			"2,1,1,1,1\n2,2,2,2,2\n2,3,3,3,3",
			"4,4,4,4,4\n4,4,4,4,4\n4,4,4,4,4\n4,4,4,4,4",
			"5,5,5,5,5\n6,5,6,6,6\n7,7,7,7,7\n8,8,7,8,8\n9,9,9,8,9",
		}
		var cnt atomic.Int32
		testfailpoint.EnableCall(s.T(), fpName, func(errP *error) {
			s.NoError(*errP)
			// we have 4 KV group to handle, return a retryable error after handle
			// first 2
			if cnt.Add(1) == 2 {
				*errP = status.New(codes.Unknown, "inject error").Err()
			}
		})
		s.testConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			fileContents,
			[]string{"0 0 0 0 0"},
		)
	}

	s.Run("retry on collect conflicts step", func() {
		doTestWithFP(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/afterCollectOneKVGroup")
	})

	s.Run("retry on conflict resolution step", func() {
		doTestWithFP(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/afterResolveOneKVGroup")
	})
}

func (s *mockGCSSuite) TestGlobalSortConflictedRowsExceedMaxFileSize() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/forceHandleConflictsBySingleThread", "return(true)")
	bak := conflictedkv.MaxConflictRowFileSize
	conflictedkv.MaxConflictRowFileSize = 48
	s.T().Cleanup(func() {
		conflictedkv.MaxConflictRowFileSize = bak
	})
	contents := []string{
		"16 bytes: 000000",
		"16 bytes: 000001", "16 bytes: 000001", "16 bytes: 000001",
		"16 bytes: 000002", "16 bytes: 000002", "16 bytes: 000002",
		"16 bytes: 000003", "16 bytes: 000003", "16 bytes: 000003",
		"16 bytes: 000004", "16 bytes: 000004",
	}
	jobID := s.testConflictResolution(
		`create table t(pk varchar(64) primary key clustered)`,
		contents,
		[]string{"16 bytes: 000000"},
	)
	task := s.getTaskByJob(jobID)
	subtasks := s.getSubtasksOfStep(task.ID, proto.ImportStepCollectConflicts)
	s.Len(subtasks, 1)
	subtaskM := &importinto.CollectConflictsStepMeta{}
	s.NoError(json.Unmarshal(subtasks[0].Meta, subtaskM))
	// 3 rows per file except the last one
	s.Len(subtaskM.ConflictedRowFilenames, 4)
	s.False(subtaskM.TooManyConflictsFromIndex)

	// we still need to verify checksum
	subtasks = s.getSubtasksOfStep(task.ID, proto.ImportStepPostProcess)
	s.Len(subtasks, 1)
	ppMeta := &importinto.PostProcessStepMeta{}
	s.NoError(json.Unmarshal(subtasks[0].Meta, ppMeta))
	s.False(ppMeta.TooManyConflictsFromIndex)
}

func (s *mockGCSSuite) TestGlobalSortTooManyConflictedRowsFromIndex() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/forceHandleConflictsBySingleThread", "return(true)")
	var fpEntered atomic.Int32
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/disttask/importinto/conflictedkv/trySaveHandledRowFromIndex", func(limitP *int64) {
		*limitP = 0
		fpEntered.CompareAndSwap(0, 1)
	})
	jobID := s.testConflictResolution(
		`create table t(pk int primary key clustered, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
		[]string{
			"1,0,0,0,0\n2,1,1,1,1\n3,1,1,1,1\n4,3,3,3,3\n5,3,3,3,3",
			"6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n",
			"7,7,7,7,7\n7,8,8,8,8\n9,8,9,9,9\n10,10,9,10,10\n11,11,11,10,11",
			"12,12,12,12\n12,12,12,12\n12,12,12,12\n12,12,12,12",
		},
		[]string{"1 0 0 0 0"},
	)
	s.EqualValues(1, fpEntered.Load())

	task := s.getTaskByJob(jobID)

	subtasks := s.getSubtasksOfStep(task.ID, proto.ImportStepCollectConflicts)
	s.Len(subtasks, 1)
	subtaskM := &importinto.CollectConflictsStepMeta{}
	s.NoError(json.Unmarshal(subtasks[0].Meta, subtaskM))
	s.True(subtaskM.TooManyConflictsFromIndex)

	// verify checksum is skipped, else there will be a checksum mismatch error
	subtasks = s.getSubtasksOfStep(task.ID, proto.ImportStepPostProcess)
	s.Len(subtasks, 1)
	ppMeta := &importinto.PostProcessStepMeta{}
	s.NoError(json.Unmarshal(subtasks[0].Meta, ppMeta))
	s.True(ppMeta.TooManyConflictsFromIndex)
}
