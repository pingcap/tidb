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
	s.T().Helper()
	for i, content := range sourceContents {
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

	sortStorageURI := fmt.Sprintf("gs://sorted/conflicts?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://conflicts/t.*.csv?endpoint=%s'
		with __max_engine_size = '1', cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
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
	return int64(jobID)
}

func (s *mockGCSSuite) TestGlobalSortConflictResolutionBasicCases() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	/****************************************************************************
	* ALL CASES CONTAINS NON-UNIQUE SECONDARY INDEXES, SO WE DON'T TEST IT SEPARATELY.
	****************************************************************************/

	s.Run("all row duplicated", func() {
		// Note: '2,2,3' is not whole row duplicated, but it only differs in the
		// column that have non-unique index, so it's the same as '2,2,2' from the
		// conflict resolution perspective.
		s.testSingleFileConflictResolution(
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
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
	})

	s.Run("whole row duplicated, but not all", func() {
		// Note: '2,2,3' is not whole row duplicated, but it only differs in the
		// column that have non-unique index, so it's the same as '2,2,2' from the
		// conflict resolution perspective.
		s.testSingleFileConflictResolution(
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
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
	})

	s.Run("clustered pk conflict", func() {
		for _, tblSQL := range []string{
			// `create table t(a int primary key clustered, b int, index(b))`,
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
		s.testSingleFileConflictResolution(
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
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
	})

	s.Run("mixing pk/single-uk conflict, cycle", func() {
		// A---B---C---D
		// └-----------┘
		s.testSingleFileConflictResolution(
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			`
0,0,0
1,1,1
1,2,1
2,2,1
1,2,2
`,
			[]string{"0 0 0"},
		)
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

	s.Run("partition table", func() {
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))
	partition by range(pk)(
		partition p0 values less than (6),
		partition p1 values less than (12),
		partition p2 values less than (18),
		partition p3 values less than (MAXVALUE)
	)`,
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

12,12,12,12,12
12,12,12,12,12
12,12,12,12,14
13,13,13,13,13
`,
			[]string{"1 0 0 0 0", "13 13 13 13 13"},
		)
	})

	s.Run("multiple value index", func() {
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
		s.testConflictResolution(
			`create table t(pk int primary key, a int, b int, unique(a), index(b))`,
			fileContents,
			[]string{},
		)
	})

	s.Run("duplicate file content, not all row duplicated", func() {
		fileContents := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			fileContents = append(fileContents, "1,1,1\n2,2,2\n3,3,3\n4,4,4\n4,4,4\n4,4,4\n4,4,4")
		}
		s.testConflictResolution(
			`create table t(pk int primary key, a int, b int, unique(a), index(b))`,
			fileContents,
			[]string{},
		)
	})

	s.Run("rows in conflict group remains in one file", func() {
		s.testConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			[]string{
				"1,0,0,0,0\n2,1,1,1,1\n3,1,2,2,2\n4,3,2,3,3\n5,1,5,3,5",
				"6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n6,6,6,6,6\n",
				"7,7,7,7,7\n7,8,8,8,8\n9,8,9,9,9\n10,10,9,10,10\n11,11,11,10,11",
			},
			[]string{"1 0 0 0 0"},
		)
	})

	s.Run("rows in conflict group split into multiple files", func() {
		s.testConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			[]string{
				"1,0,0,0,0\n2,1,1,1,1\n3,1,2,2,2",
				"4,3,2,3,3\n5,1,5,3,5\n6,6,6,6,6\n6,6,6,6,6",
				"6,6,6,6,6\n6,6,6,6,6\n7,7,7,7,7\n7,8,8,8,8",
				"9,8,9,9,9\n10,10,9,10,10\n11,11,11,10,11",
			},
			[]string{"1 0 0 0 0"},
		)
	})

	s.Run("mixing above cases", func() {
		s.testConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
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
	for _, st := range subtasks {
		m := importinto.MergeSortStepMeta{}
		s.NoError(json.Unmarshal(st.Meta, &m))
		if m.KVGroup == "2" {
			// this is the non-unique index
			s.Zero(m.ConflictInfo.Count)
		} else {
			s.Greater(m.ConflictInfo.Count, uint64(0))
		}
	}
}

func (s *mockGCSSuite) TestGlobalSortConflictResolutionFoundInMergeSort() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	s.Run("duplicate file content, all row duplicated", func() {
		fileContents := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			fileContents = append(fileContents, "0,0,0\n0,0,0\n0,0,0\n0,0,0")
		}
		jobID := s.testConflictResolutionWithOptions(
			`create table t(pk int primary key, a int, b int, unique(a), index(b))`,
			fileContents,
			[]string{},
			"__force_merge_step",
		)
		s.checkMergeStepConflictInfo(jobID)
	})

	s.Run("duplicate file content, not all row duplicated", func() {
		fileContents := make([]string, 0, 10)
		for i := 0; i < 10; i++ {
			fileContents = append(fileContents, "1,1,1\n2,2,2\n3,3,3\n4,4,4\n4,4,4\n4,4,4\n4,4,4")
		}
		jobID := s.testConflictResolutionWithOptions(
			`create table t(pk int primary key, a int, b int, unique(a), index(b))`,
			fileContents,
			[]string{},
			"__force_merge_step",
		)
		s.checkMergeStepConflictInfo(jobID)
	})
}
