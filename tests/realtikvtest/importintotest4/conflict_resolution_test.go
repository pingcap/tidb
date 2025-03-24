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
	"fmt"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest/addindextestutil"
)

func (s *mockGCSSuite) testSingleFileConflictResolution(tblSQL string, sourceContent []byte, resultRows []string) {
	s.T().Helper()
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "conflicts", Name: "t.1.csv"},
		Content:     sourceContent,
	})
	s.T().Cleanup(func() {
		addindextestutil.RemoveAllObjects(s.T(), s.server, "conflicts")
		addindextestutil.RemoveAllObjects(s.T(), s.server, "sorted")
	})

	s.prepareAndUseDB("conflicts")
	s.tk.MustExec(tblSQL)

	sortStorageURI := fmt.Sprintf("gs://sorted/conflicts?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://conflicts/t.*.csv?endpoint=%s'
		with __max_engine_size = '1', cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
	result := s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	s.Equal(fmt.Sprintf("%d", len(resultRows)), result[0][7])
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(resultRows...))
	s.tk.MustExec("admin check table t")
}

func (s *mockGCSSuite) TestGlobalSortConflictResolutionBasicCases() {
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "conflicts"})
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})

	/****************************************************************************
	* ALL CASES CONTAINS NON-UNIQUE SECONDARY INDEXES, SO WE DON'T TEST IT SEPARATELY.
	****************************************************************************/

	s.Run("whole row duplicated", func() {
		s.testSingleFileConflictResolution(
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			[]byte(`
0,0,0
1,1,1
1,1,1
1,1,1
2,2,2
2,2,3
3,\N,3
3,\N,3
`),
			[]string{"0 0 0"},
		)
	})

	s.Run("clustered pk conflict", func() {
		for _, tblSQL := range []string{
			// `create table t(a int primary key clustered, b int, index(b))`,
			`create table t(a int primary key nonclustered, b int, index(b))`,
		} {
			s.testSingleFileConflictResolution(tblSQL,
				[]byte(`
0,0
1,1
1,2
2,2
2,2
2,3
`),
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
				[]byte(`
0,0,0
1,1,0
2,1,1
3,2,1
4,2,2
5,2,2
`),
				[]string{"0 0 0"},
			)
		}
	})

	s.Run("mixing pk/single-uk conflict, no cycle", func() {
		s.testSingleFileConflictResolution(
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			[]byte(`
0,0,0
1,1,1
1,1,1
2,2,1
3,2,2
4,2,3
4,3,3
5,3,2
`),
			[]string{"0 0 0"},
		)
	})

	s.Run("mixing pk/single-uk conflict, cycle", func() {
		// A---B---C---D
		// └-----------┘
		s.testSingleFileConflictResolution(
			`create table t(a int primary key clustered, b int, c int, unique(b), index(c))`,
			[]byte(`
0,0,0
1,1,1
1,2,1
2,2,1
1,2,2
`),
			[]string{"0 0 0"},
		)
	})

	s.Run("multiple-uk conflict, no cycle", func() {
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, unique(a), unique(b), index(c))`,
			[]byte(`
1,0,0,0
2,1,1,1
3,1,1,1
4,2,2,1
5,3,2,2
6,4,2,3
7,4,3,3
8,5,3,2
`),
			[]string{"1 0 0 0"},
		)
	})

	s.Run("multiple-uk conflict, cycle", func() {
		// A---B---C---D---E
		// └---------------┘
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, unique(a), unique(b), index(c))`,
			[]byte(`
1,0,0,0
2,1,1,1
3,1,2,1
4,2,2,1
5,2,3,2
6,1,3,2
`),
			[]string{"1 0 0 0"},
		)
	})

	s.Run("multiple-uk conflict, complex cycle", func() {
		// A---B---C
		//  \  |  /
		//   \ | /
		//     D
		// in below case, C---D have 2 conflicts
		s.testSingleFileConflictResolution(
			`create table t(pk int primary key, a int, b int, c int, d int, unique(a), unique(b), unique(c), index(d))`,
			[]byte(`
1,0,0,0,0
2,1,1,1,1
3,1,2,2,2
4,3,2,3,3
5,1,2,3,5
`),
			[]string{"1 0 0 0 0"},
		)
	})
}
