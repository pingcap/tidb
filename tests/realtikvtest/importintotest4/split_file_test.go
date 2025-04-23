// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/tikv/client-go/v2/util"
)

func (s *mockGCSSuite) TestSplitFile() {
	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "taskManager")
	var allData []string
	var content []byte
	for j := 0; j < 500; j++ {
		content = append(content, []byte(fmt.Sprintf("%d,test-%d\n", j, j))...)
		allData = append(allData, fmt.Sprintf("%d test-%d", j, j))
	}
	slices.Sort(allData)
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "split-file", Name: "1.csv"},
		Content:     content,
	})
	s.prepareAndUseDB("split_file")
	s.tk.MustExec("create table t (a bigint primary key , b varchar(100));")
	// 1.csv should be split into 3 chunks
	backup := config.MaxRegionSize
	config.MaxRegionSize = config.ByteSize(int64(math.Ceil(float64(len(content)) / 3)))
	s.T().Cleanup(func() {
		config.MaxRegionSize = backup
	})
	// split into 3 engines(subtasks)
	importSQL := fmt.Sprintf(`import into split_file.t FROM 'gs://split-file/1.csv?endpoint=%s'
		with split_file, lines_terminated_by = '\n', __max_engine_size = '1'`, gcsEndpoint)
	result := s.tk.MustQuery(importSQL).Rows()
	s.Len(result, 1)
	jobID, err := strconv.Atoi(result[0][0].(string))
	s.NoError(err)
	taskManager, err := storage.GetTaskManager()
	s.NoError(err)
	taskKey := importinto.TaskKey(int64(jobID))
	s.NoError(err)
	task, err2 := taskManager.GetTaskByKeyWithHistory(ctx, taskKey)
	s.NoError(err2)

	subtasks, err2 := taskManager.GetSubtasksWithHistory(ctx, task.ID, proto.ImportStepImport)
	s.NoError(err2)
	s.Len(subtasks, 3)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(allData...))

	// skip 1 row
	s.tk.MustExec("truncate table t")
	importSQL = fmt.Sprintf(`import into split_file.t FROM 'gs://split-file/1.csv?endpoint=%s'
		with split_file, lines_terminated_by = '\n', skip_rows = 1, __max_engine_size = '1'`, gcsEndpoint)
	s.tk.MustQuery(importSQL)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(allData[1:]...))

	s.tk.MustExec("create table t2 (a int primary key nonclustered, b varchar(100));")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "split-file", Name: "2.csv"},
		Content:     []byte("1,2\r\n3,4\r\n5,6\r\n7,8\r\n9,10\r\n"),
	})
	config.MaxRegionSize = 9
	importSQL = fmt.Sprintf(`import into split_file.t2 FROM 'gs://split-file/2.csv?endpoint=%s'
		with split_file, lines_terminated_by='\r\n'`, gcsEndpoint)
	s.tk.MustQuery(importSQL)
	s.tk.MustExec("admin check table t2")
}
