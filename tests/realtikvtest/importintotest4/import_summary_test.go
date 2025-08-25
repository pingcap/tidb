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
	"slices"
	"strconv"

	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func (s *mockGCSSuite) TestGlobalSortSummary() {
	var allData []string
	for i := range 10 {
		var content []byte
		keyCnt := 1000
		for j := range keyCnt {
			idx := i*keyCnt + j
			content = append(content, fmt.Appendf(nil, "%d,test-%d\n", idx, idx)...)
			allData = append(allData, fmt.Sprintf("%d test-%d", idx, idx))
		}
		s.server.CreateObject(fakestorage.Object{
			ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "global-sort-files", Name: fmt.Sprintf("t.%d.csv", i)},
			Content:     content,
		})
	}
	slices.Sort(allData)
	s.prepareAndUseDB("global_sort_summary")
	s.server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: "sorted"})
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), key(b), key(a, b), key(b, a))")
	// 1 subtask, encoding 10 files using 4 threads.
	sortStorageURI := fmt.Sprintf("gs://sorted/gs_multi_files?endpoint=%s", gcsEndpoint)
	importSQL := fmt.Sprintf(`import into t FROM 'gs://global-sort-files/t.*.csv?endpoint=%s'
		with __force_merge_step, cloud_storage_uri='%s'`, gcsEndpoint, sortStorageURI)
	rs := s.tk.MustQuery(importSQL).Rows()
	jobID, err := strconv.Atoi(rs[0][0].(string))
	require.NoError(s.T(), err)

	// Check the result
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(allData...))

	// Check summaries of subtasks
	rs = s.tk.MustQuery("select summary from mysql.tidb_import_jobs where id = ?", jobID).Rows()
	var summaries importer.Summary
	require.NoError(s.T(), json.Unmarshal([]byte(rs[0][0].(string)), &summaries))

	// For global sort with force merge sort, rows of all steps should be 10000 except encode step,
	// since we don't know the exact number of rows before encoding.
	require.EqualValues(s.T(), 10000, summaries.MergeSummary.RowCnt)
	require.EqualValues(s.T(), 10000, summaries.IngestSummary.RowCnt)
	require.EqualValues(s.T(), 10000, summaries.ImportedRows)

	rs = s.tk.MustQuery("show import jobs where Job_ID = ?", jobID).Rows()
	importedRows, err := strconv.Atoi(rs[0][fmap["ImportedRows"]].(string))
	require.NoError(s.T(), err)
	require.EqualValues(s.T(), 10000, importedRows)

	// One encode/postprocess task plus four merge/ingest tasks, total 10 subtasks
	taskKey := importinto.TaskKey(int64(jobID))
	idSQL := `select id from mysql.tidb_global_task where task_key = ?
		union select id from mysql.tidb_global_task_history where task_key = ?`
	rs = s.tk.MustQuery(idSQL, taskKey, taskKey).Rows()
	id, err := strconv.Atoi(rs[0][0].(string))
	require.NoError(s.T(), err)

	sql := `
		SELECT step, summary 
		FROM mysql.tidb_background_subtask 
		WHERE task_key = ?
		UNION ALL
		SELECT step, summary 
		FROM mysql.tidb_background_subtask_history 
		WHERE task_key = ?
		`
	rs = s.tk.MustQuery(sql, id, id).Rows()
	require.Len(s.T(), rs, 10)

	for _, r := range rs {
		step, err := strconv.Atoi(r[0].(string))
		require.NoError(s.T(), err)

		var summary execute.SubtaskSummary
		require.NoError(s.T(), json.Unmarshal([]byte(r[1].(string)), &summary))

		switch proto.Step(step) {
		case proto.ImportStepEncodeAndSort:
			require.EqualValues(s.T(), 10000, summary.RowCnt.Load())
		case proto.ImportStepMergeSort:
			require.EqualValues(s.T(), 10000, summary.RowCnt.Load())
		case proto.ImportStepPostProcess:
			require.EqualValues(s.T(), 0, summary.RowCnt.Load())
		}
	}
}

func (s *mockGCSSuite) TestLocallSortSummary() {
	allData := make([]string, 0, 10000)
	content := make([]byte, 0, 10000*10)

	for idx := range 10000 {
		content = append(content, fmt.Appendf(nil, "%d,test-%d\n", idx, idx)...)
		allData = append(allData, fmt.Sprintf("%d test-%d", idx, idx))
	}
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "local-sort-file", Name: "t.csv"},
		Content:     content,
	})

	slices.Sort(allData)
	s.prepareAndUseDB("local_sort_summary")
	s.tk.MustExec("create table t (a bigint primary key, b varchar(100), key(b), key(a, b), key(b, a))")
	importSQL := fmt.Sprintf(`import into t FROM 'gs://local-sort-file/t.csv?endpoint=%s'`, gcsEndpoint)
	rs := s.tk.MustQuery(importSQL).Rows()
	jobID, err := strconv.Atoi(rs[0][0].(string))
	require.NoError(s.T(), err)

	// Check the result
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows(allData...))

	// Check summaries of subtasks
	rs = s.tk.MustQuery("select summary from mysql.tidb_import_jobs where id = ?", jobID).Rows()
	var summaries importer.Summary
	require.NoError(s.T(), json.Unmarshal([]byte(rs[0][0].(string)), &summaries))

	require.EqualValues(s.T(), 0, summaries.MergeSummary.RowCnt)
	require.EqualValues(s.T(), 10000, summaries.ImportedRows)

	rs = s.tk.MustQuery("show import jobs where Job_ID = ?", jobID).Rows()
	importedRows, err := strconv.Atoi(rs[0][fmap["ImportedRows"]].(string))
	require.NoError(s.T(), err)
	require.EqualValues(s.T(), 10000, importedRows)
}
