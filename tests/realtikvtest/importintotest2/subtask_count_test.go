// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"strconv"

	"github.com/docker/go-units"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/schstatus"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func (s *mockGCSSuite) TestStepSubtaskCount() {
	if kerneltype.IsClassic() {
		s.T().Skip("it's related to the DXF status API, we only test it in next-gen")
	}
	ctx := util.WithInternalSourceType(context.Background(), "taskManager")
	s.prepareAndUseDB("step_subtasks")
	s.tk.MustExec("create table t (a int primary key, b int, c int, d int, unique(c), unique(d))")
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "step-subtasks", Name: "1.csv"},
		Content:     []byte("1,1,1,1\n2,2,2,2\n3,3,3,3\n"),
	})
	s.server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{BucketName: "step-subtasks", Name: "2.csv"},
		// first 3 rows conflicts with the first file.
		Content: []byte("1,1,1,1\n4,4,4,2\n5,5,3,5\n6,6,6,6\n"),
	})
	// after amplify, the real size is 58*14G=812G, and we mock the node as 16c,
	// so the computed resource should be at least 2 nodes
	var status *schstatus.Status
	testfailpoint.Enable(s.T(), "github.com/pingcap/tidb/pkg/executor/importer/amplifyRealSize", fmt.Sprintf("return(%d)", 14*units.GiB))
	testfailpoint.EnableCall(s.T(), "github.com/pingcap/tidb/pkg/dxf/importinto/syncBeforePostProcess", func(int64) {
		var err error
		status, err = handle.GetScheduleStatus(ctx)
		s.NoError(err)
	})
	importSQL := fmt.Sprintf(`import into t FROM 'gs://step-subtasks/*.csv?endpoint=%s'
		with __force_merge_step, __max_engine_size='1', cloud_storage_uri='%s'`, gcsEndpoint,
		realtikvtest.GetNextGenObjStoreURI("gl-sort"))
	rs := s.tk.MustQuery(importSQL).Rows()
	jobID, err := strconv.Atoi(rs[0][0].(string))
	require.NoError(s.T(), err)
	s.tk.MustQuery("select * from t").Sort().Check(testkit.Rows("6 6 6 6"))
	s.NotNil(status)
	// only 1 node is required in post process step.
	s.Equal(status.TiDBWorker.RequiredCount, 1)

	taskKey := importinto.TaskKey(int64(jobID))
	mgr, err := storage.GetTaskManager()
	s.NoError(err)
	task, err := mgr.GetTaskByKeyWithHistory(ctx, taskKey)
	s.NoError(err)
	s.EqualValues(16, task.RequiredSlots)
	s.Greater(task.MaxNodeCount, 2)
	for step, stCnt := range map[proto.Step]int{
		proto.ImportStepEncodeAndSort:  2,
		proto.ImportStepMergeSort:      3,
		proto.ImportStepWriteAndIngest: 3,
		// if later we support resolve conflicts distributedly, should also
		// change getNeededNodes to make sure the status API works.
		proto.ImportStepCollectConflicts:   1,
		proto.ImportStepConflictResolution: 1,
		proto.ImportStepPostProcess:        1,
	} {
		sts, err := mgr.GetSubtasksWithHistory(ctx, task.ID, step)
		s.NoError(err)
		s.Len(sts, stCnt)
	}
}
