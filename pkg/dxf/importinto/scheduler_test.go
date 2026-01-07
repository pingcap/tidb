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

package importinto

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/executor/importer"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type importIntoSuite struct {
	suite.Suite
}

func TestImportInto(t *testing.T) {
	suite.Run(t, &importIntoSuite{})
}

func (s *importIntoSuite) enableFailPoint(path, term string) {
	require.NoError(s.T(), failpoint.Enable(path, term))
	s.T().Cleanup(func() {
		_ = failpoint.Disable(path)
	})
}

func (s *importIntoSuite) TestSchedulerGetEligibleInstances() {
	sch := importScheduler{}
	task := &proto.Task{Meta: []byte("{}")}
	ctx := context.WithValue(context.Background(), "etcd", true)
	eligibleInstances, err := sch.GetEligibleInstances(ctx, task)
	s.NoError(err)
	// order of slice is not stable, change to map
	s.Empty(eligibleInstances)

	task.Meta = []byte(`{"EligibleInstances":[{"ip": "1.1.1.1", "listening_port": 4000}]}`)
	eligibleInstances, err = sch.GetEligibleInstances(ctx, task)
	s.NoError(err)
	s.Equal([]string{"1.1.1.1:4000"}, eligibleInstances)
}

func (s *importIntoSuite) TestUpdateCurrentTask() {
	taskMeta := TaskMeta{
		Plan: importer.Plan{
			DisableTiKVImportMode: true,
		},
	}
	bs, err := json.Marshal(taskMeta)
	require.NoError(s.T(), err)

	sch := importScheduler{}
	require.Equal(s.T(), int64(0), sch.currTaskID.Load())
	require.False(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		TaskBase: proto.TaskBase{ID: 1},
		Meta:     bs,
	})
	require.Equal(s.T(), int64(1), sch.currTaskID.Load())
	require.True(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		TaskBase: proto.TaskBase{ID: 1},
		Meta:     bs,
	})
	require.Equal(s.T(), int64(1), sch.currTaskID.Load())
	require.True(s.T(), sch.disableTiKVImportMode.Load())
}

func (s *importIntoSuite) TestSchedulerInit() {
	meta := TaskMeta{
		Plan: importer.Plan{
			CloudStorageURI: "",
		},
	}
	bytes, err := json.Marshal(meta)
	s.NoError(err)
	taskKS := ""
	if kerneltype.IsNextGen() {
		taskKS = "user_keyspace"
	}
	sch := importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			TaskBase: proto.TaskBase{Keyspace: taskKS},
			Meta:     bytes,
		}, scheduler.Param{TaskStore: &StoreWithKS{ks: taskKS}}),
	}
	s.NoError(sch.Init())
	s.False(sch.Extension.(*importScheduler).GlobalSort)

	meta.Plan.CloudStorageURI = "s3://test"
	bytes, err = json.Marshal(meta)
	s.NoError(err)
	sch = importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			TaskBase: proto.TaskBase{Keyspace: taskKS},
			Meta:     bytes,
		}, scheduler.Param{TaskStore: &StoreWithKS{ks: taskKS}}),
	}
	s.NoError(sch.Init())
	s.True(sch.Extension.(*importScheduler).GlobalSort)

	if kerneltype.IsNextGen() {
		sch = importScheduler{
			BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
				TaskBase: proto.TaskBase{Keyspace: taskKS},
				Meta:     bytes,
			}, scheduler.Param{TaskStore: &StoreWithKS{}}),
		}
		s.ErrorContains(sch.Init(), "store keyspace mismatch with task")
	}
}

func (s *importIntoSuite) TestGetNextStep() {
	task := &proto.TaskBase{Step: proto.StepInit}
	ext := &importScheduler{}
	for _, nextStep := range []proto.Step{proto.ImportStepImport, proto.ImportStepPostProcess, proto.StepDone} {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}

	task.Step = proto.StepInit
	ext = &importScheduler{GlobalSort: true}
	var targetSteps []proto.Step
	if kerneltype.IsClassic() {
		targetSteps = []proto.Step{proto.ImportStepEncodeAndSort, proto.ImportStepMergeSort,
			proto.ImportStepWriteAndIngest, proto.ImportStepCollectConflicts, proto.ImportStepConflictResolution,
			proto.ImportStepPostProcess, proto.StepDone}
	} else {
		targetSteps = []proto.Step{proto.ImportStepEncodeAndSort, proto.ImportStepMergeSort,
			proto.ImportStepWriteAndIngest, proto.ImportStepPostProcess, proto.StepDone}
	}
	for _, nextStep := range targetSteps {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}
}

func (s *importIntoSuite) TestGetStepOfEncode() {
	s.Equal(proto.ImportStepImport, getStepOfEncode(false))
	s.Equal(proto.ImportStepEncodeAndSort, getStepOfEncode(true))
}

func (s *importIntoSuite) TestIsRetryable() {
	ext := &importScheduler{}
	require.True(s.T(), ext.IsRetryableErr(drivererr.ErrRegionUnavailable))
	require.True(s.T(), ext.IsRetryableErr(errors.Annotatef(errGetCrossKSSessionPool, "test")))
}

func TestIsImporting2TiKV(t *testing.T) {
	ext := &importScheduler{}
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepEncodeAndSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepMergeSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepPostProcess}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepImport}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepWriteAndIngest}}))
}
