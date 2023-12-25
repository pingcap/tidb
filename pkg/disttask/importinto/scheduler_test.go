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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/executor/importer"
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
	sch := ImportSchedulerExt{}
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

	sch := ImportSchedulerExt{}
	require.Equal(s.T(), int64(0), sch.currTaskID.Load())
	require.False(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		ID:   1,
		Meta: bs,
	})
	require.Equal(s.T(), int64(1), sch.currTaskID.Load())
	require.True(s.T(), sch.disableTiKVImportMode.Load())

	sch.updateCurrentTask(&proto.Task{
		ID:   1,
		Meta: bs,
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
	sch := importScheduler{
		BaseScheduler: &scheduler.BaseScheduler{
			Task: &proto.Task{
				Meta: bytes,
			},
		},
	}
	s.NoError(sch.Init())
	s.False(sch.Extension.(*ImportSchedulerExt).GlobalSort)

	meta.Plan.CloudStorageURI = "s3://test"
	bytes, err = json.Marshal(meta)
	s.NoError(err)
	sch = importScheduler{
		BaseScheduler: &scheduler.BaseScheduler{
			Task: &proto.Task{
				Meta: bytes,
			},
		},
	}
	s.NoError(sch.Init())
	s.True(sch.Extension.(*ImportSchedulerExt).GlobalSort)
}

func (s *importIntoSuite) TestGetNextStep() {
	task := &proto.Task{
		Step: proto.StepInit,
	}
	ext := &ImportSchedulerExt{}
	for _, nextStep := range []proto.Step{StepImport, StepPostProcess, proto.StepDone} {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}

	task.Step = proto.StepInit
	ext = &ImportSchedulerExt{GlobalSort: true}
	for _, nextStep := range []proto.Step{StepEncodeAndSort, StepMergeSort,
		StepWriteAndIngest, StepPostProcess, proto.StepDone} {
		s.Equal(nextStep, ext.GetNextStep(task))
		task.Step = nextStep
	}
}

func (s *importIntoSuite) TestStr() {
	s.Equal("init", stepStr(proto.StepInit))
	s.Equal("import", stepStr(StepImport))
	s.Equal("post-process", stepStr(StepPostProcess))
	s.Equal("merge-sort", stepStr(StepMergeSort))
	s.Equal("encode&sort", stepStr(StepEncodeAndSort))
	s.Equal("write&ingest", stepStr(StepWriteAndIngest))
	s.Equal("done", stepStr(proto.StepDone))
	s.Equal("unknown", stepStr(111))
}

func (s *importIntoSuite) TestGetStepOfEncode() {
	s.Equal(StepImport, getStepOfEncode(false))
	s.Equal(StepEncodeAndSort, getStepOfEncode(true))
}

func TestIsImporting2TiKV(t *testing.T) {
	ext := &ImportSchedulerExt{}
	require.False(t, ext.isImporting2TiKV(&proto.Task{Step: StepEncodeAndSort}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{Step: StepMergeSort}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{Step: StepPostProcess}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{Step: StepImport}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{Step: StepWriteAndIngest}))
}
