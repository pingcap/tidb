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

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	sqlsvrapimock "github.com/pingcap/tidb/pkg/domain/sqlsvrapi/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/mock"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	drivererr "github.com/pingcap/tidb/pkg/store/driver/error"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
)

type importIntoSuite struct {
	suite.Suite
}

func TestImportInto(t *testing.T) {
	suite.Run(t, &importIntoSuite{})
}

func newMockRuntime(
	ctrl *gomock.Controller,
	store kv.Storage,
	sePool tidbutil.DestroyableSessionPool,
) *sqlsvrapimock.MockRuntime {
	runtime := sqlsvrapimock.NewMockRuntime(ctrl)
	runtime.EXPECT().Store().Return(store).AnyTimes()
	runtime.EXPECT().SysSessionPool().Return(sePool).AnyTimes()
	return runtime
}

func newSchedulerParamForTest(
	t *testing.T,
	taskMgr scheduler.TaskManager,
	store kv.Storage,
	sePool tidbutil.DestroyableSessionPool,
) scheduler.Param {
	t.Helper()

	ctrl := gomock.NewController(t)
	if sePool == nil {
		sePool = tidbutil.NewSessionPool(1, func() (pools.Resource, error) {
			se := utilmock.NewContext()
			se.Store = store
			return se, nil
		}, nil, nil, nil)
		t.Cleanup(sePool.Close)
	}

	param := scheduler.NewParamForTest(taskMgr, newMockRuntime(ctrl, store, sePool))
	return param
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
		}, newSchedulerParamForTest(s.T(), nil, &StoreWithKS{ks: taskKS}, nil)),
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
		}, newSchedulerParamForTest(s.T(), nil, &StoreWithKS{ks: taskKS}, nil)),
	}
	s.NoError(sch.Init())
	s.True(sch.Extension.(*importScheduler).GlobalSort)

	if kerneltype.IsNextGen() {
		sch = importScheduler{
			BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
				TaskBase: proto.TaskBase{Keyspace: taskKS},
				Meta:     bytes,
			}, newSchedulerParamForTest(s.T(), nil, &StoreWithKS{}, nil)),
		}
		s.ErrorContains(sch.Init(), "store keyspace mismatch with task")
	}
}

func (s *importIntoSuite) TestGetTaskMgrForAccessingImportJobUsesTaskRuntime() {
	if !kerneltype.IsNextGen() {
		s.T().Skip("TaskRuntime is used only for nextgen user keyspace tasks")
	}
	ctrl := gomock.NewController(s.T())
	defer ctrl.Finish()
	taskMgr := mock.NewMockTaskManager(ctrl)

	sessPool := tidbutil.NewSessionPool(1, func() (pools.Resource, error) {
		return nil, errors.New("unexpected session pool use")
	}, nil, nil, nil)
	s.T().Cleanup(sessPool.Close)

	taskKS := "user_keyspace"
	param := newSchedulerParamForTest(s.T(), taskMgr, &StoreWithKS{ks: taskKS}, sessPool)
	sch := importScheduler{
		BaseScheduler: scheduler.NewBaseScheduler(context.Background(), &proto.Task{
			TaskBase: proto.TaskBase{Keyspace: taskKS},
		}, param),
	}

	got, err := sch.getTaskMgrForAccessingImportJob()
	require.NoError(s.T(), err)
	require.NotNil(s.T(), got)
	require.Same(s.T(), got, sch.taskKSTaskMgr)
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
	for _, nextStep := range []proto.Step{proto.ImportStepEncodeAndSort, proto.ImportStepMergeSort,
		proto.ImportStepWriteAndIngest, proto.ImportStepCollectConflicts, proto.ImportStepConflictResolution,
		proto.ImportStepPostProcess, proto.StepDone} {
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
	require.False(s.T(), ext.IsRetryableErr(exeerrors.ErrLoadDataPreCheckFailed.FastGenByArgs("target table is not empty")))
}

func TestIsImporting2TiKV(t *testing.T) {
	ext := &importScheduler{}
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepEncodeAndSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepMergeSort}}))
	require.False(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepPostProcess}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepImport}}))
	require.True(t, ext.isImporting2TiKV(&proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepWriteAndIngest}}))
}
