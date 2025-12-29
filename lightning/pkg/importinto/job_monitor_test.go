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

package importinto_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/pingcap/tidb/lightning/pkg/importinto"
	mockimport "github.com/pingcap/tidb/lightning/pkg/importinto/mock"
	"github.com/pingcap/tidb/pkg/importsdk"
	sdkmock "github.com/pingcap/tidb/pkg/importsdk/mock"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestJobMonitorWaitForJobs(t *testing.T) {
	tests := []struct {
		name    string
		jobs    []*importinto.ImportJob
		setup   func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager)
		wantErr bool
	}{
		{
			name:  "no jobs",
			jobs:  []*importinto.ImportJob{},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager) {},
		},
		{
			name: "one job success",
			jobs: []*importinto.ImportJob{
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager) {
				// First poll: running
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "running"},
				}, nil)
				// Second poll: finished
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "finished", ImportedRows: 100},
				}, nil)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t1"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFinished, cp.Status)
					return nil
				})
			},
		},
		{
			name: "one job failed",
			jobs: []*importinto.ImportJob{
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager) {
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed", ResultMessage: "some error"},
				}, nil)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t1"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFailed, cp.Status)
					return nil
				})
			},
			wantErr: true,
		},
		{
			name: "fast fail",
			jobs: []*importinto.ImportJob{
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"}},
				{JobID: 2, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t2"}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager) {
				// Job 1 fails, Job 2 is running
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed", ResultMessage: "fail"},
					{JobID: 2, Status: "running"},
				}, nil)

				// Should record failure for job 1
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t1"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFailed, cp.Status)
					return nil
				})

				// Should cancel running job 2
				mockSDK.EXPECT().CancelJob(gomock.Any(), int64(2)).Return(nil)

				// Should trigger fast fail and return error immediately
			},
			wantErr: true,
		},
		{
			name: "ignore old jobs",
			jobs: []*importinto.ImportJob{
				{JobID: 2, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t2"}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager) {
				// Job 1 is old failed job, Job 2 is current running job
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed"},
					{JobID: 2, Status: "running"},
				}, nil)

				// Next poll: Job 2 finished
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed"},
					{JobID: 2, Status: "finished"},
				}, nil)

				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t2"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFinished, cp.Status)
					return nil
				})
			},
		},
		{
			name: "GetJobsByGroup error",
			jobs: []*importinto.ImportJob{
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager) {
				// First poll error
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return(nil, errors.New("network error"))
				// Second poll success
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "finished"},
				}, nil)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t1"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFinished, cp.Status)
					return nil
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSDK := sdkmock.NewMockSDK(ctrl)
			mockCpMgr := mockimport.NewMockCheckpointManager(ctrl)
			monitor := importinto.NewJobMonitor(mockSDK, mockCpMgr, time.Millisecond, time.Hour, log.L())

			tt.setup(mockSDK, mockCpMgr)
			err := monitor.WaitForJobs(context.Background(), tt.jobs)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
