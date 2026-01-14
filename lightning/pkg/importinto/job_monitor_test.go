// Copyright 2026 PingCAP, Inc.
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
	const (
		mb = int64(1000 * 1000)
	)
	tests := []struct {
		name    string
		jobs    []*importinto.ImportJob
		setup   func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater)
		wantErr bool
	}{
		{
			name: "no jobs",
			jobs: []*importinto.ImportJob{},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater) {
			},
		},
		{
			name: "one job success",
			jobs: []*importinto.ImportJob{
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1", TotalSize: 100 * mb}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater) {
				// First poll: running
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "running", Phase: "importing", Step: "import", TotalSize: "100MB", ProcessedSize: "50MB"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(100 * mb)
				mockPU.EXPECT().UpdateFinishedSize(25 * mb)

				// Second poll: finished
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "finished", ImportedRows: 100},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(100 * mb)
				mockPU.EXPECT().UpdateFinishedSize(100 * mb)

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
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1", TotalSize: 100 * mb}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater) {
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed", ResultMessage: "some error"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(100 * mb)
				mockPU.EXPECT().UpdateFinishedSize(int64(0))

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
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1", TotalSize: 100 * mb}},
				{JobID: 2, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t2", TotalSize: 100 * mb}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater) {
				// Job 1 fails, Job 2 is running
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed", ResultMessage: "fail"},
					{JobID: 2, Status: "running"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(200 * mb)
				mockPU.EXPECT().UpdateFinishedSize(int64(0))

				// Should record failure for job 1
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t1"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFailed, cp.Status)
					return nil
				})

				// Should trigger fast fail and return error immediately
			},
			wantErr: true,
		},
		{
			name: "ignore old jobs",
			jobs: []*importinto.ImportJob{
				{JobID: 2, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t2", TotalSize: 100 * mb}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater) {
				// Job 1 is old failed job, Job 2 is current running job
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed"},
					{JobID: 2, Status: "running"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(100 * mb)
				mockPU.EXPECT().UpdateFinishedSize(int64(0))

				// Next poll: Job 2 finished
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "failed"},
					{JobID: 2, Status: "finished"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(100 * mb)
				mockPU.EXPECT().UpdateFinishedSize(100 * mb)

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
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1", TotalSize: 100 * mb}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater) {
				// First poll error
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return(nil, errors.New("network error"))
				// Second poll success
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "finished"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(100 * mb)
				mockPU.EXPECT().UpdateFinishedSize(100 * mb)

				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t1"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFinished, cp.Status)
					return nil
				})
			},
		},
		{
			name: "progress never rollbacks when jobs switch state",
			jobs: []*importinto.ImportJob{
				{JobID: 1, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1", TotalSize: 100 * mb}},
				{JobID: 2, GroupKey: "g1", TableMeta: &importsdk.TableMeta{Database: "db", Table: "t2", TotalSize: 400 * mb}},
			},
			setup: func(mockSDK *sdkmock.MockSDK, mockCpMgr *mockimport.MockCheckpointManager, mockPU *mockimport.MockProgressUpdater) {
				// Poll 1: job1 is almost done, job2 hasn't started reporting step progress yet.
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "running", Phase: "importing", Step: "import", TotalSize: "100MB", ProcessedSize: "100MB"},
					{JobID: 2, Status: "pending"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(500 * mb)
				mockPU.EXPECT().UpdateFinishedSize(50 * mb)

				// Poll 2: job1 becomes finished (step sizes are NULL in SHOW IMPORT JOBS for finished jobs),
				// job2 starts running. Group progress should not rollback.
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "finished"},
					{JobID: 2, Status: "running", Phase: "importing", Step: "import", TotalSize: "400MB", ProcessedSize: "2MB"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(500 * mb)
				mockPU.EXPECT().UpdateFinishedSize(101 * mb)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t1"), cp.TableName)
					require.Equal(t, importinto.CheckpointStatusFinished, cp.Status)
					return nil
				})

				// Poll 3: job2 finishes.
				mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "g1").Return([]*importsdk.JobStatus{
					{JobID: 1, Status: "finished"},
					{JobID: 2, Status: "finished"},
				}, nil)
				mockPU.EXPECT().UpdateTotalSize(500 * mb)
				mockPU.EXPECT().UpdateFinishedSize(500 * mb)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cp *importinto.TableCheckpoint) error {
					require.Equal(t, common.UniqueTable("db", "t2"), cp.TableName)
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
			mockPU := mockimport.NewMockProgressUpdater(ctrl)
			monitor := importinto.NewJobMonitor(mockSDK, mockCpMgr, time.Millisecond, time.Hour, log.L(), mockPU)

			tt.setup(mockSDK, mockCpMgr, mockPU)
			err := monitor.WaitForJobs(context.Background(), tt.jobs)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
