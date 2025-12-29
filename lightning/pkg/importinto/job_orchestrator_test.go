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
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestJobOrchestratorSubmitAndWait(t *testing.T) {
	tests := []struct {
		name    string
		tables  []*importsdk.TableMeta
		setup   func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK)
		wantErr bool
	}{
		{
			name:   "no tables",
			tables: []*importsdk.TableMeta{},
			setup: func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK) {
			},
		},
		{
			name: "one table, successful submission",
			tables: []*importsdk.TableMeta{
				{Database: "db", Table: "t1", DataFiles: []importsdk.DataFileMeta{{Path: "f1"}}, TotalSize: 100},
			},
			setup: func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK) {
				mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockSubmitter.EXPECT().SubmitTable(gomock.Any(), gomock.Any()).Return(&importinto.ImportJob{
					JobID:     1,
					TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"},
					GroupKey:  "group1",
				}, nil)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
				mockMonitor.EXPECT().WaitForJobs(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			name: "one table, already finished",
			tables: []*importsdk.TableMeta{
				{Database: "db", Table: "t1", DataFiles: []importsdk.DataFileMeta{{Path: "f1"}}, TotalSize: 100},
			},
			setup: func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK) {
				mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&importinto.TableCheckpoint{
					Status: importinto.CheckpointStatusFinished,
				}, nil)
			},
		},
		{
			name: "one table, resume running",
			tables: []*importsdk.TableMeta{
				{Database: "db", Table: "t1", DataFiles: []importsdk.DataFileMeta{{Path: "f1"}}, TotalSize: 100},
			},
			setup: func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK) {
				mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&importinto.TableCheckpoint{
					JobID:  1,
					Status: importinto.CheckpointStatusRunning,
				}, nil)
				mockSubmitter.EXPECT().GetGroupKey().Return("group1")
				mockMonitor.EXPECT().WaitForJobs(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			name: "one table, resubmit failed",
			tables: []*importsdk.TableMeta{
				{Database: "db", Table: "t1", DataFiles: []importsdk.DataFileMeta{{Path: "f1"}}, TotalSize: 100},
			},
			setup: func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK) {
				mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(&importinto.TableCheckpoint{
					JobID:  1,
					Status: importinto.CheckpointStatusFailed,
				}, nil)
				mockSubmitter.EXPECT().SubmitTable(gomock.Any(), gomock.Any()).Return(&importinto.ImportJob{
					JobID:     2,
					TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"},
					GroupKey:  "group1",
				}, nil)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
				mockMonitor.EXPECT().WaitForJobs(gomock.Any(), gomock.Any()).Return(nil)
			},
		},
		{
			name: "submission error",
			tables: []*importsdk.TableMeta{
				{Database: "db", Table: "t1", DataFiles: []importsdk.DataFileMeta{{Path: "f1"}}, TotalSize: 100},
			},
			setup: func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK) {
				mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockSubmitter.EXPECT().SubmitTable(gomock.Any(), gomock.Any()).Return(nil, errors.New("submit error"))
			},
			wantErr: true,
		},
		{
			name: "monitor error",
			tables: []*importsdk.TableMeta{
				{Database: "db", Table: "t1", DataFiles: []importsdk.DataFileMeta{{Path: "f1"}}, TotalSize: 100},
			},
			setup: func(mockSubmitter *mockimport.MockJobSubmitter, mockCpMgr *mockimport.MockCheckpointManager, mockMonitor *mockimport.MockJobMonitor, mockSDK *sdkmock.MockSDK) {
				mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, nil)
				mockSubmitter.EXPECT().SubmitTable(gomock.Any(), gomock.Any()).Return(&importinto.ImportJob{
					JobID:     1,
					TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"},
					GroupKey:  "group1",
				}, nil)
				mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)
				mockMonitor.EXPECT().WaitForJobs(gomock.Any(), gomock.Any()).Return(errors.New("monitor error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSubmitter := mockimport.NewMockJobSubmitter(ctrl)
			mockCpMgr := mockimport.NewMockCheckpointManager(ctrl)
			mockMonitor := mockimport.NewMockJobMonitor(ctrl)
			mockSDK := sdkmock.NewMockSDK(ctrl)

			orchestrator := importinto.NewJobOrchestrator(importinto.OrchestratorConfig{
				Submitter:         mockSubmitter,
				CheckpointMgr:     mockCpMgr,
				SDK:               mockSDK,
				Monitor:           mockMonitor,
				SubmitConcurrency: 2,
				PollInterval:      time.Millisecond,
				Logger:            log.L(),
			})

			tt.setup(mockSubmitter, mockCpMgr, mockMonitor, mockSDK)
			err := orchestrator.SubmitAndWait(context.Background(), tt.tables)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJobOrchestratorCancel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSubmitter := mockimport.NewMockJobSubmitter(ctrl)
	mockCpMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockMonitor := mockimport.NewMockJobMonitor(ctrl)
	mockSDK := sdkmock.NewMockSDK(ctrl)

	logger := log.L()

	orchestrator := importinto.NewJobOrchestrator(importinto.OrchestratorConfig{
		Submitter:         mockSubmitter,
		CheckpointMgr:     mockCpMgr,
		SDK:               mockSDK,
		Monitor:           mockMonitor,
		SubmitConcurrency: 2,
		PollInterval:      time.Millisecond,
		Logger:            logger,
	})

	// Setup active jobs
	tables := []*importsdk.TableMeta{
		{Database: "db", Table: "t1", DataFiles: []importsdk.DataFileMeta{{Path: "f1"}}, TotalSize: 100},
		{Database: "db", Table: "t2", DataFiles: []importsdk.DataFileMeta{{Path: "f2"}}, TotalSize: 100},
	}

	mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, nil)
	mockSubmitter.EXPECT().SubmitTable(gomock.Any(), gomock.Any()).Return(&importinto.ImportJob{
		JobID:     1,
		TableMeta: &importsdk.TableMeta{Database: "db", Table: "t1"},
		GroupKey:  "group1",
	}, nil)
	mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

	mockCpMgr.EXPECT().Get(gomock.Any(), gomock.Any()).Return(nil, nil)
	mockSubmitter.EXPECT().SubmitTable(gomock.Any(), gomock.Any()).Return(&importinto.ImportJob{
		JobID:     2,
		TableMeta: &importsdk.TableMeta{Database: "db", Table: "t2"},
		GroupKey:  "group1",
	}, nil)
	mockCpMgr.EXPECT().Update(gomock.Any(), gomock.Any()).Return(nil)

	mockMonitor.EXPECT().WaitForJobs(gomock.Any(), gomock.Any()).Return(nil)

	err := orchestrator.SubmitAndWait(context.Background(), tables)
	require.NoError(t, err)

	// Now call Cancel
	// Expect GetJobsByGroup
	mockSDK.EXPECT().GetJobsByGroup(gomock.Any(), "group1").Return([]*importsdk.JobStatus{
		{JobID: 1, Status: "finished"},
		{JobID: 2, Status: "running"},
	}, nil)

	// Expect CancelJob only for job 2
	mockSDK.EXPECT().CancelJob(gomock.Any(), int64(2)).Return(nil)

	err = orchestrator.Cancel(context.Background())
	require.NoError(t, err)
}
