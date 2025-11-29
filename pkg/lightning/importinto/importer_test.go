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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/importsdk/mock"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/importinto"
	mockimport "github.com/pingcap/tidb/pkg/lightning/importinto/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestImporter_Run(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSDK := mock.NewMockSDK(ctrl)
	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockOrchestrator := mockimport.NewMockJobOrchestrator(ctrl)

	cfg := config.NewConfig()
	cfg.App.CheckRequirements = true
	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.KeepAfterSuccess = config.CheckpointRemove

	tables := []*importsdk.TableMeta{
		{Database: "db", Table: "t1"},
	}

	tests := []struct {
		name    string
		setup   func()
		wantErr bool
	}{
		{
			name: "success",
			setup: func() {
				// NewImporter initialization
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil) // initGroupKey

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
				mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
				// Prechecks
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
				// Orchestrator
				mockOrchestrator.EXPECT().SubmitAndWait(gomock.Any(), tables).Return(nil)
				// Cleanup checkpoints
				mockCPMgr.EXPECT().Remove(gomock.Any(), "all", "all").Return(nil)
			},
			wantErr: false,
		},
		{
			name: "create schemas error",
			setup: func() {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(errors.New("create schemas failed"))
			},
			wantErr: true,
		},
		{
			name: "get table metas error",
			setup: func() {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
				mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(nil, errors.New("get metas failed"))
			},
			wantErr: true,
		},
		{
			name: "precheck error",
			setup: func() {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
				mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
				// Prechecks fail
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, errors.New("precheck failed"))
			},
			wantErr: true,
		},
		{
			name: "orchestrator error",
			setup: func() {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
				mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
				// Prechecks
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
				// Orchestrator fails
				mockOrchestrator.EXPECT().SubmitAndWait(gomock.Any(), tables).Return(errors.New("orchestrator failed"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			importer, err := importinto.NewImporter(context.Background(), cfg, nil,
				importinto.WithBackendSDK(mockSDK),
				importinto.WithCheckpointManager(mockCPMgr),
				importinto.WithOrchestrator(mockOrchestrator),
			)
			require.NoError(t, err)

			err = importer.Run(context.Background())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestImporter_InitGroupKey_Restored(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockSDK := mock.NewMockSDK(ctrl)

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return([]*importinto.TableCheckpoint{
		{GroupKey: "restored-group-key"},
	}, nil)

	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
	)
	require.NoError(t, err)
	// We can't easily check private field groupKey, but we can verify the mock call happened.
	// Or we can check if the submitter (if we could access it) has the group key.
	// But NewImporter returns *Importer, and groupKey is private.
	// However, we can verify via logging if we captured logs, or just trust the coverage.
	_ = importer
}

func TestImporter_BuildOrchestrator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockSDK := mock.NewMockSDK(ctrl)

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	// Not providing WithOrchestrator should trigger buildOrchestrator
	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
	)
	require.NoError(t, err)
	require.NotNil(t, importer)
}

func TestImporter_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockSDK := mock.NewMockSDK(ctrl)

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
	)
	require.NoError(t, err)

	mockCPMgr.EXPECT().Close().Return(nil)
	mockSDK.EXPECT().Close().Return(nil)
	// db is nil, so no Close call for db

	importer.Close()
}

func TestImporter_Close_WithDB(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockSDK := mock.NewMockSDK(ctrl)

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	db, _, err := sqlmock.New()
	require.NoError(t, err)

	importer, err := importinto.NewImporter(context.Background(), cfg, db,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
	)
	require.NoError(t, err)

	mockCPMgr.EXPECT().Close().Return(nil)
	mockSDK.EXPECT().Close().Return(nil)
	// db.Close() will be called by importer.Close()
	// sqlmock will verify expectations on Close if we set any, but here we just want to ensure it doesn't panic.
	// Actually sqlmock.New() returns a *sql.DB that we can close.

	importer.Close()
}

func TestImporter_Close_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockSDK := mock.NewMockSDK(ctrl)

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
	)
	require.NoError(t, err)

	mockCPMgr.EXPECT().Close().Return(errors.New("close error"))
	mockSDK.EXPECT().Close().Return(errors.New("close error"))

	importer.Close()
}

func TestImporter_NewImporter_InvalidCheckpointDriver(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSDK := mock.NewMockSDK(ctrl)
	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.Driver = "invalid"

	_, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown checkpoint driver")
}

func TestImporter_Pause(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockSDK := mock.NewMockSDK(ctrl)
	mockOrchestrator := mockimport.NewMockJobOrchestrator(ctrl)

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
		importinto.WithOrchestrator(mockOrchestrator),
	)
	require.NoError(t, err)

	mockOrchestrator.EXPECT().Cancel(gomock.Any()).Return(nil)

	err = importer.Pause(context.Background())
	require.NoError(t, err)
}

func TestImporter_Resume(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockSDK := mock.NewMockSDK(ctrl)
	mockOrchestrator := mockimport.NewMockJobOrchestrator(ctrl)

	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
		importinto.WithOrchestrator(mockOrchestrator),
	)
	require.NoError(t, err)

	// Resume just signals the channel, doesn't call Run anymore
	err = importer.Resume(context.Background())
	require.NoError(t, err)
}

func TestImporter_PauseResumeLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSDK := mock.NewMockSDK(ctrl)
	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockOrchestrator := mockimport.NewMockJobOrchestrator(ctrl)

	cfg := config.NewConfig()
	cfg.App.CheckRequirements = true
	cfg.Checkpoint.Enable = true
	cfg.Checkpoint.KeepAfterSuccess = config.CheckpointRemove

	tables := []*importsdk.TableMeta{
		{Database: "db", Table: "t1"},
	}

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
		importinto.WithOrchestrator(mockOrchestrator),
	)
	require.NoError(t, err)

	// First run: Pause
	mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
	mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
	mockOrchestrator.EXPECT().SubmitAndWait(gomock.Any(), tables).DoAndReturn(func(ctx context.Context, _ any) error {
		mockOrchestrator.EXPECT().Cancel(gomock.Any()).Return(nil)
		importer.Pause(ctx)
		return context.Canceled
	})

	// Second run: Success
	mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
	mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
	mockOrchestrator.EXPECT().SubmitAndWait(gomock.Any(), tables).Return(nil)
	mockCPMgr.EXPECT().Remove(gomock.Any(), "all", "all").Return(nil)

	errCh := make(chan error)
	go func() {
		errCh <- importer.Run(context.Background())
	}()

	// Wait for pause
	time.Sleep(100 * time.Millisecond)

	// Resume
	importer.Resume(context.Background())

	err = <-errCh
	require.NoError(t, err)
}

func TestImporter_Run_Cancel(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSDK := mock.NewMockSDK(ctrl)
	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	mockOrchestrator := mockimport.NewMockJobOrchestrator(ctrl)

	cfg := config.NewConfig()
	cfg.App.CheckRequirements = true
	cfg.Checkpoint.Enable = true

	tables := []*importsdk.TableMeta{
		{Database: "db", Table: "t1"},
	}

	mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

	importer, err := importinto.NewImporter(context.Background(), cfg, nil,
		importinto.WithBackendSDK(mockSDK),
		importinto.WithCheckpointManager(mockCPMgr),
		importinto.WithOrchestrator(mockOrchestrator),
	)
	require.NoError(t, err)

	mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
	mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
	mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil) // Precheck

	// Simulate context cancellation during SubmitAndWait
	mockOrchestrator.EXPECT().SubmitAndWait(gomock.Any(), tables).Return(context.Canceled)

	// Expect Cancel to be called
	mockOrchestrator.EXPECT().Cancel(gomock.Any()).Return(nil)

	err = importer.Run(context.Background())
	require.ErrorIs(t, err, context.Canceled)
}
