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
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/lightning/pkg/importinto"
	mockimport "github.com/pingcap/tidb/lightning/pkg/importinto/mock"
	"github.com/pingcap/tidb/pkg/importsdk"
	"github.com/pingcap/tidb/pkg/importsdk/mock"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestImporterRun(t *testing.T) {
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
		wantErr error
	}{
		{
			name: "success",
			setup: func() {
				// NewImporter initialization
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				// initGroupKey
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
				mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
				// Prechecks
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
				// Orchestrator
				mockOrchestrator.EXPECT().SubmitAndWait(gomock.Any(), tables).Return(nil)
				// Cleanup checkpoints
				mockCPMgr.EXPECT().Remove(gomock.Any(), "all").Return(nil)
			},
		},
		{
			name: "create schemas error",
			setup: func() {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(errors.New("create schemas failed"))
			},
			wantErr: errors.New("create schemas failed"),
		},
		{
			name: "get table metas error",
			setup: func() {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
				mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(nil, errors.New("get metas failed"))
			},
			wantErr: errors.New("get metas failed"),
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
			wantErr: errors.New("precheck failed"),
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
			wantErr: errors.New("orchestrator failed"),
		},
		{
			name: "cancel",
			setup: func() {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)

				mockSDK.EXPECT().CreateSchemasAndTables(gomock.Any()).Return(nil)
				mockSDK.EXPECT().GetTableMetas(gomock.Any()).Return(tables, nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil) // Precheck

				// Simulate context cancellation during SubmitAndWait
				mockOrchestrator.EXPECT().SubmitAndWait(gomock.Any(), tables).Return(context.Canceled)
				// Expect Cancel to be called
				mockOrchestrator.EXPECT().Cancel(gomock.Any()).Return(nil)
			},
			wantErr: context.Canceled,
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
			if tt.wantErr != nil {
				require.ErrorContains(t, err, tt.wantErr.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestImporterNewImporter(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(cfg *config.Config, mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager)
		wantErr string
	}{
		{
			name: "restored group key",
			setup: func(cfg *config.Config, mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return([]*importinto.TableCheckpoint{
					{GroupKey: "restored-group-key"},
				}, nil)
			},
		},
		{
			name: "build orchestrator",
			setup: func(cfg *config.Config, mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
			},
		},
		{
			name: "invalid checkpoint driver",
			setup: func(cfg *config.Config, mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = true
				cfg.Checkpoint.Driver = "invalid"
			},
			wantErr: "unknown checkpoint driver",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSDK := mock.NewMockSDK(ctrl)
			mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)

			cfg := config.NewConfig()
			tt.setup(cfg, mockSDK, mockCPMgr)

			opts := []importinto.ImporterOption{
				importinto.WithBackendSDK(mockSDK),
			}
			if tt.name != "invalid checkpoint driver" {
				opts = append(opts, importinto.WithCheckpointManager(mockCPMgr))
			}

			importer, err := importinto.NewImporter(context.Background(), cfg, nil, opts...)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
				require.NotNil(t, importer)
			}
		})
	}
}

func TestImporterClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSDK := mock.NewMockSDK(ctrl)
	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)

	tests := []struct {
		name    string
		setup   func(mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager) *sql.DB
		wantErr string
	}{
		{
			name: "normal close",
			setup: func(mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager) *sql.DB {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
				mockSDK.EXPECT().Close().Return(nil)
				mockCPMgr.EXPECT().Close().Return(nil)
				return nil
			},
		},
		{
			name: "close with db",
			setup: func(mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager) *sql.DB {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				mock.ExpectClose()
				mockSDK.EXPECT().Close().Return(nil)
				mockCPMgr.EXPECT().Close().Return(nil)
				return db
			},
		},
		{
			name: "close with error",
			setup: func(mockSDK *mock.MockSDK, mockCPMgr *mockimport.MockCheckpointManager) *sql.DB {
				mockCPMgr.EXPECT().Initialize(gomock.Any()).Return(nil)
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
				mockSDK.EXPECT().Close().Return(errors.New("sdk close error"))
				mockCPMgr.EXPECT().Close().Return(errors.New("cp close error"))
				return nil
			},
			wantErr: "sdk close error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := tt.setup(mockSDK, mockCPMgr)
			importer, err := importinto.NewImporter(context.Background(), config.NewConfig(), db,
				importinto.WithBackendSDK(mockSDK),
				importinto.WithCheckpointManager(mockCPMgr),
			)
			require.NoError(t, err)
			importer.Close()
		})
	}
}
