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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/importinto"
	mockimport "github.com/pingcap/tidb/pkg/lightning/importinto/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCheckpointCheckItem_Check(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
	cfg := config.NewConfig()
	cfg.Checkpoint.Enable = true

	checker := importinto.NewCheckpointCheckItem(cfg, mockCPMgr)

	tests := []struct {
		name    string
		setup   func()
		wantRes *precheck.CheckResult
		wantErr bool
	}{
		{
			name: "checkpoint disabled",
			setup: func() {
				cfg.Checkpoint.Enable = false
			},
			wantRes: &precheck.CheckResult{Passed: true},
		},
		{
			name: "get checkpoints error",
			setup: func() {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, errors.New("get error"))
			},
			wantErr: true,
		},
		{
			name: "no checkpoints",
			setup: func() {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
			},
			wantRes: &precheck.CheckResult{Passed: true},
		},
		{
			name: "has failed checkpoints",
			setup: func() {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return([]*importinto.TableCheckpoint{
					{DBName: "db", TableName: "t1", Status: importinto.CheckpointStatusFailed},
				}, nil)
			},
			wantRes: &precheck.CheckResult{Passed: false},
		},
		{
			name: "has running/finished checkpoints",
			setup: func() {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return([]*importinto.TableCheckpoint{
					{DBName: "db", TableName: "t1", Status: importinto.CheckpointStatusRunning},
				}, nil)
			},
			wantRes: &precheck.CheckResult{Passed: true, Severity: precheck.Warn},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			res, err := checker.Check(context.Background())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantRes.Passed, res.Passed)
				if tt.wantRes.Severity != "" {
					require.Equal(t, tt.wantRes.Severity, res.Severity)
				}
			}
		})
	}
}

func TestClusterVersionCheckItem_Check(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	checker := importinto.NewClusterVersionCheckItem(db)

	tests := []struct {
		name    string
		setup   func()
		wantRes *precheck.CheckResult
		wantErr bool
	}{
		{
			name: "query error",
			setup: func() {
				mock.ExpectQuery("SELECT VERSION()").WillReturnError(errors.New("query error"))
			},
			wantErr: true,
		},
		{
			name: "not tidb",
			setup: func() {
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-MySQL"))
			},
			wantRes: &precheck.CheckResult{Passed: false},
		},
		{
			name: "version too low",
			setup: func() {
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-TiDB-v8.4.0"))
			},
			wantRes: &precheck.CheckResult{Passed: false},
		},
		{
			name: "version ok",
			setup: func() {
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow("5.7.25-TiDB-v8.5.0"))
			},
			wantRes: &precheck.CheckResult{Passed: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			res, err := checker.Check(context.Background())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantRes.Passed, res.Passed)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
