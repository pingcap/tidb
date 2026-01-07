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

	"github.com/pingcap/tidb/lightning/pkg/importinto"
	mockimport "github.com/pingcap/tidb/lightning/pkg/importinto/mock"
	"github.com/pingcap/tidb/lightning/pkg/precheck"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestCheckpointCheckItemCheck(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(cfg *config.Config, mockCPMgr *mockimport.MockCheckpointManager)
		wantRes *precheck.CheckResult
		wantErr bool
	}{
		{
			name: "checkpoint disabled",
			setup: func(cfg *config.Config, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = false
			},
			wantRes: &precheck.CheckResult{Passed: true},
		},
		{
			name: "get checkpoints error",
			setup: func(cfg *config.Config, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, errors.New("get error"))
			},
			wantErr: true,
		},
		{
			name: "no checkpoints",
			setup: func(cfg *config.Config, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return(nil, nil)
			},
			wantRes: &precheck.CheckResult{Passed: true},
		},
		{
			name: "has failed checkpoints",
			setup: func(cfg *config.Config, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return([]*importinto.TableCheckpoint{
					{TableName: "db.t1", Status: importinto.CheckpointStatusFailed},
				}, nil)
			},
			wantRes: &precheck.CheckResult{Passed: false},
		},
		{
			name: "has running/finished checkpoints",
			setup: func(cfg *config.Config, mockCPMgr *mockimport.MockCheckpointManager) {
				cfg.Checkpoint.Enable = true
				mockCPMgr.EXPECT().GetCheckpoints(gomock.Any()).Return([]*importinto.TableCheckpoint{
					{TableName: "db.t1", Status: importinto.CheckpointStatusRunning},
				}, nil)
			},
			wantRes: &precheck.CheckResult{Passed: true, Severity: precheck.Warn},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockCPMgr := mockimport.NewMockCheckpointManager(ctrl)
			cfg := config.NewConfig()
			checker := importinto.NewCheckpointCheckItem(cfg, mockCPMgr)

			require.Equal(t, precheck.CheckCheckpoints, checker.GetCheckItemID())

			tt.setup(cfg, mockCPMgr)
			res, err := checker.Check(context.Background())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantRes.Passed, res.Passed)
				if tt.wantRes.Severity != "" {
					require.Equal(t, tt.wantRes.Severity, res.Severity)
				}
				if !res.Passed || res.Severity == precheck.Warn {
					require.NotEmpty(t, res.Message)
				}
			}
		})
	}
}

type mockChecker struct {
	id  precheck.CheckItemID
	res *precheck.CheckResult
	err error
}

func (m *mockChecker) Check(ctx context.Context) (*precheck.CheckResult, error) {
	return m.res, m.err
}

func (m *mockChecker) GetCheckItemID() precheck.CheckItemID {
	return m.id
}

func TestPrecheckRunner(t *testing.T) {
	tests := []struct {
		name     string
		checkers []precheck.Checker
		wantErr  string
	}{
		{
			name: "all passed",
			checkers: []precheck.Checker{
				&mockChecker{id: "c1", res: &precheck.CheckResult{Passed: true}},
				&mockChecker{id: "c2", res: &precheck.CheckResult{Passed: true, Message: "passed with msg"}},
			},
		},
		{
			name: "checker error",
			checkers: []precheck.Checker{
				&mockChecker{id: "c1", err: errors.New("check error")},
			},
			wantErr: "precheck c1 failed: check error",
		},
		{
			name: "checker not passed",
			checkers: []precheck.Checker{
				&mockChecker{id: "c1", res: &precheck.CheckResult{Passed: false, Message: "failed msg"}},
			},
			wantErr: "precheck c1 failed: failed msg",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := importinto.NewPrecheckRunner()
			for _, c := range tt.checkers {
				runner.Register(c)
			}
			err := runner.Run(context.Background())
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
