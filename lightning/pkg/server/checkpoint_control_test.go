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

package server

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/lightning/config"
	mockimport "github.com/pingcap/tidb/pkg/lightning/importinto/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestImportIntoCheckpointControl(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := mockimport.NewMockCheckpointManager(ctrl)
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendImportInto

	// We can't easily mock NewCheckpointManager inside NewImportIntoCheckpointControl
	// So we construct the struct directly for testing
	control := &ImportIntoCheckpointControl{
		cfg: cfg,
		mgr: mockMgr,
	}

	ctx := context.Background()

	tests := []struct {
		name      string
		operation func() error
		setup     func()
		wantErr   bool
	}{
		{
			name: "Remove",
			operation: func() error {
				return control.Remove(ctx, "db.t1")
			},
			setup: func() {
				mockMgr.EXPECT().Remove(ctx, "db", "t1").Return(nil)
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: false,
		},
		{
			name: "IgnoreError",
			operation: func() error {
				return control.IgnoreError(ctx, "db.t1")
			},
			setup: func() {
				mockMgr.EXPECT().IgnoreError(ctx, "db", "t1").Return(nil)
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: false,
		},
		{
			name: "DestroyError",
			operation: func() error {
				return control.DestroyError(ctx, "db.t1")
			},
			setup: func() {
				mockMgr.EXPECT().DestroyError(ctx, "db", "t1").Return(nil)
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: false,
		},
		{
			name: "Dump",
			operation: func() error {
				tempDir := t.TempDir()
				err := control.Dump(ctx, tempDir)
				if err == nil {
					require.FileExists(t, filepath.Join(tempDir, "tables.csv"))
					require.FileExists(t, filepath.Join(tempDir, "engines.csv"))
					require.FileExists(t, filepath.Join(tempDir, "chunks.csv"))
				}
				return err
			},
			setup: func() {
				mockMgr.EXPECT().DumpTables(ctx, gomock.Any()).Return(nil)
				mockMgr.EXPECT().DumpEngines(ctx, gomock.Any()).Return(nil)
				mockMgr.EXPECT().DumpChunks(ctx, gomock.Any()).Return(nil)
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: false,
		},
		{
			name: "GetLocalStoringTables",
			operation: func() error {
				res, err := control.GetLocalStoringTables(ctx)
				require.Nil(t, res)
				return err
			},
			setup:   func() {},
			wantErr: false,
		},
		{
			name: "ParseTable Error",
			operation: func() error {
				return control.Remove(ctx, "invalid")
			},
			setup: func() {
				mockMgr.EXPECT().Close().Return(nil)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setup()
			err := tt.operation()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
