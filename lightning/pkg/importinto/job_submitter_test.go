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

	"github.com/pingcap/tidb/lightning/pkg/importinto"
	"github.com/pingcap/tidb/pkg/importsdk"
	sdkmock "github.com/pingcap/tidb/pkg/importsdk/mock"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestJobSubmitterSubmitTable(t *testing.T) {
	tests := []struct {
		name      string
		tableMeta *importsdk.TableMeta
		setup     func(mockSDK *sdkmock.MockSDK)
		wantErr   bool
	}{
		{
			name: "successful submission",
			tableMeta: &importsdk.TableMeta{
				Database: "db",
				Table:    "t1",
			},
			setup: func(mockSDK *sdkmock.MockSDK) {
				mockSDK.EXPECT().GenerateImportSQL(gomock.Any(), gomock.Any()).Return("IMPORT INTO ...", nil)
				mockSDK.EXPECT().SubmitJob(gomock.Any(), "IMPORT INTO ...").Return(int64(123), nil)
			},
		},
		{
			name: "generate sql error",
			tableMeta: &importsdk.TableMeta{
				Database: "db",
				Table:    "t1",
			},
			setup: func(mockSDK *sdkmock.MockSDK) {
				mockSDK.EXPECT().GenerateImportSQL(gomock.Any(), gomock.Any()).Return("", errors.New("gen error"))
			},
			wantErr: true,
		},
		{
			name: "submit job error",
			tableMeta: &importsdk.TableMeta{
				Database: "db",
				Table:    "t1",
			},
			setup: func(mockSDK *sdkmock.MockSDK) {
				mockSDK.EXPECT().GenerateImportSQL(gomock.Any(), gomock.Any()).Return("IMPORT INTO ...", nil)
				mockSDK.EXPECT().SubmitJob(gomock.Any(), "IMPORT INTO ...").Return(int64(0), errors.New("submit error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockSDK := sdkmock.NewMockSDK(ctrl)
			groupKey := "g1"
			submitter := importinto.NewJobSubmitter(mockSDK, config.NewConfig(), groupKey, log.L())

			tt.setup(mockSDK)
			job, err := submitter.SubmitTable(context.Background(), tt.tableMeta)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, job)
			} else {
				require.NoError(t, err)
				require.NotNil(t, job)
				require.Equal(t, int64(123), job.JobID)
				require.Equal(t, groupKey, job.GroupKey)
			}
		})
	}
}

func TestJobSubmitterGetGroupKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockSDK := sdkmock.NewMockSDK(ctrl)
	logger := log.L()
	cfg := config.NewConfig()
	groupKey := "g1"

	submitter := importinto.NewJobSubmitter(mockSDK, cfg, groupKey, logger)
	require.Equal(t, groupKey, submitter.GetGroupKey())
}
