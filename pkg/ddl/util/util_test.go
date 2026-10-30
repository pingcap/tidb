// Copyright 2024 PingCAP, Inc.
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

package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestFolderNotEmpty(t *testing.T) {
	tmp := t.TempDir()
	require.False(t, FolderNotEmpty(tmp))
	require.False(t, FolderNotEmpty(filepath.Join(tmp, "not-exist")))

	f, err := os.Create(filepath.Join(tmp, "test-file"))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.True(t, FolderNotEmpty(tmp))
}

func TestHasSysDB(t *testing.T) {
	tests := []struct {
		name string
		job  *model.Job
		want bool
	}{
		{
			name: "user database",
			job: &model.Job{
				InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "test", Table: "t"}},
			},
			want: false,
		},
		{
			name: "system database",
			job: &model.Job{
				InvolvingSchemaInfo: []model.InvolvingSchemaInfo{{Database: "mysql", Table: "tidb_ddl_job"}},
			},
			want: true,
		},
		{
			name: "mixed databases",
			job: &model.Job{
				InvolvingSchemaInfo: []model.InvolvingSchemaInfo{
					{Database: "test", Table: "t"},
					{Database: "sys", Table: "schema_table_statistics"},
				},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, HasSysDB(tt.job))
		})
	}
}

func TestPauseRunningJob(t *testing.T) {
	tests := []struct {
		name        string
		job         *model.Job
		byWho       model.AdminCommandOperator
		errEqual    *terror.Error
		wantState   model.JobState
		wantAdminOp model.AdminCommandOperator
	}{
		{
			name: "queueing job becomes pausing",
			job: &model.Job{
				ID:    101,
				State: model.JobStateQueueing,
			},
			byWho:       model.AdminCommandByEndUser,
			wantState:   model.JobStatePausing,
			wantAdminOp: model.AdminCommandByEndUser,
		},
		{
			name: "pausing job returns paused error",
			job: &model.Job{
				ID:            102,
				State:         model.JobStatePausing,
				AdminOperator: model.AdminCommandBySystem,
			},
			byWho:       model.AdminCommandByEndUser,
			errEqual:    dbterror.ErrPausedDDLJob,
			wantState:   model.JobStatePausing,
			wantAdminOp: model.AdminCommandBySystem,
		},
		{
			name: "paused job returns paused error",
			job: &model.Job{
				ID:            103,
				State:         model.JobStatePaused,
				AdminOperator: model.AdminCommandBySystem,
			},
			byWho:       model.AdminCommandByEndUser,
			errEqual:    dbterror.ErrPausedDDLJob,
			wantState:   model.JobStatePaused,
			wantAdminOp: model.AdminCommandBySystem,
		},
		{
			name: "non pausable job returns cannot pause error",
			job: &model.Job{
				ID:            104,
				State:         model.JobStateDone,
				SchemaState:   model.StatePublic,
				AdminOperator: model.AdminCommandBySystem,
			},
			byWho:       model.AdminCommandByEndUser,
			errEqual:    dbterror.ErrCannotPauseDDLJob,
			wantState:   model.JobStateDone,
			wantAdminOp: model.AdminCommandBySystem,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := PauseRunningJob(tt.job, tt.byWho)
			if tt.errEqual == nil {
				require.NoError(t, err)
			} else {
				require.True(t, tt.errEqual.Equal(err), err)
			}
			require.Equal(t, tt.wantState, tt.job.State)
			require.Equal(t, tt.wantAdminOp, tt.job.AdminOperator)
		})
	}
}
