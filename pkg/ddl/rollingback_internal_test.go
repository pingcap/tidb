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

package ddl

import (
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/stretchr/testify/require"
)

func TestRollingbackCreateMaterializedViewCannotCancelKeepsState(t *testing.T) {
	job := &model.Job{
		ID:          42,
		Version:     model.JobVersion2,
		Type:        model.ActionCreateMaterializedView,
		State:       model.JobStateCancelling,
		SchemaState: model.StatePublic,
	}
	job.FillArgs(&model.CreateMaterializedViewArgs{TableInfo: &model.TableInfo{ID: 1}})

	_, err := rollingbackCreateMaterializedView(nil, job)
	require.True(t, dbterror.ErrCannotCancelDDLJob.Equal(err))
	require.Equal(t, model.JobStateCancelling, job.State)
}
