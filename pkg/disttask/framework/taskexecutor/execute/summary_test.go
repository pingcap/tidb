// Copyright 2023 PingCAP, Inc.
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

package execute_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestSummary(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	mockTaskTable := mock.NewMockTaskTable(ctrl)
	summary := execute.NewSummary()
	summary.TaskTable = mockTaskTable
	summary.UpdateRowCount(1, 1)
	mockTaskTable.EXPECT().UpdateSubtaskRowCount(gomock.Any(), int64(1), int64(1))
	summary.PersistRowCount(ctx)
}

func TestSummaryWithStore(t *testing.T) {
	_, sm, ctx := testutil.InitTableTest(t)
	summary := execute.NewSummary()
	summary.TaskTable = sm
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "id", proto.EmptyMeta, proto.SubtaskStateRunning, proto.TaskTypeExample, 1)
	testutil.InsertSubtask(t, sm, 1, proto.StepOne, "id", proto.EmptyMeta, proto.SubtaskStateRunning, proto.TaskTypeExample, 1)
	summary.UpdateRowCount(1, 1)
	summary.UpdateRowCount(2, 3)

	summary.PersistRowCount(ctx)
	cnt, err := sm.GetSubtaskRowCount(ctx, 1, proto.StepOne)
	require.NoError(t, err)
	require.Equal(t, int64(4), cnt)
}
