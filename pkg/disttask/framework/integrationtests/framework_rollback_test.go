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

package integrationtests

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFrameworkRollback(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 2, 16, true)
	testutil.RegisterRollbackTaskMeta(t, c.MockCtrl, testutil.GetMockRollbackSchedulerExt(c.MockCtrl), c.TestContext)
	cnt := 0
	scheduler.OnTaskRefreshed = func(ctx context.Context, taskMgr scheduler.TaskManager, task *proto.Task) {
		if cnt < 2 && task.State == proto.TaskStateRunning {
			err := taskMgr.CancelTask(ctx, task.ID)
			if err != nil {
				logutil.BgLogger().Error("cancel task failed", zap.Error(err))
			}
			cnt++
		}
	}
	testkit.EnableFailPoint(t, "github.com/pingcap/tidb/pkg/disttask/framework/scheduler/onTaskRefreshed", "return()")
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}
