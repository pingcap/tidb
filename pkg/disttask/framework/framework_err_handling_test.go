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

package framework_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/stretchr/testify/require"
)

func TestRetryErrOnNextSubtasksBatch(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetPlanErrSchedulerExt(ctrl, testContext), testContext, nil)
	submitTaskAndCheckSuccessForBasic(ctx, t, "key1", testContext)
	distContext.Close()
}

func TestPlanNotRetryableOnNextSubtasksBatchErr(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetPlanNotRetryableErrSchedulerExt(ctrl), testContext, nil)
	task := testutil.SubmitAndWaitTask(ctx, t, "key1")
	require.Equal(t, proto.TaskStateFailed, task.State)
	distContext.Close()
}
