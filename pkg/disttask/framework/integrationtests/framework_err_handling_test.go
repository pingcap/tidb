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
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/stretchr/testify/require"
)

func TestRetryErrOnNextSubtasksBatch(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 2, 16, true)
	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetPlanErrSchedulerExt(c.MockCtrl, c.TestContext), c.TestContext, nil)
	submitTaskAndCheckSuccessForBasic(c.Ctx, t, "key1", c.TestContext)
}

func TestPlanNotRetryableOnNextSubtasksBatchErr(t *testing.T) {
	c := testutil.NewTestDXFContext(t, 2, 16, true)

	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetPlanNotRetryableErrSchedulerExt(c.MockCtrl), c.TestContext, nil)
	task := testutil.SubmitAndWaitTask(c.Ctx, t, "key1", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
	testutil.RegisterTaskMeta(t, c.MockCtrl, testutil.GetStepTwoPlanNotRetryableErrSchedulerExt(c.MockCtrl), c.TestContext, nil)
	task = testutil.SubmitAndWaitTask(c.Ctx, t, "key2", "", 1)
	require.Equal(t, proto.TaskStateReverted, task.State)
}
