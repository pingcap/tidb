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
)

func TestPlanErr(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetPlanErrDispatcherExt(ctrl, test_context), test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)
	test_context.CallTime = 0
	distContext.Close()
}

func TestRevertPlanErr(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()
	testutil.RegisterTaskMeta(t, ctrl, testutil.GetPlanErrDispatcherExt(ctrl, test_context), test_context)
	testutil.DispatchTaskAndCheckSuccess(t, ctx, "key1", test_context)
	test_context.CallTime = 0
	distContext.Close()
}

func TestPlanNotRetryableErr(t *testing.T) {
	ctx, ctrl, test_context, distContext := testutil.InitTestContext(t, 2)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetPlanNotRetryableErrDispatcherExt(ctrl), test_context)
	testutil.DispatchTaskAndCheckState(t, ctx, "key1", test_context, proto.TaskStateFailed)
	distContext.Close()
}
