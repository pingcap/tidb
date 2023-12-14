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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/disttask/framework/testutil"
	"github.com/stretchr/testify/require"
)

func TestFrameworkDynamicBasic(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockDynamicDispatchExt(ctrl), testContext, nil)
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key1", testContext, nil)
	distContext.Close()
}

func TestFrameworkDynamicHA(t *testing.T) {
	ctx, ctrl, testContext, distContext := testutil.InitTestContext(t, 3)
	defer ctrl.Finish()

	testutil.RegisterTaskMeta(t, ctrl, testutil.GetMockDynamicDispatchExt(ctrl), testContext, nil)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDynamicDispatchErr", "5*return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key1", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDynamicDispatchErr"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDynamicDispatchErr1", "5*return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key2", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDynamicDispatchErr1"))

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDynamicDispatchErr2", "5*return()"))
	testutil.DispatchTaskAndCheckSuccess(ctx, t, "key3", testContext, nil)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/disttask/framework/scheduler/mockDynamicDispatchErr2"))
	distContext.Close()
}
