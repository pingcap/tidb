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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestFrameworkDynamicBasic(t *testing.T) {
	defer dispatcher.ClearTaskDispatcher()
	defer scheduler.ClearSchedulers()
	var m sync.Map
	RegisterTaskMeta(&m, &testDispatcher{})
	distContext := testkit.NewDistExecutionContext(t, 2)
	DispatchTaskAndCheckSuccess("key1", true, t, &m)
	distContext.Close()
}

func TestFrameworkDynamicHA(t *testing.T) {
	defer dispatcher.ClearTaskDispatcher()
	defer scheduler.ClearSchedulers()
	var m sync.Map
	RegisterTaskMeta(&m, &testDispatcher{})
	distContext := testkit.NewDistExecutionContext(t, 2)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/dispatcher/mockDynamicDispatchErr", "5*return()"))
	DispatchTaskAndCheckSuccess("key1", true, t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/dispatcher/mockDynamicDispatchErr"))
	distContext.Close()
}
