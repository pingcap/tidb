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

package handle_test

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/disttask/framework/handle"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestHandle(t *testing.T) {
	ctx := context.Background()
	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	mgr := storage.NewTaskManager(util.WithInternalSourceType(ctx, "taskManager"), pool)
	storage.SetTaskManager(mgr)

	// no dispatcher registered
	err := handle.SubmitAndRunGlobalTask(ctx, "1", proto.TaskTypeExample, 2, []byte("byte"))
	require.Error(t, err)

	task, err := mgr.GetGlobalTaskByID(1)
	require.NoError(t, err)
	require.Equal(t, int64(1), task.ID)
	require.Equal(t, "1", task.Key)
	require.Equal(t, proto.TaskTypeExample, task.Type)
	// no dispatcher registered
	require.Equal(t, proto.TaskStateFailed, task.State)
	require.Equal(t, proto.StepInit, task.Step)
	require.Equal(t, uint64(2), task.Concurrency)
	require.Equal(t, []byte("byte"), task.Meta)

	require.NoError(t, handle.CancelGlobalTask("1"))
}
