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
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/backoff"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func TestHandle(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu", "return(8)"))
	t.Cleanup(func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/cpu/mockNumCpu"))
	})

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, "handle_test")

	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)

	// no scheduler registered
	err := handle.SubmitAndWaitTask(ctx, "1", proto.TaskTypeExample, 2, []byte("byte"))
	require.Error(t, err)

	task, err := mgr.GetTaskByID(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), task.ID)
	require.Equal(t, "1", task.Key)
	require.Equal(t, proto.TaskTypeExample, task.Type)
	// no scheduler registered
	require.Equal(t, proto.TaskStateFailed, task.State)
	require.Equal(t, proto.StepInit, task.Step)
	require.Equal(t, 2, task.Concurrency)
	require.Equal(t, []byte("byte"), task.Meta)

	require.NoError(t, handle.CancelTask(ctx, "1"))

	task, err = handle.SubmitTask(ctx, "2", proto.TaskTypeExample, 2, []byte("byte"))
	require.NoError(t, err)
	require.Equal(t, int64(2), task.ID)
	require.Equal(t, "2", task.Key)
	require.NoError(t, handle.PauseTask(ctx, "2"))
	require.NoError(t, handle.ResumeTask(ctx, "2"))
}

func TestRunWithRetry(t *testing.T) {
	ctx := context.Background()

	// retry count exceed
	backoffer := backoff.NewExponential(100*time.Millisecond, 1, time.Second)
	err := handle.RunWithRetry(ctx, 3, backoffer, log.L(),
		func(ctx context.Context) (bool, error) {
			return true, errors.New("mock error")
		},
	)
	require.ErrorContains(t, err, "mock error")

	// non-retryable error
	var end atomic.Bool
	go func() {
		defer end.Store(true)
		backoffer = backoff.NewExponential(100*time.Millisecond, 1, time.Second)
		err = handle.RunWithRetry(ctx, math.MaxInt, backoffer, log.L(),
			func(ctx context.Context) (bool, error) {
				return false, errors.New("mock error")
			},
		)
		require.Error(t, err)
	}()
	require.Eventually(t, func() bool {
		return end.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// fail with retryable error once, then success
	end.Store(false)
	go func() {
		defer end.Store(true)
		backoffer = backoff.NewExponential(100*time.Millisecond, 1, time.Second)
		var i int
		err = handle.RunWithRetry(ctx, math.MaxInt, backoffer, log.L(),
			func(ctx context.Context) (bool, error) {
				if i == 0 {
					i++
					return true, errors.New("mock error")
				}
				return false, nil
			},
		)
		require.NoError(t, err)
	}()
	require.Eventually(t, func() bool {
		return end.Load()
	}, 5*time.Second, 100*time.Millisecond)

	// context done
	subctx, cancel := context.WithCancel(ctx)
	cancel()
	backoffer = backoff.NewExponential(100*time.Millisecond, 1, time.Second)
	err = handle.RunWithRetry(subctx, math.MaxInt, backoffer, log.L(),
		func(ctx context.Context) (bool, error) {
			return true, errors.New("mock error")
		},
	)
	require.ErrorIs(t, err, context.Canceled)
}
