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

package importinto

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSendMeterOnCleanUpInParallelLimitsConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	cleanUpTasks := make([]cleanUpTaskInfo, cleanUpMeteringConcurrency*2)
	for i := range cleanUpTasks {
		cleanUpTasks[i] = cleanUpTaskInfo{
			task: &proto.Task{
				TaskBase: proto.TaskBase{
					ID:    int64(i + 1),
					State: proto.TaskStateSucceed,
				},
			},
			needFileCleanUp: true,
		}
	}

	firstBatchStarted := make(chan struct{})
	release := make(chan struct{})
	overflow := make(chan struct{})
	done := make(chan error, 1)
	var active, maxActive, started, overflowed int32
	sendFn := func(ctx context.Context, task *proto.Task, logger *zap.Logger) error {
		current := atomic.AddInt32(&active, 1)
		defer atomic.AddInt32(&active, -1)
		if current > int32(cleanUpMeteringConcurrency) && atomic.CompareAndSwapInt32(&overflowed, 0, 1) {
			close(overflow)
		}
		for {
			max := atomic.LoadInt32(&maxActive)
			if current <= max || atomic.CompareAndSwapInt32(&maxActive, max, current) {
				break
			}
		}
		if atomic.AddInt32(&started, 1) == int32(cleanUpMeteringConcurrency) {
			close(firstBatchStarted)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	go func() {
		done <- sendMeterOnCleanUpInParallel(ctx, cleanUpTasks, sendFn)
	}()

	select {
	case <-firstBatchStarted:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	select {
	case <-overflow:
		close(release)
		require.FailNow(t, "metering cleanup exceeded concurrency limit")
	case err := <-done:
		require.NoError(t, err)
		require.FailNow(t, "metering cleanup returned before workers were released")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		require.NoError(t, ctx.Err())
	}
	require.NoError(t, err)
	require.Equal(t, int32(len(cleanUpTasks)), atomic.LoadInt32(&started))
	require.Equal(t, int32(cleanUpMeteringConcurrency), atomic.LoadInt32(&maxActive))
}

func TestSendMeterOnCleanUpInParallelRecoversPanic(t *testing.T) {
	restoreLog := log.ReplaceGlobals(zap.NewNop(), &log.ZapProperties{Level: zap.NewAtomicLevelAt(zap.FatalLevel)})
	defer restoreLog()

	cleanUpTasks := []cleanUpTaskInfo{{
		task: &proto.Task{
			TaskBase: proto.TaskBase{
				ID:    1,
				State: proto.TaskStateSucceed,
			},
		},
		needFileCleanUp: true,
	}}
	sendFn := func(context.Context, *proto.Task, *zap.Logger) error {
		panic("metering panic")
	}

	err := sendMeterOnCleanUpInParallel(context.Background(), cleanUpTasks, sendFn)
	require.ErrorContains(t, err, "metering panic")
}
