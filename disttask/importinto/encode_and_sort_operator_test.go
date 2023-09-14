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

package importinto

import (
	"context"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/importinto/mock"
	"github.com/pingcap/tidb/disttask/operator"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
)

func TestEncodeAndSortOperator(t *testing.T) {
	bak := os.Stdout
	logFileName := path.Join(t.TempDir(), "test.log")
	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	require.NoError(t, err)
	os.Stdout = file
	t.Cleanup(func() {
		require.NoError(t, os.Stdout.Close())
		os.Stdout = bak
	})
	logger := zap.NewExample()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	executor := mock.NewMockMiniTaskExecutor(ctrl)
	backup := newImportMinimalTaskExecutor
	t.Cleanup(func() {
		newImportMinimalTaskExecutor = backup
	})
	newImportMinimalTaskExecutor = func(t *importStepMinimalTask) MiniTaskExecutor {
		return executor
	}

	source := operator.NewSimpleDataChannel(make(chan *importStepMinimalTask))
	op := newEncodeAndSortOperator(context.Background(), 3, logger)
	op.SetSource(source)
	require.NoError(t, op.Open())
	require.Greater(t, len(op.String()), 0)

	// cancel on error
	mockErr := errors.New("mock err")
	executor.EXPECT().Run(gomock.Any()).Return(mockErr)
	source.Channel() <- &importStepMinimalTask{}
	require.Eventually(t, func() bool {
		return op.hasError()
	}, 3*time.Second, 300*time.Millisecond)
	require.Equal(t, mockErr, op.firstErr.Load())
	// should not block
	<-op.ctx.Done()
	require.ErrorIs(t, op.Close(), mockErr)

	// cancel on error and log other errors
	mockErr2 := errors.New("mock err 2")
	source = operator.NewSimpleDataChannel(make(chan *importStepMinimalTask))
	op = newEncodeAndSortOperator(context.Background(), 2, logger)
	op.SetSource(source)
	executor1 := mock.NewMockMiniTaskExecutor(ctrl)
	executor2 := mock.NewMockMiniTaskExecutor(ctrl)
	var id atomic.Int32
	newImportMinimalTaskExecutor = func(t *importStepMinimalTask) MiniTaskExecutor {
		if id.Add(1) == 1 {
			return executor1
		}
		return executor2
	}
	var wg sync.WaitGroup
	wg.Add(2)
	// wait until 2 executor start running, else workerpool will be cancelled.
	executor1.EXPECT().Run(gomock.Any()).DoAndReturn(func(context.Context) error {
		wg.Done()
		wg.Wait()
		return mockErr2
	})
	executor2.EXPECT().Run(gomock.Any()).DoAndReturn(func(context.Context) error {
		wg.Done()
		wg.Wait()
		// wait error in executor1 has been processed
		require.Eventually(t, func() bool {
			return op.hasError()
		}, 3*time.Second, 300*time.Millisecond)
		return errors.New("mock error should be logged")
	})
	require.NoError(t, op.Open())
	// send 2 tasks
	source.Channel() <- &importStepMinimalTask{}
	source.Channel() <- &importStepMinimalTask{}
	// should not block
	<-op.ctx.Done()
	require.ErrorIs(t, op.Close(), mockErr2)
	require.NoError(t, os.Stdout.Sync())
	content, err := os.ReadFile(logFileName)
	require.NoError(t, err)
	require.Contains(t, string(content), "mock error should be logged")
}
