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

package spool

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

type DummyIntWorker struct {
	receive chan int
}

func (d *DummyIntWorker) HandleTask(_ context.Context, task int) {
	d.receive <- task
}

func (d *DummyIntWorker) Close() {}

func NewDummyIntWorker(receive chan int) *DummyIntWorker {
	return &DummyIntWorker{
		receive: receive,
	}
}

func TestDynamicConcurrencyOneshot(t *testing.T) {
	p, err := NewPool("TestDynamicConcurrencyOneshot", int32(1), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TestDynamicConcurrencyOneshot pool failed: %v", err)
	defer p.ReleaseAndWait()
	receive := make(chan int)
	handle, err := RunWithDynamicalConcurrency[int, *DummyIntWorker](context.Background(), p, func() *DummyIntWorker {
		return NewDummyIntWorker(receive)
	}, 0, 1)
	require.NoError(t, err)
	handle.Send(1)
	require.Equal(t, 1, <-receive)
	handle.Close(false)
	p.wg.Wait()
	require.Zero(t, p.Running())
}

func TestDynamicConcurrencyMultiTask(t *testing.T) {
	p, err := NewPool("TestDynamicConcurrencyMultiTask", int32(1), util.UNKNOWN, WithBlocking(false))
	require.NoErrorf(t, err, "create TestDynamicConcurrencyMultiTask pool failed: %v", err)
	defer p.ReleaseAndWait()
	receive := make(chan int, 200)
	handle, err := RunWithDynamicalConcurrency[int, *DummyIntWorker](context.Background(), p, func() *DummyIntWorker {
		return NewDummyIntWorker(receive)
	}, 0, 1)
	require.NoError(t, err)

	results := make(map[int]struct{}, 100)
	for i := 0; i < 100; i++ {
		results[i] = struct{}{}
		handle.Send(i)
	}
	handle.Close(false)
	for i := 0; i < 100; i++ {
		r, ok := <-receive
		require.True(t, ok)
		_, ok = results[r]
		require.True(t, ok)
		delete(results, r)
	}
	require.Len(t, receive, 0)
	close(receive)
	p.wg.Wait()
	require.Zero(t, p.Running())
}
