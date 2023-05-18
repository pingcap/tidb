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

package runtime

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/pingcap/tidb/timer/api"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	goleak.VerifyTestMain(m)
}

type mockHook struct {
	mock.Mock
	started chan struct{}
	stopped chan struct{}
}

func newMockHook() *mockHook {
	return &mockHook{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func (h *mockHook) Start() {
	h.Called()
	close(h.started)
}

func (h *mockHook) Stop() {
	h.Called()
	close(h.stopped)
}

func (h *mockHook) OnPreSchedEvent(ctx context.Context, event api.TimerShedEvent) (api.PreSchedEventResult, error) {
	args := h.Called(ctx, event)
	return args.Get(0).(api.PreSchedEventResult), args.Error(1)
}

func (h *mockHook) OnSchedEvent(ctx context.Context, event api.TimerShedEvent) error {
	args := h.Called(ctx, event)
	return args.Error(0)
}

func waitDone(obj any, timeout time.Duration) {
	var ch <-chan struct{}
	switch o := obj.(type) {
	case chan struct{}:
		ch = o
	case <-chan struct{}:
		ch = o
	case *sync.WaitGroup:
		newCh := make(chan struct{})
		ch = newCh

		go func() {
			o.Wait()
			close(newCh)
		}()
	default:
		panic(fmt.Sprintf("unsupported type: %T", obj))
	}

	tm := time.NewTimer(timeout)
	defer tm.Stop()
	select {
	case <-ch:
		return
	case <-tm.C:
		panic("wait done timeout")
	}
}

func checkNotDone(ch <-chan struct{}, after time.Duration) {
	if after != 0 {
		time.Sleep(after)
	}
	select {
	case <-ch:
		panic("the channel is expected not done")
	default:
	}
}
