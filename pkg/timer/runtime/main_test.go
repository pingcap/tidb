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

	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/timer/api"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/mock"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	testsetup.SetupForCommonTest()
	goleak.VerifyTestMain(m, opts...)
}

type mockHook struct {
	mock.Mock
	started chan struct{}
	stopped chan struct{}
}

type newHookFn struct {
	mock.Mock
}

func (n *newHookFn) OnFuncCall() *mock.Call {
	return n.On("Func")
}

func (n *newHookFn) Func() api.Hook {
	args := n.Called()
	if v := args.Get(0); v != nil {
		return v.(api.Hook)
	}
	return nil
}

func onlyOnceNewHook(hook api.Hook) func() api.Hook {
	n := newHookFn{}
	n.OnFuncCall().Return(hook).Once()
	return n.Func
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

type mockStoreCore struct {
	mock.Mock
}

func newMockStore() (*mockStoreCore, *api.TimerStore) {
	core := &mockStoreCore{}
	return core, &api.TimerStore{TimerStoreCore: core}
}

func (s *mockStoreCore) mock() *mock.Mock {
	return &s.Mock
}

func (s *mockStoreCore) Create(ctx context.Context, record *api.TimerRecord) (string, error) {
	args := s.Called(ctx, record)
	return args.String(0), args.Error(1)
}

func (s *mockStoreCore) List(ctx context.Context, cond api.Cond) ([]*api.TimerRecord, error) {
	args := s.Called(ctx, cond)
	return args.Get(0).([]*api.TimerRecord), args.Error(1)
}

func (s *mockStoreCore) Update(ctx context.Context, timerID string, update *api.TimerUpdate) error {
	args := s.Called(ctx, timerID, update)
	return args.Error(0)
}

func (s *mockStoreCore) Delete(ctx context.Context, timerID string) (bool, error) {
	args := s.Called(ctx, timerID)
	return args.Bool(0), args.Error(1)
}

func (s *mockStoreCore) WatchSupported() bool {
	args := s.Called()
	return args.Bool(0)
}

func (s *mockStoreCore) Watch(ctx context.Context) api.WatchTimerChan {
	args := s.Called(ctx)
	return args.Get(0).(api.WatchTimerChan)
}

func (s *mockStoreCore) Close() {
}

func waitDone(obj any, timeout time.Duration) {
	var ch <-chan struct{}
	switch o := obj.(type) {
	case chan struct{}:
		ch = o
	case <-chan struct{}:
		ch = o
	case *util.WaitGroupWrapper:
		newCh := make(chan struct{})
		ch = newCh

		go func() {
			o.Wait()
			close(newCh)
		}()
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
