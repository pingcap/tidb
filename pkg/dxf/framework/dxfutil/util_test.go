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

package dxfutil

import (
	goerrors "errors"
	"testing"

	"github.com/ngaut/pools"
	sqlsvrapimock "github.com/pingcap/tidb/pkg/domain/sqlsvrapi/mock"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

type storeWithKeyspace struct {
	kv.Storage
	keyspace string
}

func (s *storeWithKeyspace) GetKeyspace() string {
	return s.keyspace
}

func newCheckTaskRuntimeSessionPool(t *testing.T, sessionStore kv.Storage) tidbutil.DestroyableSessionPool {
	t.Helper()

	sePool := tidbutil.NewSessionPool(1, func() (pools.Resource, error) {
		se := utilmock.NewContext()
		se.Store = sessionStore
		return se, nil
	}, nil, nil, nil)
	t.Cleanup(sePool.Close)
	return sePool
}

func newCheckTaskRuntimeMockRuntime(
	ctrl *gomock.Controller,
	store kv.Storage,
	sePool tidbutil.DestroyableSessionPool,
) *sqlsvrapimock.MockRuntime {
	runtime := sqlsvrapimock.NewMockRuntime(ctrl)
	runtime.EXPECT().Store().Return(store).AnyTimes()
	if sePool != nil {
		runtime.EXPECT().SysSessionPool().Return(sePool).AnyTimes()
	}
	return runtime
}

type taskSessionProvider struct {
	se  sessionctx.Context
	err error
}

func (p *taskSessionProvider) WithNewSession(fn func(se sessionctx.Context) error) error {
	if p.err != nil {
		return p.err
	}
	return fn(p.se)
}

func newTaskSessionProvider(server *sqlsvrapimock.MockServer, currentKS string) *taskSessionProvider {
	se := utilmock.NewContext()
	se.Store = &storeWithKeyspace{keyspace: currentKS}
	se.BindDomainAndSchValidator(server, nil)
	return &taskSessionProvider{se: se}
}

func TestAcquireTaskRuntime(t *testing.T) {
	t.Run("current keyspace uses server runtime", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := &storeWithKeyspace{keyspace: "task_ks"}
		runtime := newCheckTaskRuntimeMockRuntime(ctrl, store, nil)
		server := sqlsvrapimock.NewMockServer(ctrl)
		server.EXPECT().GetRuntime().Return(runtime)

		gotRuntime, releaseRuntime, err := AcquireTaskRuntime(
			newTaskSessionProvider(server, "task_ks"),
			"task_ks",
			"holder",
		)
		require.NoError(t, err)
		require.Same(t, runtime, gotRuntime)
		require.NotNil(t, releaseRuntime)
		require.NotPanics(t, releaseRuntime)
	})

	t.Run("different keyspace acquires and releases handle", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		runtimeHandle := sqlsvrapimock.NewMockKSRuntimeHandle(ctrl)
		runtimeHandle.EXPECT().Release()
		server := sqlsvrapimock.NewMockServer(ctrl)
		server.EXPECT().AcquireKSRuntime("task_ks", "holder").Return(runtimeHandle, nil)

		gotRuntime, releaseRuntime, err := AcquireTaskRuntime(
			newTaskSessionProvider(server, "current_ks"),
			"task_ks",
			"holder",
		)
		require.NoError(t, err)
		require.Same(t, runtimeHandle, gotRuntime)
		require.NotNil(t, releaseRuntime)
		releaseRuntime()
	})

	t.Run("acquire error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		runtimeErr := goerrors.New("ks runtime not found")
		server := sqlsvrapimock.NewMockServer(ctrl)
		server.EXPECT().AcquireKSRuntime("task_ks", "holder").Return(nil, runtimeErr)

		gotRuntime, releaseRuntime, err := AcquireTaskRuntime(
			newTaskSessionProvider(server, "current_ks"),
			"task_ks",
			"holder",
		)
		require.ErrorIs(t, err, runtimeErr)
		require.Nil(t, gotRuntime)
		require.Nil(t, releaseRuntime)
	})

	t.Run("session error", func(t *testing.T) {
		sessionErr := goerrors.New("session error")

		gotRuntime, releaseRuntime, err := AcquireTaskRuntime(
			&taskSessionProvider{err: sessionErr},
			"task_ks",
			"holder",
		)
		require.ErrorIs(t, err, sessionErr)
		require.Nil(t, gotRuntime)
		require.Nil(t, releaseRuntime)
	})
}

func TestCheckTaskRuntime(t *testing.T) {
	t.Run("valid runtime", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := &storeWithKeyspace{keyspace: "task_ks"}
		runtime := newCheckTaskRuntimeMockRuntime(ctrl, store, newCheckTaskRuntimeSessionPool(t, store))
		require.NoError(t, CheckTaskRuntime(runtime, "task_ks"))
	})

	t.Run("store keyspace mismatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		runtime := newCheckTaskRuntimeMockRuntime(ctrl, &storeWithKeyspace{keyspace: "store_ks"}, nil)
		require.ErrorContains(t, CheckTaskRuntime(runtime, "task_ks"),
			"store keyspace mismatch with task: store_ks vs task_ks")
	})

	t.Run("session keyspace mismatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := &storeWithKeyspace{keyspace: "task_ks"}
		runtime := newCheckTaskRuntimeMockRuntime(
			ctrl,
			store,
			newCheckTaskRuntimeSessionPool(t, &storeWithKeyspace{keyspace: "session_ks"}),
		)
		require.ErrorContains(t, CheckTaskRuntime(runtime, "task_ks"),
			"invalid task runtime with mismatched keyspace: task_ks vs session_ks")
	})
}
