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
	"testing"

	"github.com/ngaut/pools"
	sqlsvrapimock "github.com/pingcap/tidb/pkg/domain/sqlsvrapi/mock"
	"github.com/pingcap/tidb/pkg/kv"
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

func newCheckRuntimeSessionPool(t *testing.T, sessionStore kv.Storage) tidbutil.DestroyableSessionPool {
	t.Helper()

	sePool := tidbutil.NewSessionPool(1, func() (pools.Resource, error) {
		se := utilmock.NewContext()
		se.Store = sessionStore
		return se, nil
	}, nil, nil, nil)
	t.Cleanup(sePool.Close)
	return sePool
}

func newCheckRuntimeMockRuntime(
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

func TestCheckRuntime(t *testing.T) {
	t.Run("valid runtime", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := &storeWithKeyspace{keyspace: "task_ks"}
		runtime := newCheckRuntimeMockRuntime(ctrl, store, newCheckRuntimeSessionPool(t, store))
		require.NoError(t, CheckRuntime(runtime, "task_ks"))
	})

	t.Run("store keyspace mismatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		runtime := newCheckRuntimeMockRuntime(ctrl, &storeWithKeyspace{keyspace: "store_ks"}, nil)
		require.ErrorContains(t, CheckRuntime(runtime, "task_ks"),
			"store keyspace mismatch with task: store_ks vs task_ks")
	})

	t.Run("session keyspace mismatch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		store := &storeWithKeyspace{keyspace: "task_ks"}
		runtime := newCheckRuntimeMockRuntime(
			ctrl,
			store,
			newCheckRuntimeSessionPool(t, &storeWithKeyspace{keyspace: "session_ks"}),
		)
		require.ErrorContains(t, CheckRuntime(runtime, "task_ks"),
			"invalid task runtime with mismatched keyspace: task_ks vs session_ks")
	})
}
