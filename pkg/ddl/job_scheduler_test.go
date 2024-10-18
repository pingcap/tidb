// Copyright 2024 PingCAP, Inc.
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

package ddl

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/ddl/mock"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func reduceIntervals(t testing.TB) {
	loopRetryIntBak := schedulerLoopRetryInterval
	schedulerLoopRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		schedulerLoopRetryInterval = loopRetryIntBak
	})
}

func TestMustReloadSchemas(t *testing.T) {
	reduceIntervals(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	loader := mock.NewMockSchemaLoader(ctrl)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sch := &jobScheduler{
		schCtx:       ctx,
		schemaLoader: loader,
	}
	// directly success
	loader.EXPECT().Reload().Return(nil)
	sch.mustReloadSchemas()
	require.True(t, ctrl.Satisfied())
	// success after retry
	loader.EXPECT().Reload().Return(errors.New("mock err"))
	loader.EXPECT().Reload().Return(nil)
	sch.mustReloadSchemas()
	require.True(t, ctrl.Satisfied())
	// exit on context cancel
	loader.EXPECT().Reload().Do(func() error {
		cancel()
		return errors.New("mock err")
	})
	sch.mustReloadSchemas()
	require.True(t, ctrl.Satisfied())
}

func TestUnSyncedJobTracker(t *testing.T) {
	jt := newUnSyncedJobTracker()
	jt.addUnSynced(1)
	require.True(t, jt.isUnSynced(1))
	jt.removeUnSynced(1)
	require.False(t, jt.isUnSynced(1))
}

func TestUpdateClusterStateTicker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ownerMgr := owner.NewMockManager(ctx, "owner_damen_test", nil, DDLOwnerKey)
	stateSyncer := serverstate.NewMemSyncer()
	stateSyncer.Init(ctx)

	ddlCtx := ddlCtx{
		ownerManager:      ownerMgr,
		serverStateSyncer: stateSyncer,
	}
	scheCtx, cancelFunc := context.WithCancel(context.TODO())
	defer cancelFunc()
	s := &jobScheduler{
		schCtx: scheCtx,
		cancel: cancelFunc,
		ddlCtx: &ddlCtx,
	}

	// run the ticker.
	s.wg.RunWithLog(s.updateClusterStateTicker)

	require.False(t, s.serverStateSyncer.IsUpgradingState())
	opType, err := owner.GetOwnerOpValue(ctx, nil, DDLOwnerKey, "")
	require.NoError(t, err)
	require.Equal(t, opType, owner.OpNone)

	// upgrading state
	err = s.serverStateSyncer.UpdateGlobalState(ctx, serverstate.NewStateInfo(serverstate.StateUpgrading))
	require.NoError(t, err)
	require.True(t, s.serverStateSyncer.IsUpgradingState())

	for {
		if len(s.serverStateSyncer.WatchChan()) == 0 {
			time.Sleep(time.Microsecond * 50)
			break
		}
		time.Sleep(time.Microsecond * 50)
	}

	opType, err = owner.GetOwnerOpValue(ctx, nil, DDLOwnerKey, "")
	require.NoError(t, err)
	require.Equal(t, opType, owner.OpSyncUpgradingState)

	// inject failpoint.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/ddl/failed-before-id-maps-saved", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/ddl/failed-before-id-maps-saved"))
	}()

	// normal state
	err = s.serverStateSyncer.UpdateGlobalState(ctx, serverstate.NewStateInfo(serverstate.StateNormalRunning))
	require.NoError(t, err)
	require.False(t, s.serverStateSyncer.IsUpgradingState())

	for {
		if len(s.serverStateSyncer.WatchChan()) == 0 {
			time.Sleep(time.Microsecond * 50)
			break
		}
		time.Sleep(time.Microsecond * 50)
	}

	opType, err = owner.GetOwnerOpValue(ctx, nil, DDLOwnerKey, "")
	require.NoError(t, err)
	require.Equal(t, opType, owner.OpNone)
}
