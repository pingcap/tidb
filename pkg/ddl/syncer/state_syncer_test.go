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

package syncer_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	. "github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/syncer"
	util2 "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
)

func TestStateSyncerSimple(t *testing.T) {
	variable.EnableMDL.Store(false)
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	origin := syncer.CheckVersFirstWaitTime
	syncer.CheckVersFirstWaitTime = 0
	defer func() {
		syncer.CheckVersFirstWaitTime = origin
	}()

	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ic := infoschema.NewCache(nil, 2)
	ic.Insert(infoschema.MockInfoSchemaWithSchemaVer(nil, 0), 0)
	d := NewDDL(
		ctx,
		WithEtcdClient(cli),
		WithStore(store),
		WithLease(testLease),
		WithInfoCache(ic),
	)
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		require.NoError(t, d.OwnerManager().CampaignOwner())
	})
	defer d.OwnerManager().Cancel()
	// TODO: We can remove it when we call it in newDDL.
	require.NoError(t, d.StateSyncer().Init(ctx))

	// for GetGlobalState
	// for the initial state
	stateInfo := &syncer.StateInfo{State: syncer.StateNormalRunning}
	respState, err := d.StateSyncer().GetGlobalState(ctx)
	require.Nil(t, err)
	require.Equal(t, stateInfo, respState)
	require.False(t, d.StateSyncer().IsUpgradingState())
	// for watchCh
	var checkErr string
	stateInfo.State = syncer.StateUpgrading
	stateInfoByte, err := stateInfo.Marshal()
	require.Nil(t, err)
	checkValue := func() {
		select {
		case resp := <-d.StateSyncer().WatchChan():
			if len(resp.Events) < 1 {
				checkErr = "get chan events count less than 1"
				return
			}
			checkRespKV(t, 1, util2.ServerGlobalState, string(stateInfoByte), resp.Events[0].Kv)
			if stateInfo.State == syncer.StateUpgrading {
				require.False(t, d.StateSyncer().IsUpgradingState())
			} else {
				require.True(t, d.StateSyncer().IsUpgradingState())
			}
			// for GetGlobalState
			respState, err := d.StateSyncer().GetGlobalState(ctx)
			require.Nil(t, err)
			require.Equal(t, stateInfo, respState)
			if stateInfo.State == syncer.StateUpgrading {
				require.True(t, d.StateSyncer().IsUpgradingState())
			} else {
				require.False(t, d.StateSyncer().IsUpgradingState())
			}
		case <-time.After(3 * time.Second):
			checkErr = "get update state failed"
			return
		}
	}

	// for update UpdateGlobalState
	// for StateUpgrading
	wg.Run(checkValue)
	require.NoError(t, d.StateSyncer().UpdateGlobalState(ctx, &syncer.StateInfo{State: syncer.StateUpgrading}))
	wg.Wait()
	require.Equal(t, "", checkErr)
	// for StateNormalRunning
	stateInfo.State = syncer.StateNormalRunning
	stateInfoByte, err = stateInfo.Marshal()
	require.Nil(t, err)
	wg.Run(checkValue)
	require.NoError(t, d.StateSyncer().UpdateGlobalState(ctx, &syncer.StateInfo{State: syncer.StateNormalRunning}))
	wg.Wait()
	require.Equal(t, "", checkErr)
}
