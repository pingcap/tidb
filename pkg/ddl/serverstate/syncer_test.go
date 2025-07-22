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

package serverstate_test

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/serverstate"
	util2 "github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/tests/v3/integration"
)

func checkRespKV(t *testing.T, kvCount int, key, val string, kvs ...*mvccpb.KeyValue) {
	require.Len(t, kvs, kvCount)

	if kvCount == 0 {
		return
	}

	kv := kvs[0]
	require.Equal(t, key, string(kv.Key))
	require.Equal(t, val, string(kv.Value))
}

func TestStateSyncerSimple(t *testing.T) {
	vardef.EnableMDL.Store(false)
	if runtime.GOOS == "windows" {
		t.Skip("integration.NewClusterV3 will create file contains a colon which is not allowed on Windows")
	}
	integration.BeforeTestExternal(t)

	origin := schemaver.CheckVersFirstWaitTime
	schemaver.CheckVersFirstWaitTime = 0
	defer func() {
		schemaver.CheckVersFirstWaitTime = origin
	}()

	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer cluster.Terminate(t)
	cli := cluster.RandClient()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg util.WaitGroupWrapper
	serverStateSyncer := serverstate.NewEtcdSyncer(cli, util2.ServerGlobalState)
	// TODO: We can remove it when we call it in newDDL.
	require.NoError(t, serverStateSyncer.Init(ctx))

	// for GetGlobalState
	// for the initial state
	stateInfo := &serverstate.StateInfo{State: serverstate.StateNormalRunning}
	respState, err := serverStateSyncer.GetGlobalState(ctx)
	require.Nil(t, err)
	require.Equal(t, stateInfo, respState)
	require.False(t, serverStateSyncer.IsUpgradingState())
	// for watchCh
	var checkErr string
	stateInfo.State = serverstate.StateUpgrading
	stateInfoByte, err := stateInfo.Marshal()
	require.Nil(t, err)
	checkValue := func() {
		select {
		case resp := <-serverStateSyncer.WatchChan():
			if len(resp.Events) < 1 {
				checkErr = "get chan events count less than 1"
				return
			}
			checkRespKV(t, 1, util2.ServerGlobalState, string(stateInfoByte), resp.Events[0].Kv)
			if stateInfo.State == serverstate.StateUpgrading {
				require.False(t, serverStateSyncer.IsUpgradingState())
			} else {
				require.True(t, serverStateSyncer.IsUpgradingState())
			}
			// for GetGlobalState
			respState, err := serverStateSyncer.GetGlobalState(ctx)
			require.Nil(t, err)
			require.Equal(t, stateInfo, respState)
			if stateInfo.State == serverstate.StateUpgrading {
				require.True(t, serverStateSyncer.IsUpgradingState())
			} else {
				require.False(t, serverStateSyncer.IsUpgradingState())
			}
		case <-time.After(3 * time.Second):
			checkErr = "get update state failed"
			return
		}
	}

	// for update UpdateGlobalState
	// for StateUpgrading
	wg.Run(checkValue)
	require.NoError(t, serverStateSyncer.UpdateGlobalState(ctx, &serverstate.StateInfo{State: serverstate.StateUpgrading}))
	wg.Wait()
	require.Equal(t, "", checkErr)
	// for StateNormalRunning
	stateInfo.State = serverstate.StateNormalRunning
	stateInfoByte, err = stateInfo.Marshal()
	require.Nil(t, err)
	wg.Run(checkValue)
	require.NoError(t, serverStateSyncer.UpdateGlobalState(ctx, &serverstate.StateInfo{State: serverstate.StateNormalRunning}))
	wg.Wait()
	require.Equal(t, "", checkErr)
}
