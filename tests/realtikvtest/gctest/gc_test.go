// Copyright 2025 PingCAP, Inc.
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

package gctest

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestMockExternalServicesCall(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)

	pdStore := store.(kv.StorageWithPD)
	keyspaceID := store.GetCodec().GetKeyspaceID()
	pdcli := pdStore.GetPDClient()

	gccli := pdcli.GetGCStatesClient(uint32(keyspaceID))
	state, err := gccli.GetGCState(context.Background())
	require.NoError(t, err)
	require.Empty(t, state.GCBarriers)

	nowTS := oracle.GoTimeToTS(time.Now())
	ttl := 15 * time.Minute
	const barrierID = "demo-service"
	info, err := gccli.SetGCBarrier(context.Background(), barrierID, nowTS, ttl)
	require.NoError(t, err)
	require.Equal(t, barrierID, info.BarrierID)
	require.Equal(t, nowTS, info.BarrierTS)
	require.Equal(t, ttl, info.TTL)

	state, err = gccli.GetGCState(context.Background())
	require.NoError(t, err)
	require.Len(t, state.GCBarriers, 1)
	info = state.GCBarriers[0]
	require.Equal(t, barrierID, info.BarrierID)
	require.Equal(t, nowTS, info.BarrierTS)
	require.Equal(t, ttl, info.TTL)

	cli := pdcli.GetGCInternalController(uint32(keyspaceID))
	res, err := cli.AdvanceTxnSafePoint(context.Background(), nowTS+1)
	require.NoError(t, err)
	require.Equal(t, res.OldTxnSafePoint, state.TxnSafePoint)
	require.Equal(t, res.NewTxnSafePoint, nowTS)
	require.Equal(t, res.Target, nowTS+1)
	require.True(t, strings.Contains(res.BlockerDescription, "GCBarrier"))

	_, err = gccli.DeleteGCBarrier(context.Background(), barrierID)
	require.NoError(t, err)
}
